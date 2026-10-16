"""Alembic works off `Base.metadata`, with no DeclarativeBase involved.

This is the motivation for the whole design:
DDL, reflection and migrations are large, mature, and already written. If
autogenerate did not work against a metaclass-built `MetaData`, there would be no
reason to depend on SQLAlchemy at all.

`target_metadata = Base.metadata` is the single most-copied line in every Alembic
`env.py`, and these tests assert exactly that line's worth of integration.
"""

from __future__ import annotations

import datetime as dt

import pytest
import sqlalchemy as sa
from conftest import Base, Wide
from sqlalchemy.orm import Mapped

import rowform as rf

alembic = pytest.importorskip("alembic")
from alembic.autogenerate import compare_metadata, produce_migrations
from alembic.autogenerate.api import AutogenContext
from alembic.autogenerate.render import _render_cmd_body
from alembic.migration import MigrationContext

RENDER_OPTS = {
    "sqlalchemy_module_prefix": "sa.",
    "alembic_module_prefix": "op.",
}


@pytest.fixture
def blank_db():
    engine = sa.create_engine("sqlite:///:memory:")
    yield engine
    engine.dispose()


def context(conn, metadata):
    return MigrationContext.configure(
        conn, opts={"target_metadata": metadata, "compare_type": True, **RENDER_OPTS}
    )


class TestAutogenerate:
    def test_an_empty_database_needs_every_table_created(self, blank_db):
        with blank_db.connect() as conn:
            diffs = compare_metadata(context(conn, Base.metadata), Base.metadata)
        created = {d[1].name for d in diffs if d[0] == "add_table"}
        assert created == set(Base.metadata.tables)

    def test_the_rendered_migration_is_real_alembic_code(self, blank_db):
        with blank_db.connect() as conn:
            ctx = context(conn, Base.metadata)
            migrations = produce_migrations(ctx, Base.metadata)
            body = _render_cmd_body(
                migrations.upgrade_ops, AutogenContext(ctx, metadata=Base.metadata)
            )

        assert "op.create_table('t_authors'" in body
        assert "sa.Column('id', sa.Integer(), nullable=False)" in body
        assert "sa.PrimaryKeyConstraint('id')" in body
        assert "sa.ForeignKeyConstraint(['author_id'], ['t_authors.id'], )" in body
        assert "sa.Column('note', sa.String(), nullable=True)" in body

    def test_declared_types_survive_into_the_migration(self, blank_db):
        with blank_db.connect() as conn:
            ctx = context(conn, Base.metadata)
            body = _render_cmd_body(
                produce_migrations(ctx, Base.metadata).upgrade_ops,
                AutogenContext(ctx, metadata=Base.metadata),
            )
        assert "sa.Numeric(precision=12, scale=3)" in body
        assert "sa.Uuid()" in body
        assert "sa.Enum('RED', 'BLUE'" in body

    def test_a_matching_schema_produces_no_diff(self, blank_db):
        Base.metadata.create_all(blank_db)
        with blank_db.connect() as conn:
            assert compare_metadata(context(conn, Base.metadata), Base.metadata) == []

    def test_a_new_field_shows_up_as_add_column(self, blank_db):
        Base.metadata.create_all(blank_db)

        drifted = sa.MetaData()
        for table in Base.metadata.tables.values():
            table.to_metadata(drifted)
        drifted.tables["t_authors"].append_column(sa.Column("age", sa.Integer))

        with blank_db.connect() as conn:
            diffs = compare_metadata(context(conn, drifted), drifted)
        assert [(d[0], d[2], d[3].name) for d in diffs] == [("add_column", "t_authors", "age")]

    def test_a_table_no_longer_declared_shows_up_as_remove_table(self, blank_db):
        Base.metadata.create_all(blank_db)

        drifted = sa.MetaData()
        for name, table in Base.metadata.tables.items():
            if name != "t_tags":
                table.to_metadata(drifted)

        with blank_db.connect() as conn:
            diffs = compare_metadata(context(conn, drifted), drifted)
        assert [(d[0], d[1].name) for d in diffs] == [("remove_table", "t_tags")]


def test_a_model_added_later_joins_the_same_metadata(blank_db):
    """The realistic workflow: declare a model, and it is in the next migration."""

    class Scratch(rf.Base):
        metadata = sa.MetaData()

    class Event(Scratch):
        __tablename__ = "events"
        id: Mapped[int] = rf.mapped_column(primary_key=True)
        at: Mapped[dt.datetime]

    Scratch.metadata.create_all(blank_db)

    class Later(Scratch):
        __tablename__ = "later"
        id: Mapped[int] = rf.mapped_column(primary_key=True)
        event_id: Mapped[int] = rf.mapped_column(sa.ForeignKey("events.id"))

    with blank_db.connect() as conn:
        diffs = compare_metadata(context(conn, Scratch.metadata), Scratch.metadata)
    assert [d[1].name for d in diffs if d[0] == "add_table"] == ["later"]
    assert Event.__table__ is not Later.__table__


def test_column_order_drift_is_invisible_to_alembic(blank_db):
    """The column-order hazard, asserted rather than asserted-away. Adding a
    mixin moves its columns to the front of CREATE TABLE, and autogenerate
    reports *nothing* — which is why `__column_order__` exists and is worth
    pinning on an existing table."""
    Base.metadata.create_all(blank_db)

    reordered = sa.MetaData()
    original = Base.metadata.tables["t_wide"]
    sa.Table(
        "t_wide",
        reordered,
        *[
            sa.Column(c.name, c.type, primary_key=c.primary_key, nullable=c.nullable)
            for c in reversed(list(original.columns))
        ],
    )
    for name, table in Base.metadata.tables.items():
        if name != "t_wide":
            table.to_metadata(reordered)

    with blank_db.connect() as conn:
        diffs = compare_metadata(context(conn, reordered), reordered)
    assert diffs == [], "if this ever fails, alembic learned to diff column order"
    assert list(reordered.tables["t_wide"].c.keys()) != list(Wide.__column_order__)


def test_a_view_is_created_by_a_migration_and_invisible_to_autogenerate(blank_db):
    """`rf.view()` stays out of `metadata.tables`, so autogenerate neither adds it
    as a table nor, once it exists, reports it as one to drop.
    """
    from alembic.operations import Operations

    metadata = sa.MetaData()
    table = sa.Table("t_people", metadata, sa.Column("id", sa.Integer, primary_key=True))
    people = rf.view("v_people", metadata, sa.select(table.c.id))
    with blank_db.begin() as conn:
        metadata.tables["t_people"].create(conn)
        Operations(MigrationContext.configure(conn)).execute(rf.CreateView(people))
        assert "v_people" in sa.inspect(conn).get_view_names()
        diffs = compare_metadata(context(conn, metadata), metadata)
    assert diffs == []
