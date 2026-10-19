"""`rf.view()`: a database view a model can sit on.

The view is a `sa.Table`, as SQLAlchemy's own reflection presents one, kept out
of the caller's `MetaData.tables` so `create_all()` never emits `CREATE TABLE`
for it; `CREATE VIEW`/`DROP VIEW` ride on that metadata's create/drop events.
"""

from __future__ import annotations

import pytest
import sqlalchemy as sa
from conftest import Author, Book
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import Mapped

import rowform as rf


def ada_books(metadata: sa.MetaData) -> sa.Table:
    return rf.view(
        "v_ada_books",
        metadata,
        sa.select(Book.id, Book.title).where(Book.author_id == 1),
    )


class TestDeclaration:
    def test_it_is_a_table_outside_the_metadata(self):
        metadata = sa.MetaData()
        view = ada_books(metadata)
        assert isinstance(view, sa.Table)
        assert view.c.keys() == ["id", "title"]
        assert isinstance(view.c.title.type, sa.String)
        assert metadata.tables == {}, "create_all() must not CREATE TABLE it"
        assert rf.views(metadata) == {"v_ada_books": view}

    def test_a_statement_over_it_is_cacheable(self):
        """Every SELECT over a view compiled afresh is what made views slow."""
        view = ada_books(sa.MetaData())
        first = sa.select(view).where(view.c.id == 10)._generate_cache_key()
        second = sa.select(view).where(view.c.id == 11)._generate_cache_key()
        assert first is not None
        assert first == second
        assert [b.value for b in second.bindparams] == [11]

    def test_keys_and_names_follow_the_select(self):
        """A labelled column is a view column under its label."""
        view = rf.view(
            "v_titles", sa.MetaData(), sa.select(Book.title.label("headline"), Book.id)
        )
        assert view.c.keys() == ["headline", "id"]

    def test_a_name_is_declared_once_per_metadata(self):
        metadata = sa.MetaData()
        ada_books(metadata)
        with pytest.raises(rf.DeclarationError, match="v_ada_books"):
            ada_books(metadata)

    def test_the_ddl_renders_its_select_with_literals(self):
        view = ada_books(sa.MetaData())
        create = str(rf.CreateView(view).compile(dialect=postgresql.dialect()))
        assert create.startswith("CREATE VIEW v_ada_books AS SELECT")
        assert "t_books.author_id = 1" in create
        drop = str(rf.DropView(view).compile(dialect=postgresql.dialect()))
        assert drop == "DROP VIEW IF EXISTS v_ada_books"

    def test_a_model_sits_on_it_without_annotating_table(self):
        class AdaBook(rf.Base):
            metadata = sa.MetaData()
            __table__ = ada_books(metadata)
            id: Mapped[int]
            title: Mapped[str]

        assert rf.model_for(AdaBook.__table__) is AdaBook
        assert AdaBook.title is AdaBook.__table__.c.title


@pytest.fixture
async def views(engine):
    metadata = sa.MetaData()
    view = ada_books(metadata)
    await engine.drop_all(metadata)
    await engine.create_all(metadata)
    yield metadata, view
    await engine.drop_all(metadata)


class TestOnTheDatabase:
    async def test_create_all_creates_it_and_a_model_reads_it(self, engine, views):
        _, view = views

        class AdaBook(rf.Base):
            __table__ = view
            title: Mapped[str]
            id: Mapped[int]

        rows = await engine.fetch_all(sa.select(AdaBook).order_by(AdaBook.id))
        assert rows == [AdaBook(title="structures", id=10), AdaBook(title="algorithms", id=11)]

    async def test_create_all_twice_is_harmless(self, engine, views):
        metadata, _ = views
        await engine.create_all(metadata)

    async def test_drop_all_drops_it(self, engine, views):
        metadata, view = views
        await engine.drop_all(metadata)
        async with engine.connect() as conn:
            names = await conn.sa_connection.run_sync(
                lambda sync: sa.inspect(sync).get_view_names()
            )
        assert view.name not in names
        await engine.create_all(metadata)  # the fixture drops it again

    async def test_a_view_over_a_view(self, engine, views):
        """Created in declaration order and dropped in reverse, so nothing needs
        CASCADE, which would also drop objects rowform does not know about."""
        metadata, base = views
        top = rf.view("v_ada_first", metadata, sa.select(base).where(base.c.id == 10))
        await engine.create_all(metadata)
        assert await engine.fetch_all(sa.select(top.c.title)) == ["structures"]
        await engine.drop_all(metadata)
        await engine.create_all(metadata)
        assert rf.views(metadata)["v_ada_first"] is top


def test_author_is_untouched_by_a_view_over_it():
    """The view's columns are its own; the source table keeps its columns."""
    view = rf.view("v_authors", sa.MetaData(), sa.select(Author.id, Author.name))
    assert view.c.id is not Author.__table__.c.id
