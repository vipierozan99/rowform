"""rowform's hydrated values equal SQLAlchemy's own, field by field.

The load-bearing claim of the whole design is that bypassing `Row` changes
*nothing* about the values: every column's own `result_processor` runs, so a
`DateTime` on sqlite (stored as a string), a `Numeric`, a `Uuid`, an `Enum` or a
`JSON` column decodes to exactly the Python object SQLAlchemy's result layer
would have produced. An earlier hand-written converter table got this wrong for 7
of 8 columns, returning plausible-looking values of the wrong type
(docs/BENCHMARKS.md, lesson 11).

So this builds one physical schema and reads it two ways — through a rowform
`Base` and through a stock SQLAlchemy `DeclarativeBase` ORM over the same rows —
and asserts that every column of every row matches, in both value *and* type.
Run on both backends via the parametrised fixture, because sqlite and postgres
decode these types on opposite sides of the driver.
"""

from __future__ import annotations

import datetime as dt
import decimal
import enum
import uuid

import pytest
import sqlalchemy as sa
from sync.conftest import engine_at, pg_url, sqlite_url
from sqlalchemy import orm
from sqlalchemy.orm import Mapped

import rowform as rf
import rowform.sync as rfs


class Colour(enum.Enum):
    RED = "red"
    GREEN = "green"
    BLUE = "blue"


# --------------------------------------------------------------------------
# The same two tables, declared twice: once as a rowform Base, once as a stock
# SQLAlchemy DeclarativeBase. Column types are pinned identically on both sides
# so the physical schema rowform creates is the one the ORM reads back.
# --------------------------------------------------------------------------


class RfBase(rf.Base):
    metadata = sa.MetaData()


class RfMaker(RfBase):
    __tablename__ = "eq_maker"

    id: Mapped[int] = rf.mapped_column(primary_key=True)
    name: Mapped[str]
    founded: Mapped[dt.date]


class RfWidget(RfBase):
    __tablename__ = "eq_widget"

    id: Mapped[int] = rf.mapped_column(primary_key=True)
    maker_id: Mapped[int] = rf.mapped_column(sa.ForeignKey("eq_maker.id"))
    name: Mapped[str]
    in_stock: Mapped[bool]
    made_at: Mapped[dt.datetime]
    made_on: Mapped[dt.date]
    made_time: Mapped[dt.time]
    price: Mapped[decimal.Decimal] = rf.mapped_column(sa.Numeric(12, 3))
    weight: Mapped[float]
    # A named type so the postgres `CREATE TYPE` does not collide with any other
    # module's enum in the shared test database: an unnamed `Enum(Colour)` maps
    # to type "colour", which conftest also declares, and a lingering dependant
    # of that shared type silently blocks this suite's `DROP TYPE` on teardown.
    colour: Mapped[Colour] = rf.mapped_column(sa.Enum(Colour, name="eq_colour"))
    serial: Mapped[uuid.UUID]
    spec: Mapped[dict]
    thumbnail: Mapped[bytes]
    note: Mapped[str | None]


class SaBase(orm.DeclarativeBase):
    pass


class SaMaker(SaBase):
    __tablename__ = "eq_maker"

    id: orm.Mapped[int] = orm.mapped_column(primary_key=True)
    name: orm.Mapped[str]
    founded: orm.Mapped[dt.date]


class SaWidget(SaBase):
    __tablename__ = "eq_widget"

    id: orm.Mapped[int] = orm.mapped_column(primary_key=True)
    maker_id: orm.Mapped[int] = orm.mapped_column(sa.ForeignKey("eq_maker.id"))
    name: orm.Mapped[str]
    in_stock: orm.Mapped[bool]
    made_at: orm.Mapped[dt.datetime]
    made_on: orm.Mapped[dt.date]
    made_time: orm.Mapped[dt.time]
    price: orm.Mapped[decimal.Decimal] = orm.mapped_column(sa.Numeric(12, 3))
    weight: orm.Mapped[float]
    colour: orm.Mapped[Colour] = orm.mapped_column(sa.Enum(Colour, name="eq_colour"))
    serial: orm.Mapped[uuid.UUID]
    # dict has no default SA mapping; rowform maps it through DEFAULT_TYPE_MAP.
    spec: orm.Mapped[dict] = orm.mapped_column(sa.JSON)
    thumbnail: orm.Mapped[bytes]
    note: orm.Mapped[str | None]


MAKERS = [
    {"id": 1, "name": "acme", "founded": dt.date(1990, 5, 1)},
    {"id": 2, "name": "globex", "founded": dt.date(2001, 12, 31)},
]

WIDGETS = [
    {
        "id": 10,
        "maker_id": 1,
        "name": "gadget",
        "in_stock": True,
        "made_at": dt.datetime(2024, 3, 1, 12, 30, 45, 123456),
        "made_on": dt.date(2024, 3, 1),
        "made_time": dt.time(9, 15, 0),
        "price": decimal.Decimal("19.990"),
        "weight": 1.5,
        "colour": Colour.RED,
        "serial": uuid.UUID("12345678-1234-5678-1234-567812345678"),
        "spec": {"a": [1, 2], "b": "x"},
        "thumbnail": b"\x00\x01binary",
        "note": None,
    },
    {
        "id": 11,
        "maker_id": 1,
        "name": "sprocket",
        "in_stock": False,
        "made_at": dt.datetime(2023, 1, 2, 3, 4, 5),
        "made_on": dt.date(2023, 1, 2),
        "made_time": dt.time(23, 59, 59),
        "price": decimal.Decimal("0.010"),
        "weight": 42.0,
        "colour": Colour.BLUE,
        "serial": uuid.UUID("00000000-0000-0000-0000-000000000001"),
        "spec": {"nested": {"k": True}},
        "thumbnail": b"",
        "note": "chip",
    },
]


def _sync_pg_url(dsn: str) -> str:
    """The same DSN, for a synchronous psycopg SQLAlchemy engine.

    rowform drives asyncpg; the reference reads here go through
    a stock synchronous `Session`, which needs `postgresql+psycopg://`.
    """
    return dsn.replace("postgresql://", "postgresql+psycopg://", 1)


def _seed(rf_engine: rfs.Engine) -> None:
    rf_engine.drop_all(RfBase.metadata)
    rf_engine.create_all(RfBase.metadata)
    rf_engine.execute_many(sa.insert(RfMaker.__table__), MAKERS)
    rf_engine.execute_many(sa.insert(RfWidget.__table__), WIDGETS)


@pytest.fixture(params=["sqlite", "postgres"])
def backend(request, tmp_path):
    """A rowform engine and a stock SQLAlchemy engine over one physical database.

    rowform owns the schema and the seed; the SQLAlchemy side only reads. Both
    backends, for the reason the rest of the suite runs both: the interesting
    half is the driver that does *not* decode these types natively.
    """
    if request.param == "sqlite":
        path = str(tmp_path / "eq.sqlite3")
        url, sync_url = sqlite_url(path), f"sqlite:///{path}"
    else:
        dsn = request.getfixturevalue("pg_dsn")
        url, sync_url = pg_url(dsn), _sync_pg_url(dsn)

    sa_engine = sa.create_engine(sync_url)
    with engine_at(url) as rf_engine:
        try:
            _seed(rf_engine)
            yield rf_engine, sa_engine
        finally:
            rf_engine.drop_all(RfBase.metadata)
            sa_engine.dispose()


def _canonical_type(value: object) -> type:
    """The type to compare on.

    rowform reads through asyncpg and the reference through psycopg (see
    `_sync_pg_url`), and the two drivers agree on the Python type of every value
    here except one: asyncpg decodes `uuid` to its own `pgproto.UUID`, a
    `uuid.UUID` subclass, while psycopg returns stdlib `uuid.UUID`. That is a
    driver choice, not a row-layer one — SQLAlchemy's own `Row` over asyncpg
    returns `pgproto.UUID` too — so collapse the subclass to `uuid.UUID` before
    the identity check rather than let the two drivers' UUID reprs read as a
    type mismatch.
    """
    if isinstance(value, uuid.UUID):
        return uuid.UUID
    return type(value)


def _assert_same(rf_obj: object, sa_obj: object, columns) -> None:
    """Every column matches in value and in type — the type check is the point,
    since the failure mode being guarded against is a right-looking value of the
    wrong type."""
    for name in columns:
        rf_val = getattr(rf_obj, name)
        sa_val = getattr(sa_obj, name)
        assert rf_val == sa_val, f"{name}: {rf_val!r} != {sa_val!r}"
        assert _canonical_type(rf_val) is _canonical_type(sa_val), (
            f"{name}: {type(rf_val).__name__} != {type(sa_val).__name__}"
        )


def test_a_whole_model_matches_the_orm_field_by_field(backend):
    rf_engine, sa_engine = backend

    rf_rows = rf_engine.fetch_all(sa.select(RfWidget).order_by(RfWidget.id))
    with orm.Session(sa_engine) as session:
        sa_rows = session.scalars(sa.select(SaWidget).order_by(SaWidget.id)).all()

    assert len(rf_rows) == len(sa_rows) == len(WIDGETS)
    for rf_row, sa_row in zip(rf_rows, sa_rows, strict=True):
        _assert_same(rf_row, sa_row, RfWidget.__columns__)


def test_a_join_matches_the_orm_field_by_field(backend):
    rf_engine, sa_engine = backend

    rf_rows = rf_engine.fetch_all(
        sa.select(RfMaker, RfWidget).join(RfWidget).order_by(RfWidget.id)
    )
    with orm.Session(sa_engine) as session:
        sa_rows = session.execute(
            sa.select(SaMaker, SaWidget).join(SaWidget).order_by(SaWidget.id)
        ).all()

    assert len(rf_rows) == len(sa_rows)
    for (rf_maker, rf_widget), (sa_maker, sa_widget) in zip(rf_rows, sa_rows, strict=True):
        _assert_same(rf_maker, sa_maker, RfMaker.__columns__)
        _assert_same(rf_widget, sa_widget, RfWidget.__columns__)
