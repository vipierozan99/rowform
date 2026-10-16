"""Types that must be inferred exactly. Checked by basedpyright; never run.

`assert_type` compares for exact equality, so a type that has decayed to `Any`
fails here — which is the failure mode worth guarding, since `Any` silently makes
every other assertion pass.

The whole reason the declaration layer is a base class rather than a decorator is
this file: a decorator *factory*, which is what `@model(metadata)` would have to
be, erases every field type to `Any` and each `assert_type` below would fail.
"""

from __future__ import annotations

import contextlib
import datetime as dt
import decimal
import uuid
from typing import Any, assert_type

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import (
    AsyncResult,
    AsyncScalarResult,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import InstrumentedAttribute, Mapped

import rowform as rf


class Base(rf.Base):
    metadata = sa.MetaData()


class Author(Base):
    __tablename__ = "authors"

    id: Mapped[int] = rf.mapped_column(primary_key=True)
    name: Mapped[str]
    active: Mapped[bool]
    born: Mapped[dt.date | None]


class Book(Base):
    __tablename__ = "books"

    id: Mapped[int] = rf.mapped_column(primary_key=True)
    author_id: Mapped[int] = rf.mapped_column(sa.ForeignKey("authors.id"))
    title: Mapped[str]
    price: Mapped[decimal.Decimal]
    uid: Mapped[uuid.UUID]


author = Author(id=1, name="ada", active=True, born=None)

# --- instance attributes keep their declared types -------------------------
assert_type(author.id, int)
assert_type(author.name, str)
assert_type(author.active, bool)
assert_type(author.born, "dt.date | None")

# --- class attributes are SQL expressions ----------------------------------
# Declared as InstrumentedAttribute, an sa.Column at runtime. Both are
# ColumnOperators, which is why the comparisons below work; the mismatch is an
# inherited type-level fiction from Mapped.__get__'s overloads.
assert_type(Author.id, InstrumentedAttribute[int])
assert_type(Author.name, InstrumentedAttribute[str])

assert_type(Author.id > 100, sa.ColumnElement[bool])
assert_type(Author.name == "ada", sa.ColumnElement[bool])
assert_type(Author.id.in_([1, 2]), sa.BinaryExpression[bool])

# --- statements carry their selected types ---------------------------------
assert_type(sa.select(Author), sa.Select[tuple[Author]])
assert_type(sa.select(Author.name), sa.Select[tuple[str]])
assert_type(sa.select(Author, Book), sa.Select[tuple[Author, Book]])
assert_type(sa.select(Author, Book.title), sa.Select[tuple[Author, str]])
assert_type(sa.select(Author).where(Author.id > 1), sa.Select[tuple[Author]])

# --- an alias types exactly as the model it aliases ------------------------
# The whole reason `alias()` is declared `type[_M]`: an alias class of its own
# could only expose `__getattr__`, and every field below would be `Any`.
manager = rf.alias(Author, "mgr")

assert_type(manager.id, InstrumentedAttribute[int])
assert_type(manager.born, InstrumentedAttribute["dt.date | None"])
assert_type(manager.id > 100, sa.ColumnElement[bool])
assert_type(sa.select(manager), sa.Select[tuple[Author]])
assert_type(sa.select(Author, manager), sa.Select[tuple[Author, Author]])

# A declared subquery or CTE is the model too — which is the whole point of
# `of=` refusing anything but that model's exact columns.
recent = rf.alias(Author, of=sa.select(Author).limit(10).subquery())

assert_type(recent.name, InstrumentedAttribute[str])
assert_type(sa.select(recent), sa.Select[tuple[Author]])


# --- prepare() and fetch_all() preserve them -------------------------------
engine = rf.Engine(create_async_engine("sqlite+aiosqlite:///app.db"))
assert_type(engine.around, rf.Around | None)
engine.around = lambda sql: contextlib.nullcontext()

assert_type(engine.prepare(sa.select(Author)), rf.CoreQuery[Author])
assert_type(engine.prepare(sa.select(Author.name)), rf.CoreQuery[str])
assert_type(engine.prepare(sa.select(Author, Book)), rf.CoreQuery[tuple[Author, Book]])


async def reads() -> None:
    # One selected entity yields that entity, model or scalar.
    assert_type(await engine.fetch_all(sa.select(Author)), list[Author])
    assert_type(await engine.fetch_all(sa.select(Author.name)), list[str])
    assert_type(await engine.fetch_all(sa.select(Book.price)), list[decimal.Decimal])
    assert_type(await engine.fetch_all(sa.select(Book.uid)), list[uuid.UUID])

    # Two or more yield a tuple, in select order.
    assert_type(await engine.fetch_all(sa.select(Author, Book)), list[tuple[Author, Book]])
    assert_type(
        await engine.fetch_all(sa.select(Author.name, Author.id)), list[tuple[str, int]]
    )
    assert_type(
        await engine.fetch_all(sa.select(Author, Book, Book.title)),
        list[tuple[Author, Book, str]],
    )

    # A hoisted query keeps its row type.
    hoisted = engine.prepare(sa.select(Author).where(Author.id > sa.bindparam("floor")))
    assert_type(await engine.fetch_all(hoisted, floor=3), list[Author])

    # fetch_one shapes its row exactly as fetch_all shapes its rows, and adds the
    # None. Before it carried the arity overloads, everything past one selected
    # entity was silently `Any`.
    assert_type(await engine.fetch_one(sa.select(Author)), "Author | None")
    assert_type(await engine.fetch_one(sa.select(Author.name)), "str | None")
    assert_type(await engine.fetch_one(sa.select(Author, Book)), "tuple[Author, Book] | None")
    assert_type(
        await engine.fetch_one(sa.select(Author.name, Author.id)), "tuple[str, int] | None"
    )
    assert_type(
        await engine.fetch_one(sa.select(Author, Book, Book.title)),
        "tuple[Author, Book, str] | None",
    )
    assert_type(
        await engine.fetch_one(sa.select(Author.id, Author.name, Book.id, Book.title)),
        "tuple[int, str, int, str] | None",
    )
    assert_type(await engine.fetch_one(hoisted, floor=3), "Author | None")

    # A self-join is a list of pairs of the model, with no cast.
    assert_type(
        await engine.fetch_all(sa.select(Author, manager).join(manager, Author.id < manager.id)),
        list[tuple[Author, Author]],
    )
    assert_type(await engine.fetch_all(sa.select(recent)), list[Author])


async def one_column() -> None:
    """There is no `fetch_value`: one selected entity already arrives unwrapped,
    so `fetch_one` *is* the "one value" read.

    For one column of a wider statement, narrow the statement rather than the
    row. That is the better half of the trade — the type stays exact, and the
    discarded column never reaches the database.
    """
    assert_type(await engine.fetch_one(sa.select(Author.name)), "str | None")
    assert_type(await engine.fetch_one(sa.select(Book.price)), "decimal.Decimal | None")
    assert_type(await engine.fetch_one(sa.select(sa.func.count())), "int | None")

    wide = sa.select(Author.id, Author.name)
    assert_type(wide, sa.Select[tuple[int, str]])
    assert_type(wide.with_only_columns(Author.id), sa.Select[tuple[int]])
    assert_type(await engine.fetch_one(wide.with_only_columns(Author.id)), "int | None")
    assert_type(await engine.fetch_all(wide.with_only_columns(Author.name)), list[str])


async def documented() -> None:
    """The example snippets, in the exact spellings the docs print beside a type.

    A doc comment saying `# int | None` is a claim, and an untested claim is the
    kind that quietly stops being true. These are the ones written out in
    README.md and GUIDE.md — same statement, same annotation.
    """
    assert_type(
        await engine.fetch_one(sa.select(Author, Book).join(Book)),
        "tuple[Author, Book] | None",
    )
    assert_type(
        await engine.fetch_one(sa.select(sa.func.count()).select_from(Author)), "int | None"
    )
    assert_type(
        await engine.fetch_one(sa.func.count().select().select_from(Author.__table__)),
        "int | None",
    )
    assert_type(
        await engine.fetch_one(
            sa.select(Author.id, Author.name).with_only_columns(Author.id)
        ),
        "int | None",
    )


async def streams() -> None:
    # fetch_iter is overloaded on the same arity rule as fetch_all, so the loop
    # variable is exact rather than Any.
    async for author in engine.fetch_iter(sa.select(Author)):
        assert_type(author, Author)

    async for name in engine.fetch_iter(sa.select(Author.name), chunk=10):
        assert_type(name, str)

    async for pair in engine.fetch_iter(sa.select(Author, Book)):
        assert_type(pair, tuple[Author, Book])

    hoisted = engine.prepare(sa.select(Author))
    async for hoisted_author in engine.fetch_iter(hoisted, chunk=100):
        assert_type(hoisted_author, Author)


async def scopes() -> None:
    async with engine.begin() as conn:
        assert_type(conn.in_transaction(), bool)
        assert_type(rf.active_connection(), "rf.Connection | None")
        assert_type(await conn.fetch_all(sa.select(Author)), list[Author])

    # A scope's reads are typed exactly as the engine's — the hot track is the
    # same track wherever it is reached from, and half of why it exists is that
    # the row type survives.
    async with engine.connect() as conn:
        assert_type(await conn.fetch_all(sa.select(Author, Book)), list[tuple[Author, Book]])
        assert_type(await conn.fetch_one(sa.select(Author)), "Author | None")
        assert_type(await conn.fetch_one(sa.select(Author, Book)), "tuple[Author, Book] | None")
        assert_type(await conn.fetch_one(sa.select(Author.name)), "str | None")
        assert_type(await conn.fetch_all(engine.prepare(sa.select(Author))), list[Author])

        async for author in conn.fetch_iter(sa.select(Author), chunk=100):
            assert_type(author, Author)

        # The connection track carries the same per-arity overloads as the engine,
        # up to four selected entities (F3).
        async for triple in conn.fetch_iter(sa.select(Author, Book, Author.name)):
            assert_type(triple, tuple[Author, Book, str])

        # The compatibility track is SQLAlchemy's own types, deliberately: these
        # are `Result` and friends, not anything rowform defines.
        assert_type(await conn.execute(sa.select(Author)), sa.Result[Any])
        assert_type(await conn.scalars(sa.select(Author)), sa.ScalarResult[Any])
        assert_type(await conn.stream(sa.select(Author)), AsyncResult[Any])
        assert_type(await conn.stream_scalars(sa.select(Author)), AsyncScalarResult[Any])
        assert_type(await conn.exec_driver_sql("select 1"), sa.Result[Any])

    # A read on a connection an existing application already holds — the adoption
    # path, so it has to type as any other scope does.
    session = async_sessionmaker(engine.sa_engine)()
    async with engine.connect(bind=session) as conn:
        assert_type(await conn.fetch_all(sa.select(Author)), list[Author])


# --- schema surface --------------------------------------------------------
assert_type(Base.metadata, sa.MetaData)
assert_type(Author.__table__, sa.Table)


class ActiveAuthor(Base):
    # No `ClassVar[...]` annotation needed: a view is a `sa.Table`.
    __table__ = rf.view("active_authors", Base.metadata, sa.select(Author.id, Author.name))
    id: Mapped[int]
    name: Mapped[str]


assert_type(rf.view("v", sa.MetaData(), sa.select(Author.id)), rf.View)
assert_type(rf.views(Base.metadata), dict[str, rf.View])
assert_type(Author.__tablename__, str)
assert_type(Author.__column_order__, tuple[str, ...])


# --- mixins ----------------------------------------------------------------
class Timestamped(Base):
    created: Mapped[dt.datetime]


class Review(Timestamped, kw_only=True):
    __tablename__ = "reviews"

    id: Mapped[int] = rf.mapped_column(primary_key=True)
    body: Mapped[str]


# A mixin's fields are real constructor parameters, not merely visible
# attributes — the metaclass is shared, so the checker sees one dataclass.
review = Review(created=dt.datetime(2024, 1, 1), id=1, body="good")
assert_type(review.created, dt.datetime)
assert_type(review.body, str)


# --- what is deliberately Any ----------------------------------------------
async def wide() -> None:
    # Past four selected entities the row degrades, rather than growing
    # overloads without end.
    assert_type(
        await engine.fetch_all(
            sa.select(Author.id, Author.name, Author.active, Author.born, Book.title)
        ),
        list[Any],
    )

    # A write's result is the driver's own answer — an int on sqlite and
    # psycopg, a status string on asyncpg — so it is not normalised.
    assert_type(await engine.execute(sa.insert(Author.__table__)), Any)
    assert_type(await engine.execute_many(sa.insert(Author.__table__), [{"id": 1}]), Any)
    assert_type(await engine.scalar(sa.select(sa.func.count())), Any)
    assert_type(await engine.scalars(sa.select(Author)), Any)

    # These two are not Any, and the difference is the point: `copy_in` counts
    # rows itself rather than relaying a driver report, and DDL returns nothing.
    assert_type(await engine.copy_in(Author.__table__, [{"id": 1}]), int)
    assert_type(await engine.create_all(Base.metadata), None)
    assert_type(await engine.drop_all(Base.metadata), None)
