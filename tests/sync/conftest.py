"""Shared fixtures.

Two tiers, deliberately:

* Everything that can be tested without a server is tested without one —
  declaration, statement planning, codegen — so the suite runs anywhere.
* Engine and transaction behaviour runs against **both** sqlite and PostgreSQL
  from one parametrised `engine` fixture, because the two differ in exactly the
  place this library is most exposed: sqlite stores temporal types as strings and
  booleans as integers, postgres does not. A test that passes on only one of them
  has not tested the interesting half. PostgreSQL skips with a clear reason when
  unreachable; `--pg-required` turns that skip into a failure.

The schema is created by `engine.create_all(Base.metadata)` rather than by
hand-written `CREATE TABLE` strings. That is the first dividend of letting
SQLAlchemy own the schema, and it is also a test: if the declaration layer built
the wrong table, every fixture below fails.

Async tests use pytest-asyncio in `asyncio_mode = auto` (see pytest.ini), so an
`def test_*` is collected with no decorator. The loop scope is per-function
on purpose: engines hold a pool bound to the loop that opened it, so sharing one
loop across tests would let a closed pool from one test be reached by another.
"""

from __future__ import annotations

import datetime as dt
import decimal
import enum
import os
import re
import socket
import sys
import uuid
from contextlib import contextmanager
from pathlib import Path

import pytest
import sqlalchemy as sa
from sqlalchemy import create_engine
from sqlalchemy.orm import Mapped

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import rowform as rf
import rowform.sync as rfs

PG_DSN = os.environ.get(
    "ROWFORM_TEST_DSN",
    "postgresql://postgres:postgres@127.0.0.1:5432/rowform_bench?sslmode=disable",
)




# --------------------------------------------------------------------------
# Models. Deliberately not the benchmark models: these have a foreign key and a
# nullable column so joins and NULL handling can be exercised, and keeping them
# separate means a change made for a test cannot move a published number.
# --------------------------------------------------------------------------


class Base(rf.Base):
    metadata = sa.MetaData()


class Author(Base):
    __tablename__ = "t_authors"

    id: Mapped[int] = rf.mapped_column(primary_key=True)
    name: Mapped[str]
    active: Mapped[bool]


class Book(Base):
    __tablename__ = "t_books"

    id: Mapped[int] = rf.mapped_column(primary_key=True)
    author_id: Mapped[int] = rf.mapped_column(sa.ForeignKey("t_authors.id"))
    title: Mapped[str]


class Tag(Base):
    __tablename__ = "t_tags"

    id: Mapped[int] = rf.mapped_column(primary_key=True)
    book_id: Mapped[int] = rf.mapped_column(sa.ForeignKey("t_books.id"))
    label: Mapped[str]


class Colour(enum.Enum):
    RED = "red"
    BLUE = "blue"


class Wide(Base):
    """Every type whose driver representation differs from its Python one.

    This is the shape docs/BENCHMARKS.md, lesson 11 asks for: on sqlite, 8 of
    these come back as something other than what they went in as unless the right
    processor runs. `int/str/str/bool` — the old benchmark shape — is the one
    layout where that hazard is invisible.
    """

    __tablename__ = "t_wide"

    id: Mapped[int] = rf.mapped_column(primary_key=True)
    text: Mapped[str]
    flag: Mapped[bool]
    when: Mapped[dt.datetime]
    day: Mapped[dt.date]
    clock: Mapped[dt.time]
    amount: Mapped[decimal.Decimal] = rf.mapped_column(sa.Numeric(12, 3))
    ratio: Mapped[float]
    colour: Mapped[Colour]
    uid: Mapped[uuid.UUID]
    payload: Mapped[dict]
    blob: Mapped[bytes]
    note: Mapped[str | None]


AUTHORS = [
    {"id": 1, "name": "ada", "active": True},
    {"id": 2, "name": "brian", "active": True},
    {"id": 3, "name": "carol", "active": False},
    {"id": 4, "name": "dan", "active": True},  # no books -> exercises the outer join
]
BOOKS = [
    {"id": 10, "author_id": 1, "title": "structures"},
    {"id": 11, "author_id": 1, "title": "algorithms"},
    {"id": 12, "author_id": 2, "title": "compilers"},
    {"id": 13, "author_id": 3, "title": "typography"},
]
TAGS = [
    {"id": 100, "book_id": 10, "label": "classic"},
    {"id": 101, "book_id": 12, "label": "classic"},
]

WIDE_ROW = {
    "id": 1,
    "text": "hello",
    "flag": True,
    "when": dt.datetime(2024, 3, 1, 12, 30, 45, 123456),
    "day": dt.date(2024, 3, 1),
    "clock": dt.time(12, 30, 45),
    "amount": decimal.Decimal("19.990"),
    "ratio": 1.5,
    "colour": Colour.RED,
    "uid": uuid.UUID("12345678-1234-5678-1234-567812345678"),
    "payload": {"a": [1, 2], "b": "x"},
    "blob": b"\x00\x01binary",
    "note": None,
}


def seed(engine):
    """A clean schema and a known set of rows, per test.

    Drop-then-create rather than delete-the-rows: it keeps tests independent of
    each other's DDL (one of them drops everything on purpose), and it exercises
    the `create_all` path on every single test rather than once.
    """
    engine.drop_all(Base.metadata)
    engine.create_all(Base.metadata)
    engine.execute_many(sa.insert(Author.__table__), AUTHORS)
    engine.execute_many(sa.insert(Book.__table__), BOOKS)
    engine.execute_many(sa.insert(Tag.__table__), TAGS)
    engine.execute_many(sa.insert(Wide.__table__), [WIDE_ROW])


# --------------------------------------------------------------------------
# sqlite
# --------------------------------------------------------------------------


@pytest.fixture(scope="session")
def sqlite_path(tmp_path_factory):
    return str(tmp_path_factory.mktemp("rowform") / "test.sqlite3")


# --------------------------------------------------------------------------
# PostgreSQL
# --------------------------------------------------------------------------


def _pg_reachable():
    try:
        with socket.create_connection(("127.0.0.1", 5432), timeout=1):
            return True
    except OSError:
        return False


@pytest.fixture(scope="session")
def pg_dsn(request):
    if not _pg_reachable():
        message = f"PostgreSQL not reachable for {PG_DSN}"
        if request.config.getoption("--pg-required"):
            pytest.fail(message)
        pytest.skip(message)
    return PG_DSN


# --------------------------------------------------------------------------
# The parametrised engine both halves of the suite run against
# --------------------------------------------------------------------------


def sqlite_url(path: str) -> str:
    return f"sqlite+pysqlite:///{path}"


def pg_url(dsn: str, driver: str = "psycopg") -> str:
    """`postgresql://...?sslmode=disable` -> the URL a SQLAlchemy dialect wants.

    The query string goes for asyncpg only: `sslmode` is libpq's spelling, which
    asyncpg's dialect does not accept as a URL parameter. psycopg speaks libpq,
    so it keeps whatever the DSN asked for.
    """
    url = re.sub(r"^postgresql://", f"postgresql+{driver}://", dsn)
    return url.split("?")[0] if driver == "asyncpg" else url


@contextmanager
def engine_at(url: str, **kwargs):
    """An `rfs.Engine` over a SQLAlchemy engine built for one test, disposed after.

    rowform does not own the `AsyncEngine`, so whatever made one is what disposes
    it — the ownership rule, written as a context manager. `kwargs` split: pool
    and connection options go to `create_engine`, rowform's own
    (`observer`, `cache_size`) to `rfs.Engine`.
    """
    ours = {k: kwargs.pop(k) for k in ("observer", "cache_size") if k in kwargs}
    sa_engine = create_engine(url, **kwargs)
    try:
        yield rfs.Engine(sa_engine, **ours)
    finally:
        sa_engine.dispose()


def sqlite_db(path: str, **kwargs):
    """`engine_at` for a sqlite file — the shape most tests want."""
    return engine_at(sqlite_url(path), **kwargs)


@contextmanager
def seeded(url: str):
    with engine_at(url) as db:
        seed(db)
        yield db


@pytest.fixture(params=["sqlite", "psycopg"])
def engine(request):
    """A seeded engine — once per driver.

    Parametrised rather than duplicated so a behaviour asserted here is asserted
    on a driver that decodes types natively *and* on one that does not.

    **All three drivers, not two.** The three disagree about what happens to a
    statement run on the driver connection outside a transaction SQLAlchemy
    opened: sqlite is put in autocommit by `SqliteDriver.configure`, asyncpg has
    no implicit transaction, and psycopg's connection is transactional in its own
    right — so psycopg is the only one where such a write is rolled back on
    release. Running the matrix on the first two alone is how a discarded
    `RETURNING` write survived (`Engine._acquire_for`).
    """
    if request.param == "sqlite":
        url = sqlite_url(request.getfixturevalue("sqlite_path"))
    else:
        url = pg_url(request.getfixturevalue("pg_dsn"), request.param)
    with seeded(url) as db:
        yield db


@pytest.fixture(params=["sqlite"])
def streamable_engine(request):
    """`engine`, minus the driver that cannot stream a write with RETURNING.

    postgres will not `DECLARE` a server-side cursor for one, which is psycopg's
    only streaming primitive (`PsycopgDriver.stream`); asyncpg uses a portal and
    sqlite a plain cursor, so both manage it.
    """
    if request.param == "sqlite":
        url = sqlite_url(request.getfixturevalue("sqlite_path"))
    else:
        url = pg_url(request.getfixturevalue("pg_dsn"), request.param)
    with seeded(url) as db:
        yield db


@pytest.fixture
def sqlite_engine(sqlite_path):
    with seeded(sqlite_url(sqlite_path)) as db:
        yield db


@pytest.fixture
def pg_engine(pg_dsn):
    """A seeded PostgreSQL engine, for the postgres-only surface — COPY, server
    cursors, pipelining. Skips with the rest when no server is reachable."""
    with seeded(pg_url(pg_dsn)) as db:
        yield db
