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
`async def test_*` is collected with no decorator. The loop scope is per-function
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
from contextlib import asynccontextmanager
from pathlib import Path

import pytest
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm import Mapped
from sqlalchemy.pool import NullPool

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import rowform as rf

PG_DSN = os.environ.get(
    "ROWFORM_TEST_DSN",
    "postgresql://postgres:postgres@127.0.0.1:5432/rowform_bench?sslmode=disable",
)


def pytest_addoption(parser):
    parser.addoption(
        "--pg-required",
        action="store_true",
        help="fail instead of skipping when PostgreSQL is unreachable",
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


async def seed(engine):
    """A clean schema and a known set of rows, per test.

    Drop-then-create rather than delete-the-rows: it keeps tests independent of
    each other's DDL (one of them drops everything on purpose), and it exercises
    the `create_all` path on every single test rather than once.
    """
    await engine.drop_all(Base.metadata)
    await engine.create_all(Base.metadata)
    await engine.execute_many(sa.insert(Author.__table__), AUTHORS)
    await engine.execute_many(sa.insert(Book.__table__), BOOKS)
    await engine.execute_many(sa.insert(Tag.__table__), TAGS)
    await engine.execute_many(sa.insert(Wide.__table__), [WIDE_ROW])


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
    return f"sqlite+aiosqlite:///{path}"


def pg_url(dsn: str, driver: str = "asyncpg") -> str:
    """`postgresql://...?sslmode=disable` -> the URL a SQLAlchemy dialect wants.

    The query string goes for asyncpg only: `sslmode` is libpq's spelling, which
    asyncpg's dialect does not accept as a URL parameter. psycopg speaks libpq,
    so it keeps whatever the DSN asked for.
    """
    url = re.sub(r"^postgresql://", f"postgresql+{driver}://", dsn)
    return url.split("?")[0] if driver == "asyncpg" else url


@asynccontextmanager
async def engine_at(url: str, **kwargs):
    """An `rf.Engine` over a SQLAlchemy engine built for one test, disposed after.

    rowform does not own the `AsyncEngine`, so whatever made one is what disposes
    it — the ownership rule, written as a context manager. `kwargs` split: pool
    and connection options go to `create_async_engine`, rowform's own
    (`observer`, `cache_size`) to `rf.Engine`.
    """
    ours = {k: kwargs.pop(k) for k in ("observer", "around", "cache_size") if k in kwargs}
    sa_engine = create_async_engine(url, **kwargs)
    try:
        yield rf.Engine(sa_engine, **ours)
    finally:
        await sa_engine.dispose()


def sqlite_db(path: str, **kwargs):
    """`engine_at` for a sqlite file — the shape most tests want."""
    return engine_at(sqlite_url(path), **kwargs)


@asynccontextmanager
async def seeded(url: str):
    async with engine_at(url) as db:
        await seed(db)
        yield db


@pytest.fixture(params=["sqlite", "asyncpg", "psycopg"])
async def engine(request):
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
    async with seeded(url) as db:
        yield db


@pytest.fixture(params=["sqlite", "asyncpg"])
async def streamable_engine(request):
    """`engine`, minus the driver that cannot stream a write with RETURNING.

    postgres will not `DECLARE` a server-side cursor for one, which is psycopg's
    only streaming primitive (`PsycopgDriver.stream`); asyncpg uses a portal and
    sqlite a plain cursor, so both manage it.
    """
    if request.param == "sqlite":
        url = sqlite_url(request.getfixturevalue("sqlite_path"))
    else:
        url = pg_url(request.getfixturevalue("pg_dsn"), request.param)
    async with seeded(url) as db:
        yield db


@pytest.fixture
async def sqlite_engine(sqlite_path):
    async with seeded(sqlite_url(sqlite_path)) as db:
        yield db


@pytest.fixture
async def pg_engine(pg_dsn):
    """A seeded PostgreSQL engine, for the postgres-only surface — COPY, server
    cursors, pipelining. Skips with the rest when no server is reachable."""
    async with seeded(pg_url(pg_dsn)) as db:
        yield db


# --------------------------------------------------------------------------
# pgbouncer, in front of the same server (scripts/pgbouncer.sh, CI services)
# --------------------------------------------------------------------------

#: Port of each pooler; 1.21 is the first pgbouncer with `max_prepared_statements`.
PGBOUNCERS = {
    "1.20-transaction": 6431,
    "1.22-transaction": 6432,
    "1.22-session": 6433,
}


def pgbouncer_dsn(dsn: str, pooler: str) -> str:
    return re.sub(r":\d+/", f":{PGBOUNCERS[pooler]}/", dsn, count=1)


def pgbouncer_config(driver: str, pooler: str) -> dict:
    """The engine configuration SQLAlchemy and the driver document for `pooler`, and
    nothing rowform-specific: the suite holds rowform to the configuration Core needs.

    Before 1.21 transaction pooling cannot carry a prepared statement from one
    transaction to the next. asyncpg: SQLAlchemy's recipe (`NullPool`, unique
    `prepared_statement_name_func`) plus asyncpg's own `statement_cache_size=0`,
    without which Core's `executemany` and codec setup still use cached named
    statements. psycopg: `prepare_threshold=None`, also wanted on 1.22 unless libpq
    is 17+, which psycopg needs to close statements in a way pgbouncer tracks.
    """
    if pooler == "1.20-transaction":
        if driver == "asyncpg":
            return {
                "poolclass": NullPool,
                "connect_args": {
                    "prepared_statement_name_func": lambda: f"__asyncpg_{uuid.uuid4()}__",
                    "statement_cache_size": 0,
                },
            }
        return {"connect_args": {"prepare_threshold": None}}
    if pooler == "1.22-transaction" and driver == "psycopg":
        import psycopg

        if psycopg.pq.version() < 170000:
            return {"connect_args": {"prepare_threshold": None}}
    return {}


async def reset_pgbouncer(dsn: str, servers: int = 3) -> None:
    """Drop every server connection (and the prepared statements on it), then open
    `servers` at once so the pool has that many: with `server_round_robin` each
    transaction after this lands on a different server than the one before.
    """
    import asyncpg

    plain = dsn.split("?")[0]
    admin = await asyncpg.connect(re.sub(r"/[^/]*$", "/pgbouncer", plain), statement_cache_size=0)
    try:
        await admin.execute("KILL rowform_bench")
        await admin.execute("RESUME rowform_bench")
    finally:
        await admin.close()
    clients = [await asyncpg.connect(plain, statement_cache_size=0) for _ in range(servers)]
    try:
        for client in clients:
            await client.execute("BEGIN; SELECT 1")
        for client in clients:
            await client.execute("ROLLBACK")
    finally:
        for client in clients:
            await client.close()


async def require_pgbouncer(request, pg_dsn: str, pooler: str) -> str:
    """`pooler`'s DSN, reset and spread; skips like `pg_dsn` when it is not running."""
    try:
        with socket.create_connection(("127.0.0.1", PGBOUNCERS[pooler]), timeout=1):
            pass
    except OSError:
        message = f"pgbouncer {pooler} not reachable; scripts/pgbouncer.sh up"
        if request.config.getoption("--pg-required"):
            pytest.fail(message)
        pytest.skip(message)
    dsn = pgbouncer_dsn(pg_dsn, pooler)
    await reset_pgbouncer(dsn)
    return dsn


@pytest.fixture(params=list(PGBOUNCERS))
async def pgbouncer(request, pg_dsn):
    """`(pooler, dsn)` for each pgbouncer, reset and spread."""
    return request.param, await require_pgbouncer(request, pg_dsn, request.param)
