"""Rolling deploys: the schema changes under statements a running app already warmed.

Old code keeps serving while an Alembic migration runs, so every statement it
prepared, and every hydrator rowform generated, was built against the schema
before. Each test warms one statement through rowform and, separately, through
Core, runs one migration shape from another connection, then runs the statement
three more times. A fresh Core engine, with nothing cached, says what is true now.

rowform is held to Core: an attempt either returns exactly what is true now or
raises what Core raised on the same attempt, it never returns anything else, and
by the last attempt it has recovered wherever there is something to recover to.
Run directly and through pgbouncer (`conftest.PGBOUNCERS`), since that is how
production meets a migration; session pooling behaves as a direct connection
and is left out.
"""

from __future__ import annotations

import decimal
import enum
from typing import Any

import pytest
import sqlalchemy as sa
from conftest import (
    PGBOUNCERS,
    pg_url,
    pgbouncer_config,
    require_pgbouncer,
    sqlite_url,
)
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import Mapped

import rowform as rf


class Kind(enum.Enum):
    a = "a"
    b = "b"


class MigrationBase(rf.Base):
    metadata = sa.MetaData()


class Item(MigrationBase):
    """The old code's model: every column type a migration in `SHAPES` changes."""

    __tablename__ = "m_items"

    id: Mapped[int] = rf.mapped_column(primary_key=True)
    name: Mapped[str] = rf.mapped_column(sa.String(50))
    qty: Mapped[int]
    price: Mapped[decimal.Decimal] = rf.mapped_column(sa.Numeric(12, 3))
    ratio: Mapped[float]
    kind: Mapped[Kind] = rf.mapped_column(sa.Enum(Kind, name="m_kind"))
    data: Mapped[dict]
    note: Mapped[str | None]


_items = Item.__table__


class ItemSummary(MigrationBase):
    __table__ = rf.view(
        "m_items_v", MigrationBase.metadata, sa.select(_items.c.id, _items.c.qty, _items.c.name)
    )

    id: Mapped[int]
    qty: Mapped[int]
    name: Mapped[str]


ROWS = [
    {"id": 1, "name": "one", "qty": 1, "price": decimal.Decimal("1.500"), "ratio": 0.5,
     "kind": Kind.a, "data": {"x": 1}, "note": "first"},
    {"id": 2, "name": "two", "qty": 2, "price": decimal.Decimal("2.250"), "ratio": 1.5,
     "kind": Kind.b, "data": {"x": 2}, "note": None},
]

#: What the old code runs, by the model it hydrates.
STATEMENTS: dict[str, tuple[Any, Any, dict[str, Any]]] = {
    "items": (Item, sa.select(Item).order_by(Item.id), {}),
    # A bound enum parameter: its type OID is cached on the statement too.
    "by_kind": (Item, sa.select(Item).where(Item.kind == sa.bindparam("k")), {"k": Kind.a}),
    "view": (ItemSummary, sa.select(ItemSummary).order_by(ItemSummary.id), {}),
}

_REBUILD_SQLITE = [
    (
        "CREATE TABLE m_items_new AS SELECT id, name, CAST(qty AS BIGINT) AS qty, price, ratio,"
        " kind, data, note FROM m_items"
    ),
    "DROP TABLE m_items",
    "ALTER TABLE m_items_new RENAME TO m_items",
]

#: `shape: (statement, postgres DDL, sqlite DDL or None)`. sqlite cannot alter a
#: column's type; Alembic's batch mode rebuilds the table instead, as `int_to_bigint` does.
SHAPES: dict[str, tuple[str, list[str], list[str] | None]] = {
    "add_nullable_column": (
        "items", ["ALTER TABLE m_items ADD COLUMN extra integer"], None,
    ),
    "add_column_with_server_default": (
        "items", ["ALTER TABLE m_items ADD COLUMN flag boolean NOT NULL DEFAULT false"],
        ["ALTER TABLE m_items ADD COLUMN flag boolean NOT NULL DEFAULT 0"],
    ),
    "rename_a_selected_column": (
        "items", ["ALTER TABLE m_items RENAME COLUMN note TO memo"], None,
    ),
    "drop_a_selected_column": ("items", ["ALTER TABLE m_items DROP COLUMN note"], None),
    "int_to_bigint": (
        "items", ["ALTER TABLE m_items ALTER COLUMN qty TYPE bigint"], _REBUILD_SQLITE,
    ),
    "varchar_to_text": (
        "items", ["ALTER TABLE m_items ALTER COLUMN name TYPE text"], None,
    ),
    "varchar_longer": (
        "items", ["ALTER TABLE m_items ALTER COLUMN name TYPE varchar(100)"], None,
    ),
    "numeric_to_float": (
        "items", ["ALTER TABLE m_items ALTER COLUMN price TYPE double precision"], None,
    ),
    "float_to_numeric": (
        "items", ["ALTER TABLE m_items ALTER COLUMN ratio TYPE numeric(10, 2)"], None,
    ),
    "json_to_jsonb": ("items", ["ALTER TABLE m_items ALTER COLUMN data TYPE jsonb"], None),
    "enum_add_value": ("by_kind", ["ALTER TYPE m_kind ADD VALUE 'c'"], None),
    "enum_recreated": (
        "by_kind",
        [
            "CREATE TYPE m_kind_new AS ENUM ('a', 'b', 'c')",
            "ALTER TABLE m_items ALTER COLUMN kind TYPE m_kind_new USING kind::text::m_kind_new",
            "DROP TYPE m_kind",
            "ALTER TYPE m_kind_new RENAME TO m_kind",
        ],
        None,
    ),
    # A column a view selects cannot change type under it, and `DropView` has no
    # CASCADE: the migration drops the view, alters, and creates it again.
    "view_dropped_and_recreated": (
        "view",
        [
            "DROP VIEW IF EXISTS m_items_v",
            "ALTER TABLE m_items ALTER COLUMN qty TYPE bigint",
            "CREATE VIEW m_items_v AS SELECT m_items.id, m_items.qty, m_items.name FROM m_items",
        ],
        [
            "DROP VIEW IF EXISTS m_items_v",
            "CREATE VIEW m_items_v AS SELECT m_items.id, m_items.qty, m_items.name FROM m_items",
        ],
    ),
}
for _shape in ("add_nullable_column", "rename_a_selected_column", "drop_a_selected_column"):
    SHAPES[_shape] = (SHAPES[_shape][0], SHAPES[_shape][1], SHAPES[_shape][1])

#: Warm-up runs before the migration: past psycopg's `prepare_threshold` (5).
WARM = 6
#: Runs after it: the first meets the stale cache, the rest show whether it recovered.
AFTER = 3


@pytest.fixture(
    params=[
        "sqlite",
        "asyncpg",
        "psycopg",
        "asyncpg@1.20-transaction",
        "psycopg@1.20-transaction",
        "asyncpg@1.22-transaction",
        "psycopg@1.22-transaction",
    ]
)
async def target(request, sqlite_path):
    """`(dialect name, app URL, app engine options, migration URL)`: the app may go
    through a pooler; the migration connects to the server itself.
    """
    if request.param == "sqlite":
        url = sqlite_url(sqlite_path)
        return "sqlite", url, {}, url
    driver, _, pooler = request.param.partition("@")
    pg_dsn = request.getfixturevalue("pg_dsn")
    direct = pg_url(pg_dsn, driver)
    if not pooler:
        return "postgresql", direct, {}, direct
    dsn = await require_pgbouncer(request, pg_dsn, pooler)
    return "postgresql", pg_url(dsn, driver), pgbouncer_config(driver, pooler), direct


def as_values(rows: list[tuple]) -> list[tuple]:
    """Rows with each value's type alongside: `Decimal('1.5')` and `1.5` are equal."""
    return [tuple((type(v), v) for v in row) for row in rows]


def failure(err: BaseException) -> tuple[type, str]:
    """An error as the class raised and the driver's own class at the bottom of it."""
    root: BaseException = getattr(err, "orig", None) or err
    while root.__cause__ is not None:
        root = root.__cause__
    return type(err), type(root).__name__


async def outcome(run) -> Any:
    try:
        return as_values(await run())
    except Exception as err:  # noqa: BLE001 -- the outcome under test
        return failure(err)


async def drop_schema(admin: Any) -> None:
    async with admin.begin() as conn:
        await conn.exec_driver_sql("DROP VIEW IF EXISTS m_items_v")
        await conn.exec_driver_sql("DROP TABLE IF EXISTS m_items")
        if admin.dialect.name == "postgresql":
            await conn.exec_driver_sql("DROP TYPE IF EXISTS m_kind")
            await conn.exec_driver_sql("DROP TYPE IF EXISTS m_kind_new")


async def clean_up(url: str) -> None:
    """After each test: other modules compare the database's tables with theirs."""
    admin = create_async_engine(url)
    try:
        await drop_schema(admin)
    finally:
        await admin.dispose()


async def build_schema(url: str, shape: str) -> None:
    admin = create_async_engine(url)
    try:
        await drop_schema(admin)
        await rf.Engine(admin).create_all(MigrationBase.metadata)
        async with admin.begin() as conn:
            await conn.execute(sa.insert(_items), ROWS)
            if SHAPES[shape][0] != "view":
                await conn.exec_driver_sql("DROP VIEW m_items_v")
    finally:
        await admin.dispose()


async def migrate(url: str, ddl: list[str]) -> None:
    admin = create_async_engine(url)
    try:
        async with admin.begin() as conn:
            for statement in ddl:
                await conn.exec_driver_sql(statement)
    finally:
        await admin.dispose()


def runner(context: str, engine: Any, statement: Any, params: dict[str, Any], model: Any):
    """One run of the old code's statement in `context`: Core when `engine` is an
    `AsyncEngine`, rowform when it is an `rf.Engine`. Both answer Core's row shape.
    """
    keys = [c.key for c in model.__table__.columns]

    def rows_of(objs: list[Any]) -> list[tuple]:
        return [tuple(getattr(o, k) for k in keys) for o in objs]

    if isinstance(engine, rf.Engine):
        db = engine

        async def run() -> list[tuple]:
            if context == "outside":
                return rows_of(await db.fetch_all(statement, **params))
            if context == "scope":
                async with db.begin() as conn:
                    return rows_of(await conn.fetch_all(statement, **params))
            async with AsyncSession(db.sa_engine) as session, db.connect(bind=session) as conn:
                return rows_of(await conn.fetch_all(statement, **params))

        return run

    async def core() -> list[tuple]:
        if context == "outside":
            async with engine.connect() as conn:
                return [tuple(r) for r in (await conn.execute(statement, params)).all()]
        if context == "scope":
            async with engine.begin() as conn:
                return [tuple(r) for r in (await conn.execute(statement, params)).all()]
        async with AsyncSession(engine) as session:
            return [tuple(r) for r in (await session.execute(statement, params)).all()]

    return core


@pytest.mark.parametrize("context", ["outside", "scope", "session"])
@pytest.mark.parametrize("shape", list(SHAPES))
async def test_old_code_after_a_migration(target, shape, context):
    dialect, url, options, migration_url = target
    which, postgres_ddl, sqlite_ddl = SHAPES[shape]
    ddl = postgres_ddl if dialect == "postgresql" else sqlite_ddl
    if ddl is None:
        pytest.skip(f"sqlite has no {shape.replace('_', ' ')}")
    model, statement, params = STATEMENTS[which]
    await build_schema(migration_url, shape)

    engines = [create_async_engine(url, **options) for _ in range(3)]
    core_engine, our_engine, fresh_engine = engines
    try:
        core = runner(context, core_engine, statement, params, model)
        ours = runner(context, rf.Engine(our_engine), statement, params, model)
        for _ in range(WARM):
            await core()
            await ours()

        await migrate(migration_url, ddl)

        truth = await outcome(runner("outside", fresh_engine, statement, params, model))
        cores = [await outcome(core) for _ in range(AFTER)]
        rowforms = [await outcome(ours) for _ in range(AFTER)]
    finally:
        for engine in engines:
            await engine.dispose()
        await clean_up(migration_url)

    for attempt, (theirs, mine) in enumerate(zip(cores, rowforms, strict=True), 1):
        assert mine == truth or (mine == theirs and isinstance(mine, tuple)) or (
            shares_statements(url) and isinstance(mine, tuple) and mine[1] in STALE
        ), f"attempt {attempt}: rowform {mine!r}; Core {theirs!r}; true now {truth!r}"
    if isinstance(truth, list) and not shares_statements(url):
        assert rowforms[-1] == truth, f"rowform never recovered: {rowforms!r}"


#: What postgres raises for a prepared statement the migration made stale: `cached
#: plan must not change result type`, and `cache lookup failed for type` for an enum.
STALE = {"InvalidCachedStatementError", "FeatureNotSupported", "InternalServerError", "InternalError_"}


def shares_statements(url: str) -> bool:
    """pgbouncer 1.21+ keeps one server-side statement per query text and hands it to
    every client that prepares that text, so which attempt meets a stale one depends
    on the server it lands on, and none recovers until the pooler drops it
    (`TestAPoolerThatSharesStatements`). There rowform is held to no stale values and
    no error but a stale statement's.
    """
    return f":{PGBOUNCERS['1.22-transaction']}/" in url


class TestAPoolerThatSharesStatements:
    """Through pgbouncer 1.21+ a type change leaves a stale server-side statement that
    every client's re-prepare is handed again, so Core never recovers on its own:
    the pooler has to drop its server connections (`RECONNECT`). Each engine here has
    met the stale statement first; one that has not still holds the old row
    description when the pooler re-prepares behind it, and asyncpg decodes the new
    type's bytes with the old codec. That is why `docs/GUIDE.md` says not to change a
    column's type in place behind such a pooler.
    """

    async def test_core_fails_until_the_pooler_reconnects_and_then_both_recover(
        self, request, pg_dsn
    ):
        import asyncpg

        dsn = await require_pgbouncer(request, pg_dsn, "1.22-transaction")
        direct, url = pg_url(pg_dsn), pg_url(dsn)
        await build_schema(direct, "int_to_bigint")
        model, statement, params = STATEMENTS["items"]
        engines = [create_async_engine(url) for _ in range(2)]
        try:
            core = runner("scope", engines[0], statement, params, model)
            ours = runner("scope", rf.Engine(engines[1]), statement, params, model)
            for _ in range(WARM):
                truth = await outcome(core)
                await ours()
            await migrate(direct, SHAPES["int_to_bigint"][1])

            stale = {(sa.exc.NotSupportedError, "InvalidCachedStatementError")}
            assert {await outcome(core) for _ in range(AFTER)} == stale
            assert {await outcome(ours) for _ in range(AFTER)} == stale

            admin = await asyncpg.connect(
                dsn.split("?")[0].replace("/rowform_bench", "/pgbouncer"), statement_cache_size=0
            )
            try:
                await admin.execute("RECONNECT rowform_bench")
            finally:
                await admin.close()
            # The same warm engines: the app is not restarted, only the pooler's servers.
            assert await outcome(core) == await outcome(ours) == truth
        finally:
            for engine in engines:
                await engine.dispose()
            await clean_up(direct)


@pytest.mark.parametrize("driver", ["asyncpg", "psycopg"])
async def test_a_row_holding_an_added_enum_value_raises_as_core_does(pg_dsn, driver):
    """`ADD VALUE` is safe for old code until it reads a row holding the new value."""
    url = pg_url(pg_dsn, driver)
    await build_schema(url, "enum_add_value")
    await migrate(url, ["ALTER TYPE m_kind ADD VALUE 'c'"])
    await migrate(url, ["UPDATE m_items SET kind = 'c' WHERE id = 2"])
    model, statement, params = STATEMENTS["items"]
    engines = [create_async_engine(url) for _ in range(2)]
    try:
        core = await outcome(runner("outside", engines[0], statement, params, model))
        ours = await outcome(runner("outside", rf.Engine(engines[1]), statement, params, model))
    finally:
        for engine in engines:
            await engine.dispose()
        await clean_up(url)
    assert core == ours == (LookupError, "LookupError")
