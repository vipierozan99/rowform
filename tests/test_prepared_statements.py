"""rowform's statements are prepared where Core's are and recover as Core's do.

On asyncpg that is SQLAlchemy's per-connection adapter cache, so the dialect's own
settings reach rowform; on psycopg, psycopg's. After a migration a statement
prepared against the old schema fails once, the caches are invalidated as Core
invalidates them, and a read with no transaction open runs again.
`tests/test_migrations.py` holds every shape of that to Core; this module pins the
mechanism and the private API it reads.
"""

from __future__ import annotations

import pytest
import sqlalchemy as sa
from conftest import Author, engine_at, pg_url, seeded
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

#: A table of its own, so a type change leaves the shared fixtures alone.
RETYPED = sa.table("t_retyped", sa.column("id"), sa.column("v"))
READ = sa.select(RETYPED.c.id, RETYPED.c.v).order_by(RETYPED.c.id)


@pytest.fixture(params=["asyncpg", "psycopg"])
def pg_driver(request):
    return request.param


@pytest.fixture
async def retyped(pg_dsn, pg_driver):
    """`(rf.Engine, migrate)`: a warm engine over `t_retyped`, and the migration that
    changes `v` from integer to bigint from another connection.
    """
    url = pg_url(pg_dsn, pg_driver)
    admin = create_async_engine(url)
    async with admin.begin() as conn:
        await conn.exec_driver_sql("DROP TABLE IF EXISTS t_retyped")
        await conn.exec_driver_sql("CREATE TABLE t_retyped (id integer, v integer)")
        await conn.exec_driver_sql("INSERT INTO t_retyped VALUES (1, 10)")

    async def migrate() -> None:
        async with admin.begin() as conn:
            await conn.exec_driver_sql("ALTER TABLE t_retyped ALTER COLUMN v TYPE bigint")

    try:
        async with engine_at(url, pool_size=1, max_overflow=0) as db:
            for _ in range(6):  # past psycopg's prepare_threshold
                assert await db.fetch_all(READ) == [(1, 10)]
            yield db, migrate
    finally:
        async with admin.begin() as conn:
            await conn.exec_driver_sql("DROP TABLE IF EXISTS t_retyped")
        await admin.dispose()


class TestAfterTheSchemaChanged:
    async def test_a_read_outside_a_transaction_runs_again(self, retyped):
        """Two round trips, both inside `around`, which sees the first one fail, and one
        statement for the observer.
        """
        db, migrate = retyped
        await migrate()
        trips, observed = [], []
        db.around = lambda sql: _Counted(trips)
        db.observer = lambda sql, seconds, rows: observed.append(rows)
        assert await db.fetch_all(READ) == [(1, 10)]
        assert observed == [1]
        assert len(trips) == 2 and trips[1] is None
        assert trips[0] is not None and issubclass(trips[0], sa.exc.NotSupportedError)
        assert await db.fetch_all(READ) == [(1, 10)]
        assert trips[2:] == [None]

    async def test_inside_a_transaction_it_raises_once_as_core_does(self, retyped):
        db, migrate = retyped
        await migrate()
        with pytest.raises(sa.exc.NotSupportedError):
            async with db.begin() as conn:
                await conn.fetch_all(READ)
        async with db.begin() as conn:
            assert await conn.fetch_all(READ) == [(1, 10)]


class _Counted:
    """Records, per round trip, the exception class `__exit__` saw (None for an answer)."""

    def __init__(self, trips: list) -> None:
        self.trips = trips

    def __enter__(self) -> None:
        return None

    def __exit__(self, kind: type[BaseException] | None, *exc: object) -> None:
        self.trips.append(kind)


class TestAsyncpgSharesCoresCache:
    """asyncpg: one prepared-statement cache per connection, SQLAlchemy's."""

    async def test_a_statement_core_prepared_is_the_one_rowform_runs(self, pg_dsn):
        async with seeded(pg_url(pg_dsn)) as db, AsyncSession(db.sa_engine) as session:
            statement = sa.select(Author).where(Author.id > sa.bindparam("lo"))
            await session.execute(statement, {"lo": 0})
            adapter = (await (await session.connection()).get_raw_connection()).dbapi_connection
            cache = adapter._prepared_statement_cache
            sql, _ = db.prepare(statement).bind({"lo": 0})
            prepared_by_core = cache.get(sql)[0]
            async with db.connect(bind=session) as conn:
                assert len(await conn.fetch_all(statement, lo=0)) == 4
            assert cache.get(sql)[0] is prepared_by_core

    async def test_the_dialects_name_func_names_rowforms_statements(self, pg_dsn):
        names: list[str] = []

        def name() -> str:
            names.append(f"__rowform_test_{len(names)}__")
            return names[-1]

        async with seeded(pg_url(pg_dsn)):
            pass
        async with engine_at(
            pg_url(pg_dsn),
            connect_args={"prepared_statement_name_func": name, "prepared_statement_cache_size": 0},
        ) as db:
            await db.fetch_all(sa.select(Author))  # the dialect's first-connect queries
            before = len(names)
            for _ in range(3):
                assert len(await db.fetch_all(sa.select(Author))) == 4
            assert len(names) - before == 3, "no cache: every execution prepares, by that name"


class TestPrivateAPI:
    """What the drivers read that their libraries do not promise; each fails here first."""

    async def test_sqlalchemys_asyncpg_adapter(self, pg_dsn):
        """`_prepare(sql, asof)`, its cache entries `(statement, attributes, prepared at)`,
        `dialect._invalidate_schema_cache()` and `_check_type_cache_invalidation`.
        """
        async with seeded(pg_url(pg_dsn)) as db, db.sa_engine.connect() as sa_conn:
            adapter = (await sa_conn.get_raw_connection()).dbapi_connection
            driver, sql = db.driver, "SELECT id FROM t_authors"
            assert driver._cached(adapter, sql) is None
            prepared, attributes = await driver._prepare(adapter, sql)
            assert driver._cached(adapter, sql) == (prepared, attributes)
            assert driver.type_codes(attributes) == [23]  # int4
            before = db.dialect._invalidate_schema_cache_asof
            driver.invalidate(adapter)
            assert db.dialect._invalidate_schema_cache_asof > before
            assert driver._cached(adapter, sql) is None
            await adapter._check_type_cache_invalidation(db.dialect._invalidate_schema_cache_asof)

    async def test_psycopgs_prepare_manager(self, pg_dsn):
        """`_prepared.clear()` forgets every prepared statement and has the next
        statement deallocate them on the server.
        """
        async with seeded(pg_url(pg_dsn, "psycopg")) as db, db.sa_engine.connect() as sa_conn:
            conn = (await sa_conn.get_raw_connection()).driver_connection
            for _ in range(6):
                await conn.execute("SELECT id FROM t_authors")
            count = "SELECT count(*) FROM pg_prepared_statements"
            assert (await (await conn.execute(count)).fetchone())[0] >= 1
            db.driver.invalidate(conn)
            await conn.execute("SELECT 1")
            assert (await (await conn.execute(count)).fetchone())[0] == 0

