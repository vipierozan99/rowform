"""rowform behind pgbouncer, held to the engine configuration Core needs there.

Each pooler (`conftest.PGBOUNCERS`) is reset and spread over three server
connections first, and `server_round_robin` then moves every transaction to a
different server, so anything that relies on landing on the server it used last
fails every run rather than now and then. Each path runs `REPEATS` times, past
psycopg's `prepare_threshold`, and Core runs it first on the *same* `AsyncEngine`:
rowform must work wherever Core does, with no setting of its own.
"""

from __future__ import annotations

import uuid

import pytest
import sqlalchemy as sa
from conftest import (
    AUTHORS,
    Author,
    Wide,
    engine_at,
    pg_url,
    pgbouncer_config,
    seeded,
)
from sqlalchemy.ext.asyncio import AsyncSession

#: Past psycopg's default `prepare_threshold` (5), so its prepared path runs too.
REPEATS = 8

WIDE = sa.select(Wide).order_by(Wide.id)
AUTHORS_ABOVE = sa.select(Author).where(Author.id > sa.bindparam("lo")).order_by(Author.id)


def as_rows(objs, model) -> list[tuple]:
    """rowform's hydrated objects as Core's rows, column for column."""
    return [tuple(getattr(o, c.key) for c in model.__table__.columns) for o in objs]


def new_authors(n: int = 3) -> list[dict]:
    return [
        {"id": 1000 + uuid.uuid4().int % 10**9, "name": "pooled", "active": True}
        for _ in range(n)
    ]


@pytest.fixture(params=["asyncpg", "psycopg"])
async def db(request, pg_dsn, pgbouncer):
    """`rf.Engine` over one `AsyncEngine` through `pgbouncer`, configured as Core needs."""
    pooler, dsn = pgbouncer
    async with seeded(pg_url(pg_dsn, request.param)):
        pass
    async with engine_at(pg_url(dsn, request.param), **pgbouncer_config(request.param, pooler)) as engine:
        yield engine


async def core_rows(db, statement, **params) -> list[tuple]:
    async with db.sa_engine.connect() as conn:
        return [tuple(row) for row in (await conn.execute(statement, params)).all()]


class TestReads:
    async def test_a_one_shot_read(self, db):
        """`select(Wide)` carries an enum, so asyncpg introspects a type mid-statement."""
        expected = await core_rows(db, WIDE)
        for _ in range(REPEATS):
            assert as_rows(await db.fetch_all(WIDE), Wide) == expected

    async def test_the_same_read_in_a_scope(self, db):
        expected = await core_rows(db, AUTHORS_ABOVE, lo=1)
        for _ in range(REPEATS):
            async with db.connect() as conn:
                assert as_rows(await conn.fetch_all(AUTHORS_ABOVE, lo=1), Author) == expected

    async def test_execute_of_a_select(self, db):
        expected = await core_rows(db, AUTHORS_ABOVE, lo=1)
        for _ in range(REPEATS):
            result = await db.execute(AUTHORS_ABOVE, {"lo": 1})
            assert as_rows(result.scalars().all(), Author) == expected

    async def test_a_statement_prepared_fresh_each_time(self, db):
        """`prepare()` hands back an unhydrated `CoreQuery`, so each one describes its result."""
        expected = await core_rows(db, AUTHORS_ABOVE, lo=0)
        for _ in range(REPEATS):
            assert as_rows(await db.fetch_all(db.prepare(AUTHORS_ABOVE), lo=0), Author) == expected

    async def test_past_the_statement_caches(self, db):
        """More distinct statements than asyncpg's, SQLAlchemy's or psycopg's cache holds
        (100 each), so evictions close statements on whichever server is current.
        """
        for limit in range(1, 111):
            statement = sa.select(Author.id).order_by(Author.id).limit(limit)
            assert await db.fetch_all(statement) == [a["id"] for a in AUTHORS][:limit]
        for _ in range(REPEATS):
            assert len(await db.fetch_all(sa.select(Author.id).limit(1))) == 1

    async def test_a_stream(self, db):
        async with db.sa_engine.connect() as conn:
            expected = [tuple(r) for r in await (await conn.stream(AUTHORS_ABOVE, {"lo": 0})).all()]
        for _ in range(REPEATS):
            streamed = [a async for a in db.fetch_iter(AUTHORS_ABOVE, chunk=2, lo=0)]
            assert as_rows(streamed, Author) == expected


class TestWrites:
    async def test_execute(self, db):
        insert = sa.insert(Author.__table__)
        async with db.sa_engine.begin() as conn:
            await conn.execute(insert, new_authors(1)[0])
        for _ in range(REPEATS):
            row = new_authors(1)[0]
            await db.execute(insert, row)
            assert await db.fetch_one(sa.select(Author.name).where(Author.id == row["id"])) == "pooled"

    async def test_execute_many(self, db):
        insert = sa.insert(Author.__table__)
        async with db.sa_engine.begin() as conn:
            await conn.execute(insert, new_authors())
        for _ in range(REPEATS):
            rows = new_authors()
            await db.execute_many(insert, rows)
            ids = [r["id"] for r in rows]
            assert len(await db.fetch_all(sa.select(Author.id).where(Author.id.in_(ids)))) == 3

    async def test_copy_in(self, db):
        """Core has no COPY; asyncpg introspects the table first, inside the scope's transaction."""
        for _ in range(REPEATS):
            rows = new_authors()
            assert await db.copy_in(Author.__table__, rows) == 3
            ids = [r["id"] for r in rows]
            assert len(await db.fetch_all(sa.select(Author.id).where(Author.id.in_(ids)))) == 3

    async def test_a_scope_that_reads_its_own_write(self, db):
        insert = sa.insert(Author.__table__)
        for _ in range(REPEATS):
            row = new_authors(1)[0]
            async with db.begin() as conn:
                await conn.execute(insert, row)
                assert await conn.fetch_one(
                    sa.select(Author.name).where(Author.id == row["id"])
                ) == "pooled"


class TestASecondWorkerProcess:
    """Production runs several processes through one pgbouncer, and asyncpg numbers its
    statements per process: a second worker starts from `__asyncpg_stmt_1__` again,
    on servers where the first worker's statements of that name may still exist.
    A new pool and a reset counter before each round stand in for that worker.
    """

    @pytest.fixture(autouse=True)
    def asyncpg_only(self, db):
        if db.dialect.driver != "asyncpg":
            pytest.skip("psycopg names statements per connection, not per process")

    @pytest.fixture
    def fresh_worker(self, db, monkeypatch):
        import asyncpg.connection

        async def start() -> None:
            await db.sa_engine.dispose()
            monkeypatch.setattr(asyncpg.connection, "_uid", 0)

        return start

    async def test_reads(self, db, fresh_worker):
        expected = await core_rows(db, WIDE)
        for _ in range(REPEATS):
            await fresh_worker()
            assert as_rows(await db.fetch_all(WIDE), Wide) == expected
            await fresh_worker()
            assert as_rows(await db.fetch_all(db.prepare(WIDE)), Wide) == expected

    async def test_streams_and_sessions(self, db, fresh_worker):
        for _ in range(REPEATS):
            await fresh_worker()
            assert len([a async for a in db.fetch_iter(AUTHORS_ABOVE, lo=0)]) >= 4
            await fresh_worker()
            async with AsyncSession(db.sa_engine) as session, db.connect(bind=session) as conn:
                assert len(await conn.fetch_all(AUTHORS_ABOVE, lo=0)) >= 4


class TestSQLAlchemysOwnSettings:
    """`prepared_statement_name_func` and `prepared_statement_cache_size=0` without
    asyncpg's `statement_cache_size=0`: Core's statements are prepared afresh under a
    unique name in every transaction, so its reads work through a pgbouncer that
    cannot carry them over. rowform's statements are SQLAlchemy's, so they must too.
    (Core's own `executemany` and codec setup go through asyncpg's cache and do not,
    which is why `pgbouncer_config` sets both.)
    """

    @pytest.fixture
    async def db(self, pg_dsn, pgbouncer):
        pooler, dsn = pgbouncer
        if pooler != "1.20-transaction":
            pytest.skip("only a pooler without prepared-statement support needs these")
        async with seeded(pg_url(pg_dsn)):
            pass
        names = {"prepared_statement_name_func": lambda: f"__asyncpg_{uuid.uuid4()}__"}
        async with engine_at(
            pg_url(dsn), connect_args={**names, "prepared_statement_cache_size": 0}
        ) as engine:
            yield engine

    async def test_reads(self, db):
        expected = await core_rows(db, WIDE)
        for _ in range(REPEATS):
            assert as_rows(await db.fetch_all(WIDE), Wide) == expected
            assert await core_rows(db, WIDE) == expected

    async def test_scopes_streams_and_sessions(self, db):
        for _ in range(REPEATS):
            async with db.begin() as conn:
                assert len(await conn.fetch_all(AUTHORS_ABOVE, lo=0)) >= 4
            assert len([a async for a in db.fetch_iter(AUTHORS_ABOVE, lo=0)]) >= 4
            async with AsyncSession(db.sa_engine) as session, db.connect(bind=session) as conn:
                assert len(await conn.fetch_all(AUTHORS_ABOVE, lo=0)) >= 4


class TestBoundToASession:
    async def test_reads_inside_the_sessions_transaction(self, db):
        insert = sa.insert(Author.__table__)
        for _ in range(REPEATS):
            row = new_authors(1)[0]
            async with AsyncSession(db.sa_engine) as session:
                await session.execute(insert, row)
                core = (await session.execute(AUTHORS_ABOVE, {"lo": 0})).all()
                async with db.connect(bind=session) as conn:
                    ours = await conn.fetch_all(AUTHORS_ABOVE, lo=0)
                    again = await conn.fetch_all(AUTHORS_ABOVE, lo=0)
                assert as_rows(ours, Author) == as_rows(again, Author) == [tuple(r) for r in core]
                assert row["id"] in [a.id for a in ours]
