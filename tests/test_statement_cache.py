"""The compiled-statement cache is bounded, and evicts the least recently used.

`fetch_all` compiles a bare statement once and keeps it under SQLAlchemy's
structural cache key. That key is structural, so an application whose statements
vary in *shape* per request mints a new one every time, and an uncapped dict held
every one of them for the life of the process.

Note what does *not* grow it: `Author.id.in_([...])` compiles to a single
expanding placeholder, so every list length shares one key. The shape that grows
is a varying number of clauses, which is what these tests build.

Eviction is by least-recent *use*, not by insertion: a plain cap would throw out
whatever compiled first, which in a long-lived service is its startup statements.
"""

from __future__ import annotations

import pytest
import sqlalchemy as sa
from conftest import Author, seed, sqlite_db, sqlite_url
from sqlalchemy.ext.asyncio import create_async_engine

import rowform


def varied(n: int) -> sa.Select:
    """A statement whose structure — and so whose cache key — differs per `n`."""
    return sa.select(Author).where(*[Author.id != i for i in range(n)])


@pytest.fixture
async def db(sqlite_path):
    """A freshly-opened engine, so cache counts start from zero and are exact."""
    async with sqlite_db(sqlite_path) as engine:
        await seed(engine)
        engine._queries.clear()  # seeding compiles statements of its own
        yield engine


class TestBounding:
    async def test_it_stops_growing(self, db):
        db._cache_size = 8
        for n in range(1, 40):
            await db.fetch_all(varied(n))
        assert db.cached_statements == 8

    async def test_results_stay_correct_across_eviction(self, db):
        """An evicted statement recompiles; it must not come back wrong."""
        db._cache_size = 2
        expected = [a.id for a in await db.fetch_all(sa.select(Author))]
        for n in range(1, 12):
            await db.fetch_all(varied(n))
        assert [a.id for a in await db.fetch_all(sa.select(Author))] == expected

    async def test_none_means_unbounded(self, sqlite_path):
        async with sqlite_db(sqlite_path, cache_size=None) as engine:
            await seed(engine)
            engine._queries.clear()
            for n in range(1, 25):
                await engine.fetch_all(varied(n))
            assert engine.cached_statements == 24

    async def test_an_expanding_in_shares_one_entry(self, db):
        """Worth pinning: the obvious "dynamic statement" is not one."""
        for n in range(1, 20):
            await db.fetch_all(sa.select(Author).where(Author.id.in_(list(range(n)))))
        assert db.cached_statements == 1

    async def test_a_hoisted_query_is_not_cached_at_all(self, db):
        """`prepare()` hands back the query; nothing is stored, so a caller
        holding their own statements cannot grow this."""
        hoisted = db.prepare(sa.select(Author))
        for _ in range(5):
            await db.fetch_all(hoisted)
        assert db.cached_statements == 0

    def test_an_impossible_size_is_refused(self, sqlite_path):
        for bad in (0, -1):
            with pytest.raises(rowform.ConfigurationError, match="cache_size"):
                rowform.Engine(create_async_engine(sqlite_url(sqlite_path)), cache_size=bad)


class TestEvictionOrder:
    async def test_the_least_recently_used_goes_first(self, db):
        """Use A, then B, then A again, then add C at a cap of two: B is the one
        that has not been used recently, so B goes — not A, which is older but
        hotter."""
        db._cache_size = 2
        a, b, c = varied(1), varied(2), varied(3)

        await db.fetch_all(a)
        await db.fetch_all(b)
        await db.fetch_all(a)  # A is now the most recently used
        await db.fetch_all(c)  # evicts one

        keys = list(db._queries)
        assert a._generate_cache_key().key in keys, "the hot statement was evicted"
        assert b._generate_cache_key().key not in keys
        assert c._generate_cache_key().key in keys

    async def test_a_repeated_statement_reuses_its_entry(self, db):
        for _ in range(10):
            await db.fetch_all(sa.select(Author).where(Author.id > 1))
        assert db.cached_statements == 1


class TestResolveOnce:
    async def test_the_executemany_path_resolves_the_statement_once(self, db, monkeypatch):
        """`execute(stmt, [sets])` used to resolve the statement in `Engine.execute`
        and again in `Connection.execute_many` — the second structural cache-key
        computation `resolved` exists to avoid (F8)."""
        calls = 0
        original = db._query_for

        def counting(statement, params=None):
            nonlocal calls
            calls += 1
            return original(statement, params)

        monkeypatch.setattr(db, "_query_for", counting)
        await db.execute(
            sa.insert(Author.__table__),
            [
                {"id": 8001, "name": "a", "active": True},
                {"id": 8002, "name": "b", "active": True},
            ],
        )
        assert calls == 1


class TestUncacheableStatements:
    """SQLAlchemy declines to cache some constructs, and says so by returning no
    cache key at all. Those must still run.

    `postgresql.insert()` sets `inherit_cache = False`, so *every* ON CONFLICT
    upsert lands here — reading `.key` off the None crashed with an
    `AttributeError` naming neither the statement nor the cause.
    """

    async def test_a_statement_with_no_cache_key_still_runs(self, db):
        class Uncacheable(sa.sql.expression.Select):
            inherit_cache = False

        stmt = sa.select(Author).where(Author.id > 0)
        stmt.__class__ = Uncacheable
        assert stmt._generate_cache_key() is None, "precondition: not cacheable"

        rows = await db.fetch_all(stmt)
        assert rows, "an uncacheable statement must still return its rows"

    async def test_an_uncacheable_statement_is_not_cached(self, db):
        class Uncacheable(sa.sql.expression.Select):
            inherit_cache = False

        stmt = sa.select(Author).where(Author.id > 0)
        stmt.__class__ = Uncacheable

        for _ in range(3):
            await db.fetch_all(stmt)
        assert db.cached_statements == 0, "a keyless statement must not enter the cache"
