"""`fetch_iter`: the same rows as `fetch_all`, a chunk at a time.

Run against both backends, because the three drivers stream through three
different primitives — `fetchmany` on a sqlite cursor, a portal on asyncpg, a
`DECLARE`d cursor on psycopg — and the interesting failures are per-driver:
asyncpg refuses to open a portal outside a transaction (so the engine opens one),
and postgres refuses to `DECLARE` a cursor for a write with RETURNING (so
psycopg says so rather than passing on a syntax error).

`fetch_all` is the oracle throughout: a stream that does not agree with it,
row for row and type for type, is broken however elegantly it chunks.
"""

from __future__ import annotations

import pytest
import sqlalchemy as sa
from conftest import Author, Book, Wide, engine_at, pg_url, seed, sqlite_db

import rowform


async def collect(iterator):
    return [row async for row in iterator]


@pytest.fixture
async def seeded_sqlite(sqlite_path):
    """Schema and rows at `sqlite_path`, for tests opening their own engine."""
    async with sqlite_db(sqlite_path) as db:
        await seed(db)


class TestAgreesWithFetchAll:
    async def test_models(self, engine):
        statement = sa.select(Author).order_by(Author.id)
        streamed = await collect(engine.fetch_iter(statement, chunk=2))
        assert [a.name for a in streamed] == [
            a.name for a in await engine.fetch_all(statement)
        ]
        assert all(isinstance(a, Author) for a in streamed)

    async def test_scalars(self, engine):
        statement = sa.select(Author.name).order_by(Author.name)
        assert await collect(engine.fetch_iter(statement, chunk=1)) == (
            await engine.fetch_all(statement)
        )

    async def test_tuples_from_a_join(self, engine):
        statement = (
            sa.select(Author, Book).join(Book, Book.author_id == Author.id).order_by(Book.id)
        )
        streamed = await collect(engine.fetch_iter(statement, chunk=2))
        expected = await engine.fetch_all(statement)
        assert len(streamed) == len(expected)
        for (author, book), (exp_author, exp_book) in zip(streamed, expected, strict=True):
            assert isinstance(author, Author)
            assert (author.id, book.title) == (exp_author.id, exp_book.title)

    async def test_type_processors_run_on_every_chunk(self, engine):
        """The chunk boundary is where a hydrator built from the first chunk's
        description could quietly stop converting."""
        statement = sa.select(Wide).order_by(Wide.id)
        streamed = await collect(engine.fetch_iter(statement, chunk=1))
        expected = await engine.fetch_all(statement)
        assert len(streamed) == len(expected)
        for got, want in zip(streamed, expected, strict=True):
            assert got.when == want.when
            assert type(got.when) is type(want.when)
            assert got.amount == want.amount
            assert type(got.amount) is type(want.amount)
            assert got.colour == want.colour

    async def test_an_empty_result_yields_nothing(self, engine):
        statement = sa.select(Author).where(Author.name == "nobody")
        assert await collect(engine.fetch_iter(statement)) == []

    async def test_bind_parameters(self, engine):
        hoisted = engine.prepare(
            sa.select(Author).where(Author.id > sa.bindparam("floor")).order_by(Author.id)
        )
        streamed = await collect(engine.fetch_iter(hoisted, chunk=1, floor=1))
        assert [a.id for a in streamed] == [
            a.id for a in await engine.fetch_all(hoisted, floor=1)
        ]


class TestAliases:
    """`alias()` and `fetch_iter` landed independently; this is where they meet.

    A stream builds its hydrator from the *driver's* description of a cursor
    result, so an alias or CTE resolving to a model has to survive that path as
    well as the `fetch_all` one.
    """

    async def test_a_self_join_through_an_alias_streams_as_models(self, engine):
        mgr = rowform.alias(Author, "mgr")
        statement = (
            sa.select(Author, mgr).join(mgr, mgr.id == Author.id).order_by(Author.id)
        )
        streamed = await collect(engine.fetch_iter(statement, chunk=2))
        expected = await engine.fetch_all(statement)
        assert len(streamed) == len(expected)
        for author, aliased in streamed:
            assert isinstance(author, Author)
            assert isinstance(aliased, Author)

    async def test_a_cte_marked_with_of_streams_as_models(self, engine):
        active = rowform.alias(
            Author, of=sa.select(Author).where(Author.active).cte("act_stream")
        )
        statement = sa.select(active).order_by(active.id)
        streamed = await collect(engine.fetch_iter(statement, chunk=1))
        assert streamed
        assert all(isinstance(a, Author) for a in streamed)
        assert [a.id for a in streamed] == [a.id for a in await engine.fetch_all(statement)]


class TestChunking:
    @pytest.mark.parametrize("chunk", [1, 2, 3, 1000])
    async def test_the_result_is_the_same_at_any_chunk_size(self, engine, chunk):
        statement = sa.select(Author.id).order_by(Author.id)
        assert await collect(engine.fetch_iter(statement, chunk=chunk)) == (
            await engine.fetch_all(statement)
        )

    async def test_it_really_arrives_in_chunks(self, engine, monkeypatch):
        """Otherwise this is `fetch_all` with extra steps. One yield of the driver
        hook is one server fetch, so N rows at chunk=1 must be N yields."""
        original = type(engine.driver).stream
        yields = 0

        async def counting(self, conn, sql, params, chunk, query):
            nonlocal yields
            async for item in original(self, conn, sql, params, chunk, query):
                yields += 1
                yield item

        monkeypatch.setattr(type(engine.driver), "stream", counting)
        rows = await collect(engine.fetch_iter(sa.select(Author.id), chunk=1))
        assert len(rows) > 1
        assert yields == len(rows)

        yields = 0
        rows = await collect(engine.fetch_iter(sa.select(Author.id), chunk=1000))
        assert yields == 1

    @pytest.mark.parametrize("chunk", [0, -1])
    async def test_an_impossible_chunk_is_refused(self, engine, chunk):
        with pytest.raises(rowform.ConfigurationError, match="chunk must be at least 1"):
            await collect(engine.fetch_iter(sa.select(Author), chunk=chunk))


class TestConnectionHandling:
    async def test_abandoning_the_loop_leaves_the_engine_usable(self, engine):
        """A consumer that breaks out must return its connection and close its
        cursor — on asyncpg the portal's transaction has to unwind too."""
        async for _ in engine.fetch_iter(sa.select(Author), chunk=1):
            break
        assert await engine.fetch_all(sa.select(Author))

    async def test_closing_the_iterator_closes_the_driver_cursor_first(self, engine, monkeypatch):
        """`async for` does not close an inner generator, so without this the
        cursor lingered until GC, after its connection was released: on sqlite it
        kept the file's lock past the scope."""
        driver_stream = engine.driver.stream
        closed = []

        async def spy(*args):
            try:
                async for batch in driver_stream(*args):
                    yield batch
            finally:
                closed.append(True)

        monkeypatch.setattr(engine.driver, "stream", spy)
        async with engine.connect() as conn:
            rows = conn.fetch_iter(sa.select(Author), chunk=1)
            async for _ in rows:
                break
            await rows.aclose()
            assert closed == [True]

    async def test_repeated_streams_do_not_exhaust_the_pool(self, sqlite_path, seeded_sqlite):
        async with sqlite_db(sqlite_path, pool_size=1, max_overflow=1) as db:
            for _ in range(6):
                async for _row in db.fetch_iter(sa.select(Author), chunk=1):
                    break
            assert await db.fetch_all(sa.select(Author))

    async def test_it_is_refused_on_the_engine_inside_a_transaction(self, engine):
        """Same reason `fetch_all` is: it would take a different pooled connection
        and miss the transaction's uncommitted writes."""
        async with engine.begin():
            with pytest.raises(rowform.EngineStateError, match="fetch_iter"):
                await collect(engine.fetch_iter(sa.select(Author)))

    async def test_streaming_inside_a_transaction_sees_its_writes(self, engine):
        async with engine.begin() as conn:
            await conn.execute(
                sa.insert(Author.__table__).values(id=8001, name="uncommitted", active=True)
            )
            names = [a.name async for a in conn.fetch_iter(sa.select(Author), chunk=2)]
        assert "uncommitted" in names

    async def test_a_savepoint_can_stream_too(self, engine):
        async with engine.begin() as conn, conn.begin_nested():
            rows = await collect(conn.fetch_iter(sa.select(Author), chunk=2))
        assert rows


class TestStatementsItRefuses:
    async def test_a_statement_that_returns_no_rows(self, engine):
        statement = sa.insert(Author.__table__).values(id=8100, name="ada", active=True)
        with pytest.raises(rowform.StatementError, match="produces no rows"):
            await collect(engine.fetch_iter(statement))

    async def test_returning_streams_on_sqlite_and_asyncpg(self, streamable_engine):
        """sqlite has no restriction, and asyncpg opens a portal over the write —
        both stream what the psycopg driver cannot. psycopg's half of this is
        `test_psycopg_refuses_returning_with_a_reason` below."""
        statement = (
            sa.insert(Author.__table__)
            .values(id=8200, name="barbara", active=True)
            .returning(Author.__table__)
        )
        streamed = await collect(streamable_engine.fetch_iter(statement, chunk=1))
        assert [a.name for a in streamed] == ["barbara"]

    async def test_psycopg_refuses_returning_with_a_reason(self, pg_dsn):
        """Postgres cannot DECLARE a cursor for a write, so this fails as an
        `UnsupportedError` naming the alternative rather than as a syntax error
        from the server."""
        async with engine_at(pg_url(pg_dsn, "psycopg")) as db:
            await seed(db)
            statement = (
                sa.insert(Author.__table__)
                .values(id=8300, name="edsger", active=True)
                .returning(Author.__table__)
            )
            with pytest.raises(rowform.UnsupportedError, match="only DECLARE one for a SELECT"):
                await collect(db.fetch_iter(statement))

    async def test_psycopg_streams_nested_on_one_connection(self, pg_dsn):
        """Two live streams on the same pinned connection.

        Cursor names are per session, so a fixed one made the second stream fail
        with `DuplicateCursor: cursor "rowform_stream" already exists`. Only the
        psycopg driver declares a named cursor, so only it can hit this.
        """
        async with engine_at(pg_url(pg_dsn, "psycopg")) as db:
            await seed(db)
            async with db.begin() as conn:
                outer = conn.fetch_iter(sa.select(Author).order_by(Author.id), chunk=1)
                try:
                    async for _first in outer:
                        inner = await collect(conn.fetch_iter(sa.select(Author), chunk=1))
                        assert inner
                        break
                finally:
                    await outer.aclose()

    async def test_psycopg_streams_a_select(self, pg_dsn):
        async with engine_at(pg_url(pg_dsn, "psycopg")) as db:
            await seed(db)
            statement = sa.select(Author).order_by(Author.id)
            streamed = await collect(db.fetch_iter(statement, chunk=2))
            assert [a.name for a in streamed] == [
                a.name for a in await db.fetch_all(statement)
            ]


class TestObserver:
    async def test_a_stream_reports_once_with_the_total(self, engine):
        seen: list[tuple[str, float, int | None]] = []
        engine.observer = lambda *call: seen.append(call)
        rows = await collect(engine.fetch_iter(sa.select(Author), chunk=1))
        assert len(seen) == 1
        assert seen[0][2] == len(rows)
