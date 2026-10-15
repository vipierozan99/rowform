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
from sync.conftest import Author, Book, Wide, engine_at, pg_url, seed, sqlite_db

import rowform


def collect(iterator):
    return [row for row in iterator]


@pytest.fixture
def seeded_sqlite(sqlite_path):
    """Schema and rows at `sqlite_path`, for tests opening their own engine."""
    with sqlite_db(sqlite_path) as db:
        seed(db)


class TestAgreesWithFetchAll:
    def test_models(self, engine):
        statement = sa.select(Author).order_by(Author.id)
        streamed = collect(engine.fetch_iter(statement, chunk=2))
        assert [a.name for a in streamed] == [
            a.name for a in engine.fetch_all(statement)
        ]
        assert all(isinstance(a, Author) for a in streamed)

    def test_scalars(self, engine):
        statement = sa.select(Author.name).order_by(Author.name)
        assert collect(engine.fetch_iter(statement, chunk=1)) == (
            engine.fetch_all(statement)
        )

    def test_tuples_from_a_join(self, engine):
        statement = (
            sa.select(Author, Book).join(Book, Book.author_id == Author.id).order_by(Book.id)
        )
        streamed = collect(engine.fetch_iter(statement, chunk=2))
        expected = engine.fetch_all(statement)
        assert len(streamed) == len(expected)
        for (author, book), (exp_author, exp_book) in zip(streamed, expected, strict=True):
            assert isinstance(author, Author)
            assert (author.id, book.title) == (exp_author.id, exp_book.title)

    def test_type_processors_run_on_every_chunk(self, engine):
        """The chunk boundary is where a hydrator built from the first chunk's
        description could quietly stop converting."""
        statement = sa.select(Wide).order_by(Wide.id)
        streamed = collect(engine.fetch_iter(statement, chunk=1))
        expected = engine.fetch_all(statement)
        assert len(streamed) == len(expected)
        for got, want in zip(streamed, expected, strict=True):
            assert got.when == want.when
            assert type(got.when) is type(want.when)
            assert got.amount == want.amount
            assert type(got.amount) is type(want.amount)
            assert got.colour == want.colour

    def test_an_empty_result_yields_nothing(self, engine):
        statement = sa.select(Author).where(Author.name == "nobody")
        assert collect(engine.fetch_iter(statement)) == []

    def test_bind_parameters(self, engine):
        hoisted = engine.prepare(
            sa.select(Author).where(Author.id > sa.bindparam("floor")).order_by(Author.id)
        )
        streamed = collect(engine.fetch_iter(hoisted, chunk=1, floor=1))
        assert [a.id for a in streamed] == [
            a.id for a in engine.fetch_all(hoisted, floor=1)
        ]


class TestAliases:
    """`alias()` and `fetch_iter` landed independently; this is where they meet.

    A stream builds its hydrator from the *driver's* description of a cursor
    result, so an alias or CTE resolving to a model has to survive that path as
    well as the `fetch_all` one.
    """

    def test_a_self_join_through_an_alias_streams_as_models(self, engine):
        mgr = rowform.alias(Author, "mgr")
        statement = (
            sa.select(Author, mgr).join(mgr, mgr.id == Author.id).order_by(Author.id)
        )
        streamed = collect(engine.fetch_iter(statement, chunk=2))
        expected = engine.fetch_all(statement)
        assert len(streamed) == len(expected)
        for author, aliased in streamed:
            assert isinstance(author, Author)
            assert isinstance(aliased, Author)

    def test_a_cte_marked_with_of_streams_as_models(self, engine):
        active = rowform.alias(
            Author, of=sa.select(Author).where(Author.active).cte("act_stream")
        )
        statement = sa.select(active).order_by(active.id)
        streamed = collect(engine.fetch_iter(statement, chunk=1))
        assert streamed
        assert all(isinstance(a, Author) for a in streamed)
        assert [a.id for a in streamed] == [a.id for a in engine.fetch_all(statement)]


class TestChunking:
    @pytest.mark.parametrize("chunk", [1, 2, 3, 1000])
    def test_the_result_is_the_same_at_any_chunk_size(self, engine, chunk):
        statement = sa.select(Author.id).order_by(Author.id)
        assert collect(engine.fetch_iter(statement, chunk=chunk)) == (
            engine.fetch_all(statement)
        )

    def test_it_really_arrives_in_chunks(self, engine, monkeypatch):
        """Otherwise this is `fetch_all` with extra steps. One yield of the driver
        hook is one server fetch, so N rows at chunk=1 must be N yields."""
        original = type(engine.driver).stream
        yields = 0

        def counting(self, conn, sql, params, chunk, query):
            nonlocal yields
            for item in original(self, conn, sql, params, chunk, query):
                yields += 1
                yield item

        monkeypatch.setattr(type(engine.driver), "stream", counting)
        rows = collect(engine.fetch_iter(sa.select(Author.id), chunk=1))
        assert len(rows) > 1
        assert yields == len(rows)

        yields = 0
        rows = collect(engine.fetch_iter(sa.select(Author.id), chunk=1000))
        assert yields == 1

    @pytest.mark.parametrize("chunk", [0, -1])
    def test_an_impossible_chunk_is_refused(self, engine, chunk):
        with pytest.raises(rowform.ConfigurationError, match="chunk must be at least 1"):
            collect(engine.fetch_iter(sa.select(Author), chunk=chunk))


class TestConnectionHandling:
    def test_abandoning_the_loop_leaves_the_engine_usable(self, engine):
        """A consumer that breaks out must return its connection and close its
        cursor — on asyncpg the portal's transaction has to unwind too."""
        for _ in engine.fetch_iter(sa.select(Author), chunk=1):
            break
        assert engine.fetch_all(sa.select(Author))

    def test_repeated_streams_do_not_exhaust_the_pool(self, sqlite_path, seeded_sqlite):
        with sqlite_db(sqlite_path, pool_size=1, max_overflow=1) as db:
            for _ in range(6):
                for _row in db.fetch_iter(sa.select(Author), chunk=1):
                    break
            assert db.fetch_all(sa.select(Author))

    def test_it_is_refused_on_the_engine_inside_a_transaction(self, engine):
        """Same reason `fetch_all` is: it would take a different pooled connection
        and miss the transaction's uncommitted writes."""
        with engine.begin():
            with pytest.raises(rowform.EngineStateError, match="fetch_iter"):
                collect(engine.fetch_iter(sa.select(Author)))

    def test_streaming_inside_a_transaction_sees_its_writes(self, engine):
        with engine.begin() as conn:
            conn.execute(
                sa.insert(Author.__table__).values(id=8001, name="uncommitted", active=True)
            )
            names = [a.name for a in conn.fetch_iter(sa.select(Author), chunk=2)]
        assert "uncommitted" in names

    def test_a_savepoint_can_stream_too(self, engine):
        with engine.begin() as conn, conn.begin_nested():
            rows = collect(conn.fetch_iter(sa.select(Author), chunk=2))
        assert rows


class TestStatementsItRefuses:
    def test_a_statement_that_returns_no_rows(self, engine):
        statement = sa.insert(Author.__table__).values(id=8100, name="ada", active=True)
        with pytest.raises(rowform.StatementError, match="produces no rows"):
            collect(engine.fetch_iter(statement))

    def test_returning_streams_on_sqlite_and_asyncpg(self, streamable_engine):
        """sqlite has no restriction, and asyncpg opens a portal over the write —
        both stream what the psycopg driver cannot. psycopg's half of this is
        `test_psycopg_refuses_returning_with_a_reason` below."""
        statement = (
            sa.insert(Author.__table__)
            .values(id=8200, name="barbara", active=True)
            .returning(Author.__table__)
        )
        streamed = collect(streamable_engine.fetch_iter(statement, chunk=1))
        assert [a.name for a in streamed] == ["barbara"]

    def test_psycopg_refuses_returning_with_a_reason(self, pg_dsn):
        """Postgres cannot DECLARE a cursor for a write, so this fails as an
        `UnsupportedError` naming the alternative rather than as a syntax error
        from the server."""
        with engine_at(pg_url(pg_dsn, "psycopg")) as db:
            seed(db)
            statement = (
                sa.insert(Author.__table__)
                .values(id=8300, name="edsger", active=True)
                .returning(Author.__table__)
            )
            with pytest.raises(rowform.UnsupportedError, match="only DECLARE one for a SELECT"):
                collect(db.fetch_iter(statement))

    def test_psycopg_streams_nested_on_one_connection(self, pg_dsn):
        """Two live streams on the same pinned connection.

        Cursor names are per session, so a fixed one made the second stream fail
        with `DuplicateCursor: cursor "rowform_stream" already exists`. Only the
        psycopg driver declares a named cursor, so only it can hit this.
        """
        with engine_at(pg_url(pg_dsn, "psycopg")) as db:
            seed(db)
            with db.begin() as conn:
                outer = conn.fetch_iter(sa.select(Author).order_by(Author.id), chunk=1)
                try:
                    for _first in outer:
                        inner = collect(conn.fetch_iter(sa.select(Author), chunk=1))
                        assert inner
                        break
                finally:
                    outer.close()

    def test_psycopg_streams_a_select(self, pg_dsn):
        with engine_at(pg_url(pg_dsn, "psycopg")) as db:
            seed(db)
            statement = sa.select(Author).order_by(Author.id)
            streamed = collect(db.fetch_iter(statement, chunk=2))
            assert [a.name for a in streamed] == [
                a.name for a in db.fetch_all(statement)
            ]


class TestObserver:
    def test_a_stream_reports_once_with_the_total(self, engine):
        seen: list[tuple[str, float, int | None]] = []
        engine.observer = lambda *call: seen.append(call)
        rows = collect(engine.fetch_iter(sa.select(Author), chunk=1))
        assert len(seen) == 1
        assert seen[0][2] == len(rows)
