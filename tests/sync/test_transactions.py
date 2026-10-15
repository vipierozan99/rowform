"""Connection scopes, transactions and savepoints, on both backends.

All three are SQLAlchemy's — `engine.begin()`, `conn.begin_nested()`,
`conn.commit()` — with rowform's statements running on the driver connection
underneath. So what is being asserted here is that the two halves agree: that a
savepoint SQLAlchemy opened contains writes rowform issued, and that rolling one
back discards them.
"""

from __future__ import annotations

import pytest
import sqlalchemy as sa
from sync.conftest import Author, Book, Tag, seed, sqlite_db

import rowform as rf
import rowform.sync as rfs


class Boom(Exception):
    """Raised on purpose, so a rollback test cannot pass by accident."""


def count_authors():
    return sa.select(sa.func.count()).select_from(Author)


class TestAtomicity:
    def test_commit_persists_every_statement(self, engine):
        with engine.begin() as conn:
            conn.execute(sa.insert(Author.__table__).values(id=50, name="x", active=True))
            conn.execute(sa.insert(Author.__table__).values(id=51, name="y", active=True))
        assert engine.fetch_one(count_authors()) == 6

    def test_an_exception_rolls_the_whole_block_back(self, engine):
        with pytest.raises(Boom):
            with engine.begin() as conn:
                conn.execute(
                    sa.insert(Author.__table__).values(id=50, name="x", active=True)
                )
                raise Boom
        assert engine.fetch_one(count_authors()) == 4

    def test_the_exception_is_not_swallowed(self, engine):
        with pytest.raises(Boom):
            with engine.begin():
                raise Boom


class TestConnectIsCommitAsYouGo:
    """`connect()` is SQLAlchemy's other scope: the first statement autobegins,
    and leaving without `commit()` discards the work. That is a footgun rowform's
    old always-committing `transaction()` did not have, and it is kept because it
    is what a SQLAlchemy user expects."""

    def test_without_commit_the_write_is_discarded(self, engine):
        with engine.connect() as conn:
            conn.execute(sa.insert(Author.__table__).values(id=50, name="x", active=True))
        assert engine.fetch_one(count_authors()) == 4

    def test_with_commit_it_persists(self, engine):
        with engine.connect() as conn:
            conn.execute(sa.insert(Author.__table__).values(id=50, name="x", active=True))
            conn.commit()
        assert engine.fetch_one(count_authors()) == 5

    def test_rollback_discards_explicitly(self, engine):
        with engine.connect() as conn:
            conn.execute(sa.insert(Author.__table__).values(id=50, name="x", active=True))
            conn.rollback()
            assert conn.fetch_one(count_authors()) == 4

    def test_the_first_statement_autobegins(self, engine):
        with engine.connect() as conn:
            assert conn.in_transaction() is False
            conn.fetch_all(sa.select(Author))
            assert conn.in_transaction() is True

    def test_a_stream_autobegins_like_any_other_first_statement(self, engine):
        """`fetch_iter` returns its iterator rather than awaiting, so it was the
        one statement that left the scope untransacted — and a `commit()` after
        it then had nothing to end."""
        with engine.connect() as conn:
            assert conn.in_transaction() is False
            for _row in conn.fetch_iter(sa.select(Author), chunk=2):
                break
            assert conn.in_transaction() is True


class TestVisibility:
    def test_reads_its_own_uncommitted_writes(self, engine):
        with engine.begin() as conn:
            conn.execute(sa.insert(Author.__table__).values(id=50, name="x", active=True))
            assert conn.fetch_one(count_authors()) == 5

    def test_invisible_to_other_connections_until_commit(self, engine):
        with engine.begin() as tx:
            tx.execute(sa.insert(Author.__table__).values(id=50, name="x", active=True))
            with engine.acquire() as conn:
                rows, _ = engine.driver.fetch(
                    conn, "SELECT count(*) FROM t_authors", (), False
                )
                assert rows[0][0] == 4


class TestReadsInsideScopes:
    def test_fetch_all_hydrates_models(self, engine):
        with engine.begin() as conn:
            authors = conn.fetch_all(sa.select(Author).order_by(Author.id))
        assert [a.name for a in authors] == ["ada", "brian", "carol", "dan"]

    def test_joins_work_inside_a_scope(self, engine):
        with engine.begin() as conn:
            rows = conn.fetch_all(
                sa.select(Author, Book).outerjoin(Book).order_by(Author.id, Book.id)
            )
        assert {a.name: b for a, b in rows}["dan"] is None

    def test_fetch_one(self, engine):
        with engine.begin() as conn:
            assert (conn.fetch_one(sa.select(Author).order_by(Author.id))).name == "ada"
            assert conn.fetch_one(count_authors()) == 4

    def test_the_engine_and_the_scope_share_compiled_queries(self, engine):
        statement = sa.select(Author).order_by(Author.id)
        engine.fetch_all(statement)
        compiled = dict(engine._queries)
        with engine.begin() as conn:
            conn.fetch_all(statement)
        assert engine._queries == compiled, "the scope recompiled the statement"

    def test_a_raw_string_runs_on_the_pinned_connection(self, engine):
        with engine.begin() as conn:
            conn.exec_driver_sql("DELETE FROM t_tags")
            assert conn.fetch_one(sa.select(sa.func.count()).select_from(Tag)) == 0

    def test_execute_many_inside_a_scope(self, engine):
        with engine.begin() as conn:
            conn.execute_many(
                sa.insert(Tag.__table__),
                [{"id": 300 + i, "book_id": 10, "label": "l"} for i in range(3)],
            )
        assert engine.fetch_one(sa.select(sa.func.count()).select_from(Tag)) == 5

    def test_execute_with_a_list_is_an_executemany(self, engine):
        """SQLAlchemy's spelling of `execute_many`."""
        with engine.begin() as conn:
            result = conn.execute(
                sa.insert(Tag.__table__),
                [{"id": 400 + i, "book_id": 10, "label": "l"} for i in range(3)],
            )
            assert result.returns_rows is False
        assert engine.fetch_one(sa.select(sa.func.count()).select_from(Tag)) == 5


class TestSavepoints:
    def test_a_nested_block_reports_itself(self, engine):
        with engine.begin() as conn:
            assert conn.in_nested_transaction() is False
            with conn.begin_nested() as sp:
                assert conn.in_nested_transaction() is True
                assert sp.is_active is True

    def test_inner_failure_keeps_the_outer_work(self, engine):
        with engine.begin() as conn:
            conn.execute(sa.insert(Author.__table__).values(id=50, name="x", active=True))
            with pytest.raises(Boom):
                with conn.begin_nested():
                    conn.execute(
                        sa.insert(Author.__table__).values(id=51, name="y", active=True)
                    )
                    raise Boom
            assert conn.fetch_one(count_authors()) == 5
        assert engine.fetch_one(count_authors()) == 5

    def test_inner_success_is_kept(self, engine):
        with engine.begin() as conn, conn.begin_nested():
            conn.execute(sa.insert(Author.__table__).values(id=50, name="x", active=True))
        assert engine.fetch_one(count_authors()) == 5

    def test_outer_failure_discards_a_released_savepoint(self, engine):
        with pytest.raises(Boom):
            with engine.begin() as conn:
                with conn.begin_nested():
                    conn.execute(
                        sa.insert(Author.__table__).values(id=50, name="x", active=True)
                    )
                raise Boom
        assert engine.fetch_one(count_authors()) == 4

    def test_an_explicit_savepoint_rollback(self, engine):
        with engine.begin() as conn:
            with conn.begin_nested() as sp:
                conn.execute(
                    sa.insert(Author.__table__).values(id=50, name="x", active=True)
                )
                sp.rollback()
            assert conn.fetch_one(count_authors()) == 4


class TestTheFootgunGuard:
    """`engine.fetch_all()` inside a scope would take a *different* pooled
    connection, miss the uncommitted writes, and not roll back with the block —
    producing plausible wrong results rather than an error."""

    def test_engine_fetch_all_inside_a_scope_raises(self, engine):
        with engine.begin():
            with pytest.raises(RuntimeError, match="different pooled connection"):
                engine.fetch_all(sa.select(Author))

    def test_it_guards_connect_too(self, engine):
        with engine.connect():
            with pytest.raises(RuntimeError, match="different pooled connection"):
                engine.fetch_all(sa.select(Author))

    @pytest.mark.parametrize("method", ["execute", "scalar", "scalars"])
    def test_the_write_one_shots_are_guarded_too(self, engine, method):
        """Worse than a stale read: these check out on their own and *commit*, so
        inside a scope they would survive its rollback — and on a pool of one they
        would deadlock waiting for the connection the scope is holding."""
        with engine.begin():
            with pytest.raises(RuntimeError, match="different pooled connection"):
                getattr(engine, method)(sa.select(Author))

    def test_execute_many_is_guarded_too(self, engine):
        with engine.begin():
            with pytest.raises(RuntimeError, match="different pooled connection"):
                engine.execute_many(
                    sa.insert(Author.__table__), [{"id": 60, "name": "n", "active": True}]
                )

    def test_the_engine_works_again_after_the_block(self, engine):
        with engine.begin():
            pass
        assert len(engine.fetch_all(sa.select(Author))) == 4

    def test_the_guard_is_scoped_to_the_same_engine(self, engine, sqlite_path):
        """`other` is seeded here rather than relied upon: when `engine` is the
        postgres parametrisation nothing has touched the sqlite file, so an
        unseeded read would pass by returning [] whatever the guard did."""
        with sqlite_db(sqlite_path) as other:
            seed(other)
            with engine.begin():
                rows = other.fetch_all(sa.select(Author).order_by(Author.id))
        assert [a.name for a in rows] == ["ada", "brian", "carol", "dan"]

    def test_active_connection_tracks_the_innermost_scope(self, engine):
        assert rfs.active_connection() is None
        with engine.begin() as conn:
            assert rfs.active_connection() is conn
        assert rfs.active_connection() is None


class TestScopeOptions:
    """Options are SQLAlchemy's `execution_options`, so what a backend will and
    will not honour is SQLAlchemy's answer rather than a table maintained here."""

    def test_an_unknown_isolation_level_is_refused(self, sqlite_engine):
        with pytest.raises(sa.exc.ArgumentError, match="[Ii]nvalid value"):
            with sqlite_engine.begin(isolation_level="NONSENSE"):
                pass

    def test_a_level_sqlite_has_is_accepted(self, sqlite_engine):
        with sqlite_engine.begin(isolation_level="SERIALIZABLE") as conn:
            assert conn.in_transaction() is True

    def test_no_options_is_a_plain_block(self, sqlite_engine):
        with sqlite_engine.begin() as conn:
            assert conn.in_nested_transaction() is False

    def test_options_are_refused_on_a_borrowed_connection(self, sqlite_engine):
        with sqlite_engine.connect() as conn:
            with pytest.raises(rf.ConfigurationError, match="did not open"):
                with sqlite_engine.connect(
                    bind=conn.sa_connection, isolation_level="SERIALIZABLE"
                ):
                    pass


class TestSqliteBeginCost:
    """pysqlite needs a literal `BEGIN` for savepoints to work at all
    (`SqliteDriver.configure`), and *how* it is sent is a measured cost rather
    than a detail: through SQLAlchemy's cursor adapter it is three round trips to
    aiosqlite's worker thread — `cursor()`, `execute()`, `close()` — and on the
    driver connection it is one. Worth 0.10 ms per scope, per request rather than
    per row, which is most of a small read (docs/BENCHMARKS.md).

    Asserted here because nothing else would notice it coming back: the cheaper
    spelling and the expensive one are behaviourally identical, and every test
    above this one passes either way.
    """

    def test_begin_costs_one_driver_round_trip(self, sqlite_engine, monkeypatch):
        import aiosqlite

        hops: list[str] = []
        original = aiosqlite.Connection._execute

        def record(self, fn, *args, **kwargs):
            sql = next((a for a in args if isinstance(a, str)), "")
            hops.append(f"{getattr(fn, '__name__', '?')} {sql}".strip())
            return original(self, fn, *args, **kwargs)

        monkeypatch.setattr(aiosqlite.Connection, "_execute", record)
        with sqlite_engine.begin() as conn:
            conn.fetch_all(sa.select(Author))

        select = next(i for i, hop in enumerate(hops) if "SELECT" in hop)
        assert hops[:select] == ["execute BEGIN"], hops

    def test_wrapping_one_engine_twice_still_sends_one_begin(self, sqlite_engine, monkeypatch):
        """A second registration would send `BEGIN` twice, which pysqlite rejects."""
        import aiosqlite

        second = rfs.Engine(sqlite_engine.sa_engine)
        hops: list[str] = []
        original = aiosqlite.Connection._execute

        def record(self, fn, *args, **kwargs):
            sql = next((a for a in args if isinstance(a, str)), "")
            hops.append(f"{getattr(fn, '__name__', '?')} {sql}".strip())
            return original(self, fn, *args, **kwargs)

        monkeypatch.setattr(aiosqlite.Connection, "_execute", record)
        with second.begin() as conn:
            conn.fetch_all(sa.select(Author))

        assert [hop for hop in hops if "BEGIN" in hop] == ["execute BEGIN"], hops
