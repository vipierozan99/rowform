"""`db.connect(bind=...)` — rowform's reads on a connection somebody else owns.

This is the payoff for giving up rowform's own pool (CLAUDE.md, goal 2), and
the thing an own pool structurally could not do: an application keeps its
`AsyncEngine`, its `AsyncSession` and its migrations, and adopts rowform one query
at a time. If these pass, `CLAUDE.md`'s goal 2 holds; if they fail, the checkout
cost bought nothing.

The claim under test is not "it runs" but "it is *the same transaction*": it sees
uncommitted writes, and it is rolled back with them.
"""

from __future__ import annotations

import pytest
import sqlalchemy as sa
from conftest import Author, Base, Book, pg_url, sqlite_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase

import rowform as rf


class Boom(Exception):
    pass


class OrmBase(DeclarativeBase):
    pass


class AuthorORM(OrmBase):
    """A stock ORM mapping over the *same* `Table` the rowform model declares —
    the shape of an application migrating one query at a time, and the only way
    to exercise `session.add()`, which rowform models do not support."""

    __table__ = Author.__table__


@pytest.fixture(params=["sqlite", "asyncpg", "psycopg"])
async def pair(request, tmp_path):
    """One `AsyncEngine`, wrapped — the shape a migrating application is in.

    All three drivers, because "the same transaction" is three different
    mechanisms underneath: psycopg's connection is transactional in its own
    right, sqlite gets an explicit BEGIN from `SqliteDriver.configure`, and
    asyncpg has none until `AsyncpgDriver.enter_transaction` puts it in one.
    Asserting the goal on sqlite alone is how the asyncpg case came to be written
    and stay broken — so naming psycopg here and then not running it was the same
    mistake one driver over.
    """
    if request.param == "sqlite":
        url = sqlite_url(str(tmp_path / "bind.sqlite3"))
    else:
        url = pg_url(request.getfixturevalue("pg_dsn"), request.param)
    sa_engine = create_async_engine(url)
    db = rf.Engine(sa_engine)
    await db.drop_all(Base.metadata)
    await db.create_all(Base.metadata)
    await db.execute(sa.insert(Author.__table__).values(id=1, name="ada", active=True))
    try:
        yield db, sa_engine
    finally:
        await sa_engine.dispose()


class TestOnAConnection:
    async def test_it_reads(self, pair):
        db, sa_engine = pair
        async with sa_engine.connect() as their, db.connect(bind=their) as conn:
            assert [a.name for a in await conn.fetch_all(sa.select(Author))] == ["ada"]

    async def test_it_sees_uncommitted_writes(self, pair):
        """SQLAlchemy writes, rowform reads, one transaction. Two connections
        could not do this, which is the whole point."""
        db, sa_engine = pair
        async with sa_engine.connect() as their, their.begin():
            await their.execute(sa.insert(Author.__table__).values(id=2, name="bo", active=True))
            async with db.connect(bind=their) as conn:
                rows = await conn.fetch_all(sa.select(Author))
            assert sorted(a.name for a in rows) == ["ada", "bo"]

    async def test_its_writes_roll_back_with_the_block(self, pair):
        db, sa_engine = pair
        with pytest.raises(Boom):
            async with sa_engine.connect() as their, their.begin():
                async with db.connect(bind=their) as conn:
                    await conn.execute(
                        sa.insert(Author.__table__).values(id=3, name="cy", active=True)
                    )
                    assert len(await conn.fetch_all(sa.select(Author))) == 2
                raise Boom
        assert [a.name for a in await db.fetch_all(sa.select(Author))] == ["ada"]

    async def test_the_compat_track_works_bound_too(self, pair):
        db, sa_engine = pair
        async with sa_engine.connect() as their, db.connect(bind=their) as conn:
            result = await conn.execute(sa.select(Author))
            assert [a.name for a in result.scalars().all()] == ["ada"]

    async def test_it_does_not_end_the_transaction_it_was_handed(self, pair):
        """Leaving the bound block must not commit or roll back: the caller's
        block is the scope, and ending it here would surprise them."""
        db, sa_engine = pair
        async with sa_engine.connect() as their, their.begin():
            async with db.connect(bind=their) as conn:
                await conn.execute(
                    sa.insert(Author.__table__).values(id=7, name="fi", active=True)
                )
            assert their.in_transaction() is True
            assert len(await db.fetch_all(sa.select(Author))) == 1, "committed early"
        assert len(await db.fetch_all(sa.select(Author))) == 2


class TestOnASession:
    async def test_it_joins_the_sessions_transaction(self, pair):
        db, sa_engine = pair
        Session = async_sessionmaker(sa_engine)
        async with Session() as session, session.begin():
            await session.execute(
                sa.insert(Author.__table__).values(id=4, name="di", active=True)
            )
            async with db.connect(bind=session) as conn:
                rows = await conn.fetch_all(sa.select(Author))
            assert sorted(a.name for a in rows) == ["ada", "di"]

    async def test_a_pending_add_is_not_visible_until_it_is_flushed(self, pair):
        """rowform reads the connection under the session, not the session.

        So nothing it does triggers autoflush, and a `session.add()` still
        pending in the identity map is not in the database for rowform to find.
        `Engine.connect` documents the flush as the caller's; this pins both
        halves so the documented rule cannot quietly stop being true.
        """
        db, sa_engine = pair
        Session = async_sessionmaker(sa_engine)
        async with Session() as session, session.begin():
            session.add(AuthorORM(id=6, name="fran", active=True))

            async with db.connect(bind=session) as conn:
                before = await conn.fetch_all(sa.select(Author))
            assert [a.name for a in before] == ["ada"]

            await session.flush()
            async with db.connect(bind=session) as conn:
                after = await conn.fetch_all(sa.select(Author))
            assert sorted(a.name for a in after) == ["ada", "fran"]

    async def test_a_session_rollback_takes_rowforms_reads_with_it(self, pair):
        db, sa_engine = pair
        Session = async_sessionmaker(sa_engine)
        async with Session() as session:
            await session.execute(
                sa.insert(Author.__table__).values(id=5, name="eve", active=True)
            )
            async with db.connect(bind=session) as conn:
                assert len(await conn.fetch_all(sa.select(Author))) == 2
            await session.rollback()
        assert [a.name for a in await db.fetch_all(sa.select(Author))] == ["ada"]


class TestOnASessionWithSeveralEngines:
    """A session binding tables to different engines has no single connection;
    each rf.Engine joins the session through the connection for its own engine."""

    @staticmethod
    def sessions(sa_engine, other):
        return async_sessionmaker(binds={Author.__table__: sa_engine, Book.__table__: other})

    async def test_it_joins_through_its_own_engine(self, pair):
        db, sa_engine = pair
        other = create_async_engine(sa_engine.url)
        try:
            async with self.sessions(sa_engine, other)() as session, session.begin():
                await session.execute(
                    sa.insert(Author.__table__).values(id=5, name="eve", active=True)
                )
                async with db.connect(bind=session) as conn:
                    names = await conn.fetch_all(sa.select(Author.name).order_by(Author.id))
                    mine = conn.sa_connection.sync_connection
                async with rf.Engine(other).connect(bind=session) as conn:
                    theirs = conn.sa_connection.sync_connection
                assert names == ["ada", "eve"], "the session's uncommitted write"
                assert mine is not theirs
        finally:
            await other.dispose()

    async def test_an_engine_the_session_is_not_bound_to_is_refused(self, pair):
        _, sa_engine = pair
        other, stranger = create_async_engine(sa_engine.url), create_async_engine(sa_engine.url)
        try:
            async with self.sessions(sa_engine, other)() as session, session.begin():
                with pytest.raises(rf.ConfigurationError, match="not bound to"):
                    async with rf.Engine(stranger).connect(bind=session):
                        pass
        finally:
            await other.dispose()
            await stranger.dispose()


class TestItWillNotEndSomebodyElsesTransaction:
    """`connect(bind=...)` promises the caller's block is the scope. `_owns` kept
    rowform from *starting* a transaction there; these are the three methods that
    could have ended one."""

    @pytest.mark.parametrize("method", ["commit", "rollback", "close"])
    async def test_it_refuses(self, pair, method):
        db, sa_engine = pair
        async with sa_engine.connect() as their, db.connect(bind=their) as conn:
            with pytest.raises(rf.EngineStateError, match=f"conn.{method}"):
                await getattr(conn, method)()

    async def test_the_caller_is_still_usable_afterwards(self, pair):
        """The point of the refusal: before it, `conn.close()` closed the
        connection under the caller and their next statement raised."""
        db, sa_engine = pair
        Session = async_sessionmaker(sa_engine)
        async with Session() as session, session.begin():
            async with db.connect(bind=session) as conn:
                with pytest.raises(rf.EngineStateError):
                    await conn.close()
            await session.execute(
                sa.insert(Author.__table__).values(id=7, name="gil", active=True)
            )
        assert len(await db.fetch_all(sa.select(Author))) == 2


class TestItRefusesWhatItCannotDo:
    async def test_something_that_is_neither(self, pair):
        db, _ = pair
        with pytest.raises(rf.ConfigurationError, match="AsyncConnection or an AsyncSession"):
            async with db.connect(bind=object()):
                pass

    async def test_a_connection_from_another_driver(self, pg_dsn, pair, tmp_path):
        """The compiled SQL carries one driver's paramstyle. Running it on
        another's would bind wrongly rather than fail cleanly, so it is refused
        by name. Needs a real postgres, because the check is on a live
        connection's dialect — it skips with the rest of the postgres suite."""
        db, _ = pair
        other_url = (
            pg_url(pg_dsn)
            if db.dialect.name == "sqlite"
            else sqlite_url(str(tmp_path / "other.sqlite3"))
        )
        other = create_async_engine(other_url)
        try:
            async with other.connect() as their:
                with pytest.raises(rf.ConfigurationError, match="wrong paramstyle"):
                    async with db.connect(bind=their):
                        pass
        finally:
            await other.dispose()

    async def test_a_one_shot_inside_a_bound_scope_is_refused(self, pair):
        """A bound scope registers, so an `engine.fetch_*` one-shot inside it is
        refused (R2): it would take a *different* pooled connection and miss the
        bound transaction's uncommitted writes — the silent-wrong-read this guard
        exists to stop. The registration is block-scoped, so `active_connection()`
        is the bound conn inside and None the moment the block exits."""
        db, sa_engine = pair
        async with sa_engine.connect() as their, db.connect(bind=their) as conn:
            assert rf.active_connection() is conn
            with pytest.raises(rf.EngineStateError, match=r"conn\.fetch_all"):
                await db.fetch_all(sa.select(Author))
            # The bound conn's own track still works inside the scope.
            assert [a.name for a in await conn.fetch_all(sa.select(Author))] == ["ada"]
        assert rf.active_connection() is None
