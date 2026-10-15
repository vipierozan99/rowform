"""The compatibility track: `conn.execute()` returns a real SQLAlchemy `Result`.

The claim is not "close enough" but "the same object". rowform hydrates the rows
and hands the list to `IteratorResult` — which builds a `Row` only if one is
asked for — so what is asserted below is that every accessor behaves as it does
upstream, including the ones nothing in rowform implements.

The matrix is deliberate: one entity, one scalar column, two entities, two scalar
columns, an entity beside a scalar, and a statement with no result set. Those are
the shapes where `.all()`, `.scalars()` and `.mappings()` disagree with each
other, and the single-entity case is the one where rowform's own `fetch_all()`
differs — `[User]` there against `[Row(User,)]` here.
"""

from __future__ import annotations

import pytest
import sqlalchemy as sa
from sync.conftest import Author, Book

import rowform as rf
import rowform.sync as rfs


class TestOneEntity:
    """`select(Author)` — the shape where the two tracks differ."""

    def test_all_gives_rows_not_entities(self, engine):
        with engine.connect() as conn:
            rows = (conn.execute(sa.select(Author).order_by(Author.id))).all()
        assert isinstance(rows[0], sa.Row)
        assert rows[0][0].name == "ada"
        assert len(rows[0]) == 1

    def test_the_hot_track_gives_the_entity(self, engine):
        """Same statement, other track, no Row."""
        with engine.connect() as conn:
            rows = conn.fetch_all(sa.select(Author).order_by(Author.id))
        assert isinstance(rows[0], Author)

    def test_scalars_unwraps(self, engine):
        with engine.connect() as conn:
            authors = (conn.execute(sa.select(Author).order_by(Author.id))).scalars().all()
        assert [a.name for a in authors] == ["ada", "brian", "carol", "dan"]

    def test_tuples_is_the_row_shape(self, engine):
        with engine.connect() as conn:
            rows = (conn.execute(sa.select(Author).order_by(Author.id))).tuples().all()
        assert rows[0][0].name == "ada"

    def test_attribute_access_by_entity_name(self, engine):
        with engine.connect() as conn:
            row = (conn.execute(sa.select(Author).order_by(Author.id))).first()
        assert row.Author.name == "ada"

    def test_scalar_one_and_friends(self, engine):
        with engine.connect() as conn:
            one = sa.select(Author).where(Author.id == 1)
            assert (conn.execute(one)).scalar_one().name == "ada"
            assert (conn.execute(one)).scalar_one_or_none().name == "ada"
            assert (conn.execute(one)).scalar().name == "ada"
            none = sa.select(Author).where(Author.id == 999)
            assert (conn.execute(none)).scalar_one_or_none() is None
            assert (conn.execute(none)).scalar() is None

    def test_iteration_yields_rows(self, engine):
        with engine.connect() as conn:
            names = [row[0].name for row in conn.execute(sa.select(Author).order_by(Author.id))]
        assert names == ["ada", "brian", "carol", "dan"]


class TestOneScalarColumn:
    def test_all_and_scalars(self, engine):
        with engine.connect() as conn:
            statement = sa.select(Author.name).order_by(Author.id)
            rows = (conn.execute(statement)).all()
            assert rows[0][0] == "ada", "a 1-tuple, as SQLAlchemy gives"
            assert (conn.execute(statement)).scalars().all()[0] == "ada"

    def test_mappings_keys_off_the_column(self, engine):
        with engine.connect() as conn:
            rows = (conn.execute(sa.select(Author.name).order_by(Author.id))).mappings().all()
        assert rows[0] == {"name": "ada"}

    def test_keys(self, engine):
        with engine.connect() as conn:
            result = conn.execute(sa.select(Author.name, Author.id))
        assert list(result.keys()) == ["name", "id"]


class TestTwoEntities:
    def test_all_is_already_a_tuple(self, engine):
        with engine.connect() as conn:
            rows = (
                conn.execute(
                    sa.select(Author, Book).outerjoin(Book).order_by(Author.id, Book.id)
                )
            ).all()
        assert len(rows[0]) == 2
        assert rows[0][0].name == "ada"

    def test_scalars_takes_the_first(self, engine):
        with engine.connect() as conn:
            firsts = (
                conn.execute(
                    sa.select(Author, Book).outerjoin(Book).order_by(Author.id, Book.id)
                )
            ).scalars().all()
        assert all(isinstance(a, Author) for a in firsts)


class TestTwoScalarColumns:
    def test_row_attribute_and_index_agree(self, engine):
        with engine.connect() as conn:
            row = (conn.execute(sa.select(Author.name, Author.id).order_by(Author.id))).first()
        assert row.name == "ada"
        assert row[1] == 1
        assert tuple(row) == ("ada", 1)

    def test_mappings(self, engine):
        with engine.connect() as conn:
            rows = (
                conn.execute(sa.select(Author.name, Author.id).order_by(Author.id))
            ).mappings().all()
        assert rows[0] == {"name": "ada", "id": 1}

    def test_scalars_index(self, engine):
        with engine.connect() as conn:
            ids = (
                conn.execute(sa.select(Author.name, Author.id).order_by(Author.id))
            ).scalars(1).all()
        assert ids == [1, 2, 3, 4]


class TestEntityBesideScalar:
    def test_it_is_a_two_slot_row(self, engine):
        with engine.connect() as conn:
            row = (
                conn.execute(
                    sa.select(Author, Book.title).join(Book).order_by(Author.id).limit(1)
                )
            ).first()
        assert isinstance(row[0], Author)
        assert isinstance(row[1], str)


class TestTheExceptionsAreSQLAlchemysOwn:
    def test_one_with_no_rows(self, engine):
        with engine.connect() as conn:
            with pytest.raises(sa.exc.NoResultFound):
                (conn.execute(sa.select(Author).where(Author.id == 999))).one()

    def test_one_with_many_rows(self, engine):
        with engine.connect() as conn:
            with pytest.raises(sa.exc.MultipleResultsFound):
                (conn.execute(sa.select(Author))).one()

    def test_one_or_none_with_many_rows(self, engine):
        with engine.connect() as conn:
            with pytest.raises(sa.exc.MultipleResultsFound):
                (conn.execute(sa.select(Author))).one_or_none()

    def test_one_returns_the_row(self, engine):
        with engine.connect() as conn:
            row = (conn.execute(sa.select(Author).where(Author.id == 1))).one()
        assert row[0].name == "ada"


class TestNoResultSet:
    def test_rowcount_and_returns_rows(self, engine):
        with engine.begin() as conn:
            result = conn.execute(
                sa.insert(Author.__table__).values(id=80, name="p", active=True)
            )
        assert result.rowcount == 1
        assert result.returns_rows is False

    def test_reading_it_raises_rather_than_returning_empty(self, engine):
        with engine.begin() as conn:
            result = conn.execute(
                sa.insert(Author.__table__).values(id=81, name="q", active=True)
            )
            for accessor in (result.all, result.first, result.scalar):
                with pytest.raises(sa.exc.ResourceClosedError):
                    accessor()

    def test_a_write_with_returning_does_return(self, engine):
        """RETURNING is a result set, so it takes the normal path — no closed
        result, no ResourceClosedError."""
        with engine.begin() as conn:
            result = conn.execute(
                sa.insert(Author.__table__)
                .values(id=82, name="r", active=True)
                .returning(Author.__table__.c.name)
            )
            assert result.scalars().all() == ["r"]


class TestPartitionsAndUnique:
    def test_partitions(self, engine):
        with engine.connect() as conn:
            result = conn.execute(sa.select(Author).order_by(Author.id))
            sizes = [len(p) for p in result.partitions(3)]
        assert sizes == [3, 1]

    def test_unique_on_scalars(self, engine):
        with engine.connect() as conn:
            result = conn.execute(sa.select(Author.active))
            assert sorted(result.scalars().unique().all()) == [False, True]

    def test_unique_on_entities_is_refused_by_the_dataclass(self, engine):
        """A rowform model is an `eq=True` dataclass, so Python sets `__hash__` to
        None and SQLAlchemy's identity-based dedupe cannot hash it. Recorded
        rather than worked around: the fix is `frozen=True` on the model."""
        with engine.connect() as conn:
            result = conn.execute(sa.select(Author))
            with pytest.raises(TypeError, match="unhashable"):
                result.unique().all()


class TestStreaming:
    def test_stream_scalars(self, engine):
        with engine.connect() as conn:
            result = conn.stream(sa.select(Author).order_by(Author.id), chunk=2)
            names = [a.name for a in result.scalars()]
        assert names == ["ada", "brian", "carol", "dan"]

    def test_stream_partitions_honour_the_requested_size(self, engine):
        with engine.connect() as conn:
            result = conn.stream(sa.select(Author).order_by(Author.id))
            sizes = [len(p) for p in result.partitions(3)]
        assert sizes == [3, 1]

    def test_a_zero_yield_per_is_refused_not_defaulted(self, engine):
        """`yield_per(0)` reaches the chunk factory as an explicit 0, a bad size —
        it must hit the same guard `fetch_iter(chunk=0)` does rather than silently
        fall back to the stream's `chunk=` the way an unset (None) size does (T4)."""
        with engine.connect() as conn:
            result = (conn.stream(sa.select(Author).order_by(Author.id))).yield_per(0)
            with pytest.raises(rf.ConfigurationError, match="chunk must be at least 1"):
                for _ in result:
                    pass

    def test_stream_scalars_shorthand(self, engine):
        with engine.connect() as conn:
            result = conn.stream_scalars(sa.select(Author).order_by(Author.id))
            assert [a.name for a in result] == ["ada", "brian", "carol", "dan"]

    def test_rows_stream_as_rows(self, engine):
        with engine.connect() as conn:
            result = conn.stream(sa.select(Author.name, Author.id).order_by(Author.id))
            rows = [row for row in result]
        assert rows[0].name == "ada"


class TestTheEngineOneShots:
    def test_execute_scalar_scalars(self, engine):
        assert (engine.execute(sa.select(Author))).returns_rows is True
        assert engine.scalar(sa.select(sa.func.count()).select_from(Author)) == 4
        assert len((engine.scalars(sa.select(Author))).all()) == 4

    def test_the_result_outlives_the_connection(self, engine):
        """Rows are buffered by the time `execute()` returns, so the scope it
        opened for itself can close under them."""
        result = engine.execute(sa.select(Author).order_by(Author.id))
        assert [a.name for a in result.scalars().all()] == ["ada", "brian", "carol", "dan"]

    def test_params_reach_bindparams_either_way(self, engine):
        statement = sa.select(Author).where(Author.id > sa.bindparam("floor"))
        assert len((engine.execute(statement, {"floor": 2})).all()) == 2
        assert len((engine.execute(statement, floor=0)).all()) == 4

    def test_a_returning_write_with_many_parameter_sets_is_committed(self, pg_engine):
        """A list of parameter sets takes the executemany path, which reports
        rather than returns rows. Deciding to commit from `returns_rows` alone
        left this one uncommitted, and the pool's rollback on release discarded
        it — silently, because the executemany path returns no rows to miss.

        postgres only: sqlite3 refuses `executemany` on a statement that returns
        rows at all (`InterfaceError`), so the combination cannot get far enough
        there to be discarded.
        """
        pg_engine.execute(
            sa.insert(Author.__table__).returning(Author.__table__.c.id),
            [
                {"id": 70, "name": "grace", "active": True},
                {"id": 71, "name": "edsger", "active": True},
            ],
        )
        count = sa.select(sa.func.count()).select_from(Author)
        assert pg_engine.fetch_one(count) == 6

    def test_a_one_shot_returning_write_is_committed(self, engine):
        """The other half of the bug above: a *single* parameter set.

        `insert(...).returning(...)` returns rows and is still a write, so
        deciding from `returns_rows` sent it to the non-committing checkout and
        the pool's rollback on release discarded it. `execute()` reaches that
        checkout through `_scope` and `fetch_all()` through `_acquire_for`, so
        both are asserted.

        The rows come back either way, so only persistence can catch this — and
        only on psycopg, whose connection is transactional in its own right.
        """
        table = Author.__table__
        engine.execute(
            sa.insert(table).values(id=80, name="ada l", active=True).returning(table.c.id)
        )
        engine.fetch_all(
            sa.insert(table).values(id=81, name="grace h", active=True).returning(table)
        )
        landed = engine.fetch_all(
            sa.select(Author.id).where(Author.id >= 80).order_by(Author.id)
        )
        assert landed == [80, 81]

    def test_a_streamed_returning_write_is_committed(self, streamable_engine):
        """`fetch_iter` takes the same checkout and needed the same fix.

        Not on psycopg: postgres will not `DECLARE` a cursor for a write with
        RETURNING, so the statement is refused before it can be discarded
        (`PsycopgDriver.stream`).
        """
        table = Author.__table__
        streamed = streamable_engine.fetch_iter(
            sa.insert(table).values(id=83, name="edsger d", active=True).returning(table)
        )
        assert len([row for row in streamed]) == 1
        assert streamable_engine.fetch_one(
            sa.select(Author.name).where(Author.id == 83)
        ) == "edsger d"

    def test_params_cannot_be_mixed_with_many_parameter_sets(self, engine):
        with pytest.raises(rf.StatementError, match="cannot be combined"):
            engine.execute(
                sa.insert(Author.__table__),
                [{"id": 72, "name": "z"}],
                active=True,
            )


class TestBothTracksAgree:
    def test_same_objects_either_way(self, engine):
        """The tracks differ in packaging, never in the values."""
        statement = sa.select(Author).order_by(Author.id)
        with engine.connect() as conn:
            hot = conn.fetch_all(statement)
            compat = (conn.execute(statement)).scalars().all()
        assert hot == compat
        assert all(isinstance(a, Author) for a in hot)

    def test_scalar_projection_agrees(self, engine):
        statement = sa.select(Author.name, Author.id).order_by(Author.id)
        with engine.connect() as conn:
            hot = conn.fetch_all(statement)
            compat = (conn.execute(statement)).tuples().all()
        assert hot == compat
