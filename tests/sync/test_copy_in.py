"""`copy_in` loads the same rows `execute_many` would.

COPY bypasses the statement path, and with it the bind processors that turn a
`Decimal`, `datetime`, `Enum`, `UUID` or `dict` into what the driver sends. That
is the same class of mistake as docs/BENCHMARKS.md, lesson 11, in the other
direction: values that look plausible and are not what came in.

So `execute_many` is the oracle. Both paths load the same rows into the same
table, and what comes back has to match field for field and type for type — over
the `Wide` model, which exists precisely because it holds one column per type
whose driver representation differs from its Python one.
"""

from __future__ import annotations

import pytest
import sqlalchemy as sa
from sync.conftest import WIDE_ROW, Author, Wide, engine_at, pg_url, seed

import rowform


def wide_rows(count: int, start: int) -> list[dict]:
    return [{**WIDE_ROW, "id": start + i} for i in range(count)]


class TestItMatchesExecuteMany:
    def test_every_type_round_trips_identically(self, pg_engine):
        """The load-bearing test: two paths, one oracle, all types."""
        pg_engine.execute(sa.delete(Wide.__table__))
        pg_engine.execute_many(sa.insert(Wide.__table__), wide_rows(3, 100))
        via_insert = pg_engine.fetch_all(sa.select(Wide).order_by(Wide.id))

        pg_engine.execute(sa.delete(Wide.__table__))
        copied = pg_engine.copy_in(Wide.__table__, wide_rows(3, 100))
        via_copy = pg_engine.fetch_all(sa.select(Wide).order_by(Wide.id))

        assert copied == 3
        assert len(via_copy) == len(via_insert) == 3
        for got, want in zip(via_copy, via_insert, strict=True):
            for column in Wide.__table__.columns:
                mine, theirs = getattr(got, column.key), getattr(want, column.key)
                assert mine == theirs, f"{column.key}: {mine!r} != {theirs!r}"
                assert type(mine) is type(theirs), f"{column.key}: {type(mine)}"

    def test_a_column_subset_leaves_the_rest_to_the_server(self, pg_engine):
        """`Wide.note` is the nullable one; omitting it must land NULL rather
        than shifting every value one column to the left."""
        named = [c.key for c in Wide.__table__.columns if c.key != "note"]
        rows = [{k: v for k, v in row.items() if k != "note"} for row in wide_rows(2, 200)]

        pg_engine.execute(sa.delete(Wide.__table__))
        pg_engine.copy_in(Wide.__table__, rows, columns=named)

        loaded = pg_engine.fetch_all(sa.select(Wide).order_by(Wide.id))
        assert [w.id for w in loaded] == [200, 201]
        assert all(w.note is None for w in loaded)
        assert all(w.text == WIDE_ROW["text"] for w in loaded)
        assert all(w.amount == WIDE_ROW["amount"] for w in loaded)

    def test_no_rows_is_a_no_op(self, pg_engine):
        assert pg_engine.copy_in(Wide.__table__, []) == 0

    def test_it_reports_to_the_observer(self, pg_engine):
        seen: list[tuple[str, float, int | None]] = []
        pg_engine.observer = lambda *call: seen.append(call)
        pg_engine.execute(sa.delete(Wide.__table__))
        pg_engine.copy_in(Wide.__table__, wide_rows(2, 300))
        pg_engine.observer = None
        copies = [call for call in seen if call[0].startswith("COPY")]
        assert len(copies) == 1
        assert copies[0][2] == 2


class TestWhatItRefuses:
    def test_sqlite_says_what_to_use_instead(self, sqlite_engine):
        with pytest.raises(rowform.UnsupportedError, match="execute_many"):
            sqlite_engine.copy_in(Author.__table__, [{"id": 1, "name": "a", "active": True}])

    def test_a_missing_column_in_a_row_is_loud(self, pg_engine):
        with pytest.raises(KeyError):
            pg_engine.copy_in(Author.__table__, [{"id": 1}], columns=["id", "name"])

    def test_it_is_refused_inside_a_transaction(self, pg_engine):
        """It would take a different pooled connection and commit on its own, so a
        rollback of the surrounding block would leave the loaded rows behind —
        the same reason `fetch_all` is refused there."""
        with pg_engine.begin():
            with pytest.raises(rowform.EngineStateError, match="copy_in"):
                pg_engine.copy_in(Wide.__table__, wide_rows(1, 400))


class TestSchemaQualification:
    """A table with no explicit schema must resolve through `search_path`, and both
    postgres engines must agree about where the rows went.

    asyncpg qualifies the target itself from `schema_name`; psycopg gets a name
    quoted by SQLAlchemy's preparer. Defaulting the asyncpg side to `"public"`
    would send the two to different tables whenever `search_path` says otherwise —
    invisible until someone runs with a per-tenant search_path.
    """

    @pytest.fixture
    def two_schemas(self, pg_dsn):
        """The same table name in `tenant_a` and in `public`, both empty.

        Which one a copy lands in is then a question with a wrong answer, which is
        what makes the search_path behaviour testable at all.
        """
        with engine_at(pg_url(pg_dsn)) as db, db.acquire() as conn:
            conn.execute("CREATE SCHEMA IF NOT EXISTS tenant_a")
            for schema in ("tenant_a", "public"):
                conn.execute(f"DROP TABLE IF EXISTS {schema}.copy_target")
                conn.execute(
                    f"CREATE TABLE {schema}.copy_target (id int primary key, name text)"
                )
        yield
        with engine_at(pg_url(pg_dsn)) as db, db.acquire() as conn:
            for schema in ("tenant_a", "public"):
                conn.execute(f"DROP TABLE IF EXISTS {schema}.copy_target")

    @staticmethod
    def _unqualified() -> sa.Table:
        return sa.Table(
            "copy_target",
            sa.MetaData(),
            sa.Column("id", sa.Integer, primary_key=True),
            sa.Column("name", sa.String),
        )

    @staticmethod
    def _tenant_scoped(pg_dsn: str, driver: str):
        """An engine whose connections resolve unqualified names to `tenant_a`."""
        connect_args = (
            {"server_settings": {"search_path": "tenant_a"}}
            if driver == "asyncpg"
            else {"options": "-c search_path=tenant_a"}
        )
        return engine_at(pg_url(pg_dsn, driver), connect_args=connect_args)

    @pytest.mark.parametrize("driver", ["psycopg"])
    def test_an_unqualified_table_follows_search_path(
        self, pg_dsn, two_schemas, driver
    ):
        """The regression this guards: qualifying the target as "public" whenever
        the table declares no schema sends asyncpg somewhere psycopg would not go,
        and nothing says so until a tenant's rows appear in the wrong schema."""
        table = self._unqualified()
        with self._tenant_scoped(pg_dsn, driver) as db:
            db.copy_in(table, [{"id": 1, "name": driver}])

        with engine_at(pg_url(pg_dsn)) as check, check.acquire() as conn:
            tenant = conn.fetch("SELECT name FROM tenant_a.copy_target")
            public = conn.fetch("SELECT name FROM public.copy_target")
        assert [r["name"] for r in tenant] == [driver], "did not follow search_path"
        assert public == [], "landed in public despite the search_path"

    @pytest.mark.parametrize("driver", ["psycopg"])
    def test_an_explicit_schema_is_honoured(self, pg_dsn, two_schemas, driver):
        table = sa.Table(
            "copy_target",
            sa.MetaData(),
            sa.Column("id", sa.Integer, primary_key=True),
            sa.Column("name", sa.String),
            schema="tenant_a",
        )
        with engine_at(pg_url(pg_dsn, driver)) as db:
            db.copy_in(table, [{"id": 2, "name": driver}])
            landed = db.fetch_all(sa.select(table.c.name))
            assert landed == [driver]


class TestBothPostgresDrivers:
    """asyncpg copies over the binary protocol and psycopg over `FROM STDIN`, so
    each needs its own round trip."""

    @pytest.mark.parametrize("engine_factory", ["psycopg"])
    def test_each_driver_loads_the_same_values(self, pg_dsn, engine_factory):
        with engine_at(pg_url(pg_dsn, engine_factory)) as db:
            seed(db)
            db.execute(sa.delete(Wide.__table__))
            db.execute_many(sa.insert(Wide.__table__), wide_rows(2, 500))
            expected = db.fetch_all(sa.select(Wide).order_by(Wide.id))

            db.execute(sa.delete(Wide.__table__))
            db.copy_in(Wide.__table__, wide_rows(2, 500))
            got = db.fetch_all(sa.select(Wide).order_by(Wide.id))

            for mine, theirs in zip(got, expected, strict=True):
                for column in Wide.__table__.columns:
                    assert getattr(mine, column.key) == getattr(theirs, column.key), column.key
