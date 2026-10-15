"""Server errors arrive as the `sa.exc.DBAPIError` SQLAlchemy would have raised, with
the driver's own exception as `.orig`, so an `except sa.exc.IntegrityError` keeps
catching a write after it moves to rowform.
"""

from __future__ import annotations

from typing import Any

import pytest
import sqlalchemy as sa
from sync.conftest import Author

#: `(module, class)` each driver raises, keyed by `dialect.driver`.
UNIQUE_VIOLATION = {
    "pysqlite": ("sqlite3", "IntegrityError"),
    "asyncpg": ("asyncpg.exceptions", "UniqueViolationError"),
    "psycopg": ("psycopg.errors", "UniqueViolation"),
}

UNDEFINED_COLUMN = {
    "pysqlite": ("sqlite3", "OperationalError"),
    "asyncpg": ("asyncpg.exceptions", "UndefinedColumnError"),
    "psycopg": ("psycopg.errors", "UndefinedColumn"),
}

#: SQLAlchemy classes sqlite and postgres drivers map a missing column to.
WRONG_COLUMN_WRAPPER = {
    "pysqlite": sa.exc.OperationalError,
    "asyncpg": sa.exc.ProgrammingError,
    "psycopg": sa.exc.ProgrammingError,
}


def duplicate() -> Any:
    return sa.insert(Author.__table__).values(id=1, name="already-taken", active=True)


def assert_wrapped(caught, wrapper: type[sa.exc.DBAPIError], expected: tuple[str, str]) -> None:
    """`expected` is the driver's own class; asyncpg's sits one `__cause__` below `.orig`,
    which is SQLAlchemy's adapter class.
    """
    error = caught.value
    assert type(error) is wrapper
    root = error.orig
    while root.__cause__ is not None:
        root = root.__cause__
    assert (type(root).__module__, type(root).__name__) == expected


class TestUniqueViolation:
    def test_inside_a_scope_it_is_the_drivers_own(self, engine):
        with pytest.raises(Exception) as caught:
            with engine.begin() as conn:
                conn.execute(duplicate())
        assert_wrapped(caught, sa.exc.IntegrityError, UNIQUE_VIOLATION[engine.dialect.driver])

    def test_a_one_shot_raises_the_same_way(self, engine):
        """Write one-shots go through `sa_engine.begin()`, which could have wrapped it."""
        with pytest.raises(Exception) as caught:
            engine.execute(duplicate())
        assert_wrapped(caught, sa.exc.IntegrityError, UNIQUE_VIOLATION[engine.dialect.driver])

    def test_it_matches_what_sqlalchemy_raises_for_the_same_statement(self, engine):
        with pytest.raises(sa.exc.IntegrityError) as theirs:
            with engine.sa_engine.begin() as sa_conn:
                sa_conn.execute(duplicate())
        with pytest.raises(sa.exc.IntegrityError) as ours:
            engine.execute(duplicate())
        assert type(ours.value.orig) is type(theirs.value.orig)
        assert ours.value.statement == theirs.value.statement
        assert ours.value.code == theirs.value.code

    def test_execute_many_marks_the_failure_as_multi(self, engine):
        with pytest.raises(sa.exc.IntegrityError) as caught:
            with engine.begin() as conn:
                conn.execute_many(
                    sa.insert(Author.__table__), [{"id": 1, "name": "x", "active": True}]
                )
        assert caught.value.params is not None

    def test_a_driver_sql_error_is_wrapped_too(self, engine):
        with pytest.raises(sa.exc.DBAPIError):
            with engine.begin() as conn:
                conn.exec_driver_sql("SELECT no_such_column FROM t_authors")


class TestMalformedStatement:
    def test_a_read_of_a_column_that_does_not_exist(self, engine):
        missing = sa.select(sa.column("nope")).select_from(sa.table("t_authors"))
        with pytest.raises(Exception) as caught:
            engine.fetch_all(missing)
        assert_wrapped(caught, WRONG_COLUMN_WRAPPER[engine.dialect.driver], UNDEFINED_COLUMN[engine.dialect.driver])

    def test_the_engine_is_still_usable_afterwards(self, engine):
        """An aborted statement must not leave a poisoned connection in the pool."""
        for _ in range(3):
            with pytest.raises(sa.exc.DBAPIError):
                engine.execute(sa.text("SELECT no_such_column FROM t_authors"))
        assert len(engine.fetch_all(sa.select(Author))) == 4
