"""Compile a statement once with SQLAlchemy, then run it on the raw driver.

A `CoreQuery` holds the compiled SQL, the recipe for turning keyword arguments
into the driver's parameter shape, and (after the first execute) the generated
hydrator. `fetch_all` builds and caches one per structural cache key.
"""

from __future__ import annotations

import logging
from typing import Any, Generic, TypeVar

from sqlalchemy import Select
from sqlalchemy.engine.result import SimpleResultMetaData
from sqlalchemy.sql import compiler

from .compile import compile_hydrator
from .errors import PlanError, UnsupportedError
from .planner import Plan, plan

_LOG = logging.getLogger("rowform")

R = TypeVar("R")


class CoreQuery(Generic[R]):
    """One statement, compiled for one dialect."""

    __slots__ = (
        "_compiled",
        "_expanding",
        "_hydrate",
        "_keys",
        "_metadata",
        "_plan",
        "_positional",
        "_prefetch",
        "dialect",
        "is_select",
        "sql",
    )

    def __init__(self, statement: Any, dialect: Any):
        #: Kept so an engine can refuse a `CoreQuery` compiled for another driver.
        self.dialect = dialect
        # Compiled *with* the cache key, or `bind()` could not substitute another
        # statement's literals (`extracted_parameters`).
        cache_key = statement._generate_cache_key()
        # The engine's `enable_from_linting`, as Core's execute applies it: a
        # cartesian product warns once per compiled shape.
        linting = dialect.compiler_linting | compiler.WARN_LINTING
        compiled = self._compiled = (
            statement.compile(dialect=dialect, cache_key=cache_key, linting=linting)
            if cache_key is not None
            else statement.compile(dialect=dialect, linting=linting)
        )
        # For an expanding statement this holds a POSTCOMPILE placeholder; `bind()`
        # returns the executable string.
        self.sql: str = compiled.string
        self._positional: bool = dialect.positional
        self._keys: tuple[str, ...] = tuple(compiled.positiontup or ())
        self._expanding: bool = bool(
            compiled.post_compile_params or compiled.literal_execute_params
        )
        self._prefetch = _prefetch_defaults(compiled)
        self._plan: Plan | None = plan(statement) if _returns_rows(statement) else None
        self._hydrate: Any = None
        self._metadata: Any = None
        #: A SELECT, as opposed to a write with RETURNING; decides commit and streaming.
        self.is_select: bool = bool(getattr(statement, "is_select", False))
        _LOG.debug("compiled: %s", self.sql)

    @property
    def returns_rows(self) -> bool:
        return self._plan is not None

    @property
    def result_metadata(self) -> Any:
        """Column labels for `execute()`'s `Result`, built once per statement.
        `SimpleResultMetaData` is SQLAlchemy-private.
        """
        metadata = self._metadata
        if metadata is None:
            from .result import keys_for

            if self._plan is None:
                raise PlanError("this statement returns no rows; it has no result metadata")
            metadata = self._metadata = SimpleResultMetaData(
                keys_for(self._plan, self._compiled)
            )
        return metadata

    @property
    def entities(self) -> Plan | None:
        """What the rows hydrate into; None for a write without RETURNING."""
        return self._plan

    def bind(
        self, params: dict[str, Any] | None = None, extracted: Any = None
    ) -> tuple[str, Any]:
        """Keyword arguments -> `(sql, parameters)` in the driver's own shape.

        `extracted` carries the *calling* statement's literals and is required for a
        query that came out of the cache: statements differing only in literals share
        one compiled object, so without it `values(id=51)` would silently run with the
        first statement's `50`. Values go through SQLAlchemy's bind processors, an
        expanding `IN` is rewritten into the SQL string, and the shape (tuple or dict)
        follows the dialect's paramstyle. Positional order always comes from
        `positiontup`, never from the caller.
        """
        compiled = self._compiled
        values = compiled.construct_params(
            params or None, extracted_parameters=extracted, escape_names=False
        )
        if self._prefetch:
            _fill_defaults(self._prefetch, values, params or {})

        if self._expanding:
            state = compiled._process_parameters_for_postcompile(values)
            return state.statement, self._shape(
                state.parameters,
                {**compiled._bind_processors, **state.processors},
                tuple(state.positiontup or ()),
            )

        return self.sql, self._shape(values, compiled._bind_processors, self._keys)

    def _shape(self, values: dict[str, Any], processors: Any, keys: tuple[str, ...]) -> Any:
        if self._positional:
            return tuple(
                processors[key](values[key]) if key in processors else values[key]
                for key in keys
            )
        escaped = self._compiled.escaped_bind_names
        return {
            escaped.get(key, key) if escaped else key: (
                processors[key](value) if key in processors else value
            )
            for key, value in values.items()
        }

    def hydrator(self, dialect: Any, description: Any) -> Any:
        """The generated `rows -> list` function, built on first use and cached: the
        result processors need the DBAPI type codes the driver reports.
        """
        hydrate = self._hydrate
        if hydrate is None:
            assert self._plan is not None
            hydrate = self._hydrate = compile_hydrator(
                self._plan, dialect, [column[1] for column in description]
            )
        return hydrate

    def __repr__(self) -> str:
        return f"<CoreQuery {self.sql!r} {self._plan!r}>"


def _prefetch_defaults(compiled: Any) -> tuple[Any, ...]:
    """The Python-side `default=`/`onupdate=` columns Core's execution context fills
    at execute time: `(column, bind key, (arg, is_scalar, is_callable, is_sentinel))`.
    The compiler lists them; the description tuples and key getter are private.
    """
    if compiled.insert_prefetch:
        columns, description = compiled.insert_prefetch, "_default_description_tuple"
    elif compiled.update_prefetch:
        columns, description = compiled.update_prefetch, "_onupdate_description_tuple"
    else:
        return ()
    key_of = compiled._within_exec_param_key_getter
    return tuple((column, key_of(column), getattr(column, description)) for column in columns)


class _DefaultContext:
    """What a context-sensitive default (`default=lambda ctx: ...`) may read."""

    __slots__ = ("current_column", "current_parameters")

    def __init__(self, parameters: dict[str, Any]):
        self.current_parameters = parameters
        self.current_column: Any = None

    def get_current_parameters(self, isolate_multiinsert_groups: bool = True) -> dict[str, Any]:
        return self.current_parameters


def _fill_defaults(prefetch: tuple[Any, ...], values: dict[str, Any], given: dict[str, Any]) -> None:
    """Fill each defaulted column the caller left out, as Core's execution context
    does. Compiled once without the caller's keys, an `insert(table)` lists every
    defaulted column, so a supplied value must win.
    """
    context = _DefaultContext(values)
    for column, key, (arg, is_scalar, is_callable, _sentinel) in prefetch:
        if key in given:
            continue
        if is_scalar:
            values[key] = arg
        elif is_callable:
            context.current_column = column
            values[key] = arg(context)
        else:
            raise UnsupportedError(
                f"{column} has a default rowform cannot fill client-side ({arg!r}); "
                f"give the column a value, or use a server_default"
            )


def _returns_rows(statement: Any) -> bool:
    """A SELECT, or a write with RETURNING."""
    return bool(getattr(statement, "is_select", False) or getattr(statement, "_returning", None))


def _one_row(statement: Any) -> Any:
    """`statement` narrowed to `LIMIT 1` where that is safe: a `Select` with no
    limit of its own (a caller's limit may be a bind parameter; a `CoreQuery` is
    already compiled). `_limit_clause` is SQLAlchemy-private.
    """
    if isinstance(statement, Select) and statement._limit_clause is None:
        return statement.limit(1)
    return statement
