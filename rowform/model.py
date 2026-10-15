"""Declaration: one class is both the SQLAlchemy `Table` and the row container.

`User.__table__` is a real `sa.Table`; `sa.select(User)` works through
`__clause_element__` on the metaclass; `User.id` is a `Column` on the class
and a plain attribute on an instance. Instances are stdlib dataclasses.

A metaclass rather than a decorator because a decorator *factory* (needed to
take `metadata`) erases field types to `Any` under `dataclass_transform`. The
cost is a metaclass conflict with `ABC` and `Protocol`, accepted.
"""

from __future__ import annotations

import dataclasses
import datetime
import decimal
import enum
import functools
import operator
import types
import typing
import uuid
from typing import (
    Any,
    ClassVar,
    TypeVar,
    dataclass_transform,
    get_args,
    get_origin,
    get_type_hints,
)

import sqlalchemy as sa
from sqlalchemy.orm import Mapped
from sqlalchemy.sql.operators import ColumnOperators

from .errors import DeclarationError

# Where the model class is recorded on its `Table`, for `planner.py`.
MODEL_KEY = "rowform_model"

# The same record on a subquery/CTE from `alias(of=...)`, which has no `.info`.
MODEL_ATTR = "_rowform_model"

_M = TypeVar("_M")

# Marks a finished class; `dataclasses`' slots rebuild re-enters the metaclass
# with it present, and must not build twice.
_BUILT = "__rowform_built__"

_RESERVED = frozenset(
    {"metadata", "registry", "type_annotation_map", "__table__", "__tablename__"}
)

#: `Mapped[<key>]` -> column type; extend with `type_annotation_map` on your Base.
DEFAULT_TYPE_MAP: dict[Any, sa.types.TypeEngine[Any]] = {
    bool: sa.Boolean(),
    int: sa.Integer(),
    float: sa.Float(),
    str: sa.String(),
    bytes: sa.LargeBinary(),
    datetime.datetime: sa.DateTime(),
    datetime.date: sa.Date(),
    datetime.time: sa.Time(),
    datetime.timedelta: sa.Interval(),
    decimal.Decimal: sa.Numeric(),
    uuid.UUID: sa.Uuid(),
    dict: sa.JSON(),
    list: sa.JSON(),
}


class _MappedColumn(ColumnOperators):
    """Marker left in the class body by `mapped_column()`. It must not survive class
    creation: `dataclasses` probes `getattr(cls, name)` for a default, and a
    marker still on the class would become every field's default.

    Inside the class body it stands for its column, so `__table_args__` can name it
    (`UniqueConstraint(username)`, `CheckConstraint(sent_at.is_not(None))`) as with
    the ORM's `mapped_column()`. The column is built here and given its name, type
    and nullability once the annotations are resolved.
    """

    __slots__ = ("claimed", "column", "default", "default_factory", "init", "kwargs")

    def __init__(self, args, kwargs, default, default_factory, init):
        self.column: sa.Column[Any] = sa.Column(*args, **kwargs)
        self.kwargs = kwargs
        self.default = default
        self.default_factory = default_factory
        self.init = init
        self.claimed = False

    # `ColumnOperators` defines `__eq__`; a marker is still a dict key in `ns`.
    __hash__ = object.__hash__

    def __clause_element__(self) -> Any:
        return self.column

    def operate(self, op: Any, *other: Any, **kwargs: Any) -> Any:
        return op(self.column, *other, **kwargs)

    def reverse_operate(self, op: Any, other: Any, **kwargs: Any) -> Any:
        return op(other, self.column, **kwargs)


def mapped_column(
    *args: Any,
    default: Any = dataclasses.MISSING,
    default_factory: Any = dataclasses.MISSING,
    init: bool = True,
    **kwargs: Any,
) -> Any:
    """Per-column overrides; everything not named here goes straight to `sa.Column`.
    A leading string renames the column, a `TypeEngine` overrides the annotation.
    `default`/`default_factory`/`init` configure the dataclass `__init__`; `default`
    also reaches `sa.Column`. Returns `Any` so the assignment typechecks.
    """
    if default is not dataclasses.MISSING and "default" not in kwargs:
        kwargs["default"] = default
    return _MappedColumn(args, kwargs, default, default_factory, init)


def _is_union(annotation: Any) -> bool:
    origin = get_origin(annotation)
    return origin is typing.Union or origin is types.UnionType


def _unwrap_optional(annotation: Any) -> tuple[Any, bool]:
    """`Mapped[str | None]` -> (str, nullable=True); `Mapped[A | B]` keeps the union,
    which needs its column type named."""
    if _is_union(annotation):
        args = get_args(annotation)
        present = [a for a in args if a is not type(None)]
        nullable = len(present) != len(args)
        if len(present) == 1:
            return present[0], nullable
        return functools.reduce(operator.or_, present), nullable
    return annotation, False


def _sa_type(py_type: Any, type_map: dict[Any, Any]) -> sa.types.TypeEngine[Any]:
    if _is_union(py_type):
        raise DeclarationError(
            f"Mapped[{py_type}] is a union of more than one non-None type; "
            f"rowform maps one Python type to one column. Name the column type "
            f"explicitly with `mapped_column(sa.SomeType())`."
        )
    if isinstance(py_type, type) and issubclass(py_type, enum.Enum):
        return sa.Enum(py_type)
    if get_origin(py_type) is typing.Literal and all(isinstance(a, str) for a in get_args(py_type)):
        # SQLAlchemy's mapping: the values as a VARCHAR-backed, non-native enum.
        return sa.Enum(*get_args(py_type), native_enum=False)
    for candidate in (py_type, get_origin(py_type)):
        if candidate is None:
            continue
        try:
            found = type_map.get(candidate)
        except TypeError:  # unhashable annotation
            continue
        if found is not None:
            return found
    if isinstance(py_type, typing.NewType):
        # SQLAlchemy's fallback: an unmapped NewType resolves as its root supertype.
        root = py_type.__supertype__
        while isinstance(root, typing.NewType):
            root = root.__supertype__
        return _sa_type(root, type_map)
    raise DeclarationError(
        f"no SQLAlchemy type registered for {py_type!r}. Add it to your Base's "
        f"`type_annotation_map`, or name the type explicitly with "
        f"`mapped_column(sa.SomeType())`."
    )


@dataclass_transform(field_specifiers=(mapped_column,))
class ModelMeta(type):
    """Builds the `Table` and the dataclass from one set of `Mapped[]` annotations.
    Class keyword arguments go to `dataclasses.dataclass`.
    """

    def __new__(mcls, name, bases, ns, **dc_kwargs):
        # A slots rebuild re-enters here; let it through or the build runs twice.
        if _BUILT in ns:
            return super().__new__(mcls, name, bases, ns)

        # The root `Base` has no ModelMeta ancestor and declares no fields.
        if not any(isinstance(b, ModelMeta) for b in bases):
            return super().__new__(mcls, name, bases, ns)

        # Only to resolve string annotations against the real MRO.
        probe = super().__new__(mcls, name, bases, dict(ns))
        specs = _collect_specs(probe, bases, ns)
        selectable = ns.get("__table__")
        if selectable is None:
            fields = _build_fields(probe, specs)
        else:
            fields = _fields_over(name, specs, selectable)

        abstract = selectable is None and (
            ns.get("__abstract__", False) or "__tablename__" not in ns
        )
        if not fields:
            if not abstract:
                raise DeclarationError(
                    f"{name} declares __tablename__ but no Mapped[] fields, so it "
                    f"would build a table with no columns"
                )
            # A user's own `Base`: not a dataclass, or `frozen=True` models could not
            # inherit from it; `__slots__ = ()` so a `slots=True` model is fully slotted.
            slotted = dict(ns)
            slotted.setdefault("__slots__", ())
            return super().__new__(mcls, name, bases, slotted)

        namespace: dict[str, Any] = {
            key: value
            for key, value in ns.items()
            if key not in fields and key not in ("__dict__", "__weakref__")
        }
        namespace["__annotations__"] = {n: f.py_type for n, f in fields.items()}
        for field_name, field in fields.items():
            declared = field.dataclass_field()
            if declared is not None:
                namespace[field_name] = declared
        namespace["__rowform_specs__"] = specs
        namespace[_BUILT] = True

        built: Any = super().__new__(mcls, name, bases, namespace)
        try:
            built = dataclasses.dataclass(**dc_kwargs)(built)
        except TypeError as err:
            if "follows default argument" not in str(err):
                raise
            # Inherited fields sort ahead of own fields; the stdlib message would
            # name an order the author never wrote.
            raise DeclarationError(
                f"{name}: {err}. Fields inherited from a base or mixin come before "
                f"this class's own fields ({', '.join(fields)}), so a base field "
                f"with a default blocks every field declared after it. Declare "
                f"`class {name}(..., kw_only=True)`, or give the later fields "
                f"defaults too."
            ) from err

        columns = {n: f.column for n, f in fields.items()}
        if selectable is not None:
            setattr(selectable, MODEL_ATTR, built)
        elif not abstract:
            table = sa.Table(
                ns["__tablename__"],
                built.metadata,
                *columns.values(),
                *ns.get("__table_args__", ()),
            )
            table.info[MODEL_KEY] = built
            built.__table__ = table

        # Set last: while absent, `__getattribute__` delegates everything, so
        # `dataclasses`' default probe above never sees a Column.
        built.__columns__ = columns
        built.__column_order__ = tuple(columns)
        return built

    # Runtime only: to the checker `User.id` already resolves through `Mapped.__get__`,
    # and a `__getattribute__ -> Any` would make `User.typo` valid.
    if not typing.TYPE_CHECKING:

        def __getattribute__(cls, key: str) -> Any:
            """`User.id` -> `sa.Column`; instance reads never come through here.

            Gated on this class's **own** `__columns__`, set only once `__new__` has
            finished: an inherited one would be visible while a subclass is still being
            built, and `dataclasses`' default probe would take the base's `Column` as the
            default of every inherited field.
            """
            columns = type.__getattribute__(cls, "__dict__").get("__columns__")
            if columns is not None and key in columns:
                return columns[key]
            return type.__getattribute__(cls, key)

    def __clause_element__(cls) -> Any:
        """What lets `sa.select(User)` and `.join(User)` treat the class as its `Table`."""
        try:
            return type.__getattribute__(cls, "__table__")
        except AttributeError:
            raise DeclarationError(
                f"{cls.__name__} is abstract (no __tablename__), so it has no table "
                f"to select from"
            ) from None


class _Spec:
    """A declared field, resolved but not yet an `sa.Column`, so a subclass can
    inherit the declaration: a `Column` belongs to exactly one `Table`.
    """

    __slots__ = ("column", "marker", "nullable", "py_type")

    def __init__(self, py_type, nullable, marker):
        self.py_type = py_type
        self.nullable = nullable
        self.marker = marker
        #: The column built for the declaring class; inheritors get a `_copy()`.
        self.column: sa.Column[Any] | None = None


class _Field:
    __slots__ = ("column", "default", "default_factory", "init", "py_type")

    def __init__(self, py_type, column, default, default_factory, init):
        self.py_type = py_type
        self.column = column
        self.default = default
        self.default_factory = default_factory
        self.init = init

    def dataclass_field(self):
        """The value to leave in the namespace, or None so `dataclasses` makes the field required."""
        if (
            self.default is dataclasses.MISSING
            and self.default_factory is dataclasses.MISSING
            and self.init
        ):
            return None
        kwargs: dict[str, Any] = {"init": self.init}
        if self.default is not dataclasses.MISSING:
            kwargs["default"] = self.default
        if self.default_factory is not dataclasses.MISSING:
            kwargs["default_factory"] = self.default_factory
        return dataclasses.field(**kwargs)


def _collect_specs(cls: type, bases: tuple[type, ...], ns: dict[str, Any]) -> dict[str, _Spec]:
    """Inherited declarations first (reverse MRO), then this class's own.

    Inherited-first means adding a mixin moves its columns to the front of
    `CREATE TABLE`, which Alembic does not diff; pin with `__column_order__` on a
    table that already exists. Hydration is planned from the statement, so it is
    unaffected.
    """
    specs: dict[str, _Spec] = {}
    for base in reversed(cls.__mro__[1:]):
        specs.update(getattr(base, "__rowform_specs__", None) or {})

    own_names = ns.get("__annotations__", {})
    hints = get_type_hints(cls, include_extras=True)
    for field_name in own_names:
        hint = hints.get(field_name)
        if get_origin(hint) is not Mapped:
            continue
        if field_name in _RESERVED:
            raise DeclarationError(
                f"{cls.__name__}.{field_name} collides with a reserved name "
                f"({', '.join(sorted(_RESERVED))})"
            )
        py_type, nullable = _unwrap_optional(get_args(hint)[0])
        marker = ns.get(field_name)
        if not isinstance(marker, _MappedColumn):
            marker = _MappedColumn((), {}, dataclasses.MISSING, dataclasses.MISSING, True)
        specs[field_name] = _Spec(py_type, nullable, marker)

    pinned = ns.get("__column_order__")
    if pinned is not None:
        if set(pinned) != set(specs):
            raise DeclarationError(
                f"{cls.__name__}.__column_order__ must list every Mapped[] field "
                f"exactly once; missing {sorted(set(specs) - set(pinned))}, "
                f"unknown {sorted(set(pinned) - set(specs))}"
            )
        specs = {n: specs[n] for n in pinned}
    return specs


def _build_fields(cls: type, specs: dict[str, _Spec]) -> dict[str, _Field]:
    """One `sa.Column` per declared field: the marker's own for the class that
    declares it, a `_copy()` for every class inheriting it, since a `Column` (and
    its `ForeignKey`) belongs to one `Table`.
    """
    type_map = {**DEFAULT_TYPE_MAP, **getattr(cls, "type_annotation_map", {})}
    fields: dict[str, _Field] = {}
    for field_name, spec in specs.items():
        marker = spec.marker
        if spec.column is not None:
            column = spec.column._copy()
        elif marker.claimed:
            # One `mapped_column()` object assigned in two class bodies.
            column = _finish(marker.column._copy(), field_name, spec, type_map)
        else:
            marker.claimed = True
            column = spec.column = _finish(marker.column, field_name, spec, type_map)
        fields[field_name] = _Field(
            spec.py_type | None if spec.nullable else spec.py_type,
            column,
            marker.default,
            marker.default_factory,
            marker.init,
        )
    return fields


def _fields_over(name: str, specs: dict[str, _Spec], selectable: Any) -> dict[str, _Field]:
    """Fields over the columns of `__table__`, a view or subquery declared elsewhere.
    It owns the columns, so every one must be declared (a `select()` of it expands to
    all of them), and `mapped_column()` may only rename or set dataclass options.
    """
    if not isinstance(selectable, sa.FromClause):
        raise DeclarationError(
            f"{name}.__table__ must be a FromClause (a view's table clause, a "
            f"subquery, a CTE), not {type(selectable).__name__}"
        )
    available = selectable.columns
    fields: dict[str, _Field] = {}
    for field_name, spec in specs.items():
        marker = spec.marker
        own = marker.column
        if set(marker.kwargs) - {"default"} or not own.type._isnull or own.foreign_keys:
            raise DeclarationError(
                f"{name}.{field_name}: a model over a selectable takes its columns from "
                f"__table__; mapped_column() may only rename the field or set "
                f"default/default_factory/init"
            )
        key = own.name or field_name
        column = available.get(key)
        if column is None:
            raise DeclarationError(
                f"{name}.{field_name}: __table__ has no column {key!r}; it has "
                f"{list(available.keys())}"
            )
        fields[field_name] = _Field(
            spec.py_type | None if spec.nullable else spec.py_type,
            column,
            marker.default,
            marker.default_factory,
            marker.init,
        )
    declared = {field.column.key for field in fields.values()}
    undeclared = [column.key for column in available if column.key not in declared]
    if undeclared:
        raise DeclarationError(
            f"{name} does not declare {undeclared} of its __table__; a select() of it "
            f"returns every column, so each needs a field"
        )
    return fields


def _finish(
    column: sa.Column[Any], field_name: str, spec: _Spec, type_map: dict[Any, Any]
) -> sa.Column[Any]:
    """Name, type and nullability from the annotation, where `mapped_column()` gave none.
    The key is the field name even when the SQL name differs, as in SQLAlchemy.
    """
    if column.name is None:
        column.name = field_name
    if "key" not in spec.marker.kwargs:
        column.key = field_name
    if column.type._isnull:
        column._set_type(sa.types.to_instance(_sa_type(spec.py_type, type_map)))
    if column.primary_key:
        column.nullable = False
    elif "nullable" not in spec.marker.kwargs:
        column.nullable = spec.nullable
    # The class body may already have compared against the untyped column; its
    # memoized comparator would bind later values without the resolved type.
    column._reset_memoizations()
    return column


class Base(metaclass=ModelMeta):
    """Subclass this to make your own base, then declare models against it.
    `metadata` is what Alembic's `target_metadata` points at; a subclass declaring
    its own `metadata = sa.MetaData()` gets a separate schema.
    """

    __slots__ = ()

    metadata = sa.MetaData()

    #: Extends `DEFAULT_TYPE_MAP` for models under this base.
    type_annotation_map: ClassVar[dict[Any, sa.types.TypeEngine[Any]]] = {}

    if typing.TYPE_CHECKING:
        # ClassVar is load-bearing: a bare annotation would become a dataclass field.
        __table__: ClassVar[sa.Table]
        __tablename__: ClassVar[str]
        __columns__: ClassVar[dict[str, sa.Column[Any]]]
        __column_order__: ClassVar[tuple[str, ...]]


def model_for(from_clause: Any) -> type[Any] | None:
    """The model class a `FromClause` yields rows of, if any: from `Table.info`, the
    mark `alias(of=...)` leaves, or the aliased element.
    """
    info = getattr(from_clause, "info", None)
    if isinstance(info, dict) and MODEL_KEY in info:
        return info[MODEL_KEY]
    marked = getattr(from_clause, MODEL_ATTR, None)
    if marked is not None:
        return marked
    element = getattr(from_clause, "element", None)
    if element is not None and element is not from_clause:
        return model_for(element)
    return None


class _Alias:
    """Runtime half of `alias()`: resolves field names (not `.c` names, which differ
    when a column was renamed) against the from clause.
    """

    __slots__ = ("_columns", "_from", "_model")

    def __init__(self, model: type[Any], from_clause: Any):
        self._model = model
        self._from = from_clause
        declared = type.__getattribute__(model, "__columns__")
        self._columns = {name: from_clause.columns[col.key] for name, col in declared.items()}

    def __clause_element__(self) -> Any:
        return self._from

    def __getattr__(self, key: str) -> Any:
        try:
            return self._columns[key]
        except KeyError:
            raise AttributeError(
                f"{self._model.__name__} has no column {key!r}; an alias exposes "
                f"that model's fields and nothing else"
            ) from None

    def __repr__(self) -> str:
        name = getattr(self._from, "name", None) or "<unnamed>"
        return f"<alias {self._model.__name__} AS {name}>"


def alias(model: type[_M], name: str | None = None, *, of: Any = None) -> type[_M]:
    """A second reference to a model's rows: another alias of its table, or a
    subquery/CTE that yields them.

    `sa.orm.aliased()` cannot serve: it looks for a `Mapper`. Declared as returning
    `type[_M]` so `mgr.name` and `select(User, mgr)` infer as the model does — the
    same type-level fiction as `User.id`. `of=` records the model on the from
    clause itself, not a wrapper, so `of.c.id` and the alias's `.id` stay one column.
    """
    if of is None:
        try:
            table = type.__getattribute__(model, "__table__")
        except AttributeError:
            raise DeclarationError(
                f"{model.__name__} is abstract (no __tablename__), so it has no table to alias"
            ) from None
        source = table.alias(name)
    else:
        if name is not None:
            raise DeclarationError(
            "pass a name to .subquery()/.cte() itself, not to alias(of=...)"
        )
        source = of
        _require_exact_columns(model, source)
        setattr(source, MODEL_ATTR, model)
    return typing.cast("type[_M]", _Alias(model, source))


def _require_exact_columns(model: type[Any], from_clause: Any) -> None:
    """`of=` demands the model's columns, in order, and nothing else: `select(alias)`
    expands to every column of the from clause, so an extra or reordered one would
    change the rows without changing the type.
    """
    if not isinstance(from_clause, sa.FromClause):
        raise DeclarationError(
            f"alias(of=...) needs a FromClause — a subquery, CTE, alias or table — "
            f"not {type(from_clause).__name__}. A Select becomes one with "
            f".subquery() or .cte()."
        )

    declared = type.__getattribute__(model, "__columns__")
    want = [col.key for col in declared.values()]
    got = list(from_clause.columns.keys())
    if got != want:
        raise DeclarationError(
            f"alias({model.__name__}, of=...) needs exactly that model's columns, "
            f"in order: expected {want}, got {got}. `select()` on a from clause "
            f"expands to all of its columns, so an extra or reordered one would "
            f"change the rows without changing the type. Narrow the subquery to "
            f"these columns — filter on the extras inside it."
        )
