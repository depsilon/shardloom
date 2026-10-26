# SPDX-License-Identifier: Apache-2.0
"""Typed test intent shared by renderers, independent expectations and coverage.

This is a finite benchmark declaration, not runtime IR or an execution provider.
It validates generated compositions before submitting them to public APIs.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, replace
import hashlib
import json
import re
from typing import Any

from shardloom._result_schema import ResultType
from .datasets import SCHEMAS, canonical
from .semantics import ResultContract, Schema, schema_record


SURFACES = ("cli_sql", "embedded_workflow", "python_sql", "dataframe", "typed_relation")
FAMILIES = ("projection", "predicates", "nullable", "numeric", "strings", "temporal",
            "global_aggregation", "grouped_aggregation", "distinct", "joins", "multiway_joins",
            "ordering", "windows", "subqueries", "cte_composition", "sets", "nested",
            "composite", "input_output", "lifecycle", "resources")
NUMERIC = {"int8", "int16", "int32", "int64", "uint8", "uint16", "uint32", "uint64",
           "float32", "float64", "decimal128"}
IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z_0-9]*\Z")


def identifier(value: str) -> str:
    if not isinstance(value, str) or not IDENTIFIER.fullmatch(value):
        raise ValueError(f"invalid corpus identifier: {value!r}")
    return value


@dataclass(frozen=True)
class Expression:
    op: str
    dtype: ResultType
    args: tuple[Expression, ...] = ()
    value: Any = None

    @classmethod
    def column(cls, name: str, schema: Schema):
        identifier(name)
        return cls("column", dict(schema)[name], value=name)

    @classmethod
    def literal(cls, value, dtype=None):
        if dtype is None:
            name = {int: "int64", float: "float64", str: "utf8", bool: "bool", type(None): "null"}.get(type(value))
            if name is None:
                raise ValueError("rich literals require an explicit dtype")
            dtype = ResultType(name, value is None)
        return cls("literal", dtype, value=value)

    def binary(self, op, other):
        if not isinstance(other, Expression):
            other = Expression.literal(other)
        nullable = self.dtype.nullable or other.dtype.nullable
        if op in {"eq", "ne", "lt", "le", "gt", "ge"}:
            if (self.dtype.name != other.dtype.name and
                    not {self.dtype.name, other.dtype.name} <= NUMERIC):
                raise ValueError("comparison types are incompatible")
            dtype = ResultType("bool", nullable)
        elif op in {"and", "or"}:
            if self.dtype.name != "bool" or other.dtype.name != "bool":
                raise ValueError("boolean operators require boolean arguments")
            dtype = ResultType("bool", nullable)
        elif op in {"add", "sub", "mul", "div", "mod"}:
            if self.dtype.name not in NUMERIC or other.dtype.name not in NUMERIC:
                raise ValueError("arithmetic requires numeric arguments")
            # Mixed/decimal arithmetic needs an explicitly reviewed result-domain
            # rule. Do not guess that every coercion is legal merely to add cases.
            if self.dtype.name != other.dtype.name:
                raise ValueError("mixed numeric arithmetic requires explicit casts")
            dtype = replace(self.dtype, nullable=nullable)
        else:
            raise ValueError("unknown binary operator")
        return Expression(op, dtype, (self, other))

    def unary(self, op):
        if op in {"is_null", "is_not_null"}:
            dtype = ResultType("bool", False)
        elif op == "not" and self.dtype.name == "bool":
            dtype = self.dtype
        elif op == "neg" and self.dtype.name in NUMERIC:
            dtype = self.dtype
        else:
            raise ValueError("invalid unary operator or argument")
        return Expression(op, dtype, (self,))

    def cast(self, dtype, *, tolerant=False):
        return Expression("try_cast" if tolerant else "cast", dtype, (self,))

    @classmethod
    def call(cls, name, args, dtype):
        identifier(name)
        if not args or any(not isinstance(arg, cls) for arg in args):
            raise ValueError("scalar calls require typed expressions")
        return cls("call", dtype, tuple(args), name)

    def columns(self):
        return {self.value} if self.op == "column" else set().union(*(arg.columns() for arg in self.args))


@dataclass(frozen=True)
class Measure:
    function: str
    argument: Expression | None
    alias: str
    dtype: ResultType

    def __post_init__(self):
        identifier(self.function)
        identifier(self.alias)
        if self.argument is None and self.function != "count":
            raise ValueError("only count accepts a star argument")


@dataclass(frozen=True)
class Order:
    column: str
    descending: bool = False
    nulls: str = "last"

    def __post_init__(self):
        identifier(self.column)
        if self.nulls not in {"first", "last"}:
            raise ValueError("ordering must specify null placement")


@dataclass(frozen=True)
class Relation:
    op: str
    schema: Schema
    inputs: tuple[Relation, ...] = ()
    args: tuple = ()

    def __post_init__(self):
        ResultContract(self.schema)

    @classmethod
    def scan(cls, table):
        return cls("scan", SCHEMAS[table], args=(table,))

    def col(self, name):
        return Expression.column(name, self.schema)

    def check_expression(self, expression):
        if expression.columns() - set(dict(self.schema)):
            raise ValueError("expression references an absent column")
        for name in expression.columns():
            def check(node):
                if node.op == "column" and node.value == name and node.dtype != dict(self.schema)[name]:
                    raise ValueError("expression column type differs from its input")
                for child in node.args:
                    check(child)
            check(expression)

    def project(self, *items):
        for name, expression in items:
            identifier(name)
            self.check_expression(expression)
        return Relation("project", tuple((name, expression.dtype) for name, expression in items), (self,), tuple(items))

    def select(self, *names):
        return self.project(*((name, self.col(name)) for name in names))

    def where(self, predicate):
        self.check_expression(predicate)
        if predicate.dtype.name != "bool":
            raise ValueError("filter predicate must be boolean")
        return Relation("filter", self.schema, (self,), (predicate,))

    def aggregate(self, keys, measures):
        for key in keys:
            self.col(key)
        for measure in measures:
            if measure.argument is not None:
                self.check_expression(measure.argument)
        return Relation("aggregate", tuple((key, dict(self.schema)[key]) for key in keys)
                        + tuple((m.alias, m.dtype) for m in measures), (self,), (tuple(keys), tuple(measures)))

    def order(self, *keys):
        for key in keys:
            self.col(key.column)
        return Relation("sort", self.schema, (self,), tuple(keys))

    def take(self, count, offset=0):
        if any(type(value) is not int or value < 0 for value in (count, offset)):
            raise ValueError("range arguments must be nonnegative integers")
        return Relation("limit", self.schema, (self,), (count, offset))

    def distinct(self):
        return Relation("distinct", self.schema, (self,))

    def set(self, other, kind):
        if kind not in {"union_all", "union_distinct", "intersect", "except"}:
            raise ValueError("unknown set operation")
        if self.schema != other.schema:
            raise ValueError("set types/names require explicit alignment")
        return Relation("set", self.schema, (self, other), (kind,))

    def join(self, other, keys, columns, kind="inner"):
        if kind not in {"inner", "left", "right", "full", "semi", "anti"}:
            raise ValueError("unknown join kind")
        for left, right in keys:
            if self.col(left).dtype.name != other.col(right).dtype.name:
                raise ValueError("join key types require explicit alignment")
        schema = []
        for side, source, alias in columns:
            if side not in {"left", "right"} or (kind in {"semi", "anti"} and side != "left"):
                raise ValueError("invalid projected join side")
            dtype = (self if side == "left" else other).col(source).dtype
            if kind == "full" or (side == "right" and kind == "left") or (side == "left" and kind == "right"):
                dtype = replace(dtype, nullable=True)
            schema.append((identifier(alias), dtype))
        return Relation("join", tuple(schema), (self, other), (tuple(keys), tuple(columns), kind))

    def sources(self):
        return {self.args[0]} if self.op == "scan" else set().union(*(item.sources() for item in self.inputs))

    def record(self):
        return canonical(asdict(self))


@dataclass(frozen=True)
class Case:
    id: str
    family: str
    archetype: str
    relation: Relation | None
    contract: ResultContract | None
    factors: tuple[tuple[str, str], ...]
    partition: str = "development"
    neighbor_of: str | None = None
    desired_sql: str | None = None
    pending_reason: str | None = None

    def __post_init__(self):
        identifier(self.id)
        identifier(self.archetype)
        if self.family not in FAMILIES or self.partition not in {"development", "held_out"}:
            raise ValueError("invalid coverage family or partition")
        if len(dict(self.factors)) != len(self.factors):
            raise ValueError("coverage factor names must be unique")
        if self.relation is None:
            if self.desired_sql is None or not self.pending_reason or self.contract is not None:
                raise ValueError("unimplemented cases need an exact proposed request and visible reason")
        elif self.contract is None or self.relation.schema != self.contract.schema or self.pending_reason:
            raise ValueError("executable cases require their frozen result contract")

    def record(self):
        value = {"id": self.id, "family": self.family, "archetype": self.archetype,
                 "factors": dict(self.factors), "partition": self.partition, "neighbor_of": self.neighbor_of,
                 "relation": self.relation.record() if self.relation else None,
                 "desired_sql": self.desired_sql, "pending_reason": self.pending_reason,
                 "surfaces": list(SURFACES)}
        if self.contract:
            value["contract"] = {"schema": schema_record(self.contract.schema), "ordered": self.contract.ordered,
                                 "floats": {name: asdict(policy) for name, policy in self.contract.floats.items()},
                                 "max_matching_edges": self.contract.max_matching_edges}
        return value

    @property
    def identity(self):
        return hashlib.sha256(json.dumps(self.record(), sort_keys=True, allow_nan=False).encode()).hexdigest()
