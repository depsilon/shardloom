# SPDX-License-Identifier: Apache-2.0
"""Strict observable result contracts, independent of any execution adapter."""
from __future__ import annotations

from collections import Counter, defaultdict, deque
from dataclasses import dataclass, field
import datetime as dt
from decimal import Decimal
import math
from types import MappingProxyType
from typing import Any, Mapping, Sequence

from shardloom._result_schema import ResultType


Schema = tuple[tuple[str, ResultType], ...]


class ResultMismatch(AssertionError):
    """A complete result violates its frozen semantic contract."""


class ComparisonLimit(RuntimeError):
    """Comparison is inconclusive, never an engine pass or mismatch."""


@dataclass(frozen=True)
class FloatPolicy:
    absolute: float = 0.0
    relative: float = 0.0
    nan_equal: bool = False
    signed_zero_equal: bool = False
    reason: str = "exact observable values"

    def __post_init__(self):
        if any(type(x) not in (int, float) or not math.isfinite(x) or x < 0
               for x in (self.absolute, self.relative)):
            raise ValueError("floating tolerances must be finite and nonnegative")
        if type(self.nan_equal) is not bool or type(self.signed_zero_equal) is not bool:
            raise ValueError("floating special-value policies must be boolean")
        if not isinstance(self.reason, str) or not self.reason.strip():
            raise ValueError("a floating policy requires a semantic justification")


@dataclass(frozen=True)
class ResultContract:
    schema: Schema
    ordered: bool = False
    floats: Mapping[str, FloatPolicy] = field(default_factory=dict)
    # This is a comparator work limit, not a query result limit. Large exact
    # multisets have no such restriction. An ambiguous tolerant cohort must be
    # retried with an explicit deterministic order or a reviewed larger budget.
    max_matching_edges: int = 1_000_000

    def __post_init__(self):
        object.__setattr__(self, "schema", tuple(self.schema))
        object.__setattr__(self, "floats", MappingProxyType(dict(self.floats)))
        names = [name for name, _ in self.schema]
        if any(not isinstance(name, str) or not name for name in names) or len(set(names)) != len(names):
            raise ValueError("result fields must be nonempty and unique")
        if type(self.ordered) is not bool or any(not isinstance(p, FloatPolicy) for p in self.floats.values()):
            raise ValueError("result ordering and floating policies must be explicit")
        if set(self.floats) - set(names):
            raise ValueError("floating policy names an absent field")
        if any(dtype.name not in {"float16", "float32", "float64"}
               for name, dtype in self.schema if name in self.floats):
            raise ValueError("floating policy requires a floating field")
        if type(self.max_matching_edges) is not int or self.max_matching_edges < 1:
            raise ValueError("matching work budget must be positive")


@dataclass(frozen=True)
class TypedResult:
    schema: Schema
    rows: Sequence[Mapping[str, Any]]


@dataclass(frozen=True)
class ErrorContract:
    code: str
    stage: str

    def check(self, code: str, stage: str) -> None:
        if (code, stage) != (self.code, self.stage):
            raise ResultMismatch(f"expected error {(self.code, self.stage)!r}, got {(code, stage)!r}")


def schema_record(schema: Schema) -> list[dict]:
    def record(dtype):
        params = []
        for value in dtype.parameters:
            if isinstance(value, ResultType):
                params.append(record(value))
            elif isinstance(value, tuple) and len(value) == 2 and isinstance(value[1], ResultType):
                params.append({"name": value[0], "type": record(value[1])})
            else:
                params.append(value)
        return {"name": dtype.name, "nullable": dtype.nullable, "parameters": params}
    return [{"name": name, "type": record(dtype)} for name, dtype in schema]


def schema_from_record(fields) -> Schema:
    def dtype(value):
        if (not isinstance(value, dict) or set(value) != {"name", "nullable", "parameters"}
                or not isinstance(value["name"], str) or type(value["nullable"]) is not bool
                or not isinstance(value["parameters"], list)):
            raise ValueError("invalid recorded dtype")
        parameters = value["parameters"]
        if value["name"] == "struct":
            parameters = schema_from_record(parameters)
        elif value["name"] in {"list", "fixed_size_list"}:
            size = 2 if value["name"] == "fixed_size_list" else 1
            if len(parameters) != size or (size == 2 and (type(parameters[1]) is not int or parameters[1] < 0)):
                raise ValueError("invalid recorded list dtype")
            parameters = (dtype(parameters[0]), *parameters[1:])
        elif value["name"] == "decimal128":
            if (len(parameters) != 2 or any(type(p) is not int for p in parameters)
                    or not 1 <= parameters[0] <= 38 or not -128 <= parameters[1] <= 127):
                raise ValueError("invalid recorded decimal dtype")
        elif value["name"] == "timestamp":
            if (len(parameters) != 2 or parameters[0] not in {"s", "ms", "us", "ns"}
                    or parameters[1] is not None and not isinstance(parameters[1], str)):
                raise ValueError("invalid recorded timestamp dtype")
        elif parameters:
            raise ValueError("unexpected parameters for recorded scalar dtype")
        return ResultType(value["name"], value["nullable"], tuple(parameters))
    if not isinstance(fields, list) or any(not isinstance(item, dict) or set(item) != {"name", "type"} for item in fields):
        raise ValueError("invalid recorded fields")
    schema = tuple((item["name"], dtype(item["type"])) for item in fields)
    ResultContract(schema)
    return schema


def _wire(value, dtype, *, decode=False):
    if value is None:
        return None
    if dtype.name.startswith("float"):
        return float.fromhex(value) if decode else value.hex()
    if dtype.name == "decimal128":
        if decode:
            coefficient = int(value)
            return Decimal((int(coefficient < 0), tuple(map(int, str(abs(coefficient)))), -dtype.parameters[1]))
        return str(_value(value, dtype)[1])
    if dtype.name == "binary":
        return bytes.fromhex(value) if decode else value.hex()
    if dtype.name in {"date32", "timestamp_micros"}:
        return value if decode else _value(value, dtype)
    if dtype.name in {"list", "fixed_size_list"}:
        return [_wire(item, dtype.parameters[0], decode=decode) for item in value]
    if dtype.name == "struct":
        if set(value) != set(dict(dtype.parameters)):
            raise ValueError("recorded struct fields differ")
        return {name: _wire(value[name], child, decode=decode) for name, child in dtype.parameters}
    return value


def result_record(result: TypedResult) -> dict:
    ResultContract(result.schema)
    # Validate before projection: otherwise an extra field would silently vanish
    # from retained evidence, even though the comparator rejects it later.
    for row in result.rows:
        _row(row, result.schema)
    return {"schema": schema_record(result.schema), "rows": [
        {name: _wire(row[name], dtype) for name, dtype in result.schema} for row in result.rows]}


def result_from_record(record) -> TypedResult:
    if not isinstance(record, dict) or set(record) != {"schema", "rows"} or not isinstance(record["rows"], list):
        raise ValueError("invalid typed result record")
    schema = schema_from_record(record["schema"])
    rows = []
    for row in record["rows"]:
        if not isinstance(row, dict) or set(row) != set(dict(schema)):
            raise ValueError("recorded result fields differ")
        decoded = {name: _wire(row[name], dtype, decode=True) for name, dtype in schema}
        _row(decoded, schema)
        rows.append(decoded)
    return TypedResult(schema, rows)


def arrow_schema(schema) -> Schema:
    """Normalize only transport spelling; preserve widths, units and nullability."""
    import pyarrow as pa

    def dtype(field):
        value = field.type
        if pa.types.is_struct(value):
            return ResultType("struct", field.nullable,
                              tuple((child.name, dtype(child)) for child in value))
        if pa.types.is_list(value) or pa.types.is_large_list(value) or pa.types.is_fixed_size_list(value):
            # LargeList is a transport offset width, not a different value domain.
            params = (dtype(value.value_field),)
            name = "list"
            if pa.types.is_fixed_size_list(value):
                name, params = "fixed_size_list", (*params, value.list_size)
            return ResultType(name, field.nullable, params)
        if pa.types.is_decimal128(value):
            return ResultType("decimal128", field.nullable, (value.precision, value.scale))
        if pa.types.is_timestamp(value):
            if value.unit == "us" and value.tz is None:
                return ResultType("timestamp_micros", field.nullable)
            return ResultType("timestamp", field.nullable, (value.unit, value.tz))
        names = {"string": "utf8", "large_string": "utf8", "bool": "bool", "date32[day]": "date32",
                 "double": "float64", "float": "float32", "halffloat": "float16",
                 "large_binary": "binary"}
        return ResultType(names.get(str(value), str(value)), field.nullable)
    return tuple((field.name, dtype(field)) for field in schema)


def _value(value: Any, dtype: ResultType) -> Any:
    """Validate without rounding, float-to-int coercion or null normalization."""
    if value is None:
        if not dtype.nullable:
            raise ResultMismatch(f"NULL in nonnullable {dtype.name}")
        return None
    name = dtype.name
    if name.startswith(("int", "uint")):
        signed = name.startswith("int")
        bits = int(name[3:] if signed else name[4:])
        low, high = (-(1 << (bits - 1)), 1 << (bits - 1)) if signed else (0, 1 << bits)
        if type(value) is not int or not low <= value < high:
            raise ResultMismatch(f"invalid {name} value: {value!r}")
    elif name.startswith("float"):
        if type(value) is not float:
            raise ResultMismatch(f"expected a floating value, got {type(value).__name__}")
    elif name == "decimal128":
        precision, scale = dtype.parameters
        if not isinstance(value, Decimal) or not value.is_finite():
            raise ResultMismatch("decimal result must be a finite Decimal")
        parts = value.as_tuple()
        # Use tuple arithmetic, independent of the process decimal context.
        coefficient = int("".join(map(str, parts.digits)))
        shift = parts.exponent + scale
        if shift < 0:
            divisor = 10 ** -shift
            if coefficient % divisor:
                raise ResultMismatch("decimal value loses digits at its declared scale")
            coefficient //= divisor
        else:
            coefficient *= 10 ** shift
        if len(str(coefficient)) > precision:
            raise ResultMismatch("decimal value exceeds its declared precision")
        return ("decimal", -coefficient if parts.sign else coefficient, precision, scale)
    elif name == "date32":
        if type(value) is dt.date:
            value = (value - dt.date(1970, 1, 1)).days
        return _value(value, ResultType("int32", dtype.nullable))
    elif name == "timestamp_micros":
        if type(value) is dt.datetime:
            if value.tzinfo is not None:
                raise ResultMismatch("timezone identity differs from naive microseconds")
            delta = value - dt.datetime(1970, 1, 1)
            value = (delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds
        return _value(value, ResultType("int64", dtype.nullable))
    elif name in {"list", "fixed_size_list"}:
        if type(value) not in (list, tuple):
            raise ResultMismatch("list result requires a sequence")
        if name == "fixed_size_list" and len(value) != dtype.parameters[1]:
            raise ResultMismatch("fixed list width differs")
        return tuple(_value(item, dtype.parameters[0]) for item in value)
    elif name == "struct":
        return _row(value, dtype.parameters)
    elif name == "bool":
        if type(value) is not bool:
            raise ResultMismatch("boolean result requires a bool")
    elif name == "utf8":
        if type(value) is not str:
            raise ResultMismatch("UTF8 result requires a string")
    elif name == "binary":
        if type(value) is not bytes:
            raise ResultMismatch("binary result requires bytes")
    elif name == "null":
        raise ResultMismatch("non-NULL value in null dtype")
    else:
        raise ComparisonLimit(f"strict comparison not implemented for {dtype!r}")
    return value


def _row(row: Mapping[str, Any], schema: Schema) -> tuple:
    if not isinstance(row, Mapping) or set(row) != {name for name, _ in schema}:
        raise ResultMismatch("row fields differ from the ordered schema")
    return tuple(_value(row[name], dtype) for name, dtype in schema)


def _float_equal(left: float, right: float, policy: FloatPolicy) -> bool:
    if math.isnan(left) or math.isnan(right):
        return policy.nan_equal and math.isnan(left) and math.isnan(right)
    if left == right:
        return (policy.signed_zero_equal or left != 0.0
                or math.copysign(1, left) == math.copysign(1, right))
    if not math.isfinite(left) or not math.isfinite(right):
        return False
    return math.isclose(left, right, rel_tol=policy.relative, abs_tol=policy.absolute)


def _exact_key(value):
    if isinstance(value, float):
        return ("float", value.hex())
    if isinstance(value, tuple):
        return tuple(_exact_key(item) for item in value)
    return (type(value).__name__, value)


def compare(expected: TypedResult, actual: TypedResult, contract: ResultContract) -> None:
    """Check complete schemas and ordered sequences or duplicate-aware multisets.

    Unordered approximate equality is not transitive. Use bipartite matching,
    never rounded hash keys or a greedy pairing that can reject a valid result.
    """
    if expected.schema != contract.schema or actual.schema != contract.schema:
        raise ResultMismatch(f"schema differs: expected {contract.schema!r}, got {actual.schema!r}")
    if len(expected.rows) != len(actual.rows):
        raise ResultMismatch(f"row count differs: expected {len(expected.rows)}, got {len(actual.rows)}")
    left = [_row(row, contract.schema) for row in expected.rows]
    right = [_row(row, contract.schema) for row in actual.rows]
    policies = {i: contract.floats.get(name, FloatPolicy())
                for i, (name, dtype) in enumerate(contract.schema) if dtype.name.startswith("float")}

    def contains_nan(value):
        return (math.isnan(value) if isinstance(value, float) else
                any(contains_nan(item) for item in value) if isinstance(value, tuple) else False)

    for rows in (left, right):
        for row in rows:
            for i, value in enumerate(row):
                if contains_nan(value) and not policies.get(i, FloatPolicy()).nan_equal:
                    raise ResultMismatch("NaN equality was not admitted by the result contract")

    def equal(a, b):
        return all((_float_equal(x, y, policies[i]) if i in policies and x is not None and y is not None
                    else _exact_key(x) == _exact_key(y)) for i, (x, y) in enumerate(zip(a, b)))

    if contract.ordered:
        for index, (a, b) in enumerate(zip(left, right)):
            if not equal(a, b):
                raise ResultMismatch(f"ordered row {index} differs: expected {a!r}, got {b!r}")
        return
    tolerant = {i for i, policy in policies.items()
                if policy.absolute or policy.relative or policy.signed_zero_equal}
    if not tolerant:
        if Counter(map(_exact_key, left)) != Counter(map(_exact_key, right)):
            raise ResultMismatch("unordered rows or duplicate multiplicities differ")
        return
    def key(row):
        return tuple(_exact_key(value) if i not in tolerant or value is None else ("tolerant",)
                     for i, value in enumerate(row))
    left_groups, right_groups = defaultdict(list), defaultdict(list)
    for row in left:
        left_groups[key(row)].append(row)
    for row in right:
        right_groups[key(row)].append(row)
    if left_groups.keys() != right_groups.keys():
        raise ResultMismatch("exact fields or NULL multiplicities differ")
    for group, candidates in left_groups.items():
        observed = right_groups[group]
        if len(candidates) != len(observed):
            raise ResultMismatch("group multiplicity differs")
        if Counter(map(_exact_key, candidates)) == Counter(map(_exact_key, observed)):
            continue
        # Do not cancel individual exact pairs: approximate equality is not
        # transitive and that can destroy a valid complete matching.
        if len(candidates) * len(observed) > contract.max_matching_edges:
            raise ComparisonLimit("unordered tolerant matching exceeds its declared work budget")
        edges = [[j for j, row in enumerate(observed) if equal(wanted, row)] for wanted in candidates]
        owners = {}
        for start in range(len(candidates)):
            queue, parents, seen = deque([start]), {}, {start}
            terminal = None
            while queue and terminal is None:
                node = queue.popleft()
                for target in edges[node]:
                    if target in parents:
                        continue
                    parents[target] = node
                    if target not in owners:
                        terminal = target
                        break
                    if owners[target] not in seen:
                        seen.add(owners[target])
                        queue.append(owners[target])
            if terminal is None:
                raise ResultMismatch("no complete duplicate-aware floating match exists")
            while terminal is not None:
                node = parents[terminal]
                previous = next((target for target, owner in owners.items() if owner == node), None)
                owners[terminal] = node
                terminal = previous
