"""Decode pinned native result metadata at explicit Python output boundaries.

This adapter performs no query execution. JSON row reports keep their documented
transport values; typed conversions use the schema delivered by that execution.
"""

from __future__ import annotations

from dataclasses import dataclass
import datetime as dt
from decimal import Decimal
import json
import math
from typing import Any, Mapping, Sequence

from .errors import ShardLoomProtocolError

_PRIMITIVES = {
    "i8": "int8", "i16": "int16", "i32": "int32", "i64": "int64",
    "u8": "uint8", "u16": "uint16", "u32": "uint32", "u64": "uint64",
    "f16": "float16", "f32": "float32", "f64": "float64",
}


@dataclass(frozen=True, slots=True)
class ResultType:
    name: str
    nullable: bool
    parameters: tuple[Any, ...] = ()

    @property
    def label(self) -> str:
        if self.name == "decimal128":
            return f"decimal128({self.parameters[0]},{self.parameters[1]})"
        if self.name in {"list", "fixed_size_list"}:
            child = self.parameters[0].label
            return f"list<{child}>" if self.name == "list" else f"fixed_size_list<{child},{self.parameters[1]}>"
        return self.name


def schema_fields(raw: str, encoding: str) -> tuple[tuple[str, ResultType], ...]:
    if encoding != "vortex.dtype.serde.v1":
        raise ShardLoomProtocolError(f"unsupported native result schema format: {encoding!r}")
    try:
        result = _dtype(json.loads(raw), 0)
        if result.name != "struct":
            raise ValueError("a result requires an ordered struct schema")
        return result.parameters
    except (ValueError, TypeError, KeyError, IndexError, RecursionError) as error:
        raise ShardLoomProtocolError(f"invalid native result schema: {error}") from error


def _dtype(wire: Any, depth: int) -> ResultType:
    if depth > 64:
        raise ValueError("more than 64 nested schema levels")
    if wire == "Null":
        return ResultType("null", True)
    if not isinstance(wire, dict) or len(wire) != 1:
        raise ValueError("a native dtype must contain one variant")
    kind, payload = next(iter(wire.items()))
    if kind in {"Bool", "Utf8", "Binary", "Variant"}:
        if type(payload) is not bool:
            raise ValueError("invalid scalar nullability")
        return ResultType({"Bool": "bool", "Utf8": "utf8", "Binary": "binary", "Variant": "variant"}[kind], payload)
    if kind == "Extension":
        if not isinstance(payload, dict):
            raise ValueError("invalid extension dtype parameters")
        storage = _dtype(payload["storage_dtype"], depth + 1)
        if payload["id"] == "vortex.date" and payload["metadata"] == [4] and storage.name == "int32":
            return ResultType("date32", storage.nullable)
        if payload["id"] == "vortex.timestamp" and payload["metadata"] == [1, 0, 0] and storage.name == "int64":
            return ResultType("timestamp_micros", storage.nullable)
        raise ValueError(f"unsupported result extension: {payload.get('id')!r}")
    if not isinstance(payload, list) or len(payload) != (3 if kind == "FixedSizeList" else 2):
        raise ValueError("invalid native dtype parameters")
    if type(payload[-1]) is not bool:
        raise ValueError("invalid dtype nullability")
    nullable = payload[-1]
    if kind == "Primitive":
        return ResultType(_PRIMITIVES[payload[0]], nullable)
    if kind == "Decimal":
        precision, scale = payload[0]["precision"], payload[0]["scale"]
        if type(precision) is not int or type(scale) is not int or not 1 <= precision <= 38 or not 0 <= scale <= precision:
            raise ValueError("unsupported decimal result domain")
        return ResultType("decimal128", nullable, (precision, scale))
    if kind in {"List", "FixedSizeList"}:
        child = _dtype(payload[0], depth + 1)
        if kind == "List":
            return ResultType("list", nullable, (child,))
        size = payload[1]
        if type(size) is not int or size < 0:
            raise ValueError("invalid fixed-size list width")
        return ResultType("fixed_size_list", nullable, (child, size))
    if kind == "Struct":
        names, dtypes = payload[0]["names"], payload[0]["dtypes"]
        if (not isinstance(names, list) or not isinstance(dtypes, list) or len(names) != len(dtypes)
                or any(not isinstance(name, str) or not name for name in names) or len(set(names)) != len(names)):
            raise ValueError("invalid ordered result fields")
        return ResultType("struct", nullable, tuple((name, _dtype(dtype, depth + 1)) for name, dtype in zip(names, dtypes)))
    raise ValueError(f"unsupported native result dtype: {kind}")


def python_rows(rows: Sequence[Mapping[str, Any]], fields: Sequence[tuple[str, ResultType]],
                *, temporal_objects: bool = True) -> list[dict[str, Any]]:
    names = tuple(name for name, _ in fields)
    result = []
    for row in rows:
        if not isinstance(row, Mapping) or set(row) != set(names):
            raise ShardLoomProtocolError("native result values do not match their ordered schema")
        result.append({name: _python_value(row[name], dtype, temporal_objects) for name, dtype in fields})
    return result


def _python_value(value: Any, dtype: ResultType, temporal: bool) -> Any:
    if value is None:
        if not dtype.nullable:
            raise ShardLoomProtocolError("NULL value in a nonnullable native result field")
        return None
    try:
        if dtype.name == "null":
            raise ValueError("a null dtype requires a NULL value")
        if dtype.name == "bool" and type(value) is not bool:
            raise ValueError("boolean result requires a JSON boolean")
        if dtype.name.startswith(("int", "uint")):
            signed = dtype.name.startswith("int")
            width = int(dtype.name[3:] if signed else dtype.name[4:])
            minimum, maximum = (-(1 << (width - 1)), (1 << (width - 1)) - 1) if signed else (0, (1 << width) - 1)
            if type(value) is not int or not minimum <= value <= maximum:
                raise ValueError("integer result is outside its declared domain")
        if dtype.name.startswith("float") and (type(value) not in {int, float} or not math.isfinite(value)):
            raise ValueError("floating result requires a finite number")
        if dtype.name == "utf8" and not isinstance(value, str):
            raise ValueError("UTF8 result requires a string")
        if dtype.name == "binary":
            if not isinstance(value, str) or len(value) % 2 or any(digit not in "0123456789abcdefABCDEF" for digit in value):
                raise ValueError("binary result requires hexadecimal bytes")
            return bytes.fromhex(value)
        if dtype.name == "decimal128":
            precision, scale = dtype.parameters
            prefix = f"decimal128({precision},{scale}):"
            if not isinstance(value, str) or not value.startswith(prefix):
                raise ValueError("decimal payload does not match its result dtype")
            digits = value[len(prefix):].removeprefix("-")
            if not digits or not digits.isascii() or not digits.isdecimal() or len(digits) > precision:
                raise ValueError("decimal coefficient exceeds its declared precision")
            coefficient = int(value[len(prefix):])
            return Decimal((int(coefficient < 0), tuple(int(digit) for digit in str(abs(coefficient))), -scale))
        if dtype.name in {"date32", "timestamp_micros"}:
            bits = 32 if dtype.name == "date32" else 64
            if type(value) is not int or not -(1 << (bits - 1)) <= value < (1 << (bits - 1)):
                raise ValueError("temporal result requires an integer in its declared domain")
            if temporal:
                try:
                    if dtype.name == "date32":
                        return dt.date(1970, 1, 1) + dt.timedelta(days=value)
                    return dt.datetime(1970, 1, 1) + dt.timedelta(microseconds=value)
                except OverflowError:
                    # Python's calendar is narrower than validated native storage.
                    # Preserve exact epoch days/microseconds outside that calendar.
                    return value
        if dtype.name in {"list", "fixed_size_list"}:
            if not isinstance(value, list) or (dtype.name == "fixed_size_list" and len(value) != dtype.parameters[1]):
                raise ValueError("invalid native list payload")
            return [_python_value(item, dtype.parameters[0], temporal) for item in value]
        if dtype.name == "struct":
            return python_rows([value], dtype.parameters, temporal_objects=temporal)[0]
        return value
    except (ValueError, TypeError, OverflowError) as error:
        raise ShardLoomProtocolError(f"native {dtype.label} value cannot be converted: {error}") from error


def arrow_table(rows: Sequence[Mapping[str, Any]], fields: Sequence[tuple[str, ResultType]], pyarrow: Any) -> Any:
    schema = pyarrow.schema([pyarrow.field(name, _arrow_type(dtype, pyarrow), nullable=dtype.nullable)
                             for name, dtype in fields])
    return pyarrow.Table.from_pylist(python_rows(rows, fields, temporal_objects=False), schema=schema)


def _arrow_type(dtype: ResultType, pyarrow: Any) -> Any:
    if dtype.name == "struct":
        return pyarrow.struct([pyarrow.field(name, _arrow_type(child, pyarrow), nullable=child.nullable)
                              for name, child in dtype.parameters])
    if dtype.name in {"list", "fixed_size_list"}:
        child = dtype.parameters[0]
        field = pyarrow.field("item", _arrow_type(child, pyarrow), nullable=child.nullable)
        return pyarrow.list_(field, dtype.parameters[1]) if dtype.name == "fixed_size_list" else pyarrow.list_(field)
    if dtype.name == "decimal128":
        return pyarrow.decimal128(*dtype.parameters)
    if dtype.name == "timestamp_micros":
        return pyarrow.timestamp("us")
    if dtype.name == "variant":
        raise ShardLoomProtocolError("Arrow conversion does not admit native variant result fields")
    name = {"bool": "bool_", "utf8": "string"}.get(dtype.name, dtype.name)
    return getattr(pyarrow, name)()
