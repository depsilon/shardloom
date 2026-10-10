"""Exact input conversion into native memory declarations; no query evaluation."""

from __future__ import annotations

import datetime as dt
from decimal import Decimal
import json
import math
import re
import struct
from typing import Mapping, Sequence

from ._result_schema import ResultType

_ENCODING = "vortex.dtype.serde.v1"
_MAX_BYTES = 8 << 20
_LEGACY = {"int64", "float64", "bool", "utf8"}
_ALIASES = {"int": "int64", "integer": "int64", "float": "float64", "double": "float64",
            "boolean": "bool", "str": "utf8", "string": "utf8"}
_PRIMITIVES = {f"{kind}{bits}" for kind in ("int", "uint") for bits in (8, 16, 32, 64)} | {
    "float32", "float64"}


def normalize_schema(schema: Mapping[str, object] | None) -> tuple[tuple[str, ResultType], ...]:
    if schema is None:
        return ()
    if not isinstance(schema, Mapping):
        raise TypeError("native input schema must be a mapping")
    fields = []
    for name, specification in schema.items():
        if not isinstance(name, str) or not name or len(name.encode("utf-8")) > 256:
            raise ValueError("native input field names require 1..=256 UTF8 bytes")
        fields.append((name, _parse_type(specification, 0, [0, 0])))
    if len({name for name, _ in fields}) != len(fields):
        raise ValueError("native input field names must be distinct")
    return tuple(fields)


def _parse_type(specification: object, depth: int, budget: list[int]) -> ResultType:
    budget[0] += 1
    budget[1] += 1024
    if depth > 24 or budget[0] > 4096 or budget[1] > _MAX_BYTES:
        raise ValueError("nested input schema exceeds depth 24, 4096 nodes or 8 MiB metadata")
    parameters = specification if isinstance(specification, Mapping) else {"type": specification}
    token = parameters.get("type")
    if not isinstance(token, str):
        raise TypeError("native input type requires a string type name")
    token = _ALIASES.get(token.lower(), token.lower())
    nullable = parameters.get("nullable", True)
    if type(nullable) is not bool:
        raise TypeError("native input nullable must be a bool")
    allowed = {"type", "nullable"}
    if token in {"list", "fixed_size_list"}:
        allowed.add("item")
    if token == "fixed_size_list":
        allowed.add("size")
    if token == "struct":
        allowed.add("fields")
    if set(parameters) - allowed:
        raise ValueError("unknown native input type parameters")
    if token in _PRIMITIVES | {"bool", "utf8", "binary", "date32", "timestamp_micros"}:
        return ResultType(token, nullable)
    if match := re.fullmatch(r"decimal128\(([0-9]+),\s*([0-9]+)\)", token):
        # Bound text before integer conversion, including hostile giant digits.
        if any(len(part) > 2 for part in match.groups()):
            raise ValueError("decimal input requires precision 1..38 and scale 0..precision")
        precision, scale = map(int, match.groups())
        if not 1 <= precision <= 38 or not 0 <= scale <= precision:
            raise ValueError("decimal input requires precision 1..38 and scale 0..precision")
        return ResultType("decimal128", nullable, (precision, scale))
    if token in {"list", "fixed_size_list"}:
        if "item" not in parameters:
            raise ValueError("native list input requires an item type")
        child = _parse_type(parameters["item"], depth + 1, budget)
        if token == "list":
            return ResultType(token, nullable, (child,))
        size = parameters.get("size")
        if type(size) is not int or not 0 <= size <= (1 << 32) - 1:
            raise ValueError("fixed-size list width must be a uint32")
        return ResultType(token, nullable, (child, size))
    if token == "struct":
        fields = parameters.get("fields")
        if not isinstance(fields, Mapping) or not 1 <= len(fields) <= 1024:
            raise ValueError("nested input structs require 1..=1024 fields")
        result = []
        for name, child in fields.items():
            if not isinstance(name, str) or not name:
                raise ValueError("nested input field names must be nonempty strings")
            budget[1] += 2 * len(name.encode("utf-8"))
            result.append((name, _parse_type(child, depth + 1, budget)))
        if len({name for name, _ in result}) != len(result):
            raise ValueError("nested input field names must be distinct")
        return ResultType(token, nullable, tuple(result))
    raise ValueError(f"unsupported native input type {token!r}")


def schema_hints(fields: Sequence[tuple[str, ResultType]]) -> tuple[tuple[str, str], ...]:
    return tuple((name, dtype.label) for name, dtype in fields)


def wire_schema(fields: Sequence[tuple[str, ResultType]]) -> tuple[tuple[str, object], ...]:
    result = tuple((name, dtype.name if dtype.name in _LEGACY and dtype.nullable else {
        "native": {"encoding": _ENCODING, "dtype": json.dumps(
            _wire_type(dtype), ensure_ascii=False, separators=(",", ":"))}
    }) for name, dtype in fields)
    if len(json.dumps(result, ensure_ascii=False).encode("utf-8")) > _MAX_BYTES:
        raise ValueError("native input schema exceeds the 8 MiB declaration bound")
    return result


def _wire_type(dtype: ResultType) -> object:
    name, nullable = dtype.name, dtype.nullable
    if name in _PRIMITIVES:
        prefix = "u" if name.startswith("uint") else name[0]
        return {"Primitive": [prefix + re.search(r"[0-9]+$", name).group(), nullable]}
    if name in {"bool", "utf8", "binary"}:
        return {{"bool": "Bool", "utf8": "Utf8", "binary": "Binary"}[name]: nullable}
    if name == "decimal128":
        precision, scale = dtype.parameters
        return {"Decimal": [{"precision": precision, "scale": scale}, nullable]}
    if name in {"date32", "timestamp_micros"}:
        date = name == "date32"
        return {"Extension": {"id": "vortex.date" if date else "vortex.timestamp",
                "storage_dtype": {"Primitive": ["i32" if date else "i64", nullable]},
                "metadata": [4] if date else [1, 0, 0]}}
    if name in {"list", "fixed_size_list"}:
        child = _wire_type(dtype.parameters[0])
        return {"List": [child, nullable]} if name == "list" else {
            "FixedSizeList": [child, dtype.parameters[1], nullable]}
    if name == "struct":
        return {"Struct": [{"names": [name for name, _ in dtype.parameters],
                            "dtypes": [_wire_type(child) for _, child in dtype.parameters]}, nullable]}
    raise ValueError("unsupported native input schema")


def encode_cell(dtype: ResultType, value: object) -> str | None:
    if value is None:
        if not dtype.nullable:
            raise ValueError(f"null in a nonnullable native {dtype.label} input")
        return None
    if dtype.name in _LEGACY and dtype.nullable:
        # Preserve existing declaration hashes, aliases and float promotion policy.
        from .query import _memory_value
        return _memory_value(dtype.name, value)
    converted = _value(value, dtype, [0])
    encoded = json.dumps(converted, ensure_ascii=False, allow_nan=False, separators=(",", ":"))
    if len(encoded.encode("utf-8")) > _MAX_BYTES:
        raise ValueError("native input value exceeds 8 MiB")
    return encoded


def _charge(budget: list[int], size: int) -> None:
    budget[0] += size
    if budget[0] > _MAX_BYTES:
        raise ValueError("native input value exceeds 8 MiB")


def _integer(value: object, bits: int, signed: bool) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError("native integer input requires int values")
    lower, upper = (-(1 << (bits - 1)), 1 << (bits - 1)) if signed else (0, 1 << bits)
    if not lower <= value < upper:
        raise ValueError("native integer input exceeds its declared width")
    return value


def _decimal(value: object, dtype: ResultType) -> str:
    if not isinstance(value, Decimal) or not value.is_finite():
        raise TypeError("native decimal input requires finite Decimal values")
    precision, scale = dtype.parameters
    sign, digits, exponent = value.as_tuple()
    # Decimal's context must never round an input during conversion.
    if not any(digits):
        coefficient = 0
    else:
        shift = exponent + scale
        if shift < 0:
            cut = -shift
            if cut >= len(digits) or any(digits[-cut:]):
                raise ValueError("decimal input cannot be represented at the declared scale")
            digits = digits[:-cut]
            shift = 0
        if len(digits) + shift > precision:
            raise ValueError("decimal input exceeds its declared precision")
        coefficient = 0
        for digit in digits:
            coefficient = coefficient * 10 + digit
        coefficient *= 10 ** shift
        if sign:
            coefficient = -coefficient
    return f"decimal128({precision},{scale}):{coefficient}"


def _value(value: object, dtype: ResultType, budget: list[int]) -> object:
    _charge(budget, 1)
    if value is None:
        if not dtype.nullable:
            raise ValueError(f"null in a nonnullable native {dtype.label} input")
        return None
    name = dtype.name
    if name == "bool":
        if not isinstance(value, bool):
            raise TypeError("native bool input requires bool values")
        return value
    if name.startswith(("int", "uint")):
        signed = name.startswith("int")
        return _integer(value, int(name[3:] if signed else name[4:]), signed)
    if name in {"float32", "float64"}:
        if isinstance(value, bool) or not isinstance(value, (float, int)):
            raise TypeError("native floating input requires numeric values")
        try:
            numeric = float(value)
            exact = math.isfinite(numeric) and (not isinstance(value, int) or numeric == value)
            if name == "float32":
                narrowed = struct.unpack("!f", struct.pack("!f", numeric))[0]
                exact = exact and narrowed == numeric
            if not exact:
                raise ValueError("native floating input must be finite and exactly representable")
            return numeric
        except OverflowError as error:
            raise ValueError("native floating input exceeds its declared width") from error
    if name == "utf8":
        if not isinstance(value, str):
            raise TypeError("native utf8 input requires str values")
        _charge(budget, len(value.encode("utf-8")))
        return value
    if name == "binary":
        if not isinstance(value, (bytes, bytearray, memoryview)):
            raise TypeError("native binary input requires bytes-like values")
        size = value.nbytes if isinstance(value, memoryview) else len(value)
        _charge(budget, size * 2)
        return bytes(value).hex()
    if name == "decimal128":
        return _decimal(value, dtype)
    if name == "date32":
        if isinstance(value, dt.datetime):
            raise TypeError("native date32 input requires a date, not a datetime")
        if isinstance(value, dt.date):
            value = (value - dt.date(1970, 1, 1)).days
        return _integer(value, 32, True)
    if name == "timestamp_micros":
        if isinstance(value, dt.datetime):
            if value.tzinfo is not None:
                raise ValueError("native timestamp_micros input requires a naive datetime")
            delta = value - dt.datetime(1970, 1, 1)
            value = ((delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds)
        return _integer(value, 64, True)
    if name in {"list", "fixed_size_list"}:
        if isinstance(value, (str, bytes, bytearray, memoryview)) or not isinstance(value, Sequence):
            raise TypeError("native list input requires a sequence")
        if name == "fixed_size_list" and len(value) != dtype.parameters[1]:
            raise ValueError("native fixed-size list input has the wrong width")
        if len(value) > _MAX_BYTES - budget[0]:
            raise ValueError("native input value exceeds 8 MiB")
        return [_value(child, dtype.parameters[0], budget) for child in value]
    if name == "struct":
        if not isinstance(value, Mapping) or set(value) != {name for name, _ in dtype.parameters}:
            raise ValueError("native struct input must match its complete declared fields")
        result = {}
        for field, child in dtype.parameters:
            _charge(budget, len(field.encode("utf-8")))
            result[field] = _value(value[field], child, budget)
        return result
    raise ValueError("unsupported native input value")
