# SPDX-License-Identifier: Apache-2.0
"""Metadata-only constructors for declared ShardLoom schema types.

These values describe schema metadata; they do not convert or evaluate data.
Timezone metadata is explicit. Native calendar operations use pinned rules.
"""

from __future__ import annotations

from typing import Mapping
from ._compat import dataclass

from ._input_schema import normalize_schema
from ._result_schema import ResultType
from ._temporal import temporal_parts

__all__ = (
    "ResultType",
    "int8",
    "int16",
    "int32",
    "int64",
    "uint8",
    "uint16",
    "uint32",
    "uint64",
    "float32",
    "float64",
    "boolean",
    "string",
    "binary",
    "date32",
    "date64",
    "time",
    "timestamp",
    "duration",
    "timestamp_micros",
    "decimal128",
    "list_",
    "fixed_size_list",
    "struct",
    "scalar",
)


def _type(specification: object) -> ResultType:
    return normalize_schema({"value": specification})[0][1]


@dataclass(frozen=True, slots=True)
class _ScalarParameter:
    value: object
    dtype: ResultType


def scalar(value: object, dtype: object):
    """Bind a scalar with an explicit native type, including typed nulls.

    The shared native binder checks the conversion. Scalar parameters support
    bool, Int64, UInt64, Float64, UTF8, binary, temporal types and Decimal128.
    Input column schemas have a broader type set.
    """
    dtype = _type(dtype)
    if dtype.name not in {"bool", "int64", "uint64", "float64", "utf8", "binary",
                          "date32", "timestamp_micros", "decimal128"} and temporal_parts(dtype.name) is None:
        raise ValueError(f"type {dtype.name!r} has no native scalar parameter contract")
    if value is None and not dtype.nullable:
        raise ValueError("a non-nullable scalar parameter cannot be null")
    return _ScalarParameter(value, dtype)


def int8(*, nullable: bool = True) -> ResultType:
    return _type({"type": "int8", "nullable": nullable})


def int16(*, nullable: bool = True) -> ResultType:
    return _type({"type": "int16", "nullable": nullable})


def int32(*, nullable: bool = True) -> ResultType:
    return _type({"type": "int32", "nullable": nullable})


def int64(*, nullable: bool = True) -> ResultType:
    return _type({"type": "int64", "nullable": nullable})


def uint8(*, nullable: bool = True) -> ResultType:
    return _type({"type": "uint8", "nullable": nullable})


def uint16(*, nullable: bool = True) -> ResultType:
    return _type({"type": "uint16", "nullable": nullable})


def uint32(*, nullable: bool = True) -> ResultType:
    return _type({"type": "uint32", "nullable": nullable})


def uint64(*, nullable: bool = True) -> ResultType:
    return _type({"type": "uint64", "nullable": nullable})


def float32(*, nullable: bool = True) -> ResultType:
    return _type({"type": "float32", "nullable": nullable})


def float64(*, nullable: bool = True) -> ResultType:
    return _type({"type": "float64", "nullable": nullable})


def boolean(*, nullable: bool = True) -> ResultType:
    return _type({"type": "bool", "nullable": nullable})


def string(*, nullable: bool = True) -> ResultType:
    return _type({"type": "utf8", "nullable": nullable})


def binary(*, nullable: bool = True) -> ResultType:
    return _type({"type": "binary", "nullable": nullable})


def date32(*, nullable: bool = True) -> ResultType:
    return _type({"type": "date32", "nullable": nullable})


def timestamp_micros(*, nullable: bool = True) -> ResultType:
    return _type({"type": "timestamp_micros", "nullable": nullable})


def date64(*, nullable: bool = True) -> ResultType:
    """Whole epoch days stored as signed millisecond ticks."""
    return _type({"type": "date64", "nullable": nullable})


def time(unit: str = "us", *, nullable: bool = True) -> ResultType:
    """Time of day in s, ms, us or ns, without timezone inference."""
    return _type({"type": f"time({unit})", "nullable": nullable})


def timestamp(unit: str = "us", timezone: str | None = None, *, nullable: bool = True) -> ResultType:
    """Civil ticks when naive; epoch ticks with an explicit presentation zone."""
    if timezone is not None and not isinstance(timezone, str):
        raise TypeError("timestamp timezone must be a string or None")
    zone = f",{timezone}" if timezone is not None else ""
    return _type({"type": f"timestamp({unit}{zone})", "nullable": nullable})


def duration(unit: str = "us", *, nullable: bool = True) -> ResultType:
    """Signed elapsed ticks; calendar months and days are separate operations."""
    return _type({"type": f"duration({unit})", "nullable": nullable})


def decimal128(precision: int, scale: int, *, nullable: bool = True) -> ResultType:
    if type(precision) is not int or type(scale) is not int:
        raise TypeError("decimal precision and scale must be exact integers")
    if not 1 <= precision <= 38 or not 0 <= scale <= precision:
        raise ValueError("decimal precision/scale requires 1 <= precision <= 38 and 0 <= scale <= precision")
    return _type({"type": f"decimal128({precision},{scale})", "nullable": nullable})


def list_(item: object, *, nullable: bool = True) -> ResultType:
    return _type({"type": "list", "item": item, "nullable": nullable})


def fixed_size_list(item: object, size: int, *, nullable: bool = True) -> ResultType:
    return _type({"type": "fixed_size_list", "item": item, "size": size, "nullable": nullable})


def struct(fields: Mapping[str, object], *, nullable: bool = True) -> ResultType:
    return _type({"type": "struct", "fields": fields, "nullable": nullable})
