# SPDX-License-Identifier: Apache-2.0
"""Metadata-only constructors for declared ShardLoom schema types.

These values describe schema metadata; they do not convert or evaluate data.
The supported timestamp type is naive microsecond precision and performs no
timezone conversion.
"""

from __future__ import annotations

from typing import Mapping
from ._compat import dataclass

from ._input_schema import normalize_schema
from ._result_schema import ResultType

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
    bool, Int64, UInt64, Float64, UTF8, binary, Date32, naive microsecond
    timestamps and Decimal128. Input column schemas have a broader type set.
    """
    dtype = _type(dtype)
    if dtype.name not in {"bool", "int64", "uint64", "float64", "utf8", "binary",
                          "date32", "timestamp_micros", "decimal128"}:
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
