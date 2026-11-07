"""Inert variable-tensor metadata and explicit boundary value checks."""

from __future__ import annotations

import json
import sys
from typing import Mapping, Sequence

from ._tensor import ELEMENT_TYPES, metadata_json

EXTENSION_ID = "shardloom.variable_shape_tensor.v1"
ARROW_EXTENSION_ID = "arrow.variable_shape_tensor"


def validate_rank(rank: object) -> int:
    if type(rank) is not int or not 0 <= rank <= (1 << 32) - 1:
        raise ValueError("variable tensor ndim must be a uint32")
    return rank


def storage_rank(storage) -> int:
    if (storage.name != "struct" or len(storage.parameters) != 2
            or tuple(name for name, _ in storage.parameters) != ("data", "shape")):
        raise ValueError("variable tensor storage requires data then shape struct fields")
    data, shape = (child for _, child in storage.parameters)
    if (data.name != "list" or len(data.parameters) != 1
            or data.parameters[0].name not in ELEMENT_TYPES):
        raise ValueError("variable tensor data requires a Boolean/integer/Float32/Float64 list")
    if (shape.name != "fixed_size_list" or len(shape.parameters) != 2
            or shape.parameters[0].name != "int32"):
        raise ValueError("variable tensor shape requires a fixed-size Int32 list")
    return validate_rank(shape.parameters[1])


def normalize_metadata(value: object, rank: int) -> tuple[tuple[str, object], ...]:
    validate_rank(rank)
    if not isinstance(value, Mapping) or set(value) - {"dim_names", "permutation", "uniform_shape"}:
        raise ValueError("unknown variable tensor metadata field")
    result = {}
    for name, values in value.items():
        if not isinstance(values, (list, tuple)) or len(values) != rank:
            raise ValueError("variable tensor metadata arrays must match ndim")
        if name == "dim_names" and any(not isinstance(item, str) for item in values):
            raise ValueError("tensor dimension names must be strings")
        if name == "permutation" and (
                any(type(item) is not int or not 0 <= item < rank for item in values)
                or len(set(values)) != rank):
            raise ValueError("tensor permutation must contain each rank index exactly once")
        if name == "uniform_shape" and any(item is not None and (
                type(item) is not int or not 0 <= item <= (1 << 31) - 1) for item in values):
            raise ValueError("uniform tensor dimensions require nonnegative int32 or null")
        result[name] = tuple(values)
    metadata = tuple(sorted(result.items()))
    if len(metadata_json(metadata).encode("utf-8")) > 8 << 20:
        raise ValueError("tensor metadata exceeds 8 MiB")
    return metadata


def decode_metadata(raw: str, rank: int) -> tuple[tuple[str, object], ...]:
    if len(raw.encode("utf-8")) > 8 << 20:
        raise ValueError("tensor metadata exceeds 8 MiB")

    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError("duplicate tensor metadata field")
            result[key] = value
        return result

    return normalize_metadata(json.loads(raw or "{}", object_pairs_hook=pairs), rank)


def validate_value(value: object, storage, metadata) -> None:
    rank = storage_rank(storage)
    if not isinstance(value, Mapping) or set(value) != {"data", "shape"}:
        raise ValueError("variable tensor value requires exactly data and shape")
    data, shape = value["data"], value["shape"]
    if any(not isinstance(values, Sequence) or isinstance(values, (str, bytes, bytearray))
           for values in (data, shape)):
        raise ValueError("present variable tensor requires data and shape lists")
    if len(shape) != rank:
        raise ValueError("tensor shape has the wrong rank")
    uniform = dict(metadata).get("uniform_shape")
    product = 1
    for index, dimension in enumerate(shape):
        if type(dimension) is not int or not 0 <= dimension <= (1 << 31) - 1:
            raise ValueError("tensor dimensions require nonnull nonnegative int32 values")
        if uniform is not None and uniform[index] is not None and dimension != uniform[index]:
            raise ValueError(f"tensor shape violates a declared uniform dimension at index {index}")
        # Saturate until a later zero, rather than allocating arbitrarily large
        # Python integers for a product no physical list could contain.
        product = min(sys.maxsize + 1, product * dimension)
    if product != len(data):
        raise ValueError("tensor shape product differs from data length")
