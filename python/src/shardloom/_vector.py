"""Inert, versioned sparse and affine-quantized representation contracts."""
from __future__ import annotations

import json
import math
from typing import Mapping, Sequence

from ._embedding import _IDENTITY_FIELDS, metadata_json, normalize_metadata as identity_metadata

KINDS = frozenset({"sparse_vector", "quantized_vector"})
INTEGERS = frozenset({f"{prefix}int{width}" for prefix in ("", "u") for width in (8, 16, 32, 64)})
FLOATS = frozenset({"float16", "float32", "float64"})
IDS = {name: f"shardloom.{name}.v1" for name in KINDS}


def uint64(value: object, name: str, *, positive=False) -> int:
    if type(value) is not int or not int(positive) <= value <= (1 << 64) - 1:
        raise ValueError(f"{name} requires {'positive ' if positive else ''}uint64")
    return value


def normalize_metadata(kind: str, value: object) -> tuple[tuple[str, object], ...]:
    if kind not in KINDS or not isinstance(value, Mapping):
        raise ValueError("vector metadata requires its declared representation and a mapping")
    fields = dict(value)
    allowed = _IDENTITY_FIELDS | {"version", "uniform_size"} | (
        {"index_policy"} if kind == "sparse_vector" else {"parameter_mode", "block_size"})
    if set(fields) - allowed:
        raise ValueError("unknown vector metadata field")
    if type(fields.get("version")) is not int or fields["version"] != 1:
        raise ValueError("vector metadata version must be 1")
    identity_metadata({key: val for key, val in fields.items() if key in _IDENTITY_FIELDS | {"version"}})
    if "uniform_size" in fields:
        uint64(fields["uniform_size"], "uniform_size")
    if kind == "sparse_vector":
        if fields.get("index_policy") not in ("preserve", "strictly_increasing"):
            raise ValueError("sparse index_policy requires preserve or strictly_increasing")
    elif fields.get("parameter_mode") == "per_vector":
        if "block_size" in fields:
            raise ValueError("per-vector quantization forbids block_size")
    elif fields.get("parameter_mode") == "blocked":
        uint64(fields.get("block_size"), "block_size", positive=True)
    else:
        raise ValueError("quantized parameter_mode requires per_vector or blocked")
    result = tuple(sorted(fields.items()))
    if len(metadata_json(result).encode("utf-8")) > 8 << 20:
        raise ValueError("vector metadata exceeds 8 MiB")
    return result


def decode_metadata(kind: str, raw: str) -> tuple[tuple[str, object], ...]:
    if len(raw.encode("utf-8")) > 8 << 20:
        raise ValueError("vector metadata exceeds 8 MiB")

    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError("duplicate vector metadata field")
            result[key] = value
        return result

    return normalize_metadata(kind, json.loads(raw, object_pairs_hook=pairs))


def storage_fields(kind: str, storage, metadata):
    names = ("size", "indices", "values") if kind == "sparse_vector" else ("codes", "scale", "zero_point")
    if storage.name != "struct" or tuple(name for name, _ in storage.parameters) != names:
        raise ValueError(f"vector storage requires {names} in that order")
    fields = tuple(child for _, child in storage.parameters)

    def item(dtype):
        return dtype.parameters[0] if dtype.name == "list" and len(dtype.parameters) == 1 else None

    if kind == "sparse_vector":
        size, indices, values = fields
        index, value = item(indices), item(values)
        if size.name != "uint64" or index is None or index.name not in INTEGERS:
            raise ValueError("sparse size requires uint64 and indices require an integer list")
        if value is None or value.name not in FLOATS:
            raise ValueError("sparse values require a Float16/Float32/Float64 list")
    else:
        codes, scale, zero = fields
        code = item(codes)
        if code is None or code.name not in INTEGERS:
            raise ValueError("quantized codes require an integer list")
        if dict(metadata)["parameter_mode"] == "blocked":
            scale, zero = item(scale), item(zero)
        if scale is None or scale.name not in FLOATS:
            raise ValueError("quantized scale requires Float16/Float32/Float64 with the declared parameter mode")
        if zero is None or zero.name != code.name:
            raise ValueError("quantized zero points require the code primitive and declared parameter mode")
    return fields


def validate_value(kind: str, value: object, storage, metadata) -> None:
    storage_fields(kind, storage, metadata)
    names = {name for name, _ in storage.parameters}
    if not isinstance(value, Mapping) or set(value) != names:
        raise ValueError("vector value must match its complete declared fields")
    fields = dict(metadata)

    def sequence(name):
        values = value[name]
        if not isinstance(values, Sequence) or isinstance(values, (str, bytes, bytearray, memoryview)):
            raise ValueError(f"present vector requires {name} list")
        return values

    def uniform(size):
        if "uniform_size" in fields and size != fields["uniform_size"]:
            raise ValueError("vector size violates uniform_size")

    if kind == "sparse_vector":
        size = uint64(value["size"], "sparse size")
        uniform(size)
        indices, values = sequence("indices"), sequence("values")
        if len(indices) != len(values):
            raise ValueError("sparse index and value lengths differ")
        previous = None
        for index in indices:
            if type(index) is not int or not 0 <= index < size:
                raise ValueError("sparse index is outside its declared dimension")
            if fields["index_policy"] == "strictly_increasing" and previous is not None and previous >= index:
                raise ValueError("sparse indices must be strictly increasing")
            previous = index
        return
    codes = sequence("codes")
    uniform(len(codes))
    if fields["parameter_mode"] == "blocked":
        block = fields["block_size"]
        expected = len(codes) // block + int(len(codes) % block != 0)
        scales, zeros = sequence("scale"), sequence("zero_point")
        if len(scales) != expected or len(zeros) != expected:
            raise ValueError("quantized parameter counts differ from code blocks")
    else:
        scales, zeros = (value["scale"],), (value["zero_point"],)
    for scale, zero in zip(scales, zeros):
        try:
            valid = type(scale) in (int, float) and math.isfinite(scale) and scale > 0
        except OverflowError:
            valid = False
        if not valid:
            raise ValueError("quantized scales must be nonnull, finite and positive")
        if zero is None:
            raise ValueError("quantized zero points must be nonnull")
