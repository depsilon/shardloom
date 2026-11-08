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
from ._embedding import EXTENSION_ID, normalize_metadata, metadata_json
from . import _tensor
from . import _variable_tensor
from . import _vector
from ._temporal import parse_temporal_declaration, encode_temporal_dtype, temporal_parts, validate_temporal_ticks

_ENCODING = "vortex.dtype.serde.v1"
_BINARY_OBJECT_EXTENSION_ID = "shardloom.binary-object.v1"
_BINARY_OBJECT_METADATA = b'{"version":1}'
_MAX_BYTES = 8 << 20
_LEGACY = {"int64", "float64", "bool", "utf8"}
_ALIASES = {"int": "int64", "integer": "int64", "float": "float64", "double": "float64",
            "boolean": "bool", "str": "utf8", "string": "utf8"}
_PRIMITIVES = {f"{kind}{bits}" for kind in ("int", "uint") for bits in (8, 16, 32, 64)} | {
    "float16", "float32", "float64"}


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


def _result_type_parameters(dtype: ResultType) -> dict[str, object]:
    """Inspect one level; ordinary parsing owns recursive depth/node admission."""
    if not isinstance(dtype.name, str) or not isinstance(dtype.parameters, tuple):
        raise TypeError("ResultType requires a string name and tuple parameters")
    expected = {"list": 1, "fixed_size_list": 2, "embedding": 3, "fixed_shape_tensor": 3,
                "variable_shape_tensor": 2, "sparse_vector": 2, "quantized_vector": 2, "decimal128": 2,
                "binary_object": 0}.get(dtype.name)
    if (expected is not None and len(dtype.parameters) != expected) or (
        expected is None and dtype.name != "struct" and dtype.parameters
    ):
        raise ValueError("ResultType has invalid parameters")
    value: dict[str, object] = {"type": dtype.name, "nullable": dtype.nullable}
    if dtype.name == "fixed_shape_tensor":
        metadata = dtype.parameters[2]
        if (not isinstance(metadata, tuple) or any(not isinstance(pair, tuple) or len(pair) != 2
                or not isinstance(pair[1], tuple) for pair in metadata)):
            raise ValueError("ResultType tensor requires immutable metadata pairs and arrays")
        fields = dict(metadata)
        if len(fields) != len(metadata):
            raise ValueError("duplicate tensor metadata field")
        normalized = _tensor.normalize_metadata(fields)
        if type(dtype.parameters[1]) is not int or dtype.parameters[1] != _tensor.shape_size(fields["shape"]):
            raise ValueError("tensor storage width differs from its shape product")
        value.update(dict(normalized))
        value["item"] = dtype.parameters[0]
    elif dtype.name == "variable_shape_tensor":
        storage, metadata = dtype.parameters
        if (not isinstance(metadata, tuple) or any(not isinstance(pair, tuple) or len(pair) != 2
                or not isinstance(pair[1], tuple) for pair in metadata)):
            raise ValueError("ResultType variable tensor requires immutable metadata pairs and arrays")
        fields = dict(metadata)
        if len(fields) != len(metadata):
            raise ValueError("duplicate variable tensor metadata field")
        value.update({"storage": storage, "metadata": fields})
    elif dtype.name in _vector.KINDS:
        storage, metadata = dtype.parameters
        if not isinstance(metadata, tuple) or any(not isinstance(pair, tuple) or len(pair) != 2 for pair in metadata):
            raise ValueError("ResultType vector requires immutable metadata pairs")
        fields = dict(metadata)
        if len(fields) != len(metadata):
            raise ValueError("duplicate vector metadata field")
        value.update({"storage": storage, "metadata": fields})
    elif dtype.name in {"list", "fixed_size_list", "embedding"}:
        value["item"] = dtype.parameters[0]
        if dtype.name != "list":
            value["size"] = dtype.parameters[1]
        if dtype.name == "embedding":
            metadata = dtype.parameters[2]
            if (not isinstance(metadata, tuple) or
                    any(not isinstance(pair, tuple) or len(pair) != 2 for pair in metadata)):
                raise ValueError("ResultType embedding requires immutable metadata pairs")
            value["metadata"] = dict(metadata)
            if len(value["metadata"]) != len(metadata):
                raise ValueError("duplicate embedding metadata field")
    elif dtype.name == "struct":
        fields = {}
        for pair in dtype.parameters:
            if not isinstance(pair, tuple) or len(pair) != 2 or not isinstance(pair[0], str):
                raise ValueError("ResultType struct requires named fields")
            name, child = pair
            if name in fields:
                raise ValueError("ResultType struct field names must be distinct")
            fields[name] = child
        value["fields"] = fields
    elif dtype.name == "decimal128":
        precision, scale = dtype.parameters
        if type(precision) is not int or type(scale) is not int or not 1 <= precision <= 38 or not 0 <= scale <= precision:
            raise ValueError("decimal input requires precision 1..38 and scale 0..precision")
        value["type"] = f"decimal128({precision},{scale})"
    return value


def _parse_type(specification: object, depth: int, budget: list[int]) -> ResultType:
    budget[0] += 1
    budget[1] += 1024
    if depth > 24 or budget[0] > 4096 or budget[1] > _MAX_BYTES:
        raise ValueError("nested input schema exceeds depth 24, 4096 nodes or 8 MiB metadata")
    if isinstance(specification, ResultType):
        specification = _result_type_parameters(specification)
    parameters = specification if isinstance(specification, Mapping) else {"type": specification}
    token = parameters.get("type")
    if not isinstance(token, str):
        raise TypeError("native input type requires a string type name")
    temporal = parse_temporal_declaration(token)
    token = temporal or _ALIASES.get(token.lower(), token.lower())
    nullable = parameters.get("nullable", True)
    if type(nullable) is not bool:
        raise TypeError("native input nullable must be a bool")
    allowed = {"type", "nullable"}
    if token in {"list", "fixed_size_list", "embedding", "fixed_shape_tensor"}:
        allowed.add("item")
    if token in {"fixed_size_list", "embedding"}:
        allowed.add("size")
    if token == "embedding":
        allowed.add("metadata")
    if token == "fixed_shape_tensor":
        allowed.update(("shape", "dim_names", "permutation"))
    if token == "variable_shape_tensor":
        allowed.update(("storage", "metadata", "item", "ndim", "dim_names", "permutation", "uniform_shape"))
    if token in _vector.KINDS:
        allowed.update(("storage", "metadata", "uniform_size"))
        allowed.update(("item", "index", "index_policy") if token == "sparse_vector" else ("code", "scale", "block_size"))
    if token == "struct":
        allowed.add("fields")
    if set(parameters) - allowed:
        raise ValueError("unknown native input type parameters")
    if temporal is not None or token in _PRIMITIVES | {"bool", "utf8", "binary", "binary_object"}:
        return ResultType(token, nullable)
    if token == "fixed_shape_tensor":
        if "item" not in parameters:
            raise ValueError("tensor input requires an item type")
        child = _parse_type(parameters["item"], depth + 1, budget)
        if child.name not in _tensor.ELEMENT_TYPES:
            raise ValueError("tensor elements require Boolean, integer, Float16, Float32 or Float64")
        metadata = _tensor.normalize_metadata({key: parameters[key]
            for key in ("shape", "dim_names", "permutation") if key in parameters})
        budget[1] += 128 * len(_tensor.metadata_json(metadata).encode("utf-8"))
        if budget[1] > _MAX_BYTES:
            raise ValueError("nested input schema exceeds 8 MiB metadata")
        size = _tensor.shape_size(dict(metadata)["shape"])
        return ResultType(token, nullable, (child, size, metadata))
    if token == "variable_shape_tensor":
        explicit = "storage" in parameters or "metadata" in parameters
        simple = "item" in parameters or "ndim" in parameters or any(
            key in parameters for key in ("dim_names", "permutation", "uniform_shape"))
        if explicit == simple:
            raise ValueError("variable tensor requires either storage and metadata or item and ndim")
        if explicit:
            if "storage" not in parameters or "metadata" not in parameters:
                raise ValueError("variable tensor storage declarations require storage and metadata")
            storage = _parse_type(parameters["storage"], depth + 1, budget)
            if storage.nullable != nullable:
                raise ValueError("variable tensor storage nullability must match the outer declaration")
            rank = _variable_tensor.storage_rank(storage)
            metadata_value = parameters["metadata"]
        else:
            if "item" not in parameters or "ndim" not in parameters:
                raise ValueError("variable tensor input requires an item type and ndim")
            rank = _variable_tensor.validate_rank(parameters["ndim"])
            storage = _parse_type({"type": "struct", "nullable": nullable, "fields": {
                "data": {"type": "list", "item": parameters["item"], "nullable": False},
                "shape": {"type": "fixed_size_list", "item": {"type": "int32", "nullable": False},
                          "size": rank, "nullable": False},
            }}, depth + 1, budget)
            _variable_tensor.storage_rank(storage)
            metadata_value = {key: parameters[key] for key in (
                "dim_names", "permutation", "uniform_shape") if key in parameters}
        metadata = _variable_tensor.normalize_metadata(metadata_value, rank)
        budget[1] += 128 * len(_variable_tensor.metadata_json(metadata).encode("utf-8"))
        if budget[1] > _MAX_BYTES:
            raise ValueError("nested input schema exceeds 8 MiB metadata")
        return ResultType(token, nullable, (storage, metadata))
    if token in _vector.KINDS:
        if "storage" in parameters:
            if set(parameters) != {"type", "nullable", "storage", "metadata"} and set(parameters) != {"type", "storage", "metadata"}:
                raise ValueError("explicit vector declarations require only storage, metadata and nullability")
            storage = _parse_type(parameters["storage"], depth + 1, budget)
            if storage.nullable != nullable:
                raise ValueError("vector storage nullability must match its outer declaration")
            metadata_value = parameters["metadata"]
        else:
            metadata_value = dict(normalize_metadata(parameters.get("metadata")))
            if "uniform_size" in parameters:
                metadata_value["uniform_size"] = parameters["uniform_size"]
            if token == "sparse_vector":
                if "item" not in parameters:
                    raise ValueError("sparse vector requires a floating item type")
                metadata_value["index_policy"] = parameters.get("index_policy", "preserve")
                children = {"size": {"type": "uint64", "nullable": False},
                    "indices": {"type": "list", "item": parameters.get("index", "uint32"), "nullable": False},
                    "values": {"type": "list", "item": parameters["item"], "nullable": False}}
            else:
                if "code" not in parameters:
                    raise ValueError("quantized vector requires an integer code type")
                code = parameters["code"]
                zero = _result_type_parameters(code) if isinstance(code, ResultType) else (
                    dict(code) if isinstance(code, Mapping) else {"type": code})
                zero["nullable"] = False
                scale = parameters.get("scale", "float32")
                blocked = "block_size" in parameters
                metadata_value["parameter_mode"] = "blocked" if blocked else "per_vector"
                if blocked:
                    metadata_value["block_size"] = parameters["block_size"]
                children = {"codes": {"type": "list", "item": code, "nullable": False},
                    "scale": {"type": "list", "item": scale, "nullable": False} if blocked else scale,
                    "zero_point": {"type": "list", "item": zero, "nullable": False} if blocked else zero}
            storage = _parse_type({"type": "struct", "fields": children, "nullable": nullable}, depth + 1, budget)
        metadata = _vector.normalize_metadata(token, metadata_value)
        _vector.storage_fields(token, storage, metadata)
        budget[1] += 128 * len(_vector.metadata_json(metadata).encode("utf-8"))
        if budget[1] > _MAX_BYTES:
            raise ValueError("nested input schema exceeds 8 MiB metadata")
        return ResultType(token, nullable, (storage, metadata))
    if match := re.fullmatch(r"decimal128\(([0-9]+),\s*([0-9]+)\)", token):
        # Bound text before integer conversion, including hostile giant digits.
        if any(len(part) > 2 for part in match.groups()):
            raise ValueError("decimal input requires precision 1..38 and scale 0..precision")
        precision, scale = map(int, match.groups())
        if not 1 <= precision <= 38 or not 0 <= scale <= precision:
            raise ValueError("decimal input requires precision 1..38 and scale 0..precision")
        return ResultType("decimal128", nullable, (precision, scale))
    if token in {"list", "fixed_size_list", "embedding"}:
        if "item" not in parameters:
            raise ValueError("native list input requires an item type")
        child = _parse_type(parameters["item"], depth + 1, budget)
        if token == "list":
            return ResultType(token, nullable, (child,))
        size = parameters.get("size")
        if type(size) is not int or not 0 <= size <= (1 << 32) - 1:
            raise ValueError("fixed-size list width must be a uint32")
        if token == "embedding":
            if not size or child.name not in {"float16", "float32", "float64"}:
                raise ValueError("embedding requires a positive fixed-size Float16/Float32/Float64 list")
            metadata = normalize_metadata(parameters.get("metadata"))
            budget[1] += 8 * len(metadata_json(metadata).encode("utf-8"))
            if budget[1] > _MAX_BYTES:
                raise ValueError("nested input schema exceeds 8 MiB metadata")
            return ResultType(token, nullable, (child, size, metadata))
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
    if name in _vector.KINDS:
        return {"Extension": {"id": _vector.IDS[name],
            "metadata": list(_vector.metadata_json(dtype.parameters[1]).encode("utf-8")),
            "storage_dtype": _wire_type(dtype.parameters[0])}}
    if name == "fixed_shape_tensor":
        return {"Extension": {
            "id": _tensor.EXTENSION_ID,
            "metadata": list(_tensor.metadata_json(dtype.parameters[2]).encode("utf-8")),
            "storage_dtype": {"FixedSizeList": [
                _wire_type(dtype.parameters[0]), dtype.parameters[1], nullable]},
        }}
    if name == "variable_shape_tensor":
        return {"Extension": {
            "id": _variable_tensor.EXTENSION_ID,
            "metadata": list(_variable_tensor.metadata_json(dtype.parameters[1]).encode("utf-8")),
            "storage_dtype": _wire_type(dtype.parameters[0]),
        }}
    if name == "embedding":
        return {"Extension": {
            "id": EXTENSION_ID,
            "metadata": list(metadata_json(dtype.parameters[2]).encode("utf-8")),
            "storage_dtype": {"FixedSizeList": [
                _wire_type(dtype.parameters[0]), dtype.parameters[1], nullable]},
        }}
    if name == "binary_object":
        return {"Extension": {
            "id": _BINARY_OBJECT_EXTENSION_ID,
            "metadata": list(_BINARY_OBJECT_METADATA),
            "storage_dtype": {"Binary": nullable},
        }}
    if name in _PRIMITIVES:
        prefix = "u" if name.startswith("uint") else name[0]
        return {"Primitive": [prefix + re.search(r"[0-9]+$", name).group(), nullable]}
    if name in {"bool", "utf8", "binary"}:
        return {{"bool": "Bool", "utf8": "Utf8", "binary": "Binary"}[name]: nullable}
    if name == "decimal128":
        precision, scale = dtype.parameters
        return {"Decimal": [{"precision": precision, "scale": scale}, nullable]}
    if temporal := encode_temporal_dtype(name, nullable):
        return temporal
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
    if dtype.name == "binary_object":
        raise ValueError("managed binary object values require in-process native handles; JSON cell encoding is unavailable")
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
    if name in {"float16", "float32", "float64"}:
        if isinstance(value, bool) or not isinstance(value, (float, int)):
            raise TypeError("native floating input requires numeric values")
        try:
            numeric = float(value)
            exact = math.isfinite(numeric) and (not isinstance(value, int) or numeric == value)
            if name in {"float16", "float32"}:
                code = "!e" if name == "float16" else "!f"
                narrowed = struct.unpack(code, struct.pack(code, numeric))[0]
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
    if name == "binary_object":
        raise ValueError("managed binary object values require in-process native handles; JSON cell encoding is unavailable")
    if name == "decimal128":
        return _decimal(value, dtype)
    if temporal_parts(name) is not None:
        return temporal_input_value(name, value)
    if name in {"list", "fixed_size_list", "embedding", "fixed_shape_tensor"}:
        if isinstance(value, (str, bytes, bytearray, memoryview)) or not isinstance(value, Sequence):
            raise TypeError("native list input requires a sequence")
        if name != "list" and len(value) != dtype.parameters[1]:
            raise ValueError("native fixed-size list input has the wrong width")
        if len(value) > _MAX_BYTES - budget[0]:
            raise ValueError("native input value exceeds 8 MiB")
        return [_value(child, dtype.parameters[0], budget) for child in value]
    if name in _vector.KINDS:
        storage, metadata = dtype.parameters
        _vector.validate_value(name, value, storage, metadata)
        return _value(value, storage, budget)
    if name == "variable_shape_tensor":
        storage, metadata = dtype.parameters
        _variable_tensor.validate_value(value, storage, metadata)
        return _value(value, storage, budget)
    if name == "struct":
        if not isinstance(value, Mapping) or set(value) != {name for name, _ in dtype.parameters}:
            raise ValueError("native struct input must match its complete declared fields")
        result = {}
        for field, child in dtype.parameters:
            _charge(budget, len(field.encode("utf-8")))
            result[field] = _value(value[field], child, budget)
        return result
    raise ValueError("unsupported native input value")


def temporal_input_value(name: str, value: object) -> int:
    """Normalize explicitly supplied objects or ticks without a host-zone lookup."""
    parts = temporal_parts(name)
    if parts is None:
        raise ValueError("unsupported temporal input type")
    family, unit, zone = parts
    micros = None
    if family in {"date32", "date64"}:
        if isinstance(value, dt.datetime):
            raise TypeError("native date input requires a date, not a datetime")
        if isinstance(value, dt.date):
            value = (value - dt.date(1970, 1, 1)).days * (86_400_000 if family == "date64" else 1)
    elif family == "timestamp" and isinstance(value, dt.datetime):
        offset = value.utcoffset()
        if (zone is None) != (offset is None):
            raise ValueError("naive and timezone-bearing timestamp inputs require matching datetime awareness")
        # The caller explicitly supplied this offset. The declared zone is
        # presentation metadata; input does not infer or assign a host zone.
        civil = value.replace(tzinfo=None)
        delta = civil - dt.datetime(1970, 1, 1)
        if offset is not None:
            delta -= offset
        micros = (delta.days * 86_400 + delta.seconds) * 1_000_000 + delta.microseconds
    elif family == "time" and isinstance(value, dt.time):
        if value.tzinfo is not None:
            raise ValueError("time-of-day input cannot carry timezone metadata")
        micros = ((value.hour * 60 + value.minute) * 60 + value.second) * 1_000_000 + value.microsecond
    elif family == "duration" and isinstance(value, dt.timedelta):
        micros = (value.days * 86_400 + value.seconds) * 1_000_000 + value.microseconds
    if micros is not None:
        nanos = micros * 1_000
        divisor = {"s": 1_000_000_000, "ms": 1_000_000, "us": 1_000, "ns": 1}[unit]
        if nanos % divisor:
            raise ValueError("temporal input conversion would discard fractional precision")
        value = nanos // divisor
    return validate_temporal_ticks(name, value)
