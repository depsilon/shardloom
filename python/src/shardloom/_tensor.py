"""Checked, inert fixed tensor shape and physical layout metadata."""

from __future__ import annotations

import json
from typing import Mapping

EXTENSION_ID = "shardloom.fixed_shape_tensor.v1"
ARROW_EXTENSION_ID = "arrow.fixed_shape_tensor"
ELEMENT_TYPES = frozenset({"bool", "float16", "float32", "float64"} | {
    f"{kind}{bits}" for kind in ("int", "uint") for bits in (8, 16, 32, 64)
})


def shape_size(shape: object) -> int:
    if not isinstance(shape, (list, tuple)) or any(
        type(n) is not int or not 0 <= n <= (1 << 63) - 1 for n in shape
    ):
        raise ValueError("tensor shape requires nonnegative int64 dimensions")
    if 0 in shape:
        return 0
    size = 1
    for dimension in shape:
        size *= dimension
        if size > (1 << 32) - 1:
            raise ValueError("tensor shape product exceeds native uint32 list width")
    return size


def normalize_metadata(value: object) -> tuple[tuple[str, object], ...]:
    if not isinstance(value, Mapping) or "shape" not in value:
        raise ValueError("tensor metadata requires shape")
    if set(value) - {"shape", "dim_names", "permutation"}:
        raise ValueError("unknown tensor metadata field")
    shape_size(value["shape"])
    rank = len(value["shape"])
    result = {"shape": tuple(value["shape"])}
    if "dim_names" in value:
        names = value["dim_names"]
        if not isinstance(names, (list, tuple)) or len(names) != rank or any(
            not isinstance(name, str) for name in names
        ):
            raise ValueError("tensor dimension names must be strings matching shape rank")
        result["dim_names"] = tuple(names)
    if "permutation" in value:
        permutation = value["permutation"]
        if (not isinstance(permutation, (list, tuple)) or len(permutation) != rank
                or any(type(n) is not int or not 0 <= n < rank for n in permutation)
                or len(set(permutation)) != rank):
            raise ValueError("tensor permutation must contain each rank index exactly once")
        result["permutation"] = tuple(permutation)
    metadata = tuple(sorted(result.items()))
    if len(metadata_json(metadata).encode("utf-8")) > 8 << 20:
        raise ValueError("tensor metadata exceeds 8 MiB")
    return metadata


def metadata_json(metadata: tuple[tuple[str, object], ...]) -> str:
    return json.dumps(dict(metadata), ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def decode_metadata(raw: str) -> tuple[tuple[str, object], ...]:
    if len(raw.encode("utf-8")) > 8 << 20:
        raise ValueError("tensor metadata exceeds 8 MiB")

    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError("duplicate tensor metadata field")
            result[key] = value
        return result

    return normalize_metadata(json.loads(raw, object_pairs_hook=pairs))


def restore_arrow_schema(table, fields, pyarrow):
    """Retain the native schema when PyArrow normalizes its built-in extension.

    Return ordinary Arrow storage fields carrying canonical extension metadata.
    PyArrow's tensor deserializer replaces component nullability and drops
    present-empty optional arrays. Reattach the authoritative native metadata;
    numeric buffers are shared. A sliced containing struct may rebuild validity
    at this explicit, caller-owned Arrow export boundary.
    """
    from ._result_schema import _arrow_field
    from .errors import ShardLoomProtocolError

    def contains(dtype):
        if dtype.name in {"fixed_shape_tensor", "variable_shape_tensor"}:
            return True
        if dtype.name in {"list", "fixed_size_list"}:
            return contains(dtype.parameters[0])
        if dtype.name == "struct":
            return any(contains(child) for _, child in dtype.parameters)
        return False

    def array(value, dtype):
        if not contains(dtype):
            return value
        target = _arrow_field("item", dtype, pyarrow).type
        if dtype.name == "variable_shape_tensor":
            storage = dtype.parameters[0]
            if isinstance(value.type, pyarrow.BaseExtensionType):
                if value.type.extension_name != _variable_extension_id():
                    raise ShardLoomProtocolError("unexpected variable tensor extension at Arrow export")
                value = value.storage
            if not pyarrow.types.is_struct(value.type):
                raise ShardLoomProtocolError("variable tensor Arrow storage must be a struct")
            return _restore_variable_storage(value, storage, target, pyarrow,
                                            ShardLoomProtocolError)
        if dtype.name == "fixed_shape_tensor":
            if isinstance(value.type, pyarrow.BaseExtensionType):
                if value.type.extension_name != ARROW_EXTENSION_ID:
                    raise ShardLoomProtocolError("unexpected tensor extension at Arrow export")
                value = value.storage
            if not pyarrow.types.is_fixed_size_list(value.type) or value.type.list_size != dtype.parameters[1]:
                raise ShardLoomProtocolError("tensor Arrow storage differs from native shape")
            children = [value.values]
        elif dtype.name in {"list", "fixed_size_list"}:
            children = [array(value.values, dtype.parameters[0])]
        else:
            children = [array(value.field(index), child)
                        for index, (_, child) in enumerate(dtype.parameters)]
            return pyarrow.StructArray.from_arrays(children, fields=list(target),
                mask=value.is_null() if value.null_count else None)
        return pyarrow.Array.from_buffers(target, len(value),
            value.buffers()[:value.type.num_buffers], null_count=value.null_count,
            offset=value.offset, children=children)

    if not any(contains(dtype) for _, dtype in fields):
        return table
    arrays, output_fields = [], []
    for index, (name, dtype) in enumerate(fields):
        if contains(dtype):
            field = _arrow_field(name, dtype, pyarrow)
            arrays.append(pyarrow.chunked_array([array(chunk, dtype)
                for chunk in table.column(index).chunks], type=field.type))
            output_fields.append(field)
        else:
            arrays.append(table.column(index))
            output_fields.append(table.schema.field(index))
    return pyarrow.Table.from_arrays(arrays,
        schema=pyarrow.schema(output_fields, metadata=table.schema.metadata))


def _variable_extension_id():
    from ._variable_tensor import ARROW_EXTENSION_ID
    return ARROW_EXTENSION_ID


def _restore_variable_storage(value, dtype, target, pyarrow, error_type):
    """Rebuild variable-tensor storage wrappers while retaining their buffers."""
    name = dtype.name
    if name == "struct":
        if (not pyarrow.types.is_struct(value.type)
                or len(value.type) != len(dtype.parameters)
                or tuple(value.type.field(index).name for index in range(len(value.type)))
                != tuple(field_name for field_name, _ in dtype.parameters)):
            raise error_type("variable tensor struct storage differs from its schema")
        children = [
            _restore_variable_storage(value.field(index), child, target.field(index).type,
                                      pyarrow, error_type)
            for index, (_, child) in enumerate(dtype.parameters)
        ]
        # Struct.field() already applies its parent's offset. Rebuilding at
        # offset zero avoids applying that offset twice; children keep buffers.
        return pyarrow.StructArray.from_arrays(children, fields=list(target),
            mask=value.is_null() if value.null_count else None)
    if name == "list":
        if not pyarrow.types.is_list(value.type):
            raise error_type("variable tensor data storage must use an Arrow list")
        child = _restore_variable_storage(value.values, dtype.parameters[0],
                                          target.value_type, pyarrow, error_type)
        return pyarrow.Array.from_buffers(
            target, len(value), value.buffers()[:target.num_buffers],
            null_count=value.null_count, offset=value.offset, children=[child])
    if name == "fixed_size_list":
        if (not pyarrow.types.is_fixed_size_list(value.type)
                or value.type.list_size != dtype.parameters[1]):
            raise error_type("variable tensor shape storage must use its declared fixed-size list")
        child = _restore_variable_storage(value.values, dtype.parameters[0],
                                          target.value_type, pyarrow, error_type)
        return pyarrow.Array.from_buffers(
            target, len(value), value.buffers()[:target.num_buffers],
            null_count=value.null_count, offset=value.offset, children=[child])
    if value.type != target:
        raise error_type("variable tensor primitive storage differs from its schema")
    return value
