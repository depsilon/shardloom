"""GeoArrow schema/value admission only; geometry operations run in native code."""
from __future__ import annotations

import json
import math
from typing import Mapping, Sequence

KINDS = frozenset({"point", "linestring", "polygon"})
IDS = {kind: "geoarrow." + kind for kind in KINDS}
MAX_METADATA_BYTES = 8 << 20


def normalize_metadata(value=None) -> str:
    value = {} if value is None else value
    if not isinstance(value, Mapping) or set(value) - {"crs", "crs_type", "edges"}:
        raise ValueError("spatial metadata requires only crs, crs_type and edges")
    if "crs" in value and not (isinstance(value["crs"], dict) or
            isinstance(value["crs"], str) and value["crs"]):
        raise ValueError("spatial crs requires an object or nonempty string")
    if "crs_type" in value:
        kind, crs = value["crs_type"], value.get("crs")
        if not ((kind == "projjson" and isinstance(crs, dict)) or
                (kind in {"wkt2:2019", "authority_code", "srid"} and isinstance(crs, str) and crs)):
            raise ValueError("unsupported spatial crs_type or mismatched crs representation")
    if "edges" in value and value["edges"] not in {"spherical", "vincenty", "thomas", "andoyer", "karney"}:
        raise ValueError("unsupported spatial edges; omit edges for planar coordinates")
    raw = json.dumps(dict(value), ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
    if len(raw.encode("utf-8")) > MAX_METADATA_BYTES:
        raise ValueError("spatial metadata exceeds 8 MiB")
    return raw


def decode_metadata(raw: str) -> str:
    if not isinstance(raw, str) or len(raw.encode("utf-8")) > MAX_METADATA_BYTES:
        raise ValueError("spatial metadata requires bounded UTF8 JSON")
    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate spatial metadata field")
            result[key] = value
        return result
    def nonfinite(_):
        raise ValueError("spatial metadata numbers must be finite")
    return normalize_metadata(json.loads(raw, object_pairs_hook=unique, parse_constant=nonfinite))


def storage_type(kind, nullable):
    from ._result_schema import ResultType
    coordinate = ResultType("struct", False, (("x", ResultType("float64", False)), ("y", ResultType("float64", False))))
    if kind == "point":
        return ResultType("struct", nullable, coordinate.parameters)
    vertices = ResultType("list", kind == "linestring" and nullable, (coordinate,))
    return vertices if kind == "linestring" else ResultType("list", nullable, (vertices,))


def validate_storage(kind, storage):
    if kind not in KINDS:
        raise ValueError("unsupported spatial geometry kind")
    coordinate = storage
    for _ in range({"point": 0, "linestring": 1, "polygon": 2}[kind]):
        if coordinate.name != "list" or len(coordinate.parameters) != 1:
            raise ValueError("spatial storage requires native list offsets")
        coordinate = coordinate.parameters[0]
    if (coordinate.name != "struct" or tuple(name for name, _ in coordinate.parameters) != ("x", "y")
            or any(child.name != "float64" for _, child in coordinate.parameters)):
        raise ValueError("spatial coordinates require separated 2D Float64 x/y fields")


def validate_value(kind, value):
    def coordinate(value):
        if not isinstance(value, Mapping) or set(value) != {"x", "y"}:
            raise ValueError("spatial coordinates require exactly x/y fields")
        result = []
        for axis in ("x", "y"):
            item = value[axis]
            if isinstance(item, bool) or not isinstance(item, (int, float)):
                raise ValueError("spatial coordinates require nonnull numeric values")
            number = float(item)
            if not math.isfinite(number) or isinstance(item, int) and number != item:
                raise ValueError("JSON spatial coordinates must be finite and exactly representable; use Arrow or ST_EMPTYPOINT for empty Points")
            result.append(number)
        return tuple(result)
    def vertices(values, ring):
        if not isinstance(values, Sequence) or isinstance(values, (str, bytes, bytearray)):
            raise ValueError("spatial vertices require a sequence")
        if (ring and len(values) < 4) or (not ring and len(values) == 1):
            raise ValueError("LineString requires zero or at least two vertices; rings require at least four")
        first = last = None
        for index, value in enumerate(values):
            last = coordinate(value)
            if index == 0:
                first = last
        if ring and first != last:
            raise ValueError("polygon rings must be closed")
    if kind == "point":
        coordinate(value)
    elif kind == "linestring":
        vertices(value, False)
    else:
        if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
            raise ValueError("polygon requires a sequence of rings")
        for ring in value:
            vertices(ring, True)
