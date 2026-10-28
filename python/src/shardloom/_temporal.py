"""Provider-neutral temporal declarations shared with the native dtype owner.

This module validates temporal syntax and wire metadata only. It deliberately
does not consult the host timezone database; native provider admission owns
zone membership checks.
"""

from __future__ import annotations

from typing import Any

_UNITS = {"s": 0, "ms": 1, "us": 2, "ns": 3}
_TICKS_PER_SECOND = {"s": 1, "ms": 1_000, "us": 1_000_000, "ns": 1_000_000_000}
_I32_MIN = -(1 << 31)
_I32_MAX = (1 << 31) - 1
_I64_MIN = -(1 << 63)
_I64_MAX = (1 << 63) - 1


def _valid_zone(zone: str) -> bool:
    return (
        bool(zone)
        and len(zone) <= 255
        and all(char.isascii() and (char.isalnum() or char in '/_+-.:') for char in zone)
        and not zone.startswith("/")
        and all(part not in {"", ".", ".."} for part in zone.split("/"))
    )


def temporal_parts(name: str) -> tuple[str, str | None, str | None] | None:
    """Return ``(family, unit, timezone)`` for an admitted temporal name."""
    if not isinstance(name, str):
        return None
    if name in {"date32", "date64"}:
        return name, None, None
    if name == "timestamp_micros":
        return "timestamp", "us", None
    if not name.endswith(")") or "(" not in name:
        return None
    family, arguments = name[:-1].split("(", 1)
    if family not in {"time", "timestamp", "duration"}:
        return None
    unit, separator, zone = arguments.partition(",")
    if unit not in _UNITS:
        return None
    if family == "timestamp":
        if separator and not _valid_zone(zone):
            return None
        return family, unit, zone if separator else None
    if separator:
        return None
    return family, unit, None


def canonical_temporal_name(name: str) -> str | None:
    """Return the core owner's canonical name, preserving timezone spelling."""
    parts = temporal_parts(name)
    if parts is None:
        return None
    family, unit, timezone = parts
    if family in {"date32", "date64"}:
        return family
    if family == "timestamp" and unit == "us" and timezone is None:
        return "timestamp_micros"
    if family == "timestamp":
        suffix = f",{timezone}" if timezone is not None else ""
        return f"timestamp({unit}{suffix})"
    return f"{family}({unit})"


def parse_temporal_declaration(name: str) -> str | None:
    """Normalize type/unit spelling while preserving a zone's exact case."""
    if not isinstance(name, str):
        return None
    name = name.strip()
    if name.endswith(")") and "(" in name:
        family, args = name[:-1].split("(", 1)
        unit, separator, zone = args.partition(",")
        suffix = ""
        if separator:
            zone = zone.strip()
            if zone.startswith("'") and zone.endswith("'"):
                zone = zone[1:-1]
            suffix = f",{zone}"
        name = f"{family.strip().lower()}({unit.strip().lower()}{suffix})"
    else:
        name = name.lower()
    return canonical_temporal_name(name)


def _byte_list(value: object) -> list[int] | None:
    if not isinstance(value, list) or not 1 <= len(value) <= 258 or any(
        type(byte) is not int or not 0 <= byte <= 255 for byte in value
    ):
        return None
    return value


def decode_temporal_dtype(
    extension: dict[str, object], storage_name: str, nullable: bool
) -> str | None:
    """Decode Vortex temporal extension metadata without inferring from width.

    ``extension`` may be the payload of an Extension variant (``id`` and
    ``metadata``) or the complete ``{"Extension": payload}`` wire type.
    ``storage_name`` is the declared logical primitive name (``int32`` or
    ``int64``); if the complete wire type includes storage metadata, it must
    agree with both supplied arguments.
    """
    if (
        not isinstance(extension, dict)
        or type(nullable) is not bool
        or not isinstance(storage_name, str)
    ):
        return None
    payload: object = extension
    if set(extension) == {"Extension"}:
        payload = extension["Extension"]
    if not isinstance(payload, dict):
        return None
    if set(payload) not in (
        {"id", "metadata"},
        {"id", "metadata", "storage_dtype"},
    ):
        return None
    identifier = payload.get("id")
    metadata = _byte_list(payload.get("metadata"))
    if metadata is None:
        return None
    if "storage_dtype" in payload:
        wire_storage = payload["storage_dtype"]
        storage_code = {"int32": "i32", "int64": "i64"}.get(storage_name)
        if storage_code is None or wire_storage != {
            "Primitive": [storage_code, nullable]
        }:
            return None
        if type(wire_storage["Primitive"][1]) is not bool:
            return None

    name: str | None = None
    if identifier == "vortex.date":
        if metadata == [4] and storage_name == "int32":
            name = "date32"
        elif metadata == [2] and storage_name == "int64":
            name = "date64"
    elif identifier == "vortex.time":
        time_fixtures = {
            ("int32", (3,)): "time(s)",
            ("int32", (2,)): "time(ms)",
            ("int64", (1,)): "time(us)",
            ("int64", (0,)): "time(ns)",
        }
        name = time_fixtures.get((storage_name, tuple(metadata)))
    elif (
        identifier == "vortex.timestamp"
        and storage_name == "int64"
        and len(metadata) >= 3
    ):
        tag, low, high = metadata[:3]
        zone_length = low | (high << 8)
        if len(metadata) == 3 + zone_length:
            units = {3: "s", 2: "ms", 1: "us", 0: "ns"}
            unit = units.get(tag)
            if unit is not None:
                try:
                    zone = bytes(metadata[3:]).decode("utf-8")
                except UnicodeDecodeError:
                    return None
                if zone:
                    if _valid_zone(zone):
                        name = f"timestamp({unit},{zone})"
                else:
                    name = f"timestamp({unit})"
    elif (
        identifier == "shardloom.duration.v1"
        and storage_name == "int64"
        and len(metadata) == 1
    ):
        unit = {0: "s", 1: "ms", 2: "us", 3: "ns"}.get(metadata[0])
        if unit is not None:
            name = f"duration({unit})"
    return canonical_temporal_name(name) if name is not None else None


def encode_temporal_dtype(name: str, nullable: bool) -> dict[str, object] | None:
    """Encode one admitted temporal name as a Vortex Extension wire dtype."""
    canonical = canonical_temporal_name(name)
    parts = temporal_parts(name)
    if canonical is None or parts is None or type(nullable) is not bool:
        return None
    family, unit, timezone = parts
    if family == "date32":
        identifier, storage, metadata = "vortex.date", "i32", [4]
    elif family == "date64":
        identifier, storage, metadata = "vortex.date", "i64", [2]
    elif family == "time":
        identifier = "vortex.time"
        storage = "i32" if unit in {"s", "ms"} else "i64"
        metadata = [{"s": 3, "ms": 2, "us": 1, "ns": 0}[unit]]
    elif family == "timestamp":
        identifier, storage = "vortex.timestamp", "i64"
        tag = {"s": 3, "ms": 2, "us": 1, "ns": 0}[unit]
        zone_bytes = timezone.encode("utf-8") if timezone is not None else b""
        metadata = [tag, len(zone_bytes) & 0xFF, len(zone_bytes) >> 8, *zone_bytes]
    else:
        identifier, storage = "shardloom.duration.v1", "i64"
        metadata = [{"s": 0, "ms": 1, "us": 2, "ns": 3}[unit]]
    return {
        "Extension": {
            "id": identifier,
            "storage_dtype": {"Primitive": [storage, nullable]},
            "metadata": metadata,
        }
    }


def validate_temporal_ticks(name: str, value: object) -> int:
    """Validate and return exact signed ticks for a declared temporal domain."""
    canonical = canonical_temporal_name(name)
    parts = temporal_parts(name)
    if canonical is None or parts is None:
        raise ValueError(f"unsupported temporal dtype {name!r}")
    if type(value) is not int:
        raise TypeError("temporal ticks require an exact int value")
    family, unit, _timezone = parts
    if family == "date32":
        if not _I32_MIN <= value <= _I32_MAX:
            raise ValueError("date32 ticks exceed signed 32-bit days")
        return value
    if not _I64_MIN <= value <= _I64_MAX:
        raise ValueError("temporal ticks exceed signed 64-bit storage")
    if family == "date64" and value % 86_400_000:
        raise ValueError("date64 requires a whole epoch day in milliseconds")
    if family == "time" and not 0 <= value < 86_400 * _TICKS_PER_SECOND[unit]:
        raise ValueError("time-of-day ticks must be in [midnight, next midnight)")
    return value


def arrow_temporal_type(name: str, pa: Any) -> Any | None:
    """Map a temporal name to its standard Arrow type through a supplied API."""
    parts = temporal_parts(name)
    if parts is None:
        return None
    family, unit, timezone = parts
    if family == "date32":
        return pa.date32()
    if family == "date64":
        return pa.date64()
    if family == "time":
        return pa.time32(unit) if unit in {"s", "ms"} else pa.time64(unit)
    if family == "timestamp":
        return pa.timestamp(unit, tz=timezone)
    if family == "duration":
        return pa.duration(unit)
    return None
