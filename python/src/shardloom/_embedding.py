"""Versioned, inert embedding identity at schema boundaries."""

from __future__ import annotations

import json
from typing import Mapping

EXTENSION_ID = "shardloom.embedding.v1"
_IDENTITY_FIELDS = frozenset({
    "space_id", "model_id", "model_version", "representation_version",
    "normalization", "preprocessing_id", "source_provenance",
})


def normalize_metadata(value: object) -> tuple[tuple[str, object], ...]:
    if value is None:
        value = {}
    if not isinstance(value, Mapping):
        raise TypeError("embedding metadata requires a mapping")
    fields = dict(value)
    fields.setdefault("version", 1)
    if type(fields["version"]) is not int or fields["version"] != 1:
        raise ValueError("embedding metadata version must be 1")
    if set(fields) - (_IDENTITY_FIELDS | {"version"}):
        raise ValueError("unknown embedding metadata field")
    for key, identity in fields.items():
        if key != "version" and identity is not None and (
            not isinstance(identity, str) or not identity
        ):
            raise ValueError("embedding identity fields require nonempty strings or null")
    result = tuple(sorted(fields.items()))
    if len(metadata_json(result).encode("utf-8")) > 8 << 20:
        raise ValueError("embedding metadata exceeds 8 MiB")
    return result


def metadata_json(metadata: tuple[tuple[str, object], ...]) -> str:
    return json.dumps(dict(metadata), ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def decode_metadata(raw: str) -> tuple[tuple[str, object], ...]:
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError("duplicate embedding metadata field")
            result[key] = value
        return result
    value = json.loads(raw, object_pairs_hook=pairs)
    if not isinstance(value, dict) or "version" not in value:
        raise ValueError("embedding metadata version must be 1")
    return normalize_metadata(value)
