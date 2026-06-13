#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Small shared helpers for release, docs, and benchmark evidence scripts."""

from __future__ import annotations

import gzip
import json
from pathlib import Path
from typing import Any


def resolve_path(repo_root: Path, path: Path | str) -> Path:
    candidate = Path(path)
    return candidate if candidate.is_absolute() else repo_root / candidate


def read_text(path: Path, *, missing_ok: bool = True) -> str:
    if not path.exists():
        if missing_ok:
            return ""
        raise FileNotFoundError(path)
    return path.read_text(encoding="utf-8")


def load_json(path: Path, *, missing_ok: bool = False) -> Any:
    if not path.exists():
        if missing_ok:
            return None
        raise FileNotFoundError(path)
    if path.name.endswith(".gz"):
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            return json.load(handle)
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def fail_closed_fields() -> dict[str, bool]:
    return {
        "public_release_claim_allowed": False,
        "public_package_claim_allowed": False,
        "performance_claim_allowed": False,
        "production_claim_allowed": False,
        "spark_replacement_claim_allowed": False,
        "publication_attempted": False,
        "tag_created": False,
        "package_upload_attempted": False,
        "fallback_attempted": False,
        "external_engine_invoked": False,
    }


def require_markers(label: str, text: str, markers: tuple[str, ...]) -> list[str]:
    if not text:
        return [f"{label}: missing file or empty text"]
    return [f"{label}: missing marker {marker!r}" for marker in markers if marker not in text]
