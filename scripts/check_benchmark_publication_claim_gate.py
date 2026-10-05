#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Check that retired benchmark mirrors are absent and public comparison stays ClickBench-only."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from check_benchmark_artifact_completeness import (  # noqa: E402
    CLICKBENCH_URL,
    PUBLIC_BENCHMARK_SURFACE,
    validate_manifest,
)


SCHEMA_VERSION = "shardloom.benchmark_publication_claim_gate.v1"
DEFAULT_MANIFEST = Path("website/assets/benchmarks/latest/manifest.json")
DEFAULT_OUTPUT = Path("target/benchmark-publication-claim-gate-report.json")

# The retired dashboard exposed these generated mirrors. A clean public surface requires all
# eight paths to be absent, including the canonical site manifest.
MIRROR_GROUPS = (
    (
        "benchmark_results",
        (
            "website/assets/benchmarks/latest/benchmark-results.json",
            "website-public/assets/benchmarks/latest/benchmark-results.json",
            "website/assets/data/benchmark-evidence.json",
            "website-public/assets/data/benchmark-evidence.json",
            "website-src/src/data/benchmark-evidence.json",
        ),
    ),
    (
        "benchmark_manifest",
        (
            "website/assets/benchmarks/latest/manifest.json",
            "website-public/assets/benchmarks/latest/manifest.json",
            "website-src/src/data/benchmark-manifest.json",
        ),
    ),
)


def resolve(repo_root: Path, path: Path) -> Path:
    path = Path(path)
    return path if path.is_absolute() else repo_root / path


def inspect_retired_public_mirrors(repo_root: Path) -> dict[str, Any]:
    """Inspect all retired dashboard mirrors without reading their contents."""
    repo_root = Path(repo_root).resolve()
    groups: list[dict[str, Any]] = []
    present_refs: list[str] = []
    for label, refs in MIRROR_GROUPS:
        present = [
            ref
            for ref in refs
            if (repo_root / ref).exists() or (repo_root / ref).is_symlink()
        ]
        present_refs.extend(present)
        groups.append(
            {
                "label": label,
                "status": "passed" if not present else "blocked",
                "refs": list(refs),
                "present_retired_refs": present,
            }
        )
    blockers = []
    if present_refs:
        blockers.append(
            "retired public benchmark mirrors remain present: "
            + ",".join(sorted(present_refs))
        )
    return {
        "status": "passed" if not blockers else "blocked",
        "inspected_path_count": sum(len(refs) for _, refs in MIRROR_GROUPS),
        "groups": groups,
        "present_retired_refs": sorted(present_refs),
        "blockers": blockers,
    }


def _manifest_explanation(path: Path) -> dict[str, Any]:
    """Describe local completeness for context; this never admits publication."""
    if not path.exists():
        return {"status": "not_available", "blockers": []}
    try:
        blockers, _payload = validate_manifest(path)
    except (OSError, ValueError) as error:
        blockers = [str(error)]
    return {
        "status": "passed" if not blockers else "blocked",
        "blockers": blockers,
        "publication_admission": "none",
    }


def validate_publication_claim_gate(
    manifest_path: Path = DEFAULT_MANIFEST,
    *,
    repo_root: Path = ROOT,
) -> dict[str, Any]:
    repo_root = Path(repo_root).resolve()
    requested_manifest = resolve(repo_root, manifest_path)
    canonical_manifest = resolve(repo_root, DEFAULT_MANIFEST)
    is_canonical = requested_manifest.resolve(strict=False) == canonical_manifest.resolve(
        strict=False
    )
    mirror_check = inspect_retired_public_mirrors(repo_root)
    blockers = list(mirror_check["blockers"])

    if not is_canonical:
        blockers.append(
            "requested manifest is not the canonical public site path; local run evidence "
            "has no publication admission"
        )
        if not requested_manifest.exists():
            blockers.append(f"requested custom manifest is missing: {requested_manifest}")
    elif requested_manifest.exists():
        blockers.append("canonical public benchmark manifest remains present")

    status = "passed" if is_canonical and not blockers else "blocked"
    return {
        "schema_version": SCHEMA_VERSION,
        "status": status,
        "manifest": str(requested_manifest),
        "canonical_public_manifest_requested": is_canonical,
        "public_benchmark_surface": PUBLIC_BENCHMARK_SURFACE,
        "public_benchmark_url": CLICKBENCH_URL,
        "evidence_class": "public_surface_absence_check",
        "retired_mirror_absence_check": mirror_check,
        "local_manifest_completeness_explanation": _manifest_explanation(
            requested_manifest
        ),
        "claim_gate_status": "not_claim_grade",
        "runtime_execution_performed": False,
        "benchmark_run_performed": False,
        "performance_claim_allowed": False,
        "performance_equivalence_claim_allowed": False,
        "production_claim_allowed": False,
        "superiority_claim_allowed": False,
        "parity_claim_allowed": False,
        "publication_allowed": False,
        "fallback_attempted": False,
        "external_engine_invoked": False,
        "blockers": blockers,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=ROOT)
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--output", type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    repo_root = args.repo_root.resolve()
    report = validate_publication_claim_gate(args.manifest, repo_root=repo_root)
    serialized = json.dumps(report, indent=2, sort_keys=True) + "\n"
    if args.output is None:
        print(serialized, end="")
    else:
        output = resolve(repo_root, args.output)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(serialized, encoding="utf-8")
        print(output)
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
