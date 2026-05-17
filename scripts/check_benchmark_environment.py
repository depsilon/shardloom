#!/usr/bin/env python
# SPDX-License-Identifier: Apache-2.0
"""Check benchmark lane availability for a declared ShardLoom profile."""

from __future__ import annotations

import argparse
import importlib
import json
import os
import platform
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from benchmarks.traditional_analytics.benchmark_registry import (  # noqa: E402
    LANES,
    MANIFEST_SCHEMA_VERSION,
    PROFILES,
    expected_lanes_for_profile,
    lane_required_for_profile,
    profile_dict,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", choices=tuple(PROFILES), default="smoke")
    parser.add_argument("--json-output", type=Path, default=None)
    parser.add_argument(
        "--allow-missing-required",
        action="store_true",
        help="Report missing required lanes but return success. Intended for docs/tests only.",
    )
    return parser.parse_args()


def module_version(module_name: str, version_attribute: str) -> tuple[bool, str | None, str]:
    try:
        module = importlib.import_module(module_name)
    except Exception as exc:  # pragma: no cover - exact local dependency state varies
        return False, None, f"{type(exc).__name__}: {exc}"
    version = getattr(module, version_attribute, None)
    return True, str(version) if version is not None else "unknown", "module import succeeded"


def java_status() -> tuple[bool, str | None, str]:
    java = shutil.which("java")
    if not java:
        return False, None, "java executable not found on PATH"
    try:
        completed = subprocess.run(
            [java, "-version"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=10,
            check=False,
        )
    except Exception as exc:  # pragma: no cover - depends on local JDK
        return False, None, f"{type(exc).__name__}: {exc}"
    text = (completed.stderr or completed.stdout).splitlines()
    version = text[0] if text else "java version unknown"
    return completed.returncode == 0, version, "java executable available"


def executable_status(name: str) -> tuple[bool, str | None, str]:
    path = shutil.which(name)
    if not path:
        return False, None, f"{name} executable not found on PATH"
    return True, path, "executable available on PATH"


def gpu_status() -> tuple[bool, str | None, str]:
    cuda_visible = os.environ.get("CUDA_VISIBLE_DEVICES")
    nvidia_smi = shutil.which("nvidia-smi")
    if cuda_visible or nvidia_smi:
        return True, cuda_visible or nvidia_smi, "GPU signal detected"
    return False, None, "no CUDA_VISIBLE_DEVICES or nvidia-smi signal detected"


def check_lane(lane_name: str, profile_name: str) -> dict[str, Any]:
    lane = LANES[lane_name]
    required = lane_required_for_profile(profile_name, lane_name)
    available = True
    version: str | None = "workspace"
    reasons: list[str] = []

    if lane.module:
        module_available, module_version_value, reason = module_version(
            lane.module,
            lane.version_attribute,
        )
        available = available and module_available
        version = module_version_value
        reasons.append(reason)

    if lane.executable:
        executable_available, executable_version, reason = executable_status(lane.executable)
        available = available and executable_available
        version = executable_version if executable_version is not None else version
        reasons.append(reason)

    if lane.requires_java:
        java_available, java_version, reason = java_status()
        available = available and java_available
        reasons.append(reason)
        if java_version and version in {None, "unknown"}:
            version = java_version

    if lane.requires_gpu:
        gpu_available, gpu_version, reason = gpu_status()
        available = available and gpu_available
        reasons.append(reason)
        if gpu_version and version in {None, "unknown"}:
            version = gpu_version

    if not reasons and lane.availability_hint:
        reasons.append(lane.availability_hint)
    elif not reasons:
        reasons.append("workspace-local lane")

    return {
        "lane": lane_name,
        "available": available,
        "required_for_profile": required,
        "version": version if available else None,
        "availability_reason": "; ".join(reasons),
        "group": lane.group,
        "external_baseline_only": lane.external_baseline_only,
        "adapter_backend": lane.adapter_backend,
    }


def environment_report() -> dict[str, Any]:
    return {
        "python": sys.version.split()[0],
        "platform": platform.platform(),
        "machine": platform.machine(),
        "processor": platform.processor(),
        "cpu_count": os.cpu_count(),
        "java_on_path": shutil.which("java") is not None,
    }


def build_report(profile_name: str) -> dict[str, Any]:
    lane_rows = [check_lane(lane, profile_name) for lane in expected_lanes_for_profile(profile_name)]
    available = [row["lane"] for row in lane_rows if row["available"]]
    missing = [row["lane"] for row in lane_rows if not row["available"]]
    missing_required = [
        row["lane"]
        for row in lane_rows
        if row["required_for_profile"] and not row["available"]
    ]
    return {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "benchmark_profile": profile_name,
        "profile": profile_dict(profile_name),
        "expected_lanes": list(expected_lanes_for_profile(profile_name)),
        "available_lanes": available,
        "missing_lanes": missing,
        "missing_required_lanes": missing_required,
        "lane_versions": {
            row["lane"]: row["version"] for row in lane_rows if row["available"]
        },
        "lane_availability_reasons": {
            row["lane"]: row["availability_reason"] for row in lane_rows
        },
        "lane_status": lane_rows,
        "environment": environment_report(),
        "claim_boundary": PROFILES[profile_name].claim_boundary,
        "performance_claim_allowed": False,
    }


def main() -> int:
    args = parse_args()
    report = build_report(args.profile)
    output = json.dumps(report, indent=2, sort_keys=True) + "\n"
    if args.json_output:
        args.json_output.parent.mkdir(parents=True, exist_ok=True)
        args.json_output.write_text(output, encoding="utf-8")
    else:
        print(output, end="")
    if report["missing_required_lanes"] and not args.allow_missing_required:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
