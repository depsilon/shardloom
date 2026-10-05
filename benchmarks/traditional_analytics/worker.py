#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Isolated fixture generation and independent comparison-engine calls."""
from __future__ import annotations

import argparse
from dataclasses import asdict
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
from benchmark_models import BenchmarkUnsupported, DatasetPaths
from fixtures import ensure_dataset


def execute(job):
    if job["operation"] == "fixture":
        paths = ensure_dataset(Path(job["root"]), job["rows"], job["dim_rows"],
                               tuple(job["formats"]), job["dataset_profile"])
        return {"status": "passed", "paths": asdict(paths)}
    if job["operation"] != "baseline":
        raise ValueError("unknown benchmark worker operation")
    from baselines import ENGINE_FACTORIES

    runner = ENGINE_FACTORIES[job["engine"]]()
    try:
        paths = DatasetPaths.from_record(job["paths"])
        data_format, scenario = job["format"], job["scenario"]
        if data_format not in runner.formats or scenario not in runner.scenarios:
            raise BenchmarkUnsupported("comparison engine does not admit this declaration")
        if runner.prepare is not None:
            runner.prepare(paths, (data_format,))
        result = runner.scenarios[scenario](paths, data_format)
        return {"status": "passed", "engine": runner.name, "version": runner.version,
                "result": result, "execution_role": "independent_comparison_only"}
    finally:
        if runner.close is not None:
            runner.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--job", type=Path, required=True)
    args = parser.parse_args()
    try:
        result = execute(json.loads(args.job.read_text()))
    except (BenchmarkUnsupported, ImportError) as error:
        result = {"status": "unsupported", "error": f"{type(error).__name__}: {error}"}
    except Exception as error:
        result = {"status": "failed", "error": f"{type(error).__name__}: {error}"}
    print(json.dumps(result, default=str, allow_nan=False))
    return 0 if result["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
