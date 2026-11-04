# SPDX-License-Identifier: Apache-2.0
"""Run one previously frozen comparison request in a fresh supervised process."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / "scripts"), str(ROOT / "python/src"), str(Path(__file__).resolve().parents[1])]
from query_surface.adapters import execute
from query_surface.corpus import corpus
from query_surface.evidence import no_fallback
from query_surface.expanded_catalog import build
from query_surface.semantics import (
    ComparisonLimit,
    ResultMismatch,
    compare,
    result_from_record,
    result_record,
)
from query_surface.worker import failure, load_native


def request(job):
    row = job["case"]
    if "cell" in row:
        item = build(row)
    else:
        (item,) = (c for c in corpus(interactions="all", include_pending=False) if c.id == row["id"])
    if item.identity != row["case_identity"]:
        raise ValueError("request identity changed since freeze")
    wanted = result_from_record(job["expected"])
    kind = job["engine"]
    if kind == "polars":
        from query_surface.polars_adapter import execute as evaluate, compare_result

        actual, report = evaluate(item.relation, job["sources"], parallelism=job["parallelism"])
        comparison = compare_result
    elif kind == "native":
        native = load_native(job["runtime"]["library"]["path"])
        actual, report = execute(
            item.relation,
            job["sources"],
            job["surface"],
            native=native,
            binary=job["runtime"]["binary"]["path"],
            memory_bytes=job["memory_bytes"],
            parallelism=job["parallelism"],
            timeout=job["timeout"],
            time_public_request_only=True,
        )
        no_fallback(report["fields"])
        comparison = compare
    else:
        raise ValueError("invalid selected engine")
    # The frozen recipe and case identity reproduce this large relation tree.
    # Preserve real submitted SQL/plans, result envelopes, work fields and every row.
    report.pop("public_declaration")
    outcome = {
        "status": "passed",
        "id": item.id,
        "case_identity": item.identity,
        "execution_engine": kind,
        "observation": report,
    }
    try:
        outcome["actual"] = result_record(actual)
    except ResultMismatch as exc:
        outcome.update(
            status="incorrect",
            detail=str(exc),
            invalid_public_result=True,
            observed_rows=repr(actual.rows),
            observed_schema=repr(actual.schema),
        )
        return outcome
    try:
        comparison(wanted, actual, item.contract)
    except ResultMismatch as exc:
        outcome.update(status="incorrect", detail=str(exc))
    except ComparisonLimit as exc:
        outcome.update(status="comparison_inconclusive", detail=str(exc))
    return outcome


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--job", type=Path, required=True)
    args = parser.parse_args()
    job = json.loads(args.job.read_bytes())
    try:
        outcome = request(job)
    except Exception as exc:
        outcome = failure(exc)
        if type(exc).__module__.startswith("polars."):
            outcome["status"] = "engine_error"
        outcome.update(
            id=job["case"]["id"], case_identity=job["case"]["case_identity"], execution_engine=job["engine"]
        )
        outcome["comparison_engine_invoked"] = job["engine"] == "polars"
    print(json.dumps(outcome, allow_nan=False, separators=(",", ":")))


if __name__ == "__main__":
    main()
