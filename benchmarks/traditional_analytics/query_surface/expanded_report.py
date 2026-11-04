# SPDX-License-Identifier: Apache-2.0
"""Replay every result and generate per-case and family evidence, without executing a query."""

from __future__ import annotations

import argparse
import base64
from collections import Counter
from contextlib import ExitStack
import gzip
import hashlib
import json
import math
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / "scripts"), str(ROOT / "python/src"), str(Path(__file__).resolve().parents[1])]
from query_surface.corpus import corpus
from query_surface.evidence import identity, no_fallback, worker_outcome
from query_surface.expanded_catalog import build, spaces
from query_surface.expanded_freeze import read, unpack
from query_surface.expanded_metrics import summarize
from query_surface.expanded_run import admission, closing_db, request, stage_complete
from query_surface.polars_adapter import compare_result
from query_surface.run import write_json
from query_surface.semantics import ComparisonLimit, ResultMismatch, compare, result_from_record


def raw_files(packet):
    verified = {}
    for name, spec in packet["files"].items():
        data = spec["utf8"].encode() if "utf8" in spec else base64.b64decode(spec["base64"], validate=True)
        if len(data) != spec["bytes"] or hashlib.sha256(data).hexdigest() != spec["sha256"]:
            raise ValueError("archived worker file differs: " + name)
        verified[name] = data
    return verified


def replay(entry, row, stage, expected, item, inputs, prepared, policy):
    seq, id_, status, blob = entry
    if id_ != row["id"]:
        raise ValueError("attempt row is outside its declared position")
    if blob is None:
        if status != "incomplete":
            raise ValueError("terminal result lacks its observed packet")
        return dict(
            status=status, engine_seconds=None, workflow_seconds=None, actual_rows=None, missing_packet=True
        )
    packet = unpack(blob)
    files = raw_files(packet)
    job = request(row, stage, inputs, prepared, expected, policy)
    if json.loads(files["one.job.json"]) != job:
        raise ValueError("executed request/reference differs from freeze: " + id_)
    outcome = worker_outcome(packet["supervision"], files["one.stdout.json"])
    if status != packet["classified_status"] or status != outcome["status"]:
        raise ValueError("attempt status differs from raw observation: " + id_)
    report = outcome.get("observation", {})
    result = dict(
        status=status,
        engine_seconds=None,
        workflow_seconds=None,
        actual_rows=None,
        failure_detail=outcome.get("detail"),
        peak_process_rss_bytes=packet["supervision"].get("native_peak_rss_bytes"),
    )
    if status not in {"passed", "incorrect", "comparison_inconclusive"}:
        return result
    if status == "incorrect" and (not report or outcome.get("invalid_public_result") is True):
        result["delivered_invalid_result"] = True
        return result
    if (
        outcome["id"] != id_
        or outcome["case_identity"] != item.identity
        or row["case_identity"] != item.identity
        or outcome["execution_engine"] != job["engine"]
        or report.get("complete_consumption") is not True
    ):
        raise ValueError("result is not bound to the completed public request")
    actual = result_from_record(outcome["actual"])
    observed = "passed"
    try:
        (compare_result if stage == "polars" else compare)(
            result_from_record(expected), actual, item.contract
        )
    except ResultMismatch:
        observed = "incorrect"
    except ComparisonLimit:
        observed = "comparison_inconclusive"
    if status != observed:
        raise ValueError("offline complete-result comparator disagrees for " + id_)
    clocks = report["clocks"]
    elapsed = clocks["complete_workflow_seconds"]
    if type(elapsed) not in (int, float) or not math.isfinite(elapsed) or elapsed < 0:
        raise ValueError("completed request lacks a valid API-through-delivery clock")
    if stage == "polars":
        if clocks["engine_execution_seconds"] is not None or report["execution_engine"] != "polars":
            raise ValueError("invalid external-engine clock attribution")
        if report["thread_pool_size"] != policy["threads"]:
            raise ValueError("Polars parallelism differs")
    else:
        fields = report["fields"]
        no_fallback(fields)
        if report["surface"] != job["surface"]:
            raise ValueError("observed public interface differs")
        e = clocks["engine_execution_seconds"]
        if e is not None and (type(e) not in (int, float) or not math.isfinite(e) or e < 0):
            raise ValueError("invalid native engine clock")
        result["engine_seconds"] = e
        # Retain the original field labels and values. They describe particular
        # providers and operators, and are never summed as interchangeable bytes.
        result["work"] = {
            key: value
            for key, value in fields.items()
            if len(str(value)) <= 500
            and (
                key.startswith(
                    (
                        "native_source_demand_",
                        "relational_scan_",
                        "relational_group_",
                        "relational_join_",
                        "relational_window_",
                        "local_primitive_sort_",
                        "execution_resource_observed_",
                    )
                )
                or key
                in (
                    "relational_provider_selection_reason",
                    "local_primitive_rows_scanned",
                    "local_primitive_rows_selected",
                    "local_primitive_group_output_strategy",
                    "local_primitive_group_admission_strategy",
                    "local_primitive_aggregate_update_strategy",
                )
            )
        }
    result.update(actual_rows=len(actual.rows), workflow_seconds=elapsed)
    return result


def preparation_summary(root, prepared, inputs):
    parts = []
    for profile, proof in prepared["profiles"].items():
        record = read(Path(proof["manifest"]["path"]))
        table_by_path = {p: name for name, paths in proof["sources"].items() for p in paths}
        for pair in record["receipts"]:
            saved = read(Path(pair["prepare"]))
            envelope = read(Path(saved["stdout"]))
            if saved["returncode"] or saved["guard_failures"] or envelope["status"] != "success":
                raise ValueError("preparation receipt was not successful")
            fields = {v["key"]: v["value"] for v in envelope["fields"]}
            no_fallback(fields)
            path = saved["command"][3]
            parts.append(
                dict(
                    profile=profile,
                    table=table_by_path[path],
                    source=saved["command"][2],
                    input_bytes=Path(saved["command"][2]).stat().st_size,
                    prepared_bytes=Path(path).stat().st_size,
                    reopened_rows=int(fields["reopen_row_count"]),
                    preparation_process_seconds=saved["seconds"],
                    peak_rss_bytes=saved["native_peak_rss_bytes"],
                    receipt=identity(Path(pair["prepare"])),
                    observed_work={
                        k: v
                        for k, v in fields.items()
                        if "work_nanos" in k
                        or k
                        in {
                            "vortex_ingest_text_active_nanos",
                            "source_state_materialization_layout",
                            "source_bytes",
                            "source_state_materialized_column_count",
                        }
                    },
                )
            )
    return dict(
        status="verified",
        primary_input="existing 1M prepared generation with historical complete readback",
        new_source="equivalent Parquet, each persisted part checked against its original Arrow IPC",
        primary_rows=inputs["profiles"]["primary"]["spec"]["fact_rows"],
        parts=parts,
        clock="one CLI preparation process per part; complete readback, hashing and subsequent queries are separate",
        work_scope="reported owner work counters may overlap; they do not sum to end-to-end elapsed time",
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    root = args.root.resolve()
    args.output.mkdir(exist_ok=False)
    design, inputs, _ = admission(root)
    prepared = read(root / "native-preparation/complete.json")
    original = {c.id: c for c in corpus(interactions="all", include_pending=False)}
    definitions = {s.name: s for s in spaces()}
    rows = []
    surrogates = []
    with ExitStack() as stack:

        def db(path):
            return stack.enter_context(closing_db(path, readonly=True))

        stages = {}
        for stage in ("polars", "native", "alternate"):
            count = len(design["alternate_route_cases"]) if stage == "alternate" else design["count"]
            complete = stage_complete(root, stage, count)
            stages[stage] = iter(
                db(Path(complete["ledger"]["path"])).execute("SELECT * FROM attempt ORDER BY seq")
            )
        refs = {
            profile: db(root / ("reference-" + profile) / "expectations.sqlite")
            for profile in inputs["profiles"]
        }
        writer = stack.enter_context(gzip.open(args.output / "cases.jsonl.gz", "xt", encoding="utf-8"))
        for index, r in enumerate(design["cases"]):
            c = build(r, definitions) if "cell" in r else original[r["id"]]
            ident, blob = (
                refs[r["profile"]]
                .execute("SELECT identity,value FROM expected WHERE id=?", (c.id,))
                .fetchone()
            )
            if ident != c.identity or c.identity != r["case_identity"]:
                raise ValueError("replayed intent differs")
            wanted = unpack(blob)
            out = {
                k: r[k]
                for k in (
                    "id",
                    "family",
                    "archetype",
                    "profile",
                    "surface",
                    "native_format",
                    "polars_format",
                    "partition",
                    "surrogate",
                )
            }
            for stage in ("polars", "native"):
                e = next(stages[stage])
                if e[0] != index:
                    raise ValueError("query execution order differs")
                out[stage] = replay(e, r, stage, wanted, c, inputs, prepared, design["execution"])
            if r["surrogate"]:
                extra = next(stages["alternate"])
                if extra[0] != len(surrogates) or r["id"] != design["alternate_route_cases"][extra[0]]:
                    raise ValueError("alternate-source observation is out of order")
                alt = replay(extra, r, "alternate", wanted, c, inputs, prepared, design["execution"])
                surrogates.append(
                    dict(
                        id=c.id,
                        label=r["surrogate"],
                        cell=r["cell"],
                        polars=out["polars"],
                        native_parquet=out["native"],
                        native_prepared_vortex=alt,
                    )
                )
            writer.write(json.dumps(out, separators=(",", ":"), allow_nan=False) + "\n")
            rows.append({**out, "native": {k: v for k, v in out["native"].items() if k != "work"}})
        if any(next(stream, None) is not None for stream in stages.values()):
            raise ValueError("extra attempt records")
    overview = summarize(rows)
    for engine in ("native", "polars"):
        outcome = [r[engine] for r in rows]
        overview[engine + "_accounting"] = dict(
            generated=len(rows),
            attempt_slots=len(outcome),
            returned_complete_typed_result=sum(r["actual_rows"] is not None for r in outcome),
            correct=sum(r["status"] == "passed" for r in outcome),
            statuses=dict(Counter(r["status"] for r in outcome)),
            interrupted_or_unexecuted=sum(r["status"] in {"incomplete", "unexecuted"} for r in outcome),
        )
    write_json(args.output / "metrics.json", overview)
    write_json(args.output / "surrogates.json", surrogates)
    write_json(args.output / "preparation.json", preparation_summary(root, prepared, inputs))
    write_json(
        args.output / "replay.json",
        dict(
            status="passed_offline_result_replay",
            unique_cases=len(rows),
            baseline_attempts=len(rows),
            primary_native_attempts=len(rows),
            alternate_native_attempts=len(surrogates),
            case_rows=identity(args.output / "cases.jsonl.gz"),
            metrics=identity(args.output / "metrics.json"),
            surrogate_rows=identity(args.output / "surrogates.json"),
            frozen=identity(root / "frozen.json"),
            limitations="One observation per request and route with uncontrolled filesystem caches. "
            "No Polars engine-only timing or general performance superiority follows.",
        ),
    )
    print(
        json.dumps(
            {
                "verified": len(rows),
                "paired_scale_checks": len(surrogates),
                "native_status": dict(Counter(r["native"]["status"] for r in rows)),
                "polars_status": dict(Counter(r["polars"]["status"] for r in rows)),
            }
        )
    )


if __name__ == "__main__":
    main()
