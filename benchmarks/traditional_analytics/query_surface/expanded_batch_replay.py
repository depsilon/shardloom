# SPDX-License-Identifier: Apache-2.0
"""Replay the explicit-plan and unscored operator investigations from captured worker output."""

from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from contextlib import closing
import json
import math
from pathlib import Path
import re
import sqlite3
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / "scripts"), str(ROOT / "python/src"), str(Path(__file__).resolve().parents[1])]
from native_workflow_protocol import strict_json
from query_surface.adapters import relation_sql
from query_surface.expanded_catalog import build, spaces
from query_surface.expanded_explicit_plans import alternate
from query_surface.expanded_range_replay import verify_ranges
from query_surface.expanded_membership_replay import contents
from query_surface.expanded_packet import CASES_PER_PART, bounded_unpack
from query_surface.expanded_replay import (
    bounded,
    lines,
    metadata,
    require,
    sha,
    source_json,
    verify_bindings,
    verify_transport,
)
from query_surface.expanded_report import raw_files, replay

LABELS = ("prepared_window", "left_join_distinct", "multiway_group", "distinct_additive")
WORK = {
    "relational_scan_rows_delivered",
    "native_source_demand_projected_columns",
    "native_source_demand_available_columns",
    "native_source_demand_data_read_bytes",
    "native_input_columnar_buffer_bytes_copied",
    "native_input_ordering_rows_detached",
    "relational_provider_selection_reason",
    "local_primitive_native_io_pushdown_proof_basis",
}


def source_artifacts(archive, base):
    index = strict_json(bounded(archive / "index.json", 2 << 20))
    original = bounded(base / "packet/index.json", 2 << 20)
    require(
        index["format"] == "shardloom.expanded_investigation_batch.v1"
        and index["original_packet_sha256"] == sha(original)
        and index["reader_sha256"] == sha(Path(__file__).read_bytes()),
        "batch source binding differs",
    )
    member = lambda name: contents(archive, name, index["files"][name])
    texts = strict_json(member("proofs.json.xz"))
    hashes = {k: {"bytes": len(v.encode()), "sha256": sha(v.encode())} for k, v in texts.items()}
    require(hashes == index["source_members"], "proof member inventory differs")
    proofs = {k: strict_json(v) for k, v in texts.items() if k.endswith(".json")}
    shape, sample = proofs["shape-protocol.json"], proofs["sample-protocol.json"]
    previous = strict_json(bounded(base / "operator-investigations/index.json", 2 << 20))
    require(
        index["prior_operator_index_sha256"]
        == sha((base / "operator-investigations/index.json").read_bytes())
        and previous["members"]["candidate-runtime.json"] == hashes["runtime-manifest.json"],
        "previously measured runtime differs",
    )
    manifest = proofs["runtime-manifest.json"]
    stamp = shape["runtime_manifest"]["sha256"]
    require(
        stamp == sample["runtime"]["manifest"]["sha256"] == hashes["runtime-manifest.json"]["sha256"]
        and shape["runtime"] == sample["runtime"]["runtime"]
        and manifest["source_revision"] == previous["candidate"]
        and {Path(v["path"]).name: v["sha256"] for v in shape["runtime"].values()} == manifest["products"],
        "probes did not use the same accepted runtime",
    )
    variant = Path(__file__).with_name("expanded_explicit_plans.py").read_bytes()
    require(
        sha(variant) == index["variant_sha256"] == hashes["variant.py"]["sha256"] == shape["rewrite_sha256"]
        and shape["worker_sha256"] == hashes["shape-worker.py"]["sha256"]
        and shape["driver_sha256"] == hashes["shape-driver.py"]["sha256"]
        and sample["worker"]["sha256"] == hashes["sample-worker.py"]["sha256"]
        and sample["driver"]["sha256"] == hashes["sample-driver.py"]["sha256"],
        "diagnostic source differs",
    )
    for prefix in ("shape", "sample"):
        require(
            proofs[prefix + "-complete.json"]["protocol"]["sha256"]
            == hashes[prefix + "-protocol.json"]["sha256"],
            "completion refers to another protocol",
        )
    summary_bytes = bounded(archive / "findings.json", 2 << 20)
    require(
        sha(summary_bytes) == index["findings_sha256"]
        and sha(Path(__file__).with_name("expanded_range_replay.py").read_bytes())
        == index["range_reader_sha256"],
        "review table or range reader differs",
    )
    return member, proofs, strict_json(original), strict_json(summary_bytes)


def selector(cases, excluded):
    selected = {}
    for row in cases:
        name, cell, profile, fmt = row["archetype"], row.get("cell", {}), row["profile"], row["native_format"]
        if profile not in {"primary", "holdout"} or row["id"] in excluded:
            continue
        if name == "rank_hidden_payload" and cell["payload"] in {"numeric", "wide"}:
            key = ("narrow_rank", profile, fmt, cell["payload"])
        elif name == "nullable_expression_order":
            key = ("defer_nullable", profile, fmt, cell["function"])
        elif name in {"subquery_membership", "subquery_null_aware_anti"} and fmt == "vortex":
            key = ("narrow_file_membership", profile, name, cell["build"])
        else:
            continue
        if key not in selected or (
            cell["scope"] == "population" and selected[key]["cell"]["scope"] != "population"
        ):
            selected[key] = row
    return {row["id"]: key for key, row in selected.items()}


def original_cases(base, original, proofs):
    docs, hashes = metadata(base / "packet", original)
    verify_bindings(original, docs, hashes)
    design = docs["design.json"]
    shape, sample = proofs["shape-protocol.json"], proofs["sample-protocol.json"]
    require(
        shape["design_sha256"] == sample["original_frozen_design"] == hashes["design.json"]
        and shape["source_freeze_sha256"] == hashes["frozen.json"]
        and shape["policy"] == sample["policy"] == design["execution"],
        "design or resource policy differs",
    )
    need_ids = {r["id"] for r in shape["rows"]} | {r["id"] for r in sample["selection"]}
    previous = strict_json(bounded(base / "operator-investigations/results.json", 8 << 20))
    excluded = {r["id"] for r in previous["rows"] if r["route"] == "native"}
    sources, status_counts, engine_times = {}, defaultdict(Counter), defaultdict(list)
    scope_reads = Counter()
    fact_families = {
        "ordering",
        "strings",
        "numeric",
        "nullable",
        "windows",
        "grouped_aggregation",
        "global_aggregation",
        "projection",
        "predicates",
        "distinct",
        "nested",
    }
    for i, part in enumerate(original["parts"]):
        for env in lines(base / "packet", f"cases-{i:04d}.jsonl.xz", part, CASES_PER_PART):
            spec = design["cases"][env["seq"]]
            require(spec["id"] == env["id"], "original case order differs")
            status_counts[spec["family"]][env["native"]["status"]] += 1
            if env["native"]["status"] != "passed":
                excluded.add(env["id"])
            else:
                response = strict_json(raw_files(env["native"]["packet"])["one.stdout.json"])
                require(
                    response["id"] == spec["id"] and response["case_identity"] == spec["case_identity"],
                    "original case receipt differs",
                )
                o = response["observation"]
                engine_times[spec["family"]].append(o["clocks"]["engine_execution_seconds"])
                caps = proofs["input-shape.json"]["profiles"][spec["profile"]][
                    "scope_rowgroups_potentially_matching_footer_min_max"
                ]
                scope = spec.get("cell", {}).get("scope")
                fields = o["fields"]
                if (
                    spec["native_format"] == "parquet"
                    and scope in caps
                    and (spec["family"] in fact_families or spec["archetype"] == "calendar_expression_order")
                    and int(fields.get("native_source_demand_row_groups_total", -1)) == caps["population"]
                    and "native_source_demand_row_groups_read" in fields
                ):
                    read = int(fields["native_source_demand_row_groups_read"])
                    scope_reads[
                        "exceeded" if read > caps[scope] else "equal" if read == caps[scope] else "below"
                    ] += 1
            if env["id"] in need_ids:
                require(spec["id"] == env["id"] and spec["id"] not in sources, "original case order differs")
                source_json(env["expected"], "value")
                sources[spec["id"]] = (spec, env)
    require(sources.keys() == need_ids, "missing frozen case")
    census = proofs["derived-inventory.json"]
    require(
        set(status_counts) == set(census["original_family"])
        and dict(scope_reads) == census["scope_upper_bound_check"]["scope_group_read_vs_footer"],
        "family coverage or scope pruning result differs",
    )
    for family, counts in status_counts.items():
        saved = census["original_family"][family]
        require(
            dict(counts) == saved["status"]
            and sum(counts.values()) == saved["declared"]
            and math.fsum(engine_times[family]) == saved["completed_engine_seconds"],
            "original family total differs",
        )
    chosen = selector(design["cases"], excluded)
    require(
        len(chosen) == 33 and chosen == {r["id"]: (r["family"], *r["selector_cell"]) for r in shape["rows"]},
        "case selection differs",
    )
    return (
        sources,
        dict(docs["inputs.json"], runtime=shape["runtime"]),
        docs["native-preparation/complete.json"],
    )


def check_worker(entry, spec, route, item, expected, inputs, prepared, policy):
    packet, _ = bounded_unpack(entry[3], spec["id"])
    verify_transport(packet, spec, route)
    stats = replay(entry, spec, route, expected, item, inputs, prepared, policy)
    require(stats["status"] == "passed", "complete independent result differs")
    files = raw_files(packet)
    worker = strict_json(files["one.stdout.json"])
    fields = worker["observation"]["fields"]
    peak = int(fields["execution_resource_observed_native_peak_reserved_bytes"])
    require(
        0 < peak <= policy["native_memory_bytes"]
        and 0 < stats["peak_process_rss_bytes"] <= policy["process_group_rss_ceiling_bytes"],
        "diagnostic exceeded its resource policy",
    )
    return files, worker, stats, peak


def check_shapes(member, proofs, sources, inputs, prepared, definitions):
    p, complete, result = (
        proofs[k] for k in ("shape-protocol.json", "shape-complete.json", "shape-results.json")
    )
    require(
        complete["queries_dispatched"] == 66
        and complete["statuses"] == {"passed": 66}
        and result["capture"] == complete,
        "paired capture is incomplete",
    )
    data = member("shapes.sqlite.xz")
    require(sha(data) == complete["ledger"]["sha256"], "shape ledger differs")
    schedule = [
        (i, row["id"], role)
        for i, row in enumerate(p["rows"])
        for role in (("alternate", "original") if i % 2 else ("original", "alternate"))
    ]
    compared = []
    with tempfile.TemporaryDirectory() as temp:
        path = Path(temp) / "attempts.sqlite"
        path.write_bytes(data)
        with closing(sqlite3.connect(path.as_uri() + "?mode=ro&immutable=1", uri=True)) as db:
            db.execute("PRAGMA trusted_schema=OFF")
            require(
                db.execute("SELECT type FROM sqlite_master WHERE name='attempt'").fetchall() == [("table",)]
                and db.execute("SELECT seq,id,role FROM attempt ORDER BY rowid").fetchall() == schedule,
                "dispatch order or roles differ",
            )
            for i, row in enumerate(p["rows"]):
                spec, env = sources[row["id"]]
                item = build(spec, definitions)
                require(
                    row["seq"] == i
                    and item.identity == row["case_identity"] == env["expected"]["identity"]
                    and row["expected_blob_sha256"] == env["expected"]["blob_sha256"]
                    and row["archived_native_packet_sha256"] == env["native"]["blob_sha256"],
                    "expected output or original observation differs",
                )
                actual = {
                    "id": spec["id"],
                    "family": row["family"],
                    "format": spec["native_format"],
                    "factors": spec["cell"],
                    "profile": spec["profile"],
                    "surface": spec["surface"],
                }
                for role, relation in (
                    ("original", item.relation),
                    ("alternate", alternate(spec, item.relation)),
                ):
                    entry = db.execute(
                        "SELECT seq,id,status,packet FROM attempt WHERE seq=? AND role=?", (i, role)
                    ).fetchone()
                    files, worker, stats, peak = check_worker(
                        entry, spec, "native", item, env["expected"]["value"], inputs, prepared, p["policy"]
                    )
                    sql = relation_sql(relation, strict_json(files["one.job.json"])["sources"])
                    digest = sha(
                        json.dumps(
                            relation.record(), sort_keys=True, separators=(",", ":"), allow_nan=False
                        ).encode()
                    )
                    require(
                        row[role] == {"relation": digest, "sql": sha(sql.encode())}
                        and worker["observation"]["sql_declaration"] == sql
                        and worker["diagnostic_plan_binding"]
                        == {
                            "role": role,
                            "case_identity": item.identity,
                            "source_plan_sha256": row["original"]["relation"],
                            "submitted_plan_sha256": digest,
                            "submitted_sql_sha256": sha(sql.encode()),
                        },
                        "submitted declaration differs",
                    )
                    fields = worker["observation"]["fields"]
                    actual[role] = {
                        "status": "passed",
                        "detail": stats["failure_detail"],
                        "engine": stats["engine_seconds"],
                        "rows": stats["actual_rows"],
                        "peak_reserved": peak,
                        "peak_rss": stats["peak_process_rss_bytes"],
                        "work": {k: v for k, v in fields.items() if k in WORK},
                    }
                compared.append(actual)
    require(compared == result["rows"], "published per-case results differ")
    groups = {}
    for family in sorted({r["family"] for r in compared}):
        rows = [r for r in compared if r["family"] == family]
        groups[family] = {
            "declared_pairs": len(rows),
            "valid_pairs": len(rows),
            "complete": True,
            "valid_subset": {k: math.fsum(r[k]["engine"] for r in rows) for k in ("original", "alternate")},
            "alternate_faster": sum(r["alternate"]["engine"] < r["original"]["engine"] for r in rows),
        }
    require(groups == result["groups"], "published plan-family totals differ")
    return groups


def main_stack_counts(raw, symbols):
    graph = raw.decode().split("Call graph:\n", 1)[1].split("Total number in stack", 1)[0]
    chain, counts = [], Counter()

    def finish():
        _, total, _, children = chain[-1]
        weight = total - children
        require(weight >= 0, "sample child count exceeds parent")
        if weight:
            names = [x[2] for x in chain]
            counts["all"] += weight
            for key, test in (
                (
                    "window_partition_order_sort",
                    lambda f: "native_relational_order::" in f
                    and "native_relational_window::Window>::evaluate_group" in f,
                ),
                (
                    "ordered_prefix_sort",
                    lambda f: "native_relational_order::" in f
                    and "native_relational_sort::Sort>::ordered_prefix" in f,
                ),
                (
                    "join_pair_flush",
                    lambda f: f.startswith(
                        "<shardloom_vortex::local_primitives::native_relational_join::Spec>::flush"
                    ),
                ),
                (
                    "distinct_accumulation",
                    lambda f: f.startswith(
                        "shardloom_vortex::local_primitives::native_relational_aggregate::accumulate_distinct"
                    ),
                ),
            ):
                if any(test(f) for f in names):
                    counts[key] += weight
            join = any(
                f.startswith("<shardloom_vortex::local_primitives::native_relational_join::Spec>::flush")
                for f in names
            )
            gather = any(
                f.startswith(
                    (
                        "shardloom_vortex::local_primitives::native_relational_batch::take_column_with_policy",
                        "<shardloom_vortex::local_primitives::native_relational_batch::Gather>::column_with_policy",
                    )
                )
                for f in names
            )
            if join and gather:
                counts["gather_in_join_flush"] += weight
        chain.pop()

    root_count = None
    for line in graph.splitlines():
        if not line.strip():
            continue
        m = re.fullmatch(r"([\s+!|:]*)(\d+)\s+(.+)", line)
        require(m is not None, "unrecognized stack graph")
        prefix, digits, frame = m.groups()
        if frame.startswith("Thread_"):
            if root_count is not None:
                break
            require("main-thread" in frame, "main-thread stack is absent")
            root_count = int(digits)
        depth = len(prefix.expandtabs())
        while chain and depth <= chain[-1][0]:
            finish()
        if chain:
            chain[-1][3] += int(digits)
        name = frame.split("  (in ", 1)[0]
        chain.append([depth, int(digits), symbols.get(name, name), 0])
    while chain:
        finish()
    require(counts["all"] == root_count, "main-thread samples do not reconcile")
    return counts


def check_samples(member, proofs, sources, inputs, prepared, definitions):
    plan, complete, summary = (
        proofs[k] for k in ("sample-protocol.json", "sample-complete.json", "sample-summary.json")
    )
    require(
        tuple(r["label"] for r in plan["selection"]) == LABELS
        and complete["queries"] == len(LABELS)
        and plan["interval_ms"] == summary["interval_ms"] == 10,
        "sampled cohort differs",
    )
    symbols = strict_json(member("symbols.json.xz"))
    for row in plan["selection"]:
        label, route = row["label"], row["route"]
        recorded = complete["observations"][label]
        spec, env = sources[row["id"]]
        item = build(spec, definitions)
        require(
            recorded["case"] == row["id"]
            and recorded["route"] == route
            and recorded["reference_identity"]
            == item.identity
            == row["case_identity"]
            == env["expected"]["identity"]
            and env[route]["blob_sha256"] == row["historical"]["packet_blob_sha256"],
            "sampled request differs",
        )
        blob = member(label + ".packet.z.xz")
        require(sha(blob) == recorded["packet"]["sha256"], "sampled result packet differs")
        files, worker, stats, _ = check_worker(
            (0, row["id"], recorded["status"], blob),
            spec,
            route,
            item,
            env["expected"]["value"],
            inputs,
            prepared,
            plan["policy"],
        )
        job = strict_json(files["one.job.json"])
        digest = sha(json.dumps(job, sort_keys=True, separators=(",", ":")).encode())
        require(
            worker["observation"]["sql_declaration"] == relation_sql(item.relation, job["sources"])
            and row["job_digest"] == digest
            and stats["actual_rows"] == recorded["rows"] == 31,
            "sampled request declaration or rows differ",
        )
        sample = recorded["sample"]
        raw = member(label + ".stacks.txt.xz")
        require(
            sha(raw) == sample["sample"]["sha256"] == summary["cases"][label]["sample_sha256"]
            and sample["target"]["pid"] == int(files["one.pid"])
            and sample["target"]["job_sha256"] == sha(files["one.job.json"])
            and sample["sampler_returncode"] == 0,
            "sampling target differs",
        )
        counts = main_stack_counts(raw, symbols)
        for key in (
            "all",
            "window_partition_order_sort",
            "ordered_prefix_sort",
            "join_pair_flush",
            "gather_in_join_flush",
            "distinct_accumulation",
        ):
            require(
                summary["cases"][label]["main_thread_path_counts"].get(key, 0) == counts[key],
                "published sampled stack count differs",
            )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive", type=Path, required=True)
    args = parser.parse_args()
    base = ROOT / "docs/benchmarks/evidence/native-expanded-query-2026-11-03"
    member, proofs, original, review = source_artifacts(args.archive, base)
    sources, inputs, prepared = original_cases(base, original, proofs)
    definitions = {s.name: s for s in spaces()}
    groups = check_shapes(member, proofs, sources, inputs, prepared, definitions)
    check_samples(member, proofs, sources, inputs, prepared, definitions)
    geometry = proofs["field-ranges.json"]
    verify_ranges(member("vortex-footer.bin.xz"), geometry, prepared, inputs)
    require(
        review["field_ranges"]
        == {k: v for k, v in geometry.items() if k not in ("vortex", "parquet", "diagnostic_driver_sha256")}
        and review["shape_groups"] == groups
        and review["sample_main_thread_paths"]
        == {k: v["main_thread_path_counts"] for k, v in proofs["sample-summary.json"]["cases"].items()},
        "review table differs from captured observations",
    )
    derived = proofs["derived-inventory.json"]
    expected_family = {
        k: {f: v[f] for f in ("declared", "status", "completed_engine_seconds")}
        for k, v in derived["original_family"].items()
    }
    require(
        review["original_complete_denominators"] == expected_family
        and sum(v["declared"] for v in expected_family.values()) == 10000
        and sum(v["status"]["passed"] for v in expected_family.values()) == 9995
        and all(
            review[k] == derived[k]
            for k in ("scope_upper_bound_check", "natural_join_order_pairs", "output_only_expressions")
        )
        and review["membership_by_format"] == derived["membership_55_by_format"]
        and review["scale_text"] == proofs["input-shape.json"]["scale_unicode"]
        and review["source_order"]
        == {
            k: {f: n for f, n in v.items() if f != "files"}
            for k, v in proofs["source-order.json"]["profiles"].items()
        },
        "original corpus summary differs",
    )
    print(
        json.dumps(
            {
                "status": "verified",
                "paired_requests": 66,
                "sampled_requests": 4,
                "queries_executed_by_replay": 0,
                "groups": groups,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
