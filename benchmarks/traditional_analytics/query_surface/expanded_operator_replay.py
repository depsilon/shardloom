# SPDX-License-Identifier: Apache-2.0
"""Replay the bounded operator investigation from portable, immutable evidence."""

import argparse
from collections import Counter
from contextlib import ExitStack, closing
import hashlib
import json
import math
from pathlib import Path
import sqlite3
import statistics
import sys
import tempfile


def verify_published_result(archive, index, result, bounded, require, sha, strict_json):
    """Bind a published result to both its sealed bytes and independent replay."""
    if "result_sha256" in index:
        published = bounded(archive / "results.json", 8 << 20)
        require(
            sha(published) == index["result_sha256"] and strict_json(published) == result,
            "published results differ",
        )


def print_verification_summary(result, checked):
    """Emit the stable, query-free verifier summary."""
    print(
        json.dumps(
            {
                "verified": True,
                "matched": result["matched"],
                "challenge_status": checked[-1]["candidate"]["status"],
                "query_executions": 0,
            },
            sort_keys=True,
        )
    )


def verify_dispatch_proofs(index, proofs, plan, bound, completed, require):
    """Check predeclared dispatch, capture completion and isolation receipts."""
    selected = proofs["pre-score-disposition.json"]["selected_r2"]
    require(
        selected["protocol_sha256"] == index["members"]["protocol.json"]["sha256"]
        and selected["driver_sha256"] == index["members"]["driver.py"]["sha256"]
        and selected["rehearsal_sha256"] == index["members"]["rehearsal.json"]["sha256"]
        and selected["order"] == plan["execution_contract"]["order"] == bound["order"],
        "predeclared dispatch differs",
    )
    for key, name in (("protocol", "protocol.json"), ("driver", "driver.py")):
        require(bound[key]["sha256"] == index["members"][name]["sha256"], "dispatch binding differs")
    require(
        completed["binding"]["sha256"] == index["members"]["binding.json"]["sha256"]
        and completed["protocol"]["sha256"] == index["members"]["protocol.json"]["sha256"]
        and completed["posthash_verified"]
        and completed["status"] == "captured_once",
        "capture is incomplete",
    )
    require(
        plan["selector"] == proofs["prior-protocol.json"]["selector"], "pre-score revision changed requests"
    )
    isolation = proofs["isolation.json"]
    capture_guard = proofs["validation/matched-capture-r2.json"]
    require(
        isolation["driver_sha256"] == index["members"]["isolation.py"]["sha256"]
        and isolation["capture_returncode"] == 0
        and [o["pid"] for o in isolation["held_task_observers"]] == isolation["resumed_pids"]
        and capture_guard["status"] == "passed"
        and capture_guard["returncode"] == 0
        and capture_guard["process_group_drained"]
        and capture_guard["direct_child_reaped"]
        and capture_guard["query_workload"]
        and capture_guard["working_tree"] == ""
        and capture_guard["head"] == index["candidate"]
        and capture_guard["driver_sha256"] == index["members"]["guard.py"]["sha256"]
        and capture_guard["log_sha256"] == index["members"]["validation/matched-capture-r2.log"]["sha256"],
        "capture supervision or workload isolation differs",
    )
    require(
        all(
            item["scopes"] == ["active_output"]
            for item in capture_guard["storage_inventory_scopes"]
            if item["child_started"] and not item["forced"]
        ),
        "unexpected inventory during capture",
    )


def summarize_replay(index, bound, proofs, counts, checked, clock_table, declarations, require):
    """Reduce replay observations and reconcile them with the captured replay."""
    full = [row for row in checked if row["cohort"] == "matched"]

    def reduce(rows):
        complete = all(
            row[role]["status"] == "passed" and row[role]["engine_seconds"] is not None
            for row in rows
            for role in ("parent", "candidate")
        )
        changes = (
            Counter(
                "lower"
                if row["candidate"]["engine_seconds"] < row["parent"]["engine_seconds"]
                else "higher"
                if row["candidate"]["engine_seconds"] > row["parent"]["engine_seconds"]
                else "equal"
                for row in rows
            )
            if complete
            else {}
        )
        return {
            "matched_case_routes": len(rows),
            "complete": complete,
            "candidate_case_directions": dict(changes),
            **{
                role + "_engine_seconds": math.fsum(row[role]["engine_seconds"] for row in rows)
                if complete
                else None
                for role in ("parent", "candidate")
            },
        }

    result = {
        "format": index["format"],
        "queries_executed_by_replay": 0,
        "sources": {k: bound["roles"][k]["source_revision"] for k in ("parent", "candidate")},
        "statuses": [[list(key), n] for key, n in sorted(counts.items())],
        "matched": reduce(full),
        "by_investigation": {k: reduce([r for r in full if r["investigation"] == k]) for k in "ABC"},
        "challenge": checked[-1],
        "rows": full,
        "archived_surface_clocks": clock_table,
        "fresh_uncollected_declarations": declarations,
    }
    local = proofs["local-replay.json"]
    require(
        result["matched"]["complete"] == local["paired_engine_sum_valid"]
        and all(
            result["matched"][role + "_engine_seconds"] == local[role + "_engine_sum"]
            for role in ("parent", "candidate")
        )
        and local["challenge"]["candidate"]["status"] == checked[-1]["candidate"]["status"]
        and local["challenge_separate_from_matched_sum"],
        "capture replay and portable replay differ",
    )
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, required=True)
    parser.add_argument("--archive", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    repo, archive = args.repo.resolve(), args.archive.resolve()
    sys.path[:0] = [
        str(repo / "scripts"),
        str(repo / "python/src"),
        str(repo / "benchmarks/traditional_analytics"),
    ]
    from native_workflow_protocol import strict_json
    from query_surface.expanded_catalog import build, spaces
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

    base = repo / "docs/benchmarks/evidence/native-expanded-query-2026-11-03/packet"
    base_index_bytes = bounded(base / "index.json", 2 << 20)
    index = strict_json(bounded(archive / "index.json", 2 << 20))
    require(
        index["format"] == "shardloom.expanded_operator_investigations.v1"
        and index["original_packet_sha256"] == sha(base_index_bytes),
        "original archive differs",
    )
    require(index["verifier_sha256"] == sha(bounded(Path(__file__), 1 << 20)), "verifier source differs")
    require(args.output is not None or "result_sha256" in index, "published results are not sealed")
    raw_proofs = contents(archive, "proofs.json.xz", index["files"]["proofs.json.xz"])
    text = strict_json(raw_proofs)
    require(set(text) == set(index["members"]), "proof inventory differs")
    for name, source in text.items():
        raw = source.encode()
        require(index["members"][name] == {"bytes": len(raw), "sha256": sha(raw)}, "proof differs: " + name)
    proofs = {k: strict_json(v) for k, v in text.items() if k.endswith(".json")}
    plan, bound, completed = (proofs[k] for k in ("protocol.json", "binding.json", "complete.json"))
    verify_dispatch_proofs(index, proofs, plan, bound, completed, require)
    original = strict_json(base_index_bytes)
    docs, hashes = metadata(base, original)
    verify_bindings(original, docs, hashes)
    design, inputs, prepared = (
        docs["design.json"],
        docs["inputs.json"],
        docs["native-preparation/complete.json"],
    )
    require(
        plan["policy"] == bound["policy"] == design["execution"]
        and plan["source"]["design"]["sha256"] == hashes["design.json"]
        and plan["source"]["frozen"]["sha256"] == hashes["frozen.json"],
        "source or execution policy differs",
    )
    cmd = [
        "cargo",
        "build",
        "--locked",
        "--offline",
        "--release",
        "--features",
        "release-user-surfaces",
        "-p",
        "shardloom-cli",
        "-p",
        "shardloom-python",
    ]
    for role in ("parent", "candidate"):
        runtime, receipt = proofs[role + "-runtime.json"], proofs[role + "-build.json"]
        binding = bound["roles"][role]
        require(
            binding["manifest"]["sha256"] == index["members"][role + "-runtime.json"]["sha256"]
            and runtime["build_receipt_sha256"]
            == binding["build_receipt"]["sha256"]
            == index["members"][role + "-build.json"]["sha256"],
            "role artifact differs: " + role,
        )
        require(
            receipt["head"] == binding["source_revision"] == runtime["source_revision"]
            and receipt["command"] == cmd
            and receipt["status"] == "passed"
            and receipt["process_group_drained"]
            and receipt["working_tree"] == ""
            and not receipt["changed_source_inventory"]
            and runtime["profile"] == "release"
            and runtime["features"] == ["release-user-surfaces"]
            and runtime["source_overlay"]["bytes"] == 0
            and not runtime["untracked_source_paths"],
            "unverified build",
        )
        require(
            receipt["log_sha256"] == index["members"][role + "-build.log"]["sha256"]
            and all(
                f"Compiling {crate} v1.1.0 ({receipt['cwd']}/{crate})" in text[role + "-build.log"]
                for crate in ("shardloom-vortex", "shardloom-cli", "shardloom-python")
            ),
            "fresh product compilation differs",
        )
        for key, product in (("binary", "shardloom"), ("library", "_native.abi3.so")):
            require(binding["runtime"][key]["sha256"] == runtime["products"][product], "product differs")
        commit = text[role + ".commit"].encode()
        git_id = hashlib.sha1(f"commit {len(commit)}\0".encode() + commit, usedforsecurity=False).hexdigest()
        require(git_id == receipt["head"], "commit object differs")
    require(
        bound["roles"]["parent"]["source_revision"] == plan["runtime_contract"]["parent_source_revision"]
        and all(
            proofs["parent-runtime.json"]["products"][name]
            != proofs["candidate-runtime.json"]["products"][name]
            for name in ("shardloom", "_native.abi3.so")
        ),
        "roles are not distinct",
    )
    snapshots = [proofs[k + "-runtime.json"]["source_snapshot"] for k in ("parent", "candidate")]
    changed = sorted(
        p for p in snapshots[0].keys() | snapshots[1].keys() if snapshots[0].get(p) != snapshots[1].get(p)
    )
    match = proofs["source-match.json"]
    require(
        match["changed_source_paths"] == changed
        and match["git_diff_paths"] == changed
        and match["patch_sha256"] == index["members"]["candidate.patch"]["sha256"]
        and [bound["roles"][k]["source_revision"] for k in ("parent", "candidate")]
        == [match["parent"], match["candidate"]],
        "role source snapshots or patch differ",
    )
    matched, challenge = plan["selector"]["rows"], plan["selector"]["challenge"]
    require(
        len(matched) == 55
        and len({(r["id"], r["route"]) for r in matched}) == 55
        and len({r["id"] for r in matched}) == 54
        and [r["seq"] for r in matched] == list(range(55))
        and challenge["id"] == "v2_single_key_reducer_d7da0557c5fb3f58"
        and challenge["route"] == "alternate"
        and (challenge["id"], challenge["route"]) not in {(r["id"], r["route"]) for r in matched},
        "case-route inventory differs",
    )
    contract = plan["execution_contract"]
    require(
        contract["matched_attempts_per_case_route_role"] == contract["challenge_attempts"] == 1
        and all(contract[k] for k in ("no_retries", "no_warmups", "no_polars", "no_topk_rerun"))
        and challenge["historical"]["status"] == "guard_failed"
        and challenge["historical"]["peak_process_rss_bytes"]
        > plan["policy"]["process_group_rss_ceiling_bytes"],
        "capture or historical resource challenge differs",
    )
    profile = proofs["declaration-diagnostic.json"]
    require(
        profile["subprocesses"] == 2
        and profile["query_executions"] == 0
        and profile["driver_sha256"] == index["members"]["declaration-diagnostic.py"]["sha256"],
        "declaration diagnostic differs",
    )
    required = {r["id"] for r in [*matched, challenge]} | {r["case_id"] for r in profile["observations"]}
    cases = {r["id"]: (i, r) for i, r in enumerate(design["cases"]) if r["id"] in required}
    require(cases.keys() == required, "absent original request")
    envelopes, clocks = {}, {}
    for part in range(len(original["parts"])):
        for row in lines(base, f"cases-{part:04d}.jsonl.xz", original["parts"][part], CASES_PER_PART):
            if row["native"]["status"] == "passed":
                source_json(row["native"], "packet")
                response = strict_json(raw_files(row["native"]["packet"])["one.stdout.json"])
                observation = response["observation"]
                require(
                    response["id"] == row["id"]
                    and response["execution_engine"] == "native"
                    and observation["complete_consumption"],
                    "incomplete original delivery",
                )
                t = observation["clocks"]
                residual = t["complete_workflow_seconds"] - t["engine_execution_seconds"]
                require(residual >= -1e-9, "request clock precedes engine completion")
                clocks.setdefault(observation["surface"], []).append(
                    (t["setup_and_declaration_seconds"], t["delivery_seconds"], max(residual, 0.0))
                )
            if row["id"] in required:
                require(
                    row["seq"] == cases[row["id"]][0] and row["id"] not in envelopes,
                    "duplicate or reordered original",
                )
                source_json(row["expected"], "value")
                envelopes[row["id"]] = row
    clock_table = proofs["surface-clock-table.json"]["surfaces"]
    require(
        clocks.keys() == clock_table.keys() and sum(map(len, clocks.values())) == 9995,
        "surface clock denominator differs",
    )
    for name, samples in clocks.items():
        reported = clock_table[name]
        require(len(samples) == reported["successful_native_requests"], "surface count differs")
        for i, field in enumerate(
            (
                "median_setup_and_declaration_seconds",
                "median_delivery_seconds",
                "median_request_minus_engine_seconds",
            )
        ):
            require(statistics.median(r[i] for r in samples) == reported[field], "surface median differs")
        require(
            math.isclose(
                math.fsum(r[2] for r in samples), reported["sum_request_minus_engine_seconds"], rel_tol=1e-12
            ),
            "surface request residual differs",
        )
    declarations = []
    for record in profile["observations"]:
        require(
            record["status"] == "passed"
            and record["case_identity"] == cases[record["case_id"]][1]["case_identity"]
            and record["frozen_job_sha256"]
            == sha(raw_files(envelopes[record["case_id"]]["native"]["packet"])["one.job.json"]),
            "fresh declaration is not bound to original case",
        )
        state = record["postcondition"]
        require(
            state["plan_constructed"]
            and not state["collected"]
            and not state["client_worker_started"]
            and not state["native_query_or_backend_started_by_client"]
            and state["active_client_processes"] == 0
            and not state["process_audit_events"],
            "declaration launched work",
        )
        declarations.append(
            {
                "id": record["case_id"],
                "surface": record["surface"],
                "stages": {s["name"]: s["elapsed_ns"] for s in record["stages"]},
            }
        )
    definitions = {s.name: s for s in spaces()}
    counts, checked = Counter(), []
    with tempfile.TemporaryDirectory() as tmp, ExitStack() as stack:
        databases = {}
        for name, total in (("matched", 110), ("challenge", 1)):
            filename = name + ".sqlite.xz"
            raw = contents(archive, filename, index["files"][filename])
            require(sha(raw) == completed["ledgers"][name]["sha256"], "ledger differs")
            path = Path(tmp) / (name + ".sqlite")
            path.write_bytes(raw)
            db = stack.enter_context(
                closing(sqlite3.connect(path.as_uri() + "?mode=ro&immutable=1", uri=True))
            )
            db.execute("PRAGMA trusted_schema=OFF")
            require(
                db.execute("SELECT type,name FROM sqlite_master WHERE name='attempt'").fetchall()
                == [("table", "attempt")]
                and db.execute("SELECT COUNT(*) FROM attempt").fetchone() == (total,),
                "unexpected ledger shape",
            )
            databases[name] = db
        dispatch = [
            (seq, row["id"], row["route"], role)
            for seq, row in enumerate(matched)
            for role in (("candidate", "parent") if seq % 2 else ("parent", "candidate"))
        ]
        require(
            databases["matched"].execute("SELECT seq,id,route,role FROM attempt ORDER BY rowid").fetchall()
            == dispatch,
            "recorded role order differs",
        )
        for cohort, inventory in (("matched", matched), ("challenge", [challenge])):
            for seq, frozen in enumerate(inventory):
                identifier, route = frozen["id"], frozen["route"]
                spec, cell = cases[identifier][1], envelopes[identifier]
                expected, item = cell["expected"]["value"], build(spec, definitions)
                require(
                    item.identity
                    == cell["expected"]["identity"]
                    == frozen["case_identity"]
                    == spec["case_identity"],
                    "semantic identity differs",
                )
                require(
                    frozen["independent_expected_identity"] == item.identity
                    and cell["expected"]["blob_sha256"] == frozen["independent_expected_value_sha256"],
                    "independent expectation differs",
                )
                source_json(cell[route], "packet")
                require(
                    cell[route]["blob_sha256"] == frozen["historical"]["packet_blob_sha256"]
                    and cell[route]["status"] == frozen["historical"]["status"]
                    and sha(raw_files(cell[route]["packet"])["one.job.json"])
                    == frozen["historical"]["job_sha256"],
                    "historical packet differs",
                )
                observed = {}
                for role in ("parent", "candidate") if cohort == "matched" else ("candidate",):
                    entry = (
                        databases[cohort]
                        .execute(
                            "SELECT seq,id,status,packet FROM attempt WHERE seq=? AND id=? AND route=? AND role=?",
                            (seq, identifier, route, role),
                        )
                        .fetchone()
                    )
                    require(entry is not None, "missing captured attempt")
                    packet, _ = bounded_unpack(entry[3], identifier)
                    verify_transport(packet, spec, route)
                    stats = replay(
                        entry,
                        spec,
                        route,
                        expected,
                        item,
                        dict(inputs, runtime=bound["roles"][role]["runtime"]),
                        prepared,
                        design["execution"],
                    )
                    counts[(cohort, role, stats["status"])] += 1
                    values = {
                        "status": stats["status"],
                        "engine_seconds": stats["engine_seconds"],
                        "rows": stats["actual_rows"],
                        "peak_rss_bytes": stats["peak_process_rss_bytes"],
                    }
                    if stats["status"] == "passed":
                        o = strict_json(raw_files(packet)["one.stdout.json"])["observation"]
                        f = o["fields"]
                        peak = int(f["execution_resource_observed_native_peak_reserved_bytes"])
                        require(
                            0 < peak <= plan["policy"]["native_memory_bytes"]
                            and 0
                            < values["peak_rss_bytes"]
                            <= plan["policy"]["process_group_rss_ceiling_bytes"],
                            "resource guard differs",
                        )
                        values["peak_reserved_bytes"] = peak
                        fields = (
                            "relational_provider_selection_reason",
                            "relational_scan_rows_delivered",
                            "native_source_demand_projected_columns",
                            "native_source_demand_available_columns",
                            "native_source_demand_data_read_bytes",
                            "native_source_demand_row_groups_read",
                            "native_input_columnar_buffer_bytes_copied",
                            "native_input_columnar_copy_reasons",
                            "local_primitive_candidate_groups",
                            "local_primitive_peak_buffered_order_candidates",
                            "local_primitive_retained_candidate_groups",
                            "local_primitive_compact_group_state_strategy",
                            "local_primitive_group_output_strategy",
                            "local_primitive_aggregate_update_strategy",
                            "local_primitive_estimated_group_key_storage_bytes",
                            "local_primitive_materialized_group_value_count",
                            "local_primitive_estimated_group_string_storage_bytes",
                            "local_primitive_aggregate_first_pass_scan_next_nanos",
                            "local_primitive_aggregate_first_pass_group_update_nanos",
                            "local_primitive_aggregate_result_finalization_nanos",
                            "local_primitive_native_io_decision_existing_order_observed_input_rows",
                        )
                        values["work"] = {k: f[k] for k in fields if k in f}
                        for token in f.get("local_primitive_native_io_pushdown_proof_basis", "").split():
                            name, _, v = token.partition("=")
                            if name in {
                                "streaming_ordering_prefix_rows_validated",
                                "streaming_ordering_prefix_batches",
                                "streaming_ordering_prefix_peak_retained_buffer_bytes",
                            }:
                                values["work"][name] = int(v)
                    observed[role] = values
                checked.append(
                    {
                        "id": identifier,
                        "archetype": spec["archetype"],
                        "profile": spec["profile"],
                        "surface": spec["surface"],
                        "route": route,
                        "cohort": cohort,
                        "investigation": frozen.get("selection_reasons", ["challenge"])[0].split(":")[0],
                        **observed,
                    }
                )

    result = summarize_replay(index, bound, proofs, counts, checked, clock_table, declarations, require)
    if index["owner_census_attached"]:
        owner_plan, owner_result = proofs["owner-protocol.json"], proofs["owner-results.json"]
        manifest, receipt = proofs["owner-runtime.json"], proofs["owner-build.json"]
        require(
            owner_plan["source"] == manifest["source_revision"] == receipt["head"]
            and owner_plan["parent"] == index["parent"]
            and receipt["status"] == "passed"
            and receipt["command"] == cmd
            and receipt["working_tree"] == ""
            and manifest["build_receipt_sha256"] == index["members"]["owner-build.json"]["sha256"]
            and owner_plan["source_patch_sha256"] == index["members"]["owner-trace.patch"]["sha256"]
            and owner_result["protocol"]["sha256"] == index["members"]["owner-protocol.json"]["sha256"]
            and owner_plan["scored"] is False,
            "diagnostic runtime proof differs",
        )
        case = owner_plan["case"]
        require(case == "v2_multiple_key_reducer_f30d4f98739cb9c9", "unexpected owner control")
        roles = {Path(v["path"]).name: v for v in owner_result["runtime"]}
        binaries = {"binary": roles["shardloom"], "library": roles["_native.abi3.so"]}
        require(
            all(roles[name]["sha256"] == digest for name, digest in manifest["products"].items()),
            "diagnostic product differs",
        )
        _, spec = cases[case]
        expected = envelopes[case]["expected"]["value"]
        owner_summary = {}
        for route in ("native", "alternate"):
            filename = "owner-" + route + ".packet.z.xz"
            blob = contents(archive, filename, index["files"][filename])
            require(
                sha(blob)
                == proofs["owner-complete.json"][route]["sha256"]
                == owner_result["capture"][route]["sha256"],
                "diagnostic packet differs",
            )
            packet, _ = bounded_unpack(blob, case)
            verify_transport(packet, spec, route)
            stats = replay(
                (0, case, packet["classified_status"], blob),
                spec,
                route,
                expected,
                build(spec, definitions),
                dict(inputs, runtime=binaries),
                prepared,
                design["execution"],
            )
            require(stats["status"] == "passed" and stats["actual_rows"] == 31, "diagnostic result differs")
            raw = raw_files(packet)
            fields = strict_json(raw["one.stdout.json"])["observation"]["fields"]
            peak = int(fields[owner_plan["required_peak_field"]])
            census = owner_result["routes"][route]["peak_census"]
            require(
                census["peak_bytes"] == peak
                and peak <= plan["policy"]["native_memory_bytes"]
                and 0 < stats["peak_process_rss_bytes"] <= plan["policy"]["process_group_rss_ceiling_bytes"],
                "diagnostic peak differs",
            )
            captured = {}
            selected = False
            for line in raw["one.stderr.txt"].decode().splitlines():
                columns = line.split("\t")
                if len(columns) < 2 or columns[1] != str(census["pool"]):
                    continue
                if columns[0] == "SL_CREDIT_PEAK":
                    selected = int(columns[2]) == peak
                elif selected and columns[0] == "SL_CREDIT_OWNER":
                    key = (columns[2], columns[3])
                    require(key not in captured, "duplicate owner at peak")
                    captured[key] = int(columns[4])
            require(
                len(captured) == census["owner_count"]
                and sum(captured.values()) == peak
                and captured == {(o["stage"], o["site"]): o["bytes"] for o in census["owners"]},
                "simultaneous reservation attribution differs",
            )
            original = envelopes[case][route]
            source_json(original, "packet")
            historical_files = raw_files(original["packet"])
            require(
                original["blob_sha256"] == owner_plan["originals"][route]["packet_sha256"]
                and sha(historical_files["one.job.json"]) == owner_plan["originals"][route]["job_sha256"],
                "diagnostic original route differs",
            )
            historical_fields = strict_json(historical_files["one.stdout.json"])["observation"]["fields"]
            observed = owner_result["routes"][route]
            require(
                observed["end_reserved_bytes"]
                == int(fields["execution_resource_observed_native_reserved_bytes"])
                and observed["process_peak_bytes"] == stats["peak_process_rss_bytes"]
                and observed["rows"] == stats["actual_rows"],
                "diagnostic result fields differ",
            )
            owner_summary[route] = {
                **observed,
                "historical_peak_reserved_bytes": int(historical_fields[owner_plan["required_peak_field"]]),
            }
        result["diagnostic_reservation_origins"] = owner_summary
    verify_published_result(archive, index, result, bounded, require, sha, strict_json)
    if args.output:
        with args.output.open("x", encoding="utf-8") as stream:
            json.dump(result, stream, indent=2, allow_nan=False)
            stream.write("\n")
    print_verification_summary(result, checked)


if __name__ == "__main__":
    main()
