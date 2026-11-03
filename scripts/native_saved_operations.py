# SPDX-License-Identifier: Apache-2.0
"""Read original native operation records through a caller-supplied byte store."""
from decimal import Decimal
import gzip
from pathlib import PurePosixPath

from clickbench_reference_packet import canonical_values_sha256
from native_workflow_protocol import extract_result, report_fields, strict_json
from parquet_demand_completion_replay import replay as replay_broad
from query_surface.replay import need, sha
from query_timing_report import query_phase_timings
from run_clickbench_query_uat import equivalent


# Historical path labels in receipts. Replay resolves them in archive storage.
ORIGINAL = "/Users/dylan/LocalData/shardloom/direct-source-count-2026-10-31"
BROAD = ORIGINAL + "/broad-preparation-fresh347-r1"
PROBE_IDS = [1, 3, 13, 19, 20, 24, 34]


def linked(raw, ref):
    data = raw(ref["path"])
    need(sha(data) == ref["sha256"] and len(data) == ref.get("bytes", len(data)),
         "linked record changed: " + ref["path"])
    return strict_json(data)


def cli_fields(envelope):
    fields = report_fields(envelope)
    result_keys = set()
    for item in envelope["result"]["fields"]:
        key = item["key"]
        need(key not in result_keys and key in fields and fields[key] == item["value"],
             "ambiguous CLI result field: " + key)
        result_keys.add(key)
    return fields


def grant(fields):
    return {"memory_bytes": int(fields["execution_resource_declared_memory_bytes"]),
            "parallelism": int(fields["execution_resource_declared_max_parallelism"])}


def process_observation(raw, timing, pid):
    process = strict_json(raw(timing))
    recorded = raw(pid).strip()
    need(recorded.isdigit() and int(recorded) > 0 and process["returncode"] == 0, "invalid completed process receipt")
    return dict(process, pid=int(recorded))


def clickbench(raw, probe, workflow):
    routes = [
        ("probe-" + fmt, probe, probe["routes"][fmt]["queries"], PROBE_IDS,
         probe["inputs"][fmt], probe["runtime_build"]["files"]["shardloom"]["sha256"],
         probe["runtime_build"]["source_revision"], probe["runtime_build"]["profile"])
        for fmt in ("parquet", "vortex")
    ]
    routes.append(("fresh43", workflow, workflow["full43_acceptance"]["per_query"],
                   list(range(1, 44)), workflow["ingest_acceptance"]["output_identity"],
                   workflow["runtime_files"]["shardloom"]["sha256"], workflow["source_revision"],
                   workflow["runtime_profile"]))
    records = []
    for cohort, anchor, accepted, ids, source, runtime, revision, profile in routes:
        paths = anchor["archive"]["source_map"]
        prefix = ("full43-preparation-contracts-r1/" if cohort == "fresh43"
                  else "source-strategy-probe-r1/" + cohort.removeprefix("probe-") + "/")
        [summary_path] = [v["source_path"] for k, v in paths.items()
                          if k.startswith(prefix) and k.endswith("/summary.json")]
        summary = strict_json(raw(summary_path))
        need(summary["complete"] and summary["repeats"] == 1 and summary["selected_query_ids"] == ids
             and [r["query"] for r in summary["records"]] == ids
             and [r.get("query", r.get("query_id")) for r in accepted] == ids, "query coverage changed")
        gen = summary["source_generation"]
        need(source["generation"] == dict(st_dev=gen["device"], st_ino=gen["inode"],
             st_size=gen["size_bytes"], st_mtime_ns=gen["mtime_ns"], st_ctime_ns=gen["ctime_ns"])
             and source["path"] == summary["source"] and runtime == summary["binary_sha256"]
             and revision == summary["build_commit"], "source or binary identity changed")
        reference = linked(raw, summary["reference_packet"])
        need(reference["independent_correctness_oracle"] is True and
             reference["queries_sha256"] == summary["queries_sha256"], "independent reference absent")
        expected = {r["query_id"]: r for r in reference["records"]}
        for ordinal, (item, receipt) in enumerate(zip(summary["records"], accepted), 1):
            query = item["query"]
            stem = summary_path.rsplit("/", 1)[0] + f"/q{query:02d}_run1"
            stdout, timing = stem + ".stdout.json.gz", stem + ".timing.json"
            process = process_observation(raw, timing, stem + ".pid")
            envelope = strict_json(gzip.decompress(raw(stdout)))
            fields = cli_fields(envelope)
            actual = extract_result(envelope)
            digest = canonical_values_sha256(actual)
            need(equivalent(actual, expected[query]["values"]) and len(actual) == receipt["rows"]
                 and digest == item["result_sha256"] == receipt["result_sha256"],
                 f"{cohort} Q{query} complete result changed")
            need(process["returncode"] == item["returncode"] == 0 and item["run"] == 1
                 and item["passed"] and not item["guard_failures"] and item["validation"] == "complete_values",
                 f"{cohort} Q{query} process failed")
            clocks = query_phase_timings(fields, process["seconds"], allow_missing=False)
            need(clocks["engine_execution_seconds"] == item["engine_execution_seconds"]
                 == receipt["engine_execution_seconds"], "engine clock mismatch")
            compact = item["execution_report"]
            need(all(k in fields and fields[k] == v for k, v in compact.items()), "compact/full mismatch")
            declared = grant(fields)
            need(declared == {"memory_bytes": summary["memory_gb"] << 30,
                              "parallelism": summary["max_parallelism"]}, "resource grant mismatch")
            records.append(dict(cohort=cohort, operation=f"Q{query}", ordinal=ordinal,
                surface="cli_" + item["command"][2], session_boundary=summary["cache_policy"],
                workload_sha256=summary["queries_sha256"], runtime_sha256=runtime, source_revision=revision,
                profile=profile, input_identity=source, artifact_identity=source if summary["input_format"] == "vortex" else None,
                grant=declared, engine_seconds=str(clocks["engine_execution_seconds"]), result_rows=len(actual),
                validation="complete independent values replayed", result_sha256=digest,
                full_fields=fields, compact_fields=compact, process=process,
                evidence=dict(stdout=stdout, timing=timing, pid=stem + ".pid",
                              summary=summary_path, reference=summary["reference_packet"]["path"])))
    return records


def preparations(raw, workflow):
    records = []
    for cohort, filename in (("fresh-preparation", "preparation-contracts-ingest-acceptance.json"),
                             ("prior-preparation", "ingest-dynamic-r2-acceptance.json")):
        acceptance_path = ORIGINAL + "/" + filename
        accepted = strict_json(raw(acceptance_path))
        freeze, driver, summary, envelope = [linked(raw, accepted[role])
                                             for role in ("freeze", "driver", "summary", "stdout")]
        fields = cli_fields(envelope)
        timing_path = summary["native_timing_path"]
        pid_path = str(PurePosixPath(timing_path).with_name("native.pid"))
        timing = process_observation(raw, timing_path, pid_path)
        need(accepted["status"] == driver["status"] == "passed" and accepted["no_fallback"] is True
             and summary["returncode"] == timing["returncode"] == 0 and summary["stdout_json_ok"]
             and summary["target_bytes"] == accepted["output"]["bytes"], "incomplete preparation")
        spans = accepted["engine_spans_milliseconds"]
        for key, value in spans.items():
            need(fields["public_workflow_preparation_" + key] == value, "preparation span mismatch")
        declared = grant(fields)
        need(declared == {"memory_bytes": summary["memory_gb"] << 30, "parallelism": summary["max_parallelism"]},
             "preparation grant mismatch")
        [cli] = [r for r in freeze["runtime"] if PurePosixPath(r["path"]).name == "shardloom"]
        runtime_manifest = str(PurePosixPath(cli["path"]).with_name("manifest.json"))
        runtime = strict_json(raw(runtime_manifest))
        need(runtime["products"]["shardloom"] == cli["sha256"] and runtime["source_revision"] == freeze["source_revision"],
             "preparation runtime identity changed")
        evidence = {role: accepted[role]["path"] for role in ("freeze", "driver", "summary", "stdout")}
        evidence.update(acceptance=acceptance_path, timing=timing_path, pid=pid_path, runtime=runtime_manifest)
        if cohort == "fresh-preparation":
            need(accepted["output"] == workflow["ingest_acceptance"]["output_identity"]
                 and cli["sha256"] == workflow["runtime_files"]["shardloom"]["sha256"], "workflow identity mismatch")
        else:
            evidence["ownership_observation"] = ORIGINAL + "/preparation-de-observed-evidence.json"
            attribution = strict_json(raw(evidence["ownership_observation"]))
            need(attribution["sources"]["ingest"]["sha256"] == accepted["stdout"]["sha256"],
                 "prior ownership observation refers to different bytes")
        records.append(dict(cohort=cohort, operation="prepare", ordinal=1, runtime_sha256=cli["sha256"],
            surface="cli_" + envelope["command"], session_boundary="separate preparation process",
            workload_sha256=accepted["freeze"]["sha256"], source_revision=freeze["source_revision"],
            profile=runtime["profile"], input_identity=freeze["input"], artifact_identity=accepted["output"],
            grant=declared, engine_seconds=str(Decimal(spans["prepare_once_millis"]) / 1000),
            publication_seconds=str(Decimal(spans["prepared_olap_publication_millis"]) / 1000),
            result_rows=accepted["rows"], validation=accepted["full_value_scope"],
            full_fields=fields, compact_fields=summary, process=timing, evidence=evidence))
    return records


def broad(raw, anchor):
    paths = {PurePosixPath(ref["path"]).name: ref for ref in anchor["retained_local_evidence"]["files"]}
    frozen = linked(raw, paths["inputs.json"])
    index = linked(raw, paths["native-observations.json"])
    validation = linked(raw, paths["complete-result-validation.json"])
    checked = replay_broad(raw, BROAD)
    need(checked["cases"] == validation["cases"] and checked["complete_rows"] == anchor["complete_reference_rows"]
         and checked["candidate_engine_seconds"] == anchor["engine_execution_seconds"], "347 replay changed")
    sources = {item["path"]: item for item in frozen["frozen_files"]}
    input_identity = {"logical_sha256": frozen["logical_identity"],
                      "tables": {name: [sources[p] for p in files] for name, files in frozen["sources"].items()}}
    runtime = linked(raw, frozen["runtime_manifest"])
    binary = runtime["products"]["_native.abi3.so"]
    need(runtime["source_revision"] == anchor["source_revision"] == frozen["source_revision"] and
         any(r["sha256"] == binary and PurePosixPath(r["path"]).name == "_native.abi3.so" for r in frozen["runtime"]),
         "347 native library identity mismatch")
    records = []
    for ordinal, (case, item, comparison) in enumerate(zip(frozen["cases"], index["observations"], checked["cases"]), 1):
        outcome = strict_json(raw(item["stdout"]))
        observed = outcome["observation"]
        fields, report = observed["fields"], observed["raw_report"]
        need(fields == report["fields"] and report["row_export"]["fallback_attempted"] is False,
             "347 worker report disagrees with outcome")
        receipt = strict_json(raw(item["receipt"]))
        stem = item["receipt"].removesuffix(".receipt.json")
        process_stats = process_observation(raw, stem + ".timing.json", stem + ".pid")
        need(all(process_stats[k] == receipt[k] for k in ("returncode", "seconds", "user_cpu_seconds",
             "system_cpu_seconds", "minor_page_faults", "major_page_faults", "input_block_operations", "output_block_operations"))
             and process_stats["peak_rss_bytes"] == receipt["native_peak_rss_bytes"], "347 timing receipt mismatch")
        evidence = dict(stdout=item["stdout"], timing=stem + ".timing.json", pid=stem + ".pid",
                        receipt=item["receipt"], declaration=receipt["command"][-1],
                        reference=case["reference_path"], summary=paths["native-observations.json"]["path"],
                        inputs=paths["inputs.json"]["path"])
        records.append(dict(cohort="fresh347", operation=item["case_id"], ordinal=ordinal, family=item["family"],
            workload_sha256=case["case_identity"], runtime_sha256=binary, source_revision=frozen["source_revision"],
            profile=runtime["profile"], surface=observed["surface"], session_boundary=observed["execution_mode"],
            input_identity=input_identity, artifact_identity=None,
            grant={"memory_bytes": frozen["memory_bytes"], "parallelism": frozen["parallelism"]},
            engine_seconds=str(comparison["candidate_engine_seconds"]), result_rows=comparison["complete_rows"],
            validation="complete independent values, schema and ordering replayed",
            full_fields=fields, compact_fields=item["physical"], row_export=report["row_export"], process=process_stats,
            evidence=evidence))
    return records
