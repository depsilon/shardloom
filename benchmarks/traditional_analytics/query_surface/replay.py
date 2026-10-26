# SPDX-License-Identifier: Apache-2.0
"""Offline verification of a broad-query evidence packet; never executes queries.

Only content-addressed regular archive objects are read. No packet code is
imported or extracted. Complete values are compared with the retained independent
expectations using the frozen per-case contract and the repository comparator.
"""
from __future__ import annotations

import argparse
from collections import Counter
import hashlib
import io
import json
import lzma
import math
from pathlib import Path, PurePosixPath
import re
import sys
import tarfile

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / "scripts"), str(ROOT / "python/src"), str(Path(__file__).resolve().parents[1])]

from native_workflow_protocol import strict_json
from query_surface.evidence import no_fallback, worker_outcome
from query_surface.polars_adapter import compare_result as compare_polars_result
from query_surface.report import STATUSES
from query_surface.semantics import FloatPolicy, ResultContract, compare, result_from_record, schema_from_record

MAX_FILE = 16 << 20
MAX_EXPANDED = 256 << 20
MAX_ARCHIVE = 320 << 20
MAX_FILES = 32768


def need(value, reason):
    if not value:
        raise ValueError(reason)


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def bounded_bytes(path, maximum):
    need(not path.is_symlink() and path.is_file(), "packet input must be a regular file")
    with path.open("rb") as stream:
        value = stream.read(maximum + 1)
    need(len(value) <= maximum, "packet input exceeds bound")
    return value


def object_files(manifest, archive):
    need(manifest.get("schema_version") == "shardloom.broad_query.packet.v1", "packet schema differs")
    need(manifest.get("archive") == {"path": "packet.tar.xz", "bytes": len(archive), "sha256": sha(archive)},
         "archive identity differs")
    need(len(archive) <= MAX_ARCHIVE, "compressed packet exceeds bound")
    decoder = lzma.LZMADecompressor(memlimit=128 << 20)
    raw = decoder.decompress(archive, max_length=MAX_ARCHIVE + 1)
    need(len(raw) <= MAX_ARCHIVE and decoder.eof and not decoder.unused_data, "invalid or oversized archive")
    objects = {}
    with tarfile.open(fileobj=io.BytesIO(raw), mode="r:") as stream:
        for member in stream:
            need(len(objects) < MAX_FILES and member.isfile() and not member.islnk() and not member.issym(),
                 "invalid archive member")
            need(re.fullmatch(r"objects/sha256/[0-9a-f]{64}", member.name) is not None, "unsafe archive member")
            need(member.name not in objects and 0 <= member.size <= MAX_FILE, "duplicate or oversized member")
            content = stream.extractfile(member)
            need(content is not None, "unreadable archive member")
            value = content.read(MAX_FILE + 1)
            need(len(value) == member.size and sha(value) == member.name.rsplit("/", 1)[-1], "object digest differs")
            objects[member.name] = value
    files, used, size = {}, set(), 0
    descriptor = manifest.get("file_inventory")
    need(isinstance(descriptor, dict) and set(descriptor) == {"member", "bytes", "sha256"},
         "missing content-addressed file inventory")
    inventory_member = "objects/sha256/" + descriptor["sha256"]
    need(descriptor["member"] == inventory_member and inventory_member in objects, "missing inventory object")
    inventory_raw = objects[inventory_member]
    need(type(descriptor["bytes"]) is int and len(inventory_raw) == descriptor["bytes"], "inventory size differs")
    inventory = strict_json(inventory_raw)
    used.add(inventory_member)
    need(isinstance(inventory, list) and 0 < len(inventory) <= MAX_FILES, "invalid file inventory")
    for item in inventory:
        name = item["path"]
        need(isinstance(name, str) and name not in files and not name.startswith("/") and
             str(PurePosixPath(name)) == name and not {"..", "."}.intersection(PurePosixPath(name).parts),
             "duplicate or unsafe file path")
        member = "objects/sha256/" + item["sha256"]
        need(item["member"] == member and member in objects, "missing file object")
        value = objects[member]
        need(type(item["bytes"]) is int and len(value) == item["bytes"], "file size differs")
        files[name], size = value, size + len(value)
        used.add(member)
    need(used == set(objects), "unreferenced archive objects")
    need(manifest.get("file_count") == len(files), "file count differs")
    need(size <= MAX_EXPANDED and manifest.get("expanded_file_bytes") == size, "expanded size differs")
    need(manifest.get("unique_objects") == len(objects), "object count differs")
    return files


def contract(record):
    return ResultContract(schema_from_record(record["schema"]), ordered=record["ordered"],
        floats={name: FloatPolicy(**policy) for name, policy in record["floats"].items()},
        max_matching_edges=record["max_matching_edges"])


def clock(value):
    need(type(value) in (float, int) and math.isfinite(value) and value >= 0, "missing or invalid clock")
    return value


def summary_state(summary):
    """Recognize the complete and aborted formats emitted by both runners."""
    need(isinstance(summary, dict), "invalid run summary")
    native = "completed_selected_cohort" in summary
    need(native != ("completed" in summary), "missing or ambiguous summary completion field")
    complete = summary["completed_selected_cohort" if native else "completed"]
    need(type(complete) is bool, "summary completion must be a boolean")
    posthash = summary.get("source_runtime_posthash_verified" if native else "posthash_verified")
    need(posthash is complete, "summary completion and final identity checks differ")
    if not complete:
        need(isinstance(summary.get("failure"), str), "incomplete summary lacks failure details")
        if "exception" in summary:
            need(isinstance(summary["exception"], str), "invalid failure exception")
    return native, complete


def verify_summary_outcomes(summary, native, complete, status, observations):
    if summary is None:
        return
    if native:
        recorded_status = Counter()
        for group in summary["families"].values():
            recorded_status.update({status: count for status, count in group["case_surface_outcomes"].items()
                                    if status not in {"proposed", "unexecuted"}})
        need(recorded_status == status, "summary erased or changed an outcome")
    else:
        # Aborts omit these derived fields. If supplied, they still have to
        # match the verified observations; complete summaries require both.
        if complete or "all_passed" in summary:
            need(summary.get("all_passed") is (status["passed"] == len(observations)),
                 "baseline all-passed claim differs")
        if complete or "comparisons" in summary:
            need(summary.get("comparisons") == [row["comparison"] for row in observations],
                 "baseline summary clocks differ")


def verify_runs(files, manifest):
    prefix = manifest["original_evidence_root"].rstrip("/") + "/"
    need(prefix.startswith("/"), "missing original evidence root")

    def key(path):
        need(isinstance(path, str) and path.startswith(prefix), "evidence reference escapes the packet")
        return "evidence/" + path[len(prefix):]

    def raw(path):
        name = key(path)
        need(name in files, "missing evidence: " + name)
        return files[name]

    def read(path):
        return strict_json(raw(path))

    def bounded(path, digest):
        value = raw(path)
        need(sha(value) == digest, "referenced evidence digest differs: " + path)
        return strict_json(value)

    outcomes, results, warnings = {}, {}, []
    retained_digests = {sha(value) for value in files.values()}
    external = {(item["path"], item["sha256"]) for item in manifest["external_artifacts"]}
    retained_provenance = external_provenance = 0
    checked_results = checked_receipts = 0
    run_names = manifest["runs"]
    need(len(set(run_names)) == len(run_names), "duplicate run identity")
    for name in run_names:
        base = prefix + name + "/"
        need("/" not in name and name not in {"", ".", ".."}, "invalid run name")
        summary_present = "evidence/" + name + "/summary.json" in files
        if not summary_present:
            need(name in manifest["incomplete_before_summary"], "missing run summary")
            if "evidence/" + name + "/observations.jsonl" not in files:
                outcomes[name] = {"completed": False, "reason": "failed_before_summary", "observations": 0}
                continue
        summary = read(base + "summary.json") if summary_present else None
        summary_native, complete = summary_state(summary) if summary is not None else (None, False)
        observations = ([strict_json(line) for line in raw(base + "observations.jsonl").splitlines() if line.strip()]
                        if "evidence/" + name + "/observations.jsonl" in files else [])
        provenance_present = "evidence/" + name + "/provenance.json" in files
        if summary is not None and not complete and not observations and not provenance_present:
            # Preparation can fail before provenance or the first observation
            # is written. Preserve the failure with no execution credit.
            native, identities = summary_native, []
            warnings.append(name + ": aborted before provenance; runtime/input identities unavailable")
        else:
            provenance = read(base + "provenance.json")
            native = provenance.get("comparison_engine_invoked") is False
            need(native or provenance.get("comparison_engine_invoked") == "polars_only", "unknown engine cohort")
            need(summary is None or summary_native is native, "summary and provenance engines differ")
            identities = (provenance["runtime_and_inputs"] + provenance["harness_sources"] + provenance.get("python_sources", [])
                          if native else provenance["frozen_files"])
            if native and "python_sources" not in provenance:
                warnings.append(name + ": legacy provenance lacks a Python source snapshot inventory")
        for item in identities:
            if item["sha256"] in retained_digests:
                retained_provenance += 1
            else:
                need((item["path"], item["sha256"]) in external, "missing provenance content or explicit external identity")
                external_provenance += 1
        failure = ({field: summary[field] for field in ("failure", "exception") if field in summary}
                   if summary is not None and not complete else {})
        if not observations and not complete:
            verify_summary_outcomes(summary, native, complete, Counter(), observations)
            outcomes[name] = {"completed": False, "observations": 0, "statuses": {}, **failure,
                              "reason": "failed_before_observation" if summary is not None else "failed_before_summary"}
            continue
        corpus_root = base if native else provenance["native_run"].rstrip("/") + "/"
        catalog_rows = read(corpus_root + "corpus.json")
        catalog = {case["id"]: case for case in catalog_rows}
        need(len(catalog) == len(catalog_rows), "duplicate corpus case")
        selected = provenance["selected_cases"]
        need(len(set(selected)) == len(selected), "duplicate selected case")
        surfaces = provenance["surfaces"] if native else ["polars"]
        expected_pairs = {(case, surface) for case in selected for surface in surfaces}
        seen, status = set(), Counter()
        for row in observations:
            case_id, surface = row["case_id"], row["surface"] if native else "polars"
            pair = (case_id, surface)
            need(pair in expected_pairs and pair not in seen and row["status"] in STATUSES, "observation identity/status differs")
            seen.add(pair)
            status[row["status"]] += 1
            case = catalog[case_id]
            need(case["relation"] is not None, "pending case credited as executed")
            path = row["evidence"]
            need(path.endswith(".stdout.json"), "invalid observation suffix")
            stem = path.removesuffix(".stdout.json")
            receipt, job = read(stem + ".receipt.json"), read(stem + ".job.json")
            need(receipt["stdout"] == path and receipt["stderr"] == stem + ".stderr.txt",
                 "receipt output paths differ from the observation")
            for field in ("stdout", "stderr"):
                need(sha(raw(receipt[field])) == receipt[field + "_sha256"], "receipt output changed")
            reference_failure = native and row.get("stage") == "independent_expectation"
            need(row.get("stage") is None or reference_failure, "unknown observation stage")
            if "evidence_sha256" in row:
                need(row["evidence_sha256"] == receipt["stdout_sha256"], "observation output digest differs")
            else:
                need(reference_failure, "missing observation output digest")
                warnings.append(name + ": legacy reference-failure observation lacks its own stdout hash")
            if "job_sha256" in receipt:
                need(sha(raw(stem + ".job.json")) == receipt["job_sha256"], "receipt job changed")
            else:
                warnings.append(name + ": legacy receipt lacks original job hash")
            checked_receipts += 1
            need(job["case_id"] == case_id and job["case_identity"] == sha(json.dumps(case, sort_keys=True, allow_nan=False).encode()),
                 "frozen case request differs")
            if reference_failure:
                need(job.get("kind") == "reference" and row["status"] != "passed",
                     "reference failure is not a failed expectation request")
            else:
                need(job.get("surface") == surface if native else job.get("kind") == "polars", "request surface differs")
            outcome = worker_outcome(receipt, raw(path))
            need(outcome["status"] == row["status"], "observation outcome erased or changed")
            if row["status"] != "passed":
                continue
            need(receipt["returncode"] == 0 and not receipt["guard_failures"], "passing result has failed supervision")
            need(outcome["case_id"] == case_id and outcome["case_identity"] == job["case_identity"], "result case identity differs")
            expectation = bounded(job["reference_path"], job["reference_sha256"])
            need(expectation["status"] == "passed" and expectation["case_identity"] == job["case_identity"], "reference identity differs")
            observation = outcome["observation"]
            need(observation["complete_consumption"] is True and observation["public_declaration"] == case["relation"],
                 "execution did not consume the complete declared request")
            wanted, actual = result_from_record(expectation["reference"]), result_from_record(outcome["actual"])
            frozen_contract = contract(case["contract"])
            if native:
                need(observation["surface"] == surface, "result surface differs")
                need(outcome["comparison_engine_invoked"] is False, "native result invokes an external engine")
                no_fallback(observation["fields"])
                compare(wanted, actual, frozen_contract)
                need(row["engine_execution_seconds"] == observation["clocks"]["engine_execution_seconds"], "engine clock changed")
                clock(row["engine_execution_seconds"])
            else:
                need(outcome["comparison_engine_invoked"] is True and observation["execution_engine"] == "polars", "wrong baseline engine")
                need(observation["clocks"]["engine_execution_seconds"] is None, "Polars API clock relabelled as engine time")
                compare_polars_result(wanted, actual, frozen_contract)
                pair_record = row["comparison"]
                native_row = results[(provenance["native_run"].rsplit("/", 1)[-1], case_id, provenance["native_surface"])]
                a, b = (clock(item["observation"]["clocks"]["complete_workflow_seconds"]) for item in (native_row, outcome))
                need(a > 0 and b > 0 and pair_record["comparable"] is True and pair_record["clock"] == "complete_workflow_seconds", "incompatible comparison clock")
                need(pair_record["shardloom_seconds"] == a and pair_record["polars_seconds"] == b and
                     pair_record["native_over_polars"] == a / b and pair_record["native_minus_polars_seconds"] == a - b,
                     "comparison is not derived from matching raw clocks")
                need(native_row["case_identity"] == outcome["case_identity"], "paired case identity differs")
            checked_results += 1
            results[(name, case_id, surface)] = outcome
        if complete:
            need(seen == expected_pairs and summary["selected_cases"] == len(selected), "completed cohort is missing cases")
        verify_summary_outcomes(summary, native, complete, status, observations)
        outcomes[name] = {"completed": complete, "observations": len(observations), "statuses": dict(status), **failure}
        if summary is None:
            outcomes[name]["reason"] = "failed_before_summary"
    baseline = manifest["baseline_native_runs"]
    pairs = manifest["baseline_pairs"]
    need(len(baseline) == len(set(baseline)) and set(baseline).issubset(run_names), "invalid native baseline runs")
    need(all(outcomes[run]["completed"] is True and set(outcomes[run]["statuses"]) == {"passed"}
             for run in baseline), "baseline contains incomplete or failed execution")
    need(len(pairs) == len(baseline) and {pair[0] for pair in pairs} == set(baseline)
         and len({pair[1] for pair in pairs}) == len(pairs)
         and all(pair[1] in run_names for pair in pairs), "incomplete baseline pair selection")
    native_cases = [case for run, case, surface in results if run in baseline and surface == "python_sql"]
    need(len(native_cases) == len(set(native_cases)) == manifest["baseline_case_count"], "baseline missing or repeats a case")
    need(sorted(native_cases) == manifest["baseline_case_ids"], "baseline case selection differs")
    for native_run, polars_run in pairs:
        need(outcomes[polars_run]["completed"] is True and set(outcomes[polars_run]["statuses"]) == {"passed"},
             "baseline contains incomplete or failed execution")
        native_ids = {case for run, case, surface in results if run == native_run and surface == "python_sql"}
        polars_ids = {case for run, case, surface in results if run == polars_run and surface == "polars"}
        need(native_ids and native_ids == polars_ids, "native and Polars baseline coverage differ")
    engine = sum(result["observation"]["clocks"]["engine_execution_seconds"]
                 for (run, _, surface), result in results.items() if run in baseline and surface == "python_sql")
    need(math.isclose(engine, manifest["baseline_engine_seconds"], rel_tol=0, abs_tol=1e-10), "baseline engine total differs")
    return {"status": "passed", "checked_complete_results": checked_results, "checked_receipts": checked_receipts,
            "retained_provenance_identities": retained_provenance, "external_payload_identities": external_provenance,
            "runs": outcomes, "baseline_cases": len(native_cases), "baseline_engine_seconds": engine,
            "proof_gaps": sorted(set(warnings)), "executed_queries": 0}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("packet", type=Path)
    args = parser.parse_args()
    manifest = strict_json(bounded_bytes(args.packet / "manifest.json", MAX_FILE))
    files = object_files(manifest, bounded_bytes(args.packet / "packet.tar.xz", MAX_ARCHIVE))
    print(json.dumps(verify_runs(files, manifest), indent=2))


if __name__ == "__main__":
    main()
