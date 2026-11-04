# SPDX-License-Identifier: Apache-2.0
"""Replay bounded read ownership and the rejected public-model compatibility trial."""

import base64
import json

from query_surface.expanded_catalog import build, spaces
from query_surface.expanded_packet import CASES_PER_PART, bounded_unpack
from query_surface.expanded_replay import lines, require, sha, source_json
from query_surface.expanded_report import raw_files, replay
from query_surface.expanded_shared_trace import parse


def validate_public_models(proofs):
    report = proofs["public-model-compatibility-r1/report.json"]
    values = {}
    for role in ("baseline", "candidate"):
        observed = report["observations"][role]
        values[role] = observed["checks"]
        saved = proofs[f"public-model-compatibility-r1/{role}.json"]
        require(observed["returncode"] == saved["returncode"] == 0
                and observed["stderr"] == saved["stderr"] == ""
                and observed["stdout"] == saved["stdout"]
                and json.loads(observed["stdout"]) == observed["checks"],
                "public model observation differs")
    before, after = values["baseline"], values["candidate"]
    require(before["get_type_hints_before_lookup"]["ok"] is True and before["source_available"] is True
            and after["get_type_hints_before_lookup"]["ok"] is False
            and after["get_type_hints_before_lookup"]["error"] == "NameError"
            and after["source_available"]["ok"] is False and after["source_available"]["error"] == "OSError"
            and all(before[k] == after[k] for k in before if k not in ("get_type_hints_before_lookup", "source_available")),
            "public model compatibility disposition differs")


def validate_reads(base, original, docs, proofs, members, text):
    case_id = "v2_multiple_key_reducer_f30d4f98739cb9c9"
    design, inputs, prepared = (docs[k] for k in ("design.json", "inputs.json", "native-preparation/complete.json"))
    seq, spec = next((i, row) for i, row in enumerate(design["cases"]) if row["id"] == case_id)
    part = seq // CASES_PER_PART
    references = lines(base, f"cases-{part:04d}.jsonl.xz", original["parts"][part], CASES_PER_PART)
    record = next(row for row in references if row["seq"] == seq)
    source_json(record["expected"], "value")
    item = build(spec, {s.name: s for s in spaces()})
    require(item.identity == spec["case_identity"] == record["expected"]["identity"], "read control identity differs")
    comparison = proofs["mechanism-read-policy-comparison-r1.json"]
    require(comparison["status"] == "verified" and len(comparison["observations"]) == 3,
            "read comparison inventory differs")
    configurations = (
        ("mechanism-io-owner-capture-r2", "mechanism-io-owner-protocol-r2", "mechanism-io-trace-runtime-r2", "mechanism-io-trace-capture-r2", "mechanism-io-owner-diagnostic-r2.patch"),
        ("mechanism-io-policy-capture-r1", "mechanism-io-policy-protocol-r1", "mechanism-io-policy-runtime-r1", "mechanism-io-policy-capture-r1", "mechanism-io-policy-diagnostic-r1.patch"),
        ("mechanism-io-policy-capture-r2", "mechanism-io-policy-protocol-r2", "mechanism-io-policy-runtime-r2", "mechanism-io-policy-capture-r2", "mechanism-io-policy-diagnostic-r2.patch"),
    )
    observations = []
    for position, (capture, protocol_name, runtime_name, guard_name, patch_name) in enumerate(configurations):
        saved = comparison["observations"][position]
        result, protocol, runtime, guard = (proofs[name] for name in (
            capture + "/results.json", protocol_name + ".json", runtime_name + "/manifest.json", guard_name + ".json"))
        build_receipt = proofs[runtime["build_receipt"]]
        require(saved["capture"] == capture and result["status"] == "verified"
                and protocol["status"] == "frozen_before_capture" and protocol["case"] == case_id
                and protocol["independent_identity"] == item.identity
                and protocol["routes_in_order"] == ["native", "alternate"] and protocol["attempts_per_route"] == 1
                and protocol["scored"] is False and protocol["policy"] == design["execution"]
                and result["protocol"]["sha256"] == members[protocol_name + ".json"]["sha256"]
                and protocol["source"] == runtime["source_revision"] == build_receipt["head"] == guard["head"]
                and protocol["source_patch_sha256"] == members[patch_name]["sha256"]
                and protocol["driver"]["sha256"] == members["tools/" + protocol["driver"]["path"].rsplit("/", 1)[-1]]["sha256"]
                and runtime["features"] == ["release-user-surfaces"] and runtime["profile"] == "release"
                and build_receipt["status"] == guard["status"] == "passed"
                and build_receipt["returncode"] == guard["returncode"] == 0
                and build_receipt["process_group_drained"] and guard["process_group_drained"]
                and build_receipt["direct_child_reaped"] and guard["direct_child_reaped"]
                and build_receipt["working_tree"] == guard["working_tree"] == ""
                and guard["query_workload"] is True
                and guard["driver_sha256"] == members["tools/guard_owner_diagnostic.py"]["sha256"]
                and guard["log_sha256"] == members[guard_name + ".log"]["sha256"]
                and build_receipt["log_sha256"] == members[runtime["build_receipt"].replace(".json", ".log")]["sha256"]
                and runtime["build_receipt_sha256"] == members[runtime["build_receipt"]]["sha256"]
                and protocol["trace_parser"]["sha256"] == members["tools/parse_io_owner_trace_v2.py"]["sha256"],
                "read diagnostic provenance differs")
        bound = {row["path"].rsplit("/", 1)[-1]: row for row in result["runtime"]}
        require(bound["manifest.json"]["sha256"] == members[runtime_name + "/manifest.json"]["sha256"],
                "read diagnostic manifest differs")
        binaries = dict(binary=bound["shardloom"], library=bound["_native.abi3.so"])
        for product in ("shardloom", "_native.abi3.so"):
            require(bound[product]["sha256"] == runtime["products"][product], "read diagnostic runtime differs")
        peaks = {}
        for route in protocol["routes_in_order"]:
            blob = base64.b64decode(text[capture + "/" + route + ".packet.z.b64"], validate=True)
            require(sha(blob) == result["capture"][route]["sha256"]
                    and result["capture"][route] == proofs[capture + "/complete.json"][route],
                    "read diagnostic capture differs")
            packet, _ = bounded_unpack(blob, capture + "/" + route)
            observed = replay((seq, case_id, packet["classified_status"], blob), spec, route,
                              record["expected"]["value"], item, dict(inputs, runtime=binaries), prepared, design["execution"])
            require(observed["status"] == "passed" and observed["actual_rows"] == result["routes"][route]["rows"] == 31,
                    "read diagnostic complete result differs")
            raw = raw_files(packet)
            for filename, value in raw.items():
                name = capture + "/" + route + "/" + filename
                if name in text:
                    require(value == text[name].encode(), "diagnostic raw packet member differs")
            fields = json.loads(raw["one.stdout.json"])["observation"]["fields"]
            peaks[route] = int(fields[protocol["required_peak_field"]])
            census = result["routes"][route]["peak_census"]
            require(peaks[route] == census["peak_bytes"] == sum(row["bytes"] for row in census["owners"])
                    and 0 < peaks[route] <= design["execution"]["native_memory_bytes"], "read reservation peak differs")
            if route == "alternate":
                trace = json.loads(json.dumps(parse(raw["one.stderr.txt"].decode())))
                require(trace == proofs[capture + "/alternate-trace.json"] and trace["complete_chain"] is True,
                        "read ownership chain differs")
        require(saved["trace"]["sha256"] == members[capture + "/alternate-trace.json"]["sha256"]
                and saved["results"]["sha256"] == members[capture + "/results.json"]["sha256"]
                and saved["totals"] == trace["totals"] and saved["peak_native_reserved_bytes"] == peaks["alternate"],
                "read comparison summary differs")
        observations.append(dict(capture=capture, totals=trace["totals"], peak_native_reserved_bytes=peaks["alternate"],
                                 direct_control_peak_bytes=peaks["native"]))
    before, no_effect, after = observations
    require(before["totals"] == no_effect["totals"]
            and before["peak_native_reserved_bytes"] == no_effect["peak_native_reserved_bytes"]
            and before["totals"]["consumed_member_union_bytes"] == after["totals"]["consumed_member_union_bytes"]
            and before["direct_control_peak_bytes"] == after["direct_control_peak_bytes"]
            and comparison["returned_data_bytes_reduced"]
            == before["totals"]["returned_data_extent_bytes"] - after["totals"]["returned_data_extent_bytes"]
            and comparison["peak_native_reserved_bytes_reduced"]
            == before["peak_native_reserved_bytes"] - after["peak_native_reserved_bytes"]
            and comparison["completed_read_requests_increase"]
            == after["totals"]["completed_filesystem_read_requests"] - before["totals"]["completed_filesystem_read_requests"],
            "read policy effect or original no-effect observation differs")
    return observations
