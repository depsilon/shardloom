# SPDX-License-Identifier: Apache-2.0
"""Freeze independently reproduced inputs and controls before transport work.

This is experiment preparation, not a benchmark or native execution provider.
It uses only the Python standard library and refuses an existing output.
"""

import argparse
import gzip
import hashlib
import json
from pathlib import Path
import struct


CASES = [
    "empty", "tiny", "nullable_plain", "all_null", "constant",
    "dictionary_small", "dictionary_large", "dictionary_other", "run_end",
    "bitpacked", "wide_text", "nested", "sliced_shared",
]
REGISTRY = [
    "vortex.null", "vortex.bool", "vortex.primitive", "vortex.decimal",
    "vortex.varbin", "vortex.varbinview", "vortex.list", "vortex.listview",
    "vortex.fixed_size_list", "vortex.struct", "vortex.ext", "vortex.constant",
    "vortex.dict", "vortex.chunked", "vortex.runend", "fastlanes.bitpacked",
]
MODES = ["native_fragments", "native_assembled", "arrow_none", "arrow_lz4", "arrow_zstd"]
LIMITS = {
    "native_memory_bytes": 256 << 20, "queued_bytes": 4 << 20,
    "queued_frames": 4, "fragment_payload_bytes": 1 << 20,
    "encoded_rpc_bytes": 2 << 20, "control_bytes": 16 << 10,
    "complete_payload_bytes": 64 << 20, "frames": 256,
    "fragment_segments": 128,
}
SQL = "SELECT category AS last_value, event_key AS first_value FROM exchange_fixture"


def require(condition, message):
    if not condition:
        raise ValueError(message)


def digest(data):
    return hashlib.sha256(data).hexdigest()


def identity(path, name=None):
    data = path.read_bytes()
    return {"file": name or path.name, "bytes": len(data), "sha256": digest(data)}


def verified_file(root, record):
    name = record["file"]
    require(Path(name).name == name, "input record must be a local basename")
    path = root / name
    require(not path.is_symlink() and path.is_file(), f"invalid input {name}")
    require(identity(path) == record, f"changed input {name}")
    return path.read_bytes()


def varint(value):
    require(0 <= value < 1 << 64, "out-of-range Protobuf unsigned value")
    result = bytearray()
    while value >= 128:
        result.append((value & 127) | 128)
        value >>= 7
    result.append(value)
    return bytes(result)


def integer(tag, value):
    return varint(tag << 3) + varint(value) if value else b""


def blob(tag, value):
    return varint((tag << 3) | 2) + varint(len(value)) + value


def string(tag, value):
    return blob(tag, value.encode("utf-8")) if value else b""


def operations(case):
    messages = []
    for query in case["queries"]:
        projection = string(1, query["id"])
        for field in query["projection"]:
            projection += blob(2, string(1, field["source"]) + string(2, field["alias"]))
        messages.append(blob(1, projection))
    return b"".join(messages) + blob(2, bytes([0, 1, 2, 1]))


def profile():
    limits = b"".join(integer(tag, value) for tag, value in enumerate(LIMITS.values(), 1))
    # V0 is a default proto3 scalar and is therefore absent from canonical bytes.
    return (integer(1, 1) + string(2, "0.85.0")
            + b"".join(string(4, item) for item in REGISTRY) + blob(5, limits))


def query_checks(records, case):
    require(len(records) == 4, f"missing query execution: {case['id']}")
    for index, actual in zip([0, 1, 2, 1], records):
        expected = case["queries"][index]
        require(actual["query"] == expected["id"], "different query/order")
        require(actual["rows"] == case["rows"], "different row count")
        require(actual["dtype"] == expected["dtype"], "different complete schema")
        require(actual["complete_jsonl_sha256"] == expected["jsonl_sha256"], "different complete values")
        require(actual["certified"] is True and actual["fallback_attempted"] is False,
                "missing native no-fallback certificate")


def verify(root, report_path):
    manifest_path = root / "fixture-manifest.json"
    manifest = json.loads(manifest_path.read_bytes())
    report = json.loads(report_path.read_bytes())
    require(report["fixture_manifest_sha256"] == digest(manifest_path.read_bytes()), "report input mismatch")
    require(manifest["provider"] == "unpatched-vortex-0.85.0", "wrong independent provider")
    require([case["id"] for case in manifest["cases"]] == CASES, "missing/reordered fixture")
    require([case["id"] for case in report["cases"]] == CASES, "missing/reordered control")
    authority = []
    for case, actual in zip(manifest["cases"], report["cases"]):
        source_hash = hashlib.sha256()
        for kind in ("dtype_message", "array_message"):
            data = verified_file(root, case[kind])
            source_hash.update(struct.pack("<Q", len(data)))
            source_hash.update(data)
        require(len(case["queries"]) == 3, "wrong projection set")
        for query in case["queries"]:
            compressed = verified_file(root, query["reference"])
            restored = gzip.decompress(compressed)
            require(len(restored) == query["jsonl_bytes"], "reference length mismatch")
            require(digest(restored) == query["jsonl_sha256"], "reference hash mismatch")
            require(restored.count(b"\n") == case["rows"], "reference row mismatch")
        query_checks(actual["native_queries"], case)
        native = actual["ordinary_native_ipc_control"]
        query_checks(native["queries"], case)
        require(len(native["messages"]) == 2, "wrong native message count")
        require(native["payload_bytes"] == sum(m["bytes"] for m in native["messages"]), "wrong native total")
        require(len(native["ordered_payload_sha256"]) == 64, "missing native completion hash")
        require(native["payload_bytes"] <= 64 << 20, "native payload limit")
        for family in ("arrow_ipc_controls", "arrow_flight_controls"):
            require([control["codec"] for control in actual[family]] == ["none", "lz4", "zstd"],
                    "missing/reordered Arrow codec")
            for control in actual[family]:
                query_checks(control["queries"], case)
        for control in actual["arrow_flight_controls"]:
            require(control["dictionary_handling"] == "Resend", "hydrated dictionary control")
            require(control["max_flight_data_size"] == 1 << 20, "changed Flight encoder")
            require(0 < len(control["frames"]) <= 256, "Flight frame limit")
            require(control["frames"][0]["header_kind"] == "Schema", "missing Flight schema")
            require(control["payload_bytes"] == sum(f["header_bytes"] + f["body_bytes"] for f in control["frames"]),
                    "wrong Arrow total")
            require(control["payload_bytes"] <= 64 << 20 and control["declared_decoded_buffer_bytes"] <= 64 << 20,
                    "Arrow payload/allocation limit")
            require(len(control["ordered_payload_sha256"]) == 64, "missing Arrow completion hash")
        require(actual["final_reserved_bytes"] == 0, "leaked native credit")
        require(actual["native_intake"]["materialized_batches"] == 0
                and actual["native_intake"]["materialized_rows"] == 0, "unexpected native materialization")
        encoded_ops = operations(case)
        authority.append({
            "id": case["id"], "rows": case["rows"], "source_sha256": source_hash.hexdigest(),
            "dtype_sha256": case["dtype_message"]["sha256"],
            "operations_protobuf_hex": encoded_ops.hex(), "operation_sha256": digest(encoded_ops),
            "ordinary_native_ipc": {key: value for key, value in native.items() if key != "queries"},
            "arrow_flight": [{key: value for key, value in control.items() if key != "queries"}
                             for control in actual["arrow_flight_controls"]],
        })
    return manifest, report, authority


def freeze(args):
    require(not args.output.exists(), "freeze output already exists")
    first = verify(args.fixtures, args.controls)
    second = verify(args.repeat_fixtures, args.repeat_controls)
    require(first == second, "fresh-process inputs or controls are not reproducible")
    recipes = Path(__file__).resolve().parent
    repo = recipes.parents[3]
    for record in first[0]["recipes"]:
        require(identity(recipes / record["file"], record["file"]) == record, "changed fixture recipe")
    receiver = (repo / "shardloom-vortex/src/resident_memory_ipc.rs").read_text()
    registry_section = receiver.split("const IDS: &[&str] = &[")[1].split("];", 1)[0]
    require([line.strip().strip(',').strip('"') for line in registry_section.splitlines() if line.strip()] == REGISTRY,
            "receiver registry differs from negotiation profile")
    sources = [
        "Cargo.toml", "Cargo.lock", "shardloom-vortex/Cargo.toml",
        "shardloom-vortex/src/native_payload_schema.rs",
        "shardloom-vortex/src/resident_memory_arrow.rs",
        "shardloom-vortex/src/resident_memory_arrow_encoded_tests.rs",
        "shardloom-vortex/src/resident_memory_exchange_fixture_tests.rs",
        "shardloom-vortex/src/resident_memory_ipc.rs",
        "shardloom-vortex/tests/fixtures/native-exchange/exchange-v1.proto",
        "shardloom-vortex/tests/fixtures/native-exchange/freeze.py",
        "docs/architecture/native-exchange-experiment-plan-2026-10-20.md",
        "docs/architecture/native-encoded-exchange-protocol.md",
        "docs/architecture/native-exchange-arrow-control-2026-10-20.md",
    ]
    record = {
        "version": 1, "status": "frozen_before_candidate_implementation_and_timing",
        "scope": "finite loopback experiment; no transport support or performance claim",
        "fixture_manifest": identity(args.fixtures / "fixture-manifest.json"),
        "controls": identity(args.controls), "repeat_controls": identity(args.repeat_controls),
        "reproducibility": {"independent_generations": 2, "complete_queries_each": 416,
                            "all_input_reference_and_control_identities_equal": True},
        "sources": [identity(repo / name, name) for name in sources],
        "recipe_identities": first[0]["recipes"], "modes": MODES, "limits": LIMITS,
        "profile": {"extension_version": 1, "vortex_version": "0.85.0", "vortex_message_version": 0,
                    "physical_registry": REGISTRY, "protobuf_hex": profile().hex(), "sha256": digest(profile())},
        "execution_order": [0, 1, 2, 1], "cases": first[2],
        "ordinary_sql_client": {"provider": "arrow-flight-58.3.0 FlightSqlServiceClient",
                                "case": "dictionary_large", "sql": SQL,
                                "reference_query": "last_first_renamed"},
        "cohorts": {"unscored_preflights": 65, "independent_cohorts": 2,
                    "samples_per_case_mode": 5, "workflows_per_cohort": 325,
                    "role_rotation": "(case_index + sample_index) % 5; reverse roles and case order in cohort 2",
                    "bootstrap_resamples": 10000, "bootstrap_seed": 3347,
                    "upper_order_statistic_zero_based": 9499},
        "targets": [case for case in CASES if case not in ("empty", "tiny", "nullable_plain", "wide_text")],
        "nonbenefiting_controls": ["empty", "tiny", "nullable_plain", "wide_text"],
        "gates": {"all_correctness_security_lifetime_resources_client_checks": "required",
                  "all_cases_native_workflow_max_increase": {"fraction": 0.05, "seconds": 0.005, "rule": "larger allowance"},
                  "nonbenefiting_each_arrow_workflow_max_increase": {"fraction": 0.05, "seconds": 0.005, "rule": "larger allowance"},
                  "all_cases_native_engine_max_increase": {"fraction": 0.05, "seconds": 0.0005, "rule": "larger allowance"},
                  "target_geometric_mean_workflow_ratio_max_each_arrow": 0.90,
                  "target_upper_bootstrap_ratio_exclusive_max_each_arrow": 1.0,
                  "distinct_winning_encoded_families_min": 2,
                  "winning_target_workflow_ratio_max_each_arrow": 0.90,
                  "winning_target_wire_or_combined_peer_cpu_ratio_max_each_arrow": 0.90,
                  "candidate_whole_message_assembly_bytes": 0,
                  "native_control_whole_message_assembly_bytes": "positive in every case",
                  "decision": "both cohorts must pass every gate"},
        "resources": {"connections_including_tls": 2, "active_data_rpcs_per_connection": 1,
                      "active_attempts": 2, "terminal_attempts_per_epoch": 256,
                      "http2_stream_window_bytes": 1 << 20, "http2_connection_window_bytes": 2 << 20,
                      "http2_max_header_bytes": 16 << 10, "http2_max_frame_bytes": 16 << 10,
                      "http2_adaptive_windows": False, "grpc_compression": False,
                      "tls_handshake_seconds": 5, "idle_progress_seconds": 5, "attempt_seconds": 60,
                      "parent_workflow_seconds": 120, "drain_seconds": 5,
                      "combined_peer_rss_bytes": 2 << 30,
                      "arrow_max_header_bytes": 64 << 10, "arrow_max_nodes_or_buffers": 4096,
                      "arrow_max_rows": 65536, "arrow_declared_decoded_bytes": 64 << 20},
        "planned_commands": [
            ["cargo", "test", "--offline", "--locked", "-p", "shardloom-vortex", "--features",
             "release-user-surfaces,native-encoded-exchange-experiment", "--test", "native_encoded_exchange_protocol",
             "--", "--nocapture", "--test-threads=1"],
            *[["cargo", "test", "--offline", "--locked", "--release", "-p", "shardloom-vortex", "--features",
               "release-user-surfaces,native-encoded-exchange-experiment", "--test", target,
               "--", "--nocapture", "--test-threads=1"]
              for target in ("native_encoded_exchange_two_process", "native_encoded_exchange_client_interop")],
            ["cargo", "build", "--offline", "--locked", "--release", "-p", "shardloom-vortex", "--features",
             "native-encoded-exchange-experiment", "--bin", "shardloom-native-exchange-experiment"],
            ["shardloom-native-exchange-experiment", "sender", "<private-run-config.json>"],
            ["shardloom-native-exchange-experiment", "receiver", "<private-run-config.json>"],
            ["shardloom-native-exchange-experiment", "sql-client", "<private-run-config.json>"],
        ],
        "execution_requirements": "sequential guarded local storage; fresh child pair/connection; exact source/build/argv frozen after correctness before measurement; every failure retained",
    }
    with args.output.open("xb") as stream:
        stream.write(json.dumps(record, indent=2, sort_keys=True).encode() + b"\n")
    print(json.dumps(identity(args.output)))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    for option in ("fixtures", "repeat-fixtures", "controls", "repeat-controls", "output"):
        parser.add_argument("--" + option, type=Path, required=True)
    freeze(parser.parse_args())
