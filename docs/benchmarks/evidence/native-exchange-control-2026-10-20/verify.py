# SPDX-License-Identifier: Apache-2.0
"""Replay the portable prerequisite packet; no network or native query runs."""
import hashlib
import json
import lzma
from pathlib import Path
import re


def match(raw, record):
    assert len(raw) == record["bytes"] and hashlib.sha256(raw).hexdigest() == record["sha256"]


def main():
    root = Path(__file__).resolve().parent
    acceptance = json.loads((root / "acceptance.json").read_bytes())
    assert acceptance["schema"] == "shardloom.native_exchange_control_acceptance.v1"
    assert acceptance["status"] == "accepted_finite_prerequisite"
    packet_raw = (root / acceptance["packet"]["file"]).read_bytes()
    match(packet_raw, acceptance["packet"])
    packet = json.loads(lzma.decompress(packet_raw))
    assert packet["schema"] == "shardloom.native_exchange_control_packet.v1"
    members = packet["members"]
    assert len(members) == acceptance["packet"]["members"]
    for member in members.values():
        match(member["text"].encode(), member)

    def data(name):
        return json.loads(members[name]["text"])

    freeze_raw = (root / "protocol-freeze.json").read_bytes()
    match(freeze_raw, acceptance["protocol_freeze"])
    assert freeze_raw == members["protocol-freeze.json"]["text"].encode()
    freeze = json.loads(freeze_raw)
    assert freeze["status"] == "frozen_before_candidate_implementation_and_timing"
    for record in freeze["sources"]:
        match(members["source/" + record["file"]]["text"].encode(), record)
    for record in freeze["recipe_identities"]:
        match(members["recipe/" + record["file"]]["text"].encode(), record)
    checks = {record["name"]: record for record in acceptance["checks"]}
    for name, record in checks.items():
        receipt = data(name + ".json")
        log = members[name + ".log"]["text"]
        assert hashlib.sha256(log.encode()).hexdigest() == receipt["log_sha256"]
        assert receipt["status"] == record["status"] and receipt["returncode"] == record["returncode"]
        assert receipt["direct_child_reaped"] and receipt["process_group_drained"]
        counts = [dict(zip(("passed", "failed", "ignored"), map(int, item)))
                  for item in re.findall(r"test result: (?:ok|FAILED)\. (\d+) passed; (\d+) failed; (\d+) ignored;", log)]
        assert counts == record["tests"]
    for name in acceptance["required_checks"]:
        assert checks[name]["status"] == "passed" and checks[name]["returncode"] == 0
    assert acceptance["failure_history"] == [record["name"] for record in acceptance["checks"] if record["status"] != "passed"]
    manifest = data("native-exchange-fixtures-839-a3/fixture-manifest.json")
    assert manifest == data("native-exchange-fixtures-840-a1/fixture-manifest.json")
    match(members["native-exchange-fixtures-839-a3/fixture-manifest.json"]["text"].encode(), freeze["fixture_manifest"])
    controls = data("native-exchange-fixture-validation-840-a1.json")
    assert controls == data("native-exchange-fixture-validation-840-a2.json")
    for name in ("controls", "repeat_controls"):
        match(members[freeze[name]["file"]]["text"].encode(), freeze[name])
    assert len(controls["cases"]) == len(manifest["cases"]) == len(freeze["cases"]) == 13
    queries = frames = 0
    for expected, actual, authority in zip(manifest["cases"], controls["cases"], freeze["cases"]):
        assert expected["id"] == actual["id"] == authority["id"]
        routes = [actual["native_queries"], actual["ordinary_native_ipc_control"]["queries"]]
        for group in ("arrow_ipc_controls", "arrow_flight_controls"):
            assert [control["codec"] for control in actual[group]] == ["none", "lz4", "zstd"]
            routes.extend(control["queries"] for control in actual[group])
        for route in routes:
            assert len(route) == 4
            for index, result in zip([0, 1, 2, 1], route):
                query = expected["queries"][index]
                assert result["query"] == query["id"] and result["rows"] == expected["rows"]
                assert result["dtype"] == query["dtype"]
                assert result["complete_jsonl_sha256"] == query["jsonl_sha256"]
                assert result["certified"] is True and result["fallback_attempted"] is False
                queries += 1
        for control, frozen in zip(actual["arrow_flight_controls"], authority["arrow_flight"]):
            assert {key: value for key, value in control.items() if key != "queries"} == frozen
            frames += len(control["frames"])
        assert {key: value for key, value in actual["ordinary_native_ipc_control"].items() if key != "queries"} == authority["ordinary_native_ipc"]
        assert actual["final_reserved_bytes"] == 0
    assert queries == acceptance["complete_query_checks_per_fixture_validation"] == 416
    assert frames == acceptance["identical_flight_frames_across_generations"] == 111
    assert acceptance["completed_native_exchange_substeps"] == []
    assert acceptance["fallback_attempted"] is False and acceptance["external_engine_invoked"] is False
    print(json.dumps({"status": "passed", "members": len(members), "complete_queries": queries, "flight_frames": frames,
                      "scope": "portable prerequisite replay; no transport or timing acceptance"}))


if __name__ == "__main__":
    main()
