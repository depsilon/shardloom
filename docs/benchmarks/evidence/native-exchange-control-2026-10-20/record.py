# SPDX-License-Identifier: Apache-2.0
"""Record the finite encoded-control prerequisite, including failed attempts."""
import argparse
import hashlib
import json
import lzma
from pathlib import Path
import re
import subprocess


REQUIRED = [
    "exchange-arrow-tests-838-a2", "exchange-arrow-default-tests-838-a1",
    "exchange-fixture-clippy-839-a3", "exchange-fixture-generate-839-a3",
    "exchange-fixture-reference-839-a3", "exchange-integrated-policy-839-a1",
    "exchange-integrated-audit-839-a1", "exchange-prerequisite-native-full-839-a1",
    "exchange-prerequisite-workspace-fmt-839-a1", "exchange-prerequisite-workspace-clippy-839-a1",
    "exchange-prerequisite-workspace-tests-839-a1", "exchange-fixture-regenerate-840-a1",
    "exchange-fixture-reference-840-a1", "exchange-fixture-validation-840-a1",
    "exchange-fixture-validation-840-a2", "exchange-fixture-validation-clippy-840-a1",
]


def identity(data):
    return {"bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()}


def main(args):
    out = Path(__file__).resolve().parent
    assert not (out / "acceptance.json").exists()
    assert not (out / "prerequisites.json.xz").exists()
    freeze_bytes = (out / "protocol-freeze.json").read_bytes()
    freeze = json.loads(freeze_bytes)
    for row in freeze["sources"]:
        assert identity((args.repository / row["file"]).read_bytes()) == {k: row[k] for k in ("bytes", "sha256")}
    members = {}

    def add(path, name):
        data = path.read_bytes()
        members[name] = {**identity(data), "text": data.decode()}

    checks = []
    for path in sorted(args.evidence.glob("exchange-*.json")):
        if not re.search(r"-(837|838|839|840)-a\d+\.json$", path.name):
            continue
        receipt = json.loads(path.read_bytes())
        if "status" not in receipt or "command" not in receipt:
            continue
        log = path.with_suffix(".log")
        assert hashlib.sha256(log.read_bytes()).hexdigest() == receipt["log_sha256"]
        assert receipt["direct_child_reaped"] and receipt["process_group_drained"]
        add(path, path.name)
        add(log, log.name)
        counts = [dict(zip(("passed", "failed", "ignored"), map(int, match)))
                  for match in re.findall(r"test result: (?:ok|FAILED)\. (\d+) passed; (\d+) failed; (\d+) ignored;", log.read_text())]
        checks.append({"name": path.stem, "status": receipt["status"], "returncode": receipt["returncode"],
                       "command": receipt["command"], "tests": counts})
    by_name = {row["name"]: row for row in checks}
    for name in REQUIRED:
        assert by_name[name]["status"] == "passed" and by_name[name]["returncode"] == 0, name
    assert by_name["exchange-arrow-tests-838-a2"]["tests"][0] == {"passed": 11, "failed": 0, "ignored": 0}
    assert by_name["exchange-arrow-default-tests-838-a1"]["tests"][0] == {"passed": 1, "failed": 0, "ignored": 0}
    assert by_name["exchange-prerequisite-native-full-839-a1"]["tests"][0] == {"passed": 3052, "failed": 0, "ignored": 31}
    for name in ("exchange-fixture-validation-840-a1", "exchange-fixture-validation-840-a2"):
        assert by_name[name]["tests"][0] == {"passed": 1, "failed": 0, "ignored": 0}

    extras = [
        "exchange-integrated-lock-acceptance-839.json", "exchange-fixture-lock-acceptance-839.json",
        "native-exchange-arrow-source-acceptance-837.json", "native-exchange-transport-source-acceptance-838.json",
        "native-exchange-arrow-intake-inventory-837.json", "native-exchange-arrow-nullability-inventory-838.json",
        "native-exchange-arrow-wire-bounds-inventory-838.json", "native-exchange-engine-clock-inventory-838.json",
        "native-exchange-fixture-api-inventory-839.json", "native-exchange-fixture-api-inventory-839-r2.json",
        "native-exchange-flight-sql-api-inventory-839.json", "native-exchange-transport-api-inventory-840.json",
        "native-exchange-fixture-validation-839-a3.json", "native-exchange-fixture-validation-839-a4.json",
        "native-exchange-fixture-validation-839-a5.json", "native-exchange-fixture-validation-840-a1.json",
        "native-exchange-fixture-validation-840-a2.json", "run_guarded_observed_757.py",
    ]
    for name in extras:
        add(args.evidence / name, name)
    for name in ("native-exchange-fixtures-839-a2", "native-exchange-fixtures-839-a3", "native-exchange-fixtures-840-a1"):
        add(args.evidence / name / "fixture-manifest.json", name + "/fixture-manifest.json")
    for row in freeze["sources"]:
        add(args.repository / row["file"], "source/" + row["file"])
    for row in freeze["recipe_identities"]:
        add(args.repository / "shardloom-vortex/tests/fixtures/native-exchange" / row["file"], "recipe/" + row["file"])
    add(out / "protocol-freeze.json", "protocol-freeze.json")
    add(out / "record.py", "record.py")
    add(out / "verify.py", "verify.py")
    audit = json.loads((args.evidence / "exchange-integrated-audit-839-a1.log").read_bytes())
    assert audit["vulnerabilities"]["count"] == 0
    warnings = [{"kind": kind, "package": item["package"]["name"], "version": item["package"]["version"],
                 "advisory": item["advisory"]["id"]}
                for kind, entries in audit["warnings"].items() for item in entries]
    packet = {"schema": "shardloom.native_exchange_control_packet.v1", "members": members}
    raw = json.dumps(packet, sort_keys=True).encode()
    compressed = lzma.compress(raw)
    assert lzma.decompress(compressed) == raw
    with (out / "prerequisites.json.xz").open("xb") as stream:
        stream.write(compressed)
    record = {
        "schema": "shardloom.native_exchange_control_acceptance.v1", "status": "accepted_finite_prerequisite",
        "baseline_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=args.repository, text=True).strip(),
        "packet": {"file": "prerequisites.json.xz", **identity(compressed), "members": len(members)},
        "protocol_freeze": {"file": "protocol-freeze.json", **identity(freeze_bytes)},
        "required_checks": REQUIRED, "checks": checks,
        "complete_query_checks_per_fixture_validation": 416, "fixture_count": 13,
        "reproducible_independent_generations": 2, "identical_flight_frames_across_generations": 111,
        "advisory_database": audit["database"], "known_vulnerabilities": 0, "existing_warnings": warnings,
        "primary_review": [
            "Feature-only schema intake covers integer dictionaries and signed run ends without broadening float/decimal validation; ordinary release policy still refuses wrappers.",
            "All source and codec controls consume complete independently specified values through the native prepared relational path; repeated execution increments the native session count.",
            "Current dictionary stride exercises every code in both domains; corrected recipes are frozen before any transport candidate. The earlier incomplete/degenerate inputs are not scored.",
            "Control identity is reproducible, including native MessageEncoder output before original-source queries; Arrow Resend retains its standard header/body representation.",
            "Verified all 15 transport inventory source hashes and inspected interceptor extension preservation, peer-certificate lookup, Connected TLS metadata and HTTP request propagation.",
            "Arrow LZ4 declared size is not an allocation ceiling. The finite plan requires exact prevalidated frame identity before allocating decode; this is not arbitrary-input decompression certification.",
            "The final change after the broad feature suite only adds ordered completion hashes/totals to its ignored fixture check. Both updated full fixture validations and strict feature Clippy pass; runtime/schema policy is unchanged.",
        ],
        "failure_history": [row["name"] for row in checks if row["status"] != "passed"],
        "fixture_recipe_corrections": ["register run-end/fastlanes with the isolated no-default-feature provider", "use stride 19 so every dictionary code is exercised"],
        "proof_limits": ["no transport candidate or network execution", "no TLS/authorization/failure/client runtime acceptance", "no timing comparison or performance claim", "native grants do not cover all provider allocations or total RSS"],
        "completed_native_exchange_substeps": [], "fallback_attempted": False, "external_engine_invoked": False,
    }
    with (out / "acceptance.json").open("xb") as stream:
        stream.write(json.dumps(record, indent=2, sort_keys=True).encode() + b"\n")
    print(json.dumps({"packet_members": len(members), "checks": len(checks), "failures_preserved": record["failure_history"]}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--repository", type=Path, required=True)
    main(parser.parse_args())
