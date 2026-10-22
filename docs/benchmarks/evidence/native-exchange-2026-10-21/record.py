# SPDX-License-Identifier: Apache-2.0
"""Preserve completed exchange records, including failures, without bulk backups."""
import argparse
import json
import lzma
from pathlib import Path
import re
import subprocess

from verify import BASELINE, CANDIDATE, HERE, REPOSITORY, identity, replay, require


def main(evidence):
    require(not (HERE / "decision.json").exists() and not (HERE / "results.json.xz").exists(),
            "output already exists")
    require(subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=REPOSITORY, text=True).strip() == CANDIDATE,
            "record from the scored candidate revision before retiring its source")
    selected = sorted(path for path in evidence.iterdir() if path.is_file() and "exchange" in path.name
                      and (re.search(r"-84[1-7](?:-|\.)", path.name)
                           or re.search(r"_84[1-7]\.py$", path.name)))
    require(len(selected) == 229, "unexpected experiment artifact inventory; review before changing scope")
    members = {}

    def add(path, name):
        data = path.read_bytes()
        require(b"-----BEGIN PRIVATE KEY-----" not in data, f"private key content: {name}")
        require(name not in members, f"duplicate member: {name}")
        members[name] = {**identity(data), "text": data.decode()}

    for path in selected:
        add(path, path.name)
    add(evidence / "native-exchange-fixtures-839-a3/fixture-manifest.json", "fixture-manifest.json")
    add(HERE.parent / "native-exchange-control-2026-10-20/protocol-freeze.json", "protocol-freeze.json")
    add(evidence / "run_guarded_observed_757.py", "run_guarded_observed_757.py")
    build = json.loads(members["exchange-validated-build-freeze-846-a1.json"]["text"])
    for row in build["sources"]:
        require(identity((REPOSITORY / row["file"]).read_bytes()) == {key: row[key] for key in ("bytes", "sha256")},
                f"current candidate source changed: {row['file']}")
    result = replay(members)
    packet = {"schema": "shardloom.native_exchange_decision_packet.v1", "members": members}
    raw = json.dumps(packet, sort_keys=True, allow_nan=False).encode()
    compressed = lzma.compress(raw)
    require(lzma.decompress(compressed) == raw, "compressed readback changed")
    changes = subprocess.check_output(["git", "diff", "--name-status", BASELINE, CANDIDATE],
                                      cwd=REPOSITORY, text=True).splitlines()
    record = {
        "schema": "shardloom.native_exchange_decision.v1", "status": "rejected_frozen_mechanism",
        "candidate_commit": CANDIDATE,
        "baseline_commit": subprocess.check_output(["git", "rev-parse", BASELINE], cwd=REPOSITORY, text=True).strip(),
        "candidate_source_changes": [dict(zip(("status", "file"), line.split("\t"))) for line in changes],
        "packet": {"file": "results.json.xz", **identity(compressed), "members": len(members)},
        "replay": result, "correctness_acceptance": build["correctness_summary"],
        "proof_limits": [
            "Finite loopback harness with frozen immutable input, not a public service or arbitrary-input admission.",
            "Cost gates are rejected in both cohorts; target-family wins do not override controls or aggregate failure.",
            "Provider/TLS/kernel allocations and copies are not measured exactly; native grants and observed RSS are distinct.",
            "Ordinary desktop activity is observed context, not proof of an idle host or a causal attribution.",
            "Protocol/client checks do not close the four broader native-exchange substeps or any CG gate.",
            "Source retirement, restored-runtime validation and generated-artifact cleanup are separate postchecks.",
        ],
        "fallback_attempted": False, "external_engine_invoked": False,
        "source_retirement_proof": "postchecks.json", "generated_artifact_cleanup_proof": "cleanup.json",
    }
    with (HERE / "results.json.xz").open("xb") as stream:
        stream.write(compressed)
    require(lzma.decompress((HERE / "results.json.xz").read_bytes()) == raw, "on-disk readback changed")
    with (HERE / "decision.json").open("x") as stream:
        json.dump(record, stream, indent=2, sort_keys=True, allow_nan=False)
        stream.write("\n")
    print(json.dumps({"status": record["status"], "members": len(members), "packet_bytes": len(compressed),
                      "scored_workflows": result["scored_workflows"], "failed_receipts_preserved": len(result["failure_history"])}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence", type=Path, required=True)
    main(parser.parse_args().evidence)
