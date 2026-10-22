# SPDX-License-Identifier: Apache-2.0
"""Record or replay exchange source restoration and its fresh validation."""
import argparse
import json
import lzma
from pathlib import Path
import re
import ssl
import subprocess

from verify import BASELINE, HERE, REPOSITORY, identity, require, verify_git_sources

CHECKS = (
    "exchange-restored-fmt-848-a1", "exchange-restored-feature-clippy-848-a1",
    "exchange-restored-workspace-clippy-848-a1", "exchange-restored-workspace-tests-848-a1",
    "exchange-restored-native-tests-848-a1",
)


def load_packet(path):
    return json.loads(lzma.decompress(path.read_bytes()))


def check_members(members):
    for name, entry in members.items():
        require(identity(entry["text"].encode()) == {key: entry[key] for key in ("bytes", "sha256")}, name)


def test_counts(text):
    rows = [dict(zip(("passed", "failed", "ignored"), map(int, row))) for row in
            re.findall(r"test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored;", text)]
    return {"summaries": len(rows), "totals": {key: sum(row[key] for row in rows)
            for key in ("passed", "failed", "ignored")}, "individual": rows}


def verify_postchecks(record, packet):
    members = packet["members"]
    check_members(members)
    decision = json.loads((HERE / "decision.json").read_bytes())
    original = load_packet(HERE / "results.json.xz")["members"]
    build = json.loads(original["exchange-validated-build-freeze-846-a1.json"]["text"])
    changes = decision["candidate_source_changes"]
    require(record["removed_files"] == sorted(row["file"] for row in changes if row["status"] == "A")
            and len(record["removed_files"]) == 34, "removed-file scope differs")
    require(record["restored_files"] == sorted(row["file"] for row in changes if row["status"] == "M")
            and len(record["restored_files"]) == 3, "restored-file scope differs")
    removed = set(record["removed_files"])
    expected = {row["file"] for row in build["sources"]} - removed
    require({row["file"] for row in record["restored_source_inputs"]} == expected, "restored input coverage differs")
    verify_git_sources(REPOSITORY, BASELINE, record["restored_source_inputs"])
    for check in record["checks"]:
        name = check["name"]
        receipt = json.loads(members[name + ".json"]["text"])
        log = members[name + ".log"]["text"]
        require(receipt["status"] == "passed" and receipt["returncode"] == 0
                and receipt["direct_child_reaped"] and receipt["process_group_drained"], name)
        require(identity(log.encode())["sha256"] == receipt["log_sha256"], name)
        require(receipt["command"] == check["command"] and test_counts(log) == check["tests"], name)
    require(tuple(check["name"] for check in record["checks"]) == CHECKS, "restored check set differs")
    by_name = {check["name"]: check for check in record["checks"]}
    require(by_name[CHECKS[3]]["tests"]["totals"] == {"passed": 3235, "failed": 0, "ignored": 0},
            "workspace coverage differs")
    require(by_name[CHECKS[4]]["tests"]["individual"][0] == {"passed": 3052, "failed": 0, "ignored": 31},
            "native unit coverage differs")
    require(by_name[CHECKS[4]]["tests"]["totals"]["failed"] == 0, "native integration failure")

    manifests = {name: json.loads(entry["text"]) for name, entry in members.items()
                 if name.endswith("/public-manifest.json")}
    require(len(manifests) == 16, "public certificate manifest inventory differs")
    for name, manifest in manifests.items():
        for certificate in manifest["certificates"]:
            pem = members[str(Path(name).parent / certificate["filename"])]["text"]
            der = ssl.PEM_cert_to_DER_cert(pem)
            require(identity(der)["sha256"] == certificate["der_sha256"], name)
    for name, entry in original.items():
        if not name.endswith(".json"):
            continue
        report = json.loads(entry["text"])
        if not isinstance(report, dict) or not isinstance(report.get("certificates"), dict):
            continue
        certificates = report["certificates"]
        if "manifest" not in certificates:
            continue
        relative = "/".join(Path(certificates["manifest"]["path"]).parts[-2:])
        require(json.loads(members[relative]["text"]) == certificates["public"], relative)
        require(identity(members[relative]["text"].encode()) == {
            key: certificates["manifest"][key] for key in ("bytes", "sha256")
        }, relative)
    return {"status": "verified", "restored_files": 3, "removed_files": 34,
            "restored_source_inputs": len(record["restored_source_inputs"]),
            "passed_validation_commands": len(CHECKS), "public_certificate_sets": len(manifests),
            "workspace": by_name[CHECKS[3]]["tests"]["totals"],
            "native_all_targets": by_name[CHECKS[4]]["tests"]["totals"],
            "native_unit": by_name[CHECKS[4]]["tests"]["individual"][0],
            "public_package_or_full43_rerun": False}


def record(evidence):
    require(not (HERE / "postchecks.json").exists() and not (HERE / "postchecks.json.xz").exists(),
            "postcheck outputs already exist")
    decision = json.loads((HERE / "decision.json").read_bytes())
    original = load_packet(HERE / "results.json.xz")["members"]
    build = json.loads(original["exchange-validated-build-freeze-846-a1.json"]["text"])
    removed = sorted(row["file"] for row in decision["candidate_source_changes"] if row["status"] == "A")
    restored = sorted(row["file"] for row in decision["candidate_source_changes"] if row["status"] == "M")
    require(all(not (REPOSITORY / name).exists() for name in removed), "candidate source remains")
    sources = [{"file": row["file"], **identity((REPOSITORY / row["file"]).read_bytes())}
               for row in build["sources"] if row["file"] not in removed]
    verify_git_sources(REPOSITORY, BASELINE, sources)
    runtime_diff = subprocess.check_output([
        "git", "diff", "--name-only", BASELINE, "--", "*.rs", "Cargo.toml", "Cargo.lock", "*/Cargo.toml", "scripts"
    ], cwd=REPOSITORY, text=True)
    require(not runtime_diff, "unexpected active runtime/script difference from accepted prerequisite baseline")
    members = {}

    def add(path, name):
        data = path.read_bytes()
        require(b"PRIVATE KEY-----" not in data and not path.name.endswith(".key.pem"), name)
        members[name] = {**identity(data), "text": data.decode()}

    checks = []
    for name in CHECKS:
        for suffix in (".json", ".log"):
            add(evidence / (name + suffix), name + suffix)
        receipt = json.loads(members[name + ".json"]["text"])
        checks.append({"name": name, "command": receipt["command"],
                       "tests": test_counts(members[name + ".log"]["text"])})
    for directory in sorted(evidence.iterdir()):
        if not (directory.is_dir() and directory.name.startswith("exchange-")
                and re.search(r"-84[1-7](?:-|\.)", directory.name)):
            continue
        require(not list(directory.rglob("*.key.pem")), f"private keys remain: {directory.name}")
        for path in sorted(directory.iterdir()):
            require(path.is_file() and (path.name.endswith(".cert.pem") or path.name == "public-manifest.json"),
                    f"unexpected public certificate member: {path.name}")
            add(path, directory.name + "/" + path.name)
    packet = {"schema": "shardloom.native_exchange_postchecks_packet.v1", "members": members}
    result = {"schema": "shardloom.native_exchange_postchecks.v1", "status": "passed",
              "baseline_commit": decision["baseline_commit"], "candidate_commit": decision["candidate_commit"],
              "removed_files": removed, "restored_files": restored, "restored_source_inputs": sources,
              "runtime_and_active_scripts_diff_from_baseline": [], "checks": checks,
              "proof_scope": "Fresh restored-source validation; suites overlap. Existing public package and Full43 evidence is historical."}
    result["replay"] = verify_postchecks(result, packet)
    raw = json.dumps(packet, sort_keys=True, allow_nan=False).encode()
    compressed = lzma.compress(raw)
    require(lzma.decompress(compressed) == raw, "postcheck compression changed data")
    result["packet"] = {"file": "postchecks.json.xz", **identity(compressed), "members": len(members)}
    with (HERE / "postchecks.json.xz").open("xb") as stream:
        stream.write(compressed)
    with (HERE / "postchecks.json").open("x") as stream:
        json.dump(result, stream, indent=2, sort_keys=True, allow_nan=False)
        stream.write("\n")
    print(json.dumps(result["replay"], sort_keys=True))


def verify():
    result = json.loads((HERE / "postchecks.json").read_bytes())
    compressed = (HERE / "postchecks.json.xz").read_bytes()
    require(identity(compressed) == {key: result["packet"][key] for key in ("bytes", "sha256")},
            "postcheck packet identity changed")
    packet = json.loads(lzma.decompress(compressed))
    require(len(packet["members"]) == result["packet"]["members"], "postcheck packet inventory changed")
    replay = verify_postchecks(result, packet)
    require(replay == result["replay"], "postcheck replay changed")
    cleanup_path = HERE / "cleanup.json"
    if cleanup_path.exists():
        cleanup = json.loads(cleanup_path.read_bytes())
        require(cleanup["status"] == "completed" and not cleanup["failures"], "cleanup incomplete")
        require(all(row["retired"] and row["open_handles"] == 0 for row in cleanup["artifacts"]),
                "cleanup lacks closed-handle retirement proof")
        detail_bytes = (HERE / cleanup["details"]["file"]).read_bytes()
        require(identity(detail_bytes) == {key: cleanup["details"][key] for key in ("bytes", "sha256")},
                "cleanup detail identity changed")
        detail = json.loads(lzma.decompress(detail_bytes))
        require(detail["driver_receipt"]["status"] == "passed"
                and detail["driver_receipt"]["retained_identities_unchanged"]
                and set(detail["driver_receipt"]["completed"]) == {row["path"] for row in cleanup["artifacts"]},
                "cleanup driver does not prove the claimed retirement")
        for row in cleanup["artifacts"]:
            expected = detail["plan"]["members"][row["path"]]
            require(all(row[key] == expected[key] for key in ("path", "bytes", "sha256")),
                    "cleanup artifact identity differs from the checked plan")
        replay = {**replay, "retired_generated_artifacts": len(cleanup["artifacts"])}
    print(json.dumps(replay, sort_keys=True))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--record", type=Path, metavar="EVIDENCE_DIRECTORY")
    args = parser.parse_args()
    record(args.record) if args.record else verify()
