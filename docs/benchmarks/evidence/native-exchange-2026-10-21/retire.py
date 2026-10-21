# SPDX-License-Identifier: Apache-2.0
"""Historical, exact-path generated-artifact retirement after exchange rejection."""
import argparse
import hashlib
import importlib.util
import json
import lzma
from pathlib import Path
import stat
import subprocess
import sys

from verify import CANDIDATE, HERE, REPOSITORY, identity, require

EVIDENCE = Path("/Users/dylan/LocalData/shardloom/conditional-investigations-20261019")
CACHE = Path("/Users/dylan/.cache/shardloom/cargo-target")
PLAN = EVIDENCE / "exchange-retirement-plan-848.json"
DRIVER = EVIDENCE / "cleanup_generated_storage_799.py"
RECEIPT = EVIDENCE / "exchange-retirement-driver-848.json"


def digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def file_identity(path):
    value = path.lstat()
    require(stat.S_ISREG(value.st_mode) and not path.is_symlink(), f"unexpected file kind: {path}")
    return {"path": str(path), "bytes": value.st_size, "sha256": digest(path)}


def is_exchange(name):
    return any(part in name for part in ("native_exchange", "native-exchange", "native_encoded_exchange"))


def prepare():
    require(not PLAN.exists() and not RECEIPT.exists() and not (HERE / "cleanup.json").exists(),
            "retirement output already exists")
    subprocess.run([sys.executable, "-B", str(HERE / "verify.py")], check=True, stdout=subprocess.DEVNULL)
    subprocess.run([sys.executable, "-B", str(HERE / "verify_closeout.py")], check=True, stdout=subprocess.DEVNULL)
    packets = [HERE / "results.json.xz", HERE / "postchecks.json.xz",
               HERE.parent / "native-exchange-control-2026-10-20/prerequisites.json.xz",
               HERE.parent / "native-exchange-prerequisites-2026-10-20/native-encoded-exchange-prerequisites.json.xz"]
    files = {}
    preserved = []

    def add(path, basis):
        require(path.resolve() == path and (path.is_relative_to(EVIDENCE) or path.is_relative_to(CACHE)),
                f"unexpected retirement boundary: {path}")
        entry = file_identity(path)
        files[str(path)] = {**entry, "regeneration": basis}

    for packet_path in packets:
        packet_id = file_identity(packet_path)
        members = json.loads(lzma.decompress(packet_path.read_bytes()))["members"]
        for name, entry in members.items():
            relative = Path(name)
            if relative.is_absolute() or ".." in relative.parts or "exchange" not in name:
                continue
            path = EVIDENCE / relative
            if not path.is_file():
                continue
            data = entry["text"].encode()
            require(identity(data) == {key: entry[key] for key in ("bytes", "sha256")}, name)
            if path.stat().st_size != len(data) or digest(path) != entry["sha256"]:
                preserved.append({"path": str(path), "reason": "current bytes differ from historical member"})
                continue
            add(path, {"kind": "exact_archived_result", "packet": packet_id, "member": name})

    fixture = EVIDENCE / "native-exchange-fixtures-839-a3"
    primary = json.loads(lzma.decompress((HERE / "results.json.xz").read_bytes()))["members"]
    build = json.loads(primary["exchange-validated-build-freeze-846-a1.json"]["text"])
    for row in build["fixture_payloads"]:
        path = Path(row["path"])
        require(path.is_relative_to(fixture) and all(file_identity(path)[key] == row[key]
                for key in ("bytes", "sha256")), "frozen fixture payload differs")
    require(digest(fixture / "fixture-manifest.json") == build["fixture_manifest"]["sha256"], "fixture manifest changed")
    for path in sorted(fixture.rglob("*")):
        if path.is_file():
            add(path, {"kind": "completed_generated_fixture", "source_commit": CANDIDATE,
                       "recipe": "shardloom-vortex/tests/fixtures/native-exchange/README.md",
                       "frozen_manifest_sha256": build["fixture_manifest"]["sha256"]})

    for profile in ("debug", "release"):
        base = CACHE / profile
        for directory in (base, base / "deps"):
            for path in sorted(directory.iterdir()):
                if path.is_file() and is_exchange(path.name):
                    add(path, {"kind": "completed_private_executable_or_compiler_output", "source_commit": CANDIDATE})
        for directory in sorted((base / ".fingerprint").iterdir()):
            if not directory.is_dir() or directory.is_symlink():
                continue
            members = list(directory.iterdir())
            private = is_exchange(directory.name) or any(is_exchange(path.name) for path in members)
            native_release_library = False
            metadata = directory / "lib-shardloom_vortex.json"
            if profile == "release" and metadata.is_file():
                native_release_library = "native-encoded-exchange-experiment" in json.loads(metadata.read_bytes())["features"]
            if not private and not native_release_library:
                continue
            if native_release_library:
                suffix = directory.name.removeprefix("shardloom-vortex-")
                for name in (f"libshardloom_vortex-{suffix}.rlib", f"libshardloom_vortex-{suffix}.rmeta",
                             f"shardloom_vortex-{suffix}.d"):
                    path = base / "deps" / name
                    if path.is_file():
                        add(path, {"kind": "obsolete_experimental_release_library", "source_commit": CANDIDATE})
            for path in members:
                add(path, {"kind": "completed_private_compiler_fingerprint", "source_commit": CANDIDATE})

    restored = json.loads((EVIDENCE / "multiway-restored-public-build-823.json").read_bytes())
    retain = [*packets, HERE / "decision.json", HERE / "postchecks.json",
              EVIDENCE / "run_guarded_observed_757.py", DRIVER,
              Path(restored["binary"]["path"]), Path(restored["native_library"]["path"])]
    for key in ("binary", "native_library"):
        require(all(file_identity(Path(restored[key]["path"]))[field] == restored[key][field]
                    for field in ("bytes", "sha256")), "accepted public runtime changed")
    for path in retain:
        require(str(path) not in files, "retirement would remove a retained input")
    plan = {"status": "prepared", "receipt": RECEIPT.name,
            "retire": [{"path": row["path"], "sha256": row["sha256"], "regeneration": row["regeneration"]}
                       for row in sorted(files.values(), key=lambda item: item["path"])],
            "retain": [str(path) for path in retain], "members": files,
            "packet_identities": [file_identity(path) for path in packets],
            "driver": file_identity(DRIVER), "wrapper": file_identity(Path(__file__)),
            "preserved_changed_members": preserved, "no_bulk_backup_created": True}
    with PLAN.open("x") as stream:
        json.dump(plan, stream, indent=2, sort_keys=True)
        stream.write("\n")
    print(json.dumps({"status": "prepared", "exact_files": len(files), "preserved_changed_members": preserved}))


def execute():
    plan = json.loads(PLAN.read_bytes())
    require(plan["wrapper"] == file_identity(Path(__file__)) and plan["driver"] == file_identity(DRIVER),
            "retirement implementation changed")
    for packet in plan["packet_identities"]:
        require(file_identity(Path(packet["path"])) == packet, "retained packet changed")
    for item in plan["members"].values():
        require(file_identity(Path(item["path"])) == {key: item[key] for key in ("path", "bytes", "sha256")},
                "selected generated file changed before execution")
    specification = importlib.util.spec_from_file_location("exchange_cleanup_driver", DRIVER)
    cleanup = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(cleanup)
    sys.argv = [str(DRIVER), str(PLAN)]
    cleanup.main()
    receipt = json.loads(RECEIPT.read_bytes())
    require(receipt["status"] == "passed" and receipt["retained_identities_unchanged"], "retirement failed")
    require(set(receipt["completed"]) == set(plan["members"]), "incomplete retirement")
    artifacts = [{key: row[key] for key in ("path", "bytes", "sha256")} |
                 {"reason": row["regeneration"]["kind"],
                  "retired": not Path(row["path"]).exists(), "open_handles": 0}
                 for row in plan["members"].values()]
    require(all(row["retired"] for row in artifacts), "selected generated file remains")
    # Remove only empty parents under the explicitly retired fixture/certificate/provider/fingerprint roots.
    parents = {Path(row["path"]).parent for row in artifacts}
    for parent in sorted(parents, key=lambda path: len(path.parts), reverse=True):
        if parent in (EVIDENCE, CACHE / "debug", CACHE / "release", CACHE / "debug/deps", CACHE / "release/deps"):
            continue
        if parent.is_dir() and not any(parent.iterdir()):
            parent.rmdir()
    detail = json.dumps({"plan": plan, "driver_receipt": receipt}, sort_keys=True, allow_nan=False).encode()
    compressed = lzma.compress(detail)
    require(lzma.decompress(compressed) == detail, "cleanup detail compression changed")
    detail_path = HERE / "cleanup-details.json.xz"
    with detail_path.open("xb") as stream:
        stream.write(compressed)
    result = {"status": "completed", "failures": [], "artifacts": artifacts,
              "plan": file_identity(PLAN), "driver_receipt": file_identity(RECEIPT),
              "details": {"file": detail_path.name, **identity(compressed)},
              "preserved_changed_members": plan["preserved_changed_members"],
              "no_bulk_backup_created": True, "public_runtime_preserved": True,
              "open_handle_proof": "Existing tested cleanup driver performs two complete lsof scans and refuses open selected files before unlinking; stable identities and native-work guard remain enforced."}
    with (HERE / "cleanup.json").open("x") as stream:
        json.dump(result, stream, indent=2, sort_keys=True)
        stream.write("\n")
    print(json.dumps({"status": "completed", "retired_files": len(artifacts), "public_runtime_preserved": True}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("phase", choices=("prepare", "execute"))
    args = parser.parse_args()
    prepare() if args.phase == "prepare" else execute()
