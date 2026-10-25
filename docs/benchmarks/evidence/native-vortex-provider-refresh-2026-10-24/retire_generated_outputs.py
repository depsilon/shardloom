#!/usr/bin/env python3
"""Prepare or verify the exact, source-reproducible provider cleanup set.

The default mode only verifies the saved plan.  --prepare writes a new exact
plan.  Destructive mode is deliberately explicit and unlinks listed regular
files/symlinks one at a time; it never removes directories or follows links.
"""

from __future__ import annotations

import argparse
import datetime as dt
import gzip
import hashlib
import json
import os
import stat
import subprocess
import sys
import tarfile
from pathlib import Path

sys.dont_write_bytecode = True

V = Path("/Users/dylan/LocalData/shardloom/vortex-provider-refresh-20261024")
W_EVIDENCE = Path(
    "/Users/dylan/.codex/worktrees/public-docs-api/shardloom-local-repo/"
    "docs/benchmarks/evidence/native-vortex-provider-refresh-2026-10-24"
)
PLAN_PATH = V / "cleanup-plan-r1.json.gz"
SCRIPT_PATH = V / "cleanup-provider-r1.py"
V_REPLAY = V / "provider-packet-replay-r3.json"
FRESH_INGEST = V / "fresh-ingest-acceptance-r2.json"
CLEAN_CHECKOUT = W_EVIDENCE / "clean-checkout.json"
MANIFEST = W_EVIDENCE / "manifest.json"
SELECTION_GZIP = W_EVIDENCE / "selection.json.gz"
REPLAY = W_EVIDENCE / "replay.json"

EXPECTED = {
    "packet_manifest_sha256": "bf99431f11d4f3cfee6df9d4584fba31b557a1d3bdc3b35f8f7d8685d3262982",
    "replay_sha256": "2c608d8c5ab5d443f3964a44936c3eadd006477584b79ea6167f2f924558012e",
    "clean_checkout_sha256": "ffb0e790945b02041617d3532b21fd7054f69aae2410cf85b4c3ff204aa5cd6b",
    "v_replay_sha256": "2c608d8c5ab5d443f3964a44936c3eadd006477584b79ea6167f2f924558012e",
}

class SafetyError(RuntimeError):
    pass


def fail(message: str) -> None:
    raise SafetyError(message)


def stamp(st: os.stat_result) -> dict:
    return {
        "device": st.st_dev,
        "inode": st.st_ino,
        "mode": st.st_mode,
        "nlink": st.st_nlink,
        "size": st.st_size,
        "mtime_ns": st.st_mtime_ns,
        "ctime_ns": st.st_ctime_ns,
        "birthtime_ns": int(getattr(st, "st_birthtime", 0) * 1_000_000_000),
        "flags": getattr(st, "st_flags", 0),
    }


def stat_at(path: Path) -> os.stat_result:
    try:
        return path.lstat()
    except OSError as exc:
        fail(f"cannot lstat {path}: {exc}")


def sha256_file(path: Path) -> tuple[int, str, dict]:
    before = stat_at(path)
    if not stat.S_ISREG(before.st_mode):
        fail(f"expected regular file: {path}")
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    try:
        fd = os.open(path, flags)
    except OSError as exc:
        fail(f"cannot open without following links {path}: {exc}")
    h = hashlib.sha256()
    try:
        opened = os.fstat(fd)
        if stamp(opened) != stamp(before):
            fail(f"file changed between lstat and open: {path}")
        with os.fdopen(fd, "rb", closefd=False) as stream:
            while True:
                block = stream.read(1024 * 1024)
                if not block:
                    break
                h.update(block)
        after_fd = os.fstat(fd)
    finally:
        os.close(fd)
    after_path = stat_at(path)
    if stamp(after_fd) != stamp(before) or stamp(after_path) != stamp(before):
        fail(f"file changed while hashing: {path}")
    return before.st_size, h.hexdigest(), stamp(before)


def capture(path: Path, scope_id: str, category: str, basis: str) -> dict:
    try:
        st = path.lstat()
    except OSError as exc:
        fail(f"cannot lstat {path}: {exc}")
    if stat.S_ISREG(st.st_mode):
        size, digest, stable = sha256_file(path)
        return {
            "path": str(path),
            "scope_id": scope_id,
            "category": category,
            "kind": "regular_file",
            "bytes": size,
            "sha256": digest,
            "sha256_basis": "file_bytes",
            "stable_stat": stable,
            "regeneration_basis": basis,
        }
    if stat.S_ISLNK(st.st_mode):
        try:
            target = os.readlink(path)
            raw_target = os.fsencode(target)
            after = path.lstat()
        except OSError as exc:
            fail(f"cannot read symlink without following it {path}: {exc}")
        if stamp(after) != stamp(st):
            fail(f"symlink changed while reading target: {path}")
        return {
            "path": str(path),
            "scope_id": scope_id,
            "category": category,
            "kind": "symlink",
            "bytes": st.st_size,
            "sha256": hashlib.sha256(raw_target).hexdigest(),
            "sha256_basis": "os.fsencode(readlink_target)",
            "symlink_target": target,
            "stable_stat": stamp(st),
            "regeneration_basis": basis,
        }
    if stat.S_ISDIR(st.st_mode):
        fail(f"directory encountered where file was expected: {path}")
    fail(f"unsupported filesystem object: {path} mode={st.st_mode}")


def walk_tree(root: Path) -> list[Path]:
    root_st = stat_at(root)
    if not stat.S_ISDIR(root_st.st_mode) or stat.S_ISLNK(root_st.st_mode):
        fail(f"scope root must be a real directory: {root}")
    found: list[Path] = []
    stack = [root]
    while stack:
        parent = stack.pop()
        try:
            entries = sorted(os.scandir(parent), key=lambda item: item.name)
        except OSError as exc:
            fail(f"cannot scan {parent}: {exc}")
        for item in entries:
            child = Path(item.path)
            try:
                st = item.stat(follow_symlinks=False)
            except OSError as exc:
                fail(f"cannot stat directory entry {child}: {exc}")
            if stat.S_ISDIR(st.st_mode):
                stack.append(child)
            elif stat.S_ISLNK(st.st_mode):
                try:
                    if item.is_dir(follow_symlinks=True):
                        fail(f"symlink directory refused: {child}")
                except OSError as exc:
                    fail(f"cannot classify symlink {child}: {exc}")
                found.append(child)
            elif stat.S_ISREG(st.st_mode):
                found.append(child)
            else:
                fail(f"unsupported filesystem object: {child} mode={st.st_mode}")
    return sorted(found)


def check_under_v(path: Path) -> str:
    absolute = Path(os.path.abspath(path))
    try:
        relative = absolute.relative_to(V)
    except ValueError:
        fail(f"path escapes approved V root: {absolute}")
    if not relative.parts:
        fail("V root itself cannot be a deletion target")
    current = Path(V.anchor)
    for component in V.parts[1:]:
        current = current / component
        vstat = stat_at(current)
        if stat.S_ISLNK(vstat.st_mode):
            fail(f"approved root path contains a symlink component: {current}")
        if current == V and not stat.S_ISDIR(vstat.st_mode):
            fail(f"approved root is not a directory: {V}")
    for component in relative.parts[:-1]:
        current = current / component
        st = stat_at(current)
        if stat.S_ISLNK(st.st_mode) or not stat.S_ISDIR(st.st_mode):
            fail(f"symlink/non-directory path component refused: {current}")
    return relative.as_posix()


def load_json(path: Path) -> dict:
    try:
        value = json.loads(path.read_text())
    except Exception as exc:
        fail(f"cannot parse JSON {path}: {exc}")
    if not isinstance(value, dict):
        fail(f"expected JSON object: {path}")
    return value


def load_selection() -> tuple[dict, bytes]:
    try:
        with gzip.open(SELECTION_GZIP, "rb") as stream:
            raw = stream.read()
    except Exception as exc:
        fail(f"cannot read compressed packet selection manifest: {exc}")
    try:
        selection = json.loads(raw)
    except Exception as exc:
        fail(f"cannot parse packet selection manifest: {exc}")
    return selection, raw


def check_expected_file(path: Path, expected_sha: str) -> dict:
    size, digest, stable = sha256_file(path)
    if digest != expected_sha:
        fail(f"retained evidence identity changed: {path} expected {expected_sha} observed {digest}")
    return {"path": str(path), "bytes": size, "sha256": digest, "stable_stat": stable}


def open_handle_check(path: Path, is_directory: bool) -> dict:
    command = ["lsof", "-nP"]
    command += (["+D", str(path)] if is_directory else [str(path)])
    try:
        result = subprocess.run(command, capture_output=True, text=True, check=False)
    except OSError as exc:
        fail(f"cannot run lsof for {path}: {exc}")
    stdout = result.stdout.strip()
    stderr = result.stderr.strip()
    rows = max(0, len(stdout.splitlines()) - 1) if stdout else 0
    if result.returncode != 1 or rows or stderr:
        fail(f"open-handle check did not prove closed path {path}: rc={result.returncode}, rows={rows}, stderr={stderr[:400]}")
    return {
        "path": str(path),
        "method": "lsof -nP +D" if is_directory else "lsof -nP exact-file",
        "returncode": result.returncode,
        "matching_rows": rows,
        "status": "no_matching_open_handle_reported",
        "observed_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
    }


def identity_map(records: list[dict], label: str) -> dict[str, dict]:
    result: dict[str, dict] = {}
    for record in records:
        path = record.get("path")
        if not isinstance(path, str):
            fail(f"{label} record has no literal path")
        if path in result:
            prior = result[path]
            identity_keys = ("bytes", "sha256", "member")
            if any(prior.get(k) != record.get(k) for k in identity_keys):
                fail(f"conflicting {label} identities for {path}")
            continue
        result[path] = record
    return result


def make_plan() -> dict:
    check_under_v(V / "anchor")
    manifest_id = check_expected_file(MANIFEST, EXPECTED["packet_manifest_sha256"])
    selection, selection_raw = load_selection()
    manifest = load_json(MANIFEST)
    replay_id = check_expected_file(REPLAY, EXPECTED["replay_sha256"])
    v_replay_id = check_expected_file(V_REPLAY, EXPECTED["v_replay_sha256"])
    clean_id = check_expected_file(CLEAN_CHECKOUT, EXPECTED["clean_checkout_sha256"])
    if manifest.get("status") != "complete":
        fail("portable packet manifest is not complete")
    if load_json(REPLAY).get("status") != "passed" or load_json(V_REPLAY).get("status") != "passed":
        fail("portable packet replay evidence is not passed")
    clean = load_json(CLEAN_CHECKOUT)
    if clean.get("status") != "passed" or clean.get("source_commit") != "f7b1e566132c8a24c9cbd9f4ff8e6223ff7422e4":
        fail("clean-checkout proof no longer matches the approved source commit")
    if manifest.get("source_commit") != load_json(REPLAY).get("source_commit"):
        fail("packet manifest and replay source commits differ")
    if hashlib.sha256(selection_raw).hexdigest() != manifest.get("selection_manifest", {}).get("raw_sha256"):
        fail("decompressed selection-manifest bytes do not match packet manifest")
    compressed_size, compressed_sha, compressed_stat = sha256_file(SELECTION_GZIP)
    selection_rec = manifest["selection_manifest"]
    if compressed_size != selection_rec.get("bytes") or compressed_sha != selection_rec.get("sha256"):
        fail("compressed selection manifest identity differs from packet manifest")
    if len(selection_raw) != selection_rec.get("raw_bytes"):
        fail("decompressed selection manifest byte count differs")
    selection_id = {
        "path": str(SELECTION_GZIP),
        "bytes": compressed_size,
        "sha256": compressed_sha,
        "raw_bytes": len(selection_raw),
        "raw_sha256": hashlib.sha256(selection_raw).hexdigest(),
        "stable_stat": compressed_stat,
    }
    if selection.get("status") != "COMPLETE" or len(selection.get("archives", [])) != 4:
        fail("packet selection is incomplete or has an unexpected archive count")

    manifest_files = identity_map(manifest.get("files", []), "packet manifest file")
    selection_files = identity_map(selection.get("files", []), "packet selection file")
    omitted_files = identity_map(selection.get("intentionally_omitted_payloads", []), "packet omission")
    # Keep both packet inventories in agreement for every shared exact path.
    for path, record in selection_files.items():
        outer = manifest_files.get(path)
        if outer and any(outer.get(k) != record.get(k) for k in ("bytes", "sha256", "member")):
            fail(f"selection and packet manifest disagree for {path}")

    entries: list[dict] = []
    retained: list[dict] = []
    scopes: list[dict] = []
    walk_roots: dict[str, Path] = {}
    exact_lsof_files: list[Path] = []

    def append_path(path: Path, scope_id: str, category: str, basis: str, target: list[dict]) -> dict:
        relative = check_under_v(path)
        entry = capture(path, scope_id, category, basis)
        entry["relative_to_v"] = relative
        target.append(entry)
        return entry

    def add_exact(scope_id: str, directory: Path, names: list[str], category: str, basis: str) -> None:
        walk_roots[str(directory)] = directory
        before = len(entries)
        for name in names:
            p = directory / name
            if not p.exists() and not p.is_symlink():
                fail(f"approved exact file missing: {p}")
            append_path(p, scope_id, category, basis, entries)
            exact_lsof_files.append(p)
        scopes.append({"scope_id": scope_id, "root": str(directory), "targets": len(entries) - before})

    def add_tree(scope_id: str, root: Path, category: str, basis: str, keep_unarchived_public_logs: bool = False) -> None:
        walk_roots[str(root)] = root
        before = len(entries)
        kept_before = len(retained)
        for path in walk_tree(root):
            relative = check_under_v(path)
            if keep_unarchived_public_logs:
                if str(path) not in manifest_files:
                    suffix = path.suffix.lower()
                    reason = {
                        ".py": "source helper stored beside generated logs; preserve",
                        ".json": "reference fixture stored beside generated logs; preserve",
                        ".txt": "unarchived diagnostic text; preserve useful failure evidence",
                    }.get(suffix, "unarchived log-tree item; preserve pending review")
                    if suffix not in {".py", ".json", ".txt"}:
                        fail(f"unclassified unarchived public log item: {path}")
                    append_path(path, scope_id, "retained_unarchived_log_tree_item", reason, retained)
                    continue
                rec = manifest_files[str(path)]
                if rec.get("member") != "objects/" + rec.get("sha256", ""):
                    fail(f"invalid packet object reference for public log: {path}")
            append_path(path, scope_id, category, basis, entries)
        scopes.append({
            "scope_id": scope_id,
            "root": str(root),
            "targets": len(entries) - before,
            "retained_items": len(retained) - kept_before,
        })

    add_exact(
        "artifacts-r1-binaries",
        V / "artifacts-r1",
        ["shardloom", "_native.abi3.so"],
        "superseded_build_binary",
        "Rebuild from the retained ShardLoom source; artifacts-r2 is the current retained runtime.",
    )
    add_exact(
        "capsule-build-r1-binaries",
        V / "capsule-build-r1",
        ["shardloom", "_native.abi3.so"],
        "superseded_build_binary",
        "Rebuild from the retained ShardLoom source; preserve capsule-build-r1 summaries.",
    )
    for rev in ("r1", "r2"):
        add_tree(
            f"public-{rev}-logs",
            V / f"public-{rev}" / "logs",
            "packet_verified_public_log",
            "The packet contains the exact object bytes for this path; see packet_coverage.",
            keep_unarchived_public_logs=True,
        )
        add_tree(
            f"public-{rev}-outputs",
            V / f"public-{rev}" / "outputs",
            "generated_public_output",
            "Regenerate with the retained public case definitions, source inputs and harness; packet replay records the comparison outcomes. Direct packet coverage is recorded per entry.",
        )

    for matrix in ("provider-native-matrix-1", "provider-native-matrix-2"):
        matrix_root = V / "modular-r1" / matrix
        add_tree(
            matrix + "-native-public",
            matrix_root / "native-public",
            "generated_native_public_output",
            "Regenerate from the retained modular runner, per-run report.json and case inputs; direct output readbacks are matched to packet objects where present.",
        )
        runs_root = matrix_root / "runs"
        walk_roots[str(runs_root)] = runs_root
        if not runs_root.is_dir() or runs_root.is_symlink():
            fail(f"runs root is missing or a symlink: {runs_root}")
        run_dirs = sorted((p for p in runs_root.iterdir()), key=lambda p: p.name)
        if len(run_dirs) != (3 if matrix.endswith("1") else 19):
            fail(f"unexpected run-directory count under {runs_root}: {len(run_dirs)}")
        for run_dir in run_dirs:
            st = stat_at(run_dir)
            if stat.S_ISLNK(st.st_mode) or not stat.S_ISDIR(st.st_mode):
                fail(f"unexpected run root entry: {run_dir}")
            run_items = walk_tree(run_dir)
            if not run_items:
                fail(f"run directory has no report or generated content: {run_dir}")
            report = run_dir / "report.json"
            if report not in run_items:
                fail(f"per-run report is missing or not a regular file: {report}")
            for path in run_items:
                top_component = path.relative_to(run_dir).parts[0]
                if top_component in {"input", "baseline-output"}:
                    continue
                append_path(
                    path,
                    matrix + "-preserved-run-artifacts",
                    "preserved_run_report" if path == report else "preserved_other_run_artifact",
                    "Preserve every per-run report and all run siblings outside input/baseline-output.",
                    retained,
                )
            for name, category in (("input", "generated_modular_input"), ("baseline-output", "generated_modular_baseline_output")):
                subroot = run_dir / name
                if not subroot.is_dir() or subroot.is_symlink():
                    fail(f"expected real run subdirectory missing: {subroot}")
                before = len(entries)
                for path in walk_tree(subroot):
                    append_path(
                        path,
                        matrix + "-" + name,
                        category,
                        "Regenerate from the retained ShardLoom source, run case and per-run report.json; reports remain untouched.",
                        entries,
                    )
                scopes.append({"scope_id": matrix + "-" + name, "root": str(subroot), "targets": len(entries) - before})

    add_tree(
        "wheel-proof-r1-package",
        V / "wheel-proof-r1" / "package",
        "superseded_wheel_source_package",
        "Re-stage from the retained project source; wheel-proof-r2 passed its installed integration suite.",
    )
    add_tree(
        "wheel-proof-r1-venv",
        V / "wheel-proof-r1" / "venv",
        "superseded_wheel_environment",
        "Recreate with the recorded Python/venv and install commands; preserve r1 reports and logs.",
    )
    wheel_r1_dist = V / "wheel-proof-r1" / "dist"
    walk_roots[str(wheel_r1_dist)] = wheel_r1_dist
    dist_items = walk_tree(wheel_r1_dist)
    if not dist_items or any(p.suffix != ".whl" or p.parent != wheel_r1_dist for p in dist_items):
        fail(f"wheel-proof-r1 dist contains missing or unrecognized files: {wheel_r1_dist}")
    for p in dist_items:
        append_path(p, "wheel-proof-r1-wheel", "superseded_wheel", "Superseded by the retained and tested r2 wheel.", entries)
    scopes.append({"scope_id": "wheel-proof-r1-wheel", "root": str(wheel_r1_dist), "targets": len(dist_items)})
    add_tree(
        "wheel-proof-r2-package",
        V / "wheel-proof-r2" / "package",
        "wheel_source_package",
        "Re-stage from the retained project source; the installed-wheel reports and tested r2 wheel remain.",
    )
    add_tree(
        "wheel-proof-r2-venv",
        V / "wheel-proof-r2" / "venv",
        "installed_wheel_environment",
        "Recreate using wheel-proof-r2/report.json; integration.json records 140 tests, zero skipped, passed.",
    )
    add_tree(
        "packet-stage-r1-duplicate",
        V / "packet-stage-r1",
        "verified_packet_stage_duplicate",
        "Exact duplicate of the retained W packet shards and selection manifest; identity comparison is required below.",
    )
    add_tree(
        "clean-replay-source-r1-clone",
        V / "clean-replay-source-r1",
        "temporary_shared_git_clone",
        "Recreate from the recorded git clone --shared command and detach at clean-checkout.json source_commit; proof and logs remain.",
    )

    # Compute and attach exact packet coverage. Public command logs must be archived byte-for-byte.
    wanted_objects: dict[str, dict] = {}
    for entry in entries:
        path = entry["path"]
        record = manifest_files.get(path)
        omitted = omitted_files.get(path)
        if record:
            if (entry["bytes"], entry["sha256"]) != (record.get("bytes"), record.get("sha256")):
                fail(f"current file differs from packet manifest: {path}")
            if record.get("member") != "objects/" + record.get("sha256", ""):
                fail(f"invalid packet object member for {path}")
            item = wanted_objects.setdefault(record["member"], {"bytes": record["bytes"], "sha256": record["sha256"], "paths": []})
            if item["bytes"] != record["bytes"] or item["sha256"] != record["sha256"]:
                fail(f"conflicting packet object identities for {path}")
            item["paths"].append(path)
            entry["packet_coverage"] = {"status": "selected", "member": record["member"], "manifest_path": path}
        elif omitted:
            if (entry["bytes"], entry["sha256"]) != (omitted.get("bytes"), omitted.get("sha256")):
                fail(f"current file differs from packet omission identity: {path}")
            entry["packet_coverage"] = {
                "status": "intentionally_omitted",
                "kind": omitted.get("kind"),
                "reason": omitted.get("reason"),
                "source_inventory": omitted.get("source_inventory"),
            }
        else:
            if entry["scope_id"].endswith("-logs"):
                fail(f"unarchived public log entered deletion set: {path}")
            entry["packet_coverage"] = {
                "status": "not_separately_packaged",
                "regeneration_basis": entry["regeneration_basis"],
            }

    # Verify every current public log is either an exact packet object or explicitly retained.
    for entry in retained:
        if entry["scope_id"].endswith("-logs"):
            entry["packet_coverage"] = {"status": "not_archived_preserved", "reason": entry["regeneration_basis"]}

    # Stream only packet objects corresponding to selected cleanup entries, without extraction.
    found_objects: dict[str, str] = {}
    archive_identities: list[dict] = []
    archives = manifest.get("archives", [])
    if len(archives) != 4:
        fail("packet manifest archive count changed")
    for archive_record in archives:
        rel = archive_record.get("path")
        if not isinstance(rel, str) or Path(rel).name != rel:
            fail(f"invalid packet archive path: {rel}")
        archive_path = W_EVIDENCE / rel
        archive_size, archive_sha, archive_stat = sha256_file(archive_path)
        if (archive_size, archive_sha) != (archive_record.get("bytes"), archive_record.get("sha256")):
            fail(f"portable packet archive identity mismatch: {archive_path}")
        archive_identities.append({"path": str(archive_path), "bytes": archive_size, "sha256": archive_sha, "stable_stat": archive_stat})
        try:
            with tarfile.open(archive_path, mode="r:xz") as archive:
                for member in archive:
                    expected = wanted_objects.get(member.name)
                    if expected is None:
                        continue
                    if member.name in found_objects or not member.isfile() or member.size != expected["bytes"]:
                        fail(f"duplicate, non-file, or wrong-sized packet member: {archive_path}:{member.name}")
                    source = archive.extractfile(member)
                    if source is None:
                        fail(f"cannot read packet member: {archive_path}:{member.name}")
                    h = hashlib.sha256()
                    total = 0
                    while True:
                        block = source.read(1024 * 1024)
                        if not block:
                            break
                        total += len(block)
                        h.update(block)
                    if total != expected["bytes"] or h.hexdigest() != expected["sha256"]:
                        fail(f"packet object payload hash/size mismatch: {archive_path}:{member.name}")
                    found_objects[member.name] = str(archive_path)
        except (OSError, tarfile.TarError) as exc:
            fail(f"cannot stream packet archive {archive_path}: {exc}")
    missing_objects = sorted(set(wanted_objects) - set(found_objects))
    if missing_objects:
        fail(f"packet object members absent from archives: {missing_objects[:5]} ({len(missing_objects)} total)")
    for entry in entries:
        coverage = entry["packet_coverage"]
        if coverage["status"] == "selected":
            coverage["archive_path"] = found_objects[coverage["member"]]
            coverage["archive_member_verified"] = True

    # Staged manifest and archive copies must be byte-identical to the retained packet.
    packet_stage = V / "packet-stage-r1"
    stage_items = walk_tree(packet_stage)
    if len(stage_items) != 5 or any(p.parent != packet_stage for p in stage_items):
        fail("packet-stage must contain exactly five root-level regular files/symlinks")
    stage_by_name = {p.name: p for p in stage_items}
    expected_names = {rec["path"] for rec in archives} | {"packet-manifest.json"}
    if set(stage_by_name) != expected_names:
        fail(f"packet-stage file list differs from expected exact five files: {sorted(set(stage_by_name) ^ expected_names)}")
    stage_comparison = []
    for record in archives:
        staged = stage_by_name[record["path"]]
        size, digest, _ = sha256_file(staged)
        if (size, digest) != (record["bytes"], record["sha256"]):
            fail(f"packet-stage shard differs from retained repo copy: {staged}")
        stage_comparison.append({"path": str(staged), "bytes": size, "sha256": digest, "matches_retained_archive": True})
    staged_manifest = stage_by_name["packet-manifest.json"]
    stage_size, stage_sha, stage_stat = sha256_file(staged_manifest)
    if stage_size != len(selection_raw) or stage_sha != hashlib.sha256(selection_raw).hexdigest():
        fail("packet-stage manifest is not byte-identical to decompressed selection.json.gz")
    stage_comparison.append({
        "path": str(staged_manifest),
        "bytes": stage_size,
        "sha256": stage_sha,
        "matches_decompressed_selection_manifest": True,
        "stable_stat": stage_stat,
    })

    # Check the recorded retained inputs/runtime/wheel. Do not rehash the 15.7 GB Vortex input;
    # verify its receipt-recorded stable generation and retain its accepted full-content digest.
    fresh = load_json(FRESH_INGEST)
    if fresh.get("status") != "passed":
        fail("fresh-input-r2 acceptance receipt is not passed")
    target = fresh.get("target", {})
    input_path = Path(target.get("path", ""))
    if input_path != V / "fresh-input-r2/vortex/hits.vortex":
        fail("fresh Vortex retained-input path differs from acceptance receipt")
    input_stat = stat_at(input_path)
    generation = target.get("generation")
    current_generation = [input_stat.st_dev, input_stat.st_ino, input_stat.st_size, input_stat.st_mtime_ns, input_stat.st_ctime_ns]
    if current_generation != generation or target.get("bytes") != input_stat.st_size:
        fail("fresh Vortex input generation differs from accepted receipt")

    retained_invariants: list[dict] = []
    # Current r2 runtime and latest tested wheel are also protected by packet identities when present.
    for path in [V / "artifacts-r2/shardloom", V / "artifacts-r2/_native.abi3.so", V / "wheel-proof-r2/dist/shardloom-0.7.0-cp310-abi3-macosx_27_0_arm64.whl"]:
        rec = manifest_files.get(str(path))
        if rec:
            obs_size, obs_sha, obs_stat = sha256_file(path)
            if (obs_size, obs_sha) != (rec.get("bytes"), rec.get("sha256")):
                fail(f"retained runtime/wheel identity differs from packet manifest: {path}")
            retained_invariants.append({"path": str(path), "bytes": obs_size, "sha256": obs_sha, "stable_stat": obs_stat, "packet_member": rec.get("member")})
        else:
            obs_size, obs_sha, obs_stat = sha256_file(path)
            retained_invariants.append({"path": str(path), "bytes": obs_size, "sha256": obs_sha, "stable_stat": obs_stat})
    retained_invariants.append({
        "path": str(input_path),
        "bytes": input_stat.st_size,
        "sha256": target.get("sha256"),
        "sha256_basis": "accepted fresh-ingest receipt; receipt reports actual_file_sha256_matches_native_stream_digest=true",
        "stable_stat": stamp(input_stat),
        "receipt": str(FRESH_INGEST),
        "receipt_target_generation": generation,
    })
    original = fresh.get("original_input", {})
    retained_invariants.append({
        "path": original.get("path"),
        "bytes": original.get("bytes"),
        "sha256": original.get("sha256"),
        "sha256_basis": "accepted fresh-ingest receipt; source not re-read",
        "receipt": str(FRESH_INGEST),
        "protected": True,
    })

    # Verify the run reports and preserved summaries are represented by stable exact identities.
    for root, names in [
        (V / "artifacts-r1", ["build-summary.json", "acceptance-build.json"]),
        (V / "capsule-build-r1", ["build-summary.json", "acceptance-build.json"]),
    ]:
        for name in names:
            p = root / name
            size, digest, stable = sha256_file(p)
            retained.append({
                "path": str(p),
                "scope_id": "preserved-build-summary",
                "category": "retained_build_evidence",
                "kind": "regular_file",
                "bytes": size,
                "sha256": digest,
                "sha256_basis": "file_bytes",
                "stable_stat": stable,
                "regeneration_basis": "Preserved by approved scope; not eligible for unlink.",
            })

    # Re-walk the bounded roots and re-stat every identity. This catches newly added entries or
    # races during the multi-gigabyte hashing pass before the exact plan is written.
    all_recorded = {e["path"] for e in entries} | {e["path"] for e in retained}
    for root in walk_roots.values():
        present = {str(p) for p in walk_tree(root)}
        expected_here = {p for p in all_recorded if Path(p).is_relative_to(root)}
        if present != expected_here:
            fail(f"scope contents changed during inventory under {root}")
    for entry in entries + retained:
        current_stat = stamp(stat_at(Path(entry["path"])))
        if current_stat != entry["stable_stat"]:
            fail(f"stable stat changed during inventory: {entry['path']}")

    # Every directory root is checked for open handles now and again on every later dry-run/apply.
    handle_checks = [open_handle_check(Path(p), True) for p in sorted(walk_roots)]
    for p in sorted(set(exact_lsof_files)):
        handle_checks.append(open_handle_check(p, False))

    # Ensure the selected file universe did not change while inventorying.
    current_targets = {entry["path"] for entry in entries}
    current_retained = {entry["path"] for entry in retained}
    if len(current_targets) != len(entries) or len(current_retained) != len(retained):
        fail("duplicate path appeared in the cleanup or retained identity list")

    counts: dict[str, dict] = {}
    for entry in entries:
        value = counts.setdefault(entry["scope_id"], {"files": 0, "regular_files": 0, "symlinks": 0, "bytes": 0, "selected_packet_objects": 0, "omitted_packet_payloads": 0, "regenerable_not_packaged": 0})
        value["files"] += 1
        value["bytes"] += entry["bytes"]
        if entry["kind"] == "regular_file":
            value["regular_files"] += 1
        else:
            value["symlinks"] += 1
        status = entry["packet_coverage"]["status"]
        if status == "selected":
            value["selected_packet_objects"] += 1
        elif status == "intentionally_omitted":
            value["omitted_packet_payloads"] += 1
        else:
            value["regenerable_not_packaged"] += 1

    script_size, script_sha, script_stat = sha256_file(SCRIPT_PATH)
    all_sha = hashlib.sha256()
    for entry in entries:
        all_sha.update(entry["path"].encode())
        all_sha.update(b"\0")
        all_sha.update(entry["sha256"].encode())
        all_sha.update(b"\n")

    return {
        "schema": "shardloom.cleanup-plan.v1",
        "plan_id": "cleanup-provider-r1",
        "created_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "root": str(V),
        "mode": "dry_run_only",
        "deletion_executed": False,
        "deletion_policy": "exact file unlink only; never remove directories; never follow symlinks",
        "script": {"path": str(SCRIPT_PATH), "bytes": script_size, "sha256": script_sha, "stable_stat": script_stat},
        "primary_source_commit_for_refresh": "43dc486fcdaf4fc7bc84b329ad4a04e3273bf573",
        "portable_packet": {
            "manifest": manifest_id,
            "replay": replay_id,
            "v_replay": v_replay_id,
            "clean_checkout": clean_id,
            "selection_manifest": selection_id,
            "archives": archive_identities,
            "packet_stage_comparisons": stage_comparison,
            "archive_member_verification": {
                "unique_object_members_matched": len(found_objects),
                "candidate_paths_matched": sum(1 for e in entries if e["packet_coverage"]["status"] == "selected"),
                "text_and_log_bytes_verified_without_extraction": True,
            },
        },
        "retained_invariants": retained_invariants,
        "scopes": scopes,
        "scope_totals": counts,
        "entries": entries,
        "retained_files": retained,
        "retained_unarchived_public_logs": [e["path"] for e in retained if e.get("category") == "retained_unarchived_log_tree_item"],
        "unresolved": [],
        "open_handle_checks": handle_checks,
        "identity_summary": {
            "candidate_file_count": len(entries),
            "candidate_logical_bytes": sum(e["bytes"] for e in entries),
            "candidate_path_sha256": all_sha.hexdigest(),
            "retained_file_count": len(retained),
            "retained_logical_bytes": sum(e["bytes"] for e in retained),
        },
        "verification_limits": [
            "Current fresh-input-r2 Vortex bytes are not re-read; its accepted receipt's full-content SHA-256 and stable generation are preserved and the current generation is checked.",
            "The original Parquet source is retained by its accepted receipt identity and is not opened or modified.",
            "Unarchived public-log helpers, reference fixtures and stderr diagnostics are inventoried as retained files, not cleanup targets.",
            "Open-handle results are point-in-time observations and must be refreshed immediately before any apply."
        ],
    }


def write_plan(plan: dict) -> None:
    raw = (json.dumps(plan, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n").encode()
    check_under_v(PLAN_PATH)
    if PLAN_PATH.is_symlink():
        fail(f"refusing to replace symlink plan path: {PLAN_PATH}")
    try:
        flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC | getattr(os, "O_NOFOLLOW", 0)
        fd = os.open(PLAN_PATH, flags, 0o600)
        with os.fdopen(fd, "wb") as raw_file:
            with gzip.GzipFile(filename="", mode="wb", fileobj=raw_file, mtime=0, compresslevel=6) as out:
                out.write(raw)
            raw_file.flush()
            os.fsync(raw_file.fileno())
    except OSError as exc:
        fail(f"cannot write the authorized plan path {PLAN_PATH}: {exc}")


def load_plan() -> dict:
    try:
        with gzip.open(PLAN_PATH, "rt", encoding="utf-8") as stream:
            plan = json.load(stream)
    except Exception as exc:
        fail(f"cannot load cleanup plan {PLAN_PATH}: {exc}")
    if plan.get("schema") != "shardloom.cleanup-plan.v1" or plan.get("root") != str(V):
        fail("cleanup plan schema/root mismatch")
    if plan.get("unresolved"):
        fail("cleanup plan contains unresolved entries")
    return plan


def verify_saved_plan(plan: dict) -> dict:
    current = make_plan()
    for key in ("script", "portable_packet", "retained_invariants", "scopes", "scope_totals", "entries", "retained_files", "retained_unarchived_public_logs", "identity_summary"):
        if current.get(key) != plan.get(key):
            fail(f"current inventory differs from saved plan in field {key}; regenerate only after review")
    return current


def open_parent_dir(rel: str) -> tuple[int, str]:
    parts = Path(rel).parts
    if not parts:
        fail("empty relative path refused")
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        fd = os.open(V, flags)
        for component in parts[:-1]:
            next_fd = os.open(component, flags, dir_fd=fd)
            os.close(fd)
            fd = next_fd
        return fd, parts[-1]
    except OSError as exc:
        fail(f"cannot safely open parent directory for {rel}: {exc}")


def unlink_exact(entry: dict) -> None:
    rel = check_under_v(Path(entry["path"]))
    parent_fd, name = open_parent_dir(rel)
    try:
        st = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
        if stamp(st) != entry["stable_stat"]:
            fail(f"stable stat changed immediately before unlink: {entry['path']}")
        if stat.S_ISREG(st.st_mode):
            flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
            fd = os.open(name, flags, dir_fd=parent_fd)
            try:
                opened = os.fstat(fd)
                if stamp(opened) != entry["stable_stat"]:
                    fail(f"file changed before unlink: {entry['path']}")
                h = hashlib.sha256()
                with os.fdopen(fd, "rb", closefd=False) as source:
                    for block in iter(lambda: source.read(1024 * 1024), b""):
                        h.update(block)
                if h.hexdigest() != entry["sha256"]:
                    fail(f"SHA-256 changed immediately before unlink: {entry['path']}")
            finally:
                os.close(fd)
        elif stat.S_ISLNK(st.st_mode):
            target = os.readlink(name, dir_fd=parent_fd)
            if hashlib.sha256(os.fsencode(target)).hexdigest() != entry["sha256"]:
                fail(f"symlink target changed immediately before unlink: {entry['path']}")
        else:
            fail(f"refusing to unlink non-file: {entry['path']}")
        if stamp(os.stat(name, dir_fd=parent_fd, follow_symlinks=False)) != entry["stable_stat"]:
            fail(f"path changed immediately before unlink: {entry['path']}")
        os.unlink(name, dir_fd=parent_fd)
    finally:
        os.close(parent_fd)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument("--prepare", action="store_true", help="inventory approved paths and write the exact gzip plan; never deletes")
    modes.add_argument("--delete", action="store_true", help="unlink only exact planned files after full preflight")
    parser.add_argument("--confirm-token", help="required with --delete: DELETE_CLEANUP_PROVIDER_R1")
    args = parser.parse_args()
    try:
        if args.prepare:
            plan = make_plan()
            write_plan(plan)
            print(json.dumps({"status": "prepared_dry_run", "plan": str(PLAN_PATH), **plan["identity_summary"]}, sort_keys=True))
            return 0
        if args.delete and args.confirm_token != "DELETE_CLEANUP_PROVIDER_R1":
            fail("--delete requires --confirm-token DELETE_CLEANUP_PROVIDER_R1")
        saved = load_plan()
        current = verify_saved_plan(saved)
        if not args.delete:
            print(json.dumps({"status": "dry_run_verified", "plan": str(PLAN_PATH), **current["identity_summary"]}, sort_keys=True))
            return 0
        if current["unresolved"]:
            fail("cleanup plan has unresolved files")
        # make_plan has just rechecked stable hashes, archive readback, path boundaries and lsof.
        for entry in current["entries"]:
            unlink_exact(entry)
        print(json.dumps({"status": "unlinked_exact_files", "count": len(current["entries"]), "logical_bytes": current["identity_summary"]["candidate_logical_bytes"]}, sort_keys=True))
        return 0
    except SafetyError as exc:
        print(f"REFUSED: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
