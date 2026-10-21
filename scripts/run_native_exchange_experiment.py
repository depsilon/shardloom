#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Supervise the frozen local exchange experiment; record all failed observations."""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import selectors
import signal
import stat
import subprocess
import sys
import time

from local_uat_storage import GIB, MIB, check_budgets, require_local_path
from native_exchange_result_checks import verify_workflow


ROOT = Path(__file__).resolve().parents[1]
FREEZE = ROOT / "docs/benchmarks/evidence/native-exchange-control-2026-10-20/protocol-freeze.json"
FREEZE_SHA = "bf9f6a0e5840e03bf4536d8816b6fe4138dfb9807439633135ec111c6e28e5dc"
MODES = ["native_fragments", "native_assembled", "arrow_none", "arrow_lz4", "arrow_zstd"]
PAIR_RSS_LIMIT = 2 * GIB
WORKFLOW_LIMIT = 120
PIPE_LIMIT = MIB


def require(condition: bool, detail: str) -> None:
    if not condition:
        raise ValueError(detail)


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def identity(path: Path) -> dict:
    before = path.stat()
    with path.open("rb") as source:
        sha = hashlib.file_digest(source, "sha256").hexdigest()
    after = path.stat()
    require((before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) ==
            (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns), "input changed while hashing")
    return {"path": str(path), "bytes": after.st_size, "sha256": sha,
            "device": after.st_dev, "inode": after.st_ino, "mtime_ns": after.st_mtime_ns}


def verify_build_freeze(build: dict, executable: dict, manifest_sha256: str) -> dict:
    require(build.get("status") == "validated_before_scored_cohorts", "build freeze lacks correctness acceptance")
    require(build["executable"]["sha256"] == executable["sha256"], "executable differs from pre-timing build freeze")
    require(build["protocol_freeze"]["sha256"] == FREEZE_SHA, "build freeze names a different protocol")
    require(build["fixture_manifest"]["sha256"] == manifest_sha256, "build freeze names different fixtures")
    sources = build["sources"]
    require(isinstance(sources, list) and sources, "build freeze has no source identities")
    seen = set()
    for source in sources:
        relative = Path(source["file"])
        require(not relative.is_absolute() and ".." not in relative.parts and str(relative) not in seen,
                "invalid or repeated build source path")
        seen.add(str(relative))
        actual = identity(ROOT / relative)
        require(actual["bytes"] == source["bytes"] and actual["sha256"] == source["sha256"],
                f"frozen build source changed: {relative}")
    required = {"Cargo.toml", "Cargo.lock", "shardloom-vortex/Cargo.toml",
                "shardloom-vortex/src/native_exchange_peer.rs",
                "shardloom-vortex/src/native_exchange_service.rs",
                "shardloom-vortex/src/native_exchange_client.rs",
                "shardloom-vortex/tests/fixtures/native-exchange/certificates.py",
                "scripts/run_native_exchange_experiment.py", "scripts/native_exchange_result_checks.py",
                "scripts/native_exchange_process_faults.py", "scripts/local_uat_storage.py"}
    require(required <= seen, "build source manifest omits a required runtime or supervisor input")
    return {"verified_sources": len(sources), "source_manifest_sha256": digest(
        json.dumps(sources, sort_keys=True, separators=(",", ":")).encode())}


def process_rows() -> list[tuple[int, int, int, int, str]]:
    output = subprocess.check_output(["ps", "-axo", "pid=,ppid=,pgid=,rss=,comm="], text=True, timeout=5)
    return [(int(parts[0]), int(parts[1]), int(parts[2]), int(parts[3]) * 1024, parts[4])
            for line in output.splitlines() if len(parts := line.strip().split(None, 4)) == 5]


def native_name(name: str) -> bool:
    leaf = Path(name).name
    return leaf in {"cargo", "rustc", "shardloom", "pytest", "nextest", "plain_vortex_fixture", "plain-vortex-fixture"} or leaf.startswith(
        ("shardloom-", "shardloom_", "native_encoded_exchange_", "compare-native-artifacts"))


class Guard:
    def __init__(self, root: Path):
        self.root = root
        self.last = 0.0
        self.last_storage = None
        self.ancestors = {os.getpid()}
        by_pid = {row[0]: row for row in process_rows()}
        cursor = os.getpid()
        while cursor in by_pid and by_pid[cursor][1] > 0:
            cursor = by_pid[cursor][1]
            require(cursor not in self.ancestors, "process ancestry contains a cycle")
            self.ancestors.add(cursor)

    def check(self, peers: list["Peer"], *, force: bool = False) -> None:
        if not force and time.monotonic() - self.last < 2:
            return
        lock = Path.home() / "LocalData/shardloom/clickbench-100m-uat/.ingest-uat.lock"
        require(not lock.exists(), "another ingestion UAT owns the workload lock")
        owned = {peer.process.pid for peer in peers}
        rows = process_rows()
        require(not [row for row in rows if row[1] in owned], "finite peers unexpectedly spawned another process")
        conflicts = [{"pid": pid, "executable": name} for pid, _, _, _, name in rows
                     if pid not in self.ancestors | owned and native_name(name)]
        require(not conflicts, f"another native build/test/query is active: {conflicts}")
        self.last_storage = check_budgets(self.root, self.root / "reserved-exchange-output", self.root / "logs",
            min_free_bytes=12 * GIB, reserve_bytes=128 * MIB,
            max_workspace_bytes=100 * GIB, max_log_bytes=192 * MIB)
        self.last = time.monotonic()


class Peer:
    def __init__(self, executable: Path, role: str, selected: selectors.BaseSelector):
        self.role = role
        self.process = subprocess.Popen([str(executable), role], stdin=subprocess.PIPE,
                                        stdout=subprocess.PIPE, stderr=subprocess.PIPE, bufsize=0)
        self.events: list[dict] = []
        self.output = bytearray()
        self.stderr = bytearray()
        self.resource = None
        self.open_pipes = 2
        try:
            for kind, pipe in [("stdout", self.process.stdout), ("stderr", self.process.stderr)]:
                os.set_blocking(pipe.fileno(), False)
                selected.register(pipe, selectors.EVENT_READ, (self, kind))
        except BaseException:
            os.kill(self.process.pid, signal.SIGKILL)
            _, status, _ = os.wait4(self.process.pid, 0)
            self.process.returncode = os.waitstatus_to_exitcode(status)
            self.close()
            raise

    def send_bytes(self, data: bytes) -> None:
        require(len(data) < 16 * 1024, "supervisor command exceeds fixed control bound")
        remaining = memoryview(data + b"\n")
        while remaining:
            written = self.process.stdin.write(remaining)
            require(written is not None and written > 0, "supervisor command pipe stopped accepting bytes")
            remaining = remaining[written:]
        self.process.stdin.flush()

    def send(self, value: dict) -> None:
        self.send_bytes(json.dumps(value, sort_keys=True, separators=(",", ":")).encode())

    def reap(self) -> None:
        if self.process.returncode is not None:
            return
        pid, status, usage = os.wait4(self.process.pid, os.WNOHANG)
        if pid:
            self.process.returncode = os.waitstatus_to_exitcode(status)
            self.resource = {"user_cpu_seconds": usage.ru_utime, "system_cpu_seconds": usage.ru_stime,
                "peak_rss_bytes": usage.ru_maxrss * (1 if sys.platform == "darwin" else 1024)}

    def event(self, name: str) -> dict | None:
        matches = [event for event in self.events if event.get("event") == name]
        require(len(matches) <= 1, f"duplicate {self.role} {name} event")
        return matches[0] if matches else None

    def read(self, pipe, kind: str, selected: selectors.BaseSelector) -> None:
        block = os.read(pipe.fileno(), 64 * 1024)
        if not block:
            selected.unregister(pipe)
            pipe.close()
            self.open_pipes -= 1
            require(kind != "stdout" or not self.output, "truncated peer event line")
            return
        if kind == "stderr":
            self.stderr.extend(block)
            require(len(self.stderr) <= 64 * 1024, "peer diagnostic limit exceeded")
            return
        self.output.extend(block)
        require(len(self.output) <= PIPE_LIMIT, "peer event exceeds fixed pipe limit")
        while b"\n" in self.output:
            line, _, remaining = self.output.partition(b"\n")
            self.output[:] = remaining
            value = json.loads(line)
            require(isinstance(value, dict) and value.get("event") in {"ready", "partial", "received", "final"}, "unknown peer event")
            self.events.append(value)
            require(len(self.events) <= 3, "peer event count exceeded")

    def close(self) -> None:
        for pipe in [self.process.stdin, self.process.stdout, self.process.stderr]:
            if pipe and not pipe.closed:
                pipe.close()


class Pair:
    def __init__(self, guard: Guard, executable: Path, config: dict):
        self.guard = guard
        self.executable = executable
        self.config = config
        self.selected = selectors.DefaultSelector()
        self.peers: list[Peer] = []
        self.started = time.monotonic()
        self.peak_rss = 0
        self.rss_samples: list[dict] = []
        self.last_rss = 0.0
        self.allowed_exit_codes: dict[int, set[int]] = {}

    def tick(self, *, terminating: bool = False) -> None:
        if not terminating:
            require(time.monotonic() - self.started < WORKFLOW_LIMIT, "supervised workflow exceeded 120 seconds")
            self.guard.check(self.peers)
        for key, _ in self.selected.select(timeout=0.01):
            peer, kind = key.data
            peer.read(key.fileobj, kind, self.selected)
        alive = [peer for peer in self.peers if peer.process.returncode is None]
        if alive and time.monotonic() - self.last_rss >= 0.02:
            sampled = subprocess.run(["ps", "-o", "pid=,rss=", "-p", ",".join(str(p.process.pid) for p in alive)],
                                     capture_output=True, text=True, timeout=5, check=False)
            require(sampled.returncode in (0, 1), "peer RSS sampling failed")
            processes = [{"pid": int(parts[0]), "rss_bytes": int(parts[1]) * 1024}
                         for line in sampled.stdout.splitlines() if len(parts := line.split()) == 2]
            if processes:
                self.peak_rss = max(self.peak_rss, sum(row["rss_bytes"] for row in processes))
                self.rss_samples.append({"elapsed_seconds": time.monotonic() - self.started,
                                         "processes": processes})
                require(len(self.rss_samples) <= 6001, "RSS observation limit exceeded")
            self.last_rss = time.monotonic()
            if not terminating:
                require(self.peak_rss <= PAIR_RSS_LIMIT, "combined live peer RSS exceeded two GiB")
        for peer in self.peers:
            peer.reap()
            if not terminating:
                require(peer.process.returncode in {None, 0} | self.allowed_exit_codes.get(peer.process.pid, set()),
                        f"{peer.role} exited with {peer.process.returncode}: {peer.stderr.decode(errors='replace')}")

    def until(self, predicate) -> None:
        while not predicate():
            self.tick()
            if all(peer.process.returncode is not None and peer.open_pipes == 0 for peer in self.peers):
                require(predicate(), "peers exited before the required event")

    def shutdown(self) -> None:
        for peer in self.peers:
            if peer.process.returncode is None:
                try:
                    os.kill(peer.process.pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
        deadline = time.monotonic() + 5
        while any(peer.process.returncode is None for peer in self.peers) and time.monotonic() < deadline:
            for peer in self.peers:
                peer.reap()
            time.sleep(0.01)
        for peer in self.peers:
            if peer.process.returncode is None:
                try:
                    os.kill(peer.process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
        deadline = time.monotonic() + 5
        while any(peer.process.returncode is None for peer in self.peers) and time.monotonic() < deadline:
            for peer in self.peers:
                peer.reap()
            time.sleep(0.01)
        require(all(peer.process.returncode is not None for peer in self.peers), "owned peer did not terminate")

    def run(self, manifest: dict, freeze: dict) -> dict:
        config = json.dumps(self.config, sort_keys=True, separators=(",", ":")).encode()
        record = {"configuration": self.config, "configuration_sha256": digest(config), "status": "running"}
        try:
            self.guard.check([], force=True)
            self.started = time.monotonic()
            sender = Peer(self.executable, "sender", self.selected)
            self.peers.append(sender)
            sender.send_bytes(config)
            receiver = Peer(self.executable, "receiver", self.selected)
            self.peers.append(receiver)
            receiver.send_bytes(config)
            record.update(sender_pid=sender.process.pid, receiver_pid=receiver.process.pid)
            self.until(lambda: sender.event("ready") is not None)
            ready = sender.event("ready")
            require(ready["pid"] == sender.process.pid and ready["configuration_sha256"] == digest(config), "sender ready identity differs")
            record["sender_ready_address"] = ready["address"]
            receiver.send({"command": "connect", "address": ready["address"], "sender_pid": sender.process.pid})
            self.until(lambda: receiver.event("received") is not None)
            received = receiver.event("received")
            require(received["pid"] == receiver.process.pid and received["sender_pid"] == sender.process.pid and
                    received["configuration_sha256"] == digest(config) and received["completed_executions"] == 0,
                    "receiver executed early or changed its process/configuration identity")
            record["received_before_consumer"] = received
            sender.send({"command": "stop"})
            self.until(lambda: sender.process.returncode is not None and sender.open_pipes == 0)
            require(sender.process.returncode == 0 and sender.event("final") is not None, "sender did not complete before the barrier")
            record["sender_reaped_before_consume"] = True
            receiver.send({"command": "consume", "sender_pid": sender.process.pid, "sender_exit_code": sender.process.returncode})
            self.until(lambda: receiver.process.returncode is not None and receiver.open_pipes == 0)
            require(receiver.event("final") is not None, "receiver final report is missing")
            record.update(sender=sender.event("final")["report"], receiver=receiver.event("final")["report"],
                sender_exit_code=sender.process.returncode, receiver_exit_code=receiver.process.returncode,
                owned_processes_drained=all(peer.process.returncode is not None and peer.open_pipes == 0 for peer in self.peers),
                resources={"sender": sender.resource, "receiver": receiver.resource,
                           "peak_combined_sampled_rss_bytes": self.peak_rss, "rss_samples": self.rss_samples})
            record["checks"] = verify_workflow(record, manifest, freeze)
            record["complete_workflow_seconds"] = time.monotonic() - self.started
            record["status"] = "passed"
        except BaseException as error:
            record.update(status="failed", error=f"{type(error).__name__}: {error}")
        finally:
            try:
                self.shutdown()
                record["owned_processes_drained"] = True
            except BaseException as error:
                record.update(status="failed", shutdown_error=f"{type(error).__name__}: {error}", owned_processes_drained=False)
            record["peers"] = [{"role": peer.role, "pid": peer.process.pid, "exit_code": peer.process.returncode,
                                "resources": peer.resource, "stderr": peer.stderr.decode(errors="replace"),
                                "events": peer.events if record["status"] != "passed" else []} for peer in self.peers]
            for peer in self.peers:
                peer.close()
            self.selected.close()
        return record


def fresh_certificates(path: Path) -> dict:
    recipe = ROOT / "shardloom-vortex/tests/fixtures/native-exchange/certificates.py"
    spec = importlib.util.spec_from_file_location("exchange_certificates", recipe)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    manifest = module.generate(path)
    return {"recipe": identity(recipe), "manifest": identity(manifest), "public": json.loads(manifest.read_bytes())}


def retire_private_keys(root: Path, *, complete: bool) -> dict:
    keys = sorted(root.glob("*.key.pem"))
    roles = {"server", "reader", "reader2", "denied", "administrator", "unlisted", "expired", "unknown"}
    require(all(key.name in {f"{role}.key.pem" for role in roles} for key in keys), "unexpected key in new private directory")
    require(not complete or len(keys) == 8, "private certificate fixture key inventory differs")
    for key in keys:
        mode = key.lstat().st_mode
        require(stat.S_ISREG(mode) and stat.S_IMODE(mode) == 0o600, "unexpected private key type or permissions")
    if keys:
        lsof = subprocess.run(["/usr/sbin/lsof" if sys.platform == "darwin" else "lsof", "-Fpn", *map(str, keys)],
                              capture_output=True, text=True, timeout=10, check=False)
        require(lsof.returncode == 1 and not lsof.stdout, "private key file remains open or handle check failed")
    names = [key.name for key in keys]
    for key in keys:
        key.unlink()
    require(not list(root.glob("*.key.pem")), "private key cleanup is incomplete")
    return {"deleted_key_files": names, "all_owned_processes_reaped_before_cleanup": True,
            "open_key_handles": 0, "public_certificates_retained": True}


def jobs(args, freeze: dict):
    if args.scope == "smoke":
        require(args.case is not None and args.mode is not None, "smoke requires an explicit case and mode")
        yield args.case, args.mode, 0, 0
        return
    if args.scope == "interop":
        for position, mode in enumerate(["sql", "native_fragments"]):
            yield "dictionary_large", mode, 0, position
        return
    cases = list(enumerate(case["id"] for case in freeze["cases"]))
    if args.cohort == "2":
        cases.reverse()
    for index, case in cases:
        for sample in range(1 if args.cohort == "preflight" else 5):
            offset = (index + sample) % len(MODES)
            order = MODES[offset:] + MODES[:offset]
            if args.cohort == "2":
                order.reverse()
            for position, mode in enumerate(order):
                if args.scope == "native" and mode != "native_fragments":
                    continue
                if args.scope == "controls" and mode == "native_fragments":
                    continue
                yield case, mode, sample, position


def save(path: Path, report: dict) -> None:
    partial = path.with_suffix(path.suffix + ".partial")
    with partial.open("x") as output:
        json.dump(report, output, sort_keys=True, separators=(",", ":"), allow_nan=False)
        output.write("\n")
    require(json.loads(partial.read_bytes()) == report, "report readback differs")
    os.replace(partial, path)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--executable", required=True, type=Path)
    parser.add_argument("--fixtures", required=True, type=Path)
    parser.add_argument("--evidence", required=True, type=Path)
    parser.add_argument("--scope", choices=["all", "native", "controls", "interop", "smoke", "failures"], default="all")
    parser.add_argument("--case")
    parser.add_argument("--mode", choices=[*MODES, "sql"])
    parser.add_argument("--cohort", choices=["preflight", "1", "2"], default="preflight")
    parser.add_argument("--build-freeze", type=Path)
    args = parser.parse_args()
    require(args.scope == "smoke" or (args.case is None and args.mode is None),
            "case and mode selection is limited to unscored smoke checks")
    require(os.name == "posix" and hasattr(os, "wait4"), "experiment resource accounting requires POSIX wait4")
    require(args.cohort == "preflight" or (args.scope == "all" and args.build_freeze is not None), "scored cohorts require all frozen roles and build identity")
    args.executable, args.fixtures, args.evidence = [require_local_path(path, Path.home(), sys.platform)
        for path in (args.executable, args.fixtures, args.evidence)]
    require(native_name(args.executable.name), "experiment executable name must remain recognizable to process guards")
    require(args.evidence.parent.is_dir() and not args.evidence.exists(), "evidence requires an existing local parent and fresh filename")
    manifest_path = args.fixtures / "fixture-manifest.json"
    freeze_bytes = FREEZE.read_bytes()
    require(digest(freeze_bytes) == FREEZE_SHA, "frozen protocol identity changed")
    freeze = json.loads(freeze_bytes)
    manifest_bytes = manifest_path.read_bytes()
    require(digest(manifest_bytes) == freeze["fixture_manifest"]["sha256"], "fixture manifest differs from freeze")
    manifest = json.loads(manifest_bytes)
    executable = identity(args.executable)
    build = None
    build_verification = None
    if args.build_freeze:
        build = json.loads(args.build_freeze.read_bytes())
        build_verification = verify_build_freeze(build, executable, digest(manifest_bytes))
    guard = Guard(args.evidence.parent)
    guard.check([], force=True)
    report = {"version": 1, "status": "running", "scope": args.scope, "cohort": args.cohort,
              "protocol_freeze": identity(FREEZE), "fixture_manifest": identity(manifest_path),
              "executable": executable, "runner": identity(Path(__file__)),
              "result_verifier": identity(ROOT / "scripts/native_exchange_result_checks.py"),
              "workflows": [], "limits": {"combined_peer_rss_bytes": PAIR_RSS_LIMIT, "workflow_seconds": WORKFLOW_LIMIT},
              "timing_boundary": "process launch through both exits and complete result verification; stages are not summed",
              "retention_decision": "not_evaluated"}
    if args.build_freeze:
        report["build_freeze"] = identity(args.build_freeze)
        report["build_verification_before"] = build_verification
    tls = args.evidence.with_suffix(".tls-private")
    journal_path = args.evidence.with_suffix(".workflows.jsonl")
    require(not tls.exists() and not journal_path.exists(), "ephemeral certificate and journal paths must be fresh")
    with args.evidence.open("x") as output:
        json.dump(report, output)
    try:
        report["certificates"] = fresh_certificates(tls)
        base_config = {"version": 1, "fixture_root": str(args.fixtures), "tls_root": str(tls),
                       "certificate_manifest_sha256": report["certificates"]["manifest"]["sha256"]}
        if args.scope == "failures":
            from native_exchange_process_faults import run_failures
            report["process_fault_driver"] = identity(ROOT / "scripts/native_exchange_process_faults.py")
            observations = run_failures(Pair, Peer, guard, args.executable, base_config, manifest, freeze)
        else:
            observations = measured_workflows(args, guard, base_config, executable, manifest, freeze)
        with journal_path.open("x") as journal:
            for result in observations:
                report["workflows"].append(result)
                journal.write(json.dumps(result, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n")
                journal.flush()
                require(journal.tell() <= 192 * MIB, "workflow evidence exceeds fixed log ceiling")
                print(json.dumps({"completed_workflows": len(report["workflows"]),
                                  "case": result["configuration"]["source_id"],
                                  "mode": result["configuration"]["mode"], "status": result["status"]}), flush=True)
                require(result["status"] == "passed", "workflow failed; cohort invalid and remaining jobs were not substituted")
        report["status"] = "passed"
    except BaseException as error:
        report.update(status="failed", error=f"{type(error).__name__}: {error}")
    finally:
        try:
            require(all(row.get("owned_processes_drained") is True for row in report["workflows"]), "owned peer cleanup is incomplete")
            if tls.exists():
                report["key_retirement"] = retire_private_keys(tls, complete="certificates" in report)
            if build is not None:
                require(identity(args.build_freeze) == report["build_freeze"], "build freeze changed during cohort")
                report["build_verification_after"] = verify_build_freeze(build, identity(args.executable), digest(manifest_path.read_bytes()))
                require(report["build_verification_after"] == report["build_verification_before"], "build inputs changed during cohort")
            guard.check([], force=True)
            report["storage_final"] = guard.last_storage
        except BaseException as error:
            report.update(status="failed", cleanup_error=f"{type(error).__name__}: {error}")
        if journal_path.exists():
            original = journal_path.read_bytes()
            journal_rows = [json.loads(line) for line in original.splitlines()]
            require(journal_rows == report["workflows"], "workflow journal differs from retained report")
            report["journal_retirement"] = {"bytes": len(original), "sha256": digest(original), "rows": len(journal_rows), "verified_readback": True}
        save(args.evidence, report)
        if journal_path.exists():
            journal_path.unlink()
    print(json.dumps({"status": report["status"], "workflows": len(report["workflows"]), "evidence": str(args.evidence)}), flush=True)
    return 0 if report["status"] == "passed" else 1


def measured_workflows(args, guard, base_config: dict, executable: dict, manifest: dict, freeze: dict):
    for case, mode, sample, position in jobs(args, freeze):
        guard.check([], force=True)
        info = args.executable.stat()
        require((info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns) ==
                (executable["device"], executable["inode"], executable["bytes"], executable["mtime_ns"]),
                "executable changed during cohort")
        config = {**base_config, "source_id": case, "mode": mode,
                  "server_epoch": os.urandom(16).hex(), "attempt_id": os.urandom(16).hex()}
        result = Pair(guard, args.executable, config).run(manifest, freeze)
        result.update(sample=sample, role_position=position)
        yield result


if __name__ == "__main__":
    raise SystemExit(main())
