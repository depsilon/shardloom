# SPDX-License-Identifier: Apache-2.0
"""Supervise a single Polars-first observation of the frozen expanded catalog."""

from __future__ import annotations

import argparse
from contextlib import ExitStack, contextmanager
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import platform
import sqlite3
import sys
import time

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / "scripts"), str(ROOT / "python/src"), str(Path(__file__).resolve().parents[1])]
from local_uat_storage import GIB, MIB, check_budgets, require_local_path
from resources import BenchmarkGuard
from run_clickbench_query_uat import run_profiled_command
from query_surface.evidence import generation, identity, validate_prepared, worker_outcome
from query_surface.expanded_freeze import pack, read, unpack
from query_surface.run import prepare, write_json


def verify(files):
    unique = {f["path"]: f for f in files}
    if any(unique[f["path"]] != f for f in files):
        raise ValueError("conflicting frozen file identities")
    for path, wanted in unique.items():
        if identity(Path(path)) != wanted:
            raise ValueError("frozen file content or generation changed: " + path)


def admission(root):
    frozen = read(root / "frozen.json")
    if frozen["status"] != "frozen_before_comparison" or frozen["total"] != 10_000:
        raise ValueError("incomplete frozen cohort")
    inputs = read(root / "inputs.json")
    design = read(root / "design.json")
    cases = design["cases"]
    if (
        not __debug__
        or len(cases) != frozen["total"]
        or design["count"] != len(cases)
        or len({r["id"] for r in cases}) != len(cases)
        or len({r["semantic_sql_sha256"] for r in cases}) != len(cases)
        or sum("reference" in r for r in cases) != 347
    ):
        raise ValueError("invalid or incomplete expanded case inventory")
    files = [
        frozen["design"],
        frozen["inputs"],
        *frozen["references"],
        *frozen["harness"],
        *frozen["environment_files"],
        *inputs["files"],
        *inputs["runtime"].values(),
    ]
    verify(files)
    if frozen["design"] != inputs["design"] or any(
        design["execution"][k] != v
        for k, v in {"threads": 2, "attempts_per_case_and_route": 1, "warmups": 0}.items()
    ):
        raise ValueError("frozen one-pass contract differs")
    if sys.version != inputs["python"] or platform.machine() != inputs["machine"]:
        raise ValueError("runtime environment differs")
    return design, inputs, files


def stage_complete(root, stage, expected):
    path = root / ("observe-" + stage) / "complete.json"
    record = read(path)
    if (
        record["status"] != "complete_observation"
        or record["total"] != expected
        or record["attempt_slots"] != expected
        or not record["posthash_verified"]
        or identity(Path(record["ledger"]["path"])) != record["ledger"]
        or identity(root / "frozen.json") != record["frozen"]
    ):
        raise ValueError("prerequisite stage did not complete under the same freeze: " + stage)
    with closing_db(Path(record["ledger"]["path"]), readonly=True) as ledger:
        counts = dict(ledger.execute("SELECT status, COUNT(*) FROM attempt GROUP BY status"))
    if counts != record["outcomes"] or sum(counts.values()) != expected or "started" in counts:
        raise ValueError("prerequisite ledger is incomplete: " + stage)
    return record


@contextmanager
def stage_lock(root):
    lock = root / ".expanded-execution.lock"
    lock.mkdir()
    try:
        yield
    finally:
        lock.rmdir()


def prepare_sources(root, options):
    design, inputs, files = admission(root)
    stage_complete(root, "polars", design["count"])
    baseline = identity(root / "observe-polars/complete.json")
    options.binary = Path(inputs["runtime"]["binary"]["path"])
    options.library = Path(inputs["runtime"]["library"]["path"])
    options.memory_gb = 2
    options.parallelism = 2
    options.preparation_timeout = 900
    stage = root / "native-preparation"
    if (stage / "complete.json").exists():
        raise FileExistsError("completed preparation is immutable")
    with stage_lock(root), BenchmarkGuard(stage, binary=options.binary, memory_gb=2) as guard:
        reports = {}
        for name, group in inputs["profiles"].items():
            if group["sources"]["vortex"]:
                continue
            dataset = Path(group["dataset"])
            meta = read(dataset / "dataset.json")
            logs = stage / (name + "-logs")
            path = stage / name / "prepared.json"
            if not path.exists():
                logs.mkdir()
                prepare(
                    dataset,
                    meta,
                    stage / name,
                    logs,
                    options,
                    guard,
                    parquet_sources=group["sources"]["parquet"],
                )
            prepared, receipts = validate_prepared(
                path, dataset, meta, parquet_sources=group["sources"]["parquet"]
            )
            reports[name] = dict(
                manifest=identity(path),
                sources=prepared["sources"],
                receipts=receipts,
                inputs=prepared["inputs"],
                rows=group["spec"]["fact_rows"],
            )
        verify(files)
        write_json(
            stage / "complete.json",
            dict(
                status="passed",
                frozen=identity(root / "frozen.json"),
                polars_complete=baseline,
                completed_at=datetime.now(timezone.utc).isoformat(),
                stage="explicit_parquet_to_vortex_and_complete_ipc_readback",
                profiles=reports,
            ),
        )


def request(row, stage, inputs, prepared, expected, policy):
    group = inputs["profiles"][row["profile"]]
    surface = row["surface"]
    engine = "polars" if stage == "polars" else "native"
    fmt = row["polars_format"] if engine == "polars" else row["native_format"]
    if stage == "alternate":
        if row["profile"] != "scale" or not row["surrogate"] or fmt != "parquet" or surface != "python_sql":
            raise ValueError("alternate route outside its predeclared surrogate")
        fmt = "vortex"
    sources = group["sources"][fmt]
    if sources is None:
        sources = prepared["profiles"][row["profile"]]["sources"]
    return dict(
        case=row,
        engine=engine,
        surface=surface,
        input_format=fmt,
        sources=sources,
        runtime=inputs["runtime"],
        expected=expected,
        memory_bytes=policy["native_memory_bytes"],
        parallelism=policy["threads"],
        timeout=policy["query_deadline_seconds"],
    )


def verify_request_files(job, frozen_paths):
    names = {str(p) for parts in job["sources"].values() for p in parts}
    names.update(i["path"] for i in job["runtime"].values())
    for p in names:
        if generation(Path(p)) != frozen_paths[p]["generation"]:
            raise ValueError("input or runtime changed during the cohort: " + p)


def raw_packet(directory):
    files = {}
    for p in directory.iterdir():
        if p.is_symlink() or not p.is_file() or p.stat().st_size > 9 * MIB:
            raise ValueError("unexpected active worker output")
        data = p.read_bytes()
        try:
            text = data.decode("utf-8")
            record = {"utf8": text}
        except UnicodeDecodeError:
            import base64

            record = {"base64": base64.b64encode(data).decode("ascii")}
        files[p.name] = dict(bytes=len(data), sha256=hashlib.sha256(data).hexdigest(), **record)
    return files


def observe(root, stage, resume):
    design, inputs, frozen_files = admission(root)
    prepared = {}
    prerequisites = {}
    if stage != "polars":
        stage_complete(root, "polars", design["count"])
        prerequisites["polars"] = identity(root / "observe-polars/complete.json")
        prerequisites["preparation"] = identity(root / "native-preparation/complete.json")
        prepared = read(root / "native-preparation/complete.json")
        if (
            prepared["status"] != "passed"
            or prepared["frozen"] != identity(root / "frozen.json")
            or prepared["polars_complete"] != prerequisites["polars"]
        ):
            raise ValueError("explicit preparation is incomplete")
        extra = [
            f for p in prepared["profiles"].values() for f in [p["manifest"], *p["inputs"], *p["receipts"]]
        ]
        verify(extra)
        frozen_files += extra
    if stage == "alternate":
        stage_complete(root, "native", design["count"])
        prerequisites["native"] = identity(root / "observe-native/complete.json")
    selected = [r for r in design["cases"] if stage != "alternate" or r["surrogate"]]
    if stage == "alternate" and [r["id"] for r in selected] != design["alternate_route_cases"]:
        raise ValueError("paired probe order changed")
    target = root / ("observe-" + stage)
    if (target / "complete.json").exists():
        raise FileExistsError("completed scored stage cannot be repeated")
    if target.exists() != resume:
        raise ValueError("select a fresh stage, or explicitly resume its unattempted requests")
    policy = design["execution"]
    wanted_freeze = identity(root / "frozen.json")
    generations = {f["path"]: f for f in frozen_files}
    budget = check_budgets(
        root,
        root / "reserved-output",
        root / "logs",
        min_free_bytes=12 * GIB,
        reserve_bytes=64 * MIB,
        max_workspace_bytes=100 * GIB,
        max_log_bytes=192 * MIB,
    )
    with stage_lock(root), ExitStack() as stack:
        target.mkdir(exist_ok=resume)
        guard = stack.enter_context(
            BenchmarkGuard(target, binary=Path(inputs["runtime"]["binary"]["path"]), memory_gb=2)
        )
        references = {}
        for profile in {r["profile"] for r in selected}:
            loc = root / ("reference-" + profile) / "expectations.sqlite"
            references[profile] = stack.enter_context(closing_db(loc, readonly=True))
        ledger = target / "attempts.sqlite"
        db = stack.enter_context(closing_db(ledger))
        db.execute(
            "CREATE TABLE IF NOT EXISTS attempt (seq INTEGER PRIMARY KEY,id TEXT NOT NULL UNIQUE,status TEXT NOT NULL, packet BLOB)"
        )
        binding = target / "binding.json"
        contract = {"frozen": wanted_freeze, "stage": stage, "prerequisites": prerequisites}
        if resume:
            if read(binding)["contract"] != contract:
                raise ValueError("resumed stage bindings differ")
            db.execute("UPDATE attempt SET status='incomplete' WHERE status='started'")
            db.commit()
        else:
            write_json(binding, {"contract": contract, "started_at": datetime.now(timezone.utc).isoformat()})
        active = target / "active"
        active.mkdir(exist_ok=resume)
        # A non-empty active folder after interruption is evidence. Keep it
        # without executing that slot again.
        if any(active.iterdir()):
            if not resume:
                raise ValueError("active directory must initially be empty")
            active.rename(target / ("interrupted-" + str(time.time_ns())))
            active.mkdir()
        seen = {seq: (id_, status) for seq, id_, status in db.execute("SELECT seq,id,status FROM attempt")}
        if any(
            seq < 0 or seq >= len(selected) or selected[seq]["id"] != id_ for seq, (id_, _) in seen.items()
        ):
            raise ValueError("resumed attempt identities differ")
        for seq, row in enumerate(selected):
            if seq in seen:
                continue
            ref = (
                references[row["profile"]]
                .execute("SELECT identity,value FROM expected WHERE id=?", (row["id"],))
                .fetchone()
            )
            if ref is None or ref[0] != row["case_identity"]:
                raise ValueError("case lacks an exact independent result")
            job = request(row, stage, inputs, prepared, unpack(ref[1]), policy)
            verify_request_files(job, generations)
            guard.check()
            if ledger.exists() and ledger.stat().st_size > 1536 * MIB:
                raise ValueError(
                    "active compressed evidence exceeds the prospectively bounded storage allocation"
                )
            job_path = active / "one.job.json"
            write_json(job_path, job)
            db.execute("INSERT INTO attempt VALUES (?,?,?,NULL)", (seq, row["id"], "started"))
            db.commit()
            cmd = [
                sys.executable,
                "-B",
                str(Path(__file__).with_name("expanded_worker.py")),
                "--job",
                str(job_path),
            ]
            receipt = run_profiled_command(
                cmd, active / "one", policy["query_deadline_seconds"] + 5, guard.check
            )
            raw = (active / "one.stdout.json").read_bytes()
            outcome = worker_outcome(receipt, raw)
            status = outcome["status"]
            if status in {"passed", "incorrect", "comparison_inconclusive"} and (
                outcome.get("id") != row["id"]
                or outcome.get("case_identity") != row["case_identity"]
                or outcome.get("execution_engine") != job["engine"]
            ):
                raise ValueError("successful worker response belongs to another request")
            packet = dict(
                supervision=receipt,
                files=raw_packet(active),
                classified_status=status,
                classification_detail=outcome.get("detail"),
            )
            encoded = pack(packet)
            db.execute(
                "UPDATE attempt SET status=?,packet=? WHERE id=? AND status='started'",
                (status, encoded, row["id"]),
            )
            db.commit()
            saved = db.execute("SELECT packet FROM attempt WHERE id=?", (row["id"],)).fetchone()[0]
            if saved != encoded:
                raise ValueError("compressed evidence did not persist")
            for name, value in packet["files"].items():
                path = active / name
                if hashlib.sha256(path.read_bytes()).hexdigest() != value["sha256"]:
                    raise ValueError("active evidence changed before compact retention")
                path.unlink()
            global_failure = [
                f
                for f in receipt["guard_failures"]
                if "another native workload" in f or "free" in f or "storage" in f
            ]
            if global_failure:
                raise ValueError("external guard stopped the stage: " + str(global_failure))
            if (seq + 1) % 100 == 0 or seq + 1 == len(selected):
                print(
                    json.dumps(
                        dict(
                            stage=stage,
                            attempts=seq + 1,
                            total=len(selected),
                            outcomes=dict(db.execute("SELECT status,COUNT(*) FROM attempt GROUP BY status")),
                        )
                    ),
                    flush=True,
                )
        outcomes = dict(db.execute("SELECT status,COUNT(*) FROM attempt GROUP BY status"))
        observed = list(db.execute("SELECT id FROM attempt ORDER BY seq"))
        if observed != [(r["id"],) for r in selected] or "started" in outcomes:
            raise ValueError("incomplete attempt inventory")
    verify(frozen_files)
    write_json(
        target / "complete.json",
        dict(
            status="complete_observation",
            stage=stage,
            frozen=wanted_freeze,
            total=len(selected),
            attempt_slots=sum(outcomes.values()),
            outcomes=outcomes,
            ledger=identity(ledger),
            binding=identity(binding),
            prerequisites=prerequisites,
            completed_at=datetime.now(timezone.utc).isoformat(),
            storage_before=budget,
            posthash_verified=True,
            attempts_per_case=1,
            requested_repeats=0,
        ),
    )


@contextmanager
def closing_db(path, readonly=False):
    db = (
        sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True) if readonly else sqlite3.connect(path)
    )
    try:
        yield db
    finally:
        db.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("stage", choices=["polars", "prepare", "native", "alternate"])
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--resume-unattempted", action="store_true")
    args = parser.parse_args()
    root = require_local_path(args.root, Path.home(), sys.platform)
    os.environ["POLARS_MAX_THREADS"] = "2"
    if args.stage == "prepare":
        prepare_sources(root, args)
    else:
        observe(root, args.stage, args.resume_unattempted)


if __name__ == "__main__":
    main()
