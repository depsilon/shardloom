# SPDX-License-Identifier: Apache-2.0
"""Freeze the expanded cohort, independent expectations and physical source bindings."""

from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from contextlib import closing
from dataclasses import asdict
from importlib import metadata
import json
from pathlib import Path
import platform
import sqlite3
import subprocess
import sys
import time
import zlib

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / "scripts"), str(ROOT / "python/src"), str(Path(__file__).resolve().parents[1])]
from local_uat_storage import require_local_path
from resources import BenchmarkGuard
from query_surface.adapters import relation_sql
from query_surface.corpus import corpus
from query_surface.datasets import SCHEMAS, file_hash
from query_surface.evidence import identity, validate_dataset, validate_prepared
from query_surface.expanded_catalog import ORIGINAL, TOTAL, VERSION, build, catalog, digest, spaces
from query_surface.expanded_inputs import PROFILES
from query_surface.expanded_reference import expected, open_database
from query_surface.expanded_reference_input import build_database
from query_surface.run import write_json
from query_surface.semantics import result_from_record, result_record
from query_surface.specification import SURFACES

# The 1M fixture supports several native routes. All 5M summary surrogates use
# the same Parquet source for Polars, direct native and explicit preparation.
from query_surface.expanded_catalog import SURROGATES

# Independent input transport. Other columns in the 38-column fact source
# remain available to the historical queries under their original references.
ORACLE_COLUMNS = {
    "fact": set(
        "row_id entity_id product_id group_key medium_key high_key skew_key signed_value metric optional_value mostly_null all_null nullable_no_null flag nullable_flag category short_text unicode_text optional_text event_date event_time selection_key independent_key correlated_key anti_correlated_key run_value items detail amount".split()
    ),
    "entities": {"entity_id", "region", "active"},
    "products": {"product_id", "product_category"},
    "relationships": {"entity_id", "relationship_id", "weight"},
    "calendar": {"day_index", "week_index"},
}


def read(path):
    return json.loads(path.read_bytes())


def pack(document):
    return zlib.compress(
        json.dumps(document, sort_keys=True, separators=(",", ":"), allow_nan=False).encode(), 3
    )


def unpack(blob):
    return json.loads(zlib.decompress(blob))


def assign(rows):
    chosen = {}
    by_shape = defaultdict(list)
    for row in rows:
        by_shape[row["archetype"]].append(row)
    for name, (shape, anchor) in SURROGATES.items():
        (candidate,) = [r for r in by_shape[shape] if r["cell"] == anchor]
        chosen[candidate["id"]] = name
    scale = set(chosen)
    ranked = lambda cohort, r: digest((VERSION, cohort, r["id"]))
    for s, group in by_shape.items():
        scale.add(min((r for r in group if r["id"] not in scale), key=lambda r: ranked("scale", r))["id"])
    for r in sorted(rows, key=lambda r: ranked("scale_fill", r)):
        if len(scale) == 64:
            break
        scale.add(r["id"])
    smoke = set()
    holdout = set()
    for group in by_shape.values():
        available = [r for r in group if r["id"] not in scale]
        smoke.update(r["id"] for r in sorted(available, key=lambda r: ranked("smoke", r))[:3])
        holdout.update(
            r["id"]
            for r in sorted(
                (r for r in available if r["id"] not in smoke), key=lambda r: ranked("holdout", r)
            )[:20]
        )
    for r in rows:
        id_ = r["id"]
        profile = (
            "scale"
            if id_ in scale
            else "smoke"
            if id_ in smoke
            else "holdout"
            if id_ in holdout
            else "primary"
        )
        lane = int(ranked("source_route", r)[:8], 16) % 5
        surface_pool = (
            SURFACES
            if r["archetype"] not in {"shared_materialized_consumers", "recursive_frontier_reduction"}
            else SURFACES[:3]
        )
        surface = surface_pool[int(ranked("public_interface", r)[:8], 16) % len(surface_pool)]
        route = ("parquet", "vortex", "parquet", "arrow-ipc", "vortex")[lane]
        if id_ in chosen:
            surface, route = "python_sql", "parquet"
        original = (
            "arrow-ipc" if route == "arrow-ipc" or (route == "vortex" and profile == "primary") else "parquet"
        )
        r.update(
            profile=profile,
            surface=surface,
            native_format=route,
            polars_format=original,
            partition="held_out" if profile == "holdout" else "discovery",
            surrogate=chosen.get(id_),
        )
    return rows


def design(root, legacy_path):
    old = read(legacy_path)
    original = {c.id: c for c in corpus(interactions="all", include_pending=False)}
    if (
        len(original) != ORIGINAL
        or len(old["cases"]) != ORIGINAL
        or len({c["case_id"] for c in old["cases"]}) != ORIGINAL
    ):
        raise ValueError("original cohort changed")
    refs = {i["path"]: i["sha256"] for i in old["frozen_files"]}
    rows = []
    for stored in old["cases"]:
        c = original[stored["case_id"]]
        if (
            c.identity != stored["case_identity"]
            or refs[stored["reference_path"]] != stored["reference_sha256"]
        ):
            raise ValueError("frozen original differs")
        rows.append(
            dict(
                id=c.id,
                archetype=c.archetype,
                family=c.family,
                case_identity=c.identity,
                reference=stored["reference_path"],
                reference_sha256=stored["reference_sha256"],
                profile="primary",
                surface="python_sql",
                native_format="parquet",
                polars_format="parquet",
                partition="legacy",
                surrogate=None,
            )
        )
    new, coverage = catalog()
    declared = {s.name: s for s in spaces()}
    symbolic = {t: [t + ".parquet"] for t in SCHEMAS}
    requests = set()
    for r in rows + assign(new):
        c = build(r, declared) if "cell" in r else original[r["id"]]
        sql = relation_sql(c.relation, symbolic)
        sid = digest(sql)
        if sid in requests:
            raise ValueError("duplicate logical SQL request: " + c.id)
        requests.add(sid)
        r.update(case_identity=c.identity, semantic_sql_sha256=sid)
    rows += new
    if len(rows) != TOTAL or len(requests) != TOTAL:
        raise ValueError("incorrect expanded cohort denominator")
    record = dict(
        version=VERSION,
        count=TOTAL,
        legacy_count=ORIGINAL,
        legacy_inputs=identity(legacy_path),
        distinct_rendered_requests=len(requests),
        cases=rows,
        coverage=coverage,
        pending_v1=[c.record() for c in corpus(interactions="all") if c.relation is None],
        generated_archetypes=sorted({r["archetype"] for r in rows}),
        profile_counts=dict(Counter(r["profile"] for r in rows)),
        surface_counts=dict(Counter(r["surface"] for r in rows)),
        source_counts=dict(Counter(r["native_format"] for r in rows)),
        alternate_route_cases=[r["id"] for r in rows if r["surrogate"]],
        execution=dict(
            order=["polars_once", "explicit_preparation", "native_once", "twelve_alternate_source_routes"],
            threads=2,
            native_memory_bytes=2 << 30,
            process_group_rss_ceiling_bytes=2684354560,
            query_deadline_seconds=120,
            reference_output_limit_rows=10_000,
            attempts_per_case_and_route=1,
            warmups=0,
            cache="uncontrolled; identities checked outside timed execution",
            clocks="native engine clock separately; cross-engine comparison spans declaration through complete Python-row delivery",
            comparative_scope="different APIs, same logical source; prepared and common-source strata kept separate",
        ),
        held_out_scope="Fixed unseen combinations run on a separately seeded null-heavy shuffled input. This observation releases them for subsequent diagnosis.",
        scale_scope="Independent 5M surrogates for the twelve reported families; original SQL/data were unavailable.",
        pending_scope="V1 pending declarations remain historical requests outside the denominator. Compare actual v2 operator coverage separately.",
    )
    write_json(root / "design.json", record)
    print(
        json.dumps(
            {
                k: record[k]
                for k in (
                    "count",
                    "legacy_count",
                    "distinct_rendered_requests",
                    "profile_counts",
                    "surface_counts",
                    "source_counts",
                )
            }
        ),
        flush=True,
    )


def verify_legacy_parquet(old, dataset, manifest, guard):
    import pyarrow as pa
    import pyarrow.parquet as pq

    frozen = {f["path"]: f for f in old["frozen_files"]}
    for name, paths in old["sources"].items():
        parts = manifest["tables"][name]["files"]
        if len(paths) != len(parts):
            raise ValueError("legacy part count differs")
        for path, part in zip(paths, parts):
            if identity(Path(path)) != frozen[path]:
                raise ValueError("legacy Parquet generation or digest differs")
            with pa.memory_map(str(dataset / part["path"]), "r") as mem:
                ipc = pa.ipc.open_file(mem)
                pqf = pq.ParquetFile(path)
                if ipc.schema != pqf.schema_arrow:
                    raise ValueError("legacy input schema differs")
                # Row-group and IPC batching may differ. Compare complete values
                # with bounded pending batches and the declared physical order.
                stream = iter(pqf.iter_batches(batch_size=8192))
                actual = None
                offset = 0
                total = 0
                for index in range(ipc.num_record_batches):
                    wanted = ipc.get_batch(index)
                    i = 0
                    while i < wanted.num_rows:
                        if actual is None or offset == actual.num_rows:
                            actual = next(stream)
                            offset = 0
                        n = min(actual.num_rows - offset, wanted.num_rows - i)
                        if not actual.slice(offset, n).equals(wanted.slice(i, n)):
                            raise ValueError(f"legacy Parquet values differ at {name}/{total + i}")
                        offset += n
                        i += n
                    guard.check()
                    total += wanted.num_rows
                if (
                    total != part["rows"]
                    or (actual is not None and offset != actual.num_rows)
                    or next(stream, None) is not None
                ):
                    raise ValueError("legacy Parquet length differs")


def inputs(root, args, guard):
    contract = read(root / "design.json")
    legacy = Path(contract["legacy_inputs"]["path"])
    assert identity(legacy) == contract["legacy_inputs"]
    old = read(legacy)
    result = {}
    files = []
    for name in ("primary", *PROFILES):
        dataset = args.original.resolve() if name == "primary" else root / name / "dataset"
        spec, m, objects = validate_dataset(dataset)
        files += objects
        ipc = {t: [str(dataset / f["path"]) for f in item["files"]] for t, item in m["tables"].items()}
        if name == "primary":
            verify_legacy_parquet(old, dataset, m, guard)
            assert old["logical_identity"] == spec.identity
            parquet = old["sources"]
            p = args.prepared.resolve()
            v, proof = validate_prepared(p, dataset, m)
            vortex = v["sources"]
            files += v["inputs"] + proof
        else:
            assert (spec, m["layout"]) == PROFILES[name]
            p = root / name / "parquet/parquet.json"
            meta = read(p)
            files.append(identity(p))
            if meta["logical_identity"] != spec.identity or set(meta["files"]) != set(ipc):
                raise ValueError("Parquet manifest differs")
            parquet = {}
            for t, parts in meta["files"].items():
                if len(parts) != len(ipc[t]):
                    raise ValueError("Parquet part count differs")
                parquet[t] = []
                for f, original in zip(parts, m["tables"][t]["files"]):
                    if (
                        f["source_sha256"] != original["sha256"]
                        or f["checked_rows"] != original["rows"]
                        or identity(Path(f["path"])) != {k: f[k] for k in ("path", "sha256", "generation")}
                    ):
                        raise ValueError("Parquet readback receipt did not bind both sources")
                    parquet[t].append(f["path"])
            vortex = None
        result[name] = dict(
            dataset=str(dataset),
            spec=asdict(spec),
            logical_identity=spec.identity,
            layout=m["layout"],
            sources={"arrow-ipc": ipc, "parquet": parquet, "vortex": vortex},
        )
        files.extend(identity(Path(x)) for paths in parquet.values() for x in paths)
    native = {k: identity(getattr(args, k).resolve(strict=True)) for k in ("binary", "library")}
    write_json(
        root / "inputs.json",
        dict(
            design=identity(root / "design.json"),
            profiles=result,
            runtime=native,
            files=files,
            python=sys.version,
            platform=platform.platform(),
            machine=platform.machine(),
        ),
    )


def expectations(root, profile, guard):
    data = read(root / "inputs.json")
    design = read(root / "design.json")
    group = data["profiles"][profile]
    loc = root / ("reference-" + profile)
    loc.mkdir(exist_ok=True)
    dp = Path(group["dataset"])
    db_path = loc / "oracle.sqlite"
    meta_path = loc / "inputs.json"
    if not meta_path.exists():
        meta = build_database(db_path, dp, read(dp / "dataset.json"), ORACLE_COLUMNS, guard)
        write_json(meta_path, meta)
    else:
        assert read(meta_path)["database_sha256"] == file_hash(db_path)
    defs = {s.name: s for s in spaces()}
    output = loc / "expectations.sqlite"
    # Reuse only closed independent expectations bound to this exact design
    # and oracle database. Query-engine observations have a separate ledger.
    bindings = (
        file_hash(root / "design.json"),
        file_hash(meta_path),
        file_hash(Path(__file__).with_name("expanded_reference.py")),
    )
    originals = root / "reference-sources"
    originals.mkdir(exist_ok=True)
    snapshot = originals / (bindings[2] + ".py")
    if not snapshot.exists():
        with snapshot.open("xb") as stream:
            stream.write(Path(__file__).with_name("expanded_reference.py").read_bytes())
    assert file_hash(snapshot) == bindings[2]
    db = open_database(db_path)
    with closing(sqlite3.connect(output)) as dst, dst:
        dst.execute("CREATE TABLE IF NOT EXISTS binding (design TEXT, input TEXT, oracle TEXT)")
        rows = dst.execute("SELECT * FROM binding").fetchall()
        if not rows:
            dst.execute("INSERT INTO binding VALUES (?,?,?)", bindings)
        elif rows != [bindings]:
            raise ValueError("reference checkpoint bindings changed")
        dst.execute(
            "CREATE TABLE IF NOT EXISTS expected (id TEXT PRIMARY KEY, identity TEXT NOT NULL, value BLOB NOT NULL)"
        )
        existing = {r[0] for r in dst.execute("SELECT id FROM expected")}
        requested = [r for r in design["cases"] if r["profile"] == profile]
        for i, r in enumerate(requested):
            if r["id"] in existing:
                continue
            if "reference" in r:
                source = Path(r["reference"])
                if file_hash(source) != r["reference_sha256"]:
                    raise ValueError("frozen independent reference changed")
                value = read(source)
                assert value["status"] == "passed" and value["case_identity"] == r["case_identity"]
                payload = value["reference"]
                result_from_record(payload)
            else:
                item = build(r, defs)
                assert item.identity == r["case_identity"]
                started = time.monotonic()
                sampled = started
                stopped = []

                def check_reference():
                    nonlocal sampled
                    now = time.monotonic()
                    if now - started > 300:
                        stopped.append("independent reference exceeded 300 seconds")
                        return 1
                    if now - sampled >= 2:
                        sampled = now
                        try:
                            guard.check()
                        except (ValueError, OSError) as error:
                            stopped.append(str(error))
                            return 1
                    return 0

                db.set_progress_handler(check_reference, 250_000)
                try:
                    payload = result_record(expected(db, item))
                except sqlite3.OperationalError as error:
                    raise RuntimeError(
                        f"independent reference failed at {item.id}: {stopped or str(error)}"
                    ) from error
                finally:
                    db.set_progress_handler(None, 0)
            dst.execute("INSERT INTO expected VALUES (?,?,?)", (r["id"], r["case_identity"], pack(payload)))
            if i % 25 == 0 or profile == "scale":
                dst.commit()
                guard.check()
            if (i + 1) % (5 if profile == "scale" else 250) == 0:
                print(
                    json.dumps(
                        dict(
                            stage="independent_expectations",
                            profile=profile,
                            completed=i + 1,
                            total=len(requested),
                        )
                    ),
                    flush=True,
                )
        dst.commit()
        if set(dst.execute("SELECT id,identity FROM expected")) != {
            (r["id"], r["case_identity"]) for r in requested
        }:
            raise ValueError("expectation coverage differs from design")
    db.close()
    write_json(
        loc / "complete.json",
        dict(status="passed", count=len(requested), expectations=identity(output), bindings=bindings),
    )


def seal(root, runtime_proof=None):
    design = read(root / "design.json")
    data = read(root / "inputs.json")
    assert identity(root / "design.json") == data["design"] and design["count"] == TOTAL
    refs = []
    for name in data["profiles"]:
        receipt = root / ("reference-" + name) / "complete.json"
        r = read(receipt)
        assert r["status"] == "passed" and r["count"] == design["profile_counts"][name]
        assert identity(Path(r["expectations"]["path"])) == r["expectations"]
        snapshot = root / "reference-sources" / (r["bindings"][2] + ".py")
        if file_hash(snapshot) != r["bindings"][2]:
            raise ValueError("independent oracle snapshot differs")
        refs.extend([identity(receipt), r["expectations"], identity(snapshot)])
    source = [identity(p) for p in sorted(Path(__file__).parent.glob("*.py"))]
    source.extend(identity(p) for p in sorted((ROOT / "python/src/shardloom").rglob("*.py")))
    for rel in (
        "benchmarks/traditional_analytics/resources.py",
        "scripts/local_uat_storage.py",
        "scripts/run_clickbench_query_uat.py",
        "scripts/timed_native_command.py",
        "scripts/query_timing_report.py",
    ):
        source.append(identity(ROOT / rel))
    import polars, pyarrow

    if polars.__version__ != "2.1.0" or pyarrow.__version__ != "25.0.1":
        raise ValueError("expanded v2 requires its pinned comparison environment")
    environment = [identity(Path(sys.executable).resolve())]
    distributions = {}
    for dist in metadata.distributions():
        name = dist.metadata["Name"]
        if name not in {"polars", "pyarrow"} and not name.startswith("polars-runtime"):
            continue
        distributions[name] = dist.version
        for member in dist.files or ():
            path = Path(dist.locate_file(member))
            if path.name == "RECORD" or path.suffix in {".py", ".so", ".dylib", ".pyd", ".dll"}:
                environment.append(identity(path.resolve(strict=True)))
    provenance = None
    if runtime_proof is not None:
        proof = read(runtime_proof)
        for field, kind in (("cli", "binary"), ("native_library", "library")):
            product = proof["frozen_input_identities"][field]
            observed = data["runtime"][kind]
            if (
                product["sha256"] != observed["sha256"]
                or product["bytes"] != observed["generation"]["bytes"]
                or proof["status"] != "passed_local_staging_no_publication"
            ):
                raise ValueError("release build identity differs from the selected runtime")
        environment.append(identity(runtime_proof))
        provenance = dict(
            version=proof["version"],
            release_source=proof["release_source_commit"],
            binary_build_source=proof["package_build_source_commit"],
            receipt=identity(runtime_proof),
        )
    hardware = {}
    if sys.platform == "darwin":
        for name in ("hw.model", "hw.physicalcpu", "hw.logicalcpu", "hw.memsize", "machdep.cpu.brand_string"):
            hardware[name] = subprocess.check_output(["/usr/sbin/sysctl", "-n", name], text=True).strip()

    write_json(
        root / "frozen.json",
        dict(
            status="frozen_before_comparison",
            corpus=VERSION,
            total=TOTAL,
            design=data["design"],
            inputs=identity(root / "inputs.json"),
            references=refs,
            harness=source,
            runtime_provenance=provenance,
            environment_files=environment,
            distributions=distributions,
            hardware=hardware,
            source_revision=subprocess.check_output(
                ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
            ).strip(),
            polars=polars.__version__,
            pyarrow=pyarrow.__version__,
            sqlite=sqlite3.sqlite_version,
        ),
    )


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("stage", choices=["design", "inputs", "expectations", "seal"])
    p.add_argument("--root", required=True, type=Path)
    for name in ("legacy", "original", "prepared", "binary", "library"):
        p.add_argument("--" + name, type=Path)
    p.add_argument("--profile", choices=["primary", *PROFILES])
    p.add_argument(
        "--runtime-proof",
        type=Path,
        help="ShardLoom release staging receipt for the selected CLI and extension",
    )
    args = p.parse_args()
    if not __debug__:
        p.error("freeze requires Python assertion checks enabled")
    required = {
        "design": ("legacy",),
        "inputs": ("original", "prepared", "binary", "library"),
        "expectations": ("profile",),
        "seal": (),
    }[args.stage]
    if any(getattr(args, name) is None for name in required):
        p.error(f"{args.stage} requires " + ", ".join("--" + name for name in required))
    root = require_local_path(args.root, Path.home(), sys.platform)
    if args.stage == "design":
        return design(root, args.legacy)
    if args.stage == "seal":
        return seal(root, args.runtime_proof)
    with BenchmarkGuard(root, binary=Path("shardloom"), memory_gb=2) as guard:
        if args.stage == "inputs":
            return inputs(root, args, guard)
        if args.profile is None:
            p.error("expectations requires --profile")
        expectations(root, args.profile, guard)


if __name__ == "__main__":
    main()
