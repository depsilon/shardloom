# SPDX-License-Identifier: Apache-2.0
"""Create the additional frozen input populations, without running either engine."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / "scripts"), str(ROOT / "python/src"), str(Path(__file__).resolve().parents[1])]
from local_uat_storage import require_local_path
from resources import BenchmarkGuard
from query_surface.datasets import DatasetSpec, write_dataset
from query_surface.evidence import identity, validate_dataset
from query_surface.run import write_json


PROFILES = {
    "smoke": (
        DatasetSpec(
            seed=20261103,
            fact_rows=257,
            entity_rows=31,
            product_rows=11,
            relationship_rows=79,
            calendar_rows=37,
        ),
        "sorted",
    ),
    "holdout": (
        DatasetSpec(
            seed=20261104,
            fact_rows=100_003,
            entity_rows=12_101,
            product_rows=997,
            relationship_rows=24_117,
            profile="null_heavy",
        ),
        "shuffled",
    ),
    "scale": (
        DatasetSpec(
            seed=20261105,
            fact_rows=5_000_000,
            entity_rows=500_000,
            product_rows=50_000,
            relationship_rows=1_000_000,
            profile="skewed",
        ),
        "sorted",
    ),
}


def parquet_sources(dataset, manifest, destination, guard, *, resume=False):
    """Independently read every written row group against its input record batch."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    destination.mkdir(exist_ok=resume)
    if (destination / "parquet.json").exists():
        raise FileExistsError("validated Parquet dataset already exists")
    files = {}
    for name, item in manifest["tables"].items():
        files[name] = []
        for part in item["files"]:
            source = dataset / part["path"]
            path = destination / (source.stem + ".parquet")
            if path.exists() and not resume:
                raise ValueError("output already exists")
            # Unbuffered Arrow file reads release earlier batches. An mmap of
            # the 5M fact fixture can keep its entire scanned range resident.
            with pa.OSFile(str(source), "rb") as handle:
                batch_file = pa.ipc.open_file(handle)
                if not path.exists():
                    with pq.ParquetWriter(path, batch_file.schema, compression="zstd") as writer:
                        for index in range(batch_file.num_record_batches):
                            guard.check()
                            writer.write_batch(batch_file.get_batch(index))
                opened = pq.ParquetFile(path)
                if (
                    opened.schema_arrow != batch_file.schema
                    or opened.num_row_groups != batch_file.num_record_batches
                ):
                    raise ValueError("Parquet conversion changed schema or batch boundaries")
                checked = 0
                for index in range(opened.num_row_groups):
                    guard.check()
                    expected = pa.Table.from_batches([batch_file.get_batch(index)])
                    if not opened.read_row_group(index).equals(expected):
                        raise ValueError(f"Parquet values differ at {name}/{index}")
                    checked += expected.num_rows
                if checked != part["rows"]:
                    raise ValueError("Parquet conversion changed input size")
            files[name].append({"source_sha256": part["sha256"], "checked_rows": checked, **identity(path)})
    write_json(
        destination / "parquet.json", {"logical_identity": manifest["logical_identity"], "files": files}
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", required=True, type=Path)
    parser.add_argument("--original", required=True, type=Path)
    parser.add_argument("--profiles", nargs="+", choices=PROFILES, default=list(PROFILES))
    parser.add_argument(
        "--resume-conversion",
        action="store_true",
        help="Reopen a complete Arrow dataset, verify existing Parquet parts and write missing ones",
    )
    args = parser.parse_args()
    root = require_local_path(args.root, Path.home(), sys.platform)
    with BenchmarkGuard(root, binary=Path("shardloom"), memory_gb=2) as guard:
        for name in args.profiles:
            spec, layout = PROFILES[name]
            current = root / name
            emit = lambda event: print(json.dumps({"profile": name, **event}), flush=True)
            if not args.resume_conversion:
                current.mkdir(exist_ok=False)
                write_dataset(current / "dataset", spec, layout, guard=guard.check, progress=emit)
            checked, manifest, _ = validate_dataset(current / "dataset")
            if checked != spec or manifest["layout"] != layout:
                raise ValueError("dataset differs from the declared population")
            parquet_sources(
                current / "dataset", manifest, current / "parquet", guard, resume=args.resume_conversion
            )
        # The original observations already have verified Parquet and Vortex
        # manifests. Those immutable paths are explicitly admitted at freeze.
        validate_dataset(args.original)


if __name__ == "__main__":
    main()
