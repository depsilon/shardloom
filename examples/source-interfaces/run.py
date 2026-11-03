#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Run one small local workflow through all five source-build interfaces.

Use a matching native Python installation and CLI from the same source revision:
    python examples/source-interfaces/run.py --binary /absolute/path/to/shardloom

The example creates and removes its own small files. It is a correctness example,
not a benchmark. The 128-MiB allocation and two lanes are illustrative choices.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import subprocess
from tempfile import TemporaryDirectory

import shardloom as sl
from shardloom import native


EXPECTED = ({"id": 2}, {"id": 3})
MEMORY_BYTES = 128 << 20
LANES = 2


def verify_rows(name, rows):
    if tuple(rows) != EXPECTED:
        raise AssertionError(f"{name}: expected {EXPECTED!r}, got {rows!r}")


def verify(name, rows, fields):
    verify_rows(name, rows)
    for key in ("fallback_attempted", "external_engine_invoked"):
        if fields.get(key) != "false":
            raise AssertionError(f"{name}: missing no-fallback evidence for {key}")


def run(binary: Path) -> None:
    with TemporaryDirectory(prefix="shardloom-source-interfaces-") as directory:
        root = Path(directory)
        source = root / "orders.csv"
        source.write_text("id,amount\n1,8\n2,15\n3,21\n", encoding="utf-8")
        schema = {"id": "int64", "amount": "int64"}
        statement = f"SELECT id FROM '{source}' WHERE amount >= 10 ORDER BY id"

        with sl.context(transport="native", memory_bytes=MEMORY_BYTES,
                        max_parallelism=LANES) as ctx:
            orders = ctx.read_csv(source, schema=schema)
            selected = orders.filter(sl.col("amount") >= 10).select("id").sort("id")
            result = selected.collect()
            verify("DataFrame", result.to_rows(), result.details.field_map)

            sql = ctx.sql(
                "SELECT id FROM orders WHERE amount >= :minimum ORDER BY id",
                relations={"orders": orders}, parameters={"minimum": 10},
            )
            result = sql.collect()
            verify("Python SQL", result.to_rows(), result.details.field_map)

            # One action supplies both destinations. Each file is reopened below.
            outputs = {"vortex": root / "selected.vortex", "jsonl": root / "selected.jsonl"}
            written = selected.fanout(outputs)
            if written.output_commit_status != "committed":
                raise AssertionError("multi-output publication did not complete")
            if written.details.field_map.get("native_vortex_result_export_execution_count") != "1":
                raise AssertionError("multi-output action did not record one execution")
            for kind, path in outputs.items():
                reopened = ctx.read(path, input_format=kind).sort("id").collect()
                verify(f"reopened {kind}", reopened.to_rows(), reopened.details.field_map)

            # Ordinary reads above use the CSV directly. Durable preparation is explicit.
            prepared = root / "orders.vortex"
            ctx.prepare_vortex(source, prepared)
            result = (ctx.read_vortex(prepared).filter(sl.col("amount") >= 10)
                      .select("id").sort("id").collect())
            verify("explicitly prepared source", result.to_rows(), result.details.field_map)

        session = native.Session(native.ExecutionResources(MEMORY_BYTES, LANES))
        arguments = ["sql", "--sql", statement, "--source-bindings", json.dumps({
            str(source): {"input_format": "csv", "source_schema": "id:int64,amount:int64"},
        }), "--request", "collect", "--bounded", "true",
            "--memory-bytes", str(MEMORY_BYTES), "--max-parallelism", str(LANES)]
        try:
            relation = (native.Relation.scan(str(source))
                        .filter(native.col("amount") >= native.lit(10))
                        .project([("id", native.col("id"))])
                        .sort([("id", False, "last")]))
            result = session.plan(
                relation, files=[(str(source), "csv", "id:int64,amount:int64")],
            ).collect()
            # Explicit Python-row export for this tiny correctness example.
            verify("typed native", result.row_export()[0], result.report)

            result = session.workflow(arguments).collect()
            verify("embedded workflow", result.row_export()[0], result.report)
        finally:
            session.close()

        completed = subprocess.run(
            [str(binary), "run", *arguments, "--format", "json"],
            check=True, capture_output=True, text=True, timeout=30,
        )
        envelope = json.loads(completed.stdout)
        if envelope["status"] != "success":
            raise AssertionError(envelope)
        # The CLI carries its fallback contract in the JSON envelope.
        fallback = envelope["fallback"]
        if (fallback["attempted"] is not False or fallback["allowed"] is not False
                or fallback["engine"] is not None):
            raise AssertionError(f"CLI SQL: invalid fallback contract: {fallback!r}")
        fields = {row["key"]: row["value"] for row in envelope["result"]["fields"]}
        if fields.get("result_payload_complete") != "true":
            raise AssertionError("CLI did not return a complete result")
        rows = [json.loads(line) for line in fields["result_jsonl"].splitlines()]
        verify_rows("CLI SQL", rows)

    print(json.dumps({"status": "passed", "interfaces": ["DataFrame", "Python SQL",
        "typed native", "embedded workflow", "CLI SQL"], "rows": EXPECTED,
        "multi_output_readbacks": 2, "explicit_preparation_readback": True}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    run(parser.parse_args().binary.resolve(strict=True))
