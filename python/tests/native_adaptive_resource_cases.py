# SPDX-License-Identifier: Apache-2.0
"""Complete public file/translation results under two unchanged caller grants.

Owner-level tests force shrink/regrowth, denial, cancellation and cleanup. This
matrix proves that real public routes preserve the grants and complete values;
it does not infer allocator/RSS coverage or adaptation from a small fixture.
"""

import csv
import json
import os
from pathlib import Path
import tempfile
from unittest.mock import patch

FORMATS = ("vortex", "parquet", "arrow-ipc", "avro", "orc", "csv", "tsv", "json", "jsonl")
OUTPUTS = tuple(name for name in FORMATS if name != "tsv")
SURFACES = ("dataframe", "typed_relation", "python_sql", "embedded_workflow", "cli_sql")


def checks(memory, lanes):
    return {
        "fallback_attempted": "false",
        "external_engine_invoked": "false",
        "execution_resource_declared_memory_bytes": str(memory),
        "execution_resource_declared_max_parallelism": str(lanes),
        "local_primitive_native_io_decision_schema_version": "shardloom.native_io_decisions.v1",
    }


def fixtures(test, root):
    import pyarrow.parquet as parquet
    import pyarrow.orc as orc

    rows = [{"n": index - 128, "measure": None if index % 7 == 0 else -3 * index,
             "payload": f'row {index}, "λ" ' + "wide-" * (index % 43)}
            for index in range(257)]
    # Break key adjacency independently of the engine's ordering implementation.
    source_rows = rows[1::2] + list(reversed(rows[::2]))
    schema = test.pa.schema([("n", test.pa.int64()), ("measure", test.pa.int64()),
                             ("payload", test.pa.string())])
    table = test.pa.Table.from_pylist(source_rows, schema=schema)
    paths = {name: root / ("input.arrow" if name == "arrow-ipc" else f"input.{name}")
             for name in FORMATS}
    parquet.write_table(table, paths["parquet"], row_group_size=17, use_dictionary=True)
    orc.write_table(table, paths["orc"])
    with paths["arrow-ipc"].open("wb") as stream:
        with test.pa.ipc.new_file(stream, schema) as writer:
            writer.write_table(table, max_chunksize=17)
    for name, delimiter in (("csv", ","), ("tsv", "\t")):
        with paths[name].open("w", newline="", encoding="utf-8") as stream:
            writer = csv.DictWriter(stream, fieldnames=schema.names, delimiter=delimiter)
            writer.writeheader()
            writer.writerows(source_rows)
    paths["json"].write_text(json.dumps(source_rows, ensure_ascii=False), encoding="utf-8")
    paths["jsonl"].write_text("\n".join(json.dumps(row, ensure_ascii=False) for row in source_rows)
                              + "\n", encoding="utf-8")
    owner = test.native.Session(test.native.ExecutionResources(128 << 20, 2))
    try:
        plan = owner.sql(f"SELECT * FROM '{paths['arrow-ipc']}'")
        for name in ("vortex", "avro"):
            report = plan.write(paths[name], f"write_{name}")
            test.assertEqual(report["fallback_attempted"], "false")
        del plan
    finally:
        owner.close()
    return paths, rows


def declaration(test, sl, client, path, peer=None):
    frame = sl.read(path, client=client)
    relation = test.native.Relation.scan(str(path))
    sql = f"SELECT n,measure,payload FROM '{path}'"
    if peer is not None:
        frame = frame.union_all(sl.read(peer, client=client))
        relation = relation.set_operation(test.native.Relation.scan(str(peer)), kind="union_all")
        sql = f"SELECT * FROM ({sql} UNION ALL SELECT n,measure,payload FROM '{peer}') AS combined"
    frame = frame.filter("n >= -3").select("n", "measure", "payload").sort("n")
    E = test.native.Expression
    relation = relation.filter(E.column("n") >= E.literal(-3)).project(
        [(name, E.column(name)) for name in ("n", "measure", "payload")]).sort([("n", False, None)])
    return frame, relation, sql + " WHERE n >= -3 ORDER BY n"


def write_matrix(test, root, paths, wanted, memory, lanes, owner, client):
    import shardloom as sl

    frame, relation, statement = declaration(test, sl, client, paths["parquet"], paths["arrow-ipc"])
    expected = sorted(wanted * 2, key=lambda row: row["n"])
    arguments = ["sql", "--sql", statement, "--request", "collect", "--bounded", "true",
                 "--memory-bytes", str(memory), "--max-parallelism", str(lanes)]
    for name in OUTPUTS:
        action = "write_" + name.replace("-", "_")
        for surface in SURFACES:
            destination = root / f"result-{memory}-{surface}.{name}"
            with test.subTest(memory=memory, lanes=lanes, output=name, surface=surface):
                if surface == "dataframe":
                    fields = frame.write(destination, output_format=name, check=True).envelope.field_map
                elif surface == "cli_sql":
                    with patch("subprocess.Popen", test.real_popen):
                        cli = sl.ShardLoomClient(binary=os.environ.get("SHARDLOOM_SURFACE_TEST_BINARY"),
                            transport="subprocess", use_persistent_worker=False,
                            memory_bytes=memory, max_parallelism=lanes, timeout=30)
                        try:
                            fields = sl.sql(statement, client=cli).write(
                                destination, output_format=name, check=True).envelope.field_map
                        finally:
                            cli.close()
                else:
                    plan = (owner.plan(relation) if surface == "typed_relation" else
                            owner.sql(statement) if surface == "python_sql" else owner.workflow(arguments))
                    fields = plan.write(destination, action)
                    del plan
                for key, value in checks(memory, lanes).items():
                    test.assertEqual(fields[key], value, (surface, name, key))
                reopened = owner.sql(f"SELECT n,measure,payload FROM '{destination}'",
                    files=[(str(destination), name, None)]).collect()
                table = test.table(reopened)
                test.assertEqual(table.to_pylist(), expected)
                test.assertEqual(table.schema.names, ["n", "measure", "payload"])
                del table, reopened
                destination.unlink()


def check_matrix(test):
    import shardloom as sl

    with tempfile.TemporaryDirectory(prefix="shardloom-adaptive-public-") as directory:
        root = Path(directory)
        paths, rows = fixtures(test, root)
        wanted = [row for row in rows if row["n"] >= -3]
        for memory, lanes in ((16 << 20, 1), (128 << 20, 2)):
            owner = test.native.Session(test.native.ExecutionResources(memory, lanes))
            client = sl.ShardLoomClient(transport="native", memory_bytes=memory, max_parallelism=lanes)
            try:
                for name, path in paths.items():
                    for mixed in (False, True):
                        frame, relation, statement = declaration(test, sl, client, path,
                            paths["vortex"] if mixed else None)
                        expected = sorted(wanted * (2 if mixed else 1), key=lambda row: row["n"])
                        with test.subTest(memory=memory, lanes=lanes, input=name, mixed=mixed):
                            tables = test.all_surfaces(frame, relation, statement, checks(memory, lanes),
                                file_binding=(path, name), execution_session=owner, resources=(memory, lanes))
                            test.assertEqual(len(tables), 5)
                            for table in tables:
                                test.assertEqual(table.to_pylist(), expected)
                                test.assertEqual(table.schema, tables[0].schema)
                            del table, tables
                        del frame, relation
                write_matrix(test, root, paths, wanted, memory, lanes, owner, client)
                snapshot = owner.snapshot()
                test.assertEqual(snapshot["memory_limit_bytes"], memory)
                test.assertEqual(snapshot["admitted_max_parallelism"], lanes)
                test.assertLessEqual(snapshot["peak_reserved_bytes"], memory)
            finally:
                owner.close()
                client.close()
            test.assertEqual(owner.snapshot()["reserved_bytes"], 0)
