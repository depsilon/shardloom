# SPDX-License-Identifier: Apache-2.0
"""Complete local table queries through the five real native public routes."""
from __future__ import annotations

from contextlib import contextmanager
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest
from unittest.mock import patch

if __package__:
    from .test_native_binding import _load_native
    from .native_table_type_fixtures import make_typed_table
    from .native_table_fixtures import (
        Fixture, delta_add, delta_field, delta_metadata, id_field, iceberg_entry,
        iceberg_list, iceberg_manifest, iceberg_schema, uri,
    )
else:
    from test_native_binding import _load_native
    from native_table_type_fixtures import make_typed_table
    from native_table_fixtures import (
        Fixture, delta_add, delta_field, delta_metadata, id_field, iceberg_entry,
        iceberg_list, iceberg_manifest, iceberg_schema, uri,
    )


OLD = [{"id": 1, "label": "alpha"}, {"id": 2, "label": "deleted-key"},
       {"id": 3, "label": "deleted-position"}, {"id": 4, "label": None}]
LATEST = [{"id": 1, "label": "alpha"}, {"id": 2, "label": "new-two"},
          {"id": 4, "label": None}, {"id": 5, "label": "omega"}]


def hudi_commit(fixture, begin, completed, paths, schema):
    wire = {"type": "record", "name": "HoodieCommitMetadata", "fields": [
        {"name": "partitionToWriteStats", "type": ["null", {"type": "map", "values": {
            "type": "array", "items": {"type": "record", "name": "HoodieWriteStat", "fields": [
                {"name": "fileId", "type": ["null", "string"], "default": None},
                {"name": "path", "type": ["null", "string"], "default": None}]}}}], "default": None},
        {"name": "compacted", "type": "boolean", "default": False},
        {"name": "extraMetadata", "type": ["null", {"type": "map", "values": ["null", "string"]}], "default": None},
        {"name": "version", "type": "int", "default": 1},
        {"name": "operationType", "type": ["null", "string"], "default": None}]}
    return fixture.avro(f".hoodie/timeline/{begin}_{completed}.commit", wire, [{
        "partitionToWriteStats": {"": [{"fileId": group, "path": path} for group, path in paths]},
        "compacted": False, "extraMetadata": {"schema": json.dumps(schema)},
        "version": 1, "operationType": "UPSERT",
    }])


def make_table(root, kind, pa):
    fixture = Fixture(root)
    fields = [pa.field("id", pa.int64(), nullable=False), pa.field("label", pa.string())]

    def data(name, rows, *, renamed=False):
        current_fields = fields
        if kind == "iceberg":
            current_fields = [id_field("id", pa.int64(), False, 1),
                              id_field("old_label" if renamed else "label", pa.string(), True, 2)]
        return fixture.parquet(name, current_fields, [
            pa.array([row["id"] for row in rows], type=pa.int64()),
            pa.array([row["label"] for row in rows], type=pa.string())])

    old_name = "a_0-0-0_20261101000100.parquet" if kind == "hudi" else "a.parquet"
    new_name = "a_0-0-0_20261101000200.parquet" if kind == "hudi" else "a-current.parquet"
    b_name = "b_0-0-0_20261101000200.parquet" if kind == "hudi" else "b.parquet"
    old = data(old_name, OLD, renamed=True)
    current = data(new_name, [LATEST[0], LATEST[2]])
    other = data(b_name, [LATEST[1], LATEST[3]])
    data("uncommitted.parquet", [{"id": 999, "label": "orphan"}])
    if kind == "delta":
        metadata = delta_metadata([delta_field("id", "long", False),
                                   delta_field("label", "string", True)], [], "none")
        fixture.commit(0, [{"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}},
                           metadata, delta_add(old, old_name, {})])
        control = fixture.commit(1, [{"remove": {"path": old_name}},
                                    delta_add(current, new_name, {}), delta_add(other, b_name, {})])
        return root, {"version": 0}, "1", current, control
    if kind == "hudi":
        schema = {"type": "record", "name": "Row", "fields": [
            {"name": "id", "type": "long"}, {"name": "label", "type": ["null", "string"], "default": None}]}
        config = root / ".hoodie/hoodie.properties"
        config.parent.mkdir()
        config.write_text("hoodie.table.name=fixture\nhoodie.table.type=COPY_ON_WRITE\n"
                          "hoodie.table.version=9\nhoodie.timeline.layout.version=2\n"
                          "hoodie.table.create.schema=" + json.dumps(schema) + "\n")
        hudi_commit(fixture, "20261101000100", "20261101000300", [("a", old_name)], schema)
        control = hudi_commit(fixture, "20261101000200", "20261101000400",
                              [("a", new_name), ("b", b_name)], schema)
        return root, {"instant": "20261101000100"}, "completion:20261101000400", current, control
    logical = iceberg_schema([{"id": 1, "name": "id", "required": True, "type": "long"},
                              {"id": 2, "name": "label", "required": False, "type": "string"}], 0)
    equality = fixture.parquet("equality.parquet", [id_field("key", pa.int64(), False, 1)],
                               [pa.array([2], type=pa.int64())])
    position = fixture.parquet("position.parquet", [
        id_field("file_path", pa.string(), False, 2147483546),
        id_field("pos", pa.int64(), False, 2147483545)],
        [pa.array([uri(old)]), pa.array([2], type=pa.int64())])
    a_manifest = iceberg_manifest(fixture, "a.avro", logical, "data",
                                  [iceberg_entry(old, 4, 0, None, None)])
    b_manifest = iceberg_manifest(fixture, "b.avro", logical, "data",
                                  [iceberg_entry(other, 2, 0, None, None)])
    deletes = iceberg_manifest(fixture, "deletes.avro", logical, "deletes", [
        iceberg_entry(equality, 1, 2, None, [1]), iceberg_entry(position, 1, 1, None, None)])
    before = iceberg_list(fixture, "before.avro", [(a_manifest, 0, 1)])
    after = iceberg_list(fixture, "after.avro", [(a_manifest, 0, 1), (b_manifest, 0, 2), (deletes, 1, 2)])
    metadata = fixture.json("metadata.json", {
        "format-version": 2, "location": uri(root), "current-schema-id": 0,
        "current-snapshot-id": 2, "schemas": [logical], "partition-specs": [{"spec-id": 0, "fields": []}],
        "snapshots": [{"snapshot-id": 1, "schema-id": 0, "sequence-number": 1, "manifest-list": uri(before)},
                      {"snapshot-id": 2, "schema-id": 0, "sequence-number": 2, "manifest-list": uri(after)}]})
    return metadata, {"snapshot_id": 1}, "2", old, equality


class NativeTableBindingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.native = _load_native()
        import pyarrow as pa
        cls.pa = pa

    @contextmanager
    def owners(self, memory=64 << 20):
        import shardloom as sl
        client = sl.ShardLoomClient(transport="native", memory_bytes=memory, max_parallelism=2)
        session = self.native.Session(self.native.ExecutionResources(memory, 2))
        try:
            yield client, session
        finally:
            client.close()
            session.close()

    def checked(self, table, fields, kind, snapshot):
        self.assertEqual(fields["fallback_attempted"], "false")
        self.assertEqual(fields["external_engine_invoked"], "false")
        self.assertEqual(fields["native_source_metadata_count_sources"], "0")
        observed = json.loads(fields["native_source_table_snapshots"])
        self.assertTrue(observed)
        self.assertEqual({row["format"] for row in observed}, {kind})
        self.assertEqual({row["snapshot"] for row in observed}, {snapshot})
        self.assertEqual(fields["native_source_demand_formats"], "parquet")
        return table

    def all_surfaces(self, frame, relation, sql, path, kind, selection, snapshot, session, memory,
                     *, extra_bindings=None, files=()):
        import shardloom as sl
        from shardloom.query import Result
        bindings = {str(path): {"input_format": kind, "table_snapshot": selection}}
        bindings.update(extra_bindings or {})
        tables = [(str(path), kind, selection.get("snapshot_id"), selection.get("version"), selection.get("instant"))]
        arguments = ["sql", "--sql", sql, "--source-bindings", json.dumps(bindings),
                     "--request", "collect", "--bounded", "true",
                     "--memory-bytes", str(memory), "--max-parallelism", "2"]
        output = []
        with patch.object(subprocess, "Popen", side_effect=AssertionError("external process")):
            result = frame.collect(check=True)
            output.append(self.checked(result.to_arrow(), result.envelope.field_map, kind, snapshot))
            for plan in (session.plan(relation, tables=tables, files=list(files)),
                         session.sql(sql, tables=tables, files=list(files)),
                         session.workflow(arguments)):
                result = plan.collect()
                table = self.pa.RecordBatchReader.from_stream(result).read_all()
                output.append(self.checked(table, result.report, kind, snapshot))
        cli = sl.ShardLoomClient(binary=os.environ.get("SHARDLOOM_SURFACE_TEST_BINARY"),
                                transport="subprocess", use_persistent_worker=False,
                                memory_bytes=memory, max_parallelism=2, timeout=30)
        try:
            envelope = cli.public_workflow_run(
                "sql", sql_statement=sql, source_bindings=bindings, bounded=True,
                memory_bytes=memory, max_parallelism=2, check=True).envelope
            table = Result(workflow=frame, operation="collect", envelope=envelope).to_arrow()
            output.append(self.checked(table, envelope.field_map, kind, snapshot))
        finally:
            cli.close()
        self.assertEqual(len(output), 5)
        for table in output[1:]:
            self.assertEqual(table.schema, output[0].schema)
        return output

    def test_snapshot_visibility_composes_across_all_five_interfaces(self):
        import shardloom as sl
        with tempfile.TemporaryDirectory(prefix="shardloom-table-public-") as directory:
            for kind in ("iceberg", "delta", "hudi"):
                path, historical, snapshot, _, _ = make_table(Path(directory) / kind, kind, self.pa)
                ordinary = Fixture(Path(directory) / f"{kind}-ordinary").parquet("ordinary.parquet", [
                    self.pa.field("id", self.pa.int64(), nullable=False),
                    self.pa.field("weight", self.pa.int64(), nullable=False)], [
                    self.pa.array([2, 2, 5, 9], type=self.pa.int64()),
                    self.pa.array([10, 11, 50, 90], type=self.pa.int64())])
                for memory in (16 << 20, 128 << 20):
                    with self.owners(memory) as (client, session):
                        frame = sl.read(path, input_format=kind, client=client)
                        relation = self.native.Relation.scan(str(path))
                        col = self.native.Expression.column
                        lit = self.native.Expression.literal
                        cases = [
                            ("projection", frame.sort("id"), relation.sort([("id", False, "last")]),
                             f"SELECT * FROM '{path}' ORDER BY id", LATEST),
                            ("count", frame.agg("COUNT(*) AS total"),
                             relation.aggregate([], [("count", None, "total")]),
                             f"SELECT COUNT(*) AS total FROM '{path}'", [{"total": 4}]),
                            ("hidden_delete_key", frame.select("label").limit(1),
                             relation.project([("label", col("label"))]).limit(1),
                             f"SELECT label FROM '{path}' LIMIT 1", [{"label": "alpha"}]),
                            ("filter", frame.filter("id >= 2 AND label IS NOT NULL").sort("id"),
                             relation.filter((col("id") >= lit(2)) & col("label").is_not_null()).sort([("id", False, "last")]),
                             f"SELECT * FROM '{path}' WHERE id >= 2 AND label IS NOT NULL ORDER BY id",
                             [LATEST[1], LATEST[3]]),
                            ("grouped", frame.group_by("label").agg("COUNT(*) AS total").sort("label", nulls="last"),
                             relation.aggregate(["label"], [("count", None, "total")]).sort([("label", False, "last")]),
                             f"SELECT label,COUNT(*) AS total FROM '{path}' GROUP BY label ORDER BY label NULLS LAST",
                             [{"label": label, "total": 1} for label in ("alpha", "new-two", "omega", None)]),
                            ("join", frame.join(frame, on="id", how="inner").select(
                                "f.id AS id", "d.label AS label").sort("id"),
                             relation.join(relation, [("id", "id")],
                                [("left", "id", "id"), ("right", "label", "label")], kind="inner").sort([("id", False, "last")]),
                             f"SELECT f.id AS id,d.label AS label FROM '{path}' AS f INNER JOIN '{path}' AS d ON f.id=d.id ORDER BY id",
                             LATEST),
                        ]
                        for name, query, plan, statement, expected in cases:
                            with self.subTest(format=kind, memory=memory, query=name):
                                for actual in self.all_surfaces(query, plan, statement, path, kind, {},
                                                                snapshot, session, memory):
                                    self.assertEqual(actual.to_pylist(), expected)
                        old = sl.read(path, input_format=kind, client=client, **historical).sort("id")
                        old_snapshot = {"iceberg": "1", "delta": "0", "hudi": "completion:20261101000300"}[kind]
                        for actual in self.all_surfaces(old, relation.sort([("id", False, "last")]),
                                f"SELECT * FROM '{path}' ORDER BY id", path, kind, historical,
                                old_snapshot, session, memory):
                            self.assertEqual(actual.to_pylist(), OLD)
                        other = sl.read(ordinary, input_format="parquet", client=client)
                        mixed = frame.join(other, on="id", how="inner").select(
                            "f.id AS id", "f.label AS label", "d.weight AS weight").sort("id", "weight")
                        mixed_plan = relation.join(self.native.Relation.scan(str(ordinary)), [("id", "id")],
                            [("left", "id", "id"), ("left", "label", "label"), ("right", "weight", "weight")],
                            kind="inner").sort([("id", False, "last"), ("weight", False, "last")])
                        mixed_sql = (f"SELECT f.id AS id,f.label AS label,d.weight AS weight FROM '{path}' AS f "
                                     f"INNER JOIN '{ordinary}' AS d ON f.id=d.id ORDER BY id,weight")
                        for actual in self.all_surfaces(mixed, mixed_plan, mixed_sql, path, kind, {}, snapshot,
                                session, memory, extra_bindings={str(ordinary): {"input_format": "parquet"}},
                                files=[(str(ordinary), "parquet", None)]):
                            self.assertEqual(actual.to_pylist(), [
                                {"id": 2, "label": "new-two", "weight": 10},
                                {"id": 2, "label": "new-two", "weight": 11},
                                {"id": 5, "label": "omega", "weight": 50}])

    def test_conflicting_snapshot_and_schema_inspection_fail_before_missing_source_io(self):
        import shardloom as sl
        from shardloom.errors import ShardLoomCommandError
        with tempfile.TemporaryDirectory(prefix="shardloom-table-conflict-") as directory:
            for kind, historical in (("iceberg", {"snapshot_id": 1}), ("delta", {"version": 0}),
                                     ("hudi", {"instant": "20261101000100"})):
                path = Path(directory) / f"missing-{kind}"
                with self.owners() as (client, session):
                    latest = sl.read(path, input_format=kind, client=client)
                    old = sl.read(path, input_format=kind, client=client, **historical)
                    with self.assertRaisesRegex(ShardLoomCommandError, "conflicting"):
                        latest.join(old, on="id").collect(check=True)
                    bindings = [(str(path), kind, None, None, None),
                        (str(path), kind, historical.get("snapshot_id"), historical.get("version"), historical.get("instant"))]
                    for declare in (
                        lambda: session.sql(f"SELECT * FROM '{path}'", tables=bindings),
                        lambda: session.plan(self.native.Relation.scan(str(path)), tables=bindings),
                    ):
                        with self.assertRaisesRegex(Exception, "conflicting"):
                            declare().collect()
                    with self.assertRaisesRegex(Exception, "schema inspection"):
                        latest.schema()
                    for plan in (
                        session.sql(f"SELECT * FROM '{path}'", tables=bindings[:1]),
                        session.plan(self.native.Relation.scan(str(path)), tables=bindings[:1]),
                    ):
                        with self.assertRaisesRegex(Exception, "schema inspection"):
                            plan.schema_json()

    def test_table_logical_types_and_nulls_survive_all_five_interfaces(self):
        import shardloom as sl
        with tempfile.TemporaryDirectory(prefix="shardloom-table-types-") as directory:
            for kind in ("iceberg", "delta", "hudi"):
                path, snapshot, expected = make_typed_table(Path(directory) / kind, kind, self.pa)
                with self.owners() as (client, session):
                    frame = sl.read(path, input_format=kind, client=client).sort("id")
                    relation = self.native.Relation.scan(str(path)).sort([("id", False, "last")])
                    with self.subTest(format=kind):
                        for actual in self.all_surfaces(frame, relation, f"SELECT * FROM '{path}' ORDER BY id",
                                path, kind, {}, snapshot, session, 64 << 20):
                            self.assertEqual(actual.schema, expected.schema)
                            self.assertEqual(actual.to_pylist(), expected.to_pylist())

    def test_delta_partition_constants_preserve_types_and_override_physical_values(self):
        from datetime import date, datetime, timezone
        from decimal import Decimal
        import shardloom as sl
        with tempfile.TemporaryDirectory(prefix="shardloom-delta-partitions-") as directory:
            fixture = Fixture(Path(directory))
            data = fixture.parquet("data.parquet", [self.pa.field("id", self.pa.int64(), nullable=False),
                self.pa.field("region", self.pa.string())], [self.pa.array([1, 2]), self.pa.array(["wrong", "wrong"])])
            logical = [delta_field("id", "long", False), delta_field("region", "string", True),
                delta_field("day", "date", True), delta_field("utc", "timestamp", True),
                delta_field("local", "timestamp_ntz", True), delta_field("amount", "decimal(6,3)", True),
                delta_field("small", "byte", True), delta_field("flag", "boolean", True)]
            partitions = {"region": "right", "day": "2020-02-29", "utc": "2020-02-29T12:34:56.123456Z",
                "local": "2020-02-29 12:34:56.123456", "amount": "-12.340", "small": "-128", "flag": "true"}
            fixture.commit(0, [{"protocol": {"minReaderVersion": 3, "minWriterVersion": 7,
                "readerFeatures": ["timestampNtz"], "writerFeatures": ["timestampNtz"]}},
                delta_metadata(logical, list(partitions), "none"), delta_add(data, "data.parquet", partitions)])
            expected = [{"id": index, "region": "right", "day": date(2020, 2, 29),
                "utc": datetime(2020, 2, 29, 12, 34, 56, 123456, tzinfo=timezone.utc),
                "local": datetime(2020, 2, 29, 12, 34, 56, 123456), "amount": Decimal("-12.340"),
                "small": -128, "flag": True} for index in (1, 2)]
            with self.owners() as (client, session):
                frame = sl.read(fixture.root, input_format="delta", client=client).sort("id")
                relation = self.native.Relation.scan(str(fixture.root)).sort([("id", False, "last")])
                for actual in self.all_surfaces(frame, relation, f"SELECT * FROM '{fixture.root}' ORDER BY id",
                        fixture.root, "delta", {}, "0", session, 64 << 20):
                    self.assertEqual(actual.to_pylist(), expected)
                    self.assertEqual(actual.schema.field("small").type, self.pa.int8())
                    self.assertEqual(actual.schema.field("amount").type, self.pa.decimal128(6, 3))

    def test_table_results_keep_native_ownership_and_persist_complete_values(self):
        import pyarrow.parquet as parquet
        import shardloom as sl
        with tempfile.TemporaryDirectory(prefix="shardloom-table-owned-") as directory:
            root = Path(directory)
            for kind in ("iceberg", "delta", "hudi"):
                source_root = root / kind
                path, _, snapshot, data, control = make_table(source_root, kind, self.pa)
                with self.owners() as (client, session):
                    frame = sl.read(path, input_format=kind, client=client).sort("id")
                    for output_format in ("vortex", "parquet", "arrow-ipc"):
                        destination = root / f"{kind}.{output_format}"
                        receipt = frame.write(destination, output_format=output_format, check=True)
                        self.assertFalse(receipt.envelope.fallback.attempted)
                        if output_format == "parquet":
                            actual = parquet.read_table(destination)
                        elif output_format == "arrow-ipc":
                            with self.pa.memory_map(str(destination), "r") as stream:
                                actual = self.pa.ipc.open_file(stream).read_all()
                        else:
                            result = session.sql(f"SELECT * FROM '{destination}'").collect()
                            actual = self.pa.RecordBatchReader.from_stream(result).read_all()
                        self.assertEqual(actual.to_pylist(), LATEST)
                    for alias in (data, control):
                        with self.assertRaises(Exception):
                            frame.write(alias, output_format="parquet", allow_overwrite=True, check=True)
                    result = session.sql(f"SELECT * FROM '{path}' ORDER BY id",
                        tables=[(str(path), kind, None, None, None)]).collect()
                    self.checked(None, result.report, kind, snapshot)
                shutil.rmtree(source_root)
                retained = self.pa.RecordBatchReader.from_stream(result).read_all()
                self.assertEqual(retained.to_pylist(), LATEST)

    def test_unsupported_states_fail_across_every_interface(self):
        import shardloom as sl
        with tempfile.TemporaryDirectory(prefix="shardloom-table-refusal-") as directory:
            for kind in ("iceberg", "delta", "hudi"):
                root = Path(directory) / kind
                path, _, _, _, _ = make_table(root, kind, self.pa)
                if kind == "iceberg":
                    value = json.loads(path.read_text())
                    value["format-version"] = 3
                    path.write_text(json.dumps(value))
                    reason = "versions 1 and 2"
                elif kind == "delta":
                    Fixture(root).commit(2, [{"protocol": {"minReaderVersion": 3, "minWriterVersion": 7,
                                                       "readerFeatures": ["deletionVectors"], "writerFeatures": ["deletionVectors"]}}])
                    reason = "feature"
                else:
                    config = root / ".hoodie/hoodie.properties"
                    config.write_text(config.read_text().replace("COPY_ON_WRITE", "MERGE_ON_READ"))
                    reason = "Merge-on-Read"
                bindings = {str(path): {"input_format": kind, "table_snapshot": {}}}
                statement = f"SELECT * FROM '{path}'"
                args = ["sql", "--sql", statement, "--source-bindings", json.dumps(bindings),
                        "--request", "collect", "--bounded", "true", "--memory-bytes", str(64 << 20),
                        "--max-parallelism", "2"]
                with self.owners() as (client, session):
                    declarations = [
                        lambda: sl.read(path, input_format=kind, client=client).collect(check=True),
                        lambda: session.sql(statement, tables=[(str(path), kind, None, None, None)]).collect(),
                        lambda: session.plan(self.native.Relation.scan(str(path)),
                                             tables=[(str(path), kind, None, None, None)]).collect(),
                        lambda: session.workflow(args).collect(),
                    ]
                    with patch.object(subprocess, "Popen", side_effect=AssertionError("external process")):
                        for index, declaration in enumerate(declarations):
                            with self.subTest(format=kind, surface=index), self.assertRaisesRegex(Exception, reason):
                                declaration()
                    cli = sl.ShardLoomClient(binary=os.environ.get("SHARDLOOM_SURFACE_TEST_BINARY"),
                        transport="subprocess", use_persistent_worker=False,
                        memory_bytes=64 << 20, max_parallelism=2, timeout=30)
                    try:
                        with self.assertRaisesRegex(Exception, reason):
                            cli.public_workflow_run(
                                "sql", sql_statement=statement, source_bindings=bindings, bounded=True,
                                memory_bytes=64 << 20, max_parallelism=2, check=True)
                    finally:
                        cli.close()


if __name__ == "__main__":
    unittest.main()
