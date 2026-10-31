# SPDX-License-Identifier: Apache-2.0
"""Public-interface coverage for ordered native input fixtures."""

import os
from contextlib import nullcontext
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "benchmarks/traditional_analytics"))
sys.path.insert(0, str(ROOT / "scripts"))
if __package__:
    from . import native_ordered_input_cases as frozen
    from .test_native_join_demand_binding import _load_native
else:
    import native_ordered_input_cases as frozen
    from test_native_join_demand_binding import _load_native

from query_surface.semantics import ResultContract, TypedResult, arrow_schema, compare
from shardloom._result_schema import schema_fields


FORMATS = ("parquet", "arrow", "vortex")
SURFACES = ("dataframe", "typed_relation", "python_sql", "embedded_workflow", "cli_sql")
TEMPLATES = tuple(name for name in frozen.TEMPLATES if name not in {"sort_reversed", "sort_nullable_mixed"})


def public_cases():
    cases = [(name, size) for name in TEMPLATES for size in (0, 33, 4097)]
    cases.extend((name, 65537) for name in ("sort_ascending", "sort_shuffled"))
    return cases


class NativeOrderedInputTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.native = _load_native()
        import pyarrow as pa
        cls.pa = pa

    def checked(self, wanted, actual, contract, report, *, label, extension, surface):
        compare(wanted, actual, contract)
        self.assertTrue(report["complete_consumption"])
        self.assertEqual(report["surface"], surface)
        self.assertEqual(report["case"], label)
        fields = report["fields"]
        self.assertEqual(fields["fallback_attempted"], "false")
        self.assertEqual(fields["external_engine_invoked"], "false")
        self.assertTrue(report["source_session_released_before_export"])

    def write_sources(self, root, sources, extension):
        import pyarrow.parquet as parquet
        paths = {}
        writer = self.native.Session(self.native.ExecutionResources(256 << 20, 2))
        try:
            for name, rows in sorted(sources.items()):
                path = root / f"{name}.{extension}"
                table = self.pa.Table.from_pylist(rows, schema=frozen.source_schema())
                if extension == "parquet":
                    parquet.write_table(table, path, row_group_size=frozen.ROW_GROUP_ROWS,
                                        compression="zstd", use_dictionary=True)
                elif extension == "arrow":
                    with self.pa.OSFile(str(path), "wb") as stream:
                        with self.pa.ipc.new_file(stream, table.schema) as output:
                            output.write_table(table, max_chunksize=frozen.ROW_GROUP_ROWS)
                else:
                    source = writer.from_arrow(table)
                    uri = f"memory://ordered-input/{name}"
                    writer.plan(self.native.Relation.scan(uri), sources=[(uri, source)]).write(path, "write_vortex")
                paths[name] = (path,)
        finally:
            writer.close()
        return paths

    def declarations(self, template, paths, sl, client):
        native, E = self.native, self.native.Expression
        read = {"parquet": sl.read_parquet, "arrow": sl.read_arrow_ipc, "vortex": sl.read_vortex}
        frame = lambda key: read[Path(paths[key][0]).suffix[1:]](paths[key][0], client=client)
        if template.startswith("asof_"):
            f, d = frame("left"), frame("right")
            dataframe = f.join_asof(d, on="ord", by=["grp"], strategy="backward", allow_exact_matches=True).select(
                "f.id AS left_id", "d.id AS right_id", "d.value AS selected")
            relation = native.Relation.scan(str(paths["left"][0])).join_asof(
                native.Relation.scan(str(paths["right"][0])), ("ord", "ord"),
                [("left", "id", "left_id"), ("right", "id", "right_id"), ("right", "value", "selected")],
                by=[("grp", "grp")], strategy="backward", allow_exact_matches=True)
            return dataframe, relation
        source = frame("input")
        relation = native.Relation.scan(str(paths["input"][0]))
        if template.startswith("sort_"):
            dataframe = source.select("id", "ord AS ordered_key", "minor", "value", "payload").sort("ordered_key", nulls="last")
            relation = relation.project([(n, E.column(c)) for n, c in (("id", "id"), ("ordered_key", "ord"), ("minor", "minor"), ("value", "value"), ("payload", "payload"))]).sort([("ordered_key", False, "last")])
        elif template.startswith("window_"):
            import shardloom as slmod
            spec = slmod.Window.partition_by("grp").order_by("ord", nulls="last")
            dataframe = source.select("id", "grp", "ord", "value", slmod.row_number().over(spec).alias("position"))
            relation = relation.window(["id", "grp", "ord", "value"], [native.WindowExpression("row_number", "position", partition_by=["grp"], order_by=[("ord", False, "last")])])
        elif template.startswith("collections_"):
            from shardloom import functions as F
            dataframe = source.group_by("grp").agg(
                items=F.array_agg("value", order_by=[("ord", False, "last")]),
                joined=F.string_agg("text", " / ", order_by=[("ord", False, "last")])).sort("grp")
            relation = relation.aggregate(["grp"], [
                native.AggregateMeasure("array_agg", "value", "items", order_by=[("ord", False, "last")]),
                native.AggregateMeasure("string_agg", "text", "joined", separator=" / ", order_by=[("ord", False, "last")])]).sort([("grp", False, None)])
        elif template.startswith("selectors_"):
            from shardloom import functions as F
            ordered = [("ord", False, "last")]
            dataframe = source.group_by("grp").agg(
                initial=F.first("value", order_by=ordered), ending=F.last("value", order_by=ordered),
                least=F.arg_min("value", "minor", order_by=ordered),
                greatest=F.arg_max("value", "minor", order_by=ordered)).sort("grp")
            relation = relation.aggregate(["grp"], [
                native.AggregateMeasure("first", "value", "initial", order_by=ordered),
                native.AggregateMeasure("last", "value", "ending", order_by=ordered),
                native.AggregateMeasure("arg_min", "value", "least", secondary_column="minor", order_by=ordered),
                native.AggregateMeasure("arg_max", "value", "greatest", secondary_column="minor", order_by=ordered)]).sort([("grp", False, None)])
        else:
            raise AssertionError(f"missing declaration for {template}")
        return dataframe, relation

    def execute(self, template, paths, statement, surface, binary, label, *, output_rows):
        import shardloom as sl
        client, session = None, None
        try:
            if surface in {"dataframe", "cli_sql", "typed_relation"}:
                client = sl.ShardLoomClient(
                    binary=str(binary) if surface == "cli_sql" else None,
                    transport="subprocess" if surface == "cli_sql" else "native",
                    use_persistent_worker=False, memory_bytes=256 << 20, max_parallelism=2, timeout=60)
            if surface in {"dataframe", "typed_relation"}:
                dataframe, relation = self.declarations(template, paths, sl, client)
            if surface in {"dataframe", "cli_sql"}:
                plan = dataframe if surface == "dataframe" else sl.sql(statement, client=client)
                destination = None
                if surface == "cli_sql" and output_rows > 65536:
                    destination = Path(next(iter(paths.values()))[0]).parent / "cli-result.arrow"
                    result = plan.write_arrow_ipc(destination, check=True)
                else:
                    result = plan.collect(check=True)
                fields = dict(result.envelope.field_map)
                self.assertFalse(result.envelope.fallback.attempted)
                del plan
                if surface == "dataframe":
                    del dataframe, relation
                client.close()
                client = None
                if destination is None:
                    actual = TypedResult(result.schema, result.to_rows())
                    export = None
                else:
                    with self.pa.ipc.open_file(destination) as reader:
                        table = reader.read_all()
                    actual = TypedResult(arrow_schema(table.schema), table.to_pylist())
                    self.assertEqual(table.num_rows, output_rows)
                    export = dict(method="arrow_ipc_file", complete_rows=table.num_rows,
                                  source_client_closed_before_read=True)
            else:
                session = self.native.Session(self.native.ExecutionResources(256 << 20, 2))
                if surface == "typed_relation":
                    plan = session.plan(relation)
                    del dataframe, relation
                    client.close()
                    client = None
                elif surface == "python_sql":
                    plan = session.sql(statement)
                else:
                    self.assertEqual(surface, "embedded_workflow")
                    plan = session.workflow(["sql", "--sql", statement, "--request", "collect", "--bounded", "true",
                                             "--memory-bytes", str(256 << 20), "--max-parallelism", "2"])
                result = plan.collect(timeout=60)
                fields = dict(result.report)
                del plan
                session.close()
                session = None
                rows, export = result.row_export(temporal_objects=False)
                actual = TypedResult(schema_fields(result.schema_json, "vortex.dtype.serde.v1"), rows)
            return actual, dict(surface=surface, case=label, sql_declaration=statement, fields=fields,
                                row_export=export, complete_consumption=True,
                                source_session_released_before_export=True)
        finally:
            if session is not None:
                session.close()
            if client is not None:
                client.close()

    def test_ordered_inputs_all_public_surfaces_and_formats(self):
        import shardloom as sl
        cli = sl.ShardLoomClient(binary=os.environ.get("SHARDLOOM_SURFACE_TEST_BINARY"), transport="subprocess",
                                 use_persistent_worker=False, memory_bytes=256 << 20, max_parallelism=2, timeout=60)
        try:
            binary = cli.binary_command()[0]
        finally:
            cli.close()
        self.assertEqual(len(public_cases()), 32)
        count = 0
        with tempfile.TemporaryDirectory(prefix="shardloom-ordered-input-") as directory:
            root = Path(directory)
            for template, size in public_cases():
                sources = frozen.records(template, size)
                wanted = frozen.reference(template, sources)
                contract = ResultContract(wanted.schema, ordered=True)
                for extension in FORMATS:
                    format_root = root / f"{template}-{size}-{extension}"
                    format_root.mkdir()
                    paths = self.write_sources(format_root, sources, extension)
                    statement = frozen.statement(template, {k: v[0] for k, v in paths.items()})
                    for surface in SURFACES:
                        with self.subTest(template=template, rows=size, format=extension, surface=surface):
                            execution = nullcontext() if surface == "cli_sql" else patch.object(
                                subprocess, "Popen", side_effect=AssertionError("external process"))
                            label = f"{template}_{size}"
                            with execution:
                                actual, report = self.execute(template, paths, statement, surface, binary, label,
                                                              output_rows=len(wanted.rows))
                            self.checked(wanted, actual, contract, report, label=label, extension=extension, surface=surface)
                        count += 1
                    if extension == "parquet":
                        self.assertFalse((format_root / ".shardloom").exists())
        self.assertEqual(count, 480)


if __name__ == "__main__":
    unittest.main()
