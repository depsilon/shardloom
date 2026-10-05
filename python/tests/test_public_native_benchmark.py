from __future__ import annotations

import csv
from dataclasses import asdict, replace
import hashlib
import importlib.util
import json
import math
from pathlib import Path
import sys
import tempfile
import unittest

REPO_ROOT = Path(__file__).resolve().parents[2]
BENCHMARK_DIR = REPO_ROOT / "benchmarks" / "traditional_analytics"
SCRIPTS_DIR = REPO_ROOT / "scripts"
for directory in (BENCHMARK_DIR, SCRIPTS_DIR):
    if str(directory) not in sys.path:
        sys.path.insert(0, str(directory))

from benchmark_models import DatasetPaths, FORMAT_ORDER, COMPARISON_FORMATS, comparison_input_format
from fixtures import (
    ensure_dataset,
    fact_path,
    dim_path,
    fact_part_paths,
    fixture_arrow_column_types,
    fixture_columns_for_role,
    fixture_column_dtype,
    fixture_schema_for_role,
    fixture_scalar_from_text,
)
from native_public_runner import NativeConfiguration, NativePublicRunner, _generation, _literal
from native_workflow_protocol import read_json_output, report_fields
from run import summarize
from workloads import WORKLOADS


class PublicNativeBenchmarkTests(unittest.TestCase):
    def test_contradictory_fallback_envelope_cannot_override_safe_flat_fields(self) -> None:
        fields = [
            {"key": "public_workflow_fallback_attempted", "value": "false"},
            {"key": "public_workflow_external_engine_invoked", "value": "false"},
        ]
        safe = {"attempted": False, "allowed": False, "engine": None}
        self.assertEqual(len(report_fields({"status": "success", "fallback": safe, "fields": fields})), 2)
        for fallback in (None, [], {**safe, "attempted": True}, {**safe, "attempted": 0},
                         {**safe, "engine": "duckdb"}, {**safe, "allowed": True}):
            with self.subTest(fallback=fallback), self.assertRaisesRegex(ValueError, "envelope fallback"):
                report_fields({"status": "success", "fallback": fallback, "fields": fields})

    def test_complete_json_and_jsonl_output_reader_preserves_empty_nested_null_and_integer_values(self) -> None:
        exact_integer = 9_007_199_254_740_993
        rows = [{"nested": {"items": [None, exact_integer]}, "nullable": None}]
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            json_path = root / "rows.json"
            json_path.write_text(json.dumps(rows), encoding="utf-8")
            jsonl_path = root / "rows.jsonl"
            jsonl_path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")
            empty_json_path = root / "empty.json"
            empty_json_path.write_text("[]", encoding="utf-8")
            empty_jsonl_path = root / "empty.jsonl"
            empty_jsonl_path.write_text("", encoding="utf-8")

            self.assertEqual(read_json_output(json_path, "json"), rows)
            self.assertEqual(read_json_output(jsonl_path, "jsonl"), rows)
            self.assertEqual(read_json_output(empty_json_path, "json"), [])
            self.assertEqual(read_json_output(empty_jsonl_path, "jsonl"), [])
            self.assertEqual(read_json_output(json_path, "json")[0]["nested"]["items"][1], exact_integer)

    def test_complete_json_output_reader_rejects_malformed_duplicate_and_non_object_rows(self) -> None:
        invalid_outputs = (
            ("truncated JSON", "[{\"id\":1", "json"),
            ("truncated JSONL", "{\"id\":1", "jsonl"),
            ("duplicate JSON key", "[{\"id\":1,\"id\":2}]", "json"),
            ("duplicate JSONL key", "{\"id\":1,\"id\":2}\n", "jsonl"),
            ("non-object JSON row", "[1]", "json"),
            ("non-object JSONL row", "1\n", "jsonl"),
            ("JSON object instead of rows", "{\"id\":1}", "json"),
        )
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            for label, content, output_format in invalid_outputs:
                with self.subTest(label=label):
                    path = root / f"{label.replace(' ', '-')}.{output_format}"
                    path.write_text(content, encoding="utf-8")
                    with self.assertRaises(ValueError):
                        read_json_output(path, output_format)

    def test_json_writer_statement_reads_complete_file_without_a_second_native_query(self) -> None:
        rows = [{"nested": {"value": None}, "exact": 9_007_199_254_740_993}]
        for output_format in ("json", "jsonl"):
            with self.subTest(output_format=output_format), tempfile.TemporaryDirectory() as tempdir:
                root = Path(tempdir)
                workspace = root / "workspace"
                workspace.mkdir()
                binary = root / "shardloom"
                binary.write_bytes(b"mock executable")
                runner = NativePublicRunner.__new__(NativePublicRunner)
                runner.configuration = NativeConfiguration(binary, root, output_format=output_format)
                runner.binary = binary
                runner.call_count = 0
                runner.run_id = "fixture"
                runner._workspace = lambda _paths: (root, workspace)
                native_calls = []

                def fake_run(_paths, command, *, phase):
                    native_calls.append((command, phase))
                    output = Path(command[command.index("--output") + 1])
                    content = (json.dumps(rows) if output_format == "json" else
                               "".join(json.dumps(row) + "\n" for row in rows))
                    output.write_text(content, encoding="utf-8")
                    envelope = {
                        "status": "success",
                        "fallback": {"attempted": False, "engine": None},
                        "fields": [
                            {"key": "public_workflow_fallback_attempted", "value": "false"},
                            {"key": "public_workflow_external_engine_invoked", "value": "false"},
                            {"key": "native_vortex_result_export_all_targets_committed", "value": "true"},
                            {"key": "native_vortex_result_export_path", "value": str(output)},
                        ],
                    }
                    return envelope, {"returncode": 0, "guard_failures": [], "seconds": 1.0}

                runner._run = fake_run
                actual, _envelope, receipt, readback = runner._statement(
                    object(), "SELECT 1", {str(root / "input.vortex"): {"input_format": "vortex"}},
                    output_format=output_format,
                )

                output = Path(receipt["output"]["path"])
                self.assertEqual(actual, rows)
                self.assertEqual(len(native_calls), 1)
                self.assertEqual(native_calls[0][1], "query_and_requested_output")
                self.assertEqual(readback["kind"], "complete_json_file_readback")
                self.assertEqual(readback["format"], output_format)
                self.assertEqual(readback["path"], receipt["output"]["path"])
                self.assertEqual(readback["sha256"], receipt["output"]["sha256"])
                self.assertEqual(readback["bytes"], receipt["output"]["bytes"])
                self.assertEqual(readback["sha256"], hashlib.sha256(output.read_bytes()).hexdigest())
                self.assertGreaterEqual(readback["seconds"], 0)

    def test_fixture_schema_is_explicit_for_every_source_role_and_rejects_unknown_fields(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            paths = ensure_dataset(Path(tempdir) / "fixture", 8, 3, ("csv",), "tiny_smoke")

        fact_columns = (
            "id", "group_key", "dim_key", "value", "metric", "flag", "category",
            *paths.fact_extra_columns,
        )
        self.assertEqual(fixture_columns_for_role(paths, "fact"), fact_columns)
        self.assertEqual(fixture_columns_for_role(paths, "parts"), fact_columns)
        self.assertEqual(fixture_columns_for_role(paths, "dim"), ("dim_key", "dim_label", "weight"))
        self.assertEqual(
            fixture_schema_for_role(paths, "delta"),
            "id:int64,op:utf8,value:int64,metric:float64,effective_ts:utf8",
        )
        self.assertEqual(fixture_column_dtype("dirty_numeric"), "utf8")
        self.assertEqual(fixture_column_dtype("nullable_metric_00"), "float64")
        self.assertEqual(fixture_column_dtype("nullable_category_03"), "utf8")
        self.assertEqual(fixture_column_dtype("is_deleted"), "boolean")
        with self.assertRaisesRegex(ValueError, "unknown generated fixture column"):
            fixture_column_dtype("unknown_numeric_00")
        with self.assertRaisesRegex(ValueError, "unknown generated fixture source role"):
            fixture_schema_for_role(paths, "unknown")

    def test_raw_and_prepared_inputs_forward_the_same_role_schema(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            paths = ensure_dataset(root / "fixture", 8, 3, ("csv", "json", "jsonl"), "tiny_smoke")
            roles = ("fact", "dim", "parts", "delta")
            binary = root / "shardloom"
            binary.write_bytes(b"fixture executable identity")
            runner = NativeConfiguration(binary, root / "workspace")
            public_runner = NativePublicRunner.__new__(NativePublicRunner)
            public_runner.configuration = runner
            public_runner.fact_path = fact_path
            public_runner.dim_path = dim_path
            public_runner.fact_part_paths = lambda dataset, data_format: fact_part_paths(dataset, data_format)
            csv_sources = public_runner._source_paths(paths, "csv", roles)
            expected_csv = {
                str(source): fixture_schema_for_role(paths, role)
                for role, members in csv_sources.items()
                for source, _source_format in members
            }
            jsonl_sources = public_runner._source_paths(paths, "jsonl", roles)
            expected_jsonl = {
                str(source): fixture_schema_for_role(paths, role)
                for role, members in jsonl_sources.items()
                for source, _source_format in members
            }
            json_sources = public_runner._source_paths(paths, "json", roles)
            expected_json = {
                str(source): fixture_schema_for_role(paths, role)
                for role, members in json_sources.items()
                for source, _source_format in members
            }
            for data_format in ("csv", "json", "jsonl"):
                sources = public_runner._source_paths(paths, data_format, roles)
                _expressions, bindings, _generations = public_runner._bindings(sources, paths)
                expected = {
                    str(source): fixture_schema_for_role(paths, role)
                    for role, members in sources.items()
                    for source, _source_format in members
                }
                self.assertEqual(
                    {source: binding.get("source_schema") for source, binding in bindings.items()},
                    {str(source): fixture_schema_for_role(paths, role)
                     for role, members in sources.items() for source, _source_format in members},
                )

            public_runner.configuration = replace(runner, input_state="prepared")
            public_runner.binary = binary.resolve()
            public_runner.binary_generation = _generation(binary)
            public_runner.run_id = "fixture-test"
            public_runner.prepared = {}
            public_runner.preparation_receipts = []
            prepared_workspace = root / "prepared"
            prepared_workspace.mkdir()
            public_runner._workspace = lambda _dataset: (root, prepared_workspace)
            prepared_commands = []

            def fake_run(_dataset, command, *, phase):
                self.assertEqual(phase, "input_preparation")
                prepared_commands.append(command)
                Path(command[3]).write_bytes(b"prepared")
                return {
                    "fields": [
                        {"key": "vortex_ingest_performed", "value": "true"},
                        {"key": "external_engine_invoked", "value": "false"},
                    ]
                }, {"phase": phase}

            public_runner._run = fake_run
            all_scenarios = tuple(WORKLOADS)
            public_runner.prepare(paths, ("csv", "json", "jsonl"), all_scenarios)
            prepared_schemas = {
                command[2]: command[command.index("--schema") + 1]
                for command in prepared_commands
                if "--schema" in command
            }
            self.assertEqual(prepared_schemas, {**expected_csv, **expected_json, **expected_jsonl})

            _prepared_expressions, prepared_bindings, _prepared_generations = public_runner._bindings(
                csv_sources, paths
            )
            self.assertEqual(len(prepared_bindings), len(expected_csv))
            self.assertTrue(all(binding["input_format"] == "vortex" for binding in prepared_bindings.values()))
            self.assertTrue(all("source_schema" not in binding for binding in prepared_bindings.values()))

    def test_eight_native_formats_keep_actual_comparison_formats_explicit(self) -> None:
        self.assertEqual(set(FORMAT_ORDER), {"csv", "json", "jsonl", "vortex", "parquet", "arrow-ipc", "avro", "orc"})
        self.assertEqual(set(COMPARISON_FORMATS), set(FORMAT_ORDER) - {"vortex"})
        for data_format in FORMAT_ORDER:
            self.assertEqual(comparison_input_format(data_format), "csv" if data_format == "vortex" else data_format)
        with self.assertRaisesRegex(ValueError, "unknown candidate"):
            comparison_input_format("unknown")

    def test_json_array_fixtures_preserve_every_csv_value_and_partition(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            paths = ensure_dataset(Path(tempdir) / "fixture", 9, 3, ("json",), "tiny_smoke")
            pairs = [(paths.fact_csv, paths.fact_json), (paths.dim_csv, paths.dim_json)]
            csv_parts = fact_part_paths(paths, "csv")
            json_parts = fact_part_paths(paths, "json")
            self.assertEqual(len(csv_parts), len(json_parts))
            pairs.extend(zip(csv_parts, json_parts))
            for source, destination in pairs:
                with source.open(newline="", encoding="utf-8") as stream:
                    expected = [{key: fixture_scalar_from_text(value, fixture_column_dtype(key))
                                 for key, value in row.items()} for row in csv.DictReader(stream)]
                self.assertEqual(read_json_output(destination, "json"), expected)
            self.assertEqual(fact_path(paths, "json"), paths.fact_json)
            self.assertEqual(dim_path(paths, "json"), paths.dim_json)

    def test_json_comparison_reader_keeps_declared_types_and_rejects_column_drift(self) -> None:
        if not importlib.util.find_spec("pyarrow"):
            self.skipTest("pyarrow is an optional comparison dependency")
        from baselines import pyarrow_table_for_format
        import pyarrow as pa

        with tempfile.TemporaryDirectory() as tempdir:
            paths = ensure_dataset(Path(tempdir) / "fixture", 8, 3, ("json",), "tiny_smoke")
            for part in fact_part_paths(paths, "json"):
                table = pyarrow_table_for_format(part, "json")
                self.assertEqual(table.to_pylist(), read_json_output(part, "json"))
                self.assertEqual(table.schema.field("nullable_metric_00").type, pa.float64())
                self.assertEqual(table.schema.field("dirty_numeric").type, pa.string())
            bad = Path(tempdir) / "extra.json"
            bad.write_text('[{"id":1},{"id":2,"metric":3.0}]')
            with self.assertRaisesRegex(ValueError, "column names"):
                pyarrow_table_for_format(bad, "json")

    def test_native_vortex_fixture_generation_and_prepared_reuse_are_separate(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            paths = ensure_dataset(root / "fixture", 8, 3, ("vortex",), "tiny_smoke")
            binary = root / "shardloom"
            binary.write_bytes(b"mock executable")
            workspace = root / "workspace"
            workspace.mkdir()
            runner = NativePublicRunner(
                NativeConfiguration(binary, workspace, input_state="prepared"),
                fact_path=fact_path, dim_path=dim_path, fact_part_paths=fact_part_paths, round_float=round,
            )
            runner._workspace = lambda _paths: (root, workspace)
            commands = []

            def fake_run(_paths, command, *, phase):
                commands.append(command)
                self.assertEqual(phase, "input_preparation")
                self.assertEqual(command[command.index("--input-format") + 1], "csv")
                Path(command[3]).write_bytes(b"native fixture")
                return {"fields": [{"key": "vortex_ingest_performed", "value": "true"},
                                   {"key": "external_engine_invoked", "value": "false"}]}, {"command": command}

            runner._run = fake_run
            runner.prepare_fixture_inputs(paths, ("vortex",))
            self.assertEqual(len(commands), 2 + len(fact_part_paths(paths, "csv")))
            self.assertEqual(len(runner.fixture_preparation_receipts), len(commands))
            self.assertEqual(runner.preparation_receipts, [])
            runner.prepare(paths, ("vortex",), ("hash join",))
            self.assertEqual(len(runner.reused_native_inputs), 2)
            self.assertEqual(runner.preparation_receipts, [])
            self.assertEqual(len(commands), len(runner.fixture_preparation_receipts))
            sources = runner._source_paths(paths, "vortex", ("fact", "dim"))
            _, bindings, _ = runner._bindings(sources, paths)
            self.assertEqual(set(bindings), {str(paths.fact_vortex), str(paths.dim_vortex)})
            self.assertTrue(all(binding == {"input_format": "vortex"} for binding in bindings.values()))
            paths.fact_vortex.write_bytes(b"changed native fixture")
            with self.assertRaisesRegex(RuntimeError, "artifact changed"):
                runner._bindings(sources, paths)

    def test_split_arrow_inputs_preserve_types_for_disjoint_and_all_null_parts(self) -> None:
        if not importlib.util.find_spec("pyarrow"):
            self.skipTest("pyarrow is an optional fixture-generation dependency")
        import pyarrow as pa
        import pyarrow.parquet as pq

        with tempfile.TemporaryDirectory() as tempdir:
            paths = ensure_dataset(Path(tempdir) / "fixture", 8, 3, ("csv", "parquet"), "tiny_smoke")
            part_paths = sorted(paths.fact_parquet_parts_dir.glob("part-*.parquet"))
            self.assertEqual(len(part_paths), 8)
            expected_types = fixture_arrow_column_types(pa, fixture_columns_for_role(paths, "fact"))
            tables = [pq.read_table(path) for path in part_paths]

        for table in tables:
            self.assertEqual(table.schema.names, list(expected_types))
            for name, dtype in expected_types.items():
                self.assertEqual(table.schema.field(name).type, dtype, name)
        self.assertEqual(tables[0]["nullable_metric_00"].null_count, 1)
        self.assertEqual(tables[0]["nullable_metric_00"].type, pa.float64())
        self.assertEqual(tables[0]["dirty_numeric"].type, pa.string())
        self.assertEqual(tables[0]["dirty_numeric"][0].as_py(), "bad-number")
        self.assertEqual(tables[1]["dirty_numeric"].type, pa.string())

    def test_split_avro_inputs_preserve_csv_headers_and_typed_nulls(self) -> None:
        if not importlib.util.find_spec("fastavro"):
            self.skipTest("fastavro is an optional fixture-generation dependency")
        import fastavro

        with tempfile.TemporaryDirectory() as tempdir:
            paths = ensure_dataset(Path(tempdir) / "fixture", 8, 3, ("csv", "avro"), "tiny_smoke")
            with paths.fact_csv.open(newline="", encoding="utf-8") as source:
                header = next(csv.reader(source))
            with paths.fact_avro.open("rb") as source:
                fact_reader = fastavro.reader(source)
                fact_schema = fact_reader.writer_schema
                fact_records = list(fact_reader)
            part_path = sorted(paths.fact_avro_parts_dir.glob("part-*.avro"))[0]
            with part_path.open("rb") as source:
                part_reader = fastavro.reader(source)
                part_schema = part_reader.writer_schema
                part_records = list(part_reader)

        self.assertEqual([field["name"] for field in part_schema["fields"]], header)
        self.assertEqual([field["name"] for field in fact_schema["fields"]], header)
        self.assertEqual(len(fact_records), 8)
        self.assertIsInstance(fact_records[0]["id"], int)
        self.assertIsInstance(fact_records[0]["metric"], float)
        self.assertEqual(fact_records[0]["dirty_numeric"], "bad-number")
        self.assertIsNone(fact_records[0]["optional_metric_v2"])
        self.assertIsNone(part_records[0]["nullable_metric_00"])
        self.assertEqual(fixture_scalar_from_text("false", "boolean"), False)

    def test_fixture_refuses_existing_directory_without_changing_sentinel(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir) / "fixture"
            root.mkdir()
            sentinel = root / "keep.txt"
            sentinel.write_text("preserve me", encoding="utf-8")

            with self.assertRaises(FileExistsError):
                ensure_dataset(root, 3, 2, ("csv",), "tiny_smoke")

            self.assertEqual(sentinel.read_text(encoding="utf-8"), "preserve me")
            self.assertEqual(list(root.iterdir()), [sentinel])

    def test_fixture_rejects_nonpositive_and_boolean_counts_before_creating_root(self) -> None:
        invalid_counts = (0, -1, False, True)
        with tempfile.TemporaryDirectory() as tempdir:
            base = Path(tempdir)
            for index, count in enumerate(invalid_counts):
                for bad_argument in ("rows", "dim_rows"):
                    with self.subTest(count=count, bad_argument=bad_argument):
                        root = base / f"invalid-{index}-{bad_argument}"
                        rows, dim_rows = (count, 2) if bad_argument == "rows" else (3, count)
                        with self.assertRaises(ValueError):
                            ensure_dataset(root, rows, dim_rows, ("csv",), "tiny_smoke")
                        self.assertFalse(root.exists())

    def test_tiny_csv_fixture_writes_fact_dimension_and_profile_sidecars(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir) / "tiny"
            paths = ensure_dataset(root, 5, 2, ("csv",), "tiny_smoke")

            with paths.fact_csv.open(newline="", encoding="utf-8") as handle:
                fact_rows = list(csv.DictReader(handle))
            with paths.dim_csv.open(newline="", encoding="utf-8") as handle:
                dim_rows = list(csv.DictReader(handle))
            metadata = json.loads((root / "dataset.json").read_text(encoding="utf-8"))

            self.assertEqual(len(fact_rows), 5)
            self.assertEqual(len(dim_rows), 2)
            self.assertEqual(
                list(fact_rows[0]),
                ["id", "group_key", "dim_key", "value", "metric", "flag", "category",
                 *paths.fact_extra_columns],
            )
            self.assertEqual(metadata["formats"], ["csv"])
            self.assertEqual(metadata["rows"], 5)
            self.assertEqual(metadata["dim_rows"], 2)
            self.assertTrue(paths.fact_csv_parts_dir.is_dir())
            self.assertEqual(len(list(paths.fact_csv_parts_dir.glob("part-*.csv"))), 5)
            self.assertTrue(paths.cdc_delta_csv.is_file())
            self.assertTrue(paths.nested_jsonl.is_file())
            self.assertTrue((root / "dataset.json").is_file())

    def test_summarize_requires_exact_case_set_and_passed_status(self) -> None:
        expected = {
            ("shardloom", "csv", "selective filter", 1),
            ("shardloom", "csv", "selective filter", 2),
        }

        def record(repeat: int, status: str = "passed") -> dict:
            return {
                "engine": "shardloom",
                "format": "csv",
                "scenario": "selective filter",
                "repeat": repeat,
                "status": status,
                "seconds": float(repeat),
                "timing_boundary": "complete request",
            }

        passing = [record(1), record(2)]
        self.assertTrue(summarize(passing, expected)["complete"])

        invalid_record_sets = {
            "omitted": [record(1)],
            "duplicate_replaces_required_repeat": [record(1), record(1)],
            "unsupported": [record(1), record(2, "unsupported")],
            "failed": [record(1), record(2, "failed")],
            "mismatch": [record(1), record(2, "mismatch")],
        }
        for label, records in invalid_record_sets.items():
            with self.subTest(label=label):
                self.assertFalse(summarize(records, expected)["complete"])

    def test_native_configuration_rejects_invalid_options(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            base = NativeConfiguration(Path("unused-binary"), Path(tempdir))
            invalid_updates = (
                {"input_state": "cached"},
                {"output_format": "unknown"},
                {"memory_gb": 0},
                {"memory_gb": -1},
                {"memory_gb": True},
                {"max_parallelism": 0},
                {"max_parallelism": -1},
                {"max_parallelism": False},
                {"timeout": 0},
                {"timeout": -1},
                {"timeout": math.nan},
                {"timeout": math.inf},
            )
            for update in invalid_updates:
                with self.subTest(update=update), self.assertRaises(ValueError):
                    replace(base, **update)

    def test_workload_binding_and_literal_escape_source_path_quotes(self) -> None:
        join = WORKLOADS["hash join"]
        with self.assertRaisesRegex(ValueError, "missing"):
            join.bind({"fact": "'fact.csv'"})

        quoted_path = Path("/tmp/O'Reilly/fact.csv")
        self.assertEqual(_literal(quoted_path), "'/tmp/O''Reilly/fact.csv'")
        bound, write_statement = join.bind(
            {"fact": _literal(quoted_path), "dim": "'/tmp/dim.csv'"}
        )
        self.assertIn("FROM '/tmp/O''Reilly/fact.csv' AS f", bound[0])
        self.assertIsNone(write_statement)

    def test_high_cardinality_normalizer_preserves_every_returned_group(self) -> None:
        workload = WORKLOADS["high-cardinality string group/distinct"]
        self.assertIn("LIMIT 100", workload.statements[0])
        groups = [
            {"category": f"category-{index:03d}", "row_count": 1, "metric_sum": 1.25}
            for index in range(101)
        ]

        result = workload.result(
            [groups, [{"distinct_category_count": 101}]],
            lambda value: round(value, 2),
        )

        self.assertEqual(result["distinct_category_count"], 101)
        self.assertEqual(len(result["groups"]), 101)
        self.assertEqual(result["groups"][0]["category"], "category-000")
        self.assertEqual(result["groups"][-1]["category"], "category-100")

    def test_scalar_result_normalizer_rejects_wrong_cardinality(self) -> None:
        workload = WORKLOADS["selective filter"]
        for rows in (
            [],
            [{"row_count": 1, "metric_sum": 2.0}, {"row_count": 2, "metric_sum": 3.0}],
        ):
            with self.subTest(row_count=len(rows)), self.assertRaisesRegex(
                ValueError, "exactly one row"
            ):
                workload.result([rows], lambda value: value)

    def test_dataset_paths_record_round_trip_preserves_all_path_fields(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            paths = ensure_dataset(Path(tempdir) / "fixture", 4, 2, ("csv",), "tiny_smoke")
            paths = replace(
                paths,
                cdc_delta_csv=None,
                nested_jsonl=None,
                output_root=Path(tempdir) / "output",
            )

            restored = DatasetPaths.from_record(asdict(paths))

        self.assertEqual(restored, paths)
        self.assertIsNone(restored.cdc_delta_csv)
        self.assertIsNone(restored.nested_jsonl)
        self.assertIsNotNone(restored.output_root)


if __name__ == "__main__":
    unittest.main()
