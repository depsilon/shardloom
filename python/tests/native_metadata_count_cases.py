# SPDX-License-Identifier: Apache-2.0
"""Direct Parquet metadata cardinality across the five public entry points."""

from pathlib import Path
import tempfile

DECISIONS = "local_primitive_native_io_decision_"


def count_decisions(rows):
    return {DECISIONS + "schema_version": "shardloom.native_io_decisions.v1",
            DECISIONS + "metadata_count_considered": "1",
            DECISIONS + "metadata_count_admitted": "1",
            DECISIONS + "metadata_count_executed": "1",
            DECISIONS + "metadata_count_declined": "0",
            DECISIONS + "metadata_count_admission_reasons": "exact_held_metadata",
            DECISIONS + "metadata_count_observed_input_rows": str(rows)}


def region_decisions(total, retained):
    return {DECISIONS + "schema_version": "shardloom.native_io_decisions.v1",
            DECISIONS + "region_exclusion_considered": str(total),
            DECISIONS + "region_exclusion_admitted": str(total - retained),
            DECISIONS + "region_exclusion_executed": str(total - retained),
            DECISIONS + "region_exclusion_declined": str(retained),
            DECISIONS + "region_exclusion_observed_regions": str(total),
            DECISIONS + "region_exclusion_observed_excluded_regions": str(total - retained)}


def checked_output(test, label, destination, output_format, expected, report):
    """Read every persisted value and preserve the exact scalar schema."""
    import pyarrow.parquet as parquet

    test.assertEqual(report["fallback_attempted"], "false")
    test.assertEqual(report["external_engine_invoked"], "false")
    if output_format == "parquet":
        reopened = parquet.read_table(destination)
    elif output_format == "arrow-ipc":
        with test.pa.memory_map(str(destination), "r") as source:
            reopened = test.pa.ipc.open_file(source).read_all()
    else:
        reopened = test.table(test.session.sql(f"SELECT * FROM '{destination}'").collect())
    test.assertEqual(reopened.to_pylist(), expected, label)
    test.assertEqual(reopened.column_names, list(expected[0]), label)
    for field in reopened.schema:
        test.assertEqual(field.type, test.pa.uint64(), label)
        test.assertFalse(field.nullable, label)


def check_counts_and_outputs(test):
    import pyarrow.parquet as parquet
    import shardloom as sl

    pa, native = test.pa, test.native
    client = sl.ShardLoomClient(transport="native", memory_bytes=128 << 20, max_parallelism=2)
    test.addCleanup(client.close)
    schema = pa.schema([pa.field("entity", pa.uint64()), pa.field("label", pa.string())])
    rows = [{"entity": None, "label": None}, {"entity": 0, "label": ""},
            {"entity": (1 << 64) - 1, "label": "λ"}]
    with tempfile.TemporaryDirectory(prefix="shardloom-direct-count-") as directory:
        root = Path(directory)
        for name, contents, files in (("empty", [], 1), ("nullable", rows, 1),
                                      ("collection", rows, 3)):
            table = pa.Table.from_pylist(contents, schema=schema)
            if files == 1:
                path = root / f"{name}.parquet"
                parquet.write_table(table, path, row_group_size=1, write_statistics=False)
            else:
                path = root / name
                path.mkdir()
                for index in range(files):
                    parquet.write_table(table, path / f"part-{index}.parquet",
                                        row_group_size=1, write_statistics=False)
            expected_count = len(contents) * files
            expected = [{"record_total": expected_count, "again": expected_count}]
            frame = sl.read_parquet(path, client=client).agg(
                "COUNT(*) AS record_total", "COUNT(*) AS again")
            relation = native.Relation.scan(str(path)).aggregate(
                [], [("count", None, "record_total"), ("count", None, "again")])
            sql = f"SELECT COUNT(*) AS record_total,COUNT(*) AS again FROM '{path}'"
            checks = {
                "native_source_metadata_count_sources": "1",
                "native_source_metadata_count_rows": str(expected_count),
                "native_source_demand_files": str(files),
                "native_source_demand_projected_columns": "0",
                "native_source_demand_physical_leaf_columns": "0",
                "native_source_demand_rows_decoded": "0",
                "native_source_demand_row_groups_read": "0",
                "native_source_demand_data_read_bytes": "0",
                "native_source_demand_data_read_requests": "0",
                "native_input_batches": "0",
                "native_input_batch_rows": "0",
                "native_input_end_observed": "true",
            }
            checks.update(count_decisions(expected_count))
            for actual in test.all_surfaces(frame, relation, sql, checks, file_binding=(path, "parquet")):
                test.assertEqual(actual.to_pylist(), expected)
                for field in actual.schema:
                    test.assertEqual(field.type, pa.uint64())
                    test.assertFalse(field.nullable)
            for output_format in ("vortex", "parquet", "arrow-ipc"):
                destination = root / f"{name}-result.{output_format}"
                written = frame.write(destination, output_format=output_format, check=True)
                for key, value in checks.items():
                    test.assertEqual(written.envelope.field(key), value, key)
                test.assertFalse(written.envelope.fallback.attempted)
                checked_output(test, f"{name}-direct", destination, output_format,
                               expected, written.envelope.field_map)
            owner = native.Session(native.ExecutionResources(128 << 20, 2))
            retained = owner.sql(sql, files=[(str(path), "parquet", None)]).collect()
            owner.close()
            if path.is_dir():
                for member in path.iterdir():
                    member.unlink()
                path.rmdir()
            else:
                path.unlink()
            test.assertEqual(test.table(retained).to_pylist(), expected)
            reused = test.session.sql("SELECT * FROM 'memory://count-result'",
                sources=[("memory://count-result", retained.as_source())])
            for output_format in ("vortex", "parquet", "arrow-ipc"):
                destination = root / f"{name}-owned.{output_format}"
                report = reused.write(destination, f"write_{output_format.replace('-', '_')}")
                checked_output(test, f"{name}-owned", destination, output_format, expected, report)


def check_declines(test):
    import pyarrow.parquet as parquet
    import shardloom as sl

    pa, native = test.pa, test.native
    client = sl.ShardLoomClient(transport="native", memory_bytes=128 << 20, max_parallelism=2)
    test.addCleanup(client.close)
    with tempfile.TemporaryDirectory(prefix="shardloom-count-domains-") as directory:
        path = Path(directory) / "input.parquet"
        parquet.write_table(pa.table({"entity": pa.array([None, 0, 9], type=pa.int64())}),
                            path, row_group_size=1)
        frame = sl.read_parquet(path, client=client)
        relation = native.Relation.scan(str(path))
        for query, plan, sql, expected in (
            (frame.agg("COUNT(entity) AS present"), relation.aggregate([], [("count", "entity", "present")]),
             f"SELECT COUNT(entity) AS present FROM '{path}'", [{"present": 2}]),
            (frame.agg("COUNT(*) AS rows", "SUM(entity) AS total"),
             relation.aggregate([], [("count", None, "rows"), ("sum", "entity", "total")]),
             f"SELECT COUNT(*) AS rows,SUM(entity) AS total FROM '{path}'", [{"rows": 3, "total": 9.0}]),
            (frame.filter("entity = 9").agg("COUNT(*) AS rows"),
             relation.filter(native.Expression.column("entity") == native.Expression.literal(9)).aggregate([], [("count", None, "rows")]),
             f"SELECT COUNT(*) AS rows FROM '{path}' WHERE entity = 9", [{"rows": 1}]),
        ):
            checks = {"native_source_metadata_count_sources": "0"}
            if " WHERE " in sql:
                checks.update({DECISIONS + "metadata_count_declined": "1",
                               DECISIONS + "metadata_count_executed": "0",
                               DECISIONS + "metadata_count_decline_reasons": "required_value_evaluation"})
            for actual in test.all_surfaces(query, plan, sql, checks):
                test.assertEqual(actual.to_pylist(), expected)


def check_mapped_sources(test):
    """Schema unions and Hive constants preserve facts through all five routes."""
    import json
    import pyarrow.parquet as parquet
    import shardloom as sl

    pa, native = test.pa, test.native
    client = sl.ShardLoomClient(transport="native", memory_bytes=128 << 20, max_parallelism=2)
    test.addCleanup(client.close)
    col, lit = native.Expression.column, native.Expression.literal
    with tempfile.TemporaryDirectory(prefix="shardloom-mapped-count-") as directory:
        for mode in ("union", "hive", "both"):
            root = Path(directory) / mode
            root.mkdir()
            names = (["a.parquet", "b.parquet"] if mode == "union" else
                     ["country=US/a.parquet", "country=CA/b.parquet",
                      "country=__HIVE_DEFAULT_PARTITION__/c.parquet"])
            for index, name in enumerate(names):
                path = root / name
                path.parent.mkdir(parents=True, exist_ok=True)
                ids = list(range(index * 100, index * 100 + 8))
                payload = [n + 1000 for n in ids]
                fields = ({"right_value": payload, "n": ids}
                          if index == 1 and mode != "hive" else
                          {"n": ids, "left_value": payload})
                parquet.write_table(pa.table(fields), path, row_group_size=2)
            path = root / "dataset.manifest"
            path.write_text(json.dumps({"version": 1, "format": "parquet",
                "files": list(reversed(names)),
                "schema": "strict" if mode == "hive" else "union_by_name",
                "partitioning": "none" if mode == "union" else "hive"}))
            frame = sl.read_parquet(path, client=client)
            relation = native.Relation.scan(str(path))
            expected_count = len(names) * 8
            checks = {
                "native_source_metadata_count_sources": "1",
                "native_source_metadata_count_rows": str(expected_count),
                "native_source_demand_files": str(len(names)),
                "native_source_demand_projected_columns": "0",
                "native_source_demand_physical_leaf_columns": "0",
                "native_source_demand_rows_decoded": "0",
                "native_source_demand_row_groups_read": "0",
                "native_source_demand_data_read_bytes": "0",
                "native_source_demand_data_read_requests": "0",
                "native_input_batches": "0",
                "native_input_end_observed": "true",
            }
            checks.update(count_decisions(expected_count))
            tables = test.all_surfaces(frame.select("n AS renamed").agg("COUNT(*) AS total"),
                relation.project([("renamed", col("n"))]).aggregate([], [("count", None, "total")]),
                f"SELECT COUNT(*) AS total FROM (SELECT n AS renamed FROM '{path}') AS mapped",
                checks, file_binding=(path, "parquet"))
            for actual in tables:
                test.assertEqual(actual.to_pylist(), [{"total": expected_count}], mode)
                test.assertEqual(actual.schema.field("total").type, pa.uint64())
                test.assertFalse(actual.schema.field("total").nullable)
            cases = [(frame.filter("n = 105"), relation.filter(col("n") == lit(105)),
                      "n = 105", 105)]
            if mode != "hive":
                cases.append((frame.filter("right_value = 1105"),
                    relation.filter(col("right_value") == lit(1105)), "right_value = 1105", 105))
            if mode != "union":
                cases.append((frame.filter("country = 'US'").filter("n = 5"),
                    relation.filter(col("country") == lit("US")).filter(col("n") == lit(5)),
                    "country = 'US' AND n = 5", 5))
            for selected, typed, predicate, expected in cases:
                checks = {"native_source_metadata_count_sources": "0",
                          "native_source_demand_row_groups_read": "1",
                          "native_source_demand_rows_decoded": "2"}
                checks.update(region_decisions(len(names) * 4, 1))
                tables = test.all_surfaces(selected.select("n"), typed.project([("n", col("n"))]),
                    f"SELECT n FROM '{path}' WHERE {predicate}", checks, file_binding=(path, "parquet"))
                for actual in tables:
                    test.assertEqual(actual.to_pylist(), [{"n": expected}], (mode, predicate))
                    test.assertEqual(actual.schema, tables[0].schema)


def check_orc_decisions(test):
    """Held ORC counts and early stripe exclusion reach every public interface."""
    import pyarrow.orc as orc
    import shardloom as sl

    with tempfile.TemporaryDirectory(prefix="shardloom-orc-decisions-") as directory:
        root = Path(directory)
        for index in range(4):
            orc.write_table(test.pa.table({"n": list(range(index * 4, index * 4 + 4))}),
                            root / f"part-{index}.orc")
        for memory, lanes in ((16 << 20, 1), (128 << 20, 2)):
            client = sl.ShardLoomClient(transport="native", memory_bytes=memory, max_parallelism=lanes)
            owner = test.native.Session(test.native.ExecutionResources(memory, lanes))
            try:
                frame = sl.read_orc(root, client=client)
                relation = test.native.Relation.scan(str(root))
                for query, typed, sql, expected, checks in (
                    (frame.agg("COUNT(*) AS total"),
                     relation.aggregate([], [("count", None, "total")]),
                     f"SELECT COUNT(*) AS total FROM '{root}'", [{"total": 16}],
                     {**count_decisions(16), "native_source_demand_data_read_bytes": "0",
                      "native_source_demand_rows_decoded": "0"}),
                    (frame.filter("n = 5"),
                     relation.filter(test.native.Expression.column("n") == test.native.Expression.literal(5)),
                     f"SELECT n FROM '{root}' WHERE n = 5", [{"n": 5}],
                     {**region_decisions(4, 1), "native_source_demand_row_groups_read": "1",
                      "native_source_demand_rows_decoded": "4"}),
                    (frame.filter("n = 99"),
                     relation.filter(test.native.Expression.column("n") == test.native.Expression.literal(99)),
                     f"SELECT n FROM '{root}' WHERE n = 99", [],
                     {**region_decisions(4, 0), "native_source_demand_data_read_bytes": "0",
                      "native_source_demand_rows_decoded": "0"}),
                ):
                    with test.subTest(memory=memory, lanes=lanes, sql=sql):
                        for table in test.all_surfaces(query, typed, sql, checks,
                                file_binding=(root, "orc"), execution_session=owner,
                                resources=(memory, lanes)):
                            test.assertEqual(table.to_pylist(), expected)
            finally:
                owner.close()
                client.close()
