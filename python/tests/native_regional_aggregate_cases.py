# SPDX-License-Identifier: Apache-2.0
"""Exact regional partials through real public requests and retained outputs."""

from pathlib import Path
import shutil
import tempfile

FIXTURES = Path(__file__).parent / "data" / "regional-aggregates-v1"
PREFIX = "local_primitive_regional_aggregate_"
MEASURES = [("count", None, "rows"), ("count", "signed_value", "present"),
            ("min", "signed_value", "lo"), ("max", "unsigned_value", "hi")]
SQL_MEASURES = ("COUNT(*) AS rows", "COUNT(signed_value) AS present",
                "MIN(signed_value) AS lo", "MAX(unsigned_value) AS hi")


def oracle(profile, bounds):
    """Independent scalar values; no result is read from another query engine."""
    indices = range(0 if profile == "empty" else 71)
    selected = [row for row in indices if bounds is None or (
        not (profile == "nullable-predicate" and row < 8) and bounds[0] < row < bounds[1])]
    signed = []
    unsigned = []
    for row in selected:
        if profile != "all-null" and row % 7:
            signed.append((-(1 << 63), (1 << 63) - 1, (1 << 53) + 1, -row)[row % 4])
        if row % 5:
            unsigned.append((1 << 64) - 1 - row)
    return [{"rows": len(selected), "present": len(signed),
             "lo": min(signed, default=None), "hi": max(unsigned, default=None)}]


def queries(test, client, path, bounds, reject_having=False):
    import shardloom as sl

    native, expr = test.native, test.native.Expression
    frame = sl.read_vortex(path, client=client)
    relation = native.Relation.scan(str(path))
    sql = f"SELECT {','.join(SQL_MEASURES)} FROM '{path}'"
    if bounds is not None:
        low, high = bounds
        condition = f"selector > {low} AND selector < {high}"
        frame = frame.where(condition)
        relation = relation.filter((expr.column("selector") > expr.literal(low)) &
                                   (expr.column("selector") < expr.literal(high)))
        sql += f" WHERE {condition}"
    frame = frame.agg(*SQL_MEASURES)
    relation = relation.aggregate([], MEASURES)
    if reject_having:
        frame = frame.where("rows > 71")
        relation = relation.filter(expr.column("rows") > expr.literal(71))
        sql += " HAVING rows > 71"
    return frame, relation, sql


def expected_proof(profile, bounds):
    if profile in ("flat", "empty"):
        return {PREFIX + "source": None}
    if bounds is None:
        metadata, excluded, residual = (0, 0, 71) if profile == "missing-min" else (71, 0, 0)
    elif bounds == (10, 12):
        metadata, excluded, residual = (0, 47, 24) if profile == "independent" else (0, 63, 8)
    elif profile == "independent":
        metadata, excluded, residual = 24, 0, 47
    elif profile == "missing-min":
        metadata, excluded, residual = 0, 15, 56
    else:
        metadata, excluded, residual = 40, 15, 16
    return {PREFIX + "source_rows": "71", PREFIX + "metadata_rows": str(metadata),
            PREFIX + "excluded_rows": str(excluded), PREFIX + "residual_source_rows": str(residual),
            PREFIX + "complete_disjoint_source_coverage": "true",
            PREFIX + "whole_zones_only": "true",
            PREFIX + "fallback_attempted": "false", PREFIX + "external_engine_invoked": "false"}


def assert_schema(test, table):
    for column, dtype, nullable in (("rows", test.pa.uint64(), False),
                                   ("present", test.pa.uint64(), False),
                                   ("lo", test.pa.int64(), True),
                                   ("hi", test.pa.uint64(), True)):
        field = table.schema.field(column)
        test.assertEqual(field.type, dtype)
        test.assertEqual(field.nullable, nullable)


def check_surfaces(test):
    import shardloom as sl

    client = sl.ShardLoomClient(transport="native", memory_bytes=128 << 20, max_parallelism=2)
    test.addCleanup(client.close)
    for profile in ("complete", "independent", "all-null", "nullable-predicate", "missing-min", "flat", "empty"):
        path = FIXTURES / f"{profile}.vortex"
        for bounds in (None, (10, 60), (10, 12)):
            frame, relation, statement = queries(test, client, path, bounds)
            checks = expected_proof(profile, bounds)
            for table in test.all_surfaces(frame, relation, statement, checks):
                test.assertEqual(table.to_pylist(), oracle(profile, bounds), (profile, bounds))
                assert_schema(test, table)
    path = FIXTURES / "complete.vortex"
    frame, relation, statement = queries(test, client, path, (10, 60), reject_having=True)
    for table in test.all_surfaces(frame, relation, statement, expected_proof("complete", (10, 60))):
        test.assertEqual(table.to_pylist(), [])
        assert_schema(test, table)


def check_owned_outputs(test):
    with tempfile.TemporaryDirectory(prefix="shardloom-regional-owned-") as directory:
        root = Path(directory)
        path = root / "source.vortex"
        shutil.copyfile(FIXTURES / "independent.vortex", path)
        session = test.native.Session(test.native.ExecutionResources(128 << 20, 2))
        try:
            statement = (f"SELECT {','.join(SQL_MEASURES)} FROM '{path}' "
                         "WHERE selector > 10 AND selector < 60")
            result = session.sql(statement).collect()
            for key, value in expected_proof("independent", (10, 60)).items():
                test.assertEqual(result.report.get(key), value, key)
        finally:
            session.close()
        path.unlink()
        table = test.table(result)
        test.assertEqual(table.to_pylist(), oracle("independent", (10, 60)))
        assert_schema(test, table)
        retained = test.session.sql("SELECT * FROM 'memory://regional-result'",
                                    sources=[("memory://regional-result", result.as_source())])
        for format_name in ("vortex", "parquet", "arrow-ipc"):
            target = root / f"result.{format_name}"
            report = retained.write(target, "write_" + format_name.replace("-", "_"))
            test.assertEqual(report["fallback_attempted"], "false")
            test.assertEqual(report["external_engine_invoked"], "false")
            reopened = test.table(test.session.sql(f"SELECT * FROM '{target}'",
                files=[(str(target), format_name, None)]).collect())
            test.assertEqual(reopened.to_pylist(), oracle("independent", (10, 60)))
            assert_schema(test, reopened)
