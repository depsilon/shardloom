# SPDX-License-Identifier: Apache-2.0
"""Independent finite populations and complete public source-fact consumers."""

from collections import Counter
import gc
import math
import operator
from pathlib import Path
import shutil
import struct
import tempfile

FIXTURES = Path(__file__).parent / "data" / "source-facts-v1"
PROFILES = ("alp-f32", "alp-f64", "no-stats-f32", "no-stats-f64",
            "flat-f32", "flat-f64", "all-null-f32", "all-null-f64",
            "empty-f32", "empty-f64", "bitpacked-u64", "for-u64")
KINDS = ("zero_eq", "zero_ne", "zero_lt", "zero_le", "zero_gt", "zero_ge",
         "sparse", "dense", "none", "all", "conjunction", "inexact",
         "alias", "count", "group", "shared", "barrier")
OPS = {"eq": operator.eq, "ne": operator.ne, "lt": operator.lt,
       "le": operator.le, "gt": operator.gt, "ge": operator.ge}
SQL_OPS = {"eq": "=", "ne": "<>", "lt": "<", "le": "<=", "gt": ">", "ge": ">="}
PROOF = "local_primitive_native_io_pushdown_proof_basis"


def population(size, profile):
    """Enumerate every logical cell without reading the physical fixture writer."""
    assert size in ("public", "work") and profile in PROFILES
    if profile.startswith("empty-"):
        return
    for row_id in range(79 if size == "public" else 12291):
        if profile.startswith("all-null-") or row_id % 17 == 0:
            value = None
        elif profile.endswith("u64"):
            value = (row_id * 73) % 2048
        else:
            if row_id % 31 == 1:
                value = -0.0
            elif row_id % 29 == 1:
                value = 0.0
            elif row_id % 97 == 1:
                value = math.pi
            else:
                value = ((row_id * 73) % 2048 - 1024) / 8
            if profile.endswith("f32"):
                value = struct.unpack("<f", struct.pack("<f", value))[0]
        yield {"row_id": row_id, "value": value}


def terms(profile, kind):
    """Declare comparison operators and literals independently of any renderer."""
    integer = profile.endswith("u64")
    zero = 0 if integer else -0.0
    if kind.startswith("zero_"):
        return ((kind.removeprefix("zero_"), zero),)
    if kind == "conjunction":
        return (("ge", zero), ("le", zero))
    if kind == "sparse":
        return (("gt", 2040 if integer else 127.0),)
    if kind == "dense":
        return (("gt", 7 if integer else -127.0),)
    if kind == "none":
        return (("gt", 4096 if integer else 1024.0),)
    if kind == "all":
        return (("ge", 0 if integer else -1024.0),)
    if kind == "inexact":
        return (("gt", 0.1),)
    assert kind in ("alias", "count", "group", "shared", "barrier")
    return (("gt", zero),)


def oracle(size, profile, kind):
    values = list(population(size, profile))
    if kind == "barrier":
        values = values[:47]
    conditions = terms(profile, kind)
    values = [row for row in values if row["value"] is not None
              and all(OPS[op](row["value"], literal) for op, literal in conditions)]
    if kind == "count":
        return [{"rows": len(values)}]
    if kind == "group":
        counts = Counter(row["value"] for row in values)
        return [dict(value=value, rows=counts[value]) for value in sorted(counts)]
    if kind == "shared":
        values *= 2
    return [dict(row_id=row["row_id"]) for row in sorted(values, key=lambda row: row["row_id"])]


def observations(proof):
    tokens = {}
    for token in (proof or "").split():
        name, separator, value = token.partition("=")
        if separator and (name.startswith("native_encoded_compare_")
                          or name == "source_footer_finite_float_scan_invocations"):
            assert name not in tokens, name
            value = value.split(";", 1)[0]
            tokens[name] = int(value) if value.isdecimal() else value
    return tokens


def check_report(test, profile, kind, get):
    work = observations(get(PROOF))
    test.assertEqual(work["native_encoded_compare_scope"], "exclusive_operation")
    finite = work["source_footer_finite_float_scan_invocations"]
    declined = (profile.endswith("u64") or profile.startswith("no-stats-") or kind == "barrier"
                or (profile.endswith("f32") and kind == "inexact"))
    if declined:
        test.assertEqual(finite, 0, (profile, kind, work))
    elif profile.startswith(("alp-", "flat-")):
        test.assertGreater(finite, 0, (profile, kind, work))
    if profile.startswith(("flat-", "no-stats-", "all-null-", "empty-")) or declined:
        test.assertEqual(work["native_encoded_compare_alp_calls"], 0, (profile, kind, work))


def queries(test, client, path, profile, kind):
    import shardloom as sl

    expr = test.native.Expression
    frame = sl.read_vortex(path, client=client)
    relation = test.native.Relation.scan(str(path))
    source, name = f"'{path}'", "value"
    if kind == "alias":
        frame = frame.select("row_id", "value AS renamed")
        relation = relation.project([("row_id", expr.column("row_id")), ("renamed", expr.column("value"))])
        source, name = f"(SELECT row_id,value AS renamed FROM {source}) AS aliased", "renamed"
    if kind == "barrier":
        frame = frame.sort("row_id").limit(47)
        relation = relation.sort([("row_id", False, "last")]).limit(47)
        source = f"(SELECT * FROM {source} ORDER BY row_id LIMIT 47) AS bounded"
    clauses, predicates = [], []
    for op, literal in terms(profile, kind):
        clauses.append(f"{name} {SQL_OPS[op]} {literal!r}")
        predicates.append(OPS[op](expr.column(name), expr.literal(literal)))
    predicate = predicates[0]
    for other in predicates[1:]:
        predicate = predicate & other
    condition = " AND ".join(clauses)
    frame, relation = frame.where(condition), relation.filter(predicate)
    statement = f"SELECT * FROM {source} WHERE {condition}"
    if kind == "count":
        return frame.agg("COUNT(*) AS rows"), relation.aggregate([], [("count", None, "rows")]), f"SELECT COUNT(*) AS rows FROM ({statement}) AS filtered"
    if kind == "group":
        return (frame.group_by("value").agg("COUNT(*) AS rows").sort("value"),
                relation.aggregate(["value"], [("count", None, "rows")]).sort([("value", False, "last")]),
                f"SELECT value,COUNT(*) AS rows FROM ({statement}) AS filtered GROUP BY value ORDER BY value")
    if kind == "shared":
        frame, relation = frame.union_all(frame), relation.set_operation(relation, kind="union_all")
        statement = f"SELECT * FROM ({statement}) AS left_side UNION ALL SELECT * FROM ({statement}) AS right_side"
    return (frame.select("row_id").sort("row_id"),
            relation.project([("row_id", expr.column("row_id"))]).sort([("row_id", False, "last")]),
            f"SELECT row_id FROM ({statement}) AS filtered ORDER BY row_id")


def check_surfaces(test):
    import shardloom as sl

    client = sl.ShardLoomClient(transport="native", memory_bytes=128 << 20, max_parallelism=2)
    test.addCleanup(client.close)
    for profile in PROFILES:
        selected = KINDS if profile.startswith("alp-") else ("zero_eq", "zero_ne", "inexact", "group", "barrier")
        for kind in selected:
            with test.subTest(profile=profile, kind=kind):
                frame, relation, statement = queries(test, client, FIXTURES / f"{profile}.vortex", profile, kind)
                for table in test.all_surfaces(frame, relation, statement,
                        report_check=lambda get: check_report(test, profile, kind, get)):
                    test.assertEqual(table.to_pylist(), oracle("public", profile, kind))
                    for field in table.schema:
                        dtype = (test.pa.uint64() if field.name == "rows" or profile.endswith("u64") else
                                 test.pa.float32() if profile.endswith("f32") else test.pa.float64())
                        test.assertEqual(field.type, test.pa.int64() if field.name == "row_id" else dtype)
                        test.assertEqual(field.nullable, field.name == "value")


def check_owned_and_changed_source(test):
    with tempfile.TemporaryDirectory(prefix="shardloom-source-facts-owned-") as directory:
        path = Path(directory) / "source.vortex"
        shutil.copyfile(FIXTURES / "alp-f64.vortex", path)
        session = test.native.Session(test.native.ExecutionResources(128 << 20, 2))
        statement = f"SELECT row_id FROM '{path}' WHERE value > -0.0 ORDER BY row_id"
        plan = session.sql(statement)
        first, second = plan.collect(), plan.collect()
        first_work = observations(first.report.get(PROOF))
        test.assertGreater(first_work["native_encoded_compare_alp_calls"], 0)
        test.assertEqual(first_work, observations(second.report.get(PROOF)))
        shutil.copyfile(FIXTURES / "no-stats-f64.vortex", path)
        with test.assertRaisesRegex(Exception, "prepared source changed"):
            plan.collect()
        plan.release_prepared()
        retry = session.sql(statement)
        retried = retry.collect()
        test.assertEqual(test.table(retried).to_pylist(), oracle("public", "alp-f64", "zero_gt"))
        test.assertEqual(observations(retried.report.get(PROOF))["source_footer_finite_float_scan_invocations"], 0)
        retry.release_prepared()
        del retry, retried, plan, second
        session.close()
        path.unlink()
        test.assertGreater(session.snapshot()["reserved_bytes"], 0)
        table = test.table(first)
        test.assertEqual(table.to_pylist(), oracle("public", "alp-f64", "zero_gt"))
        retained = test.session.sql("SELECT * FROM 'memory://source-facts-result'",
            sources=[("memory://source-facts-result", first.as_source())])
        for name in ("vortex", "parquet", "arrow-ipc"):
            target = Path(directory) / f"result.{name}"
            report = retained.write(target, "write_" + name.replace("-", "_"))
            test.assertEqual(report["fallback_attempted"], "false")
            test.assertEqual(report["external_engine_invoked"], "false")
            reopened = test.table(test.session.sql(f"SELECT * FROM '{target}'",
                files=[(str(target), name, None)]).collect())
            test.assertEqual(reopened.to_pylist(), table.to_pylist())
        retained.release_prepared()
        del first, table, retained, reopened
        gc.collect()
        test.assertEqual(session.snapshot()["reserved_bytes"], 0)
