# SPDX-License-Identifier: Apache-2.0
"""Independent dictionary populations and complete public consumer checks."""

from collections import Counter
import gc
from pathlib import Path
import re
import shutil
import tempfile

FIXTURES = Path(__file__).parent / "data" / "dictionary-text-v1"
PROFILES = ("dictionary", "sparse", "all-null", "unique", "flat", "empty")
FUNCTIONS = ("lower", "upper", "trim", "length", "octet_length", "regex_match")
KINDS = tuple(f"{name}_{consumer}" for name in FUNCTIONS
              for consumer in ("group", "distinct", "filter")) + (
    "shared", "normalized_group", "normalized_distinct",
    "union_normalized_group", "selected_normalized_group")
PATTERN = r"(?i)λ|東京|\s$"
PROOF = "local_primitive_native_io_pushdown_proof_basis"


def population(size, profile):
    """Enumerate logical values independently of the Rust physical writer."""
    if profile == "empty":
        return
    if size == "scale":
        chunks, remaining = divmod(1_048_577, 4097)
        lengths = (4097,) * chunks + (remaining,)
    else:
        assert size in ("public", "work")
        lengths = (23, 24, 24) if size == "public" else (4097, 4097, 4097)
    row_id = 0
    for chunk, length in enumerate(lengths):
        regular = (f" {'ABC'[chunk % 3]} ", "a",
                   " \u2003ΣΟΣİß東京\0 e\u0301 Case \u2003" * 8,
                   None, "", " \u2003Λ\u2003 ", "İßΣΟΣ")
        sparse = (f" {'ABC'[chunk % 3]} ", "a", None, "\u2003東京\0\u2003")
        for local in range(length):
            if profile == "all-null" or row_id % 11 == 0:
                value = None
            elif profile == "unique":
                value = f" unique_{row_id:08}_éΣ "
            else:
                values = sparse if profile == "sparse" else regular
                value = values[local % len(values)]
            yield {"row_id": row_id, "value": value}
            row_id += 1


def transform(name, value):
    if value is None:
        return None
    if name == "lower":
        return value.lower()
    if name == "upper":
        return value.upper()
    if name == "trim":
        return value.strip()
    if name == "length":
        return len(value)
    if name == "octet_length":
        return len(value.encode("utf-8"))
    if name == "regex_match":
        return re.search(PATTERN, value) is not None
    assert name == "normalized"
    return value.lower().strip()


def retained(name, value):
    if value is None:
        return False
    if name == "regex_match":
        return value is True
    if name in ("length", "octet_length"):
        return value > 3
    return value != ""


def oracle(size, profile, kind):
    values = list(population(size, profile))
    if kind == "shared":
        result = []
        for row in values:
            value, lower = row["value"], transform("lower", row["value"])
            result.append(dict(row_id=row["row_id"], lowered=lower, again=lower,
                uppered=transform("upper", value), trimmed=transform("trim", value),
                characters=transform("length", value), bytes=transform("octet_length", value),
                matches=transform("regex_match", value), normalized=transform("normalized", value),
                lower_characters=transform("length", lower), lower_bytes=transform("octet_length", lower)))
        return result
    if kind == "union_normalized_group":
        values *= 2
    if kind == "selected_normalized_group":
        values = [row for row in values if row["row_id"] % 5 == 0]
    name, consumer = kind.rsplit("_", 1)
    if name.endswith("normalized"):
        name = "normalized"
    result = [dict(row_id=row["row_id"], value=transform(name, row["value"])) for row in values]
    if consumer == "filter":
        return [row for row in result if retained(name, row["value"])]
    counts = Counter(row["value"] for row in result)
    ordered = sorted(counts, key=lambda value: (value is None, value))
    return [dict(value=value, rows=counts[value]) if consumer == "group" else dict(value=value)
            for value in ordered]


def expressions(native):
    expr = native.Expression
    col, lit = expr.column, expr.literal
    call = lambda name, *args: expr.call(name, list(args))
    lower = call("lower", col("value"))
    result = {name: (f"{name}(value)", call(name, col("value"))) for name in FUNCTIONS
              if name != "regex_match"}
    result["regex_match"] = (f"regex_match(value,'{PATTERN}')", call("regex_match", col("value"), lit(PATTERN)))
    result["normalized"] = ("trim(lower(value))", call("trim", lower))
    shared = [("row_id", "row_id", col("row_id"))]
    for alias, name in (("lowered", "lower"), ("again", "lower"), ("uppered", "upper"),
                        ("trimmed", "trim"), ("characters", "length"), ("bytes", "octet_length"),
                        ("matches", "regex_match"), ("normalized", "normalized")):
        sql, typed = result[name]
        shared.append((alias, sql, typed))
    shared.extend((("lower_characters", "length(lower(value))", call("length", lower)),
                   ("lower_bytes", "octet_length(lower(value))", call("octet_length", lower))))
    return result, shared


def queries(test, client, path, kind):
    import shardloom as sl

    native, expr = test.native, test.native.Expression
    frame = sl.read_vortex(path, client=client)
    relation = native.Relation.scan(str(path))
    source = f"'{path}'"
    if kind == "union_normalized_group":
        frame = frame.union_all(frame)
        relation = relation.set_operation(relation, kind="union_all")
        source = f"(SELECT * FROM {source} UNION ALL SELECT * FROM {source}) AS combined"
    if kind == "selected_normalized_group":
        frame = frame.where("row_id % 5 = 0")
        relation = relation.filter((expr.column("row_id") % expr.literal(5)) == expr.literal(0))
        source = f"(SELECT * FROM {source} WHERE row_id % 5 = 0) AS selected"
    functions, shared = expressions(native)
    if kind == "shared":
        frame = frame.select(*(f"{sql} AS {name}" for name, sql, _ in shared))
        relation = relation.project([(name, typed) for name, _, typed in shared])
        statement = "SELECT " + ",".join(f"{sql} AS {name}" for name, sql, _ in shared) + f" FROM {source}"
        order = "row_id"
    else:
        name, consumer = kind.rsplit("_", 1)
        if name.endswith("normalized"):
            name = "normalized"
        sql, typed = functions[name]
        columns = [("value", sql, typed)]
        if consumer == "filter":
            columns.insert(0, ("row_id", "row_id", expr.column("row_id")))
        frame = frame.select(*(f"{sql} AS {alias}" for alias, sql, _ in columns))
        relation = relation.project([(alias, value) for alias, _, value in columns])
        statement = "SELECT " + ",".join(f"{sql} AS {alias}" for alias, sql, _ in columns) + f" FROM {source}"
        if consumer == "group":
            frame = frame.group_by("value").agg("COUNT(*) AS rows")
            relation = relation.aggregate(["value"], [("count", None, "rows")])
            statement = f"SELECT value,COUNT(*) AS rows FROM ({statement}) AS transformed GROUP BY value"
        elif consumer == "distinct":
            frame = frame.distinct()
            relation = relation.unary("distinct_rows")
            statement = f"SELECT DISTINCT value FROM ({statement}) AS transformed"
        else:
            col = expr.column("value")
            if name == "regex_match":
                condition, predicate = "value = TRUE", col == expr.literal(True)
            elif name in ("length", "octet_length"):
                condition, predicate = "value > 3", col > expr.literal(3)
            else:
                condition, predicate = "value <> ''", col != expr.literal("")
            frame, relation = frame.where(condition), relation.filter(predicate)
            statement = f"SELECT * FROM ({statement}) AS transformed WHERE {condition}"
        order = "row_id" if consumer == "filter" else "value"
    return (frame.sort(order, nulls="last"), relation.sort([(order, False, "last")]),
            statement + f" ORDER BY {order} ASC NULLS LAST")


def dictionary_work(proof):
    result = {}
    for token in (proof or "").split():
        name, separator, value = token.partition("=")
        if separator and name.startswith("dictionary_text_") and value.isdecimal():
            assert name not in result
            result[name.removeprefix("dictionary_text_")] = int(value)
    return result


def check_report(test, profile, kind, get_field):
    work = dictionary_work(get_field(PROOF))
    if profile in ("dictionary", "sparse", "all-null") and not kind.startswith("selected_"):
        test.assertGreater(work.get("dictionary_batches", 0), 0, (profile, kind, get_field(PROOF)))
        test.assertEqual(work["row_output_rows"], 0)
        test.assertEqual(work["code_rows_copied"], work["retained_dictionary_rows"])
        test.assertLess(work["domain_slots"], work["logical_rows"])
        if profile == "all-null":
            test.assertEqual(work["dictionary_kernel_calls"], 0)
    else:
        test.assertEqual(work.get("dictionary_batches", 0), 0, (profile, kind, work))


def check_surfaces(test):
    import shardloom as sl

    client = sl.ShardLoomClient(transport="native", memory_bytes=128 << 20, max_parallelism=2)
    test.addCleanup(client.close)
    for profile in PROFILES:
        selected = KINDS if profile == "dictionary" else KINDS[-5:]
        for kind in selected:
            with test.subTest(profile=profile, kind=kind):
                frame, relation, statement = queries(test, client, FIXTURES / f"{profile}.vortex", kind)
                for table in test.all_surfaces(frame, relation, statement,
                        report_check=lambda get: check_report(test, profile, kind, get)):
                    test.assertEqual(table.to_pylist(), oracle("public", profile, kind))
                    expected_types = {"row_id": test.pa.int64(), "rows": test.pa.uint64(),
                        "characters": test.pa.int64(), "bytes": test.pa.int64(),
                        "lower_characters": test.pa.int64(), "lower_bytes": test.pa.int64(),
                        "matches": test.pa.bool_()}
                    if kind.startswith(("length_", "octet_length_")):
                        expected_types["value"] = test.pa.int64()
                    elif kind.startswith("regex_match_"):
                        expected_types["value"] = test.pa.bool_()
                    for field in table.schema:
                        test.assertEqual(field.type, expected_types.get(field.name, test.pa.string()))
                        test.assertEqual(field.nullable, field.name not in ("row_id", "rows"))


def check_owned_and_changed_source(test):
    with tempfile.TemporaryDirectory(prefix="shardloom-dictionary-owned-") as directory:
        root = Path(directory)
        path = root / "source.vortex"
        shutil.copyfile(FIXTURES / "dictionary.vortex", path)
        session = test.native.Session(test.native.ExecutionResources(128 << 20, 2))
        statement = f"SELECT row_id,trim(lower(value)) AS value FROM '{path}' ORDER BY row_id"
        plan = session.sql(statement)
        first, second = plan.collect(), plan.collect()
        for result in (first, second):
            work = dictionary_work(result.report.get(PROOF))
            test.assertGreater(work["dictionary_batches"], 0)
        test.assertEqual(dictionary_work(first.report.get(PROOF)), dictionary_work(second.report.get(PROOF)))
        expected = [dict(row_id=row["row_id"], value=transform("normalized", row["value"]))
                    for row in population("public", "dictionary")]
        shutil.copyfile(FIXTURES / "unique.vortex", path)
        with test.assertRaisesRegex(Exception, "prepared source changed"):
            plan.collect()
        plan.release_prepared()
        retry = session.sql(statement)
        retried = test.table(retry.collect())
        test.assertEqual(retried.to_pylist(), [dict(row_id=row["row_id"],
            value=transform("normalized", row["value"])) for row in population("public", "unique")])
        retry.release_prepared()
        del retry, retried
        del plan, second, result
        session.close()
        path.unlink()
        test.assertGreater(session.snapshot()["reserved_bytes"], 0)
        table = test.table(first)
        test.assertEqual(table.to_pylist(), expected)
        retained = test.session.sql("SELECT * FROM 'memory://dictionary-result'",
            sources=[("memory://dictionary-result", first.as_source())])
        for name in ("vortex", "parquet", "arrow-ipc"):
            target = root / f"result.{name}"
            report = retained.write(target, "write_" + name.replace("-", "_"))
            test.assertEqual(report["fallback_attempted"], "false")
            test.assertEqual(report["external_engine_invoked"], "false")
            reopened = test.table(test.session.sql(f"SELECT * FROM '{target}'",
                files=[(str(target), name, None)]).collect())
            test.assertEqual(reopened.to_pylist(), expected)
        retained.release_prepared()
        del first, table, retained, reopened
        gc.collect()
        test.assertEqual(session.snapshot()["reserved_bytes"], 0)
