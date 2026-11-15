# SPDX-License-Identifier: Apache-2.0
"""New public compositions over independent existing semantic oracles."""
import json
from pathlib import Path
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))


def write_fixture(test, path, table, extension):
    if extension == "vortex":
        source = test.session.from_arrow(table)
        test.session.plan(test.native.Relation.scan("memory://breadth-fixture"),
            sources=[("memory://breadth-fixture", source)]).write(path, "write_vortex")
    elif extension == "parquet":
        import pyarrow.parquet as parquet
        parquet.write_table(table, path, row_group_size=3, use_dictionary=True)
    else:
        with test.pa.OSFile(str(path), "wb") as stream, test.pa.ipc.new_file(stream, table.schema) as writer:
            writer.write_table(table, max_chunksize=3)


def public_source(path, extension, client):
    import shardloom as sl
    return getattr(sl, {"vortex": "read_vortex", "parquet": "read_parquet", "arrow": "read_arrow_ipc"}[extension])(
        path, client=client)


def check_multi_window_compositions(test):
    """Two window stages, ten framed measures, filtering and finite Sort output."""
    import shardloom as sl
    from native_window_frame_cases import declarations, MEASURES, COLUMNS
    from native_window_frame_reference import fixture_rows

    pa, native = test.pa, test.native
    source_schema = pa.schema([pa.field("id", pa.int64()), pa.field("cohort", pa.string()),
                               pa.field("priority", pa.int64()), pa.field("value", pa.int64())])
    wanted_schema = pa.schema([pa.field("id", pa.int64()),
        *[pa.field(name, pa.uint64(), False) for name in ("count_all", "nonnull", "distinct")],
        *[pa.field(name, pa.float64()) for name in ("total", "mean")],
        *[pa.field(name, pa.int64()) for name in ("minimum", "maximum", "first", "last", "nth")],
        pa.field("position", pa.int64(), False)])
    functions = ("count_all", "count", "count_distinct", "sum", "avg", "min", "max",
                 "first_value", "last_value", "nth_value")
    client = sl.ShardLoomClient(transport="native", memory_bytes=128 << 20, max_parallelism=2)
    test.addCleanup(client.close)
    matrix = list(declarations())
    test.assertEqual(len(matrix), 48)
    with tempfile.TemporaryDirectory(prefix="shardloom-multi-window-breadth-") as directory:
        for extension in ("vortex", "parquet", "arrow"):
            path = Path(directory) / ("input." + extension)
            write_fixture(test, path, pa.Table.from_pylist(fixture_rows(), schema=source_schema), extension)
            source = public_source(path, extension, client)
            for name, expressions, expected in matrix:
                unit = name.split("-", 1)[0]
                # Read declared syntax; the independent oracle above already
                # supplies every frame's result before any engine invocation.
                clause = expressions[0].split(" OVER (", 1)[1].split(") AS ", 1)[0]
                descending = "priority DESC" in clause
                nulls = "first" if "NULLS FIRST" in clause else "last"
                exclusion = clause.split(" EXCLUDE ", 1)[1].lower().replace(" ", "_")
                offset = native.FrameOffset.number(native.Expression.literal(1))
                frame = native.WindowFrame(unit, ("preceding", offset), ("following", offset), exclusion=exclusion)
                typed = [native.WindowExpression(function, alias,
                    source_column=None if function == "count_all" else "value",
                    argument=2 if function == "nth_value" else None,
                    partition_by=["cohort"], order_by=[("priority", descending, nulls)], frame=frame)
                    for function, (alias, _) in zip(functions, MEASURES, strict=True)]
                relation = native.Relation.scan(str(path)).window(["id"], typed)
                relation = relation.window(COLUMNS, [native.WindowExpression("row_number", "position",
                    order_by=[("id", True, "last")])])
                relation = relation.filter(native.Expression.column("count_all") > native.Expression.uint64(0))
                relation = relation.sort([("id", True, "last")]).limit(7)
                declaration = (source.window(*expressions).select(*COLUMNS)
                    .window("ROW_NUMBER() OVER (ORDER BY id DESC NULLS LAST) AS position")
                    .filter("count_all > 0").sort("id", descending=True, nulls="last").limit(7))
                first = f"SELECT id,{','.join(expressions)} FROM '{path}'"
                second = f"SELECT *,ROW_NUMBER() OVER (ORDER BY id DESC NULLS LAST) AS position FROM ({first}) AS framed"
                statement = f"SELECT * FROM ({second}) AS ranked WHERE count_all > 0 ORDER BY id DESC NULLS LAST LIMIT 7"
                wanted_rows = [dict(row, position=10 - row["id"]) for row in reversed(expected) if row["count_all"] > 0][:7]
                wanted = pa.Table.from_pylist(wanted_rows, schema=wanted_schema)
                with test.subTest(format=extension, frame=name):
                    results = test.all_surfaces(declaration, relation, statement,
                        file_binding=(path, "arrow-ipc" if extension == "arrow" else extension))
                    for table in results:
                        test.assertTrue(table.schema.equals(wanted.schema, check_metadata=True), (table.schema, wanted.schema))
                        test.assertEqual(table.to_pylist(), wanted_rows)


def check_pivot_melt_seeded_sampling(test):
    """Dynamic schema, null sparse cells, two reshapes and seeded populations."""
    import shardloom as sl
    from native_unary_reference import sample_ordinals

    pa, native = test.pa, test.native
    rows = [dict(entity=n // 4, category="a" if n % 4 < 2 else "b",
                 amount=None if n % 7 == 0 else n - 8) for n in range(24)]
    sums = {}
    for row in rows:
        values = sums.setdefault(row["entity"], {"a": [], "b": []})
        if row["amount"] is not None:
            values[row["category"]].append(row["amount"])
    # Sort before sampling fixes the population order across dynamic pivot,
    # melt and file partitioning. Each seed is a separate declared request.
    population = [dict(entity=key, variable="pivot_" + category,
                       value=float(sum(values[category])) if values[category] else None)
                  for key, values in sorted(sums.items()) for category in ("a", "b")]
    schema = pa.schema([pa.field("entity", pa.int64(), False), pa.field("category", pa.string(), False),
                        pa.field("amount", pa.int64())])
    wanted_schema = pa.schema([pa.field("entity", pa.int64(), False),
                               pa.field("variable", pa.string(), False), pa.field("value", pa.float64())])
    client = sl.ShardLoomClient(transport="native", memory_bytes=128 << 20, max_parallelism=2)
    test.addCleanup(client.close)
    pivot = {"index": "entity", "columns": "category", "values": "amount", "aggregate": "sum"}
    melt = {"id_columns": ["entity"], "value_columns": ["pivot_a", "pivot_b"]}
    with tempfile.TemporaryDirectory(prefix="shardloom-reshape-sample-breadth-") as directory:
        for extension in ("vortex", "parquet", "arrow"):
            path = Path(directory) / ("input." + extension)
            write_fixture(test, path, pa.Table.from_pylist(rows, schema=schema), extension)
            source = public_source(path, extension, client)
            for seed, replace in ((7, False), (29, True)):
                options = {"n": 9, "seed": seed, "replace": replace}
                # Numeric pivot deliberately rejects observed null values;
                # select its supported population explicitly on every surface.
                relation = native.Relation.scan(str(path)).filter(native.Expression.column("amount").is_not_null())
                relation = relation.unary("pivot", json.dumps(pivot)).unary("melt", json.dumps(melt))
                relation = relation.sort([("entity", False, "last"), ("variable", False, "last")])
                relation = relation.unary("sample", json.dumps(options))
                declaration = (source.filter("amount IS NOT NULL")
                    .pivot_table(index="entity", columns="category", values="amount", aggfunc="sum")
                    .melt(id_vars=["entity"], value_vars=["pivot_a", "pivot_b"])
                    .sort("entity", "variable").sample(n=9, seed=seed, replace=replace))
                pivot_sql = f"SELECT * FROM PIVOT((SELECT * FROM '{path}' WHERE amount IS NOT NULL), '{json.dumps(pivot)}') AS p"
                melt_sql = f"SELECT * FROM MELT(({pivot_sql}), '{json.dumps(melt)}') AS m ORDER BY entity,variable"
                statement = f"SELECT * FROM SAMPLE(({melt_sql}), '{json.dumps(options)}') AS sampled"
                wanted_rows = [population[index] for index in sample_ordinals(len(population), 9, seed, replace=replace)]
                with test.subTest(format=extension, seed=seed, replace=replace):
                    results = test.all_surfaces(declaration, relation, statement,
                        file_binding=(path, "arrow-ipc" if extension == "arrow" else extension))
                    for table in results:
                        test.assertTrue(table.schema.equals(wanted_schema, check_metadata=True), (table.schema, wanted_schema))
                        test.assertEqual(table.to_pylist(), wanted_rows)


def check_correlated_membership_compositions(test):
    """Independent three-valued answers after per-row correlation and filtering."""
    import shardloom as sl

    pa, native = test.pa, test.native
    rows = [dict(id=index, k=None if index % 7 == 0 else index % 4,
                 n=None if index % 5 == 0 else index % 9 - 3) for index in range(23)]
    schema = pa.schema([pa.field("id", pa.int64(), False), pa.field("k", pa.int64()),
                        pa.field("n", pa.int64())])
    client = sl.ShardLoomClient(transport="native", memory_bytes=128 << 20, max_parallelism=2)
    test.addCleanup(client.close)
    with tempfile.TemporaryDirectory(prefix="shardloom-correlated-breadth-") as directory:
        for extension in ("vortex", "parquet", "arrow"):
            path = Path(directory) / ("input." + extension)
            write_fixture(test, path, pa.Table.from_pylist(rows, schema=schema), extension)
            source = public_source(path, extension, client)
            for kind in ("exists", "in", "not_in"):
                wanted_rows = []
                for row in reversed(rows):
                    candidates = [other["n"] for other in rows
                        if row["k"] is not None and other["k"] == row["k"]
                        and other["n"] is not None and other["n"] >= 0]
                    if kind == "exists":
                        answer = bool(candidates)
                    elif not candidates:
                        answer = False
                    elif row["n"] is None:
                        answer = None
                    else:
                        answer = row["n"] in candidates
                    if kind == "not_in" and answer is not None:
                        answer = not answer
                    wanted_rows.append(dict(row, answer=answer))
                wanted_rows = wanted_rows[:17]
                wanted_schema = schema.append(pa.field("answer", pa.bool_(), kind != "exists"))
                e = native.Expression
                inner = native.Relation.scan(str(path)).filter(e.column("n") >= e.literal(0))
                inner = inner.project([("k", e.column("k")), ("n", e.column("n"))])
                relation = native.Relation.scan(str(path)).subquery(inner, "answer",
                    kind="exists" if kind == "exists" else "in",
                    keys=[] if kind == "exists" else [("n", "n")],
                    correlation=[("k", "k")], negated=kind == "not_in")
                relation = relation.sort([("id", True, "last")]).limit(17)
                inner_sql = f"SELECT n FROM '{path}' WHERE k = outer.k AND n >= 0"
                expression = (f"EXISTS ({inner_sql})" if kind == "exists" else
                              f"n {'NOT IN' if kind == 'not_in' else 'IN'} ({inner_sql})")
                declaration = source.select("id", "k", "n", expression + " AS answer").sort(
                    "id", descending=True, nulls="last").limit(17)
                statement = (f"SELECT id,k,n,{expression} AS answer FROM '{path}' "
                             "ORDER BY id DESC NULLS LAST LIMIT 17")
                with test.subTest(format=extension, kind=kind):
                    for table in test.all_surfaces(declaration, relation, statement,
                            file_binding=(path, "arrow-ipc" if extension == "arrow" else extension)):
                        test.assertTrue(table.schema.equals(wanted_schema, check_metadata=True),
                                        (table.schema, wanted_schema))
                        test.assertEqual(table.to_pylist(), wanted_rows)


def check_vector_tensor_variant_composition(test):
    """Mixed native representations feed framed selection and finite Sort output."""
    import shardloom as sl
    from shardloom import functions as f
    if __package__:
        from .test_native_variant_expression_binding import _fixture
        from .vector_fixture import tagged
    else:
        from test_native_variant_expression_binding import _fixture
        from vector_fixture import tagged

    pa, native = test.pa, test.native
    table, variant_rows = _fixture(37)
    vectors = [None if index % 7 == 0 else [0.0, 0.0] if index % 3 == 0 else [3.0, 4.0]
               for index in range(37)]
    tensors = [None if index % 6 == 0 else [1, 2, 3, 4, 5, 6] for index in range(37)]
    table = table.append_column(pa.field("v", pa.list_(pa.float32(), 2)),
                                pa.array(vectors, pa.list_(pa.float32(), 2)))
    table = table.append_column(tagged(pa, "tensor", pa.list_(pa.int16(), 6),
        "arrow.fixed_shape_tensor", {"shape": [2, 3], "permutation": [1, 0]}),
        pa.array(tensors, pa.list_(pa.int16(), 6)))
    # Null parents, zero vectors, exact integer norms and permuted tensor axes
    # all have literal expected values, independent of the runtime output.
    values = [dict(id=index, k=None if row["n"] is None else index % 5, n=row["n"],
        norm=None if vectors[index] is None else 0.0 if index % 3 == 0 else 5.0,
        shape=None if tensors[index] is None else [3, 2]) for index, row in enumerate(variant_rows)]
    ranks = {}
    for key in (None, 0, 1, 2, 3, 4):
        group = sorted((row for row in values if row["k"] == key),
            key=lambda row: (row["norm"] is None, -(row["norm"] or 0.0), row["id"]))
        ranks.update((row["id"], rank) for rank, row in enumerate(group, 1))
    wanted_rows = [dict(row, rank=ranks[row["id"]]) for row in reversed(values)
                   if ranks[row["id"]] <= 2][:9]
    wanted_schema = pa.schema([pa.field("id", pa.int64()), pa.field("k", pa.int64()),
        pa.field("n", pa.int64()), pa.field("norm", pa.float64()),
        pa.field("shape", pa.list_(pa.field("item", pa.uint64(), False), 2)),
        pa.field("rank", pa.int64(), False)])
    client = sl.ShardLoomClient(transport="native", memory_bytes=128 << 20, max_parallelism=2)
    test.addCleanup(client.close)
    with tempfile.TemporaryDirectory(prefix="shardloom-rich-breadth-") as directory:
        for extension in ("vortex", "parquet", "arrow"):
            path = Path(directory) / ("input." + extension)
            write_fixture(test, path, table, extension)
            source = public_source(path, extension, client)
            projections = [f.try_variant_get("payload", "$.key", "int64").alias("k"),
                f.try_variant_get("payload", "$.number", "int64").alias("n"),
                f.vector_norm("v").alias("norm"), f.tensor_shape("tensor").alias("shape")]
            window = "ROW_NUMBER() OVER (PARTITION BY k ORDER BY norm DESC NULLS LAST,id) AS rank"
            declaration = source.select("id", *projections).window(window).filter("rank <= 2").sort(
                "id", descending=True, nulls="last").limit(9)
            e = native.Expression
            get = lambda path: e.call("try_variant_get", [e.column("payload"), e.literal(path), e.literal("int64")])
            relation = native.Relation.scan(str(path)).project([
                ("id", e.column("id")), ("k", get("$.key")), ("n", get("$.number")),
                ("norm", e.call("vector_norm", [e.column("v")])),
                ("shape", e.call("tensor_shape", [e.column("tensor")]))])
            relation = relation.window(["id", "k", "n", "norm", "shape"], [native.WindowExpression(
                "row_number", "rank", partition_by=["k"], order_by=[("norm", True, "last"), ("id", False, "last")])])
            relation = relation.filter(e.column("rank") <= e.literal(2)).sort([("id", True, "last")]).limit(9)
            first = f"SELECT id,{','.join(map(str, projections))} FROM '{path}'"
            second = f"SELECT *,{window} FROM ({first}) AS computed"
            statement = f"SELECT * FROM ({second}) AS ranked WHERE rank <= 2 ORDER BY id DESC NULLS LAST LIMIT 9"
            with test.subTest(format=extension):
                for result in test.all_surfaces(declaration, relation, statement,
                        file_binding=(path, "arrow-ipc" if extension == "arrow" else extension)):
                    test.assertTrue(result.schema.equals(wanted_schema, check_metadata=True),
                                    (result.schema, wanted_schema))
                    test.assertEqual(result.to_pylist(), wanted_rows)
