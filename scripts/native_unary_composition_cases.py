# SPDX-License-Identifier: Apache-2.0
"""Complete public unary composition values, resource forwarding and writer proof."""

from __future__ import annotations

import csv
import json

from run_clickbench_query_uat import file_sha256, strict_json
from run_native_unary_uat import csv_cell


def run(context, output, guard, accepted, complete, sources, identity):
    import shardloom as sl
    from shardloom.query import SqlWorkflow

    output.mkdir(parents=True)
    resources = {"memory_gb": 1, "max_parallelism": 2}

    def literal(value):
        return "'" + str(value).replace("'", "''") + "'"

    def payload(value):
        return literal(json.dumps(value, separators=(",", ":")))

    def remember(*paths):
        sources.extend((path, file_sha256(path), identity(path)) for path in paths)

    def verified(name, report):
        envelope = accepted(name, report)
        if envelope.field("public_workflow_memory_gb") != "1":
            raise ValueError(f"{name}: lost the declared memory grant")
        if envelope.field("public_workflow_native_vortex_provider_scenario") != "none":
            raise ValueError(f"{name}: selected an unrelated schema-specific provider")
        return envelope

    def equal(name, actual, expected, destination=None):
        if actual != expected:
            first = next((index for index, pair in enumerate(zip(actual, expected)) if pair[0] != pair[1]), None)
            raise ValueError(f"{name}: complete values differ; rows={len(actual)}/{len(expected)}, first mismatch={first}")
        complete(name, actual, expected, destination)

    def write_all(family, workflow, expected, columns):
        for extension in ("vortex", "parquet", "arrow_ipc", "avro", "orc", "json", "jsonl", "csv"):
            guard()
            name = f"{family}-{extension}"
            destination = output / f"{name}.{extension}"
            verified(name, getattr(workflow, f"write_{extension}")(destination, check=False, **resources))
            if extension == "csv":
                with destination.open(newline="") as stream:
                    reader = csv.DictReader(stream)
                    if reader.fieldnames != columns:
                        raise ValueError(f"{name}: CSV column order differs")
                    actual = list(reader)
                equal(name, actual, [{key: csv_cell(value) for key, value in row.items()} for row in expected], destination)
                continue
            if extension == "json":
                actual = strict_json(destination.read_text())
            else:
                decoded = destination
                if extension != "jsonl":
                    reopened = destination
                    if extension != "vortex":
                        reopened = output / f"{name}-normalized.vortex"
                        accepted(f"{name}-normalize", getattr(context, f"read_{extension}")(
                            destination).prepare(reopened, check=False))
                    schema = accepted(f"{name}-schema", context.sql(
                        f"SELECT * FROM (SELECT * FROM {literal(reopened)}) AS reopened LIMIT 0"
                    ).collect(check=False))
                    if schema.field("output_columns") != ",".join(columns):
                        raise ValueError(f"{name}: reopened column order differs")
                    decoded = output / f"{name}-reopened.jsonl"
                    accepted(f"{name}-read", context.read_vortex(reopened).write_jsonl(decoded, check=False))
                actual = [strict_json(line) for line in decoded.read_text().splitlines()]
            equal(name, actual, expected, destination)

    raw, native, typed = (output / name for name in ("readings.jsonl", "readings.vortex", "readings.data"))
    original = list(zip([2, 1, 2, None, 1, 3], [10, 11, 12, 13, 14, 15]))
    prefix_rows = [{"id": key, "value": value} for key, value in [(3, 15), (1, 14), (None, 13), (2, 12), (1, 11)]]
    expected = {
        "distinct": [{"id": key} for key in [3, 1, None, 2]],
        "tail": [prefix_rows[3]],
        "drop-first": [prefix_rows[index] for index in [0, 1, 2, 3]],
        "drop-last": [prefix_rows[index] for index in [0, 2, 3, 4]],
        "drop-false": [prefix_rows[index] for index in [0, 2, 3]],
        "mask-first": [{"duplicated": value} for value in [False, False, False, False, True]],
        "mask-last": [{"duplicated": value} for value in [False, True, False, False, False]],
        "mask-false": [{"duplicated": value} for value in [False, True, False, False, True]],
        "sample-count": [prefix_rows[index] for index in [1, 4]],
        "sample-fraction": [prefix_rows[index] for index in [1, 4]],
        "sample-weighted": [prefix_rows[index] for index in [1, 4]],
        "sample-replacement": [prefix_rows[index] for index in [3, 1, 4, 3, 3, 1, 4]],
        "rewrite-index": [{"id": row["id"], "position": index} for index, row in enumerate(prefix_rows)],
        "rewrite-fill": [{"id": key} for key in [3, 1, 1, 2, 1]],
        "rewrite-mask": [{"value": value} for value in [15, 14, 13, 0, 0]],
        "rewrite-mask-null": [{"id": value} for value in [3, 1, 0, 2, 1]],
        "rewrite-replace": [{"value": value} for value in [15, 99, 13, 12, 11]],
        "rewrite-replace-null": [{"id": value} for value in [3, 9, None, 2, 9]],
        "melt": [{"id": row["id"], "variable": "value", "value": row["value"]} for row in prefix_rows],
        "rolling": [{"total": value} for value in [15.0, 29.0, 27.0, 25.0, 23.0]],
        "rolling-centered": [{"total": value} for value in [29.0, 42.0, 39.0, 36.0, 23.0]],
        "successive": [{"id": 2, "value": 12, "index": 0}, {"id": 1, "value": 11, "index": 1}],
        "aggregate-input": [{"id": 1, "total": 25.0}, {"id": 2, "total": 22.0}],
        "join-input": [{"id": 1, "value": 11}, {"id": 2, "value": 12}],
        "set-input": [{"id": 2}, {"id": 1}, {"id": 3}],
        "empty": [],
    }
    expected_file = output / "expected.json"
    expected_file.write_text(json.dumps(expected, indent=2) + "\n")
    raw.write_text("".join(json.dumps({"key": key, "amount": value}) + "\n" for key, value in original))
    with typed.open("x", newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow(["key", "amount"])
        writer.writerows(original)
    remember(expected_file, raw, typed)
    guard()
    accepted("unary-prepare", context.read_json(raw).prepare(native, check=False))
    remember(native)

    for source_name, base in [
        ("native", context.read_vortex(native, schema={"key": "uint64", "amount": "uint64"})),
        ("declared-csv", context.read_csv(typed, schema={"key": "int64", "amount": "int64"})),
    ]:
        prefix = base.sort("amount", descending=True).limit(5).select("key AS id", "amount AS value")
        source_sql = literal(base.source.uri)
        input_sql = f"SELECT key AS id,amount AS value FROM {source_sql} ORDER BY amount DESC LIMIT 5"

        def table(function, arguments, input_query=input_sql, projection="*", suffix=""):
            return f"SELECT {projection} FROM {function}(({input_query}), {arguments}) AS u{suffix}"

        cases = [
            ("distinct", prefix.select("id").distinct().select("id"), table("DISTINCT_ROWS", "'id'"), ["id"]),
            ("tail", prefix.tail(2).limit(1), table("TAIL", "2", suffix=" LIMIT 1"), ["id", "value"]),
        ]
        for keep in ["first", "last", False]:
            text = "false" if keep is False else keep
            cases.extend([
                (f"drop-{text}", prefix.drop_duplicates("id", keep=keep).select("id", "value"),
                 table("DROP_DUPLICATES", f"'id', '{text}'"), ["id", "value"]),
                (f"mask-{text}", prefix.duplicated("id", keep=keep).select("duplicated"),
                 table("DUPLICATED", f"'id', '{text}'"), ["duplicated"]),
            ])
        for label, options, dataframe in [
            ("count", {"n": 2, "seed": 7}, prefix.sample(2, seed=7)),
            ("fraction", {"fraction": 0.4, "seed": 7}, prefix.sample(frac=0.4, seed=7)),
            ("replacement", {"n": 7, "seed": 11, "replace": True}, prefix.sample(7, seed=11, replace=True)),
        ]:
            cases.append((f"sample-{label}", dataframe.select("id", "value"), table("SAMPLE", payload(options)), ["id", "value"]))
        unit_sql = f"SELECT *,1 AS weight FROM ({input_sql}) AS p"
        cases.append(("sample-weighted", prefix.with_column("weight", 1).sample(frac=0.4, weights="weight", seed=7).select("id", "value"),
                      table("SAMPLE", payload({"fraction": 0.4, "weights": "weight", "seed": 7}), unit_sql, "id,value"), ["id", "value"]))
        index = {"columns": ["id", "value"], "rewrites": [{"kind": "row_number", "target_column": "position", "start": 0}]}
        fill = {"columns": ["id", "value"], "rewrites": [{"kind": "forward_fill_null", "target_column": "id", "limit": 2}]}
        mask = {"columns": ["id", "value"], "rewrites": [{"kind": "mask_scalar", "target_column": "value", "predicate": "lt:value:13", "replacement": {"type": "int64", "value": 0}}]}
        mask_null = {"columns": ["id", "value"], "rewrites": [{"kind": "mask_scalar", "target_column": "id", "predicate": "is_null:id", "replacement": {"type": "int64", "value": 0}}]}
        replace = {"columns": ["id", "value"], "rewrites": [{"kind": "replace_scalar", "target_column": "value", "to_replace": {"type": "int64", "value": 14}, "replacement": {"type": "int64", "value": 99}}]}
        replace_null = {"columns": ["id", "value"], "rewrites": [{"kind": "replace_scalar", "target_column": "id", "to_replace": {"type": "int64", "value": 1}, "replacement": {"type": "int64", "value": 9}}]}
        melt = {"id_columns": ["id"], "value_columns": ["value"], "variable_column": "variable", "value_column": "value"}
        rolling = {"source_column": "value", "output_column": "total", "window_size": 2, "min_periods": 1, "aggregate": "sum"}
        centered = dict(rolling, window_size=3, center=True)
        cases.extend([
            ("rewrite-index", prefix.reset_index().select("id", "index AS position"), table("REWRITE", payload(index), projection="id,position"), ["id", "position"]),
            ("rewrite-fill", prefix.fillna(method="ffill", limit=2).select("id"), table("REWRITE", payload(fill), projection="id"), ["id"]),
            ("rewrite-mask", prefix.mask(sl.col("value") < 13, 0).select("value"), table("REWRITE", payload(mask), projection="value"), ["value"]),
            ("rewrite-mask-null", prefix.mask(sl.col("id").is_null(), 0).select("id"), table("REWRITE", payload(mask_null), projection="id"), ["id"]),
            ("rewrite-replace", prefix.replace({"value": {14: 99}}).select("value"), table("REWRITE", payload(replace), projection="value"), ["value"]),
            ("rewrite-replace-null", prefix.replace({"id": {1: 9}}).select("id"), table("REWRITE", payload(replace_null), projection="id"), ["id"]),
            ("melt", prefix.melt(id_vars="id", value_vars="value").select("id", "variable", "value"), table("MELT", payload(melt)), ["id", "variable", "value"]),
            ("rolling", prefix.rolling(2, min_periods=1).sum("value", alias="total").select("total"), table("ROLLING", payload(rolling)), ["total"]),
            ("rolling-centered", prefix.rolling(3, min_periods=1, center=True).sum("value", alias="total").select("total"), table("ROLLING", payload(centered)), ["total"]),
        ])
        suffix = prefix.tail(2).drop_duplicates("id").reset_index().select("id", "value", "index")
        grouped = base.group_by("key").agg(total="sum(amount)").sort("total", descending=True).limit(2).select("key AS id", "total").tail(2)
        right = prefix.select("id").drop_duplicates("id")
        joined = prefix.tail(2).join(right, on="id").select("f.id AS id", "f.value AS value").sort("id")
        combined = prefix.tail(2).select("id").union_all(prefix.limit(1).select("id"))
        empty = prefix.filter(sl.col("value") < 0).tail(2).select("id", "value")
        for label, frame, columns in [
            ("successive", suffix, ["id", "value", "index"]),
            ("aggregate-input", grouped, ["id", "total"]),
            ("join-input", joined, ["id", "value"]),
            ("set-input", combined, ["id"]),
            ("empty", empty, ["id", "value"]),
        ]:
            cases.append((label, frame, frame._relation_statement(), columns))
        for label, frame, statement, columns in cases:
            for spelling, workflow in [("dataframe", frame), ("sql", SqlWorkflow(statement, context.client, source_bindings=base._declared_sources()))]:
                family = f"unary-{source_name}-{spelling}-{label}"
                inspected = workflow.route(bounded=True, check=False, **resources)
                (output / f"{family}-route.json").write_text(json.dumps(inspected.envelope.raw, indent=2) + "\n")
                if (inspected.route_status != "admitted" or not inspected.side_effect_free
                        or inspected.fallback_attempted or inspected.external_engine_invoked):
                    raise ValueError(f"{family}: route inspection was not admitted and inert")
                for parallelism in (1, 2):
                    guard()
                    name = f"{family}-collect-{parallelism}"
                    report = workflow.collect(check=False, **dict(resources, max_parallelism=parallelism))
                    if verified(name, report).field("result_payload_complete") != "true":
                        raise ValueError(f"{name}: incomplete collection")
                    equal(name, list(report.result_rows), expected[label])
                write_all(family, workflow, expected[label], columns)

    # Quotes survive the real public source resolver, nested predicate and JSON payload.
    quoted = output / "o'clock,(join).data"
    with quoted.open("x", newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow(["position", "label"])
        writer.writerows([(0, "isn't,(join)"), (1, "東京"), (2, "keep"), (3, None)])
    remember(quoted)
    text = context.read_csv(quoted, schema={"position": "int64", "label": "utf8"})
    rewrite = text.sort("position").limit(2).filter(
        (sl.col("label") == "isn't,(join)") | (sl.col("label") == "東京")
    ).replace({"label": {"isn't,(join)": "it's fine", "東京": "'東京's'"}}).select("label", "'o''clock 東京' AS marker")
    quoted_expected = [{"label": label, "marker": "o'clock 東京"} for label in ["it's fine", "'東京's'"]]
    for spelling, workflow in [("dataframe", rewrite), ("sql", SqlWorkflow(rewrite._relation_statement(), context.client, source_bindings=text._declared_sources()))]:
        family = f"unary-quoted-{spelling}"
        report = workflow.collect(check=False, **resources)
        verified(family, report)
        equal(family, list(report.result_rows), quoted_expected)
        write_all(family, workflow, quoted_expected, ["label", "marker"])
    nullable_text = text.sort("position").limit(4).replace({"label": {"isn't,(join)": "it's fine"}}).select("label")
    nullable_expected = [{"label": value} for value in ["it's fine", "東京", "keep", None]]
    for spelling, workflow in [("dataframe", nullable_text), ("sql", SqlWorkflow(nullable_text._relation_statement(), context.client, source_bindings=text._declared_sources()))]:
        family = f"unary-nullable-text-{spelling}"
        report = workflow.collect(check=False, **resources)
        verified(family, report)
        equal(family, list(report.result_rows), nullable_expected)
        write_all(family, workflow, nullable_expected, ["label"])

    # This tied-score expectation is independent of candidate capacity and includes
    # a rejected zero weight removed by the preceding filter.
    weights = output / "weights.data"
    weights.write_text("id,weight\n0,0\n1,5e-324\n2,5e-324\n3,5e-324\n4,1\n")
    remember(weights)
    weighted = context.read_csv(weights, schema={"id": "int64", "weight": "float64"})
    for fraction, replacement in [(False, False), (True, False), (False, True), (True, True)]:
        parameters = {"frac": 0.5} if fraction else {"n": 7 if replacement else 2}
        frame = weighted.sort("id").filter(sl.col("id") > 0).sample(weights="weight", seed=7, replace=replacement, **parameters).select("id")
        selected = [4] * (2 if fraction else 7) if replacement else [1, 4]
        sample_expected = [{"id": value} for value in selected]
        for spelling, workflow in [("dataframe", frame), ("sql", SqlWorkflow(frame._relation_statement(), context.client, source_bindings=weighted._declared_sources()))]:
            family = f"unary-ties-{fraction}-replacement-{replacement}-{spelling}"
            report = workflow.collect(check=False, **resources)
            verified(family, report)
            equal(family, list(report.result_rows), sample_expected)
            write_all(family, workflow, sample_expected, ["id"])

    # A single ordered chain exercises all eight families beyond small collection.
    count = 65_541
    large_raw, large = output / "large.jsonl", output / "large.vortex"
    large_expected = [{"variable": "total", "value": float(1 if index == 0 else 2 * count - 3 if index == count - 1 else 3 * index)} for index in range(count)]
    large_oracle = output / "large-expected.json"
    large_oracle.write_text(json.dumps(large_expected) + "\n")
    with large_raw.open("x") as stream:
        for index in range(count):
            stream.write(json.dumps({"identifier": index}) + "\n")
    remember(large_raw, large_oracle)
    guard()
    accepted("unary-large-prepare", context.read_json(large_raw).prepare(large, check=False))
    remember(large)
    large_base = context.read_vortex(large, schema={"identifier": "uint64"})
    chain = (large_base.sort("identifier", descending=True).select("identifier").distinct()
             .drop_duplicates("identifier").tail(count).sample(frac=1.0, seed=7)
             .duplicated("identifier").reset_index()
             .rolling(3, min_periods=1, center=True).sum("index", alias="total")
             .melt(value_vars="total").select("variable", "value"))
    for spelling, workflow in [("dataframe", chain), ("sql", SqlWorkflow(chain._relation_statement(), context.client, source_bindings=large_base._declared_sources()))]:
        family = f"unary-large-{spelling}"
        report = workflow.limit(97).collect(check=False, **resources)
        verified(f"{family}-limited", report)
        equal(f"{family}-limited", list(report.result_rows), large_expected[:97])
        denial = workflow.collect(check=False, **resources).envelope
        (output / f"{family}-denial.json").write_text(json.dumps(denial.raw, indent=2) + "\n")
        if (denial.status != "error" or denial.fallback.attempted
                or denial.raw.get("certificates") or denial.raw.get("artifacts")
                or not any("collect exceeds 65,536 rows" in item.get("reason", "")
                           for item in denial.raw.get("diagnostics", []))):
            raise ValueError(f"{family}: collection ignored its row limit")
        write_all(family, workflow, large_expected, ["variable", "value"])

    for label, statement, bindings in [
        ("zero-weight", f"SELECT * FROM SAMPLE((SELECT * FROM {literal(weights)}), '{{\"n\":2,\"weights\":\"weight\"}}') AS u", weighted._declared_sources()),
        ("missing-column", f"SELECT * FROM TAIL((SELECT amount FROM {literal(native)}), 2) AS u WHERE missing > 0", ()),
        ("bad-options", f"SELECT * FROM SAMPLE((SELECT * FROM {literal(native)}), '{{\"n\":2,\"fraction\":0.5}}') AS u", ()),
        ("nested-pivot", f"SELECT * FROM PIVOT((SELECT * FROM {literal(native)}), '{{}}') AS u", ()),
    ]:
        guard()
        destination = output / f"must-not-publish-{label}.vortex"
        denial = SqlWorkflow(statement, context.client, source_bindings=bindings).write_vortex(destination, check=False, **resources).envelope
        (output / f"{label}-denial.json").write_text(json.dumps(denial.raw, indent=2) + "\n")
        if (denial.status not in ("error", "unsupported") or denial.fallback.attempted
                or denial.raw.get("certificates") or denial.raw.get("artifacts") or destination.exists()):
            raise ValueError(f"{label}: invalid unary operation published output or success evidence")
    guard()
