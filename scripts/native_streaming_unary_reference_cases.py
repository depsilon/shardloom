# SPDX-License-Identifier: Apache-2.0
"""Public unary cases using the predeclared sampling and rolling reference matrix."""

import json
from pathlib import Path

from shardloom._result_schema import ResultType as T
from shardloom.query import SqlWorkflow
from native_streaming_unary_cases import consume, frame, payload
from native_unary_reference import rolling_values, sample_ordinals


def run(harness):
    reference = json.loads(Path(__file__).with_name("native_streaming_unary_reference_fixtures.json").read_text())
    assert reference["status"] == "predeclared_only" and reference["engine_executions"] == 0
    assert len(reference["sampling_cases"]) == 54 and len(reference["rolling_cases"]) == 120
    # Verify every stored oracle before any engine call, then retain that exact input.
    for case in reference["sampling_cases"]:
        rows = case.get("rows", reference["sampling_rows"])
        options = case["options"]
        target = options.get("n", case.get("declared_target"))
        selected = sample_ordinals(len(rows), target, options["random_state"],
            weights=[row["weight"] for row in rows] if options.get("weights") else None,
            replace=options.get("replace", False))
        assert selected == case["ordinals"]
        assert [rows[index] for index in selected] == case["expected"]
    for case in reference["rolling_cases"]:
        assert rolling_values(reference["rolling_values"], case["window"], case["min_periods"],
                              case["aggregate"], center=case["center"]) == case["expected"]
    harness.json(harness.logs / "unary-sampling-rolling-predeclared.json", reference)
    sample_schema = {"id": "int64", "value": "int64", "weight": "float64"}
    sample_fields = tuple((name, T(dtype, True)) for name, dtype in sample_schema.items())
    for ordinal, case in enumerate(reference["sampling_cases"]):
        for surface in ("dataframe", "sql"):
            name = "unary-reference-" + case["name"] + "-" + surface

            def sample(name=name, case=case, surface=surface, ordinal=ordinal):
                rows = case.get("rows", reference["sampling_rows"])
                source, trace = frame(harness, rows, sample_schema, width=(1, 3, 8)[ordinal % 3])
                if surface == "dataframe":
                    query = source.sample(**case["options"])
                else:
                    rename = {"random_state": "seed", "frac": "fraction"}
                    options = {rename.get(key, key): value for key, value in case["options"].items()
                               if value is not None}
                    query = SqlWorkflow(f"SELECT * FROM SAMPLE(({source._relation_statement()}), {payload(options)}) AS u",
                                        source.client, source_bindings=source._declared_sources())
                consume(harness, name, query, case["expected"], sample_fields, [trace])
            harness.case(name, sample)
    for ordinal, case in enumerate(reference["rolling_cases"]):
        for surface in ("dataframe", "sql"):
            name = "unary-reference-" + case["name"] + "-" + surface

            def rolling(name=name, case=case, surface=surface, ordinal=ordinal):
                rows = [{"value": value} for value in reference["rolling_values"]]
                source, trace = frame(harness, rows, {"value": "int64"}, width=(1, 3, 8)[ordinal % 3])
                if surface == "dataframe":
                    builder = source.rolling(case["window"], min_periods=case["min_periods"], center=case["center"])
                    query = getattr(builder, case["aggregate"])("value", alias="result")
                else:
                    options = {"source_column": "value", "output_column": "result",
                               "window_size": case["window"], "min_periods": case["min_periods"],
                               "center": case["center"], "aggregate": case["aggregate"]}
                    query = SqlWorkflow(f"SELECT * FROM ROLLING(({source._relation_statement()}), {payload(options)}) AS u",
                                        source.client, source_bindings=source._declared_sources())
                fields = (("result", T("uint64" if case["aggregate"] == "count" else "float64", False)),)
                consume(harness, name, query, [{"result": value} for value in case["expected"]], fields, [trace])
            harness.case(name, rolling)
