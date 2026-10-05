"""Front doors declare work; the native engine owns admission and execution.

These tests inspect transport requests without pretending to evaluate them.
Complete result oracles live in the native integration and parameterized UAT suites.
"""
from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
import shardloom as sl
from shardloom.models import OutputEnvelope


FORMATS = ("vortex", "csv", "json", "jsonl", "parquet", "arrow_ipc", "avro", "orc")
SCHEMA = {"id": "int64", "amount": "int64", "label": "utf8", "weight": "float64"}
RESOURCES = {"memory_gb": 3, "max_parallelism": 2, "spill": "disabled"}


class NativeWorkflowDeclarationTests(unittest.TestCase):
    def setUp(self):
        self.client = sl.ShardLoomClient(binary="unused-shardloom")
        self.context = sl.ShardLoomContext(self.client)
        self.envelope = OutputEnvelope.from_field_mapping({
            "result_jsonl": "", "result_payload_complete": "true", "output_row_count": "0",
            "result_schema_format": "vortex.dtype.serde.v1",
            "result_schema_json": json.dumps({"Struct": [{"names": ["id"],
                "dtypes": [{"Primitive": ["i64", True]}]}, False]}),
            "fallback_attempted": "false", "external_engine_invoked": "false",
        }, command="run")
        self.reply = SimpleNamespace(envelope=self.envelope)

    def reader(self, kind, *, schema=SCHEMA):
        reader = "read_json" if kind == "jsonl" else f"read_{kind}"
        suffix = ".jsonl" if kind == "jsonl" else ".data"
        return getattr(self.context, reader)("source" + suffix, schema=schema)

    def assert_request(self, request, fragments):
        self.assertIsInstance(request["sql_statement"], str)
        for fragment in fragments:
            self.assertIn(fragment, request["sql_statement"])
        for key, value in RESOURCES.items():
            self.assertEqual(request[key], value)
        self.assertEqual(request["materialization_policy"], "bounded")
        self.assertFalse(any(key.startswith("vortex_") for key in request))
        self.assertFalse(any(key.startswith("generated_") for key in request))

    @staticmethod
    def unary_cases():
        return [
            ("projection", lambda f: f.select("id", "label"), ("SELECT id,label",)),
            ("filter", lambda f: f.filter(sl.col("id") > 0).select("id"), ("WHERE id > 0",)),
            ("distinct", lambda f: f.select("id", "label").distinct(), ("DISTINCT",)),
            ("tail", lambda f: f.select("id", "label").tail(2), ("FROM TAIL((", ", 2)")),
            ("sample-n", lambda f: f.sample(n=2, seed=7), ("FROM SAMPLE((", '"n":2', '"seed":7')),
            ("sample-state", lambda f: f.sample(n=2, random_state=11), ('"seed":11', '"replace":false')),
            ("sample-replace", lambda f: f.sample(n=4, seed=13, replace=True), ('"n":4', '"replace":true')),
            ("sample-fraction", lambda f: f.sample(frac=0.5, random_state=3), ('"fraction":0.5', '"seed":3')),
            ("sample-fraction-replace", lambda f: f.sample(frac=0.5, seed=3, replace=True),
             ('"fraction":0.5', '"replace":true')),
            ("sample-weight", lambda f: f.sample(n=2, weights="weight", seed=3), ('"weights":"weight"',)),
            ("mask", lambda f: f.select("id", "amount").mask(sl.col("amount") < 0, other=0),
             ("FROM REWRITE((", '"kind":"mask_scalar"')),
            ("mask-null", lambda f: f.select("amount").mask(sl.col("amount") < 0),
             ("FROM REWRITE((", '"type":"null"')),
            ("forward-fill", lambda f: f.select("amount").fillna(method="ffill", limit=2),
             ("FROM REWRITE((", '"limit":2')),
            ("replace", lambda f: f.select("label").replace("bad", "good"),
             ("FROM REWRITE((", '"value":"bad"', '"value":"good"')),
            ("replace-null", lambda f: f.select("label").replace("bad", None),
             ("FROM REWRITE((", '"type":"null"')),
            ("replace-regex", lambda f: f.select("label").replace("^bad", "ok", regex=True),
             ("FROM REWRITE((", "^bad")),
            ("string-replace", lambda f: f.select("label").with_column("label", sl.col("label").replace("bad", "ok")),
             ("REPLACE(label", "'bad'", "'ok'")),
            ("eval", lambda f: f.select("id", "amount").eval("amount = amount + 5"),
             ('"kind":"numeric_scalar_arithmetic"', '"operator":"+"', '"value":5')),
            ("transform", lambda f: f.select("amount").transform({"amount": sl.col("amount") * 2}), ("amount * 2",)),
            ("map", lambda f: f.select("amount").map(sl.column_transform(amount=sl.col("amount") + 1)), ("amount + 1",)),
            ("applymap", lambda f: f.select("amount").applymap(sl.column_transform(amount=sl.col("amount") - 1)), ("amount - 1",)),
            ("map-rows", lambda f: f.select("amount").map_rows(sl.row_transform(amount=sl.col("amount") + 2)), ("amount + 2",)),
            ("udf", lambda f: f.select("amount").map(sl.column_transform(amount=sl.fixture_double_i64(sl.col("amount")))),
             ("amount * 2",)),
            ("set-index", lambda f: f.select("id", "label").set_index("id", drop=False), ("SELECT id,label",)),
            ("reset-index", lambda f: f.select("id", "amount").reset_index(), ("FROM REWRITE((", '"kind":"row_number"')),
            ("sort", lambda f: f.sort("amount", descending=True), ("ORDER BY amount DESC",)),
            ("top-k", lambda f: f.nlargest(3, "amount"), ("ORDER BY amount DESC", "LIMIT 3")),
            ("bottom-k", lambda f: f.nsmallest(2, "amount"), ("ORDER BY amount ASC", "LIMIT 2")),
            ("rename", lambda f: f.rename(columns={"amount": "total"}).filter(sl.col("total") > 0),
             ("amount AS total", "WHERE")),
            ("drop", lambda f: f.drop("weight"), ("SELECT id,amount,label",)),
            ("dropna", lambda f: f.dropna(subset=["amount"]), ("amount IS NOT NULL",)),
            ("cast", lambda f: f.with_column("amount", sl.col("amount").cast("float64")), ("CAST(amount AS float64)",)),
            ("contains", lambda f: f.filter(sl.col("label").contains("needle")), ("LIKE '%needle%'",)),
            ("aggregate", lambda f: f.aggregate("SUM(amount) AS total"), ("SUM(amount) AS total",)),
            ("grouped", lambda f: f.groupby("label").agg("COUNT(*) AS n", "SUM(amount) AS total"),
             ("COUNT(*) AS n", "SUM(amount) AS total", "GROUP BY label")),
            ("pipe", lambda f: f.pipe(sl.plan_transform(lambda x: x.select("id").limit(2), name="ids")),
             ("SELECT id", "LIMIT 2")),
            ("apply", lambda f: f.apply(sl.plan_transform(lambda x: x.select("id").limit(2), name="ids")),
             ("SELECT id", "LIMIT 2")),
        ]

    def test_input_adapters_and_unary_workflows_submit_complete_declarations(self):
        cases = self.unary_cases()
        for keep, spelling in [("first", "first"), ("last", "last"), (False, "false")]:
            for method, function in [("drop_duplicates", "DROP_DUPLICATES"), ("duplicated", "DUPLICATED")]:
                cases.append((f"{method}-{keep}",
                              lambda f, m=method, k=keep: getattr(f, m)(subset=["id"], keep=k),
                              (f"FROM {function}((", f"'id', '{spelling}'")))
        for kind in FORMATS:
            with mock.patch.object(self.client, "public_workflow_run", return_value=self.reply) as execute, \
                    mock.patch.object(self.client, "vortex_prepare", side_effect=AssertionError("eager preparation")):
                for name, construct, fragments in cases:
                    with self.subTest(source=kind, operation=name):
                        execute.reset_mock()
                        workflow = construct(self.reader(kind))
                        self.assertIsInstance(workflow, sl.LazyFrame)
                        execute.assert_not_called()
                        report = workflow.collect(check=True, **RESOURCES)
                        execute.assert_called_once()
                        self.assertIs(report.envelope, self.envelope)
                        request = execute.call_args.kwargs
                        self.assert_request(request, fragments)
                        binding = request["source_bindings"][workflow.source.uri]
                        self.assertEqual(binding["input_format"], kind.replace("_", "-"))
                        if kind in {"csv", "json", "jsonl"}:
                            self.assertEqual(binding["source_schema"], tuple(SCHEMA.items()))
                        else:
                            self.assertNotIn("source_schema", binding)

    def test_reshape_declarations_retain_every_option(self):
        frame = self.context.read_vortex("nested.vortex", schema={
            "id": "int64", "label": "utf8", "amount": "float64", "other": "float64",
            "items": "list<int64>", "labels": "list<utf8>",
        })
        cases = [
            (frame.melt(id_vars="id", value_vars=["amount", "other"], var_name="metric", value_name="value"),
             ("FROM MELT((", '"id_columns":["id"]', '"value_columns":["amount","other"]', '"variable_column":"metric"')),
            (frame.select("id", "items").explode("items"), ("FROM EXPLODE((", '"column":"items"')),
            (frame.select("id", "items", "labels").explode("items", "labels"),
             ("FROM EXPLODE((", '"explode_columns":["items","labels"]')),
            (frame.pivot(index="id", columns="label", values="amount"), ("FROM PIVOT((",)),
        ]
        for aggregate in ("sum", "mean", "count", "min", "max"):
            cases.append((getattr(frame.rolling(window=3, min_periods=2, center=True), aggregate)("amount", alias="rolling"),
                          ("FROM ROLLING((", f'"aggregate":"{aggregate}"', '"window_size":3', '"min_periods":2', '"center":true')))
            cases.append((frame.pivot_table(index="id", columns="label", values="amount", aggfunc=aggregate,
                                           fill_value=0, dropna=False, margins=True, margins_name="total"),
                          ("FROM PIVOT((", f'"aggregate":"{aggregate}"', '"margins":true', '"margins_name":"total"')))
        for workflow, fragments in cases:
            with self.subTest(fragments=fragments), mock.patch.object(
                self.client, "public_workflow_run", return_value=self.reply
            ) as execute:
                workflow.collect(check=True, **RESOURCES)
                execute.assert_called_once()
                self.assert_request(execute.call_args.kwargs, fragments)

    def test_every_terminal_preserves_the_same_declaration_bindings_and_resources(self):
        for kind in FORMATS:
            workflow = self.reader(kind).filter(sl.col("id") > 3).select("id", "label").limit(2)
            statement = "SELECT id,label FROM '" + workflow.source.uri + "' WHERE id > 3 LIMIT 2"
            with mock.patch.object(self.client, "public_workflow_run", return_value=self.reply) as execute, \
                    mock.patch.object(self.client, "public_workflow_route", return_value=self.reply) as route, \
                    mock.patch.object(self.client, "vortex_prepare", side_effect=AssertionError("eager preparation")):
                for terminal in ("collect", "run", "route", *[f"write_{kind}" for kind in FORMATS], "fanout"):
                    with self.subTest(source=kind, terminal=terminal):
                        execute.reset_mock(); route.reset_mock()
                        if terminal == "fanout":
                            workflow.fanout([("vortex", "out.vortex"), ("jsonl", "out.jsonl")], check=True, **RESOURCES)
                        elif terminal.startswith("write_"):
                            getattr(workflow, terminal)("out." + terminal[6:], check=True, **RESOURCES)
                        else:
                            getattr(workflow, terminal)(check=True, **RESOURCES)
                        called, other = (route, execute) if terminal == "route" else (execute, route)
                        called.assert_called_once(); other.assert_not_called()
                        request = called.call_args.kwargs
                        self.assertEqual(request["sql_statement"], statement)
                        self.assert_request(request, ("WHERE id > 3", "LIMIT 2"))
                        self.assertEqual(tuple(request["source_bindings"]), (workflow.source.uri,))
                        if terminal == "fanout":
                            self.assertEqual(request["fanout_outputs"], (("jsonl", "out.jsonl"),))

    def test_count_preserves_preceding_limits_and_filters_in_a_derived_relation(self):
        source = self.reader("vortex")
        for workflow, inner in [
            (source.limit(2).filter(sl.col("id") > 3), "SELECT * FROM (SELECT * FROM (SELECT * FROM 'source.data') AS _sl_stage_0 LIMIT 2) AS _sl_stage_1 WHERE id > 3"),
            (source.filter(sl.col("id") > 3).limit(2), "SELECT * FROM (SELECT * FROM (SELECT * FROM 'source.data') AS _sl_stage_0 WHERE id > 3) AS _sl_stage_1 LIMIT 2"),
        ]:
            with self.subTest(inner=inner), mock.patch.object(self.client, "public_workflow_run", return_value=self.reply) as execute:
                workflow.count(check=True, **RESOURCES)
                execute.assert_called_once()
                self.assert_request(execute.call_args.kwargs, ("COUNT(*) AS count", f"({inner})"))

    def test_projection_helpers_preserve_preceding_aliases_and_limits(self):
        for frame in [self.reader(kind) for kind in FORMATS] + [self.context.from_rows([{"id": 1, "label": "one"}])]:
            with self.subTest(source=frame.source.source_format):
                workflow = frame.rename({"id": "key"}).limit(2).with_column("absent", None).drop("label")
                kinds = [operation.kind for operation in workflow.operations]
                self.assertEqual(kinds, ["select", "limit", "with_column", "select"])
                declaration = workflow._relation_statement()
                self.assertIn("id AS key", declaration)
                self.assertIn("NULL AS absent", declaration)
                self.assertIn("LIMIT 2)", declaration)
                self.assertTrue(declaration.startswith("SELECT key,"))

    def test_sql_always_reaches_native_admission_without_python_shape_routing(self):
        statements = [
            "SELECT 1 AS id", "SELECT id FROM 'source.data'", "SELECT id FROM catalog_table",
            "SELECT value FROM 'join-over-union.vortex' WHERE label = 'select '' join except' LIMIT 1",
            "SELECT COUNT(*) FROM 'source.vortex'", "SELECT NULL AS absent WHERE FALSE",
            "SELECT label, SUM(amount) AS total FROM 'source.vortex' WHERE id > 0 GROUP BY label",
            "SELECT id FROM 'source.vortex' ORDER BY id DESC LIMIT 0",
            "SELECT * FROM generate_series(1, 3)", "SELECT * FROM (VALUES (1), (NULL)) AS v",
            "SELECT id FROM 'source.vortex' LIMIT 1 ORDER BY id",
        ]
        for statement in statements:
            with self.subTest(statement=statement), mock.patch.object(self.client, "public_workflow_run", return_value=self.reply) as execute:
                report = self.context.sql(statement).collect(check=True, **RESOURCES)
                execute.assert_called_once()
                self.assertIs(report.envelope, self.envelope)
                self.assertEqual(execute.call_args.kwargs["sql_statement"], statement)
                self.assert_request(execute.call_args.kwargs, ())

    def test_memory_constructors_share_bindings_and_output_adapters(self):
        workflows = [
            self.context.from_rows([{"id": 1, "label": "λ"}]),
            self.context.literal_table([{"id": 1, "label": "λ"}]),
            self.context.from_rows([], schema={"id": "int64"}),
            self.context.range(1, 4), self.context.sequence(1, 3),
            self.context.calendar("2026-05-18", "2026-05-19"),
            self.context.dataframe_source_free_projection({"id": 7}),
        ]
        for workflow in workflows:
            with self.subTest(source=workflow.source.memory_input), mock.patch.object(self.client, "public_workflow_run", return_value=self.reply) as execute:
                for kind in FORMATS:
                    getattr(workflow, f"write_{kind}")(f"out.{kind}", check=True, **RESOURCES)
                self.assertEqual(execute.call_count, len(FORMATS))
                statements = {call.kwargs["sql_statement"] for call in execute.call_args_list}
                self.assertEqual(len(statements), 1)
                for call in execute.call_args_list:
                    self.assert_request(call.kwargs, ())
                    self.assertEqual(call.kwargs["source_bindings"][workflow.source.uri], {
                        "input_format": "memory", "memory_input": dict(workflow.source.memory_input),
                    })


if __name__ == "__main__":
    unittest.main()
