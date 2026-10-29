"""Shared native function discovery, SQL aliases and DataFrame semantics."""

from datetime import date, datetime, timedelta
from decimal import Decimal
import json
from pathlib import Path
import unittest

try:
    from .test_native_binding import _load_native
except ImportError:
    from test_native_binding import _load_native


# Independently specified values exercise every registered provider, rather
# than comparing two spellings that could both return the same wrong result.
DAY = "CAST('2024-02-29' AS DATE)"
STAMP = "CAST('2024-02-29T13:14:15.123456Z' AS TIMESTAMP)"
EPOCH = "CAST('1970-01-01T00:00:00Z' AS TIMESTAMP)"
ZONED_EPOCH = "CAST('1970-01-01T00:00:00Z' AS TIMESTAMP(us,'UTC'))"
SPAN = "CAST('2s' AS DURATION(s))"
TEMPORAL_CASES = {
    "temporal_extract": ([STAMP, "'hour'"], 13),
    "temporal_trunc": ([STAMP, "'day'"], datetime(2024, 2, 29)),
    "temporal_bucket": ([STAMP, "15", "'s'"], datetime(2024, 2, 29, 13, 14, 15)),
    "calendar_add_days": ([DAY, "1"], date(2024, 3, 1)),
    "calendar_add_months": ([DAY, "1"], date(2024, 3, 29)),
    "temporal_add_duration": ([STAMP, SPAN], datetime(2024, 2, 29, 13, 14, 17, 123456)),
    "temporal_sub_duration": ([STAMP, SPAN], datetime(2024, 2, 29, 13, 14, 13, 123456)),
    "timestamp_difference": ([STAMP, "CAST('2024-02-29T13:14:13.500000Z' AS TIMESTAMP)"],
                             timedelta(seconds=1, microseconds=623456)),
    "timestamp_assign_timezone": ([EPOCH, "'Asia/Tokyo'"], -32_400_000_000),
    "timestamp_convert_timezone": ([ZONED_EPOCH, "'Asia/Tokyo'"], 0),
    "timestamp_remove_timezone": ([ZONED_EPOCH], datetime(1970, 1, 1)),
}
CASES = {
    **TEMPORAL_CASES,
    "decimal_rescale": (["125", "5", "1", "'half_even'"], Decimal("125.0")),
    "decimal_add": (["1", "2", "5", "1", "'exact'"], Decimal("3.0")),
    "decimal_subtract": (["1", "2", "5", "1", "'floor'"], Decimal("-1.0")),
    "decimal_multiply": (["5", "5", "5", "2", "'exact'"], Decimal("25.00")),
    "decimal_divide": (["1", "8", "5", "2", "'half_even'"], Decimal("0.12")),
    "abs": (["-7"], 7), "floor": (["-1.2"], -2.0),
    "ceil": (["-1.2"], -1.0), "round": (["-1.5"], -2.0),
    "lower": (["'É猫'"], "é猫"), "upper": (["'é猫'"], "É猫"),
    "trim": (["'  猫  '"], "猫"), "length": (["'é猫'"], 2),
    "starts_with": (["'é猫'", "'é'"], True),
    "ends_with": (["'é猫'", "'猫'"], True),
    "contains": (["'é猫x'", "'猫'"], True),
    "regex_match": (["'é猫'", "'^é.'"], True),
    "concat": (["'é'", "'猫'"], "é猫"),
    "substr": (["'aé猫z'", "2", "2"], "é猫"),
    "left": (["'é猫z'", "2"], "é猫"),
    "right": (["'aé猫'", "2"], "é猫"),
    "replace": (["'a猫猫'", "'猫'", "'é'"], "aéé"),
    "byte_length": (["'é猫'"], 5), "unhex": (["'c3a9'"], b"\xc3\xa9"),
    "from_base64": (["'w6k='"], b"\xc3\xa9"),
    "url_domain": (["'https://www.example.com/path'"], "example.com"),
    "regex_domain": (["'https://www.example.com/path'"], "example.com"),
    "regexp_replace": (["'https://www.example.com/path'", r"'^https?://(?:www\.)?([^/]+)/.*$'", r"'\1'"], "example.com"),
    "extract_minute": (["125"], 2), "date_trunc_minute": (["125"], 120),
    "date_year": ([DAY], 2024), "date_month": ([DAY], 2), "date_day": ([DAY], 29),
    "timestamp_year": ([STAMP], 2024), "timestamp_month": ([STAMP], 2),
    "timestamp_day": ([STAMP], 29), "timestamp_hour": ([STAMP], 13),
    "timestamp_minute": ([STAMP], 14), "timestamp_second": ([STAMP], 15),
    "date_add_days": ([DAY, "1"], date(2024, 3, 1)),
    "date_sub_days": ([DAY, "1"], date(2024, 2, 28)),
    "timestamp_add_seconds": ([STAMP, "2"], datetime(2024, 2, 29, 13, 14, 17, 123456)),
    "timestamp_sub_seconds": ([STAMP, "2"], datetime(2024, 2, 29, 13, 14, 13, 123456)),
    "date_diff_days": ([DAY, "CAST('2024-02-27' AS DATE)"], 2),
    "timestamp_diff_seconds": ([STAMP, "CAST('2024-02-29T13:14:13.500000Z' AS TIMESTAMP)"], 1),
    "coalesce": (["NULL", "2"], 2), "nullif": (["2", "2"], None),
    "case_when": (["TRUE", "2", "3"], 2),
    "json_extract": (["'{\"value\":\"猫\"}'", "'$.value'"], '"猫"'),
    "strptime": (["'2024-02-29'", "'%Y-%m-%d'"], datetime(2024, 2, 29)),
    "try_strptime": (["'bad'", "'%Y-%m-%d'"], None),
}


class NativeFunctionCatalogTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.native = _load_native()
        import shardloom as sl
        from shardloom import functions as f
        cls.sl, cls.f = sl, f

    def context(self):
        context = self.sl.context(transport="native", memory_bytes=128 << 20, max_parallelism=2)
        self.addCleanup(context.close)
        return context

    def test_catalog_is_complete_unique_inert_and_matches_all_sql_aliases(self):
        context, f = self.context(), self.f
        catalog = f.catalog()
        self.assertEqual({entry["name"] for entry in catalog}, set(CASES))
        self.assertEqual(catalog, f.catalog())
        names, expressions, expected = set(), [], {}
        for entry in catalog:
            self.assertFalse(entry["external_engine_invoked"])
            self.assertEqual(entry["execution_provider"], "shared_shardloom_native")
            self.assertTrue(entry["type_rule"] and entry["null_rule"])
            arguments, value = CASES[entry["name"]]
            for name in [entry["name"], *entry["aliases"]]:
                self.assertNotIn(name, names)
                names.add(name)
                alias = f"value_{len(expressions)}"
                expressions.append(f"{name.upper()}({','.join(arguments)}) AS {alias}")
                expected[alias] = value
        self.assertIsNone(context.client._native_backend)
        query = context.sql("SELECT " + ",".join(expressions))
        self.assertEqual(query.collect().to_rows(), (expected,))
        catalog[0]["name"] = "changed"
        self.assertNotEqual(f.catalog()[0]["name"], "changed")

    def test_documented_scalar_definitions_match_executable_discovery(self):
        root = Path(__file__).resolve().parents[2]
        import sys
        sys.path.insert(0, str(root / "scripts"))
        from write_native_function_reference import snapshot, render, TARGET
        recorded = snapshot(root)
        self.assertEqual(json.loads(json.dumps(self.f.catalog())), recorded["functions"])
        self.assertEqual((root / TARGET.with_suffix(".md")).read_text(), render(recorded))

    def test_every_scalar_dataframe_constructor_matches_independent_values(self):
        from shardloom.query import ColumnExpression

        context, f = self.context(), self.f
        def literal(text):
            dates = {DAY: date(2024, 2, 29), "CAST('2024-02-27' AS DATE)": date(2024, 2, 27),
                EPOCH: datetime(1970, 1, 1),
                STAMP: datetime(2024, 2, 29, 13, 14, 15, 123456),
                "CAST('2024-02-29T13:14:13.500000Z' AS TIMESTAMP)": datetime(2024, 2, 29, 13, 14, 13, 500000)}
            if text in dates:
                return dates[text]
            if text == SPAN:
                return f.lit("2s").cast("duration(s)")
            if text == ZONED_EPOCH:
                return f.lit("1970-01-01T00:00:00Z").cast("timestamp(us,UTC)")
            if text in {"TRUE", "FALSE", "NULL"}:
                return {"TRUE": True, "FALSE": False, "NULL": None}[text]
            if text.startswith("'"):
                return text[1:-1].replace("''", "'")
            return float(text) if "." in text else int(text)

        projected, expected = [], {}
        for name, (arguments, value) in CASES.items():
            values = [literal(argument) for argument in arguments]
            if name in TEMPORAL_CASES:
                first = values[0] if isinstance(values[0], ColumnExpression) else f.lit(values[0])
                if name == "timestamp_difference":
                    values[1] = f.lit(values[1])
                projected.append(getattr(first, name)(*values[1:]).alias(name))
                expected[name] = value
                continue
            constructor = getattr(f, "null_if" if name == "nullif" else name)
            if name.startswith("decimal_"):
                values[0] = f.lit(values[0])
                if name != "decimal_rescale":
                    values[1] = f.lit(values[1])
                projected.append(constructor(*values).alias(name))
                expected[name] = value
                continue
            if name in {"concat", "coalesce", "nullif", "case_when", "date_diff_days", "timestamp_diff_seconds"}:
                values = [f.lit(value) for value in values]
            else:
                values[0] = f.lit(values[0])
            projected.append(constructor(*values).alias(name))
            expected[name] = value
        self.assertEqual(context.range(0, 1).select(*projected).collect().to_rows(), (expected,))

        rows = context.from_rows([{"text": "%_猫"}, {"text": "x猫"}, {"text": None}],
                                 schema={"text": "utf8"})
        predicate = f.starts_with("text", "%_") & f.ends_with("text", "猫")
        self.assertEqual(rows.filter(predicate).collect().to_rows(), ({"text": "%_猫"},))
        self.assertEqual(rows.select(f.contains("text", "%_").alias("literal")).collect().to_rows(),
                         ({"literal": True}, {"literal": False}, {"literal": None}))

    def test_null_rules_and_exact_decimal_types_survive_dataframe_composition(self):
        context, f = self.context(), self.f
        frame = context.sql("SELECT CAST(NULL AS UTF8) AS absent, CAST(-123.45 AS DECIMAL(7,2)) AS money")
        query = frame.select(
            f.abs("money").alias("absolute"), f.round("money").alias("rounded"),
            f.coalesce("absent", f.lit("猫")).alias("filled"),
            f.coalesce(f.lit(4)).alias("single"),
            f.case_when(f.col("absent").cast("boolean"), f.lit(1), f.lit(2)).alias("branch"),
            f.null_if(f.lit(1), f.lit(2)).alias("nullable"),
            f.json_extract(f.lit('{"a":null}'), "$.missing").alias("missing"),
            f.json_extract(f.lit('{"a":null}'), "$.a").alias("json_null"),
            f.try_strptime(f.lit("bad"), "%Y-%m-%d").alias("tolerant"),
            f.length(f.lit("é猫")).alias("characters"),
            f.byte_length(f.lit("é猫")).alias("bytes"),
            f.timestamp_month(f.lit(datetime(2024, 2, 29))).alias("month"),
        )
        fields = dict(query.schema())
        self.assertEqual(fields["absolute"].parameters, (7, 2))
        self.assertEqual(fields["rounded"].parameters[1], 0)
        for name in ("filled", "single", "branch", "characters", "bytes", "month"):
            self.assertFalse(fields[name].nullable, name)
        for name in ("nullable", "missing", "tolerant"):
            self.assertTrue(fields[name].nullable, name)
        self.assertEqual(query.collect().to_rows(), ({"absolute": Decimal("123.45"),
            "rounded": Decimal("-123"), "filled": "猫", "single": 4, "branch": 2,
            "nullable": 1, "missing": None, "json_null": "null", "tolerant": None, "characters": 2,
            "bytes": 5, "month": 2},))

    def test_null_propagation_and_binding_failures_do_not_open_producers(self):
        context = self.context()
        opened = []
        def batches():
            opened.append(True)
            yield [{"value": 1}]
        source = context.from_batches(batches, schema={"value": "int64"}, streaming=True)
        expressions = []
        for entry in self.f.catalog():
            if not entry["null_rule"].startswith("null input"):
                continue
            arguments = list(CASES[entry["name"]][0])
            if entry["name"] in TEMPORAL_CASES:
                # New temporal functions require an explicit logical domain even
                # for nulls; metadata and literal policies still bind normally.
                dtype = arguments[0].rsplit(" AS ", 1)[1][:-1]
                arguments[0] = f"CAST(NULL AS {dtype})"
            else:
                arguments[0] = "NULL"
            expressions.append(f"{entry['name']}({','.join(arguments)}) AS result_{len(expressions)}")
        query = context.sql("SELECT " + ",".join(expressions))
        self.assertTrue(all(dtype.nullable for _, dtype in query.schema()))
        self.assertEqual(query.collect().to_rows(), ({f"result_{index}": None for index in range(len(expressions))},))
        for expression in ("ABS(value, 2)", "COALESCE()", "LOWER(value)", "SUBSTR('x',1.2,1)",
                           "DATE_YEAR(value)", "REGEX_MATCH('x', '[')", "UNKNOWN_FUNCTION(value)"):
            with self.subTest(expression=expression), self.assertRaises((ValueError, RuntimeError)):
                context.sql(f"SELECT {expression} AS invalid FROM input", relations={"input": source}).schema()
            self.assertEqual(opened, [])


if __name__ == "__main__":
    unittest.main()
