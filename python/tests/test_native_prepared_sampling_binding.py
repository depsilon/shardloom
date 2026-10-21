"""Installed SQL/DataFrame sampling uses immutable prepared native ownership."""

from bisect import bisect_left
import gc
import json
import sys
import unittest
from unittest.mock import patch

if __package__:
    from .test_native_binding import _load_native
else:
    from test_native_binding import _load_native


ROWS = 4096
MASK = (1 << 64) - 1
URI = "memory://prepared-sampling"
FIELDS = [("id", json.dumps({"Primitive": ["i64", False]})),
          ("weight", json.dumps({"Primitive": ["f64", True]})),
          ("other", json.dumps({"Primitive": ["f64", True]}))]


def input_rows(reverse=False):
    weights = [float(1 + n % 16) for n in range(ROWS)]
    if reverse:
        weights.reverse()
    return [dict(id=n, weight=weight, other=weights[-1 - n])
            for n, weight in enumerate(weights)]


def reference(rows, seed=7, weight="weight", columns=("id",)):
    """Independent complete-result reference; never calls a native helper."""
    cumulative, total = [], 0.0
    for row in rows:
        total += row[weight]
        cumulative.append(total)
    result = []
    for draw in range(ROWS):
        value = (seed ^ 0xa0761d6478bd642f) ^ ((draw * 0x9e3779b97f4a7c15) & MASK)
        value = (value + 0x9e3779b97f4a7c15) & MASK
        value = ((value ^ (value >> 30)) * 0xbf58476d1ce4e5b9) & MASK
        value = ((value ^ (value >> 27)) * 0x94d049bb133111eb) & MASK
        value ^= value >> 31
        unit = max(sys.float_info.min,
                   min(1.0 - sys.float_info.epsilon, (float(value) + 0.5) / (float(MASK) + 1.0)))
        row = rows[min(len(rows) - 1, bisect_left(cumulative, unit * total))]
        result.append({name: row[name] for name in columns})
    return result


def statement(*, source=None, seed=7, weight="weight", columns=("id",), output="*"):
    options = json.dumps(dict(n=ROWS, seed=seed, replace=True, weights=weight,
                              columns=",".join(columns)))
    source = source or f"SELECT * FROM '{URI}'"
    return f"SELECT {output} FROM SAMPLE(({source}), '{options}') AS sample"


class NativePreparedSamplingBindingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.native = _load_native()

    def session(self):
        session = self.native.Session(self.native.ExecutionResources(64 << 20, 1))
        self.addCleanup(session.close)
        return session

    def check_result(self, result, expected, columns=("id",)):
        from shardloom._result_schema import ResultType, schema_fields

        actual, exported = result.row_export()
        self.assertEqual(actual, expected)
        self.assertTrue(exported["complete"])
        self.assertEqual(schema_fields(result.schema_json, "vortex.dtype.serde.v1"), tuple(
            (name, ResultType("int64", False) if name == "id" else ResultType("float64", True))
            for name in columns))
        self.assertEqual(result.report["external_engine_invoked"], "false")
        self.assertEqual(result.report["fallback_attempted"], "false")
        self.assertEqual(result.report["resident_relational_declaration_reused"], "true")

    def test_prepared_sql_retains_scalar_model_with_exact_source_seed_and_projection_identity(self):
        session = self.session()
        originals = [input_rows(), input_rows(reverse=True)]
        caller_rows = [dict(row) for row in originals[0]]
        sources = [session.from_rows(caller_rows, fields=FIELDS),
                   session.from_rows(originals[1], fields=FIELDS)]
        caller_rows.reverse()
        caller_rows[0]["weight"] = 1_000_000.0
        source_credit = session.snapshot()["reserved_bytes"]
        plans = []
        with patch("subprocess.Popen", side_effect=AssertionError("execution child process")):
            for source_index, seed, weight, columns in (
                (0, 7, "weight", ("id",)),
                (1, 7, "weight", ("id",)),
                (0, 9, "other", ("weight", "id")),
            ):
                with self.subTest(source=source_index, seed=seed, weight=weight, columns=columns):
                    plan = session.sql(statement(seed=seed, weight=weight, columns=columns),
                                       sources=[(URI, sources[source_index])])
                    plans.append(plan)
                    plan.prepare()
                    before = session.snapshot()["reserved_bytes"]
                    retained = None
                    for _ in range(3):
                        result = plan.collect()
                        self.check_result(result, reference(originals[source_index], seed, weight, columns), columns)
                        del result
                        delta = session.snapshot()["reserved_bytes"] - before
                        self.assertGreater(delta, 0, "the scalar model must actually remain owned")
                        self.assertLessEqual(delta, 128)
                        if retained is None:
                            retained = delta
                        self.assertEqual(delta, retained)
            for plan in plans:
                plan.release_prepared()
            self.assertEqual(session.snapshot()["reserved_bytes"], source_credit)

            # Re-preparation after a resource change starts without a prior model.
            narrower = self.native.ExecutionResources(32 << 20, 1)
            plan = plans[0]
            plan.prepare(resources=narrower)
            before = session.snapshot()["reserved_bytes"]
            result = plan.collect(resources=narrower)
            self.check_result(result, reference(originals[0]))
            del result
            self.assertGreater(session.snapshot()["reserved_bytes"], before)
            plan.release_prepared()
        del plan, plans, sources
        gc.collect()
        self.assertEqual(session.snapshot()["reserved_bytes"], 0)
        self.assertEqual(session.snapshot()["denied_reservations"], 0)

    def test_prepared_sql_preserves_real_projection_filter_and_limit_exclusions(self):
        session = self.session()
        rows = input_rows()
        source = session.from_rows(rows, fields=FIELDS)
        source_credit = session.snapshot()["reserved_bytes"]
        for query in (
            statement(source=f"SELECT weight,id,other FROM '{URI}'"),
            statement(source=f"SELECT * FROM '{URI}' WHERE id >= 0"),
            statement(source=f"SELECT * FROM '{URI}' LIMIT {ROWS}"),
            statement(source=f"SELECT id,weight + 0.0 AS weight,other FROM '{URI}'"),
        ):
            with self.subTest(query=query):
                plan = session.sql(query, sources=[(URI, source)])
                plan.prepare()
                before = session.snapshot()["reserved_bytes"]
                for _ in range(2):
                    result = plan.collect()
                    self.check_result(result, reference(rows))
                    del result
                    self.assertEqual(session.snapshot()["reserved_bytes"], before)
                plan.release_prepared()
                del plan
                self.assertEqual(session.snapshot()["reserved_bytes"], source_credit)
        del source
        gc.collect()
        self.assertEqual(session.snapshot()["reserved_bytes"], 0)

    def test_dataframe_sample_repeats_and_releases_through_the_native_public_path(self):
        import shardloom as sl

        rows = input_rows()
        expected = reference(rows, columns=("id", "weight", "other"))
        client = sl.ShardLoomClient(transport="native", memory_bytes=64 << 20, max_parallelism=1)
        self.addCleanup(client.close)
        with patch("subprocess.Popen", side_effect=AssertionError("execution child process")):
            frame = sl.from_rows(rows, schema={
                "id": {"type": "int64", "nullable": False},
                "weight": "float64", "other": "float64",
            }, client=client)
            sample = frame.sample(n=ROWS, seed=7, weights="weight", replace=True)
            session = client._native_backend.session
            source_credit = session.snapshot()["reserved_bytes"]
            for _ in range(2):
                for call in range(3):
                    result = sample.collect(check=True)
                    self.assertEqual(result.to_arrow().to_pylist(), expected)
                    self.assertEqual(result.envelope.field("external_engine_invoked"), "false")
                    self.assertFalse(result.envelope.raw["fallback"]["attempted"])
                    if call:
                        self.assertEqual(result.envelope.field("resident_relational_declaration_reused"), "true")
                    del result
                client.release_prepared()
                self.assertEqual(session.snapshot()["reserved_bytes"], source_credit)
            del sample, frame
        gc.collect()
        self.assertEqual(session.snapshot()["reserved_bytes"], 0)


if __name__ == "__main__":
    unittest.main()
