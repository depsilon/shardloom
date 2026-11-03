# SPDX-License-Identifier: Apache-2.0
"""Cross-check the expanded SQLite oracle against the original Python oracle."""

from pathlib import Path
import statistics
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "benchmarks/traditional_analytics"))
sys.path.insert(0, str(ROOT / "python/src"))
sys.path.insert(0, str(ROOT / "scripts"))

from query_surface import expanded_reference, reference  # noqa: E402
from query_surface.datasets import DatasetSpec, RowGenerator, write_dataset  # noqa: E402
from query_surface.semantics import ResultContract, ResultType  # noqa: E402
from query_surface.specification import Case, Measure, Order, Relation  # noqa: E402


class _Guard:
    def check(self):
        return None


def _case(name, relation, ordered=False):
    return Case(name, "composite", name, relation, ResultContract(relation.schema, ordered=ordered), ())


def _rows(result):
    return sorted(result.rows, key=lambda row: repr(sorted(row.items())))


class ExpandedOracleCrossCheckTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        try:
            import pyarrow  # noqa: F401
        except ImportError:
            raise unittest.SkipTest("PyArrow is an optional fixture dependency")
        cls.temp = tempfile.TemporaryDirectory(prefix="expanded-oracle-")
        root = Path(cls.temp.name)
        cls.spec = DatasetSpec(
            seed=20261103, fact_rows=16, entity_rows=7, product_rows=5, relationship_rows=9, calendar_rows=4
        )
        cls.dataset = root / "dataset"
        cls.manifest = write_dataset(cls.dataset, cls.spec, "sorted", guard=_Guard().check, batch_rows=8)
        fact, entities = Relation.scan("fact"), Relation.scan("entities")
        cls.fact, cls.entities = fact, entities
        cls.db = expanded_reference_input_db(root, cls.dataset, cls.manifest)

    @classmethod
    def tearDownClass(cls):
        if hasattr(cls, "db"):
            cls.db.close()
        if hasattr(cls, "temp"):
            cls.temp.cleanup()

    def assert_oracles_agree(self, name, relation, *, ordered=False):
        case = _case(name, relation, ordered)
        actual = expanded_reference.expected(self.db, case)
        wanted = reference.expected(case, self.spec)
        if ordered:
            self.assertEqual(actual.rows, wanted.rows, name)
        else:
            self.assertEqual(_rows(actual), _rows(wanted), name)

    def test_generated_operator_shapes_match_original_reference(self):
        fact, entities = self.fact, self.entities
        projection = fact.where(fact.col("row_id").binary("lt", 9)).project(
            ("row_id", fact.col("row_id")), ("value", fact.col("signed_value"))
        )
        conjunction = fact.where(
            fact.col("row_id").binary("lt", 13).binary("and", fact.col("optional_value").unary("is_not_null"))
        ).select("row_id", "optional_value")
        reduction = fact.where(fact.col("row_id").binary("lt", 12)).aggregate(
            ("category",), (Measure("sum", fact.col("signed_value"), "total", ResultType("float64", True)),)
        )
        star = fact.where(fact.col("row_id").binary("lt", 11)).aggregate(
            (), (Measure("count", None, "rows", ResultType("uint64", False)),)
        )
        ordered = (
            fact.select("row_id", "nullable_no_null")
            .order(Order("nullable_no_null", descending=True, nulls="last"), Order("row_id"))
            .take(7, 1)
        )
        left = fact.where(fact.col("row_id").binary("lt", 10))
        right = entities.where(entities.col("entity_id").binary("lt", 5))
        cases = [
            ("v2_projection", projection, False),
            ("v2_conjunction", conjunction, False),
            ("v2_reduction", reduction, False),
            ("v2_count_star", star, False),
            ("v2_bounded_sort", ordered, True),
        ]
        for kind in ("full", "semi", "anti"):
            columns = [("left", "row_id", "row_id"), ("left", "entity_id", "entity_id")]
            if kind == "full":
                columns.append(("right", "region", "region"))
            joined = left.join(right, (("entity_id", "entity_id"),), columns, kind)
            cases.append((f"v2_{kind}_join", joined, False))
        left_set = fact.where(fact.col("row_id").binary("lt", 9)).select("group_key")
        right_set = fact.where(
            fact.col("row_id").binary("ge", 5).binary("and", fact.col("row_id").binary("lt", 14))
        ).select("group_key")
        cases.extend(
            (
                ("v2_intersect", left_set.set(right_set, "intersect"), False),
                ("v2_except", left_set.set(right_set, "except"), False),
            )
        )
        for name, relation, is_ordered in cases:
            with self.subTest(shape=name):
                self.assert_oracles_agree(name, relation, ordered=is_ordered)

    def test_nullable_variance_and_median_against_separate_golden_rows(self):
        fact = self.fact.where(self.fact.col("row_id").binary("lt", 16))
        value = fact.col("optional_value")
        relation = fact.aggregate(
            (),
            (
                Measure("var_pop", value, "variance", ResultType("float64", True)),
                Measure("median", value, "median", ResultType("float64", True)),
            ),
        )
        case = _case("nullable_variance_median_golden", relation)
        actual = expanded_reference.expected(self.db, case)
        values = [
            RowGenerator(self.spec, "fact").value(i, "optional_value") for i in range(self.spec.fact_rows)
        ]
        nonnull = [value for value in values if value is not None]
        golden = [{"variance": statistics.pvariance(nonnull), "median": float(statistics.median(nonnull))}]
        self.assertEqual(actual.rows, golden)


def expanded_reference_input_db(root, dataset, manifest):
    # Keep the fixture to exactly the columns used in the statements above.
    from query_surface.expanded_reference_input import build_database

    path = root / "reference.sqlite"
    columns = {
        "fact": {
            "row_id",
            "entity_id",
            "group_key",
            "category",
            "signed_value",
            "optional_value",
            "nullable_no_null",
            "mostly_null",
        },
        "entities": {"entity_id", "region"},
    }
    build_database(path, dataset, manifest, columns, _Guard())
    return expanded_reference.open_database(path)


if __name__ == "__main__":
    unittest.main()
