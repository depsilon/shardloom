#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Independent correctness checks for the prospective finite workload."""
import unittest

import native_cyclic_existence_workload as workload


def equal(left, right):
    return left is not None and right is not None and left == right


def literal(case, inputs):
    """Small, direct relational definition; no index/domain helper is reused."""
    def outer(row):
        value = {"outer_id": row["id"], "b": row["y"], "a": row["x"]}
        if case == "skew_wide":
            value.update({f"payload_{i}": row[f"p{i}"] for i in range(8)})
        return value

    def product(r, s, t=None):
        value = {"ra": r["x"], "rb": r["y"], "rid": r["id"],
                 "sb": s["x"], "sc": s["y"], "sid": s["id"]}
        if t is not None:
            value.update({"ta": t["x"], "tc": t["y"], "tid": t["id"]})
        return value

    for r in inputs["r"]:
        if case in {"abundant", "rare", "absent", "skew_wide",
                    "null_duplicates", "semi_small", "empty_inner"}:
            if any(equal(r["y"], s["x"]) and equal(r["x"], t["x"])
                   and equal(s["y"], t["y"])
                   for s in inputs["s"] for t in inputs["t"]):
                yield outer(r)
        elif case in {"binary_semi", "binary_anti"}:
            found = any(equal(r["y"], s["x"]) for s in inputs["s"])
            if found == (case == "binary_semi"):
                yield outer(r)
        else:
            for s in inputs["s"]:
                if not equal(r["x"] if case == "star_control" else r["y"], s["x"]):
                    continue
                if case == "binary_control":
                    yield product(r, s)
                    continue
                for t in inputs["t"]:
                    matched = (equal(r["y"], t["x"]) if case == "star_control"
                               else equal(r["x"], t["x"]) and equal(s["y"], t["y"]))
                    if matched:
                        yield product(r, s, t)


class WorkloadTests(unittest.TestCase):
    def test_small_relations_match_literal_equality_and_order(self):
        pairs = {
            "r": [(1, 7), (1, 7), (2, 7), (None, 7), (1, None), (7, 1)],
            "s": [(7, 3), (7, 3), (7, 4), (None, 3), (7, None), (1, 7)],
            "t": [(1, 3), (1, 3), (2, 4), (1, None), (None, 3), (7, 1)],
        }
        original = {name: [dict(x=x, y=y, id=n, **{f"p{i}": n * 13 - i for i in range(8)})
                           for n, (x, y) in enumerate(rows)] for name, rows in pairs.items()}
        for case in workload.CASES:
            for emptied in (None, "r", "s", "t"):
                inputs = {name: ([] if name == emptied else rows) for name, rows in original.items()}
                with self.subTest(case=case, empty=emptied):
                    self.assertEqual(list(workload.reference(case, inputs)), list(literal(case, inputs)))
                    expected_inner = [{"witness_b": s["x"], "witness_a": t["x"]}
                                      for s in inputs["s"] for t in inputs["t"]
                                      if equal(s["y"], t["y"])]
                    self.assertEqual(list(workload.inner_reference(inputs)), expected_inner)
        expected = [{"outer_id": 0, "b": 7, "a": 1},
                    {"outer_id": 1, "b": 7, "a": 1},
                    {"outer_id": 2, "b": 7, "a": 2}]
        self.assertEqual(list(workload.reference("semi_small", original)), expected)

    def test_full_target_outputs_and_inner_cardinality_have_independent_formula(self):
        for case in ("abundant", "rare", "absent", "skew_wide"):
            inputs = workload.sources(case)
            expected = [] if case == "absent" else [
                dict(outer_id=n, b=7 if case == "skew_wide" else n // 64, a=n % 64,
                     **({f"payload_{i}": n * 17 - i * 3 for i in range(8)}
                        if case == "skew_wide" else {})) for n in range(4096)]
            with self.subTest(case=case):
                self.assertEqual(list(workload.reference(case, inputs)), expected)
                expected_inner = 524288 if case == "skew_wide" else 262144
                self.assertEqual(workload.digest_rows(workload.inner_reference(inputs))["rows"], expected_inner)
                self.assertEqual(inputs["s"][0], {"x": 7 if case == "skew_wide" else 0, "y": 0, "id": 0})
                self.assertEqual(inputs["t"][-1], {"x": 1087 if case == "absent" else 63, "y": 63, "id": 4095})

    def test_original_full_product_controls_preserve_complete_output(self):
        # These are the previous experiment's original controls, including the
        # one-call star warning. They must not silently become semi-joins.
        for case, expected_rows in (("binary_control", 8192), ("star_control", 4096),
                                    ("cyclic_small", 64)):
            inputs = workload.sources(case)
            result = list(workload.reference(case, inputs))
            with self.subTest(case=case):
                self.assertEqual(result, list(literal(case, inputs)))
                self.assertEqual(len(result), expected_rows)
                self.assertEqual(len(result[0]), 6 if case == "binary_control" else 9)
        inputs = workload.sources("cyclic_dense")
        expected = [dict(ra=a, rb=b, rid=b * 32 + a, sb=b, sc=c, sid=c * 32 + b,
                         ta=a, tc=c, tid=c * 32 + a)
                    for b in range(32) for a in range(32) for c in range(32)]
        self.assertEqual(list(workload.reference("cyclic_dense", inputs)), expected)

    def test_schema_and_manifest_contract(self):
        manifest = workload.manifest()
        self.assertEqual([cell["case"] for cell in manifest["cells"]], list(workload.CASES))
        self.assertEqual(sum(cell["target"] for cell in manifest["cells"]), 4)
        self.assertEqual(manifest["grant_bytes"], 128 << 20)
        self.assertEqual(manifest["max_parallelism"], 1)
        counts = [4096, 4096, 0, 4096, 486, 16, 0, 8192, 4096, 4096, 8192, 32768, 64]
        self.assertEqual([cell["expected"]["rows"] for cell in manifest["cells"]], counts)
        for cell in manifest["cells"]:
            dtype, nullable = cell["expected"]["schema"]["Struct"]
            self.assertFalse(nullable)
            self.assertEqual(len(dtype["names"]), len(dtype["dtypes"]))
            self.assertEqual(len(dtype["names"]), len(set(dtype["names"])))
            for fields in [source["fields"] for source in cell["sources"].values()]:
                self.assertEqual(fields[:3], [("x", {"Primitive": ["i64", True]}),
                                             ("y", {"Primitive": ["i64", True]}),
                                             ("id", {"Primitive": ["i64", False]})])


if __name__ == "__main__":
    unittest.main()
