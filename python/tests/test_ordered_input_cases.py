# SPDX-License-Identifier: Apache-2.0
"""Independent controls for prospective ordering fixtures; no native execution."""

from collections import Counter
import unittest

if __package__:
    from . import native_ordered_input_cases as fixture
else:
    import native_ordered_input_cases as fixture


class OrderedInputFixtureTests(unittest.TestCase):
    def test_templates_preserve_complete_populations_and_explicit_layouts(self):
        self.assertEqual(len(fixture.TEMPLATES), 12)
        self.assertEqual(fixture.SIZES, (0, 33, 4097, 65537))
        for size in (0, 1, 33, 4097):
            for template in fixture.TEMPLATES:
                with self.subTest(template=template, size=size):
                    sources = fixture.records(template, size)
                    for rows in sources.values():
                        self.assertEqual(Counter(row["id"] for row in rows), Counter(range(size)))
                        self.assertTrue(all(tuple(row) == fixture.SOURCE_FIELDS for row in rows))
                    actual = fixture.reference_rows(template, sources)
                    if template.startswith(("collections_", "selectors_")):
                        self.assertEqual([row["grp"] for row in actual], list(range(min(7, size))))
                    else:
                        self.assertEqual(len(actual), size)
                    if template.startswith("collections_"):
                        self.assertEqual(sum(len(row["items"]) for row in actual), size)
                    if template.startswith("window_"):
                        for group in range(min(7, size)):
                            positions = sorted(row["position"] for row in actual if row["grp"] == group)
                            self.assertEqual(positions, list(range(1, len(positions) + 1)))

    def test_asof_bisect_expectation_matches_independent_exhaustive_selection(self):
        for template in ("asof_ordered", "asof_shuffled"):
            sources = fixture.records(template, 83)
            expected = []
            for probe in sources["left"]:
                choices = [(row["ord"], at, row) for at, row in enumerate(sources["right"])
                           if row["grp"] == probe["grp"] and row["ord"] is not None
                           and probe["ord"] is not None and row["ord"] <= probe["ord"]]
                selected = max(choices, key=lambda item: item[:2])[2] if choices else {}
                expected.append(dict(left_id=probe["id"], right_id=selected.get("id"),
                                     selected=selected.get("value")))
            self.assertEqual(fixture.reference_rows(template, sources), expected)

    def test_reference_distinguishes_nulls_ties_and_physical_source_order(self):
        rows = [dict(id=9, grp=0, ord=1, minor=3, value=90, text="a", payload="p9"),
                dict(id=2, grp=0, ord=1, minor=3, value=20, text="", payload="p2"),
                dict(id=1, grp=0, ord=None, minor=None, value=None, text=None, payload="p1")]
        sources = {"input": rows}
        self.assertEqual([row["id"] for row in fixture.reference_rows("sort_ascending", sources)], [9, 2, 1])
        self.assertEqual(fixture.reference_rows("selectors_ordered", sources),
                         [dict(grp=0, initial=90, ending=None, least=90, greatest=90)])
        self.assertEqual(fixture.reference_rows("collections_ordered", sources),
                         [dict(grp=0, items=[90, 20, None], joined="a / ")])
        reversed_sources = {"input": list(reversed(rows))}
        self.assertEqual([row["id"] for row in fixture.reference_rows("sort_ascending", reversed_sources)], [2, 9, 1])
        mixed = [dict(rows[0], minor=-1), dict(rows[1], minor=4), dict(rows[0], id=7, minor=None)]
        self.assertEqual([row["id"] for row in fixture.reference_rows("sort_nullable_mixed", {"input": mixed})], [7, 2, 9])


if __name__ == "__main__":
    unittest.main()
