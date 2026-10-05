# SPDX-License-Identifier: Apache-2.0
"""Complete CSV proof checks preserve nested values, NULL rows and exact text."""
import csv
import io
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import Mock

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
from native_workflow_outputs import verify_output


class NativeWorkflowOutputTests(unittest.TestCase):
    @staticmethod
    def csv_text(columns, rows):
        stream = io.StringIO(newline="")
        writer = csv.writer(stream)
        writer.writerow(columns)
        writer.writerows(rows)
        return stream.getvalue()

    def verify(self, text, expected, columns):
        complete = Mock(side_effect=lambda _name, actual, oracle, _path:
                        self.assertEqual(actual, oracle))
        accepted = Mock(side_effect=AssertionError("CSV proof must read the committed file"))
        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory) / "result.csv"
            destination.write_text(text, encoding="utf-8", newline="")
            verify_output(None, destination, "csv", expected, columns,
                          name="csv-proof", accepted=accepted, complete=complete)
        complete.assert_called_once()

    def test_nested_cells_use_strict_json_and_keep_null_and_empty_rows(self):
        columns = ["items", "record", "text"]
        expected = [
            {"items": [1, None, 2], "record": {"label": 'é,"\n'}, "text": "line\nnext"},
            {"items": [], "record": None, "text": ""},
            {"items": None, "record": {}, "text": None},
        ]
        text = self.csv_text(columns, [
            ['[1,null,2]', '{"label":"é,\\\"\\n"}', "line\nnext"],
            ['[]', '', ''], ['', '{}', ''],
        ])
        self.verify(text, expected, columns)
        self.verify('items\n""\n"[]"\n', [{"items": None}, {"items": []}], ["items"])
        self.verify('items\n', [], ["items"])

    def test_corrupt_nested_cells_fail_before_complete_comparison(self):
        for cell in ('[1,', '{"id":1,"id":1}', '{"id":NaN}'):
            expected = [{"value": [1] if cell.startswith('[') else {"id": 1}}]
            with self.subTest(cell=cell), self.assertRaises(ValueError):
                self.verify(self.csv_text(["value"], [[cell]]), expected, ["value"])

    def test_missing_extra_or_changed_values_and_column_order_fail(self):
        expected = [{"items": [1, 2], "id": 7}]
        for text in (
            self.csv_text(["items", "id"], []),
            self.csv_text(["items", "id"], [['[1,2]', 7], ['[1,2]', 7]]),
            self.csv_text(["items", "id"], [['[1,3]', 7]]),
            self.csv_text(["items", "id"], [['[1,2]', 8]]),
            self.csv_text(["id", "items"], [[7, '[1,2]']]),
        ):
            with self.subTest(text=text), self.assertRaises((AssertionError, ValueError)):
                self.verify(text, expected, ["items", "id"])


if __name__ == "__main__":
    unittest.main()
