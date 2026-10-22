"""Exercise the exact maintained guide program against a clean installed package.

Opt in with SHARDLOOM_TEST_INSTALLED_QUICKSTART=1 from that package's Python.
The ordinary unit suite checks the snippet structure without starting an engine.
"""

from __future__ import annotations

import ast
import contextlib
import io
import os
from pathlib import Path
import re
import unittest

ROOT = Path(__file__).resolve().parents[2]
SOURCE = ROOT / "examples/paid-orders/quickstart.py"
EXPECTED = ({"order_id": 1, "status": "paid"}, {"order_id": 3, "status": "paid"})


def snippets() -> dict[str, str]:
    return {
        name: code.strip()
        for name, code in re.findall(
            r"^# snippet: (\w+)\n([\s\S]*?)(?=^# snippet: |\Z)",
            SOURCE.read_text(encoding="utf-8"), re.MULTILINE,
        )
        if name != "end"
    }


class PublicQuickstartStructureTests(unittest.TestCase):
    def test_all_published_snippets_are_executable_python(self) -> None:
        regions = snippets()
        self.assertEqual(set(regions), {"setup", "dataframe", "sql", "write", "cleanup"})
        for name, code in regions.items():
            with self.subTest(region=name):
                ast.parse(code)


@unittest.skipUnless(os.environ.get("SHARDLOOM_TEST_INSTALLED_QUICKSTART") == "1",
                     "requires an explicitly selected clean installed package")
class InstalledPublicQuickstartTests(unittest.TestCase):
    def _exercise(self, transport: str) -> None:
        import shardloom as sl

        self.assertFalse(Path(sl.__file__).resolve().is_relative_to(ROOT / "python/src"))
        if expected_version := os.environ.get("SHARDLOOM_TEST_PACKAGE_VERSION"):
            self.assertEqual(sl.__version__, expected_version)
        regions = snippets()
        namespace: dict[str, object] = {}
        # This is the one platform edit explicitly documented in the guide.
        setup = regions["setup"].replace('transport="native"', f'transport="{transport}"')
        try:
            exec(compile(setup, str(SOURCE), "exec"), namespace)
            with contextlib.redirect_stdout(io.StringIO()) as output:
                for name in ("dataframe", "sql", "write"):
                    exec(compile(regions[name], str(SOURCE), "exec"), namespace)
            self.assertEqual(output.getvalue().splitlines(), [repr(EXPECTED)] * 3)
            for name in ("result", "sql_result", "reopened"):
                result = namespace[name]
                self.assertIsInstance(result, sl.Result)
                self.assertEqual(result.to_rows(), EXPECTED)
                self.assertEqual(result.result_columns, ("order_id", "status"))
                self.assertFalse(result.fallback_attempted)
                self.assertFalse(result.external_engine_invoked)
            self.assertEqual(namespace["result"].schema, namespace["sql_result"].schema)
            self.assertFalse(namespace["written"].fallback_attempted)
            self.assertFalse(namespace["written"].external_engine_invoked)
        finally:
            if "ctx" in namespace:
                namespace["ctx"].close()
            if "workspace" in namespace:
                namespace["workspace"].cleanup()
        # Complete native data remains usable after client close and source cleanup.
        self.assertTrue(namespace["ctx"].closed)
        self.assertEqual(namespace["result"].to_rows(), EXPECTED)

    def test_native(self) -> None:
        self._exercise("native")

    def test_explicit_process(self) -> None:
        self._exercise("subprocess")


if __name__ == "__main__":
    unittest.main()
