from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path
from typing import Any, Mapping


REPO_ROOT = Path(__file__).resolve().parents[2]
PYTHON_SRC = REPO_ROOT / "python" / "src"
if str(PYTHON_SRC) not in sys.path:
    sys.path.insert(0, str(PYTHON_SRC))

from shardloom import Diagnostic, FallbackStatus, OutputEnvelope


MATRIX_PATH = REPO_ROOT / "docs/release/v1-api-schema-stability-matrix.json"


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _value_at_path(payload: Mapping[str, Any], path: str) -> Any:
    current: Any = payload
    for part in path.split("."):
        if not isinstance(current, Mapping):
            raise AssertionError(f"{path} traversed through non-mapping value")
        if part not in current:
            raise AssertionError(f"{path} missing from fixture")
        current = current[part]
    return current


def _complete_output_envelope(payload: Mapping[str, Any]) -> dict[str, Any]:
    complete = dict(payload)
    complete.setdefault("result", {"fields": []})
    complete.setdefault("result_refs", [])
    complete.setdefault("artifact_refs", [])
    complete.setdefault("policy", {"fields": []})
    complete.setdefault("lifecycle", {"fields": []})
    complete.setdefault("capability_snapshot", {"fields": []})
    return complete


class V1ApiSchemaStabilityAccessorTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.matrix = _load_json(MATRIX_PATH)
        cls.fixtures = _load_json(REPO_ROOT / cls.matrix["fixture_path"])["fixtures"]
        cls.contracts = {
            row["surface_id"]: _load_json(REPO_ROOT / row["schema_path"])
            for row in cls.matrix["surfaces"]
        }

    def test_python_accessors_cover_every_declared_stable_field(self) -> None:
        for surface_id, contract in self.contracts.items():
            with self.subTest(surface_id=surface_id):
                fixture = self.fixtures[surface_id]
                for field in contract["required_fields"]:
                    value = _value_at_path(fixture, field["path"])
                    if field["type"] != "nullable_string":
                        self.assertIsNotNone(value, field["path"])
                for field_path in contract["no_fallback_fields"]:
                    self.assertIs(_value_at_path(fixture, field_path), False)

                if surface_id == "output_envelope":
                    envelope = OutputEnvelope.from_json(_complete_output_envelope(fixture))
                    self.assertEqual(envelope.schema_version, "shardloom.output.v2")
                    self.assertEqual(envelope.command, fixture["command"])
                    self.assertEqual(envelope.status, fixture["status"])
                    self.assertEqual(envelope.summary, fixture["summary"])
                    self.assertEqual(envelope.human_text, fixture["human_text"])
                    self.assertFalse(envelope.fallback.attempted)
                    self.assertFalse(envelope.fallback.allowed)
                    self.assertEqual(envelope.diagnostics, ())
                    self.assertEqual(len(envelope.fields), 0)
                elif surface_id == "diagnostic":
                    diagnostic = Diagnostic.from_json(fixture)
                    self.assertEqual(diagnostic.code, fixture["code"])
                    self.assertEqual(diagnostic.severity, fixture["severity"])
                    self.assertEqual(diagnostic.category, fixture["category"])
                    self.assertEqual(diagnostic.message, fixture["message"])
                    self.assertEqual(diagnostic.feature, fixture["feature"])
                    self.assertEqual(diagnostic.reason, fixture["reason"])
                    self.assertEqual(
                        diagnostic.suggested_next_step,
                        fixture["suggested_next_step"],
                    )
                    self.assertFalse(diagnostic.fallback.attempted)
                    self.assertFalse(diagnostic.fallback.allowed)
                elif surface_id == "fallback_status":
                    fallback = FallbackStatus.from_json(fixture)
                    self.assertFalse(fallback.attempted)
                    self.assertFalse(fallback.allowed)
                    self.assertIsNone(fallback.engine)
                    self.assertEqual(fallback.reason, fixture["reason"])
                else:
                    envelope = OutputEnvelope.from_field_mapping(fixture, command=surface_id)
                    for field in contract["required_fields"]:
                        field_path = field["path"]
                        value = _value_at_path(fixture, field_path)
                        if isinstance(value, bool):
                            self.assertIs(envelope.field_bool(field_path), value)
                        elif isinstance(value, int):
                            self.assertEqual(envelope.field_int(field_path), value)
                        else:
                            self.assertEqual(envelope.field(field_path), str(value))
                    for field_path in contract["no_fallback_fields"]:
                        self.assertIs(envelope.field_bool(field_path), False)


if __name__ == "__main__":
    unittest.main()
