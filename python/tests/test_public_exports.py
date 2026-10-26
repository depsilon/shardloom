# SPDX-License-Identifier: Apache-2.0
"""Public import, discovery, and generated typing contracts."""

from __future__ import annotations

import hashlib
import importlib
import json
import os
from pathlib import Path
import subprocess
import sys
import unittest

from shardloom._exports import (
    EXPORTS,
    LEGACY_EXPORTS,
    NAMESPACE_MODULES,
    STALE_EXPORTS,
    names_for_audience,
)

ROOT = Path(__file__).resolve().parents[2]
PACKAGE = ROOT / "python" / "src" / "shardloom"
EXPECTED_STALE_EXPORTS = frozenset(
    {
        "V1_SOURCE_PREPARED_GENERATED_ROUTE_IDS",
        "V1_SOURCE_PREPARED_INTERNAL_SMOKE_ROUTE",
        "V1_SOURCE_PREPARED_INTERNAL_SMOKE_ROUTE_IDS",
    }
)
NEW_EXPORTS = frozenset(
    {
        "__version__",
        "Result",
        "from_result",
        "ResultType",
        "ShardLoomError",
        "ShardLoomInstallationError",
        "ShardLoomClosedError",
        "ShardLoomTimeoutError",
        "Window",
        "WindowSpec",
    }
)
ADVANCED_RUNTIME_NAMES = frozenset(
    {
        "ShardLoomSession",
        "SessionSqlResult",
        "SessionPreparedState",
        "LocalFileFingerprint",
        "session",
        "VortexWorkflowExecutionReport",
        "WorkflowColumnTransform",
        "WorkflowPlanTransform",
        "WorkflowRowTransform",
        "ETL_INPUT_FORMATS",
    }
)


class PublicExportTests(unittest.TestCase):
    def test_historical_names_keep_module_fingerprint_and_explicit_audience(self) -> None:
        self.assertEqual(len(LEGACY_EXPORTS), 243)
        normalized = sorted(
            (name, module) for name, (module, _audience) in LEGACY_EXPORTS.items()
        )
        fingerprint = hashlib.sha256(
            json.dumps(normalized, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
        self.assertEqual(
            fingerprint,
            "9afbe0b14f608a0ba14e5ed7b09bf0cea12669674762e90a7e915acd92bf761d",
        )
        self.assertEqual(set(LEGACY_EXPORTS) - set(EXPORTS), STALE_EXPORTS)
        self.assertEqual(STALE_EXPORTS, EXPECTED_STALE_EXPORTS)
        self.assertEqual(set(EXPORTS), (set(LEGACY_EXPORTS) - STALE_EXPORTS) | NEW_EXPORTS)

        audiences = ("application", "diagnostics", "testing")
        groups = {audience: set(names_for_audience(audience)) for audience in audiences}
        self.assertEqual(sum(map(len, groups.values())), len(set().union(*groups.values())))
        self.assertEqual(set().union(*groups.values()), set(EXPORTS))
        self.assertEqual(
            {name for names in groups.values() for name in names}, set(EXPORTS)
        )
        self.assertTrue(all(audience in set(audiences) for _, audience in EXPORTS.values()))
        self.assertEqual(NAMESPACE_MODULES, {"native", "diagnostics", "testing", "functions", "types", "adbc", "dbapi", "notebook"})

        diagnostic_names = {
            "ClaimSummary", "EvidenceSummary", "FallbackStatus", "FieldEntry",
            "OutputEnvelope", "RuntimeActivationSummary", "RuntimeEnvelopeValidationIssue",
            "RuntimeEnvelopeValidationReport",
            "validate_runtime_execution_envelope", "validate_runtime_execution_fields",
            "CommandMetadataReport", "ComputeCapabilityMatrix", "ComputeCapabilityRow",
            "EngineCapabilityMatrix", "EngineSelectionPlan", "EvidenceSchemaRegistryReport",
            "EvidenceAwareOptimizerTraceReport", "ExecutionEvidenceSlot",
            "ExecutionResultEnvelopeView",
            "PublicWorkflowExecution", "PublicWorkflowRoute", "ShardLoomClient",
            "CapabilityPosture", "CapabilityView", "ContextCapabilities",
            "DataFrameMethodCapability", "DataFrameMethodCapabilityMatrix",
            "NativeUnsupportedCoverageRow", "NativeVortexAdmissionLane",
            "OperatorFamilyCoverageRow", "PreparedVortexScanPushdownRow",
            "PredicateDtypeCoverageRow", "ProductionUnsupportedDiagnosticRow",
            "WorkflowProfileReport", "WorkflowSchemaField", "WorkflowSchemaMismatch",
            "WorkflowSchemaReport", "WorkflowSchemaValidationReport",
            "UnsupportedWorkflowOperationReport", "UnsupportedWorkflowReport",
            *ADVANCED_RUNTIME_NAMES,
        }
        self.assertEqual(groups["diagnostics"], diagnostic_names)
        self.assertFalse(groups["testing"] & ADVANCED_RUNTIME_NAMES)

        internal_query_names = {
            "UnsupportedWorkflowOperationReport", "UnsupportedWorkflowReport",
            "VortexWorkflowExecutionReport", "WorkflowCertificationReport",
            "WorkflowColumnTransform", "WorkflowDataQualityCheckResult",
            "WorkflowDataQualityReport", "WorkflowNotebookPreview",
            "WorkflowOperation", "WorkflowPlanTransform", "WorkflowProfileReport",
            "WorkflowQuarantineReport", "WorkflowRowTransform", "WorkflowSchemaField",
            "WorkflowSchemaMismatch", "WorkflowSchemaReport",
            "WorkflowSchemaValidationReport", "WorkflowSource",
            "dataframe_generated_with_column", "dataframe_source_free_projection",
            "fixture_double_i64", "sql_literal_select", "sql_values",
        }
        explicit_application_names = {
            "ExecutionResources", "ExecutionResourceLimits",
            "ShardLoomResourceConfigurationError",
            "ShardLoomContext", "context", "Diagnostic", "ShardLoomBinaryNotFoundError",
            "ShardLoomCommandError", "ShardLoomProtocolError", "ResultBatch",
            "ResultBatchIterator",
        }
        expected_application = {
            name for name, (module, _audience) in LEGACY_EXPORTS.items()
            if module == "query" and name not in internal_query_names
        } | explicit_application_names | NEW_EXPORTS
        self.assertEqual(groups["application"], expected_application)

    def test_stale_legacy_names_are_not_resolvable(self) -> None:
        import shardloom

        for name in sorted(STALE_EXPORTS):
            with self.subTest(name=name):
                self.assertNotIn(name, EXPORTS)
                with self.assertRaises(AttributeError):
                    getattr(shardloom, name)

    def test_every_current_export_resolves_once(self) -> None:
        import shardloom

        resolved = {name: getattr(shardloom, name) for name in sorted(EXPORTS)}
        self.assertEqual(set(resolved), set(EXPORTS))
        self.assertTrue(all(name in shardloom.__dict__ for name in resolved))

    def test_root_discovery_stays_curated_after_compatibility_resolution(self) -> None:
        import shardloom

        expected = tuple(sorted(shardloom.__all__))
        self.assertEqual(tuple(dir(shardloom)), expected)
        self.assertFalse(
            any(name.startswith("_") and name not in {"__version__"} for name in expected)
        )

        # A maintainer-only name remains directly importable without joining discovery.
        self.assertEqual(
            shardloom.ClaimGateCloseoutReport.__name__, "ClaimGateCloseoutReport"
        )
        self.assertEqual(tuple(dir(shardloom)), expected)
        self.assertEqual(tuple(shardloom.__all__), expected)

        importlib.import_module("shardloom.context")
        self.assertTrue(callable(shardloom.context))
        importlib.import_module("shardloom.session")
        self.assertTrue(callable(shardloom.session))
        self.assertEqual(tuple(dir(shardloom)), expected)

    def test_root_and_namespaces_are_lazy_without_optional_or_native_imports(self) -> None:
        script = r'''
import sys
import shardloom
root = tuple(dir(shardloom))
assert root == tuple(sorted(shardloom.__all__))
import shardloom.diagnostics as diagnostics
import shardloom.testing as testing
assert tuple(dir(diagnostics)) == tuple(sorted(diagnostics.__all__))
assert tuple(dir(testing)) == tuple(sorted(testing.__all__))
assert tuple(dir(shardloom)) == root
for name in (
    "shardloom.query", "shardloom.context", "shardloom.client",
    "shardloom.session", "shardloom.execution_resources", "shardloom.native",
    "shardloom._native", "shardloom._native_backend",
    "numpy", "pandas", "pyarrow",
):
    assert name not in sys.modules, f"unexpected eager import: {name}"
'''
        environment = os.environ.copy()
        environment["PYTHONPATH"] = str(PACKAGE.parent)
        completed = subprocess.run(
            [sys.executable, "-c", script],
            cwd=ROOT,
            env=environment,
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)

    def test_generated_python_stubs_are_current_and_checked_in(self) -> None:
        for name in ("__init__.pyi", "diagnostics.pyi", "testing.pyi"):
            self.assertTrue((PACKAGE / name).is_file(), f"missing generated typing stub {name}")
        root_stub = (PACKAGE / "__init__.pyi").read_text(encoding="utf-8")
        self.assertIn("from ._version import __version__ as __version__", root_stub)
        self.assertNotIn("ClaimGateCloseoutReport", root_stub)
        self.assertNotIn("VortexWorkflowExecutionReport", root_stub)
        self.assertIn("from .query import Result as Result", root_stub)
        self.assertIn("def __getattr__(name: str) -> Any", root_stub)
        completed = subprocess.run(
            [sys.executable, "scripts/write_python_api_exports.py", "--check"],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(completed.returncode, 0, completed.stdout + completed.stderr)


if __name__ == "__main__":
    unittest.main()
