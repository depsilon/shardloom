import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
PYTHON_SRC = REPO_ROOT / "python" / "src"
if str(PYTHON_SRC) not in sys.path:
    sys.path.insert(0, str(PYTHON_SRC))

from shardloom import ShardLoomContext
from shardloom.client import ShardLoomClient
from shardloom.prepared_route import (
    CompatibilityPreparedVortexRoute,
    _local_path_fingerprint,
    _manifest_path,
    _REUSE_MANIFEST_SCHEMA_VERSION,
    _stable_json_digest,
)


def _load_scope_validator():
    script = REPO_ROOT / "scripts" / "check_v1_source_prepared_state_scope.py"
    spec = importlib.util.spec_from_file_location(
        "check_v1_source_prepared_state_scope_for_test",
        script,
    )
    if spec is None or spec.loader is None:
        raise RuntimeError("cannot load v1 source/prepared-state scope validator")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _write_manifest(path: Path, manifest: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(manifest, sort_keys=True), encoding="utf-8")


class V1SourcePreparedStateScopeTests(unittest.TestCase):
    def test_context_report_exposes_v1_scope_contract(self) -> None:
        report = ShardLoomContext(client=None).source_prepared_state_scope_report()

        self.assertEqual(
            report.schema_version,
            "shardloom.v1_source_prepared_state_scope.v1",
        )
        self.assertEqual(
            report.canonical_route,
            "UniversalIngress -> SourceState -> vortex_ingest -> "
            "VortexPreparedState -> prepared_vortex",
        )
        self.assertTrue(report.v1_scope_ready)
        self.assertTrue(report.all_no_fallback_no_external_engine)
        self.assertTrue(report.all_prepared_routes_expose_reuse_contract)
        self.assertTrue(report.all_internal_source_smoke_routes_are_labeled_non_persistent)
        self.assertEqual(len(report.invalidation_case_ids), 9)
        self.assertIn("global_hidden_cache", report.unsupported_boundary_ids)
        self.assertFalse(report.performance_claim_allowed)
        self.assertFalse(report.production_claim_allowed)
        self.assertFalse(report.spark_replacement_claim_allowed)

    def test_scope_validator_passes_current_repo_contract(self) -> None:
        module = _load_scope_validator()

        report = module.build_report(REPO_ROOT)

        self.assertEqual(report["status"], "passed", report["blockers"])
        self.assertTrue(report["v1_scope_ready"])
        self.assertTrue(report["all_no_fallback_no_external_engine"])
        self.assertTrue(report["source_prepared_benchmark_required_fields_ready"])
        self.assertGreater(
            report["source_prepared_benchmark_rows_with_required_fields"],
            0,
        )
        self.assertEqual(len(report["invalidation_case_ids"]), 9)
        self.assertEqual(len(report["golden_fixture_paths"]), 3)


class PreparedStateReuseInvalidationMatrixTests(unittest.TestCase):
    def _route_workspace(
        self,
        root: Path,
    ) -> tuple[CompatibilityPreparedVortexRoute, Path, Path, Path, Path]:
        fact = root / "fact.csv"
        dim = root / "dim.csv"
        workspace = root / "prepared"
        fact_vortex = workspace / "fact.vortex"
        dim_vortex = workspace / "dim.vortex"
        root.mkdir(parents=True, exist_ok=True)
        fact.write_text("id,dim_key,value\n1,10,5\n", encoding="utf-8")
        dim.write_text("dim_key,label\n10,alpha\n", encoding="utf-8")
        workspace.mkdir(parents=True, exist_ok=True)
        fact_vortex.write_text("fact artifact v1", encoding="utf-8")
        dim_vortex.write_text("dim artifact v1", encoding="utf-8")
        route = CompatibilityPreparedVortexRoute(
            client=ShardLoomClient(binary=("unused",)),
            fact_input=fact,
            dim_input=dim,
            workspace=workspace,
            input_format="csv",
        )
        return route, fact, dim, fact_vortex, dim_vortex

    def _manifest(
        self,
        route: CompatibilityPreparedVortexRoute,
        fact_vortex: Path,
        dim_vortex: Path,
    ) -> dict:
        manifest = {
            **route._reuse_request_payload(),
            "schema_version": _REUSE_MANIFEST_SCHEMA_VERSION,
            "prepared_artifacts": {
                "fact": {
                    "path": str(fact_vortex.resolve(strict=False)),
                    "fingerprint": _local_path_fingerprint(fact_vortex),
                    "digest": "sha256:fact",
                },
                "dim": {
                    "path": str(dim_vortex.resolve(strict=False)),
                    "fingerprint": _local_path_fingerprint(dim_vortex),
                    "digest": "sha256:dim",
                },
            },
            "fallback_attempted": False,
            "external_engine_invoked": False,
        }
        manifest["manifest_digest"] = _stable_json_digest(manifest)
        return manifest

    def _write_valid_manifest(
        self,
        route: CompatibilityPreparedVortexRoute,
        fact_vortex: Path,
        dim_vortex: Path,
    ) -> dict:
        manifest = self._manifest(route, fact_vortex, dim_vortex)
        _write_manifest(_manifest_path(route.workspace), manifest)
        return manifest

    def _rewrite_manifest(self, route: CompatibilityPreparedVortexRoute, manifest: dict) -> None:
        payload = {str(key): value for key, value in manifest.items() if key != "manifest_digest"}
        manifest["manifest_digest"] = _stable_json_digest(payload)
        _write_manifest(_manifest_path(route.workspace), manifest)

    def test_reuse_invalidation_matrix_cases(self) -> None:
        cases = {
            "cold_prepare_no_manifest": (False, "no_reuse_manifest", "no_reuse_manifest"),
            "warm_reuse_manifest_match": (True, "manifest_fingerprints_match", "none"),
            "source_changed": (
                False,
                "fact_input_fingerprint_changed",
                "fact_input_fingerprint_changed",
            ),
            "artifact_changed": (
                False,
                "fact_prepared_artifact_fingerprint_changed",
                "fact_prepared_artifact_fingerprint_changed",
            ),
            "schema_changed": (
                False,
                "source_admission_packet_changed",
                "source_admission_packet_changed",
            ),
            "policy_changed": (False, "prepare_policy_changed", "prepare_policy_changed"),
            "version_changed": (
                False,
                "reuse_manifest_schema_mismatch",
                "reuse_manifest_schema_mismatch",
            ),
            "missing_artifact": (
                False,
                "fact_prepared_artifact_manifest_missing",
                "fact_prepared_artifact_manifest_missing",
            ),
            "corrupted_manifest": (
                False,
                "reuse_manifest_unreadable",
                "reuse_manifest_unreadable:JSONDecodeError",
            ),
        }
        for case_id, expected in cases.items():
            with self.subTest(case_id=case_id), tempfile.TemporaryDirectory() as tempdir:
                root = Path(tempdir)
                route, fact, _dim, fact_vortex, dim_vortex = self._route_workspace(root)
                if case_id != "cold_prepare_no_manifest":
                    manifest = self._write_valid_manifest(route, fact_vortex, dim_vortex)
                else:
                    manifest = {}

                if case_id == "source_changed":
                    fact.write_text("id,dim_key,value\n1,20,9\n", encoding="utf-8")
                elif case_id == "artifact_changed":
                    fact_vortex.write_text("fact artifact v2", encoding="utf-8")
                elif case_id == "schema_changed":
                    manifest["source_admission_packet_digest"] = "sha256:changed-schema"
                    manifest["route_request_digest"] = "sha256:old-route-request"
                    self._rewrite_manifest(route, manifest)
                elif case_id == "policy_changed":
                    manifest["prepare_policy"] = {
                        **manifest["prepare_policy"],
                        "allow_overwrite": True,
                    }
                    manifest["route_request_digest"] = "sha256:old-route-request"
                    self._rewrite_manifest(route, manifest)
                elif case_id == "version_changed":
                    manifest["schema_version"] = "shardloom.python.prepared_vortex_reuse_manifest.v0"
                    self._rewrite_manifest(route, manifest)
                elif case_id == "missing_artifact":
                    del manifest["prepared_artifacts"]["fact"]
                    self._rewrite_manifest(route, manifest)
                elif case_id == "corrupted_manifest":
                    _manifest_path(route.workspace).write_text("{", encoding="utf-8")

                decision = route._prepared_state_reuse_decision()
                expected_hit, expected_reason, expected_invalidation = expected
                self.assertEqual(decision.hit, expected_hit)
                self.assertEqual(decision.reason, expected_reason)
                self.assertEqual(
                    decision.invalidation_reason,
                    expected_invalidation,
                )

    def test_reuse_is_workspace_scoped_not_hidden_global_cache(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            route, fact, dim, fact_vortex, dim_vortex = self._route_workspace(root / "one")
            self._write_valid_manifest(route, fact_vortex, dim_vortex)
            second_workspace = root / "two" / "prepared"
            second_route = CompatibilityPreparedVortexRoute(
                client=ShardLoomClient(binary=("unused",)),
                fact_input=fact,
                dim_input=dim,
                workspace=second_workspace,
                input_format="csv",
            )

            decision = second_route._prepared_state_reuse_decision()

            self.assertFalse(decision.hit)
            self.assertEqual(decision.reason, "no_reuse_manifest")
            self.assertEqual(decision.invalidation_reason, "no_reuse_manifest")


if __name__ == "__main__":
    unittest.main()
