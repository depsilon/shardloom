import unittest
import tempfile
from pathlib import Path
from unittest import mock

from shardloom.client import ShardLoomClient
from shardloom.prepared_route import (
    CompatibilityPreparedVortexRoute,
    _local_path_fingerprint,
    _prepared_state_index_payload,
    _REUSE_MANIFEST_SCHEMA_VERSION,
    _stable_json_digest,
    _TRADITIONAL_SOURCE_ADMISSION_SCHEMA_HASH,
)


class PreparedRouteEvidenceTests(unittest.TestCase):
    def test_prepared_state_index_uses_rust_source_admission_schema_hash(self) -> None:
        payload, index_digest = _prepared_state_index_payload(
            {
                "source_admission_packet_digest": "sha256:packet",
                "prepare_policy": {"strategy": "prepare_once"},
                "prepare_fields": {
                    "vortex_array_build_strategy": "scalar_rows_to_vortex_struct",
                    "vortex_array_build_input_layout": "materialized_rows",
                    "native_io_certificate_status": "certified",
                },
                "prepared_artifacts": {
                    "fact": {"path": "fact.vortex", "digest": "sha256:fact"},
                    "dim": {"path": "dim.vortex", "digest": "sha256:dim"},
                },
                "manifest_digest": "sha256:manifest",
                "manifest_path": "target/.shardloom/prepared-vortex-reuse-manifest.json",
            }
        )

        self.assertEqual(
            payload["index_key"]["schema_hash"],
            _TRADITIONAL_SOURCE_ADMISSION_SCHEMA_HASH,
        )
        self.assertTrue(payload["index_digest"].startswith("sha256:"))
        self.assertEqual(payload["index_digest"], index_digest)

    def test_source_admission_packet_uses_rust_source_schema_hash(self) -> None:
        route = CompatibilityPreparedVortexRoute(
            client=ShardLoomClient(binary=("unused",)),
            fact_input="fact.csv",
            dim_input="dim.csv",
            workspace="target/prepared",
            input_format="csv",
        )

        packet = route._source_admission_packet(None, None, None)

        self.assertEqual(
            packet["source_schema_hash"],
            _TRADITIONAL_SOURCE_ADMISSION_SCHEMA_HASH,
        )
        self.assertEqual(packet["source_path_fingerprint_kind"], "local_path_size_mtime")
        self.assertEqual(packet["digest_policy"]["status"], "metadata_fingerprint")
        self.assertFalse(
            packet["digest_policy"]["normal_warm_reuse_content_digest_requested"]
        )
        self.assertTrue(packet["packet_digest"].startswith("sha256:"))

    def test_local_path_fingerprint_is_metadata_first_by_default(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            path = Path(tempdir) / "source.csv"
            path.write_text("id,label\n1,alpha\n", encoding="utf-8")

            with mock.patch(
                "shardloom.prepared_route._file_content_digest",
                side_effect=AssertionError("content digest should not run by default"),
            ):
                fingerprint = _local_path_fingerprint(path)

            self.assertEqual(fingerprint["kind"], "local_file_size_mtime")
            self.assertIsNone(fingerprint["content_digest"])
            self.assertEqual(
                fingerprint["content_digest_status"],
                "not_requested_metadata_first_hot_runtime",
            )
            self.assertEqual(
                fingerprint["digest_policy"],
                "metadata_size_mtime_normal_warm_reuse",
            )

    def test_local_path_fingerprint_can_request_explicit_proof_digest(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            path = Path(tempdir) / "source.csv"
            path.write_text("id,label\n1,alpha\n", encoding="utf-8")

            fingerprint = _local_path_fingerprint(path, content_digest=True)

            self.assertEqual(fingerprint["kind"], "local_file_sha256_size_mtime")
            self.assertTrue(fingerprint["content_digest"].startswith("sha256:"))
            self.assertEqual(
                fingerprint["content_digest_status"],
                "computed_for_explicit_proof_fingerprint",
            )
            self.assertEqual(
                fingerprint["digest_policy"],
                "content_digest_size_mtime_explicit_proof",
            )

    def test_directory_fingerprint_is_metadata_first_by_default(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            partition = root / "event_date=2026-08-30"
            partition.mkdir()
            (partition / "part-0.csv").write_text("id,label\n1,alpha\n", encoding="utf-8")

            with mock.patch.object(
                Path,
                "rglob",
                side_effect=AssertionError("directory hot path should not traverse"),
            ):
                fingerprint = _local_path_fingerprint(root)

            self.assertEqual(
                fingerprint["kind"],
                "local_directory_root_size_mtime_source_state_candidate",
            )
            self.assertIsNone(fingerprint["content_digest"])
            self.assertEqual(
                fingerprint["content_digest_status"],
                "not_requested_metadata_first_directory_identity",
            )
            self.assertEqual(
                fingerprint["digest_policy"],
                "directory_root_metadata_normal_warm_reuse_source_state_preferred",
            )
            self.assertEqual(
                fingerprint["directory_identity_source"],
                "root_metadata_source_state_candidate",
            )
            self.assertFalse(fingerprint["directory_tree_walk_performed"])
            self.assertEqual(fingerprint["directory_files_walked"], 0)
            self.assertEqual(fingerprint["directory_stats_performed"], 1)
            self.assertTrue(fingerprint["directory_source_state_identity_preferred"])
            self.assertEqual(fingerprint["proof_tier"], "metadata_first_hot_runtime")

    def test_directory_fingerprint_explicit_proof_walks_tree(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            partition = root / "event_date=2026-08-30"
            partition.mkdir()
            (partition / "part-0.csv").write_text("id,label\n1,alpha\n", encoding="utf-8")
            (partition / "part-1.csv").write_text("id,label\n2,beta\n", encoding="utf-8")

            fingerprint = _local_path_fingerprint(root, content_digest=True)

            self.assertEqual(
                fingerprint["kind"],
                "local_directory_tree_sha256_size_mtime",
            )
            self.assertTrue(fingerprint["content_digest"].startswith("sha256:"))
            self.assertEqual(
                fingerprint["content_digest_status"],
                "computed_for_explicit_proof_directory_tree_fingerprint",
            )
            self.assertEqual(
                fingerprint["digest_policy"],
                "directory_tree_content_digest_explicit_proof",
            )
            self.assertEqual(
                fingerprint["directory_identity_source"],
                "recursive_tree_explicit_proof",
            )
            self.assertTrue(fingerprint["directory_tree_walk_performed"])
            self.assertEqual(fingerprint["directory_files_walked"], 2)
            self.assertGreaterEqual(fingerprint["directory_stats_performed"], 3)
            self.assertEqual(fingerprint["proof_tier"], "explicit_recursive_proof")

    def test_directory_fingerprint_normal_path_is_bounded_for_many_files(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            for index in range(100):
                partition = root / f"bucket={index:03d}"
                partition.mkdir()
                (partition / "part.csv").write_text(
                    f"id,label\n{index},value-{index}\n",
                    encoding="utf-8",
                )

            with mock.patch.object(
                Path,
                "rglob",
                side_effect=AssertionError("many-file directory identity must stay bounded"),
            ):
                fingerprint = _local_path_fingerprint(root)

            self.assertEqual(
                fingerprint["kind"],
                "local_directory_root_size_mtime_source_state_candidate",
            )
            self.assertFalse(fingerprint["directory_tree_walk_performed"])
            self.assertEqual(fingerprint["directory_files_walked"], 0)
            self.assertEqual(fingerprint["directory_stats_performed"], 1)
            self.assertTrue(fingerprint["directory_manifest_identity_preferred"])
            self.assertEqual(fingerprint["proof_tier"], "metadata_first_hot_runtime")

    def test_reuse_request_payload_reports_directory_sourcestate_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            fact_dir = root / "fact"
            dim_dir = root / "dim"
            fact_dir.mkdir()
            dim_dir.mkdir()
            (fact_dir / "part-0.csv").write_text("id,dim_key\n1,10\n", encoding="utf-8")
            (dim_dir / "part-0.csv").write_text("dim_key,label\n10,alpha\n", encoding="utf-8")
            route = CompatibilityPreparedVortexRoute(
                client=ShardLoomClient(binary=("unused",)),
                fact_input=fact_dir,
                dim_input=dim_dir,
                workspace=root / "prepared",
                input_format="csv",
            )

            with mock.patch.object(
                Path,
                "rglob",
                side_effect=AssertionError("normal reuse payload should not traverse directories"),
            ):
                payload = route._reuse_request_payload()

            packet = payload["source_admission_packet"]
            self.assertEqual(
                packet["source_fingerprint_kinds"],
                "local_directory_root_size_mtime_source_state_candidate",
            )
            self.assertEqual(
                packet["source_directory_identity_sources"],
                "root_metadata_source_state_candidate",
            )
            self.assertFalse(packet["source_directory_tree_walk_performed"])
            self.assertEqual(packet["source_directory_files_walked"], 0)
            self.assertEqual(packet["source_directory_stats_performed"], 2)
            self.assertTrue(packet["source_directory_source_state_identity_preferred"])
            self.assertTrue(packet["source_directory_manifest_identity_preferred"])
            self.assertEqual(packet["source_directory_proof_tiers"], "metadata_first_hot_runtime")

    def test_role_repair_rejects_stale_unchanged_prepared_artifact(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            fact = root / "fact.csv"
            dim = root / "dim.csv"
            fact_vortex = root / "prepared" / "fact.vortex"
            dim_vortex = root / "prepared" / "dim.vortex"
            fact.write_text("id,dim_key\n1,10\n", encoding="utf-8")
            dim.write_text("dim_key,label\n10,alpha\n", encoding="utf-8")
            fact_vortex.parent.mkdir()
            fact_vortex.write_text("fact artifact v1", encoding="utf-8")
            dim_vortex.write_text("dim artifact v1", encoding="utf-8")
            route = CompatibilityPreparedVortexRoute(
                client=ShardLoomClient(binary=("unused",)),
                fact_input=fact,
                dim_input=dim,
                workspace=root / "prepared",
                input_format="csv",
            )
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

            fact.write_text("id,dim_key\n1,20\n", encoding="utf-8")
            dim_vortex.write_text("dim artifact stale", encoding="utf-8")
            request = route._reuse_request_payload()
            changed_roles = route._changed_input_roles(manifest, request)

            self.assertEqual(changed_roles, ("fact_input",))
            self.assertEqual(
                route._role_scoped_repair_blocker(manifest, request, changed_roles),
                "dim_unchanged_prepared_artifact_fingerprint_changed",
            )

    def test_role_repair_rejects_tampered_manifest_digest(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            fact = root / "fact.csv"
            dim = root / "dim.csv"
            fact_vortex = root / "prepared" / "fact.vortex"
            dim_vortex = root / "prepared" / "dim.vortex"
            fact.write_text("id,dim_key\n1,10\n", encoding="utf-8")
            dim.write_text("dim_key,label\n10,alpha\n", encoding="utf-8")
            fact_vortex.parent.mkdir()
            fact_vortex.write_text("fact artifact v1", encoding="utf-8")
            dim_vortex.write_text("dim artifact v1", encoding="utf-8")
            route = CompatibilityPreparedVortexRoute(
                client=ShardLoomClient(binary=("unused",)),
                fact_input=fact,
                dim_input=dim,
                workspace=root / "prepared",
                input_format="csv",
            )
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
            manifest["prepared_artifacts"]["fact"]["digest"] = "sha256:tampered"

            fact.write_text("id,dim_key\n1,20\n", encoding="utf-8")
            request = route._reuse_request_payload()
            changed_roles = route._changed_input_roles(manifest, request)

            self.assertEqual(changed_roles, ("fact_input",))
            self.assertEqual(
                route._role_scoped_repair_blocker(manifest, request, changed_roles),
                "reuse_manifest_digest_mismatch_requires_full_prepare",
            )

    def test_role_repair_rejects_cdc_route_shape_change(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            fact = root / "fact.csv"
            dim = root / "dim.csv"
            cdc = root / "cdc.csv"
            fact_vortex = root / "prepared" / "fact.vortex"
            dim_vortex = root / "prepared" / "dim.vortex"
            fact.write_text("id,dim_key\n1,10\n", encoding="utf-8")
            dim.write_text("dim_key,label\n10,alpha\n", encoding="utf-8")
            cdc.write_text("id,op,value\n1,update,9\n", encoding="utf-8")
            fact_vortex.parent.mkdir()
            fact_vortex.write_text("fact artifact v1", encoding="utf-8")
            dim_vortex.write_text("dim artifact v1", encoding="utf-8")
            base_route = CompatibilityPreparedVortexRoute(
                client=ShardLoomClient(binary=("unused",)),
                fact_input=fact,
                dim_input=dim,
                workspace=root / "prepared",
                input_format="csv",
            )
            manifest = {
                **base_route._reuse_request_payload(),
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
            cdc_route = CompatibilityPreparedVortexRoute(
                client=ShardLoomClient(binary=("unused",)),
                fact_input=fact,
                dim_input=dim,
                cdc_delta_input=cdc,
                workspace=root / "prepared",
                input_format="csv",
            )
            request = cdc_route._reuse_request_payload()
            changed_roles = cdc_route._changed_input_roles(manifest, request)

            self.assertEqual(changed_roles, ("cdc_delta_input",))
            self.assertEqual(
                cdc_route._role_scoped_repair_blocker(manifest, request, changed_roles),
                "prepare_policy_changed_requires_full_prepare",
            )


if __name__ == "__main__":
    unittest.main()
