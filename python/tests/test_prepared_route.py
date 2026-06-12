import unittest
import tempfile
from pathlib import Path

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
        self.assertTrue(packet["packet_digest"].startswith("sha256:"))

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
