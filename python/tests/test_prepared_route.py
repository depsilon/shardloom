import unittest

from shardloom.client import ShardLoomClient
from shardloom.prepared_route import (
    CompatibilityPreparedVortexRoute,
    _prepared_state_index_payload,
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


if __name__ == "__main__":
    unittest.main()
