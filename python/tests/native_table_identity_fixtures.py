# SPDX-License-Identifier: Apache-2.0
"""Independent Iceberg identity-partition fixture for public table tests."""

from __future__ import annotations

from pathlib import Path
from typing import Any

if __package__:
    from .native_table_fixtures import (
        Fixture,
        iceberg_entry,
        iceberg_list_specs,
        iceberg_manifest_partition,
        iceberg_schema,
        id_field,
        uri,
    )
else:
    from native_table_fixtures import (
        Fixture,
        iceberg_entry,
        iceberg_list_specs,
        iceberg_manifest_partition,
        iceberg_schema,
        id_field,
        uri,
    )


def make_identity_partition_table(root: Path, pa: Any) -> tuple[Path, dict[str, int], str]:
    """Build a v2 table whose label column is reconstructed from identity partitions."""
    fixture = Fixture(root)
    logical = iceberg_schema(
        [
            {"id": 1, "name": "id", "required": True, "type": "long"},
            {"id": 2, "name": "label", "required": False, "type": "string"},
        ],
        0,
    )
    partition_spec = [
        {"name": "legacy_label", "transform": "identity", "source-id": 2, "field-id": 1000}
    ]
    partition_record = {
        "type": "record",
        "name": "r102",
        "fields": [
            {"name": "legacy_label", "type": ["null", "string"], "field-id": 1000, "default": None}
        ],
    }

    # The data files deliberately omit logical field 2; only the Iceberg partition
    # value can provide label for these rows.
    a = fixture.parquet("a.parquet", [id_field("id", pa.int64(), False, 1)], [pa.array([1, 2])])
    b = fixture.parquet("b.parquet", [id_field("id", pa.int64(), False, 1)], [pa.array([3])])
    c = fixture.parquet("c.parquet", [id_field("id", pa.int64(), False, 1)], [pa.array([4])])
    delete = fixture.parquet(
        "delete.parquet",
        [id_field("id", pa.int64(), False, 1), id_field("old_label", pa.string(), True, 2)],
        [pa.array([1]), pa.array(["alpha"], type=pa.string())],
    )

    def data_manifest(name: str, path: Path, value: str | None) -> Path:
        entry = iceberg_entry(path, 1 if path != a else 2, 0, 1, None)
        entry["data_file"]["partition"] = {"legacy_label": value}
        return iceberg_manifest_partition(
            fixture, name, logical, "data", [entry], 1, partition_spec, partition_record
        )

    a_manifest = data_manifest("a.avro", a, "alpha")
    b_manifest = data_manifest("b.avro", b, None)
    c_manifest = data_manifest("c.avro", c, "")
    spec0_partition = {"type": "record", "name": "r102", "fields": []}
    delete_manifest = iceberg_manifest_partition(
        fixture,
        "delete.avro",
        logical,
        "deletes",
        [iceberg_entry(delete, 1, 2, 2, [1, 2])],
        0,
        [],
        spec0_partition,
    )
    manifest_list = iceberg_list_specs(
        fixture,
        "snapshot-2.avro",
        [
            (a_manifest, 0, 1, 1),
            (b_manifest, 0, 1, 1),
            (c_manifest, 0, 1, 1),
            (delete_manifest, 1, 2, 0),
        ],
    )
    metadata = fixture.json(
        "metadata.json",
        {
            "format-version": 2,
            "location": uri(root),
            "current-schema-id": 0,
            "current-snapshot-id": 2,
            "default-spec-id": 1,
            "last-partition-id": 1000,
            "schemas": [logical],
            "partition-specs": [
                {"spec-id": 0, "fields": []},
                {"spec-id": 1, "fields": partition_spec},
            ],
            "snapshots": [
                {
                    "snapshot-id": 2,
                    "schema-id": 0,
                    "sequence-number": 2,
                    "manifest-list": uri(manifest_list),
                }
            ],
        },
    )
    return metadata, {"snapshot_id": 2}, "2"
