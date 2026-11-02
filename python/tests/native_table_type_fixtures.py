# SPDX-License-Identifier: Apache-2.0
"""Typed Delta, Iceberg, and Hudi fixtures for public table integration tests."""

from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal
import json
from pathlib import Path
from typing import Any

if __package__:
    from .native_table_fixtures import (
        Fixture,
        delta_add,
        delta_field,
        delta_metadata,
        id_field,
        iceberg_entry,
        iceberg_list,
        iceberg_manifest,
        iceberg_schema,
        uri,
    )
else:
    from native_table_fixtures import (
        Fixture,
        delta_add,
        delta_field,
        delta_metadata,
        id_field,
        iceberg_entry,
        iceberg_list,
        iceberg_manifest,
        iceberg_schema,
        uri,
    )


_UTC_VALUE = datetime(2020, 2, 29, 12, 34, 56, 123456, tzinfo=timezone.utc)
_LOCAL_VALUE = datetime(2020, 2, 29, 12, 34, 56, 123456)
_COLUMNS = [
    ("flag", True),
    ("i32", -17),
    ("i64", 2**40),
    ("f32", 1.25),
    ("f64", -2.5),
    ("day", date(2020, 2, 29)),
    ("utc", _UTC_VALUE),
    ("local", _LOCAL_VALUE),
    ("text", "λ-null"),
    ("blob", b"\x00\x01\xff"),
    ("amount", Decimal("-12.34")),
]


def make_typed_table(root: Path, kind: str, pa: Any) -> tuple[Path, str, Any]:
    """Write the same two-row schema through a local table format."""
    if kind not in {"iceberg", "delta", "hudi"}:
        raise ValueError(f"unsupported table fixture kind: {kind!r}")

    fixture = Fixture(root)
    names = ["id", *(name for name, _ in _COLUMNS)]
    types = [
        pa.int64(),
        pa.bool_(),
        pa.int32(),
        pa.int64(),
        pa.float32(),
        pa.float64(),
        pa.date32(),
        pa.timestamp("us", tz="UTC"),
        pa.timestamp("us"),
        pa.string(),
        pa.binary(),
        pa.decimal128(10, 2),
    ]
    values = [[1, 2], *[[value, None] for _, value in _COLUMNS]]
    fields = [pa.field("id", pa.int64(), nullable=False)]
    fields.extend(pa.field(name, dtype, nullable=True) for name, dtype in zip(names[1:], types[1:]))
    columns = [pa.array(column, type=dtype) for column, dtype in zip(values, types)]
    expected = pa.Table.from_arrays(columns, schema=pa.schema(fields))

    if kind == "iceberg":
        physical_fields = [id_field("id", types[0], False, 1)]
        physical_fields.extend(
            id_field(name, dtype, True, index + 2)
            for index, (name, dtype) in enumerate(zip(names[1:], types[1:]))
        )
        data_path = fixture.parquet("data.parquet", physical_fields, columns)
        logical_types = [
            "long",
            "boolean",
            "int",
            "long",
            "float",
            "double",
            "date",
            "timestamptz",
            "timestamp",
            "string",
            "binary",
            "decimal(10,2)",
        ]
        logical_fields = [
            {
                "id": index + 1,
                "name": name,
                "required": index == 0,
                "type": logical_types[index],
            }
            for index, name in enumerate(names)
        ]
        logical = iceberg_schema(logical_fields, 0)
        manifest = iceberg_manifest(
            fixture,
            "manifest.avro",
            logical,
            "data",
            [iceberg_entry(data_path, 2, 0, None, None)],
        )
        manifest_list = iceberg_list(fixture, "manifest-list.avro", [(manifest, 0, 1)])
        metadata = fixture.json(
            "metadata.json",
            {
                "format-version": 2,
                "location": uri(fixture.root),
                "current-snapshot-id": 1,
                "current-schema-id": 0,
                "schemas": [logical],
                "partition-specs": [{"spec-id": 0, "fields": []}],
                "snapshots": [
                    {
                        "snapshot-id": 1,
                        "schema-id": 0,
                        "sequence-number": 1,
                        "manifest-list": uri(manifest_list),
                    }
                ],
            },
        )
        return metadata, "1", expected

    if kind == "delta":
        delta_types = [
            "long",
            "boolean",
            "integer",
            "long",
            "float",
            "double",
            "date",
            "timestamp",
            "timestamp_ntz",
            "string",
            "binary",
            "decimal(10,2)",
        ]
        schema_fields = [
            delta_field(name, delta_types[index], index != 0)
            for index, name in enumerate(names)
        ]
        data_path = fixture.parquet("data.parquet", fields, columns)
        fixture.commit(
            0,
            [
                {
                    "protocol": {
                        "minReaderVersion": 3,
                        "minWriterVersion": 7,
                        "readerFeatures": ["timestampNtz"],
                        "writerFeatures": ["timestampNtz"],
                    }
                },
                delta_metadata(schema_fields, [], "none"),
                delta_add(data_path, "data.parquet", {}),
            ],
        )
        return fixture.root, "0", expected

    return _make_hudi_table(fixture, fields, columns, expected)


def _make_hudi_table(
    fixture: Fixture,
    fields: list[Any],
    columns: list[Any],
    expected: Any,
) -> tuple[Path, str, Any]:
    avro_fields = [
        {"name": "id", "type": "long"},
        {"name": "flag", "type": ["null", "boolean"], "default": None},
        {"name": "i32", "type": ["null", "int"], "default": None},
        {"name": "i64", "type": ["null", "long"], "default": None},
        {"name": "f32", "type": ["null", "float"], "default": None},
        {"name": "f64", "type": ["null", "double"], "default": None},
        {
            "name": "day",
            "type": ["null", {"type": "int", "logicalType": "date"}],
            "default": None,
        },
        {
            "name": "utc",
            "type": [
                "null",
                {"type": "long", "logicalType": "timestamp-micros"},
            ],
            "default": None,
        },
        {
            "name": "local",
            "type": [
                "null",
                {"type": "long", "logicalType": "local-timestamp-micros"},
            ],
            "default": None,
        },
        {"name": "text", "type": ["null", "string"], "default": None},
        {"name": "blob", "type": ["null", "bytes"], "default": None},
        {
            "name": "amount",
            "type": [
                "null",
                {
                    "type": "bytes",
                    "logicalType": "decimal",
                    "precision": 10,
                    "scale": 2,
                },
            ],
            "default": None,
        },
    ]
    schema = {"type": "record", "name": "Row", "fields": avro_fields}
    config_schema = json.dumps(schema, separators=(",", ":"))
    config = fixture.root / ".hoodie" / "hoodie.properties"
    config.parent.mkdir(parents=True, exist_ok=True)
    config.write_text(
        "hoodie.table.name=fixture\n"
        "hoodie.table.type=COPY_ON_WRITE\n"
        "hoodie.table.version=9\n"
        "hoodie.timeline.layout.version=2\n"
        f"hoodie.table.create.schema={config_schema}\n",
        encoding="utf-8",
    )

    file_name = "a_0-0-0_20261101000100.parquet"
    fixture.parquet(file_name, fields, columns)
    commit_schema = {
        "type": "record",
        "name": "HoodieCommitMetadata",
        "fields": [
            {
                "name": "partitionToWriteStats",
                "type": [
                    "null",
                    {
                        "type": "map",
                        "values": {
                            "type": "array",
                            "items": {
                                "type": "record",
                                "name": "HoodieWriteStat",
                                "fields": [
                                    {
                                        "name": "fileId",
                                        "type": ["null", "string"],
                                        "default": None,
                                    },
                                    {
                                        "name": "path",
                                        "type": ["null", "string"],
                                        "default": None,
                                    },
                                ],
                            },
                        },
                    },
                ],
                "default": None,
            },
            {"name": "compacted", "type": "boolean", "default": False},
            {
                "name": "extraMetadata",
                "type": ["null", {"type": "map", "values": ["null", "string"]}],
                "default": None,
            },
            {"name": "version", "type": "int", "default": 1},
            {"name": "operationType", "type": ["null", "string"], "default": None},
        ],
    }
    fixture.avro(
        ".hoodie/timeline/20261101000100_20261101000200.commit",
        commit_schema,
        [
            {
                "partitionToWriteStats": {
                    "": [{"fileId": "a", "path": file_name}]
                },
                "compacted": False,
                "extraMetadata": {"schema": config_schema},
                "version": 1,
                "operationType": "UPSERT",
            }
        ],
    )
    return fixture.root, "completion:20261101000200", expected
