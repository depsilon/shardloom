# SPDX-License-Identifier: Apache-2.0
"""Small wire-format fixtures shared by public native table integration tests.

Avro framing is authored independently of the production metadata decoder;
Parquet data files use the existing PyArrow test dependency.
"""

from __future__ import annotations

import json as _json
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


class Fixture:
    """Own a fixture directory and create table metadata/data files within it."""

    def __init__(self, root: Path):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    def json(self, name: str, value: Any) -> Path:
        path = self.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(_json.dumps(value, separators=(",", ":")), encoding="utf-8")
        return path

    def commit(self, version: int, actions: Sequence[Any]) -> Path:
        path = self.root / "_delta_log" / f"{version:020}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            "".join(_json.dumps(action, separators=(",", ":")) + "\n" for action in actions),
            encoding="utf-8",
        )
        return path

    def parquet(self, name: str, fields: Sequence[Any], columns: Sequence[Any]) -> Path:
        import pyarrow as pa
        import pyarrow.parquet as parquet

        path = self.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        table = pa.Table.from_arrays(list(columns), schema=pa.schema(list(fields)))
        parquet.write_table(table, path, row_group_size=2)
        return path

    def avro(
        self,
        name: str,
        schema: Any,
        rows: Sequence[Any],
        properties: Iterable[tuple[str, str]] = (),
    ) -> Path:
        output = bytearray(b"Obj\x01")
        metadata = [("avro.schema", _json.dumps(schema, separators=(",", ":"))),
                    ("avro.codec", "null"), *properties]
        _avro_long(len(metadata), output)
        for key, value in metadata:
            _avro_bytes(key.encode("utf-8"), output)
            _avro_bytes(value.encode("utf-8"), output)
        _avro_long(0, output)
        sync = bytes([0x5A]) * 16
        output.extend(sync)
        if rows:
            block = bytearray()
            for row in rows:
                _avro_datum(schema, row, block)
            _avro_long(len(rows), output)
            _avro_bytes(block, output)
            output.extend(sync)
        path = self.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(output)
        return path


def id_field(name: str, dtype: Any, nullable: bool, id: int) -> Any:
    import pyarrow as pa

    return pa.field(name, dtype, nullable=nullable, metadata={"PARQUET:field_id": str(id)})


def uri(path: Path) -> str:
    return f"file://{path}"


def delta_metadata(fields: Any, partitions: Sequence[str], mode: str) -> dict[str, Any]:
    return {
        "metaData": {
            "id": "fixture-table",
            "format": {"provider": "parquet", "options": {}},
            "schemaString": _json.dumps(
                {"type": "struct", "fields": fields}, separators=(",", ":")
            ),
            "partitionColumns": list(partitions),
            "configuration": {"delta.columnMapping.mode": mode},
        }
    }


def delta_field(name: str, kind: str, nullable: bool, id: int | None = None) -> dict[str, Any]:
    metadata: dict[str, Any] = {}
    if id is not None:
        metadata = {
            "delta.columnMapping.id": id,
            "delta.columnMapping.physicalName": f"col-{id}",
        }
    return {"name": name, "type": kind, "nullable": nullable, "metadata": metadata}


def delta_add(path: Path, name: str, partitions: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "add": {
            "path": name,
            "partitionValues": dict(partitions),
            "size": path.stat().st_size,
            "modificationTime": 0,
            "dataChange": True,
        }
    }


def iceberg_schema(fields: Any, id: int) -> dict[str, Any]:
    return {"type": "struct", "schema-id": id, "fields": fields}


def iceberg_entry(
    path: Path,
    rows: int,
    content: int,
    sequence: int | None,
    equality: Any,
) -> dict[str, Any]:
    return {
        "status": 1,
        "snapshot_id": None,
        "sequence_number": sequence,
        "file_sequence_number": None,
        "data_file": {
            "content": content,
            "file_path": uri(path),
            "file_format": "PARQUET",
            "partition": {},
            "record_count": rows,
            "file_size_in_bytes": path.stat().st_size,
            "equality_ids": equality,
        },
    }


def iceberg_manifest(
    fixture: Fixture, name: str, logical: Any, content: str, rows: Sequence[Any]
) -> Path:
    return iceberg_manifest_partition(
        fixture,
        name,
        logical,
        content,
        rows,
        0,
        [],
        {"type": "record", "name": "r102", "fields": []},
    )


def iceberg_manifest_partition(
    fixture: Fixture,
    name: str,
    logical: Any,
    content: str,
    rows: Sequence[Any],
    spec_id: int,
    spec: Sequence[Any],
    partition: Any,
) -> Path:
    schema = {
        "type": "record",
        "name": "manifest_entry",
        "fields": [
            {"name": "status", "type": "int", "field-id": 0},
            {"name": "snapshot_id", "type": ["null", "long"], "field-id": 1, "default": None},
            {"name": "sequence_number", "type": ["null", "long"], "field-id": 3, "default": None},
            {"name": "file_sequence_number", "type": ["null", "long"], "field-id": 4, "default": None},
            {
                "name": "data_file",
                "field-id": 2,
                "type": {
                    "type": "record",
                    "name": "data_file",
                    "fields": [
                        {"name": "content", "type": "int", "field-id": 134},
                        {"name": "file_path", "type": "string", "field-id": 100},
                        {"name": "file_format", "type": "string", "field-id": 101},
                        {"name": "partition", "field-id": 102, "type": partition},
                        {"name": "record_count", "type": "long", "field-id": 103},
                        {"name": "file_size_in_bytes", "type": "long", "field-id": 104},
                        {
                            "name": "equality_ids",
                            "field-id": 135,
                            "type": [
                                "null",
                                {"type": "array", "items": "int", "element-id": 136},
                            ],
                            "default": None,
                        },
                    ],
                },
            },
        ],
    }
    return fixture.avro(
        name,
        schema,
        rows,
        [
            ("schema", _json.dumps(logical, separators=(",", ":"))),
            ("schema-id", "0"),
            ("partition-spec", _json.dumps(spec, separators=(",", ":"))),
            ("partition-spec-id", str(spec_id)),
            ("format-version", "2"),
            ("content", content),
        ],
    )


def iceberg_list(
    fixture: Fixture,
    name: str,
    manifests: Sequence[tuple[Path, int, int]],
) -> Path:
    return iceberg_list_specs(
        fixture, name, [(path, content, sequence, 0) for path, content, sequence in manifests]
    )


def iceberg_list_specs(
    fixture: Fixture,
    name: str,
    manifests: Sequence[tuple[Path, int, int, int]],
) -> Path:
    schema = {
        "type": "record",
        "name": "manifest_file",
        "fields": [
            {"name": "manifest_path", "type": "string", "field-id": 500},
            {"name": "manifest_length", "type": "long", "field-id": 501},
            {"name": "partition_spec_id", "type": "int", "field-id": 502},
            {"name": "content", "type": "int", "field-id": 517},
            {"name": "sequence_number", "type": "long", "field-id": 515},
            {"name": "min_sequence_number", "type": "long", "field-id": 516},
            {"name": "added_snapshot_id", "type": "long", "field-id": 503},
            {"name": "added_files_count", "type": "int", "field-id": 504},
            {"name": "existing_files_count", "type": "int", "field-id": 505},
            {"name": "deleted_files_count", "type": "int", "field-id": 506},
            {"name": "added_rows_count", "type": "long", "field-id": 512},
            {"name": "existing_rows_count", "type": "long", "field-id": 513},
            {"name": "deleted_rows_count", "type": "long", "field-id": 514},
        ],
    }
    rows = [
        {
            "manifest_path": uri(path),
            "manifest_length": path.stat().st_size,
            "partition_spec_id": spec_id,
            "content": content,
            "sequence_number": sequence,
            "min_sequence_number": sequence,
            "added_snapshot_id": sequence,
            "added_files_count": 1,
            "existing_files_count": 0,
            "deleted_files_count": 0,
            "added_rows_count": 0,
            "existing_rows_count": 0,
            "deleted_rows_count": 0,
        }
        for path, content, sequence, spec_id in manifests
    ]
    return fixture.avro(name, schema, rows)


def _avro_long(value: int, output: bytearray) -> None:
    encoded = (value << 1) ^ (value >> 63)
    while encoded >= 128:
        output.append((encoded & 0x7F) | 0x80)
        encoded >>= 7
    output.append(encoded)


def _avro_bytes(value: bytes, output: bytearray) -> None:
    _avro_long(len(value), output)
    output.extend(value)


def _avro_datum(schema: Any, value: Any, output: bytearray) -> None:
    if isinstance(schema, list):
        index = next(
            (i for i, branch in enumerate(schema) if (branch == "null") == (value is None)),
            None,
        )
        if index is None:
            raise ValueError(f"Avro union has no branch for value {value!r}")
        _avro_long(index, output)
        _avro_datum(schema[index], value, output)
        return

    kind = schema if isinstance(schema, str) else schema.get("type")
    if kind == "null":
        if value is not None:
            raise ValueError(f"Avro null datum cannot encode {value!r}")
    elif kind == "boolean":
        if not isinstance(value, bool):
            raise ValueError(f"Avro boolean datum cannot encode {value!r}")
        output.append(int(value))
    elif kind in ("int", "long"):
        if isinstance(value, bool) or not isinstance(value, int):
            raise ValueError(f"Avro {kind} datum cannot encode {value!r}")
        _avro_long(value, output)
    elif kind == "string":
        if not isinstance(value, str):
            raise ValueError(f"Avro string datum cannot encode {value!r}")
        _avro_bytes(value.encode("utf-8"), output)
    elif kind == "record":
        for field in schema["fields"]:
            _avro_datum(field["type"], value[field["name"]], output)
    elif kind == "array":
        if not isinstance(value, list):
            raise ValueError(f"Avro array datum cannot encode {value!r}")
        if value:
            _avro_long(len(value), output)
            for item in value:
                _avro_datum(schema["items"], item, output)
        _avro_long(0, output)
    elif kind == "map":
        if not isinstance(value, Mapping):
            raise ValueError(f"Avro map datum cannot encode {value!r}")
        if value:
            _avro_long(len(value), output)
            for key, item in value.items():
                _avro_bytes(str(key).encode("utf-8"), output)
                _avro_datum(schema["values"], item, output)
        _avro_long(0, output)
    else:
        raise ValueError(f"test fixture Avro type not supported: {kind!r}")
