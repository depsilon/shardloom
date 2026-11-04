# SPDX-License-Identifier: Apache-2.0
"""Materialize a narrow, independent SQLite reference input from frozen IPC."""

from __future__ import annotations

from datetime import date
import json
import os
from pathlib import Path
import sqlite3
import sys

from local_uat_storage import require_local_path
from query_surface.datasets import SCHEMAS, file_hash


PRIMARY_KEYS = {
    "fact": "row_id",
    "entities": "entity_id",
    "products": "product_id",
    "relationships": "relationship_id",
    "calendar": "day_index",
}
INDEXES = {
    "fact": ("selection_key", "entity_id", "product_id"),
    "relationships": ("entity_id",),
    "entities": ("active",),
}
_TYPE_MAP = {
    "int8": "INTEGER",
    "int16": "INTEGER",
    "int32": "INTEGER",
    "int64": "INTEGER",
    "uint8": "INTEGER",
    "uint16": "INTEGER",
    "uint32": "INTEGER",
    "uint64": "INTEGER",
    "bool": "INTEGER",
    "date32": "INTEGER",
    "timestamp_micros": "INTEGER",
    "decimal128": "TEXT",
    "float32": "REAL",
    "float64": "REAL",
    "utf8": "TEXT",
    "list": "TEXT",
    "struct": "TEXT",
}
_EPOCH = date(1970, 1, 1)


def _convert(value, dtype: str):
    if value is None:
        return None
    if dtype == "bool":
        return int(bool(value))
    if dtype == "date32":
        return (value - _EPOCH).days if isinstance(value, date) else int(value)
    if dtype == "timestamp_micros":
        # Arrow timestamp scalar.value is the exact signed integer in its declared unit.
        if hasattr(value, "value"):
            return int(value.value)
        delta = value - value.__class__(1970, 1, 1)
        return delta.days * 86_400_000_000 + delta.seconds * 1_000_000 + delta.microseconds
    if dtype == "uint64":
        result = int(value)
        if result > (1 << 63) - 1:
            raise ValueError("uint64 value exceeds SQLite signed INTEGER range")
        return result
    if dtype.startswith("uint") or dtype.startswith("int"):
        return int(value)
    if dtype == "decimal128":
        return str(value)
    if dtype in {"list", "struct"}:
        return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)
    if dtype in {"float32", "float64"}:
        return float(value)
    if dtype == "utf8":
        return str(value)
    raise ValueError(f"unsupported SQLite reference type: {dtype}")


def _q(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def build_database(path: Path, dataset: Path, manifest: dict, columns: dict[str, set[str]], guard):
    """Build a fresh, narrow SQLite oracle; failed builds remain visibly partial."""
    import pyarrow as pa

    path = require_local_path(Path(path), Path.home(), sys.platform)
    dataset = require_local_path(Path(dataset), Path.home(), sys.platform)
    if path.exists() or path.with_name(path.name + ".partial").exists():
        raise FileExistsError("SQLite output or partial path already exists")
    tables = manifest.get("tables")
    if not isinstance(tables, dict) or not isinstance(columns, dict) or set(columns) - set(tables):
        raise ValueError("declared SQLite tables do not match the input manifest")
    selection = {}
    for table, requested in columns.items():
        if table not in SCHEMAS or table not in PRIMARY_KEYS or not isinstance(requested, set):
            raise ValueError(f"unknown table or invalid columns: {table}")
        declared = dict(SCHEMAS[table])
        if requested - set(declared):
            raise ValueError(f"unknown {table} column: {sorted(requested - set(declared))[0]}")
        names = [name for name, _ in SCHEMAS[table] if name in requested or name == PRIMARY_KEYS[table]]
        selection[table] = names

    partial = path.with_name(path.name + ".partial")
    guard.check()
    partial.touch(exist_ok=False)
    connection = sqlite3.connect(str(partial))
    counts = {}
    try:
        connection.execute("PRAGMA journal_mode=DELETE")
        for table, names in selection.items():
            cols = dict(SCHEMAS[table])
            definitions = [f"{_q(name)} {_TYPE_MAP[cols[name].name]}" for name in names]
            definitions.append(f"PRIMARY KEY ({_q(PRIMARY_KEYS[table])})")
            connection.execute(f"CREATE TABLE {_q(table)} ({', '.join(definitions)})")
            insert = f"INSERT INTO {_q(table)} ({', '.join(map(_q, names))}) VALUES ({','.join('?' for _ in names)})"
            count = 0
            item = tables[table]
            files = item.get("files")
            if not isinstance(files, list):
                raise ValueError(f"missing file inventory for {table}")
            for part in files:
                file_count = 0
                source = (dataset / part["path"]).resolve()
                if not source.is_relative_to(dataset) or not source.is_file():
                    raise ValueError("IPC input escapes dataset or is not a regular file")
                if source.stat().st_size != part.get("bytes") or file_hash(source) != part.get("sha256"):
                    raise ValueError(f"content-addressed IPC input changed: {part['path']}")
                with pa.OSFile(str(source), "rb") as mapped:
                    ipc = pa.ipc.open_file(mapped)
                    actual_names = ipc.schema.names
                    if any(name not in actual_names for name in names):
                        raise ValueError(f"IPC schema is missing selected {table} columns")
                    for batch_index in range(ipc.num_record_batches):
                        guard.check()
                        batch = ipc.get_batch(batch_index)
                        positions = [batch.schema.get_field_index(name) for name in names]
                        arrays = [batch.column(position) for position in positions]
                        types = [cols[name].name for name in names]
                        rows = []
                        for row_index in range(batch.num_rows):
                            values = []
                            for array, dtype in zip(arrays, types):
                                scalar = array[row_index]
                                value = scalar.as_py()
                                if dtype == "timestamp_micros" and value is not None:
                                    value = scalar
                                values.append(_convert(value, dtype))
                            if values[names.index(PRIMARY_KEYS[table])] is None:
                                raise ValueError(f"NULL primary key in {table}")
                            rows.append(tuple(values))
                        try:
                            connection.executemany(insert, rows)
                        except sqlite3.IntegrityError as exc:
                            raise ValueError(f"duplicate or invalid primary key in {table}") from exc
                        count += batch.num_rows
                        file_count += batch.num_rows
                        connection.commit()
                if file_count != part.get("rows"):
                    raise ValueError(f"IPC file row count does not match manifest: {part['path']}")
                if count > item.get("rows", -1):
                    raise ValueError(f"IPC rows exceed manifest count for {table}")
            if count != item.get("rows"):
                raise ValueError(f"IPC row count does not match manifest for {table}")
            sql_count = connection.execute(f"SELECT COUNT(*) FROM {_q(table)}").fetchone()[0]
            if sql_count != count:
                raise ValueError(f"SQLite row count does not match IPC for {table}")
            counts[table] = count
        connection.commit()
        for table, indexes in INDEXES.items():
            if table not in selection:
                continue
            for column in indexes:
                if column not in selection[table]:
                    continue
                guard.check()
                connection.execute(
                    f"CREATE INDEX {_q('idx_' + table + '_' + column)} ON {_q(table)} ({_q(column)})"
                )
                connection.commit()
                guard.check()
        connection.close()
        os.link(partial, path)
        partial.unlink()
    except BaseException:
        connection.close()
        raise

    return {
        "logical_identity": manifest.get("logical_identity"),
        "original_input_ids": [
            {"path": part["path"], "sha256": part["sha256"]}
            for table in selection
            for part in tables[table]["files"]
        ],
        "stored_columns": {
            table: {name: _TYPE_MAP[dict(SCHEMAS[table])[name].name] for name in names}
            for table, names in selection.items()
        },
        "row_counts": counts,
        "database_sha256": file_hash(path),
    }
