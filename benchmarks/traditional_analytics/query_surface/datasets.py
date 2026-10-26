# SPDX-License-Identifier: Apache-2.0
"""Bounded deterministic fixture generation; no query engine supplies values.

Arrow is used only to persist typed source batches. Native preparation and
independent readback are separate supervised public ShardLoom operations.
"""
from __future__ import annotations

from array import array
from collections import Counter
from dataclasses import asdict, dataclass
from decimal import Decimal
import hashlib
import json
import math
from pathlib import Path
import random
import sys
from typing import Callable, Iterator

from shardloom._result_schema import ResultType, _arrow_type
from .semantics import Schema, schema_record


GENERATOR_VERSION = "independent-columns-v1"
MASK = (1 << 64) - 1
LAYOUTS = ("sorted", "shuffled", "clustered", "partitioned", "fragmented")
PROFILES = ("balanced", "null_heavy", "skewed", "high_cardinality")


@dataclass(frozen=True)
class DatasetSpec:
    seed: int = 20261026
    fact_rows: int = 1_000_000
    entity_rows: int = 100_000
    product_rows: int = 10_000
    relationship_rows: int = 200_000
    calendar_rows: int = 731
    profile: str = "balanced"

    def __post_init__(self):
        for name in ("seed", "fact_rows", "entity_rows", "product_rows", "relationship_rows", "calendar_rows"):
            value = getattr(self, name)
            if type(value) is not int or value < 0 or value > MASK:
                raise ValueError(f"{name} must be a nonnegative 64-bit integer")
        if min(self.entity_rows, self.product_rows, self.calendar_rows) < 1:
            raise ValueError("supporting dimensions must contain at least one row")
        if self.profile not in PROFILES:
            raise ValueError("unknown distribution profile")

    def counts(self):
        return {"fact": self.fact_rows, "entities": self.entity_rows, "products": self.product_rows,
                "relationships": self.relationship_rows, "calendar": self.calendar_rows}

    @property
    def identity(self):
        return hashlib.sha256(json.dumps({"generator": GENERATOR_VERSION, **asdict(self)},
                                         sort_keys=True).encode()).hexdigest()


def _field(name, dtype="int64", nullable=False, parameters=()):
    return name, ResultType(dtype, nullable, parameters)


FACT_SCHEMA: Schema = (
    _field("row_id"), _field("entity_id", nullable=True), _field("product_id", nullable=True),
    _field("group_key"), _field("medium_key"), _field("high_key"), _field("skew_key"),
    _field("signed_value"), _field("small_value", "int16"), _field("unsigned_value", "uint64"),
    _field("amount", "decimal128", True, (18, 2)), _field("metric", "float64"),
    _field("optional_value", nullable=True), _field("sparse_value", nullable=True),
    _field("mostly_null", nullable=True), _field("all_null", nullable=True),
    _field("nullable_no_null", nullable=True), _field("flag", "bool"), _field("nullable_flag", "bool", True),
    _field("category", "utf8"), _field("short_text", "utf8"), _field("long_text", "utf8"),
    _field("unicode_text", "utf8"), _field("unique_text", "utf8"), _field("optional_text", "utf8", True),
    _field("event_date", "date32"), _field("event_time", "timestamp_micros"),
    _field("selection_key"), _field("independent_key"), _field("correlated_key"),
    _field("anti_correlated_key"), _field("constant_value"), _field("run_value"),
    _field("monotone_value"), _field("entropy_value", "uint64"),
    _field("items", "list", True, (ResultType("int64", True),)),
    _field("detail", "struct", True, (("score", ResultType("int64", True)),
                                       ("label", ResultType("utf8", False)))),
    _field("json_text", "utf8"),
)
SCHEMAS = {
    "fact": FACT_SCHEMA,
    "entities": (_field("entity_id"), _field("region", "utf8"), _field("active", "bool"),
                 _field("risk", "float64"), _field("entity_label", "utf8")),
    "products": (_field("product_id"), _field("product_category", "utf8"),
                 _field("price", "decimal128", False, (18, 2))),
    "relationships": (_field("relationship_id"), _field("entity_id", nullable=True),
                      _field("related_id", nullable=True), _field("weight")),
    "calendar": (_field("day", "date32"), _field("day_index"), _field("week_index")),
}
LAYOUT_KEYS = {table: ("group_key" if table == "fact" else "day_index" if table == "calendar" else schema[0][0])
               for table, schema in SCHEMAS.items()}


def stream_seed(seed: int, table: str, column: str) -> int:
    raw = f"{GENERATOR_VERSION}:{seed}:{table}:{column}".encode()
    return int.from_bytes(hashlib.blake2b(raw, digest_size=8).digest(), "little")


def sample(index: int, seed: int) -> int:
    """SplitMix64 mixer: independent named streams, stable random access."""
    value = (seed + (index + 1) * 0x9E3779B97F4A7C15) & MASK
    value = ((value ^ (value >> 30)) * 0xBF58476D1CE4E5B9) & MASK
    value = ((value ^ (value >> 27)) * 0x94D049BB133111EB) & MASK
    return value ^ (value >> 31)


class RowGenerator:
    def __init__(self, spec: DatasetSpec, table: str):
        if table not in SCHEMAS:
            raise ValueError("unknown fixture table")
        self.spec, self.table = spec, table
        self.seeds = {name: stream_seed(spec.seed, table, name) for name, _ in SCHEMAS[table]}
        self.null_seeds = {name: stream_seed(spec.seed, table, name + ":validity") for name in self.seeds}

    def value(self, index: int, name: str):
        spec = self.spec
        value = sample(index, self.seeds[name])
        null = sample(index, self.null_seeds[name]) % 1000
        if self.table == "entities":
            return {"entity_id": lambda: index, "region": lambda: f"region-{value % 8}",
                    "active": lambda: value % 5 != 0, "risk": lambda: (value % 10001) / 10000,
                    "entity_label": lambda: f"entity-{index:08d}"}[name]()
        if self.table == "products":
            return {"product_id": lambda: index, "product_category": lambda: f"product-{value % 32}",
                    "price": lambda: Decimal((0, tuple(map(int, str(value % 100000))), -2))}[name]()
        if self.table == "relationships":
            return {"relationship_id": lambda: index,
                    "entity_id": lambda: None if null < 50 else value % (spec.entity_rows + max(1, spec.entity_rows // 20)),
                    "related_id": lambda: None if null < 20 else value % spec.entity_rows,
                    "weight": lambda: value % 101 - 50}[name]()
        if self.table == "calendar":
            return {"day": lambda: index - 365, "day_index": lambda: index,
                    "week_index": lambda: index // 7}[name]()
        heavy = spec.profile == "null_heavy"
        if name in {"entity_id", "product_id"}:
            count = spec.entity_rows if name == "entity_id" else spec.product_rows
            return None if null < (300 if heavy else 10) else value % (count + max(1, count // 100))
        if name in {"optional_value", "sparse_value", "mostly_null", "all_null", "nullable_no_null"}:
            threshold = {"optional_value": 500 if heavy else 150, "sparse_value": 10,
                         "mostly_null": 950, "all_null": 1000, "nullable_no_null": 0}[name]
            return None if null < threshold else value % 2001 - 1000
        if name == "amount":
            coefficient = value % 200001 - 100000
            return None if null < 100 else Decimal((int(coefficient < 0), tuple(map(int, str(abs(coefficient)))), -2))
        if name == "row_id":
            return index
        if name == "group_key":
            return value % (max(1, spec.fact_rows // 2) if spec.profile == "high_cardinality" else 10)
        if name == "medium_key":
            return value % 4096
        if name == "high_key":
            return value % max(1, spec.fact_rows)
        if name == "skew_key":
            return 0 if value % 100 < (95 if spec.profile == "skewed" else 60) else (value >> 32) % 999 + 1
        if name == "signed_value":
            return value % 2001 - 1000
        if name == "small_value":
            return value % 201 - 100
        if name == "unsigned_value":
            return value % 100001
        if name == "metric":
            return (value % 2_000_001 - 1_000_000) / 1024.0
        if name in {"flag", "nullable_flag"}:
            return None if name == "nullable_flag" and null < 250 else bool(value & 1)
        if name == "category":
            return f"category-{value % 10}"
        if name == "short_text":
            return ("", " a ", "shared-alpha", "shared-beta", "xyz", "0")[value % 6]
        if name == "long_text":
            return f"prefix/{value % 1000:04d}/" + ("abcdefgh" * (8 + value % 24))
        if name == "unicode_text":
            return ("", "café", "東京", "🙂", "e\u0301", "a\x00b")[value % 6]
        if name == "unique_text":
            return f"event-{index:010d}-{value:016x}"
        if name == "optional_text":
            return None if null < 350 else ("", " x ", "shared", "café")[value % 4]
        if name == "event_date":
            return value % spec.calendar_rows - 365
        if name == "event_time":
            return (value % (spec.calendar_rows * 86400) - 365 * 86400) * 1_000_000 + (value >> 32) % 1_000_000
        if name in {"selection_key", "independent_key"}:
            return value % 1_000_000
        if name in {"correlated_key", "anti_correlated_key"}:
            base = sample(index, self.seeds["selection_key"]) % 1_000_000
            return base if name == "correlated_key" else 999_999 - base
        if name == "constant_value":
            return 7
        if name == "run_value":
            return index // 4096
        if name == "monotone_value":
            return index * 3
        if name == "entropy_value":
            return value
        if name == "items":
            return None if null < 100 else [None if j == 1 else (value >> (j * 8)) % 100 for j in range(value % 4)]
        if name == "detail":
            return None if null < 100 else {"score": None if null < 250 else value % 100,
                                          "label": f"label-{(value >> 32) % 7}"}
        if name == "json_text":
            return json.dumps({"score": value % 100, "kind": f"k{value % 3}"}, separators=(",", ":"))
        raise ValueError(f"unknown fixture column: {name}")

    def row(self, index: int, columns=None) -> dict:
        if not 0 <= index < self.spec.counts()[self.table]:
            raise IndexError("fixture row is outside its table")
        return {name: self.value(index, name) for name in (self.seeds if columns is None else columns)}

    def rows(self, columns=None) -> Iterator[dict]:
        return (self.row(i, columns) for i in range(self.spec.counts()[self.table]))


def canonical(value):
    """Lossless fixture identity; no process-randomized Python hash values."""
    if isinstance(value, Decimal):
        return {"decimal": str(value)}
    if isinstance(value, dict):
        return {key: canonical(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [canonical(item) for item in value]
    if isinstance(value, float):
        return {"float64": value.hex()}
    return value


def encoded_row(row):
    return json.dumps(canonical(row), sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


class ColumnObservation:
    """Exact NULLs/distincts until 4096 values, then labelled HLL estimates."""
    def __init__(self):
        self.rows = self.nulls = 0
        self.exact = set()
        self.registers = bytearray(4096)

    def add(self, value):
        self.rows += 1
        if value is None:
            self.nulls += 1
            return
        token = encoded_row(value)
        if self.exact is not None:
            self.exact.add(token)
            if len(self.exact) > 4096:
                self.exact = None
        digest = int.from_bytes(hashlib.blake2b(token, digest_size=8).digest(), "big")
        index, tail = digest >> 52, digest & ((1 << 52) - 1)
        self.registers[index] = max(self.registers[index], 53 - tail.bit_length())

    def record(self):
        result = {"rows": self.rows, "nulls": self.nulls,
                  "null_rate": self.nulls / self.rows if self.rows else None}
        if self.exact is not None:
            return {**result, "distinct_nonnull": len(self.exact), "distinct_method": "exact_value_set"}
        estimate = (0.7213 / (1 + 1.079 / 4096)) * 4096**2 / sum(2.0 ** -r for r in self.registers)
        zeros = self.registers.count(0)
        if zeros and estimate <= 2.5 * 4096:
            estimate = 4096 * math.log(4096 / zeros)
        return {**result, "distinct_nonnull": estimate, "distinct_method": "hll_4096_registers",
                "distinct_relative_standard_error": 1.04 / math.sqrt(4096)}


def layout_indices(spec: DatasetSpec, table: str, layout: str) -> array:
    if layout not in LAYOUTS:
        raise ValueError("unknown physical layout")
    indices = array("Q", range(spec.counts()[table]))
    if layout in {"shuffled", "fragmented"}:
        random.Random(stream_seed(spec.seed, table, "layout")).shuffle(indices)
    elif layout in {"clustered", "partitioned"}:
        generator = RowGenerator(spec, table)
        key = LAYOUT_KEYS[table]
        indices = array("Q", sorted(indices, key=lambda i: (
            generator.value(i, key) % 8 if layout == "partitioned" else 0,
            generator.value(i, key), i)))
    return indices


def write_dataset(root: Path, spec: DatasetSpec, layout: str, *, guard: Callable,
                  batch_rows: int = 8192, progress: Callable | None = None) -> dict:
    """Write one fresh layout under the caller's active storage/process guard."""
    import pyarrow as pa
    from local_uat_storage import require_local_path

    root = require_local_path(root, Path.home(), sys.platform)
    if type(batch_rows) is not int or not 1 <= batch_rows <= 65536:
        raise ValueError("fixture batches must contain 1..65536 rows")
    if layout not in LAYOUTS:
        raise ValueError("unknown physical layout")
    guard()
    root.mkdir(parents=True, exist_ok=False)
    manifest = {"schema_version": 1, "generator": GENERATOR_VERSION, "spec": asdict(spec),
                "logical_identity": spec.identity, "layout": layout, "tables": {},
                "intentional_correlations": ["row_id/run_value/monotone_value", "selection_key/correlated_key/anti_correlated_key"],
                "physical_encoding_observation": "Arrow IPC source buffers; Vortex encodings require separate preparation receipt"}
    for table, count in spec.counts().items():
        generator, indices = RowGenerator(spec, table), layout_indices(spec, table, layout)
        schema = pa.schema([pa.field(name, _arrow_type(dtype, pa), dtype.nullable) for name, dtype in SCHEMAS[table]])
        observations = {name: ColumnObservation() for name, _ in SCHEMAS[table]}
        selection = Counter({str(cutoff): 0 for cutoff in (0, 100, 10000, 100000, 500000, 1000000)})
        fanout = Counter()
        digest, unordered_sum, unordered_square = hashlib.sha256(), 0, 0
        files, pending, start = [], [], 0
        file_rows = min(batch_rows, 257) if layout == "fragmented" else max(1, count)
        partition_column = LAYOUT_KEYS[table]
        previous_partition = None
        writer = handle = None
        try:
            for position in range(count + 1):
                if progress is not None and (position % 65536 == 0 or position == count):
                    progress({"stage": "fixture_generation", "table": table, "generated_rows": position, "total_rows": count})
                partition = (generator.value(indices[position], partition_column) % 8
                             if layout == "partitioned" and position < count else None)
                boundary = position == count or (position and position % file_rows == 0)
                if layout == "partitioned" and position and partition != previous_partition:
                    boundary = True
                if pending and (len(pending) == batch_rows or boundary):
                    guard()
                    writer.write_batch(pa.RecordBatch.from_pylist(pending, schema=schema))
                    pending.clear()
                if boundary and writer is not None:
                    writer.close()
                    handle.close()
                    files[-1].update(rows=position - start)
                    writer = handle = None
                if position == count:
                    break
                if writer is None:
                    path = root / f"{table}-{len(files):05d}.arrow"
                    handle = path.open("xb")
                    writer = pa.ipc.new_file(handle, schema)
                    files.append({"path": path.name, "start_row": position})
                    if layout == "partitioned":
                        files[-1].update(partition_column=partition_column,
                                         partition_modulus=8, partition_value=partition)
                    start = position
                previous_partition = partition
                row = generator.row(indices[position])
                pending.append(row)
                token = encoded_row(row)
                digest.update(len(token).to_bytes(8, "little") + token)
                hashed = int.from_bytes(hashlib.sha256(token).digest(), "big")
                unordered_sum = (unordered_sum + hashed) % (1 << 256)
                unordered_square = (unordered_square + hashed * hashed) % (1 << 256)
                for name, value in row.items():
                    observations[name].add(value)
                if table == "fact":
                    for cutoff in selection:
                        selection[cutoff] += row["selection_key"] < int(cutoff)
                if table in {"fact", "relationships"}:
                    fanout[row["entity_id"]] += 1
            if not files:
                path = root / f"{table}-00000.arrow"
                with path.open("xb") as empty:
                    with pa.ipc.new_file(empty, schema):
                        pass
                files.append({"path": path.name, "start_row": 0, "rows": 0})
        finally:
            if writer is not None:
                writer.close()
            if handle is not None:
                handle.close()
        for item in files:
            path = root / item["path"]
            item.update(bytes=path.stat().st_size, sha256=file_hash(path))
        valid_counts = [n for key, n in fanout.items() if key is not None and 0 <= key < spec.entity_rows]
        manifest["tables"][table] = {
            "rows": count, "schema": schema_record(SCHEMAS[table]), "files": files,
            "column_seeds": generator.seeds, "validity_seeds": generator.null_seeds,
            "observed_columns": {name: obs.record() for name, obs in observations.items()},
            "ordered_records_sha256": digest.hexdigest(),
            "multiset_sha256_sum": f"{unordered_sum:064x}", "multiset_sha256_square_sum": f"{unordered_square:064x}",
            "identity_method": "row-index permutation plus two 256-bit commutative fingerprints; not collision-free proof",
            "selection_key_less_than_counts": dict(selection) if table == "fact" else None,
            "entity_key_observation": ({"null_keys": fanout.get(None, 0),
                "absent_key_rows": sum(n for key, n in fanout.items() if key is not None and key >= spec.entity_rows),
                "matched_distinct_keys": len(valid_counts), "maximum_matched_fanout": max(valid_counts, default=0),
                "matched_rows": sum(valid_counts),
                "fanout_histogram": dict(sorted(Counter(valid_counts).items()))} if fanout else None),
        }
    guard()
    (root / "dataset.json").write_text(json.dumps(manifest, indent=2, allow_nan=False) + "\n")
    return manifest


def file_hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()
