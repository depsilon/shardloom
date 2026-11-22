# SPDX-License-Identifier: Apache-2.0
"""Optional AI data preparation, dataset delivery, and retrieval module.

This submodule connects ShardLoom's native SQL/DataFrame execution, native
Arrow C stream export, ASOF point-in-time joins, and vector expressions to
CPU model-ready batches, streaming/indexed dataset loaders, persisted fitted
feature preparation, exact filtered retrieval, lexical/hybrid search, and
evaluated compact int8 vector representations.

Importing this module is lazy and side-effect-free: ``pyarrow``, ``numpy``,
``pandas``, ``torch``, ``datasets``, ``shardloom._native``, and query modules are
imported only when requested at execution boundaries. No Spark, DataFusion,
DuckDB, Polars, or other external engine fallback is ever attempted.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping, Sequence, Set as AbstractSet
import hashlib
import json
import math
import os
from pathlib import Path
import random
import re
import statistics
import tempfile
from typing import Any

from ._compat import dataclass
from ._vector import normalize_metadata as normalize_vector_metadata
from .errors import NO_FALLBACK_REASON, ShardLoomClosedError, ShardLoomError
from .models import Diagnostic, FallbackStatus

FITTED_PIPELINE_SCHEMA_VERSION = "shardloom.ai_fitted_feature_pipeline.v1"
LEXICAL_INDEX_SCHEMA_VERSION = "shardloom.ai_lexical_index.v1"
COMPACT_VECTOR_EVAL_SCHEMA_VERSION = "shardloom.ai_compact_vector_evaluation.v1"

_SAFE_SQL_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z_0-9]*$")
_TOKEN_RE = re.compile(r"[A-Za-z0-9]+")
_ALLOWED_NULL_POLICIES = frozenset({"error", "allow", "drop_rows"})
_ALLOWED_VECTOR_METRICS = frozenset({"cosine", "l2", "dot"})
_ALLOWED_IMPUTE = frozenset({"mean", "median", "constant"})
_ALLOWED_SCALE = frozenset({"standard", "minmax", "none"})
_ALLOWED_FUSION = frozenset({"rrf", "weighted_sum"})


class AIWorkflowError(ShardLoomError, RuntimeError):
    """Explicit error raised by the ShardLoom AI workflow module (no fallback)."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        suggested_next_step: str = "Inspect schema, generation, or parameters and retry.",
        category: str = "configuration",
    ) -> None:
        super().__init__(message)
        self.code = code
        self.fallback_attempted = False
        self.external_engine_invoked = False
        self.fallback = FallbackStatus(
            attempted=False,
            allowed=False,
            engine=None,
            reason=NO_FALLBACK_REASON,
        )
        self.diagnostics = (
            Diagnostic(
                code=code,
                severity="error",
                category=category,
                message=message,
                feature="ai_data_workflows",
                reason=message,
                suggested_next_step=suggested_next_step,
                fallback=self.fallback,
            ),
        )


class AIWorkflowBoundaryError(AIWorkflowError):
    """Explicit shape, device, generation, or unsupported-boundary error."""


def _validate_identifier(name: str, label: str = "column") -> str:
    if not isinstance(name, str) or _SAFE_SQL_IDENTIFIER_RE.fullmatch(name) is None:
        raise AIWorkflowBoundaryError(
            "SL_AI_INVALID_IDENTIFIER",
            f"{label} {name!r} must be a valid SQL identifier",
        )
    return name


def _require_cpu(device: str) -> str:
    normalized = str(device).strip().lower()
    if normalized != "cpu":
        raise AIWorkflowBoundaryError(
            "SL_AI_UNSUPPORTED_DEVICE",
            f"device={device!r} is unsupported; current ShardLoom AI batch delivery executes on 'cpu' only",
            suggested_next_step="Set device='cpu' and perform any downstream accelerator copy explicitly.",
        )
    return "cpu"


def _normalize_finite_vector(vector: Sequence[float]) -> tuple[float, ...]:
    if isinstance(vector, (str, bytes, bytearray)) or not isinstance(vector, Sequence) or not vector:
        raise AIWorkflowBoundaryError(
            "SL_AI_INVALID_QUERY_VECTOR",
            "query_vector must be a non-empty sequence of finite numeric values",
        )
    values: list[float] = []
    for item in vector:
        if isinstance(item, bool) or not isinstance(item, (int, float)):
            raise AIWorkflowBoundaryError(
                "SL_AI_INVALID_QUERY_VECTOR",
                f"query_vector contains non-numeric element {item!r}",
            )
        fval = float(item)
        if not math.isfinite(fval):
            raise AIWorkflowBoundaryError(
                "SL_AI_INVALID_QUERY_VECTOR",
                "query_vector must contain only finite floating-point numbers",
            )
        values.append(fval)
    return tuple(values)


def _atomic_write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(payload, indent=2, sort_keys=True) + "\n"
    fd, tmp_name = tempfile.mkstemp(prefix=".sl-ai-", suffix=".json", dir=path.parent)
    tmp_path = Path(tmp_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(text)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp_path, path)
    finally:
        tmp_path.unlink(missing_ok=True)


# ---------------------------------------------------------------------------
# Group 1: Model-ready typed batches
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class TypedBatchSpec:
    """Explicit schema, shape, null, mutability, and CPU ownership contract."""

    feature_columns: tuple[str, ...]
    label_columns: tuple[str, ...] = ()
    id_column: str | None = None
    expected_shapes: Mapping[str, tuple[int, ...]] = ()
    expected_dtypes: Mapping[str, str] = ()
    allow_cast: bool = False
    null_policy: str = "error"
    writable: bool = False
    device: str = "cpu"

    def __post_init__(self) -> None:
        if not self.feature_columns:
            raise AIWorkflowBoundaryError(
                "SL_AI_INVALID_BATCH_SPEC",
                "TypedBatchSpec requires at least one feature column",
            )
        for col in self.feature_columns:
            _validate_identifier(col, "feature column")
        for col in self.label_columns:
            _validate_identifier(col, "label column")
        if self.id_column is not None:
            _validate_identifier(self.id_column, "id_column")
        if self.null_policy not in _ALLOWED_NULL_POLICIES:
            raise AIWorkflowBoundaryError(
                "SL_AI_INVALID_NULL_POLICY",
                f"invalid null_policy={self.null_policy!r}; expected one of {sorted(_ALLOWED_NULL_POLICIES)}",
            )
        object.__setattr__(self, "device", _require_cpu(self.device))

    @property
    def selected_columns(self) -> tuple[str, ...]:
        ordered: list[str] = []
        if self.id_column is not None:
            ordered.append(self.id_column)
        for col in self.feature_columns:
            if col not in ordered:
                ordered.append(col)
        for col in self.label_columns:
            if col not in ordered:
                ordered.append(col)
        return tuple(ordered)


@dataclass(frozen=True, slots=True)
class ModelReadyBatch:
    """Typed columnar batch preserving order, shapes, ownership, and provenance."""

    batch_index: int
    num_rows: int
    feature_columns: tuple[str, ...]
    label_columns: tuple[str, ...]
    id_column: str | None
    features: Mapping[str, Any]
    labels: Mapping[str, Any]
    ids: Any | None
    record_batch: Any
    device: str
    ownership: str
    writable: bool
    copied: bool
    python_rows_constructed: bool = False
    fallback_attempted: bool = False
    external_engine_invoked: bool = False

    def writable_feature_buffers(self, column: str) -> list[bytearray]:
        """Return writable bytearray copies of feature value buffers when writable=True."""

        if not self.writable:
            raise AIWorkflowBoundaryError(
                "SL_AI_READONLY_BATCH",
                f"batch feature {column!r} has shared read-only ownership; request writable=True to mutate",
            )
        if column not in self.features:
            raise AIWorkflowBoundaryError(
                "SL_AI_MISSING_COLUMN",
                f"unknown feature column {column!r}",
            )
        arr = self.features[column]
        target = arr.values if hasattr(arr, "values") else arr
        return [bytearray(b.to_pybytes()) for b in target.buffers()[1:] if b is not None]


def _collect_if_lazy(result_or_frame: Any) -> Any:
    if hasattr(result_or_frame, "__arrow_c_stream__"):
        return result_or_frame
    if hasattr(result_or_frame, "collect") and callable(result_or_frame.collect):
        return result_or_frame.collect(check=True)
    raise AIWorkflowBoundaryError(
        "SL_AI_INVALID_SOURCE",
        "expected a ShardLoom LazyFrame or collected Result exposing __arrow_c_stream__",
    )


def _apply_dtype_and_shape_checks(batch: Any, spec: TypedBatchSpec, pa: Any) -> Any:
    available = {batch.schema.field(i).name for i in range(len(batch.schema))}
    for col in spec.selected_columns:
        if col not in available:
            raise AIWorkflowBoundaryError(
                "SL_AI_MISSING_COLUMN",
                f"declared column {col!r} absent from query result schema {sorted(available)}",
            )

    projected = batch.select(list(spec.selected_columns))

    # Enforce null policy before converting/checking fixed shapes cleanly.
    if spec.null_policy == "error":
        for name in spec.selected_columns:
            col = projected.column(name)
            if col.null_count > 0:
                raise AIWorkflowBoundaryError(
                    "SL_AI_NULL_POLICY_VIOLATION",
                    f"column {name!r} contains {col.null_count} nulls under null_policy='error'",
                )
    elif spec.null_policy == "drop_rows":
        import pyarrow.compute as pc

        mask = None
        for name in spec.selected_columns:
            col = projected.column(name)
            valid = pc.is_valid(col)
            mask = valid if mask is None else pc.and_kleene(mask, valid)
        if mask is not None:
            projected = projected.filter(mask)

    arrays: list[Any] = []
    fields: list[Any] = []
    for name in spec.selected_columns:
        col = projected.column(name)
        field = projected.schema.field(name)

        if name in spec.expected_dtypes:
            target_str = str(spec.expected_dtypes[name]).strip().lower()
            target_map = {
                "float16": pa.float16(),
                "float32": pa.float32(),
                "float64": pa.float64(),
                "int8": pa.int8(),
                "int16": pa.int16(),
                "int32": pa.int32(),
                "int64": pa.int64(),
                "uint8": pa.uint8(),
                "uint16": pa.uint16(),
                "uint32": pa.uint32(),
                "uint64": pa.uint64(),
                "bool": pa.bool_(),
                "string": pa.string(),
                "utf8": pa.string(),
                "binary": pa.binary(),
            }
            if target_str not in target_map:
                raise AIWorkflowBoundaryError(
                    "SL_AI_INVALID_DTYPE",
                    f"unsupported expected_dtype {target_str!r} for column {name!r}",
                )
            expected_type = target_map[target_str]
            actual_elem_type = (
                col.type.value_type
                if (pa.types.is_list(col.type) or pa.types.is_fixed_size_list(col.type))
                else col.type
            )
            if actual_elem_type != expected_type:
                if not spec.allow_cast:
                    raise AIWorkflowBoundaryError(
                        "SL_AI_DTYPE_MISMATCH",
                        f"column {name!r} has type {col.type}, expected element dtype {expected_type} (allow_cast=False)",
                    )
                if pa.types.is_fixed_size_list(col.type):
                    col = col.cast(pa.list_(expected_type, col.type.list_size))
                elif pa.types.is_list(col.type):
                    col = col.cast(pa.list_(expected_type))
                else:
                    col = col.cast(expected_type)
                field = pa.field(name, col.type, nullable=field.nullable)

        if name in spec.expected_shapes:
            shape = spec.expected_shapes[name]
            if len(shape) != 1 or type(shape[0]) is not int or shape[0] <= 0:
                raise AIWorkflowBoundaryError(
                    "SL_AI_INVALID_SHAPE",
                    f"expected_shapes currently supports positive 1D vector lengths, got {shape!r} for {name!r}",
                )
            expected_dim = shape[0]
            if pa.types.is_fixed_size_list(col.type):
                if col.type.list_size != expected_dim:
                    raise AIWorkflowBoundaryError(
                        "SL_AI_INVALID_SHAPE",
                        f"column {name!r} has fixed size {col.type.list_size}, expected {expected_dim}",
                    )
            elif pa.types.is_list(col.type) or pa.types.is_large_list(col.type):
                import pyarrow.compute as pc

                if len(col) > 0:
                    lengths = pc.list_value_length(col)
                    mismatch = pc.any(pc.not_equal(lengths, expected_dim)).as_py()
                    if mismatch:
                        raise AIWorkflowBoundaryError(
                            "SL_AI_INVALID_SHAPE",
                            f"column {name!r} contains ragged or mismatched list lengths; expected uniform width {expected_dim}",
                        )
                # Canonicalize validated uniform list column to fixed_size_list.
                if col.null_count == 0:
                    if len(col) > 0:
                        col = pa.FixedSizeListArray.from_arrays(col.values, expected_dim)
                    else:
                        col = pa.array([], type=pa.list_(col.type.value_type, expected_dim))
                    field = pa.field(name, col.type, nullable=field.nullable)
            else:
                raise AIWorkflowBoundaryError(
                    "SL_AI_INVALID_SHAPE",
                    f"column {name!r} of type {col.type} cannot satisfy vector shape {shape!r}",
                )

        arrays.append(col)
        fields.append(field)

    out_batch = pa.RecordBatch.from_arrays(arrays, schema=pa.schema(fields))
    if spec.writable:
        # Clone into exclusive buffers via C++ Arrow IPC round-trip (zero Python row construction).
        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, out_batch.schema) as writer:
            writer.write_batch(out_batch)
        with pa.ipc.open_stream(sink.getvalue()) as reader:
            out_batch = reader.read_next_batch()
    return out_batch


def _make_model_ready_batch(batch_index: int, arrow_batch: Any, spec: TypedBatchSpec) -> ModelReadyBatch:
    features = {col: arrow_batch.column(col) for col in spec.feature_columns}
    labels = {col: arrow_batch.column(col) for col in spec.label_columns}
    ids = arrow_batch.column(spec.id_column) if spec.id_column is not None else None
    return ModelReadyBatch(
        batch_index=batch_index,
        num_rows=arrow_batch.num_rows,
        feature_columns=spec.feature_columns,
        label_columns=spec.label_columns,
        id_column=spec.id_column,
        features=features,
        labels=labels,
        ids=ids,
        record_batch=arrow_batch,
        device=spec.device,
        ownership="exclusive_copy" if spec.writable else "shared_read_only",
        writable=spec.writable,
        copied=spec.writable,
        python_rows_constructed=False,
    )


def deliver_typed_batches(
    result_or_frame: Any,
    spec: TypedBatchSpec,
    *,
    batch_size: int | None = None,
    drop_last: bool = False,
) -> Iterator[ModelReadyBatch]:
    """Stream typed model-ready Arrow batches directly from a ShardLoom result."""

    import pyarrow as pa

    if batch_size is not None and (type(batch_size) is not int or batch_size <= 0):
        raise AIWorkflowBoundaryError("SL_AI_INVALID_BATCH_SIZE", "batch_size must be a positive int")
    result = _collect_if_lazy(result_or_frame)
    reader = pa.RecordBatchReader.from_stream(result)
    try:
        pending_batches: list[Any] = []
        pending_rows = 0
        out_index = 0
        for raw_batch in reader:
            validated = _apply_dtype_and_shape_checks(raw_batch, spec, pa)
            if validated.num_rows == 0:
                continue
            if batch_size is None:
                yield _make_model_ready_batch(out_index, validated, spec)
                out_index += 1
                continue
            pending_batches.append(validated)
            pending_rows += validated.num_rows
            while pending_rows >= batch_size:
                combined = pa.Table.from_batches(pending_batches)
                head = combined.slice(0, batch_size).combine_chunks().to_batches()
                tail_table = combined.slice(batch_size)
                pending_batches = tail_table.to_batches() if tail_table.num_rows > 0 else []
                pending_rows = tail_table.num_rows
                for piece in head:
                    yield _make_model_ready_batch(out_index, piece, spec)
                    out_index += 1

        if batch_size is not None and pending_rows > 0 and not drop_last:
            combined = pa.Table.from_batches(pending_batches).combine_chunks()
            for piece in combined.to_batches():
                if piece.num_rows > 0:
                    yield _make_model_ready_batch(out_index, piece, spec)
                    out_index += 1
    finally:
        reader.close()


# ---------------------------------------------------------------------------
# Group 2: Source generations, StreamingDataset, and IndexedDataset
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class SourceGenerationToken:
    """Snapshot of backing source paths binding epoch/index validity to a generation."""

    snapshots: tuple[tuple[str, bool, int, int], ...]

    @classmethod
    def from_paths(cls, paths: Sequence[str | os.PathLike[str]]) -> SourceGenerationToken:
        snaps: list[tuple[str, bool, int, int]] = []
        for raw in paths:
            p = Path(raw).resolve()
            if not p.exists():
                snaps.append((str(p), False, 0, 0))
            elif p.is_file():
                st = p.stat()
                snaps.append((str(p), True, st.st_size, st.st_mtime_ns))
            else:
                children = sorted(f for f in p.rglob("*") if f.is_file())
                digest_acc = hashlib.sha256()
                total_size = 0
                max_mtime = 0
                for child in children:
                    st = child.stat()
                    total_size += st.st_size
                    max_mtime = max(max_mtime, st.st_mtime_ns)
                    digest_acc.update(f"{child.name}:{st.st_size}:{st.st_mtime_ns};".encode("utf-8"))
                snaps.append((str(p), True, total_size, max_mtime ^ int(digest_acc.hexdigest()[:12], 16)))
        return cls(snapshots=tuple(snaps))

    def verify_unchanged(self) -> None:
        current = SourceGenerationToken.from_paths([s[0] for s in self.snapshots])
        if current.snapshots != self.snapshots:
            raise AIWorkflowBoundaryError(
                "SL_AI_SOURCE_GENERATION_CHANGED",
                "underlying source changed or disappeared during active dataset epoch/index access",
                suggested_next_step="Re-open the dataset on the new committed source generation.",
            )


class StreamingDataset:
    """Epoch-reproducible streaming loader over an executed ShardLoom query plan."""

    def __init__(
        self,
        plan_factory: Callable[[], Any],
        batch_spec: TypedBatchSpec,
        *,
        batch_size: int = 256,
        drop_last: bool = False,
        shuffle_buffer_size: int = 0,
        seed: int = 0,
        epoch: int = 0,
        worker_id: int = 0,
        num_workers: int = 1,
        source_generation: SourceGenerationToken | None = None,
    ) -> None:
        if type(batch_size) is not int or batch_size <= 0:
            raise AIWorkflowBoundaryError("SL_AI_INVALID_BATCH_SIZE", "batch_size must be positive")
        if type(shuffle_buffer_size) is not int or shuffle_buffer_size < 0:
            raise AIWorkflowBoundaryError("SL_AI_INVALID_PARAM", "shuffle_buffer_size must be >= 0")
        if type(num_workers) is not int or num_workers <= 0:
            raise AIWorkflowBoundaryError("SL_AI_INVALID_PARAM", "num_workers must be positive")
        if type(worker_id) is not int or not 0 <= worker_id < num_workers:
            raise AIWorkflowBoundaryError("SL_AI_INVALID_PARAM", "worker_id must be in 0..num_workers")

        self._plan_factory = plan_factory
        self.batch_spec = batch_spec
        self.batch_size = batch_size
        self.drop_last = drop_last
        self.shuffle_buffer_size = shuffle_buffer_size
        self.shuffle_contract = "bounded_buffer_shuffle" if shuffle_buffer_size > 0 else "none"
        self.seed = seed
        self.epoch = epoch
        self.worker_id = worker_id
        self.num_workers = num_workers
        self.source_generation = source_generation
        self._closed = False
        self._active_readers: list[Any] = []

    def __iter__(self) -> Iterator[ModelReadyBatch]:
        if self._closed:
            raise ShardLoomClosedError()
        if self.source_generation is not None:
            self.source_generation.verify_unchanged()

        import pyarrow as pa

        result = _collect_if_lazy(self._plan_factory())
        reader = pa.RecordBatchReader.from_stream(result)
        self._active_readers.append(reader)
        rng = random.Random(f"{self.seed}:{self.epoch}:{self.worker_id}")
        reservoir_chunks: list[Any] = []
        reservoir_rows = 0
        global_row_cursor = 0
        out_index = 0

        try:
            for raw_batch in reader:
                if self._closed:
                    break
                if self.source_generation is not None:
                    self.source_generation.verify_unchanged()
                validated = _apply_dtype_and_shape_checks(raw_batch, self.batch_spec, pa)
                n = validated.num_rows
                if n == 0:
                    continue
                if self.num_workers > 1:
                    worker_indices = [
                        i
                        for i in range(n)
                        if (global_row_cursor + i) % self.num_workers == self.worker_id
                    ]
                    global_row_cursor += n
                    if not worker_indices:
                        continue
                    validated = validated.take(worker_indices)
                else:
                    global_row_cursor += n

                reservoir_chunks.append(validated)
                reservoir_rows += validated.num_rows

                if self.shuffle_buffer_size > 0 and reservoir_rows < max(self.shuffle_buffer_size, self.batch_size):
                    continue

                while reservoir_rows >= self.batch_size:
                    if self.source_generation is not None:
                        self.source_generation.verify_unchanged()
                    table = pa.Table.from_batches(reservoir_chunks).combine_chunks()
                    if self.shuffle_buffer_size > 0:
                        pool_len = min(table.num_rows, max(self.shuffle_buffer_size, self.batch_size))
                        perm = list(range(pool_len))
                        rng.shuffle(perm)
                        selected = perm[: self.batch_size]
                        kept = perm[self.batch_size :] + list(range(pool_len, table.num_rows))
                        out_batch = table.take(selected).combine_chunks().to_batches()[0]
                        tail = table.take(kept) if kept else None
                    else:
                        out_batch = table.slice(0, self.batch_size).combine_chunks().to_batches()[0]
                        tail = table.slice(self.batch_size) if table.num_rows > self.batch_size else None

                    if self.batch_spec.writable:
                        out_batch = _apply_dtype_and_shape_checks(out_batch, self.batch_spec, pa)
                    yield _make_model_ready_batch(out_index, out_batch, self.batch_spec)
                    out_index += 1
                    reservoir_chunks = tail.to_batches() if tail is not None and tail.num_rows > 0 else []
                    reservoir_rows = tail.num_rows if tail is not None else 0

            # Flush remaining reservoir after source end
            while reservoir_rows > 0 and not self._closed:
                if self.source_generation is not None:
                    self.source_generation.verify_unchanged()
                table = pa.Table.from_batches(reservoir_chunks).combine_chunks()
                if self.shuffle_buffer_size > 0:
                    perm = list(range(table.num_rows))
                    rng.shuffle(perm)
                    table = table.take(perm)
                pos = 0
                while pos < table.num_rows:
                    rem = table.num_rows - pos
                    if rem < self.batch_size and self.drop_last:
                        break
                    take_n = min(self.batch_size, rem)
                    out_batch = table.slice(pos, take_n).combine_chunks().to_batches()[0]
                    if self.batch_spec.writable:
                        out_batch = _apply_dtype_and_shape_checks(out_batch, self.batch_spec, pa)
                    yield _make_model_ready_batch(out_index, out_batch, self.batch_spec)
                    out_index += 1
                    pos += take_n
                reservoir_chunks = []
                reservoir_rows = 0
        finally:
            try:
                reader.close()
            finally:
                if reader in self._active_readers:
                    self._active_readers.remove(reader)

    def close(self) -> None:
        self._closed = True
        for reader in list(self._active_readers):
            try:
                reader.close()
            except Exception:
                pass
        self._active_readers.clear()

    def __enter__(self) -> StreamingDataset:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()


class IndexedDataset:
    """Indexed loader restoring arbitrary request order/multiplicity from sorted unique reads."""

    shuffle_contract = "full_epoch_permutation"

    def __init__(
        self,
        result_or_frame: Any,
        batch_spec: TypedBatchSpec,
        *,
        source_generation: SourceGenerationToken | None = None,
    ) -> None:
        import pyarrow as pa

        if source_generation is not None:
            source_generation.verify_unchanged()
        self.batch_spec = batch_spec
        self.source_generation = source_generation
        result = _collect_if_lazy(result_or_frame)
        reader = pa.RecordBatchReader.from_stream(result)
        try:
            reader_schema = reader.schema
            validated_batches = [
                _apply_dtype_and_shape_checks(raw, batch_spec, pa) for raw in reader
            ]
        finally:
            reader.close()
        if validated_batches:
            self._table = pa.Table.from_batches(validated_batches).combine_chunks()
        else:
            empty_batch = _apply_dtype_and_shape_checks(
                pa.RecordBatch.from_pylist([], schema=reader_schema), batch_spec, pa
            )
            self._table = pa.Table.from_batches([empty_batch])
        self._closed = False
        self.last_unique_sorted_read: tuple[int, ...] = ()

    def _ensure_open(self) -> None:
        if self._closed:
            raise AIWorkflowBoundaryError(
                "SL_AI_DATASET_CLOSED",
                "IndexedDataset has been closed and released",
            )
        if self.source_generation is not None:
            self.source_generation.verify_unchanged()

    def __len__(self) -> int:
        self._ensure_open()
        return int(self._table.num_rows)

    def take_batch(self, indices: Sequence[int], *, batch_index: int = 0) -> ModelReadyBatch:
        self._ensure_open()
        import pyarrow as pa

        n = int(self._table.num_rows)
        norm: list[int] = []
        for idx in indices:
            if type(idx) is not int or not 0 <= idx < n:
                raise AIWorkflowBoundaryError(
                    "SL_AI_INDEX_OUT_OF_RANGE",
                    f"sample index {idx!r} out of range for dataset length {n}",
                )
            norm.append(idx)

        # Adhere strictly to sorted unique index contract before scattering to requested order.
        unique_sorted = sorted(set(norm))
        self.last_unique_sorted_read = tuple(unique_sorted)
        if not unique_sorted:
            empty_batch = pa.RecordBatch.from_pylist([], schema=self._table.schema)
            return _make_model_ready_batch(batch_index, empty_batch, self.batch_spec)

        unique_tbl = self._table.take(unique_sorted)
        pos_map = {orig_idx: pos for pos, orig_idx in enumerate(unique_sorted)}
        scatter = [pos_map[orig_idx] for orig_idx in norm]
        scattered = unique_tbl.take(scatter).combine_chunks().to_batches()[0]
        if self.batch_spec.writable:
            scattered = _apply_dtype_and_shape_checks(scattered, self.batch_spec, pa)
        return _make_model_ready_batch(batch_index, scattered, self.batch_spec)

    def __getitem__(self, item: int | Sequence[int]) -> ModelReadyBatch:
        if type(item) is int:
            return self.take_batch([item])
        return self.take_batch(item)

    def epoch_indices(
        self,
        epoch: int = 0,
        *,
        seed: int = 0,
        shuffle: bool = True,
        worker_id: int = 0,
        num_workers: int = 1,
    ) -> list[int]:
        self._ensure_open()
        if type(num_workers) is not int or num_workers <= 0 or not 0 <= worker_id < num_workers:
            raise AIWorkflowBoundaryError("SL_AI_INVALID_PARAM", "invalid worker_id/num_workers")
        all_indices = list(range(self._table.num_rows))
        if shuffle:
            random.Random(f"{seed}:{epoch}").shuffle(all_indices)
        return [idx for pos, idx in enumerate(all_indices) if pos % num_workers == worker_id]

    def iter_epoch_batches(
        self,
        batch_size: int,
        *,
        epoch: int = 0,
        seed: int = 0,
        shuffle: bool = True,
        drop_last: bool = False,
        worker_id: int = 0,
        num_workers: int = 1,
    ) -> Iterator[ModelReadyBatch]:
        indices = self.epoch_indices(
            epoch=epoch, seed=seed, shuffle=shuffle, worker_id=worker_id, num_workers=num_workers
        )
        b_idx = 0
        for pos in range(0, len(indices), batch_size):
            chunk = indices[pos : pos + batch_size]
            if len(chunk) < batch_size and drop_last:
                break
            yield self.take_batch(chunk, batch_index=b_idx)
            b_idx += 1

    def close(self) -> None:
        self._closed = True
        self._table = None

    def __enter__(self) -> IndexedDataset:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()


def as_torch_iterable_dataset(streaming_ds: StreamingDataset) -> Any:
    """Wrap a ShardLoom StreamingDataset as a torch.utils.data.IterableDataset if torch is installed."""

    try:
        from torch.utils.data import IterableDataset
    except ImportError as error:
        raise AIWorkflowBoundaryError(
            "SL_AI_OPTIONAL_DEPENDENCY_MISSING",
            "optional dependency 'torch' is not installed for as_torch_iterable_dataset()",
        ) from error

    class _ShardLoomTorchIterable(IterableDataset):  # type: ignore[misc]
        def __init__(self, ds: StreamingDataset) -> None:
            self._ds = ds

        def __iter__(self):
            return iter(self._ds)

    return _ShardLoomTorchIterable(streaming_ds)


def as_torch_map_dataset(indexed_ds: IndexedDataset) -> Any:
    """Wrap a ShardLoom IndexedDataset as a torch.utils.data.Dataset if torch is installed."""

    try:
        from torch.utils.data import Dataset
    except ImportError as error:
        raise AIWorkflowBoundaryError(
            "SL_AI_OPTIONAL_DEPENDENCY_MISSING",
            "optional dependency 'torch' is not installed for as_torch_map_dataset()",
        ) from error

    class _ShardLoomTorchDataset(Dataset):  # type: ignore[misc]
        def __init__(self, ds: IndexedDataset) -> None:
            self._ds = ds

        def __len__(self) -> int:
            return len(self._ds)

        def __getitem__(self, idx: int):
            return self._ds.take_batch([idx])

    return _ShardLoomTorchDataset(indexed_ds)


def as_hf_iterable_dataset(streaming_ds: StreamingDataset) -> Any:
    """Wrap a ShardLoom StreamingDataset as a Hugging Face IterableDataset if datasets is installed."""

    try:
        import datasets as hf_datasets
    except ImportError as error:
        raise AIWorkflowBoundaryError(
            "SL_AI_OPTIONAL_DEPENDENCY_MISSING",
            "optional dependency 'datasets' is not installed for as_hf_iterable_dataset()",
        ) from error

    def _gen():
        for batch in streaming_ds:
            yield batch.features

    return hf_datasets.IterableDataset.from_generator(_gen)


# ---------------------------------------------------------------------------
# Group 3: Persisted fitted feature prep, splits, and point-in-time joins
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class FeaturePipelineSpec:
    """Declared numeric & categorical feature preparation contract."""

    numeric_columns: tuple[str, ...] = ()
    categorical_columns: tuple[str, ...] = ()
    impute_strategy: str = "mean"
    impute_constant: float = 0.0
    scale_strategy: str = "standard"
    unknown_category_id: int = 0
    null_category_id: int = -1
    algorithm_version: str = "shardloom.feature_prep.v1"

    def __post_init__(self) -> None:
        if not self.numeric_columns and not self.categorical_columns:
            raise AIWorkflowBoundaryError(
                "SL_AI_INVALID_FEATURE_SPEC",
                "FeaturePipelineSpec requires at least one numeric or categorical column",
            )
        for col in (*self.numeric_columns, *self.categorical_columns):
            _validate_identifier(col, "feature column")
        if self.impute_strategy not in _ALLOWED_IMPUTE:
            raise AIWorkflowBoundaryError("SL_AI_INVALID_IMPUTE", f"invalid impute_strategy={self.impute_strategy!r}")
        if self.scale_strategy not in _ALLOWED_SCALE:
            raise AIWorkflowBoundaryError("SL_AI_INVALID_SCALE", f"invalid scale_strategy={self.scale_strategy!r}")

    @property
    def feature_order(self) -> tuple[str, ...]:
        return (*self.numeric_columns, *self.categorical_columns)


@dataclass(frozen=True, slots=True)
class FittedFeaturePipeline:
    """Immutable fitted feature state saved/reopened across training and inference."""

    schema_version: str
    algorithm_version: str
    spec: FeaturePipelineSpec
    feature_order: tuple[str, ...]
    input_schema: tuple[tuple[str, str], ...]
    numeric_params: Mapping[str, Mapping[str, float]]
    category_vocabularies: Mapping[str, Mapping[str, int]]

    @classmethod
    def fit(cls, ctx: Any, train_frame: Any, spec: FeaturePipelineSpec) -> FittedFeaturePipeline:
        _ = ctx
        tbl = train_frame.collect(check=True).to_arrow()
        input_schema = tuple((field.name, str(field.type)) for field in tbl.schema)

        numeric_params: dict[str, dict[str, float]] = {}
        for col in spec.numeric_columns:
            raw_vals = [float(v) for v in tbl.column(col).to_pylist() if v is not None]
            if any(not math.isfinite(v) for v in raw_vals):
                raise AIWorkflowBoundaryError(
                    "SL_AI_NONFINITE_FEATURE",
                    f"training column {col!r} contains non-finite values",
                )
            if not raw_vals:
                fill_val = float(spec.impute_constant)
                center = fill_val
                scale = 1.0
            else:
                if spec.impute_strategy == "mean":
                    fill_val = float(statistics.fmean(raw_vals))
                elif spec.impute_strategy == "median":
                    fill_val = float(statistics.median(raw_vals))
                else:
                    fill_val = float(spec.impute_constant)

                imputed = [float(v) if v is not None else fill_val for v in tbl.column(col).to_pylist()]
                if spec.scale_strategy == "standard":
                    center = float(statistics.fmean(imputed))
                    std = float(statistics.pstdev(imputed))
                    scale = std if std > 0.0 else 1.0
                elif spec.scale_strategy == "minmax":
                    lo, hi = min(imputed), max(imputed)
                    center = float(lo)
                    scale = float(hi - lo) if hi > lo else 1.0
                else:
                    center = 0.0
                    scale = 1.0
            numeric_params[col] = {
                "impute_value": float(fill_val),
                "center": float(center),
                "scale": float(scale),
            }

        vocabs: dict[str, dict[str, int]] = {}
        for col in spec.categorical_columns:
            distinct_vals = sorted(
                {str(v) for v in tbl.column(col).to_pylist() if v is not None}
            )
            vocabs[col] = {val: idx + 1 for idx, val in enumerate(distinct_vals)}

        return cls(
            schema_version=FITTED_PIPELINE_SCHEMA_VERSION,
            algorithm_version=spec.algorithm_version,
            spec=spec,
            feature_order=spec.feature_order,
            input_schema=input_schema,
            numeric_params=numeric_params,
            category_vocabularies=vocabs,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "algorithm_version": self.algorithm_version,
            "spec": {
                "numeric_columns": list(self.spec.numeric_columns),
                "categorical_columns": list(self.spec.categorical_columns),
                "impute_strategy": self.spec.impute_strategy,
                "impute_constant": self.spec.impute_constant,
                "scale_strategy": self.spec.scale_strategy,
                "unknown_category_id": self.spec.unknown_category_id,
                "null_category_id": self.spec.null_category_id,
                "algorithm_version": self.spec.algorithm_version,
            },
            "feature_order": list(self.feature_order),
            "input_schema": [
                {"name": name, "type": dtype} for name, dtype in self.input_schema
            ],
            "numeric_params": {k: dict(v) for k, v in self.numeric_params.items()},
            "category_vocabularies": {k: dict(v) for k, v in self.category_vocabularies.items()},
        }

    def save(self, path: str | os.PathLike[str]) -> Path:
        target = Path(path)
        _atomic_write_json(target, self.to_dict())
        return target

    @classmethod
    def load(cls, path: str | os.PathLike[str]) -> FittedFeaturePipeline:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
        if payload.get("schema_version") != FITTED_PIPELINE_SCHEMA_VERSION:
            raise AIWorkflowBoundaryError(
                "SL_AI_CORRUPT_PIPELINE",
                f"unexpected fitted pipeline schema_version={payload.get('schema_version')!r}",
            )
        raw_spec = payload["spec"]
        spec = FeaturePipelineSpec(
            numeric_columns=tuple(raw_spec["numeric_columns"]),
            categorical_columns=tuple(raw_spec["categorical_columns"]),
            impute_strategy=raw_spec["impute_strategy"],
            impute_constant=float(raw_spec["impute_constant"]),
            scale_strategy=raw_spec["scale_strategy"],
            unknown_category_id=int(raw_spec["unknown_category_id"]),
            null_category_id=int(raw_spec["null_category_id"]),
            algorithm_version=str(raw_spec["algorithm_version"]),
        )
        return cls(
            schema_version=payload["schema_version"],
            algorithm_version=payload["algorithm_version"],
            spec=spec,
            feature_order=tuple(payload["feature_order"]),
            input_schema=tuple((row["name"], row["type"]) for row in payload["input_schema"]),
            numeric_params=payload["numeric_params"],
            category_vocabularies=payload["category_vocabularies"],
        )

    def transform(self, ctx: Any, frame: Any) -> Any:
        """Apply frozen parameters cleanly through ShardLoom without refitting."""

        import shardloom as sl

        cur = frame
        # 1. Numeric imputation (single-branch SQL CASE WHEN)
        if self.spec.numeric_columns:
            for col in self.spec.numeric_columns:
                p = self.numeric_params[col]
                cur = cur.with_column(
                    f"__imp_{col}",
                    sl.case_when(sl.col(col).is_null(), p["impute_value"], sl.col(col)),
                )
            # 2. Deterministic scaling over imputed column
            for col in self.spec.numeric_columns:
                p = self.numeric_params[col]
                center, scale = p["center"], p["scale"]
                cur = cur.with_column(col, (sl.col(f"__imp_{col}") - center) / scale)

        # 3. Deterministic semantic categorical encoding via chained single-branch case_when
        for col in self.spec.categorical_columns:
            vocab = self.category_vocabularies[col]
            out_col = f"__cat_{col}"
            cur = cur.with_column(
                out_col,
                sl.case_when(
                    sl.col(col).is_null(),
                    self.spec.null_category_id,
                    self.spec.unknown_category_id,
                ),
            )
            for cat_val, cat_id in vocab.items():
                cur = cur.with_column(
                    out_col,
                    sl.case_when(sl.col(col) == cat_val, cat_id, sl.col(out_col)),
                )
            cur = cur.with_column(col, sl.col(out_col))

        return cur


def assign_deterministic_splits(
    ctx: Any,
    frame: Any,
    *,
    id_column: str,
    group_column: str | None = None,
    time_column: str | None = None,
    train_ratio: float = 0.7,
    val_ratio: float = 0.15,
    train_end: Any = None,
    validation_end: Any = None,
    seed: str = "shardloom-v1",
    split_column: str = "split",
) -> Any:
    """Assign reproducible leakage-free train/validation/test splits via ShardLoom."""

    import shardloom as sl

    _validate_identifier(id_column, "id_column")
    _validate_identifier(split_column, "split_column")
    if (group_column is None) == (time_column is None):
        raise AIWorkflowBoundaryError(
            "SL_AI_INVALID_SPLIT_CONFIG",
            "supply exactly one of group_column= or time_column=",
        )

    if time_column is not None:
        _validate_identifier(time_column, "time_column")
        if train_end is None or validation_end is None:
            raise AIWorkflowBoundaryError(
                "SL_AI_INVALID_SPLIT_CONFIG",
                "time_column splits require both train_end and validation_end cutoffs",
            )
        cur = frame.with_column(
            split_column,
            sl.case_when(sl.col(time_column) <= train_end, "train", "test"),
        )
        return cur.with_column(
            split_column,
            sl.case_when(
                (sl.col(time_column) > train_end) & (sl.col(time_column) <= validation_end),
                "validation",
                sl.col(split_column),
            ),
        )

    assert group_column is not None
    _validate_identifier(group_column, "group_column")
    distinct_groups = (
        ctx.sql(f"SELECT DISTINCT {group_column} AS __g FROM s", relations={"s": frame})
        .collect(check=True)
        .to_arrow()
        .column("__g")
        .to_pylist()
    )
    import pyarrow as pa

    assigned: list[dict[str, Any]] = []
    train_cut = int(train_ratio * 10_000)
    val_cut = int((train_ratio + val_ratio) * 10_000)
    for gval in distinct_groups:
        digest = hashlib.sha256(f"{seed}:{gval!r}".encode("utf-8")).hexdigest()
        bucket = int(digest[:8], 16) % 10_000
        if bucket < train_cut:
            label = "train"
        elif bucket < val_cut:
            label = "validation"
        else:
            label = "test"
        assigned.append({"__g": gval, split_column: label})

    if not assigned:
        return frame.with_column(split_column, sl.case_when(sl.col(id_column).is_null(), "train", "train"))

    sample_tbl = frame.limit(0).collect(check=True).to_arrow()
    g_type = sample_tbl.schema.field(group_column).type
    map_table = pa.Table.from_pylist(
        assigned,
        schema=pa.schema([pa.field("__g", g_type), pa.field(split_column, pa.string())]),
    )
    map_frame = ctx.from_arrow_table(map_table)
    # Build explicit projection preserving all source columns cleanly.
    src_cols = ", ".join(f"s.{c} AS {c}" for c in sample_tbl.schema.names if c != split_column)
    return ctx.sql(
        f"SELECT {src_cols}, m.{split_column} AS {split_column} "
        f"FROM s AS s JOIN m AS m ON s.{group_column} = m.__g",
        relations={"s": frame, "m": map_frame},
    )


def point_in_time_feature_join(
    observations_frame: Any,
    features_frame: Any,
    *,
    entity_column: str,
    decision_time_column: str,
    availability_time_column: str,
    effective_time_column: str | None = None,
) -> Any:
    """Point-in-time ASOF join keyed strictly on application availability time."""

    _validate_identifier(entity_column, "entity_column")
    _validate_identifier(decision_time_column, "decision_time_column")
    _validate_identifier(availability_time_column, "availability_time_column")
    if effective_time_column is not None:
        _validate_identifier(effective_time_column, "effective_time_column")
        if availability_time_column == effective_time_column:
            raise AIWorkflowBoundaryError(
                "SL_AI_AVAILABILITY_TIME_CONFLICT",
                "availability_time_column must represent when information became available, distinct from economic effective_time_column",
            )
    return observations_frame.join_asof(
        features_frame,
        left_on=decision_time_column,
        right_on=availability_time_column,
        by=entity_column,
    )


# ---------------------------------------------------------------------------
# Group 4: Exact filtered vector retrieval + late payload fetch
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class VectorRetrievalSpec:
    """Exact vector search specification over an eligible population."""

    id_column: str
    vector_column: str
    query_vector: Sequence[float]
    k: int
    metric: str = "cosine"
    dimensions: int | None = None
    embedding_space: str | None = None
    expected_embedding_space: str | None = None
    eligibility_predicate: Any = None
    payload_columns: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        _validate_identifier(self.id_column, "id_column")
        _validate_identifier(self.vector_column, "vector_column")
        norm_vec = _normalize_finite_vector(self.query_vector)
        object.__setattr__(self, "query_vector", norm_vec)

        if type(self.k) is not int or self.k <= 0:
            raise AIWorkflowBoundaryError("SL_AI_INVALID_K", "k must be a positive integer")
        metric = str(self.metric).strip().lower()
        if metric not in _ALLOWED_VECTOR_METRICS:
            raise AIWorkflowBoundaryError(
                "SL_AI_UNSUPPORTED_METRIC",
                f"unsupported metric={self.metric!r}; expected one of {sorted(_ALLOWED_VECTOR_METRICS)}",
            )
        object.__setattr__(self, "metric", metric)

        if self.dimensions is not None:
            if type(self.dimensions) is not int or self.dimensions <= 0:
                raise AIWorkflowBoundaryError("SL_AI_INVALID_DIMENSIONS", "dimensions must be positive")
            if len(norm_vec) != self.dimensions:
                raise AIWorkflowBoundaryError(
                    "SL_AI_DIMENSION_MISMATCH",
                    f"query_vector has {len(norm_vec)} dimensions, expected {self.dimensions}",
                )

        if (
            self.embedding_space is not None
            and self.expected_embedding_space is not None
            and self.embedding_space != self.expected_embedding_space
        ):
            raise AIWorkflowBoundaryError(
                "SL_AI_EMBEDDING_SPACE_MISMATCH",
                f"embedding_space={self.embedding_space!r} does not match expected={self.expected_embedding_space!r}",
            )

        if metric == "cosine" and math.fsum(x * x for x in norm_vec) == 0.0:
            raise AIWorkflowBoundaryError(
                "SL_AI_ZERO_NORM_QUERY",
                "cosine similarity query_vector must have non-zero Euclidean norm",
            )

        for col in self.payload_columns:
            _validate_identifier(col, "payload column")


def exact_vector_search(
    ctx: Any,
    frame: Any,
    spec: VectorRetrievalSpec,
) -> tuple[Any, dict[str, Any]]:
    """Run exact filtered native vector scoring, deterministic top-K, and late payload fetch."""

    import shardloom as sl
    from shardloom import functions as F

    eligible = frame if spec.eligibility_predicate is None else frame.filter(spec.eligibility_predicate)
    query_arr = F.array(list(spec.query_vector))
    if spec.metric == "cosine":
        score_expr = F.vector_cosine_similarity(sl.col(spec.vector_column), query_arr)
        order_dir = "DESC"
    elif spec.metric == "l2":
        score_expr = F.vector_l2_distance(sl.col(spec.vector_column), query_arr)
        order_dir = "ASC"
    else:
        score_expr = F.vector_dot(sl.col(spec.vector_column), query_arr)
        order_dir = "DESC"

    # Score only (id, score) over the eligible population; payloads stay unmaterialized until after top-K.
    scored = eligible.select(sl.col(spec.id_column), score_expr.alias("__score"))
    topk_frame = ctx.sql(
        f"SELECT {spec.id_column}, __score AS score FROM s "
        f"WHERE __score IS NOT NULL ORDER BY __score {order_dir}, {spec.id_column} ASC LIMIT {spec.k}",
        relations={"s": scored},
    )

    if spec.payload_columns:
        payload_select = ", ".join(f"p.{col} AS {col}" for col in spec.payload_columns)
        payload_source = eligible.select(spec.id_column, *(sl.col(c) for c in spec.payload_columns))
        final_frame = ctx.sql(
            f"SELECT t.{spec.id_column} AS {spec.id_column}, t.score AS score, {payload_select} "
            f"FROM t AS t JOIN p AS p ON t.{spec.id_column} = p.{spec.id_column} "
            f"ORDER BY t.score {order_dir}, t.{spec.id_column} ASC",
            relations={"t": topk_frame, "p": payload_source},
        )
    else:
        final_frame = topk_frame

    result = final_frame.collect(check=True)
    report = {
        "exact": True,
        "metric": spec.metric,
        "k": spec.k,
        "dimensions": len(spec.query_vector),
        "embedding_space": spec.embedding_space,
        "eligibility_applied_before_topk": True,
        "late_payload_fetch": bool(spec.payload_columns),
        "fallback_attempted": False,
        "external_engine_invoked": False,
    }
    return result, report


# ---------------------------------------------------------------------------
# Group 5: Lexical BM25, hybrid retrieval, and compact int8 evaluation
# ---------------------------------------------------------------------------


def _tokenize_text(text: str, *, lowercase: bool = True) -> list[str]:
    normalized = text.lower() if lowercase else text
    return _TOKEN_RE.findall(normalized)


class LexicalIndex:
    """Generation-bound inverted BM25 index with declared tokenization & eligibility filter."""

    def __init__(
        self,
        *,
        index_id: str,
        id_column: str,
        text_column: str,
        lowercase: bool,
        doc_lengths: Mapping[Any, int],
        inverted_postings: Mapping[str, Mapping[Any, int]],
        avg_doc_length: float,
        source_generation: SourceGenerationToken | None = None,
    ) -> None:
        self.index_id = index_id
        self.id_column = id_column
        self.text_column = text_column
        self.lowercase = lowercase
        self.doc_lengths = dict(doc_lengths)
        self.inverted_postings = {term: dict(p) for term, p in inverted_postings.items()}
        self.avg_doc_length = float(avg_doc_length)
        self.source_generation = source_generation

    @classmethod
    def build(
        cls,
        ctx: Any,
        frame: Any,
        *,
        id_column: str,
        text_column: str,
        index_id: str = "default",
        lowercase: bool = True,
        source_generation: SourceGenerationToken | None = None,
    ) -> LexicalIndex:
        import shardloom as sl

        _ = ctx
        _validate_identifier(id_column, "id_column")
        _validate_identifier(text_column, "text_column")
        if source_generation is not None:
            source_generation.verify_unchanged()

        tbl = frame.select(sl.col(id_column), sl.col(text_column)).collect(check=True).to_arrow()
        doc_lengths: dict[Any, int] = {}
        postings: dict[str, dict[Any, int]] = {}
        total_len = 0
        for row in tbl.to_pylist():
            doc_id = row[id_column]
            raw_text = row[text_column]
            tokens = _tokenize_text("" if raw_text is None else str(raw_text), lowercase=lowercase)
            doc_lengths[doc_id] = len(tokens)
            total_len += len(tokens)
            for tok in tokens:
                term_map = postings.setdefault(tok, {})
                term_map[doc_id] = term_map.get(doc_id, 0) + 1

        avg_len = (total_len / len(doc_lengths)) if doc_lengths else 0.0
        return cls(
            index_id=index_id,
            id_column=id_column,
            text_column=text_column,
            lowercase=lowercase,
            doc_lengths=doc_lengths,
            inverted_postings=postings,
            avg_doc_length=avg_len,
            source_generation=source_generation,
        )

    def save(self, path: str | os.PathLike[str]) -> Path:
        payload = {
            "schema_version": LEXICAL_INDEX_SCHEMA_VERSION,
            "index_id": self.index_id,
            "id_column": self.id_column,
            "text_column": self.text_column,
            "tokenizer": "alnum_tokens.v1",
            "lowercase": self.lowercase,
            "avg_doc_length": self.avg_doc_length,
            "doc_lengths": [[k, v] for k, v in self.doc_lengths.items()],
            "inverted_postings": {
                term: [[doc_id, tf] for doc_id, tf in postings.items()]
                for term, postings in self.inverted_postings.items()
            },
            "source_snapshots": (
                [list(s) for s in self.source_generation.snapshots]
                if self.source_generation is not None
                else None
            ),
        }
        target = Path(path)
        _atomic_write_json(target, payload)
        return target

    @classmethod
    def load(cls, path: str | os.PathLike[str]) -> LexicalIndex:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
        if payload.get("schema_version") != LEXICAL_INDEX_SCHEMA_VERSION:
            raise AIWorkflowBoundaryError(
                "SL_AI_CORRUPT_INDEX",
                f"unexpected lexical index schema_version={payload.get('schema_version')!r}",
            )
        snaps = payload.get("source_snapshots")
        gen = (
            SourceGenerationToken(
                snapshots=tuple((str(s[0]), bool(s[1]), int(s[2]), int(s[3])) for s in snaps)
            )
            if snaps is not None
            else None
        )
        return cls(
            index_id=str(payload["index_id"]),
            id_column=str(payload["id_column"]),
            text_column=str(payload["text_column"]),
            lowercase=bool(payload["lowercase"]),
            doc_lengths={k: int(v) for k, v in payload["doc_lengths"]},
            inverted_postings={
                term: {k: int(tf) for k, tf in postings}
                for term, postings in payload["inverted_postings"].items()
            },
            avg_doc_length=float(payload["avg_doc_length"]),
            source_generation=gen,
        )

    def search(
        self,
        query: str,
        *,
        k: int,
        eligible_ids: AbstractSet[Any] | None = None,
        k1: float = 1.5,
        b: float = 0.75,
    ) -> list[dict[str, Any]]:
        if type(k) is not int or k <= 0:
            raise AIWorkflowBoundaryError("SL_AI_INVALID_K", "k must be a positive integer")
        if self.source_generation is not None:
            self.source_generation.verify_unchanged()

        terms = _tokenize_text(query, lowercase=self.lowercase)
        if not terms or not self.doc_lengths:
            return []

        n_docs = len(self.doc_lengths)
        avg_dl = self.avg_doc_length if self.avg_doc_length > 0.0 else 1.0
        scores: dict[Any, float] = {}

        for term in set(terms):
            postings = self.inverted_postings.get(term)
            if not postings:
                continue
            df = len(postings)
            idf = math.log((n_docs - df + 0.5) / (df + 0.5) + 1.0)
            for doc_id, tf in postings.items():
                if eligible_ids is not None and doc_id not in eligible_ids:
                    continue
                dl = self.doc_lengths[doc_id]
                denom = tf + k1 * (1.0 - b + b * (dl / avg_dl))
                bm25 = idf * ((tf * (k1 + 1.0)) / denom) if denom > 0.0 else 0.0
                scores[doc_id] = scores.get(doc_id, 0.0) + bm25

        ranked = sorted(scores.items(), key=lambda pair: (-pair[1], pair[0]))[:k]
        return [{self.id_column: doc_id, "bm25_score": float(sc)} for doc_id, sc in ranked]


def hybrid_search(
    ctx: Any,
    frame: Any,
    *,
    vector_spec: VectorRetrievalSpec,
    lexical_index: LexicalIndex,
    lexical_query: str,
    fusion: str = "rrf",
    rrf_k: int = 60,
    vector_weight: float = 0.5,
    lexical_weight: float = 0.5,
    k: int | None = None,
) -> tuple[Any, dict[str, Any]]:
    """Fuse exact eligible vector retrieval and generation-checked lexical BM25 with late payloads."""

    import pyarrow as pa
    import shardloom as sl

    if fusion not in _ALLOWED_FUSION:
        raise AIWorkflowBoundaryError(
            "SL_AI_UNSUPPORTED_FUSION",
            f"unsupported fusion={fusion!r}; expected one of {sorted(_ALLOWED_FUSION)}",
        )
    top_k = vector_spec.k if k is None else k
    eligible = frame if vector_spec.eligibility_predicate is None else frame.filter(vector_spec.eligibility_predicate)
    eligible_ids_tbl = eligible.select(sl.col(vector_spec.id_column)).collect(check=True).to_arrow()
    eligible_ids = set(eligible_ids_tbl.column(vector_spec.id_column).to_pylist())

    candidate_k = max(top_k, len(eligible_ids)) if eligible_ids else top_k
    vec_only_spec = VectorRetrievalSpec(
        id_column=vector_spec.id_column,
        vector_column=vector_spec.vector_column,
        query_vector=vector_spec.query_vector,
        k=candidate_k,
        metric=vector_spec.metric,
        dimensions=vector_spec.dimensions,
        embedding_space=vector_spec.embedding_space,
        expected_embedding_space=vector_spec.expected_embedding_space,
        eligibility_predicate=vector_spec.eligibility_predicate,
        payload_columns=(),
    )
    vec_res, _ = exact_vector_search(ctx, frame, vec_only_spec)
    vec_rows = vec_res.to_arrow().to_pylist()
    lex_rows = lexical_index.search(lexical_query, k=candidate_k, eligible_ids=eligible_ids)

    fused: dict[Any, float] = {}
    if fusion == "rrf":
        for rank, row in enumerate(vec_rows, start=1):
            doc_id = row[vector_spec.id_column]
            fused[doc_id] = fused.get(doc_id, 0.0) + vector_weight / (rrf_k + rank)
        for rank, row in enumerate(lex_rows, start=1):
            doc_id = row[lexical_index.id_column]
            fused[doc_id] = fused.get(doc_id, 0.0) + lexical_weight / (rrf_k + rank)
    else:
        max_v = max((abs(float(r["score"])) for r in vec_rows), default=1.0) or 1.0
        max_l = max((abs(float(r["bm25_score"])) for r in lex_rows), default=1.0) or 1.0
        for row in vec_rows:
            doc_id = row[vector_spec.id_column]
            val = float(row["score"])
            norm_v = (1.0 / (1.0 + val)) if vector_spec.metric == "l2" else (val / max_v)
            fused[doc_id] = fused.get(doc_id, 0.0) + vector_weight * norm_v
        for row in lex_rows:
            doc_id = row[lexical_index.id_column]
            norm_l = float(row["bm25_score"]) / max_l
            fused[doc_id] = fused.get(doc_id, 0.0) + lexical_weight * norm_l

    winners = sorted(fused.items(), key=lambda pair: (-pair[1], pair[0]))[:top_k]
    id_type = eligible_ids_tbl.schema.field(vector_spec.id_column).type
    win_tbl = pa.Table.from_pylist(
        [{vector_spec.id_column: wid, "hybrid_score": float(wscore)} for wid, wscore in winners],
        schema=pa.schema([pa.field(vector_spec.id_column, id_type), pa.field("hybrid_score", pa.float64())]),
    )
    win_frame = ctx.from_arrow_table(win_tbl)
    if vector_spec.payload_columns:
        payload_select = ", ".join(f"p.{c} AS {c}" for c in vector_spec.payload_columns)
        payload_src = eligible.select(vector_spec.id_column, *(sl.col(c) for c in vector_spec.payload_columns))
        final_frame = ctx.sql(
            f"SELECT w.{vector_spec.id_column} AS {vector_spec.id_column}, w.hybrid_score AS hybrid_score, {payload_select} "
            f"FROM w AS w JOIN p AS p ON w.{vector_spec.id_column} = p.{vector_spec.id_column} "
            f"ORDER BY w.hybrid_score DESC, w.{vector_spec.id_column} ASC",
            relations={"w": win_frame, "p": payload_src},
        )
    else:
        final_frame = win_frame

    result = final_frame.collect(check=True)
    return result, {
        "fusion": fusion,
        "k": top_k,
        "eligible_population": len(eligible_ids),
        "vector_hits": len(vec_rows),
        "lexical_hits": len(lex_rows),
        "late_payload_fetch": bool(vector_spec.payload_columns),
        "fallback_attempted": False,
        "external_engine_invoked": False,
    }


def _score_pair(a: Sequence[float], b: Sequence[float], metric: str) -> float:
    if metric == "dot":
        return math.fsum(x * y for x, y in zip(a, b))
    if metric == "l2":
        return math.sqrt(math.fsum((x - y) * (x - y) for x, y in zip(a, b)))
    na = math.sqrt(math.fsum(x * x for x in a))
    nb = math.sqrt(math.fsum(y * y for y in b))
    if na == 0.0 or nb == 0.0:
        return float("nan")
    return math.fsum(x * y for x, y in zip(a, b)) / (na * nb)


def evaluate_scalar_quantized_retrieval(
    ctx: Any,
    frame: Any,
    *,
    spec: VectorRetrievalSpec,
    rescore_candidates: int | None = None,
) -> dict[str, Any]:
    """Evaluate opt-in per-vector int8 affine quantization recall vs exact eligible search."""

    import shardloom as sl

    # Validate quantization representation contract via shared _vector metadata validator.
    normalize_vector_metadata(
        "quantized_vector",
        {
            "version": 1,
            "uniform_size": len(spec.query_vector),
            "parameter_mode": "per_vector",
        },
    )

    exact_res, _ = exact_vector_search(
        ctx,
        frame,
        VectorRetrievalSpec(
            id_column=spec.id_column,
            vector_column=spec.vector_column,
            query_vector=spec.query_vector,
            k=spec.k,
            metric=spec.metric,
            dimensions=spec.dimensions,
            embedding_space=spec.embedding_space,
            expected_embedding_space=spec.expected_embedding_space,
            eligibility_predicate=spec.eligibility_predicate,
            payload_columns=(),
        ),
    )
    exact_ids = [row[spec.id_column] for row in exact_res.to_arrow().to_pylist()]

    eligible = frame if spec.eligibility_predicate is None else frame.filter(spec.eligibility_predicate)
    rows = eligible.select(sl.col(spec.id_column), sl.col(spec.vector_column)).collect(check=True).to_arrow().to_pylist()

    raw_bytes = 0
    quant_bytes = 0
    q_scores: list[tuple[Any, float]] = []
    vectors_by_id: dict[Any, list[float]] = {}

    for row in rows:
        vec = row[spec.vector_column]
        if vec is None:
            continue
        fvec = [float(v) for v in vec]
        if any(not math.isfinite(v) for v in fvec):
            continue
        raw_bytes += len(fvec) * 4
        lo, hi = min(fvec), max(fvec)
        if hi > lo:
            scale = (hi - lo) / 254.0
            zero_pt = max(-127, min(127, round(-127.0 - lo / scale)))
            codes = [max(-127, min(127, round(v / scale) + zero_pt)) for v in fvec]
        else:
            scale = 1.0
            zero_pt = 0
            codes = [0] * len(fvec)
        # int8 codes + float32 scale (4B) + int8 zero_point (1B)
        quant_bytes += len(codes) + 5
        reconstructed = [(c - zero_pt) * scale for c in codes]
        sc = _score_pair(reconstructed, spec.query_vector, spec.metric)
        if math.isfinite(sc):
            doc_id = row[spec.id_column]
            vectors_by_id[doc_id] = fvec
            q_scores.append((doc_id, sc))

    descending = spec.metric != "l2"
    q_scores.sort(key=lambda pair: (-pair[1] if descending else pair[1], pair[0]))
    q_topk_ids = [doc_id for doc_id, _ in q_scores[: spec.k]]

    rescored_ids = q_topk_ids
    if rescore_candidates is not None and rescore_candidates > 0:
        cand_ids = [doc_id for doc_id, _ in q_scores[:rescore_candidates]]
        rescored_pairs = [
            (doc_id, _score_pair(vectors_by_id[doc_id], spec.query_vector, spec.metric))
            for doc_id in cand_ids
        ]
        rescored_pairs.sort(key=lambda pair: (-pair[1] if descending else pair[1], pair[0]))
        rescored_ids = [doc_id for doc_id, _ in rescored_pairs[: spec.k]]

    exact_set = set(exact_ids)
    denom = max(len(exact_set), 1)
    recall_q = len(exact_set & set(q_topk_ids)) / denom
    recall_r = len(exact_set & set(rescored_ids)) / denom

    return {
        "schema_version": COMPACT_VECTOR_EVAL_SCHEMA_VERSION,
        "scheme": "shardloom.compact_vector_int8_affine.v1",
        "metric": spec.metric,
        "k": spec.k,
        "rescore_candidates": rescore_candidates,
        "exact_topk_ids": exact_ids,
        "quantized_topk_ids": q_topk_ids,
        "rescored_topk_ids": rescored_ids,
        "recall_at_k_quantized": recall_q,
        "recall_at_k_rescored": recall_r,
        "raw_vector_bytes": raw_bytes,
        "quantized_vector_bytes": quant_bytes,
        "compression_ratio": (raw_bytes / quant_bytes) if quant_bytes > 0 else 1.0,
        "exact_globally": False,
        "rescoring_cannot_recover_omitted_candidates": True,
        "turboquant_upstream_adopted": False,
        "fallback_attempted": False,
        "external_engine_invoked": False,
    }


__all__ = (
    "AIWorkflowBoundaryError",
    "AIWorkflowError",
    "COMPACT_VECTOR_EVAL_SCHEMA_VERSION",
    "FITTED_PIPELINE_SCHEMA_VERSION",
    "FeaturePipelineSpec",
    "FittedFeaturePipeline",
    "IndexedDataset",
    "LEXICAL_INDEX_SCHEMA_VERSION",
    "LexicalIndex",
    "ModelReadyBatch",
    "SourceGenerationToken",
    "StreamingDataset",
    "TypedBatchSpec",
    "VectorRetrievalSpec",
    "as_hf_iterable_dataset",
    "as_torch_iterable_dataset",
    "as_torch_map_dataset",
    "assign_deterministic_splits",
    "deliver_typed_batches",
    "evaluate_scalar_quantized_retrieval",
    "exact_vector_search",
    "hybrid_search",
    "point_in_time_feature_join",
)
