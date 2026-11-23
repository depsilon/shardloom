# SPDX-License-Identifier: Apache-2.0
"""Optional thin native Foundry integration adapter for ShardLoom.

Foundry hosts the job, supplies authorized input handles and transaction bounds,
and owns platform dataset/Iceberg publication. ShardLoom owns query planning,
native columnar execution, explicit resource accounting, scratch/spill bounds,
and no-fallback verification.

Importing this module does not import ``pyarrow``, ``numpy``, ``pandas``,
``shardloom._native``, or any Foundry SDK, and does not read environment
variables or probe filesystems.

Local execution via duck-typed test handles does NOT constitute live Foundry
enrollment certification. Any report emitted locally sets
``live_foundry_enrollment_proved=False`` and ``claim_gate_status="not_claim_grade"``.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, Sequence
import hashlib
import inspect
import json
import math
import os
from pathlib import Path
import re
import shutil
import tempfile
import time
from typing import Any

from ._compat import dataclass
from .errors import NO_FALLBACK_REASON, ShardLoomError
from .execution_resources import (
    BYTES_PER_GIB,
    ExecutionResourceLimits,
    ExecutionResources,
    ShardLoomResourceConfigurationError,
)
from .models import Diagnostic, FallbackStatus

REPORT_SCHEMA_VERSION = "shardloom.foundry_native_transform_report.v1"
INCREMENTAL_STATE_SCHEMA_VERSION = "shardloom.foundry_incremental_state.v1"

_SAFE_SQL_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z_0-9]*$")
_SECRET_ASSIGNMENT_RE = re.compile(
    r"(?i)(\b(?:bearer\s+|authorization:\s*bearer\s+|"
    r"(?:access_)?token|secret|password|api_key|aws_secret_access_key|"
    r"foundry_token|client_secret)\s*[:=]\s*)([^\s,;\"']+)"
)
_BEARER_RE = re.compile(r"(?i)(\bbearer\s+)([A-Za-z0-9._\-+/=]+)")
_URL_USERINFO_RE = re.compile(r"(https?://)([^:@/\s]+):([^@/\s]+)@")

_ALLOWED_RESOURCE_KINDS = frozenset({"foundry_dataset", "foundry_managed_iceberg"})
_UNSUPPORTED_EXTERNAL_RESOURCE_KINDS = frozenset(
    {
        "virtual_table",
        "external_virtual_table",
        "external_pushdown",
        "snowflake",
        "databricks",
        "bigquery",
        "external_compute",
    }
)
_ALLOWED_READ_MODES = frozenset({"current", "added", "previous"})
_ALLOWED_WRITE_MODES = frozenset({"replace", "snapshot", "append"})
_ALLOWED_READ_STRATEGIES = frozenset({"auto", "staged_path", "filesystem", "arrow"})
_ALLOWED_OUTPUT_FORMATS = frozenset({"parquet", "vortex"})
_EXACT_SAFE_INT_LIMIT = 1 << 53

_FORMAT_EXTENSIONS: Mapping[str, str] = {
    "parquet": ".parquet",
    "vortex": ".vortex",
    "csv": ".csv",
    "tsv": ".tsv",
    "jsonl": ".jsonl",
    "json": ".json",
    "arrow-ipc": ".arrow",
    "arrow_ipc": ".arrow",
    "avro": ".avro",
    "orc": ".orc",
}
_EXT_TO_FORMAT: Mapping[str, str] = {
    ".parquet": "parquet",
    ".vortex": "vortex",
    ".csv": "csv",
    ".tsv": "tsv",
    ".jsonl": "jsonl",
    ".ndjson": "jsonl",
    ".json": "json",
    ".arrow": "arrow-ipc",
    ".ipc": "arrow-ipc",
    ".avro": "avro",
    ".orc": "orc",
}
_IGNORED_MARKER_NAMES = frozenset(
    {
        "_success",
        "_metadata",
        "_common_metadata",
        "_dataset_metadata.json",
        ".ds_store",
    }
)


class FoundryRoute:
    """Canonical Foundry integration routes from the October 25 / November 8 plan."""

    SINGLE_NODE_NATIVE_TRANSFORM = "single_node_native_transform"
    BYOC_TRANSFORM = "byoc_transform"
    EXTERNAL_NATIVE_WORKER = "external_native_worker"
    COMPUTE_MODULE = "compute_module"

    ALL = frozenset(
        {
            SINGLE_NODE_NATIVE_TRANSFORM,
            BYOC_TRANSFORM,
            EXTERNAL_NATIVE_WORKER,
            COMPUTE_MODULE,
        }
    )


def redact_sensitive_text(text: str) -> str:
    """Redact credentials, bearer tokens, and URL secrets from text."""

    redacted = _URL_USERINFO_RE.sub(r"\1***:***@", text)
    redacted = _SECRET_ASSIGNMENT_RE.sub(r"\1[REDACTED]", redacted)
    redacted = _BEARER_RE.sub(r"\1[REDACTED]", redacted)
    return redacted


class FoundryAdapterError(ShardLoomError, RuntimeError):
    """Explicit error raised by the ShardLoom Foundry adapter (no fallback)."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        suggested_next_step: str = "Inspect the Foundry input/output contract and retry.",
        category: str = "configuration",
    ) -> None:
        safe_message = redact_sensitive_text(message)
        super().__init__(safe_message)
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
                message=safe_message,
                feature="foundry_adapter",
                reason=safe_message,
                suggested_next_step=suggested_next_step,
                fallback=self.fallback,
            ),
        )


class FoundryBoundaryError(FoundryAdapterError):
    """Explicit unsupported, security, or platform-boundary rejection."""


@dataclass(frozen=True, slots=True)
class FoundryResourceProfile:
    """Mapped container allocation leaving explicit headroom for Python and exchange."""

    container_memory_bytes: int
    engine_memory_bytes: int
    reserved_headroom_bytes: int
    max_parallelism: int
    spill_dir: Path | None
    execution_resources: ExecutionResources
    engine_credit_is_container_ceiling: bool = False

    def as_dict(self) -> dict[str, Any]:
        return {
            "container_memory_bytes": self.container_memory_bytes,
            "engine_memory_bytes": self.engine_memory_bytes,
            "reserved_headroom_bytes": self.reserved_headroom_bytes,
            "max_parallelism": self.max_parallelism,
            "spill_dir": str(self.spill_dir) if self.spill_dir is not None else None,
            "engine_credit_is_container_ceiling": self.engine_credit_is_container_ceiling,
            "memory_origin": self.execution_resources.memory_origin,
            "parallelism_origin": self.execution_resources.parallelism_origin,
        }


def _require_pos_int(name: str, value: object) -> int:
    if type(value) is not int or value <= 0:
        raise ShardLoomResourceConfigurationError(
            f"{name} must be a positive integer; booleans, floats, and strings are rejected"
        )
    return value


def map_foundry_resources(
    *,
    max_parallelism: int,
    container_memory_gb: int | None = None,
    container_memory_bytes: int | None = None,
    python_headroom_bytes: int | None = None,
    python_headroom_fraction: float = 0.25,
    spill_dir: str | os.PathLike[str] | None = None,
    limits: ExecutionResourceLimits | None = None,
) -> FoundryResourceProfile:
    """Map an explicit container allocation into ShardLoom engine resources.

    Leaves measured room for the Python runtime, Arrow/IPC exchange, and codec
    owners, and never consults host OS memory or CPU counts.
    """

    if container_memory_gb is not None and container_memory_bytes is not None:
        raise ShardLoomResourceConfigurationError(
            "supply only one of container_memory_gb and container_memory_bytes"
        )
    if container_memory_gb is not None:
        total_bytes = _require_pos_int("container_memory_gb", container_memory_gb) * BYTES_PER_GIB
    elif container_memory_bytes is not None:
        total_bytes = _require_pos_int("container_memory_bytes", container_memory_bytes)
    else:
        raise ShardLoomResourceConfigurationError(
            "supply container_memory_gb or container_memory_bytes"
        )

    lanes = _require_pos_int("max_parallelism", max_parallelism)

    if python_headroom_bytes is not None:
        headroom = _require_pos_int("python_headroom_bytes", python_headroom_bytes)
        if headroom >= total_bytes:
            raise ShardLoomResourceConfigurationError(
                "python_headroom_bytes must be strictly less than container_memory_bytes"
            )
    else:
        if isinstance(python_headroom_fraction, bool) or not isinstance(
            python_headroom_fraction, (int, float)
        ):
            raise ShardLoomResourceConfigurationError(
                "python_headroom_fraction must be a finite number strictly between 0 and 1"
            )
        fraction = float(python_headroom_fraction)
        if not math.isfinite(fraction) or fraction <= 0.0 or fraction >= 1.0:
            raise ShardLoomResourceConfigurationError(
                "python_headroom_fraction must be strictly between 0 and 1"
            )
        min_floor = min(64 << 20, total_bytes // 4)
        headroom = max(int(total_bytes * fraction), min_floor)
        if headroom >= total_bytes or headroom <= 0:
            raise ShardLoomResourceConfigurationError(
                "computed headroom leaves zero engine memory for ShardLoom"
            )

    engine_bytes = total_bytes - headroom
    resolved_spill: Path | None = None
    if spill_dir is not None:
        if isinstance(spill_dir, bool):
            raise ShardLoomResourceConfigurationError("spill_dir must be a filesystem path")
        resolved_spill = Path(spill_dir)
        if not str(resolved_spill).strip():
            raise ShardLoomResourceConfigurationError("spill_dir must not be empty")

    execution_resources = ExecutionResources(
        engine_bytes,
        lanes,
        memory_origin="platform",
        parallelism_origin="platform",
        limits=limits,
    )
    return FoundryResourceProfile(
        container_memory_bytes=total_bytes,
        engine_memory_bytes=engine_bytes,
        reserved_headroom_bytes=headroom,
        max_parallelism=lanes,
        spill_dir=resolved_spill,
        execution_resources=execution_resources,
        engine_credit_is_container_ceiling=False,
    )


def _validate_resource_kind(resource_kind: str) -> str:
    normalized = str(resource_kind).strip().lower()
    if normalized in _UNSUPPORTED_EXTERNAL_RESOURCE_KINDS:
        raise FoundryBoundaryError(
            "SL_FOUNDRY_UNSUPPORTED_RESOURCE",
            f"resource_kind={normalized!r} represents an external table/pushdown boundary, "
            "not ShardLoom-native execution; materialize or stage data first",
            suggested_next_step=(
                "Use an authorized 'foundry_dataset' or 'foundry_managed_iceberg' handle."
            ),
        )
    if normalized not in _ALLOWED_RESOURCE_KINDS:
        raise FoundryBoundaryError(
            "SL_FOUNDRY_INVALID_RESOURCE",
            f"unsupported Foundry resource_kind={resource_kind!r}; expected one of {sorted(_ALLOWED_RESOURCE_KINDS)}",
        )
    return normalized


def _validate_alias(alias: str) -> str:
    if not isinstance(alias, str) or _SAFE_SQL_IDENTIFIER_RE.fullmatch(alias) is None:
        raise FoundryBoundaryError(
            "SL_FOUNDRY_INVALID_ALIAS",
            f"input alias {alias!r} must be a valid SQL identifier",
        )
    return alias.lower()


@dataclass(frozen=True, slots=True)
class FoundryInputSpec:
    """Declared Foundry input handle (LightweightInput, TableInput, or IcebergInput)."""

    alias: str
    handle: Any
    resource_kind: str = "foundry_dataset"
    input_format: str | None = None
    read_mode: str = "current"
    read_strategy: str = "auto"
    schema: Mapping[str, object] | None = None
    rid: str | None = None
    branch: str | None = None
    transaction_rid: str | None = None
    snapshot_id: str | int | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "alias", _validate_alias(self.alias))
        object.__setattr__(self, "resource_kind", _validate_resource_kind(self.resource_kind))
        mode = str(self.read_mode).strip().lower()
        if mode == "changelog":
            raise FoundryBoundaryError(
                "SL_FOUNDRY_UNSUPPORTED_READ_MODE",
                "changelog incremental read_mode is not supported on single-node Foundry/Iceberg handles; "
                "use explicit 'current', 'added', or 'previous' read modes",
                suggested_next_step="Switch read_mode to 'current', 'added', or 'previous'.",
            )
        if mode not in _ALLOWED_READ_MODES:
            raise FoundryBoundaryError(
                "SL_FOUNDRY_INVALID_READ_MODE",
                f"invalid read_mode={self.read_mode!r}; expected one of {sorted(_ALLOWED_READ_MODES)}",
            )
        object.__setattr__(self, "read_mode", mode)

        strategy = str(self.read_strategy).strip().lower()
        if strategy not in _ALLOWED_READ_STRATEGIES:
            raise FoundryBoundaryError(
                "SL_FOUNDRY_INVALID_READ_STRATEGY",
                f"invalid read_strategy={self.read_strategy!r}; expected one of {sorted(_ALLOWED_READ_STRATEGIES)}",
            )
        object.__setattr__(self, "read_strategy", strategy)

        if self.input_format is not None:
            fmt = str(self.input_format).strip().lower().replace("_", "-")
            if fmt not in _FORMAT_EXTENSIONS:
                raise FoundryBoundaryError(
                    "SL_FOUNDRY_INVALID_FORMAT",
                    f"unsupported input_format={self.input_format!r}",
                )
            object.__setattr__(self, "input_format", fmt)


@dataclass(frozen=True, slots=True)
class FoundryOutputSpec:
    """Declared Foundry output handle (LightweightOutput, TableOutput, or IcebergOutput)."""

    handle: Any
    output_format: str = "parquet"
    write_mode: str = "replace"
    resource_kind: str = "foundry_dataset"
    rid: str | None = None
    branch: str | None = None
    transaction_rid: str | None = None
    target_filename: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "resource_kind", _validate_resource_kind(self.resource_kind))
        fmt = str(self.output_format).strip().lower().replace("_", "-")
        if fmt not in _ALLOWED_OUTPUT_FORMATS:
            raise FoundryBoundaryError(
                "SL_FOUNDRY_UNSUPPORTED_OUTPUT_FORMAT",
                f"unsupported Foundry output_format={self.output_format!r}; "
                f"expected one of {sorted(_ALLOWED_OUTPUT_FORMATS)}",
            )
        object.__setattr__(self, "output_format", fmt)

        mode = str(self.write_mode).strip().lower()
        if mode == "changelog":
            raise FoundryBoundaryError(
                "SL_FOUNDRY_UNSUPPORTED_WRITE_MODE",
                "changelog incremental write_mode is unavailable for Foundry single-node/Iceberg transforms; "
                "use 'replace', 'snapshot', or 'append'",
            )
        if mode not in _ALLOWED_WRITE_MODES:
            raise FoundryBoundaryError(
                "SL_FOUNDRY_INVALID_WRITE_MODE",
                f"invalid write_mode={self.write_mode!r}; expected one of {sorted(_ALLOWED_WRITE_MODES)}",
            )
        object.__setattr__(self, "write_mode", mode)


@dataclass(frozen=True, slots=True)
class BoundFoundryInput:
    """Bound lazy ShardLoom relation alongside verified Foundry provenance metadata."""

    alias: str
    frame: Any
    resource_kind: str
    read_mode: str
    read_strategy: str
    input_format: str
    physical_file_count: int
    logical_relation_bound: bool
    materialization_boundary: str
    rid: str | None
    branch: str | None
    transaction_rid: str | None
    snapshot_id: str | int | None

    def metadata_dict(self) -> dict[str, Any]:
        return {
            "alias": self.alias,
            "resource_kind": self.resource_kind,
            "read_mode": self.read_mode,
            "read_strategy": self.read_strategy,
            "input_format": self.input_format,
            "physical_file_count": self.physical_file_count,
            "logical_relation_bound": self.logical_relation_bound,
            "materialization_boundary": self.materialization_boundary,
            "rid": self.rid,
            "branch": self.branch,
            "transaction_rid": self.transaction_rid,
            "snapshot_id": self.snapshot_id,
        }


def _extract_attr(handle: Any, *names: str) -> Any:
    for name in names:
        if not hasattr(handle, name):
            continue
        attr = getattr(handle, name)
        if callable(attr):
            try:
                sig = inspect.signature(attr)
                if all(
                    param.default is not inspect.Parameter.empty
                    for param in sig.parameters.values()
                ):
                    val = attr()
                    if val is not None:
                        return val
            except (TypeError, ValueError):
                continue
        elif attr is not None:
            return attr
    return None


def _call_with_optional_mode(handle: Any, method_name: str, read_mode: str) -> Any:
    method = getattr(handle, method_name)
    try:
        sig = inspect.signature(method)
        params = sig.parameters
        accepts_mode = "mode" in params or any(
            p.kind == inspect.Parameter.VAR_KEYWORD for p in params.values()
        )
    except (TypeError, ValueError):
        accepts_mode = False

    try:
        return method(mode=read_mode) if accepts_mode else method()
    except PermissionError as error:
        raise FoundryBoundaryError(
            "SL_FOUNDRY_INPUT_ACCESS_DENIED",
            f"authorized access denied while reading handle.{method_name}(): {error}",
            suggested_next_step="Verify dataset/table permissions on the declared input handle.",
            category="security",
        ) from None
    except FoundryAdapterError:
        raise
    except Exception as error:
        safe = redact_sensitive_text(str(error))
        raise FoundryBoundaryError(
            "SL_FOUNDRY_INPUT_ERROR",
            f"failed to read Foundry input handle via {method_name}(): {safe}",
            category="execution",
        ) from None


def _infer_format_from_paths(paths: Sequence[Path], explicit: str | None) -> str:
    if explicit is not None:
        return explicit
    detected: set[str] = set()
    for path in paths:
        ext = path.suffix.lower()
        if ext in _EXT_TO_FORMAT:
            detected.add(_EXT_TO_FORMAT[ext])
    if len(detected) == 1:
        return next(iter(detected))
    if not detected:
        raise FoundryBoundaryError(
            "SL_FOUNDRY_UNKNOWN_FORMAT",
            "cannot infer input format from staged paths; pass explicit input_format=",
        )
    raise FoundryBoundaryError(
        "SL_FOUNDRY_MIXED_FORMAT",
        f"input directory contains mixed file formats {sorted(detected)}; pass explicit input_format=",
    )


def _is_ignored_entry(name: str) -> bool:
    lowered = name.lower()
    return (
        lowered in _IGNORED_MARKER_NAMES
        or lowered.startswith(".")
        or lowered.startswith("_started_")
        or lowered.startswith("_committed_")
        or lowered.endswith(".crc")
    )


def _list_filesystem_files(fs: Any, read_mode: str) -> list[str]:
    if not hasattr(fs, "ls") or not callable(fs.ls):
        raise FoundryBoundaryError(
            "SL_FOUNDRY_FILESYSTEM_UNSUPPORTED",
            "handle.filesystem() does not expose ls()",
        )
    entries = _call_with_optional_mode(fs, "ls", read_mode)
    relpaths: list[str] = []
    for entry in entries:
        if isinstance(entry, str):
            rel = entry
        elif hasattr(entry, "path"):
            rel = str(entry.path)
        else:
            rel = str(entry)
        rel = rel.lstrip("/")
        base = Path(rel).name
        if not base or _is_ignored_entry(base):
            continue
        if rel.endswith("/"):
            continue
        relpaths.append(rel)
    relpaths.sort()
    return relpaths


def _stage_file_hardlink_or_copy(src: Path, dst: Path) -> None:
    try:
        os.link(src, dst)
    except OSError:
        shutil.copyfile(src, dst)


def bind_foundry_input(
    ctx: Any,
    spec: FoundryInputSpec,
    *,
    scratch_dir: Path,
) -> BoundFoundryInput:
    """Bind a Foundry input handle into one logical ShardLoom LazyFrame."""

    if not isinstance(spec, FoundryInputSpec):
        raise TypeError("spec must be a FoundryInputSpec")
    handle = spec.handle

    rid = spec.rid if spec.rid is not None else _extract_attr(handle, "rid", "dataset_rid", "table_rid")
    branch = spec.branch if spec.branch is not None else _extract_attr(handle, "branch")
    transaction_rid = (
        spec.transaction_rid
        if spec.transaction_rid is not None
        else _extract_attr(handle, "end_transaction_rid", "transaction_rid")
    )
    snapshot_id = spec.snapshot_id if spec.snapshot_id is not None else _extract_attr(handle, "snapshot_id")

    strategy = spec.read_strategy
    if strategy == "auto":
        if hasattr(handle, "path") and callable(handle.path):
            strategy = "staged_path"
        elif hasattr(handle, "filesystem") and callable(handle.filesystem):
            strategy = "filesystem"
        elif hasattr(handle, "arrow") and callable(handle.arrow):
            strategy = "arrow"
        else:
            raise FoundryBoundaryError(
                "SL_FOUNDRY_UNSUPPORTED_HANDLE",
                "input handle must expose path(), filesystem(), or arrow()",
            )

    if strategy == "staged_path":
        if not hasattr(handle, "path") or not callable(handle.path):
            raise FoundryBoundaryError("SL_FOUNDRY_UNSUPPORTED_HANDLE", "input handle lacks path()")
        raw_path = _call_with_optional_mode(handle, "path", spec.read_mode)
        path = Path(raw_path)
        if not path.exists():
            raise FoundryBoundaryError(
                "SL_FOUNDRY_INPUT_MISSING",
                f"staged input path does not exist: {path}",
            )
        if path.is_file():
            fmt = _infer_format_from_paths([path], spec.input_format)
            frame = ctx.read(path, input_format=fmt, schema=spec.schema)
            return BoundFoundryInput(
                alias=spec.alias,
                frame=frame,
                resource_kind=spec.resource_kind,
                read_mode=spec.read_mode,
                read_strategy=strategy,
                input_format=fmt,
                physical_file_count=1,
                logical_relation_bound=True,
                materialization_boundary="staged_local_file",
                rid=str(rid) if rid is not None else None,
                branch=str(branch) if branch is not None else None,
                transaction_rid=str(transaction_rid) if transaction_rid is not None else None,
                snapshot_id=snapshot_id,
            )

        all_files = sorted(
            p
            for p in path.rglob("*")
            if p.is_file() and not _is_ignored_entry(p.name)
        )
        if spec.input_format is not None:
            expected_ext = _FORMAT_EXTENSIONS[spec.input_format]
            admitted = [p for p in all_files if p.suffix.lower() == expected_ext]
            fmt = spec.input_format
        else:
            known = [p for p in all_files if p.suffix.lower() in _EXT_TO_FORMAT]
            fmt = _infer_format_from_paths(known, None)
            expected_ext = _FORMAT_EXTENSIONS[fmt]
            admitted = [p for p in known if p.suffix.lower() == expected_ext]

        if not admitted:
            raise FoundryBoundaryError(
                "SL_FOUNDRY_EMPTY_DATASET_FILES",
                f"no admitted {fmt} files found under directory {path}",
            )

        has_nested = any(p.parent != path for p in admitted)
        if not has_nested:
            source_target = path
        else:
            stage_root = scratch_dir / "inputs" / spec.alias
            stage_root.mkdir(parents=True, exist_ok=True)
            for idx, src_file in enumerate(admitted):
                _stage_file_hardlink_or_copy(src_file, stage_root / f"part-{idx:05d}{expected_ext}")
            source_target = stage_root

        frame = ctx.read(source_target, input_format=fmt, schema=spec.schema)
        return BoundFoundryInput(
            alias=spec.alias,
            frame=frame,
            resource_kind=spec.resource_kind,
            read_mode=spec.read_mode,
            read_strategy=strategy,
            input_format=fmt,
            physical_file_count=len(admitted),
            logical_relation_bound=True,
            materialization_boundary="staged_multi_file_logical_relation",
            rid=str(rid) if rid is not None else None,
            branch=str(branch) if branch is not None else None,
            transaction_rid=str(transaction_rid) if transaction_rid is not None else None,
            snapshot_id=snapshot_id,
        )

    if strategy == "filesystem":
        if not hasattr(handle, "filesystem") or not callable(handle.filesystem):
            raise FoundryBoundaryError(
                "SL_FOUNDRY_UNSUPPORTED_HANDLE", "input handle lacks filesystem()"
            )
        fs = _call_with_optional_mode(handle, "filesystem", spec.read_mode)
        relpaths = _list_filesystem_files(fs, spec.read_mode)
        if spec.input_format is not None:
            expected_ext = _FORMAT_EXTENSIONS[spec.input_format]
            admitted_rels = [r for r in relpaths if Path(r).suffix.lower() == expected_ext]
            fmt = spec.input_format
        else:
            known_rels = [r for r in relpaths if Path(r).suffix.lower() in _EXT_TO_FORMAT]
            fmt = _infer_format_from_paths([Path(r) for r in known_rels], None)
            expected_ext = _FORMAT_EXTENSIONS[fmt]
            admitted_rels = [r for r in known_rels if Path(r).suffix.lower() == expected_ext]

        if not admitted_rels:
            raise FoundryBoundaryError(
                "SL_FOUNDRY_EMPTY_DATASET_FILES",
                f"no admitted {fmt} files found in Foundry handle filesystem()",
            )
        stage_root = scratch_dir / "inputs" / spec.alias
        stage_root.mkdir(parents=True, exist_ok=True)
        for idx, rel in enumerate(admitted_rels):
            dst = stage_root / f"part-{idx:05d}{expected_ext}"
            try:
                stream = fs.open(rel, "rb")
            except PermissionError as error:
                raise FoundryBoundaryError(
                    "SL_FOUNDRY_INPUT_ACCESS_DENIED",
                    f"permission denied opening {rel}: {error}",
                    category="security",
                ) from None
            except Exception as error:
                safe = redact_sensitive_text(str(error))
                raise FoundryBoundaryError(
                    "SL_FOUNDRY_INPUT_ERROR",
                    f"failed opening {rel} from handle.filesystem(): {safe}",
                    category="execution",
                ) from None
            with stream as reader, dst.open("wb") as writer:
                shutil.copyfileobj(reader, writer)
                writer.flush()
                os.fsync(writer.fileno())

        frame = ctx.read(stage_root, input_format=fmt, schema=spec.schema)
        return BoundFoundryInput(
            alias=spec.alias,
            frame=frame,
            resource_kind=spec.resource_kind,
            read_mode=spec.read_mode,
            read_strategy=strategy,
            input_format=fmt,
            physical_file_count=len(admitted_rels),
            logical_relation_bound=True,
            materialization_boundary="filesystem_staged_logical_relation",
            rid=str(rid) if rid is not None else None,
            branch=str(branch) if branch is not None else None,
            transaction_rid=str(transaction_rid) if transaction_rid is not None else None,
            snapshot_id=snapshot_id,
        )

    # strategy == "arrow"
    if not hasattr(handle, "arrow") or not callable(handle.arrow):
        raise FoundryBoundaryError("SL_FOUNDRY_UNSUPPORTED_HANDLE", "input handle lacks arrow()")
    table = _call_with_optional_mode(handle, "arrow", spec.read_mode)
    frame = ctx.from_arrow_table(table, schema=spec.schema)
    return BoundFoundryInput(
        alias=spec.alias,
        frame=frame,
        resource_kind=spec.resource_kind,
        read_mode=spec.read_mode,
        read_strategy=strategy,
        input_format="arrow",
        physical_file_count=1,
        logical_relation_bound=True,
        materialization_boundary="lightweight_input_arrow_full_view",
        rid=str(rid) if rid is not None else None,
        branch=str(branch) if branch is not None else None,
        transaction_rid=str(transaction_rid) if transaction_rid is not None else None,
        snapshot_id=snapshot_id,
    )


def _atomic_copy_file(src: Path, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=".stage-", suffix=dst.suffix, dir=dst.parent)
    tmp_path = Path(tmp_name)
    try:
        with os.fdopen(fd, "wb") as out_fh, src.open("rb") as in_fh:
            shutil.copyfileobj(in_fh, out_fh)
            out_fh.flush()
            os.fsync(out_fh.fileno())
        os.replace(tmp_path, dst)
    finally:
        tmp_path.unlink(missing_ok=True)


def _next_part_index(existing_names: Iterable[str], ext: str) -> int:
    max_idx = -1
    for name in existing_names:
        base = Path(name).name
        stem = base[: -len(ext)] if base.endswith(ext) else Path(base).stem
        if stem.startswith("part-"):
            suffix = stem[5:]
            if suffix.isdigit():
                max_idx = max(max_idx, int(suffix))
    return max_idx + 1


def _write_staged_to_destination_path(
    staged_file: Path,
    dest_path: Path,
    *,
    write_mode: str,
    ext: str,
) -> None:
    if dest_path.suffix.lower() == ext:
        _atomic_copy_file(staged_file, dest_path)
        return
    dest_path.mkdir(parents=True, exist_ok=True)
    existing_parts = sorted(dest_path.glob(f"part-*{ext}"))
    if write_mode == "append":
        next_idx = _next_part_index((p.name for p in existing_parts), ext)
        _atomic_copy_file(staged_file, dest_path / f"part-{next_idx:05d}{ext}")
        return
    target = dest_path / f"part-00000{ext}"
    _atomic_copy_file(staged_file, target)
    for old_part in existing_parts:
        if old_part != target and old_part.is_file():
            old_part.unlink(missing_ok=True)


@dataclass(frozen=True, slots=True)
class FoundryPublishedOutput:
    """Publication summary emitted only after complete staging and handle commit."""

    resource_kind: str
    output_format: str
    write_mode: str
    row_count: int
    schema_fields: tuple[tuple[str, str], ...]
    commit_status: str
    general_foundry_tabular_support: bool
    engine_nanos: int
    publication_nanos: int
    rid: str | None
    branch: str | None
    transaction_rid: str | None

    def as_dict(self) -> dict[str, Any]:
        return {
            "resource_kind": self.resource_kind,
            "output_format": self.output_format,
            "write_mode": self.write_mode,
            "row_count": self.row_count,
            "schema_fields": [
                {"name": name, "type": dtype} for name, dtype in self.schema_fields
            ],
            "commit_status": self.commit_status,
            "general_foundry_tabular_support": self.general_foundry_tabular_support,
            "rid": self.rid,
            "branch": self.branch,
            "transaction_rid": self.transaction_rid,
        }


def _read_staged_schema_and_table(staged_file: Path, fmt: str) -> tuple[tuple[tuple[str, str], ...], Any]:
    if fmt == "parquet":
        import pyarrow.parquet as pq

        table = pq.read_table(staged_file)
        schema_fields = tuple((field.name, str(field.type)) for field in table.schema)
        return schema_fields, table
    return (), None


def publish_foundry_output(
    frame: Any,
    spec: FoundryOutputSpec,
    *,
    scratch_dir: Path,
    check: bool = True,
) -> FoundryPublishedOutput:
    """Execute a LazyFrame once to scratch and publish failure-safely to a Foundry handle."""

    if not isinstance(spec, FoundryOutputSpec):
        raise TypeError("spec must be a FoundryOutputSpec")
    handle = spec.handle
    ext = _FORMAT_EXTENSIONS[spec.output_format]

    rid = spec.rid if spec.rid is not None else _extract_attr(handle, "rid", "dataset_rid", "table_rid")
    branch = spec.branch if spec.branch is not None else _extract_attr(handle, "branch")
    transaction_rid = (
        spec.transaction_rid
        if spec.transaction_rid is not None
        else _extract_attr(handle, "end_transaction_rid", "transaction_rid")
    )

    attempt_dir = Path(tempfile.mkdtemp(prefix="publish-", dir=scratch_dir))
    try:
        staged_file = attempt_dir / (spec.target_filename or f"output{ext}")

        engine_start = time.monotonic_ns()
        if spec.output_format == "parquet":
            write_res = frame.write_parquet(staged_file, check=check)
        else:
            write_res = frame.write_vortex(staged_file, check=check)
        engine_nanos = time.monotonic_ns() - engine_start

        if not staged_file.is_file() or staged_file.stat().st_size == 0:
            raise FoundryBoundaryError(
                "SL_FOUNDRY_STAGED_OUTPUT_INVALID",
                "ShardLoom did not produce a valid non-empty staged output artifact",
                category="execution",
            )

        schema_fields, arrow_table = _read_staged_schema_and_table(staged_file, spec.output_format)
        row_count = getattr(write_res, "output_row_count", None)
        if row_count is None:
            row_count = arrow_table.num_rows if arrow_table is not None else 0

        pub_start = time.monotonic_ns()
        try:
            if hasattr(handle, "set_mode") and callable(handle.set_mode):
                handle.set_mode("replace" if spec.write_mode == "snapshot" else spec.write_mode)

            if spec.output_format == "parquet":
                if hasattr(handle, "write_table_from_path") and callable(handle.write_table_from_path):
                    handle.write_table_from_path(staged_file)
                elif hasattr(handle, "path_for_write_table") and callable(handle.path_for_write_table):
                    target = handle.path_for_write_table()
                    if hasattr(target, "__enter__") and hasattr(target, "__exit__"):
                        with target as dest_path:
                            _write_staged_to_destination_path(
                                staged_file,
                                Path(dest_path),
                                write_mode=spec.write_mode,
                                ext=ext,
                            )
                    else:
                        _write_staged_to_destination_path(
                            staged_file,
                            Path(target),
                            write_mode=spec.write_mode,
                            ext=ext,
                        )
                elif hasattr(handle, "write_table") and callable(handle.write_table):
                    if getattr(handle, "accepts_staged_path", False):
                        handle.write_table(staged_file)
                    else:
                        handle.write_table(arrow_table)
                else:
                    raise FoundryBoundaryError(
                        "SL_FOUNDRY_UNSUPPORTED_OUTPUT_HANDLE",
                        "output handle must expose write_table(), path_for_write_table(), or write_table_from_path()",
                    )
            else:
                if hasattr(handle, "write_vortex_file") and callable(handle.write_vortex_file):
                    handle.write_vortex_file(staged_file)
                elif hasattr(handle, "path_for_write_table") and callable(handle.path_for_write_table):
                    target = handle.path_for_write_table()
                    if hasattr(target, "__enter__") and hasattr(target, "__exit__"):
                        with target as dest_path:
                            _write_staged_to_destination_path(
                                staged_file,
                                Path(dest_path),
                                write_mode=spec.write_mode,
                                ext=ext,
                            )
                    else:
                        _write_staged_to_destination_path(
                            staged_file,
                            Path(target),
                            write_mode=spec.write_mode,
                            ext=ext,
                        )
                elif hasattr(handle, "filesystem") and callable(handle.filesystem):
                    fs = handle.filesystem()
                    if spec.target_filename is not None:
                        target_name = spec.target_filename
                    elif spec.write_mode == "append" and hasattr(fs, "ls") and callable(fs.ls):
                        existing = _list_filesystem_files(fs, "current")
                        next_idx = _next_part_index(existing, ".vortex")
                        target_name = f"part-{next_idx:05d}.vortex"
                    else:
                        target_name = "part-00000.vortex"
                    with fs.open(target_name, "wb") as out_fh, staged_file.open("rb") as in_fh:
                        shutil.copyfileobj(in_fh, out_fh)
                else:
                    raise FoundryBoundaryError(
                        "SL_FOUNDRY_UNSUPPORTED_OUTPUT_HANDLE",
                        "Vortex output handle must expose write_vortex_file(), path_for_write_table(), or filesystem()",
                    )

            if hasattr(handle, "commit") and callable(handle.commit):
                handle.commit()
        except PermissionError as error:
            raise FoundryBoundaryError(
                "SL_FOUNDRY_OUTPUT_ACCESS_DENIED",
                f"permission denied publishing Foundry output: {error}",
                category="security",
            ) from None
        except FoundryAdapterError:
            raise
        except Exception as error:
            safe = redact_sensitive_text(str(error))
            raise FoundryBoundaryError(
                "SL_FOUNDRY_PUBLICATION_FAILED",
                f"Foundry output publication failed: {safe}",
                category="execution",
            ) from None
        publication_nanos = time.monotonic_ns() - pub_start

        return FoundryPublishedOutput(
            resource_kind=spec.resource_kind,
            output_format=spec.output_format,
            write_mode=spec.write_mode,
            row_count=int(row_count),
            schema_fields=schema_fields,
            commit_status="committed",
            general_foundry_tabular_support=(spec.output_format != "vortex"),
            engine_nanos=engine_nanos,
            publication_nanos=publication_nanos,
            rid=str(rid) if rid is not None else None,
            branch=str(branch) if branch is not None else None,
            transaction_rid=str(transaction_rid) if transaction_rid is not None else None,
        )
    finally:
        shutil.rmtree(attempt_dir, ignore_errors=True)


@dataclass(frozen=True, slots=True)
class FoundryTransformReport:
    """Structured local Foundry transform report with truthful host/engine separation."""

    schema_version: str
    shardloom_version: str
    transport: str
    route: str
    execution_host: str
    query_engine: str
    resource_allocation: dict[str, Any]
    inputs: tuple[dict[str, Any], ...]
    output: dict[str, Any]
    timings_nanos: dict[str, int]
    scratch_cleaned: bool
    incremental: dict[str, Any] | None = None
    fallback_attempted: bool = False
    external_engine_invoked: bool = False
    foundry_spark_invoked: bool = False
    foundry_runtime_invoked: bool = False
    foundry_compute_invoked: bool = False
    live_foundry_enrollment_proved: bool = False
    claim_gate_status: str = "not_claim_grade"

    def as_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "schema_version": self.schema_version,
            "shardloom_version": self.shardloom_version,
            "transport": self.transport,
            "route": self.route,
            "execution_host": self.execution_host,
            "query_engine": self.query_engine,
            "resource_allocation": self.resource_allocation,
            "inputs": list(self.inputs),
            "output": self.output,
            "timings_nanos": self.timings_nanos,
            "scratch_cleaned": self.scratch_cleaned,
            "fallback_attempted": self.fallback_attempted,
            "external_engine_invoked": self.external_engine_invoked,
            "foundry_spark_invoked": self.foundry_spark_invoked,
            "foundry_runtime_invoked": self.foundry_runtime_invoked,
            "foundry_compute_invoked": self.foundry_compute_invoked,
            "live_foundry_enrollment_proved": self.live_foundry_enrollment_proved,
            "claim_gate_status": self.claim_gate_status,
        }
        if self.incremental is not None:
            payload["incremental"] = self.incremental
        return payload


def _create_scratch_root(profile: FoundryResourceProfile) -> tuple[Path, Callable[[], None]]:
    if profile.spill_dir is not None:
        profile.spill_dir.mkdir(parents=True, exist_ok=True)
        parent = profile.spill_dir
    else:
        parent = None
    scratch_dir = Path(tempfile.mkdtemp(prefix="shardloom-foundry-job-", dir=parent))
    return scratch_dir, lambda: shutil.rmtree(scratch_dir, ignore_errors=True)


def execute_foundry_transform(
    *,
    inputs: Sequence[FoundryInputSpec],
    output: FoundryOutputSpec,
    resources: FoundryResourceProfile,
    sql: str | None = None,
    transform: Callable[[Any, Mapping[str, Any]], Any] | None = None,
    transport: str = "native",
    route: str = FoundryRoute.SINGLE_NODE_NATIVE_TRANSFORM,
    execution_host: str = "local_foundry_adapter_fixture",
    binary: str | os.PathLike[str] | None = None,
    repo_root: str | os.PathLike[str] | None = None,
    check: bool = True,
) -> FoundryTransformReport:
    """Execute one single-node Foundry transform backed natively by ShardLoom."""

    if route not in FoundryRoute.ALL:
        raise FoundryBoundaryError(
            "SL_FOUNDRY_INVALID_ROUTE",
            f"unknown Foundry route {route!r}; expected one of {sorted(FoundryRoute.ALL)}",
        )
    if (sql is None) == (transform is None):
        raise FoundryBoundaryError(
            "SL_FOUNDRY_INVALID_TRANSFORM",
            "supply exactly one of sql= or transform=",
        )
    if not inputs:
        raise FoundryBoundaryError(
            "SL_FOUNDRY_MISSING_INPUTS",
            "at least one FoundryInputSpec is required",
        )
    if not isinstance(resources, FoundryResourceProfile):
        raise TypeError("resources must be a FoundryResourceProfile from map_foundry_resources()")

    from . import __version__ as shardloom_version
    from .context import context as open_shardloom_context

    job_start = time.monotonic_ns()
    scratch_dir, cleanup_scratch = _create_scratch_root(resources)
    try:
        io_start = time.monotonic_ns()
        with open_shardloom_context(
            transport=transport,
            binary=binary,
            repo_root=repo_root,
            resources=resources.execution_resources,
        ) as ctx:
            bound_inputs: list[BoundFoundryInput] = []
            relations: dict[str, Any] = {}
            for spec in inputs:
                bound = bind_foundry_input(ctx, spec, scratch_dir=scratch_dir)
                if bound.alias in relations:
                    raise FoundryBoundaryError(
                        "SL_FOUNDRY_DUPLICATE_ALIAS",
                        f"duplicate input alias {bound.alias!r}",
                    )
                bound_inputs.append(bound)
                relations[bound.alias] = bound.frame
            io_nanos = time.monotonic_ns() - io_start

            build_start = time.monotonic_ns()
            if sql is not None:
                result_frame = ctx.sql(sql, relations=relations)
            else:
                result_frame = transform(ctx, relations)  # type: ignore[misc]
            build_nanos = time.monotonic_ns() - build_start

            published = publish_foundry_output(
                result_frame,
                output,
                scratch_dir=scratch_dir,
                check=check,
            )
    finally:
        cleanup_scratch()

    total_nanos = time.monotonic_ns() - job_start
    return FoundryTransformReport(
        schema_version=REPORT_SCHEMA_VERSION,
        shardloom_version=shardloom_version,
        transport=transport,
        route=route,
        execution_host=execution_host,
        query_engine="shardloom_native",
        resource_allocation=resources.as_dict(),
        inputs=tuple(item.metadata_dict() for item in bound_inputs),
        output=published.as_dict(),
        timings_nanos={
            "io_and_conversion_nanos": io_nanos + build_nanos,
            "engine_nanos": published.engine_nanos,
            "publication_nanos": published.publication_nanos,
            "total_job_nanos": total_nanos,
        },
        scratch_cleaned=not scratch_dir.exists(),
    )


# ---------------------------------------------------------------------------
# Stateful logical incremental & managed-Iceberg maintenance
# ---------------------------------------------------------------------------

_UNSUPPORTED_INCREMENTAL_OPERATORS = frozenset(
    {"min", "max", "avg", "mean", "median", "stddev", "variance"}
)


@dataclass(frozen=True, slots=True)
class IncrementalAggregateSpec:
    """Contract for exact maintained grouped COUNT(*) and exact integer SUM(col)."""

    source_rid: str
    group_by: tuple[str, ...]
    sum_columns: tuple[str, ...] = ()
    branch: str = "master"
    count_column: str = "row_count"
    sum_aliases: Mapping[str, str] | None = None
    delta_op_column: str | None = None
    retracted_operators: tuple[str, ...] = ()
    on_gap: str = "fail"
    operator_version: str = "exact_grouped_count_int_sum.v1"

    def __post_init__(self) -> None:
        if not self.source_rid or not isinstance(self.source_rid, str):
            raise FoundryBoundaryError("SL_INCREMENTAL_INVALID_SPEC", "source_rid must be non-empty")
        if not self.group_by:
            raise FoundryBoundaryError(
                "SL_INCREMENTAL_INVALID_SPEC",
                "IncrementalAggregateSpec requires at least one group_by column",
            )
        for col in self.group_by:
            _validate_alias(col)
        for col in self.sum_columns:
            _validate_alias(col)
        _validate_alias(self.count_column)
        if self.delta_op_column is not None:
            _validate_alias(self.delta_op_column)
        for op in self.retracted_operators:
            if op.strip().lower() in _UNSUPPORTED_INCREMENTAL_OPERATORS:
                raise FoundryBoundaryError(
                    "SL_INCREMENTAL_UNSUPPORTED_OPERATOR",
                    f"incremental delta retraction is unsupported for operator {op!r}; "
                    "remove/retract requires full native recomputation",
                )
        if self.on_gap not in {"fail", "recompute"}:
            raise FoundryBoundaryError(
                "SL_INCREMENTAL_INVALID_SPEC",
                f"invalid on_gap={self.on_gap!r}; expected 'fail' or 'recompute'",
            )

    def output_sum_name(self, col: str) -> str:
        if self.sum_aliases and col in self.sum_aliases:
            return self.sum_aliases[col]
        return f"sum_{col}"


class IncrementalCheckpointStore:
    """Atomic file-backed checkpoint state for Foundry incremental transforms."""

    def __init__(self, checkpoint_path: str | os.PathLike[str]) -> None:
        self.path = Path(checkpoint_path)

    def load(self) -> dict[str, Any] | None:
        if not self.path.exists():
            return None
        payload = json.loads(self.path.read_text(encoding="utf-8"))
        if payload.get("schema_version") != INCREMENTAL_STATE_SCHEMA_VERSION:
            raise FoundryBoundaryError(
                "SL_INCREMENTAL_CORRUPT_CHECKPOINT",
                f"unexpected checkpoint schema_version={payload.get('schema_version')!r}",
            )
        return payload

    def save_atomic(self, state: Mapping[str, Any]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        serialized = json.dumps(state, sort_keys=True, indent=2) + "\n"
        fd, tmp_name = tempfile.mkstemp(
            prefix=".ckpt-", suffix=".json", dir=self.path.parent
        )
        tmp_path = Path(tmp_name)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                handle.write(serialized)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp_path, self.path)
        finally:
            tmp_path.unlink(missing_ok=True)


def _group_key_json(group_values: Sequence[Any]) -> str:
    return json.dumps(list(group_values), separators=(",", ":"), sort_keys=False)


def _decode_group_key_json(raw: str) -> list[Any]:
    return json.loads(raw)


def _check_exact_int_value(col: str, val: object) -> int | None:
    if val is None:
        return None
    if isinstance(val, bool):
        raise FoundryBoundaryError(
            "SL_INCREMENTAL_UNSUPPORTED_TYPE",
            f"column {col!r} cannot use boolean values for exact integer sum maintenance",
        )
    if isinstance(val, int):
        if abs(val) > _EXACT_SAFE_INT_LIMIT:
            raise FoundryBoundaryError(
                "SL_INCREMENTAL_PRECISION_BOUND",
                f"integer sum column {col!r} exceeded exact 2^53 boundary during SQL evaluation",
            )
        return val
    if isinstance(val, float):
        if not math.isfinite(val) or not val.is_integer():
            raise FoundryBoundaryError(
                "SL_INCREMENTAL_UNSUPPORTED_TYPE",
                f"floating-point sum value in column {col!r} prohibits direct delta maintenance",
            )
        if abs(val) > _EXACT_SAFE_INT_LIMIT:
            raise FoundryBoundaryError(
                "SL_INCREMENTAL_PRECISION_BOUND",
                f"integer sum column {col!r} exceeded exact 2^53 boundary during SQL evaluation",
            )
        return int(val)
    raise FoundryBoundaryError(
        "SL_INCREMENTAL_UNSUPPORTED_TYPE",
        f"non-integer sum column {col!r} (type {type(val).__name__}) prohibits direct delta maintenance",
    )


def _validate_delta_input_sum_types(ctx: Any, frame: Any, spec: IncrementalAggregateSpec) -> None:
    if not spec.sum_columns:
        return
    # Inspect empty limit(0) Arrow schema cleanly through ShardLoom without full collection.
    sample = frame.limit(0).collect(check=True).to_arrow()
    import pyarrow.types as pat

    for col in spec.sum_columns:
        field = sample.schema.field(col)
        if not pat.is_integer(field.type):
            raise FoundryBoundaryError(
                "SL_INCREMENTAL_UNSUPPORTED_TYPE",
                f"incremental integer sum column {col!r} has type {field.type}; "
                "floating-point and decimal subtraction require full native recomputation",
            )


def _aggregate_table_delta_via_shardloom(
    ctx: Any,
    frame: Any,
    spec: IncrementalAggregateSpec,
    *,
    delta_sign: int = 1,
    filter_expr: str | None = None,
) -> tuple[dict[str, dict[str, Any]], int]:
    """Execute grouped count & exact sum in ShardLoom SQL and return signed group deltas."""

    group_select = ", ".join(spec.group_by)
    agg_parts = ["COUNT(*) AS __sl_row_count"]
    for idx, col in enumerate(spec.sum_columns):
        agg_parts.append(f"COUNT({col}) AS __sl_nn_{idx}")
        agg_parts.append(f"SUM({col}) AS __sl_sum_{idx}")
    where_clause = f" WHERE {filter_expr}" if filter_expr else ""
    sql = f"SELECT {group_select}, {', '.join(agg_parts)} FROM __delta_src{where_clause} GROUP BY {group_select}"

    exec_start = time.monotonic_ns()
    agg_table = ctx.sql(sql, relations={"__delta_src": frame}).collect(check=True).to_arrow()
    engine_nanos = time.monotonic_ns() - exec_start

    group_deltas: dict[str, dict[str, Any]] = {}
    for row in agg_table.to_pylist():
        key = _group_key_json([row[col] for col in spec.group_by])
        cnt = int(row["__sl_row_count"]) * delta_sign
        sums_map: dict[str, dict[str, int]] = {}
        for idx, col in enumerate(spec.sum_columns):
            nn = int(row[f"__sl_nn_{idx}"]) * delta_sign
            raw_sum = row[f"__sl_sum_{idx}"]
            exact_sum = 0 if raw_sum is None else _check_exact_int_value(col, raw_sum) * delta_sign
            sums_map[col] = {"sum": exact_sum, "non_null": nn}
        group_deltas[key] = {"row_count": cnt, "sums": sums_map}
    return group_deltas, engine_nanos


def _merge_delta_into_groups(
    groups: dict[str, dict[str, Any]],
    deltas: Mapping[str, Mapping[str, Any]],
    sum_columns: tuple[str, ...],
) -> None:
    for key, delta in deltas.items():
        current = groups.get(
            key,
            {
                "row_count": 0,
                "sums": {col: {"sum": 0, "non_null": 0} for col in sum_columns},
            },
        )
        new_count = int(current["row_count"]) + int(delta["row_count"])
        if new_count < 0:
            raise FoundryBoundaryError(
                "SL_INCREMENTAL_NEGATIVE_RETRACT",
                f"retraction drove group {key} count negative ({new_count}); state cannot be consistent",
            )
        if new_count == 0:
            groups.pop(key, None)
            continue
        merged_sums: dict[str, dict[str, int]] = {}
        for col in sum_columns:
            cur_col = current["sums"][col]
            d_col = delta["sums"][col]
            new_nn = int(cur_col["non_null"]) + int(d_col["non_null"])
            if new_nn < 0:
                raise FoundryBoundaryError(
                    "SL_INCREMENTAL_NEGATIVE_RETRACT",
                    f"retraction drove group {key} non-null count for {col!r} negative ({new_nn})",
                )
            merged_sums[col] = {
                "sum": int(cur_col["sum"]) + int(d_col["sum"]),
                "non_null": new_nn,
            }
        groups[key] = {"row_count": new_count, "sums": merged_sums}


def _groups_to_arrow_table(
    groups: Mapping[str, Mapping[str, Any]],
    spec: IncrementalAggregateSpec,
    group_arrow_types: Sequence[tuple[str, Any]],
) -> Any:
    import pyarrow as pa

    sorted_keys = sorted(groups.keys(), key=lambda raw: tuple("" if v is None else str(v) for v in _decode_group_key_json(raw)))
    decoded_keys = [_decode_group_key_json(k) for k in sorted_keys]

    arrays: list[Any] = []
    fields: list[Any] = []
    for col_idx, (col_name, pa_type) in enumerate(group_arrow_types):
        fields.append(pa.field(col_name, pa_type))
        arrays.append(pa.array([vals[col_idx] for vals in decoded_keys], type=pa_type))

    fields.append(pa.field(spec.count_column, pa.uint64(), nullable=False))
    arrays.append(
        pa.array([int(groups[k]["row_count"]) for k in sorted_keys], type=pa.uint64())
    )

    for col in spec.sum_columns:
        out_name = spec.output_sum_name(col)
        fields.append(pa.field(out_name, pa.int64(), nullable=True))
        arrays.append(
            pa.array(
                [
                    None if groups[k]["sums"][col]["non_null"] == 0 else int(groups[k]["sums"][col]["sum"])
                    for k in sorted_keys
                ],
                type=pa.int64(),
            )
        )

    return pa.Table.from_arrays(arrays, schema=pa.schema(fields))


def execute_incremental_aggregate_transform(
    *,
    current_input: FoundryInputSpec,
    added_input: FoundryInputSpec | None = None,
    output: FoundryOutputSpec,
    spec: IncrementalAggregateSpec,
    checkpoint_store: IncrementalCheckpointStore | None,
    resources: FoundryResourceProfile,
    current_transaction_rid: str,
    previous_transaction_rid: str | None = None,
    is_snapshot_replacement: bool = False,
    transport: str = "native",
    route: str = FoundryRoute.SINGLE_NODE_NATIVE_TRANSFORM,
    execution_host: str = "local_foundry_adapter_fixture",
    binary: str | os.PathLike[str] | None = None,
    repo_root: str | os.PathLike[str] | None = None,
    check: bool = True,
) -> FoundryTransformReport:
    """Execute snapshot, append, or retraction incremental maintenance via ShardLoom."""

    if added_input is not None and checkpoint_store is None and output.write_mode == "replace":
        raise FoundryBoundaryError(
            "SL_INCREMENTAL_INVALID_MODE",
            "cannot replace a complete aggregate snapshot using only 'added' input without retained state; "
            "provide a checkpoint_store or use current_input complete recomputation",
        )
    if checkpoint_store is None:
        raise FoundryBoundaryError(
            "SL_INCREMENTAL_MISSING_CHECKPOINT_STORE",
            "stateful incremental maintenance requires an explicit IncrementalCheckpointStore",
        )

    from . import __version__ as shardloom_version
    from .context import context as open_shardloom_context

    prior = checkpoint_store.load()
    expected_schema_sig = hashlib.sha256(
        json.dumps(
            {
                "group_by": list(spec.group_by),
                "sum_columns": list(spec.sum_columns),
                "operator_version": spec.operator_version,
            },
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()

    if prior is not None and prior.get("last_transaction_rid") == current_transaction_rid:
        return FoundryTransformReport(
            schema_version=REPORT_SCHEMA_VERSION,
            shardloom_version=shardloom_version,
            transport=transport,
            route=route,
            execution_host=execution_host,
            query_engine="shardloom_native",
            resource_allocation=resources.as_dict(),
            inputs=(),
            output={
                "resource_kind": output.resource_kind,
                "output_format": output.output_format,
                "write_mode": output.write_mode,
                "commit_status": "skipped_duplicate_idempotent",
                "general_foundry_tabular_support": (output.output_format != "vortex"),
            },
            timings_nanos={
                "io_and_conversion_nanos": 0,
                "engine_nanos": 0,
                "publication_nanos": 0,
                "total_job_nanos": 0,
            },
            scratch_cleaned=True,
            incremental={
                "action": "duplicate_skipped_idempotent",
                "current_transaction_rid": current_transaction_rid,
                "checkpoint_transaction_rid": prior["last_transaction_rid"],
                "operator_version": spec.operator_version,
            },
        )

    use_full_recompute = False
    incremental_action = ""
    if prior is None:
        use_full_recompute = True
        incremental_action = "initial_snapshot"
    elif is_snapshot_replacement:
        use_full_recompute = True
        incremental_action = "snapshot_replacement_recomputed"
    elif previous_transaction_rid is None:
        raise FoundryBoundaryError(
            "SL_INCREMENTAL_MISSING_PREVIOUS_TX",
            "incremental update with existing checkpoint requires previous_transaction_rid or is_snapshot_replacement=True",
        )
    elif prior.get("last_transaction_rid") != previous_transaction_rid:
        if spec.on_gap == "fail":
            raise FoundryBoundaryError(
                "SL_INCREMENTAL_SEQUENCE_GAP",
                f"checkpoint transaction {prior.get('last_transaction_rid')!r} does not match "
                f"expected previous_transaction_rid={previous_transaction_rid!r}; "
                "refusing silent delta application across a transaction gap",
            )
        use_full_recompute = True
        incremental_action = "gap_fallback_full_recompute"
    elif prior.get("schema_signature") != expected_schema_sig:
        use_full_recompute = True
        incremental_action = "schema_change_full_recompute"
    elif added_input is None:
        use_full_recompute = True
        incremental_action = "snapshot_refresh_recomputed"
    else:
        incremental_action = (
            "incremental_retraction_and_append"
            if spec.delta_op_column is not None
            else "incremental_append"
        )

    job_start = time.monotonic_ns()
    scratch_dir, cleanup_scratch = _create_scratch_root(resources)
    try:
        io_start = time.monotonic_ns()
        with open_shardloom_context(
            transport=transport,
            binary=binary,
            repo_root=repo_root,
            resources=resources.execution_resources,
        ) as ctx:
            selected_spec = current_input if use_full_recompute else added_input
            assert selected_spec is not None
            bound = bind_foundry_input(ctx, selected_spec, scratch_dir=scratch_dir)
            _validate_delta_input_sum_types(ctx, bound.frame, spec)
            sample_schema = bound.frame.limit(0).collect(check=True).to_arrow().schema
            group_arrow_types = [(col, sample_schema.field(col).type) for col in spec.group_by]
            io_nanos = time.monotonic_ns() - io_start

            agg_engine_nanos = 0
            if use_full_recompute:
                groups: dict[str, dict[str, Any]] = {}
                deltas, ns = _aggregate_table_delta_via_shardloom(
                    ctx, bound.frame, spec, delta_sign=1
                )
                agg_engine_nanos += ns
                _merge_delta_into_groups(groups, deltas, spec.sum_columns)
            else:
                # Copy prior groups carefully before mutation.
                groups = json.loads(json.dumps(prior["groups"]))
                if spec.delta_op_column is None:
                    deltas, ns = _aggregate_table_delta_via_shardloom(
                        ctx, bound.frame, spec, delta_sign=1
                    )
                    agg_engine_nanos += ns
                    _merge_delta_into_groups(groups, deltas, spec.sum_columns)
                else:
                    inserts, ns_ins = _aggregate_table_delta_via_shardloom(
                        ctx,
                        bound.frame,
                        spec,
                        delta_sign=1,
                        filter_expr=f"{spec.delta_op_column} = 'insert'",
                    )
                    deletes, ns_del = _aggregate_table_delta_via_shardloom(
                        ctx,
                        bound.frame,
                        spec,
                        delta_sign=-1,
                        filter_expr=f"{spec.delta_op_column} = 'delete'",
                    )
                    agg_engine_nanos += ns_ins + ns_del
                    _merge_delta_into_groups(groups, inserts, spec.sum_columns)
                    _merge_delta_into_groups(groups, deletes, spec.sum_columns)

            maintained_table = _groups_to_arrow_table(groups, spec, group_arrow_types)
            maintained_frame = ctx.from_arrow_table(maintained_table)
            published = publish_foundry_output(
                maintained_frame,
                output,
                scratch_dir=scratch_dir,
                check=check,
            )

            # Commit checkpoint ONLY after output publication succeeded!
            checkpoint_store.save_atomic(
                {
                    "schema_version": INCREMENTAL_STATE_SCHEMA_VERSION,
                    "source_rid": spec.source_rid,
                    "branch": spec.branch,
                    "last_transaction_rid": current_transaction_rid,
                    "schema_signature": expected_schema_sig,
                    "operator_version": spec.operator_version,
                    "group_by": list(spec.group_by),
                    "sum_columns": list(spec.sum_columns),
                    "count_column": spec.count_column,
                    "groups": groups,
                }
            )
    finally:
        cleanup_scratch()

    total_nanos = time.monotonic_ns() - job_start
    return FoundryTransformReport(
        schema_version=REPORT_SCHEMA_VERSION,
        shardloom_version=shardloom_version,
        transport=transport,
        route=route,
        execution_host=execution_host,
        query_engine="shardloom_native",
        resource_allocation=resources.as_dict(),
        inputs=(bound.metadata_dict(),),
        output=published.as_dict(),
        timings_nanos={
            "io_and_conversion_nanos": io_nanos,
            "engine_nanos": agg_engine_nanos + published.engine_nanos,
            "publication_nanos": published.publication_nanos,
            "total_job_nanos": total_nanos,
        },
        scratch_cleaned=not scratch_dir.exists(),
        incremental={
            "action": incremental_action,
            "current_transaction_rid": current_transaction_rid,
            "previous_transaction_rid": previous_transaction_rid,
            "is_snapshot_replacement": is_snapshot_replacement,
            "maintained_group_count": len(groups),
            "operator_version": spec.operator_version,
        },
    )


__all__ = (
    "BoundFoundryInput",
    "FoundryAdapterError",
    "FoundryBoundaryError",
    "FoundryInputSpec",
    "FoundryOutputSpec",
    "FoundryPublishedOutput",
    "FoundryResourceProfile",
    "FoundryRoute",
    "FoundryTransformReport",
    "INCREMENTAL_STATE_SCHEMA_VERSION",
    "IncrementalAggregateSpec",
    "IncrementalCheckpointStore",
    "REPORT_SCHEMA_VERSION",
    "bind_foundry_input",
    "execute_foundry_transform",
    "execute_incremental_aggregate_transform",
    "map_foundry_resources",
    "publish_foundry_output",
    "redact_sensitive_text",
)
