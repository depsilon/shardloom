"""Inert, context-owned declarations of trusted rowwise application code."""

from __future__ import annotations

import json
import threading
import weakref
from dataclasses import dataclass, field

from ._connected import identifier
from ._input_schema import normalize_schema, _wire_type


def _schema_fields(fields):
    if not fields:
        raise ValueError("batch functions require nonempty input and output schemas")
    result = [(name, json.dumps(_wire_type(dtype), ensure_ascii=False, separators=(",", ":")))
              for name, dtype in fields]
    if sum(len(name.encode("utf-8")) + len(raw.encode("utf-8")) for name, raw in result) > 8 << 20:
        raise ValueError("batch function schema exceeds 8 MiB")
    return result


def _unsigned(name, value, *, positive):
    if type(value) is not int:
        raise TypeError(f"{name} must be an integer")
    if not int(positive) <= value <= (1 << 64) - 1:
        raise ValueError(f"{name} is outside its unsigned 64-bit range")
    return value


@dataclass(frozen=True, slots=True, eq=False)
class BatchFunction:
    """Immutable registered function snapshot accepted by ``frame.map_batches``.

    The callable receives ``(pyarrow.RecordBatch, invocation)`` and returns one
    RecordBatch with the declared schema and the same row count and order.
    Call ``invocation.check_cancelled()`` during long application work. The
    engine copies inputs and returned outputs into credited native ownership;
    private application allocations remain the caller's responsibility.
    """

    name: str
    version: str
    input_schema: tuple
    output_schema: tuple
    max_batch_rows: int
    max_output_bytes: int
    workspace_bytes: int
    _owner: object = field(repr=False, compare=False)
    _native: object = field(repr=False, compare=False)

    def _for_client(self, client):
        client._ensure_open()
        if self._owner() is not client:
            raise ValueError("batch function belongs to another context")
        if client.transport != "native":
            raise ValueError("application batch functions require the in-process native transport")
        return self._native


class BatchFunctionRegistry:
    """Named immutable callback snapshots belonging to one context or session.

    Registration and inspection never invoke the callable. Replacement and drop
    change future lookups; existing plans retain their original function.
    This boundary admits caller-trusted code, with cooperative cancellation,
    explicit workspace/output bounds, and no external execution delegation.
    """

    def __init__(self, client):
        self._client = client
        self._functions = {}
        self._lock = threading.RLock()

    def register(self, name, function, *, input_schema, output_schema, version,
                 max_output_bytes, workspace_bytes, max_batch_rows=2048,
                 trusted=False, rowwise=False, deterministic=True,
                 null_behavior="null_aware", effect="pure_deterministic", replace=False):
        """Declare trusted ``function(batch, invocation)`` without running it.

        Set ``trusted=True`` and ``rowwise=True`` explicitly. Rowwise code must
        produce the same per-row result regardless of input batch partitioning.
        Null-aware code receives nulls and owns output validity. Only deterministic
        code without external effects is admitted. Output byte bounds cover both
        returned and independently owned native buffers; workspace is reserved
        before invocation and is a caller allowance for private allocations.
        """
        self._client._ensure_open()
        name = identifier(name, label="batch function name")
        if type(version) is not str or not version.strip():
            raise ValueError("batch function version must be a nonempty string")
        if type(replace) is not bool:
            raise TypeError("replace must be a bool")
        if trusted is not True or rowwise is not True or deterministic is not True or null_behavior != "null_aware" or effect != "pure_deterministic":
            raise ValueError("batch functions require explicitly trusted, rowwise, deterministic, null-aware code without external effects")
        if not callable(function):
            raise TypeError("batch function implementation must be callable")
        if self._client.transport != "native":
            raise ValueError("application batch functions require the in-process native transport")
        max_batch_rows = _unsigned("max_batch_rows", max_batch_rows, positive=True)
        max_output_bytes = _unsigned("max_output_bytes", max_output_bytes, positive=True)
        workspace_bytes = _unsigned("workspace_bytes", workspace_bytes, positive=False)
        input_fields, output_fields = normalize_schema(input_schema), normalize_schema(output_schema)
        input_declaration, output_declaration = _schema_fields(input_fields), _schema_fields(output_fields)
        with self._lock:
            if name in self._functions and not replace:
                raise ValueError(f"batch function {name!r} already exists; use replace=True")
            backend = self._client._native_backend_for()
            native = backend.module.BatchFunction(backend.session, name, version, function,
                input_declaration, output_declaration, max_batch_rows, max_output_bytes, workspace_bytes,
                trusted=True, rowwise=True, deterministic=True,
                null_behavior=null_behavior, effect=effect)
            handle = BatchFunction(name, version, input_fields, output_fields,
                max_batch_rows, max_output_bytes, workspace_bytes, weakref.ref(self._client), native)
            self._functions[name] = handle
        return handle

    def get(self, name):
        """Return the current immutable registration; never inspect its code."""
        self._client._ensure_open()
        name = identifier(name, label="batch function name")
        with self._lock:
            try:
                return self._functions[name]
            except KeyError:
                raise KeyError(f"unknown batch function {name!r}") from None

    def list(self):
        """Return registered names in deterministic order."""
        self._client._ensure_open()
        with self._lock:
            return tuple(sorted(self._functions))

    def drop(self, name, *, if_exists=False):
        """Remove a future lookup while preserving already captured plans."""
        self._client._ensure_open()
        name = identifier(name, label="batch function name")
        if type(if_exists) is not bool:
            raise TypeError("if_exists must be a bool")
        with self._lock:
            if name not in self._functions:
                if if_exists:
                    return False
                raise KeyError(f"unknown batch function {name!r}")
            del self._functions[name]
        return True

    def clear(self):
        """Release registry handles; existing plan snapshots keep their owners."""
        with self._lock:
            self._functions.clear()
