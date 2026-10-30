"""Optional in-process transport over the shared ShardLoom native runtime.

Importing this adapter does not load the extension, allocate a runtime, inspect
data or resolve a binary. Native values stay outside report serialization.
"""

from __future__ import annotations

import importlib
import json
import math
import os
import sys
import threading
import time
import weakref
from collections.abc import Mapping

from .execution_resources import ExecutionResources
from .models import OutputEnvelope

_OUTPUT_WINDOW_BYTES = 4_194_304


def validate_output_window_bytes(value: object) -> int:
    """Validate the explicit native output-window request without loading native code."""
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError("output_window_bytes must be an integer (0 or 4194304)")
    # Preserve the actual integer payload without invoking subclass conversion,
    # equality or truth-value hooks before native admission.
    value = int.__int__(value)
    if value not in (0, _OUTPUT_WINDOW_BYTES):
        raise ValueError("output_window_bytes must be 0 or 4194304")
    return value


def normalize_transport(value: object) -> str:
    if value not in ("subprocess", "native"):
        raise ValueError("transport must be 'subprocess' or 'native'")
    return value


def normalize_float_policy(value: object) -> str:
    """Validate the immutable floating semantics selector."""
    if not isinstance(value, str) or value not in ("strict", "extended"):
        raise ValueError("float_policy must be 'strict' or 'extended'")
    return value


def application_transport(transport=None, *, binary=None, repo_root=None, env=None):
    """Choose once from installation configuration, never from execution success.

    Native Linux/macOS packages are the ordinary path. Windows and explicitly
    configured CLI/source installations use process isolation. This function
    does not load the extension, inspect datasets, or select any resources.
    """
    if transport is not None:
        return normalize_transport(transport)
    environment = dict(os.environ)
    if env is not None:
        environment.update(env)
    if (binary is not None or repo_root is not None
            or environment.get("SHARDLOOM_BIN") or environment.get("SHARDLOOM_REPO_ROOT")):
        return "subprocess"
    if sys.platform in {"linux", "darwin"}:
        return "native"
    if sys.platform == "win32":
        return "subprocess"
    from .errors import ShardLoomInstallationError
    raise ShardLoomInstallationError(
        f"No default ShardLoom installation contract exists for platform {sys.platform!r}. "
        "Select an explicitly supported native build or CLI installation."
    )


def validate_timeout(value):
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError("timeout must be a finite nonnegative number of seconds")
    try:
        seconds = float(value)
    except OverflowError as error:
        raise ValueError("timeout must be a finite nonnegative number of seconds") from error
    if not math.isfinite(seconds) or seconds < 0:
        raise ValueError("timeout must be a finite nonnegative number of seconds")
    return seconds


def _remaining(deadline):
    return None if deadline is None else max(0.0, deadline - time.monotonic())


def load_native():
    try:
        return importlib.import_module("shardloom._native")
    except ImportError as error:
        from .errors import ShardLoomInstallationError
        raise ShardLoomInstallationError(
            "transport='native' requires the optional shardloom._native extension; "
            "install a package containing that extension or explicitly select "
            "transport='subprocess'. No alternate transport was started."
        ) from error


def native_resources(module, allocation: ExecutionResources):
    limits = allocation.limits
    return module.ExecutionResources(
        allocation.memory_bytes, allocation.max_parallelism,
        memory_origin=allocation.memory_origin,
        parallelism_origin=allocation.parallelism_origin,
        memory_limit_bytes=None if limits is None else limits.memory_bytes,
        parallelism_limit=None if limits is None else limits.max_parallelism,
    )


def _native_fields(fields):
    from ._input_schema import _wire_type
    import json

    return [(name, json.dumps(_wire_type(dtype), ensure_ascii=False, separators=(",", ":")))
            for name, dtype in fields]


class NativePlanState:
    """Plan-local ownership; no result replay or process-global handle cache."""

    def __init__(self):
        self.lock = threading.Lock()
        self.key = None
        self.plan = None
        self.references = frozenset()


def execution_envelope(fields, *, result=None, operation="collect"):
    fields = dict(fields)
    fields["python_execution_transport"] = "native"
    fields["python_result_ownership"] = "retained_native" if result is not None else "native_operation_report"
    envelope = OutputEnvelope.from_field_mapping(
        fields, command="run", summary=f"native {operation} completed",
        human_text=f"native {operation} completed",
    )
    object.__setattr__(envelope, "_native_result", result)
    return envelope


def _failure(error, *, check):
    from .errors import ShardLoomCommandError

    diagnostic = getattr(error, "diagnostic", None)
    if diagnostic is None:
        raise error
    fields = dict(getattr(error, "resource_fields", {}))
    fields["python_execution_transport"] = "native"
    envelope = OutputEnvelope.from_field_mapping(
        fields, command="run", status="error",
        summary=str(error), human_text=str(error),
    )
    raw = dict(envelope.raw)
    raw["diagnostics"] = [diagnostic]
    envelope = OutputEnvelope.from_json(raw)
    if check:
        raise ShardLoomCommandError(("shardloom._native", "run"), 2, envelope, "") from error
    return envelope


def _checked_native_call(function, *args, **kwargs):
    try:
        return function(*args, **kwargs)
    except Exception as error:
        return _failure(error, check=True)


class NativeBackend:
    def __init__(self, allocation: ExecutionResources | None, limits, timeout=None, *, float_policy="strict"):
        float_policy = normalize_float_policy(float_policy)
        self.module = load_native()
        self.timeout = validate_timeout(timeout)
        self._plans = weakref.WeakSet()
        self._plans_lock = threading.Lock()
        # A shared client's administrator policy persists. Per-context/per-call
        # ceilings accompany every operation but cannot change the client policy.
        self.session = self.module.Session(
            None if allocation is None else native_resources(self.module, allocation),
            memory_limit_bytes=None if limits is None else limits.memory_bytes,
            parallelism_limit=None if limits is None else limits.max_parallelism,
            float_policy=float_policy,
        )

    def command(self, args):
        return _checked_native_call(self.session.command, args, timeout=self.timeout)

    def declaration(self, frame, spill=None):
        from ._connected import native_declaration
        if isinstance(spill, Mapping):
            spill = json.dumps(dict(spill), sort_keys=True)
        return native_declaration(self, frame, spill)

    def schema(self, frame, allocation):
        from ._result_schema import schema_fields

        raw = _checked_native_call(self.declaration(frame).schema_json,
            resources=native_resources(self.module, allocation), timeout=self.timeout)
        return schema_fields(raw, "vortex.dtype.serde.v1")

    def close(self):
        self.session.close()

    def cancel(self):
        self.session.cancel()

    def release_prepared(self):
        """Release native plan preparation without closing source/result owners."""
        with self._plans_lock:
            states = list(self._plans)
        for state in states:
            with state.lock:
                if state.plan is not None:
                    _checked_native_call(state.plan.release_prepared)
                state.key, state.plan = None, None

    def from_arrow(self, producer, allocation):
        from .query import Result
        from .session import SessionSqlResult

        # Only our exact wrappers have a trusted retained-owner accessor. Do not
        # inspect arbitrary producer attributes before native resource admission.
        producer_type = type(producer)
        retained = (producer.native_result
                    if any(producer_type is candidate for candidate in (Result, NativeResultBatch, SessionSqlResult))
                    else None)
        return _checked_native_call(self.session.from_arrow,
            producer if retained is None else retained,
            resources=native_resources(self.module, allocation),
            timeout=self.timeout,
        )

    def from_rows(self, rows, schema, allocation):
        from ._input_schema import normalize_schema

        fields = None if schema is None else _native_fields(normalize_schema(schema))
        return _checked_native_call(self.session.from_rows,
            rows, fields=fields, resources=native_resources(self.module, allocation),
            timeout=self.timeout,
        )

    def from_vortex_ipc(self, array_message, dtype_message, allocation):
        return _checked_native_call(self.session.from_vortex_ipc,
            array_message, dtype_message=dtype_message,
            resources=native_resources(self.module, allocation), timeout=self.timeout,
        )

    def _plan(self, request, allocation, deadline=None):
        if request.get("fanout_outputs") and request.get("requested_output", "collect") == "collect":
            raise ValueError("native fanout requires an explicit primary output")
        if request.get("bounded") is False:
            raise ValueError("native execution requires bounded resource admission")
        operation_options = any(
            name.startswith(("native_vortex_", "vortex_"))
            and value is not None and value is not False
            for name, value in request.items()
        )
        if request.get("_native_frame") is not None:
            frame = request["_native_frame"]
            policy = request.get("execution_policy", "vortex_middle")
            admitted_policy = policy == "vortex_middle" or (
                policy == "native_vortex" and frame.source.source_format == "vortex"
            )
            if (operation_options or not admitted_policy
                    or request.get("materialization_policy", "bounded") != "bounded"):
                raise ValueError("connected plans require the native bounded execution policy; operation-specific overrides cannot replace the relation")
            return self.declaration(frame, request.get("spill"))
        statement = request.get("sql_statement")
        if (operation_options or not statement
                or request.get("execution_policy", "vortex_middle") != "vortex_middle"
                or request.get("materialization_policy", "bounded") != "bounded"):
            return self._workflow_plan(request, allocation)
        if not isinstance(statement, str) or not statement.strip():
            raise ValueError("native execution requires a complete SQL or DataFrame plan")
        spill = request.get("spill")
        if isinstance(spill, Mapping):
            spill = json.dumps(dict(spill), sort_keys=True)
        bindings = dict(request.get("source_bindings") or {})
        reference = request.get("input_uri")
        if reference is not None:
            reference = str(reference)
            bindings.setdefault(reference, {
                "input_format": request.get("input_format"),
                "source_schema": request.get("source_schema"),
            })
        from ._batches import validate_inputs

        inputs = request.get("input_batches") or {}
        validate_inputs(bindings, inputs)
        files, sources, declarations, batches = [], [], [], []
        for reference, binding in bindings.items():
            owner = binding.get("native_input")
            if owner is not None:
                sources.append((reference, owner))
                declarations.append((reference, "native", id(owner)))
            elif "memory_input" in binding:
                memory = binding["memory_input"]
                if memory.get("kind") == "batches":
                    producer = inputs[reference]
                    fields = _native_fields(producer._types)
                    streaming = memory.get("streaming", False)
                    batches.append((reference, producer, fields, streaming))
                    declarations.append((reference, "batches", id(producer), tuple(fields), streaming))
                elif memory.get("kind") == "range":
                    declarations.append((reference, "range", tuple(memory.items())))
                else:
                    raise ValueError("native object input requires a native owner")
            else:
                from .client import _schema_command_arg
                schema = _schema_command_arg(binding.get("source_schema"))
                format_name = binding.get("input_format")
                if not isinstance(format_name, str):
                    raise ValueError(f"source {reference!r} requires input_format")
                files.append((reference, format_name, schema))
                declarations.append((reference, format_name, schema))
        state = request.get("_native_plan") or NativePlanState()
        with self._plans_lock:
            self._plans.add(state)
        key = (id(self), statement, tuple(declarations), spill)
        with state.lock:
            if state.key == key:
                return state.plan
            for reference, binding in bindings.items():
                memory = binding.get("memory_input", {})
                if memory.get("kind") == "range":
                    start, end, step = memory["start"], memory["end"], memory["step"]
                    if memory.get("inclusive"):
                        end += 1 if step > 0 else -1
                    owner = self.session.from_range(
                        memory["column"], start, step, len(range(start, end, step)),
                        resources=native_resources(self.module, allocation),
                        timeout=_remaining(deadline),
                    )
                    sources.append((reference, owner))
            plan = self.session.sql(
                statement, files=files, sources=sources, batches=batches, spill=spill,
            )
            state.key, state.plan = key, plan
            return plan

    def _workflow_plan(self, request, allocation):
        from .client import (
            _append_public_vortex_payload_args, _iter_fanout_outputs,
            _schema_command_arg, _source_bindings_command_arg,
        )

        bindings = request.get("source_bindings") or {}
        if any("native_input" in binding or "memory_input" in binding
               for binding in bindings.values()):
            raise ValueError(
                "declared memory inputs require a complete SQL declaration; "
                "separate native operator options are not consumed"
            )
        args = [request.get("surface", "python"), "--bounded", "true"]
        for name, flag, default in (
            ("input_uri", "--input", None), ("input_format", "--input-format", None),
            ("sql_statement", "--sql", None), ("plan_summary", "--plan", None),
            ("requested_output", "--request", "collect"), ("output_ref", "--output", None),
            ("execution_policy", "--execution-policy", "vortex_middle"),
            ("materialization_policy", "--materialization-policy", "bounded"),
            ("evidence_level", "--evidence-level", "runtime_smoke"),
        ):
            value = request.get(name, default)
            if value is not None:
                args.extend((flag, str(value)))
        for flag, value in (
            ("--source-schema", _schema_command_arg(request.get("source_schema"))),
            ("--source-bindings", _source_bindings_command_arg(bindings)),
        ):
            if value is not None:
                args.extend((flag, value))
        if request.get("allow_overwrite"):
            args.append("--allow-overwrite")
        for format_name, path in _iter_fanout_outputs(request.get("fanout_outputs")):
            args.extend(("--fanout-output", f"{format_name}={path}"))
        option_names = (
            "native_vortex_operation_family", "native_vortex_right_input",
            "vortex_primitive", "vortex_predicate", "vortex_columns",
            "vortex_source_order_limit", "vortex_sample_seed", "vortex_sample_fraction",
            "vortex_sample_replacement", "vortex_sample_weight_column", "vortex_duplicate_keep",
            "vortex_deduplicate_key_columns", "vortex_expression_projection",
            "vortex_melt_projection", "vortex_explode_projection", "vortex_pivot_projection",
            "vortex_rolling_window", "vortex_sort_rows", "vortex_aggregate",
        )
        _append_public_vortex_payload_args(
            args, **{name: request.get(name) for name in option_names},
            resources=allocation, spill=request.get("spill"),
        )
        state = request.get("_native_plan") or NativePlanState()
        with self._plans_lock:
            self._plans.add(state)
        key = (id(self), "workflow", tuple(args))
        with state.lock:
            if state.key == key:
                return state.plan
            plan = self.session.workflow(args)
            state.key, state.plan = key, plan
            return plan

    def execute(self, request, allocation, *, check):
        from .client import PublicWorkflowExecution

        deadline = None if self.timeout is None else time.monotonic() + self.timeout
        try:
            plan = self._plan(request, allocation, deadline)
            resources = native_resources(self.module, allocation)
            output = request.get("requested_output", "collect")
            if output == "collect":
                result = plan.collect(resources=resources, timeout=_remaining(deadline))
                envelope = execution_envelope(result.report, result=result)
            else:
                path = request.get("output_ref")
                if path is None:
                    raise ValueError("native output requires output_ref")
                from .client import _iter_fanout_outputs
                fanout = _iter_fanout_outputs(request.get("fanout_outputs"))
                options = dict(overwrite=request.get("allow_overwrite", False),
                               resources=resources, timeout=_remaining(deadline))
                if fanout:
                    from .query import _public_write_request_for_format
                    targets = [(path, output)] + [
                        (target, _public_write_request_for_format(format_name))
                        for format_name, target in fanout
                    ]
                    fields = plan.write_many(targets, **options)
                else:
                    fields = plan.write(path, output, **options)
                envelope = execution_envelope(fields, operation=output)
        except Exception as error:
            envelope = _failure(error, check=check)
        return PublicWorkflowExecution(envelope)

    def batches(self, request, allocation, batch_rows):
        output_window_bytes = validate_output_window_bytes(request.get("output_window_bytes", 0))
        if "output_window_bytes" in request:
            request = dict(request, output_window_bytes=output_window_bytes)
        if type(batch_rows) is not int or batch_rows <= 0:
            raise ValueError("batch_rows must be a positive integer")
        if request.get("requested_output", "collect") != "collect":
            raise ValueError("incremental result consumption requires collect output")
        # Retain an inert declaration until first demand; validation above does
        # not advance a producer or start native execution.
        return NativeBatchIterator(self, request, allocation, batch_rows)


def _batch_schema(fields):
    from ._result_schema import ResultType

    def dtype(value):
        name = value["type"]
        if name == "decimal128":
            parameters = (value["precision"], value["scale"])
        elif name in {"list", "fixed_size_list"}:
            parameters = (dtype(value["item"]),)
            if name == "fixed_size_list":
                parameters += (value["size"],)
        elif name == "struct":
            parameters = tuple((name, dtype(field)) for name, field in value["fields"])
        else:
            parameters = ()
        return ResultType(name, value["nullable"], parameters)

    return tuple((name, dtype(field)) for name, field in fields)


class NativeResultBatch:
    def __init__(self, batch, index):
        self.native_result = batch
        self.index = index

    @property
    def schema(self):
        return self.result_schema

    def to_rows(self):
        """Explicitly convert this provisional native batch to Python values."""
        return self.python_objects

    @property
    def result_schema(self):
        return _batch_schema(self.native_result.schema)

    @property
    def result_columns(self):
        return tuple(name for name, _ in self.result_schema)

    @property
    def result_rows(self):
        return tuple(self.native_result.row_export(wire_values=True))

    @property
    def python_objects(self):
        return tuple(self.native_result.row_export())

    def to_arrow(self):
        import pyarrow
        return pyarrow.RecordBatchReader.from_stream(self.native_result).read_all()

    def to_pandas(self):
        from .query import _result_to_pandas
        import pandas
        return _result_to_pandas(self, pandas)

    def to_numpy(self):
        from .query import _result_to_numpy
        import numpy
        return _result_to_numpy(self, numpy)

    def __arrow_c_stream__(self, requested_schema=None):
        return self.native_result.__arrow_c_stream__(requested_schema)


class NativeBatchIterator:
    def __init__(self, backend, request, allocation, batch_rows):
        self._backend = backend
        self._request = request
        self._allocation = allocation
        self._batch_rows = batch_rows
        self._stream = None
        self._pending = None
        self._offset = 0
        self._index = 0
        self._closed = False
        self._deadline = None
        self._client = None
        self._cancelled = threading.Event()
        self._iteration_lock = threading.RLock()
        self._pulling = False
        self.report = None

    def __iter__(self):
        return self

    def __next__(self):
        with self._iteration_lock:
            self._pulling = True
            try:
                return self._next_batch()
            finally:
                self._pulling = False

    def _raise_if_cancelled(self):
        # Cancellation is a failed execution, including when it closed a paused
        # iterator. Report it once before the ordinary closed-stream sentinel.
        if self._cancelled.is_set():
            self._cancelled.clear()
            from .errors import _cancelled_error
            raise _cancelled_error(("run",), forced=False)

    def _next_batch(self):
        if self._closed and not self._cancelled.is_set():
            raise StopIteration
        try:
            self._raise_if_cancelled()
            if self._client is not None:
                self._client._ensure_open()
            if self._stream is None:
                if self._backend.timeout is not None:
                    self._deadline = time.monotonic() + self._backend.timeout
                plan = self._backend._plan(self._request, self._allocation, self._deadline)
                stream_options = dict(
                    resources=native_resources(self._backend.module, self._allocation),
                    timeout=_remaining(self._deadline),
                )
                output_window_bytes = self._request.get("output_window_bytes", 0)
                if output_window_bytes:
                    stream_options["output_window_bytes"] = output_window_bytes
                self._stream = plan.iter_batches(**stream_options)
            if self._deadline is not None and time.monotonic() >= self._deadline:
                from .errors import ShardLoomTimeoutError
                raise ShardLoomTimeoutError(
                    ("shardloom._native", "run"), self._backend.timeout,
                    message="native execution deadline exceeded; no fallback execution was attempted",
                )
            if self._pending is None:
                try:
                    self._pending = next(self._stream)
                except StopIteration:
                    self._raise_if_cancelled()
                    from .client import PublicWorkflowExecution
                    fields = self._stream.report
                    if fields is None:
                        raise RuntimeError("native stream omitted its completed execution report")
                    self.report = PublicWorkflowExecution(execution_envelope(fields))
                    self.close()
                    raise
                self._offset = 0
            self._raise_if_cancelled()
            length = min(self._batch_rows, self._pending.row_count - self._offset)
            batch = self._pending.slice(self._offset, length)
            self._offset += length
            if self._offset == self._pending.row_count:
                self._pending = None
            result = NativeResultBatch(batch, self._index)
            self._index += 1
            return result
        except BaseException as error:
            try:
                self.close()
            except BaseException:
                # Preserve the original producer/native failure after draining.
                pass
            if getattr(error, "diagnostic", None) is not None:
                _failure(error, check=True)
            raise

    def close(self):
        with self._iteration_lock:
            if not self._closed:
                self._closed = True
                self._pending = None
                try:
                    if self._stream is not None:
                        self._stream.close()
                finally:
                    if self._client is not None:
                        with self._client._native_backend_lock:
                            self._client._batch_streams.discard(self)

    def cancel(self):
        """Cancel demand without blocking a running producer callback."""
        if self._closed:
            return
        self._cancelled.set()
        if self._pulling:
            return
        if self._iteration_lock.acquire(blocking=False):
            try:
                self.close()
            finally:
                self._iteration_lock.release()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

    def __del__(self):
        self.close()
