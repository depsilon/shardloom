"""Optional in-process transport over the shared ShardLoom native runtime.

Importing this adapter does not load the extension, allocate a runtime, inspect
data or resolve a binary. Native values stay outside report serialization.
"""

from __future__ import annotations

import importlib
import json
import math
import threading
import time
from collections.abc import Mapping

from .execution_resources import ExecutionResources
from .models import OutputEnvelope


def normalize_transport(value: object) -> str:
    if value not in ("subprocess", "native"):
        raise ValueError("transport must be 'subprocess' or 'native'")
    return value


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
    except ModuleNotFoundError as error:
        if error.name != "shardloom._native":
            raise
        raise ImportError(
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


class NativeBackend:
    def __init__(self, allocation: ExecutionResources | None, limits, timeout=None):
        self.module = load_native()
        self.timeout = validate_timeout(timeout)
        # A shared client's administrator policy persists. Per-context/per-call
        # ceilings accompany every operation but cannot change the client policy.
        self.session = self.module.Session(
            None if allocation is None else native_resources(self.module, allocation),
            memory_limit_bytes=None if limits is None else limits.memory_bytes,
            parallelism_limit=None if limits is None else limits.max_parallelism,
        )

    def command(self, args):
        return self.session.command(args, timeout=self.timeout)

    def close(self):
        self.session.close()

    def cancel(self):
        self.session.cancel()

    def from_arrow(self, producer, allocation):
        from .query import VortexWorkflowExecutionReport

        # Only our exact wrappers have a trusted retained-owner accessor. Do not
        # inspect arbitrary producer attributes before native resource admission.
        producer_type = type(producer)
        retained = (producer.native_result
                    if producer_type is VortexWorkflowExecutionReport or producer_type is NativeResultBatch
                    else None)
        return self.session.from_arrow(
            producer if retained is None else retained,
            resources=native_resources(self.module, allocation),
            timeout=self.timeout,
        )

    def from_rows(self, rows, schema, allocation):
        from ._input_schema import normalize_schema

        fields = None if schema is None else _native_fields(normalize_schema(schema))
        return self.session.from_rows(
            rows, fields=fields, resources=native_resources(self.module, allocation),
            timeout=self.timeout,
        )

    def from_vortex_ipc(self, array_message, dtype_message, allocation):
        return self.session.from_vortex_ipc(
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
                    batches.append((reference, producer.open, fields, streaming))
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
        self.report = None

    def __iter__(self):
        return self

    def __next__(self):
        if self._closed:
            raise StopIteration
        try:
            if self._stream is None:
                if self._backend.timeout is not None:
                    self._deadline = time.monotonic() + self._backend.timeout
                plan = self._backend._plan(self._request, self._allocation, self._deadline)
                self._stream = plan.iter_batches(
                    resources=native_resources(self._backend.module, self._allocation),
                    timeout=_remaining(self._deadline),
                )
            if self._deadline is not None and time.monotonic() >= self._deadline:
                raise TimeoutError("native execution deadline exceeded; no fallback execution was attempted")
            if self._pending is None:
                try:
                    self._pending = next(self._stream)
                except StopIteration:
                    from .client import PublicWorkflowExecution
                    fields = self._stream.report
                    if fields is None:
                        raise RuntimeError("native stream omitted its completed execution report")
                    self.report = PublicWorkflowExecution(execution_envelope(fields))
                    self.close()
                    raise
                self._offset = 0
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
        if not self._closed:
            self._closed = True
            self._pending = None
            if self._stream is not None:
                self._stream.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

    def __del__(self):
        self.close()
