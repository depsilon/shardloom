"""Bounded boundary adapters; native ShardLoom owns every query operation."""

from __future__ import annotations

from dataclasses import dataclass
import json
import math
import os
import selectors
import subprocess
import time
from typing import Any, Mapping, Sequence

from .errors import ShardLoomCommandError, ShardLoomProtocolError
from ._result_schema import ResultType, python_rows, schema_fields

_MAX_INPUT_FRAME = 8 << 20
_MAX_RESPONSE_FRAME = (16 << 20) + 4096


def _json_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON object field")
        result[key] = value
    return result


def _json_constant(_):
    raise ValueError("nonfinite JSON number")


def _json_float(text):
    value = float(text)
    if not math.isfinite(value):
        raise ValueError("nonfinite JSON number")
    return value


def validate_inputs(bindings, inputs):
    expected = {uri: binding["memory_input"] for uri, binding in (bindings or {}).items()
                if binding.get("memory_input", {}).get("kind") == "batches"}
    if set(expected) != set(inputs or {}):
        raise ValueError("batch inputs must match the complete set of batch source declarations")
    for uri, source in (inputs or {}).items():
        if not isinstance(source, BatchInput) or tuple(map(tuple, expected[uri].get("schema", ()))) != source.schema:
            raise ValueError("batch input schema differs from its source declaration")


class BatchInput:
    """Inert input declaration. A factory supplies a fresh iterable for each call."""

    def __init__(self, batches: object, schema: tuple[tuple[str, object], ...], *,
                 types: tuple[tuple[str, ResultType], ...] | None = None):
        from ._input_schema import normalize_schema

        if not callable(batches) and not hasattr(batches, "__iter__"):
            raise TypeError("batches must be an iterable or a factory returning an iterable")
        self.schema = schema
        self._types = types if types is not None else normalize_schema(dict(schema))
        self._batches = batches
        self._used = False

    def open(self):
        if callable(self._batches):
            return iter(self._batches())
        if self._used:
            raise ValueError("batch input has already been consumed; use a batch factory for repeated execution")
        self._used = True
        return iter(self._batches)

    def encode(self, batch: object) -> list[list[str | None]]:
        from ._input_schema import encode_cell

        if isinstance(batch, (str, bytes, bytearray)) or not isinstance(batch, Sequence):
            raise TypeError("each input batch must be a sequence of row mappings")
        if len(batch) > 2048:
            raise ValueError("each input batch must contain at most 2,048 rows")
        names = {name for name, _ in self.schema}
        rows = []
        # Bound encoded values before JSON serialization as well as bounding the
        # final frame. The caller's original objects remain caller-owned.
        value_bytes = 0
        for row in batch:
            if not isinstance(row, Mapping) or set(row) != names:
                raise ValueError("batch rows must match the complete declared schema")
            values = []
            for name, dtype in self._types:
                value = encode_cell(dtype, row[name])
                if value is not None and len(value) > _MAX_INPUT_FRAME:
                    raise ValueError("input batch value exceeds 8 MiB")
                value_bytes += 1 if value is None else len(value.encode("utf-8"))
                if value_bytes > _MAX_INPUT_FRAME:
                    raise ValueError("input batch values exceed 8 MiB")
                values.append(value)
            rows.append(values)
        return rows


@dataclass(frozen=True)
class ResultBatch:
    """One provisional batch; success is established by the iterator's final report."""

    index: int
    result_rows: tuple[Mapping[str, Any], ...]
    result_schema: tuple[tuple[str, ResultType], ...]

    @property
    def result_columns(self) -> tuple[str, ...]:
        return tuple(name for name, _ in self.result_schema)

    @property
    def python_objects(self) -> tuple[Mapping[str, Any], ...]:
        return tuple(python_rows(self.result_rows, self.result_schema))

    def to_pandas(self):
        from .query import _result_to_pandas
        import pandas
        return _result_to_pandas(self, pandas)

    def to_arrow(self):
        from .query import _result_to_arrow_table
        import pyarrow
        return _result_to_arrow_table(self, pyarrow)

    def to_numpy(self):
        from .query import _result_to_numpy
        import numpy
        return _result_to_numpy(self, numpy)


class ResultBatchIterator:
    """One native transaction with explicit backpressure and deterministic close.

    Use a ``with`` block when iteration may stop early. ``report`` remains None
    unless every batch is acknowledged and final source validation succeeds.
    Retaining yielded Python objects is the caller's memory responsibility.
    Forced process termination raises on close because staged output cleanup
    cannot be confirmed. An active producer/consumer error is preserved.
    """

    def __init__(self, client, args, *, inputs=None, batch_rows=2048, stream_results=True, check=True):
        if type(batch_rows) is not int or not 1 <= batch_rows <= 2048:
            raise ValueError("batch_rows must be an integer in 1..=2048")
        if os.name != "posix":
            raise ValueError("native batch transport currently requires a Unix runtime")
        from .client import _redact_command_for_error
        self._client = client
        self._args = [str(arg) for arg in args]
        self._inputs = dict(inputs or {})
        self._iterators = {}
        self._input_indices = {}
        self._ended_inputs = set()
        self._batch_rows = batch_rows
        self._stream_results = stream_results
        self._check = check
        self._process = None
        self._buffer = bytearray()
        self._ack = None
        self._next_index = 0
        self._rows = 0
        self._schema = None
        self._closed = False
        self._command = _redact_command_for_error([*client._binary_parts(), *self._args])
        self.report = None
        self._terminal_envelope = None

    def __enter__(self):
        return self

    def __exit__(self, error_type, *_):
        if error_type is None:
            self.close()
        else:
            self._close_after_error()

    def __iter__(self):
        return self

    def _deadline(self):
        return None if self._client._timeout is None else time.monotonic() + self._client._timeout

    def _start(self, deadline):
        if self._process is not None:
            return
        self._process = subprocess.Popen(
            [*self._client._binary_parts(), "python-worker", "--batch-stream"],
            cwd=self._client._cwd, env=self._client._effective_env(),
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
            bufsize=0,
        )
        os.set_blocking(self._process.stdin.fileno(), False)
        os.set_blocking(self._process.stdout.fileno(), False)
        self._write({"args": self._args, "stream_results": self._stream_results,
                     "batch_rows": self._batch_rows}, deadline, 16 << 20)

    def _ready(self, stream, event, deadline):
        remaining = None if deadline is None else deadline - time.monotonic()
        if remaining is not None and remaining <= 0:
            raise subprocess.TimeoutExpired(self._command, self._client._timeout)
        with selectors.DefaultSelector() as selector:
            selector.register(stream, event)
            if not selector.select(remaining):
                raise subprocess.TimeoutExpired(self._command, self._client._timeout)

    def _write(self, value, deadline, maximum=_MAX_INPUT_FRAME):
        # iterencode caps temporary JSON storage before completing a large frame.
        payload = bytearray()
        for piece in json.JSONEncoder(ensure_ascii=False, separators=(",", ":"), allow_nan=False).iterencode(value):
            encoded = piece.encode("utf-8")
            if len(payload) + len(encoded) > maximum:
                raise ValueError(f"native batch frame exceeds {maximum} bytes")
            payload.extend(encoded)
        payload.append(10)
        remaining = memoryview(payload)
        while remaining:
            self._ready(self._process.stdin, selectors.EVENT_WRITE, deadline)
            try:
                written = os.write(self._process.stdin.fileno(), remaining)
            except BlockingIOError:
                continue
            if written == 0:
                raise ShardLoomProtocolError("native batch input pipe closed")
            remaining = remaining[written:]

    def _read(self, deadline):
        while True:
            end = self._buffer.find(b"\n")
            if end >= 0:
                if end > _MAX_RESPONSE_FRAME:
                    raise ShardLoomProtocolError("native batch response exceeds its byte bound")
                line = bytes(self._buffer[:end])
                del self._buffer[:end + 1]
                try:
                    text = line.decode("utf-8")
                    value = json.loads(text, object_pairs_hook=_json_object,
                                       parse_constant=_json_constant, parse_float=_json_float)
                except (UnicodeError, ValueError) as error:
                    raise ShardLoomProtocolError("native batch response is not valid UTF8 JSON") from error
                if not isinstance(value, dict):
                    raise ShardLoomProtocolError("native batch response must be an object")
                return text, value
            if len(self._buffer) > _MAX_RESPONSE_FRAME:
                raise ShardLoomProtocolError("native batch response exceeds its byte bound")
            self._ready(self._process.stdout, selectors.EVENT_READ, deadline)
            try:
                chunk = os.read(self._process.stdout.fileno(), 65536)
            except BlockingIOError:
                continue
            if not chunk:
                raise ShardLoomProtocolError("native batch process closed before a complete final report")
            self._buffer.extend(chunk)

    def _provide_input(self, request, deadline):
        uri, index = request.get("uri"), request.get("index")
        if (not isinstance(uri, str) or uri not in self._inputs
                or uri in self._ended_inputs or type(index) is not int
                or index != self._input_indices.get(uri, 0)
                or request.get("max_rows") != 2048 or request.get("max_bytes") != _MAX_INPUT_FRAME):
            raise ShardLoomProtocolError("native input request does not match its declaration or sequence")
        source = self._inputs[uri]
        if uri not in self._iterators:
            self._iterators[uri] = source.open()
        try:
            batch = next(self._iterators[uri])
        except StopIteration:
            self._ended_inputs.add(uri)
            self._write({"kind": "end", "uri": uri, "index": index}, deadline)
        else:
            self._write({"kind": "rows", "uri": uri, "index": index,
                         "rows": source.encode(batch)}, deadline)
            self._input_indices[uri] = index + 1

    def __next__(self):
        if self._closed:
            raise StopIteration
        deadline = self._deadline()
        try:
            self._start(deadline)
            if self._ack is not None:
                self._write({"kind": "ack", "index": self._ack}, deadline)
                self._ack = None
            while True:
                text, value = self._read(deadline)
                if value.get("kind") == "input":
                    self._provide_input(value, deadline)
                    continue
                if value.get("kind") == "batch":
                    if (not self._stream_results or type(value.get("index")) is not int
                            or value["index"] != self._next_index):
                        raise ShardLoomProtocolError("native result batch sequence is invalid")
                    rows = value.get("rows")
                    if (not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows)
                            or len(rows) > self._batch_rows or type(value.get("row_count")) is not int
                            or value["row_count"] != len(rows)):
                        raise ShardLoomProtocolError("native result batch shape is invalid")
                    schema = schema_fields(json.dumps(value.get("schema")), "vortex.dtype.serde.v1")
                    if self._schema is not None and schema != self._schema:
                        raise ShardLoomProtocolError("native result schema changed between batches")
                    names = {name for name, _ in schema}
                    if any(set(row) != names for row in rows):
                        raise ShardLoomProtocolError("native result row differs from its schema")
                    self._schema = schema
                    self._ack = self._next_index
                    self._next_index += 1
                    self._rows += len(rows)
                    return ResultBatch(self._ack, tuple(rows), schema)
                envelope = self._client._parse_stdout(text, self._command)
                if len({field.key for field in envelope.fields}) != len(envelope.fields):
                    raise ShardLoomProtocolError("native batch final report has duplicate fields")
                remaining = None if deadline is None else max(0.001, deadline - time.monotonic())
                code = self._process.wait(timeout=remaining)
                if code != 0 or envelope.status != "success":
                    if self._check or self._stream_results:
                        raise ShardLoomCommandError(command=self._command, returncode=code or 2,
                                                    envelope=envelope, stderr="")
                elif (envelope.command != "run"
                      or envelope.field("native_batch_protocol") != "shardloom.native_batches.v1"
                      or self._ended_inputs != set(self._inputs)
                      or (self._stream_results and (
                          envelope.field_bool("result_payload_complete") is not True
                          or envelope.field_int("output_row_count") != self._rows
                          or envelope.field_int("native_result_batches_acknowledged") != self._next_index
                          or self._schema is None))):
                    raise ShardLoomProtocolError("native batch final report does not prove complete delivery")
                from .client import PublicWorkflowExecution
                self._finish_close()
                self._terminal_envelope = envelope
                if code == 0 and envelope.status == "success":
                    self.report = PublicWorkflowExecution(envelope)
                raise StopIteration
        except StopIteration as error:
            if self._terminal_envelope is None:
                self._close_after_error()
                raise ShardLoomProtocolError("native batch transaction ended without its final report") from error
            raise
        except BaseException:
            self._close_after_error()
            raise

    def _close_after_error(self):
        # Preserve the original producer/protocol/consumer exception even if a
        # producer's close also fails. _finish_close still visits every owner.
        try:
            self.close()
        except BaseException:
            pass

    def close(self):
        if self._closed:
            return
        try:
            self._stop_process()
        finally:
            self._finish_close()

    def _stop_process(self):
        if self._process is not None and self._process.poll() is None:
            try:
                self._write({"kind": "cancel"}, time.monotonic() + 0.5)
            except (OSError, ValueError, subprocess.TimeoutExpired):
                # EOF also signals cancellation to the native control reader.
                pass
            try:
                self._process.stdin.close()
                # Drain bounded protocol output while native cancellation unwinds
                # operator/sink owners. Never wait on a full child stdout pipe.
                deadline = time.monotonic() + 3
                while self._process.poll() is None:
                    try:
                        self._read(deadline)
                    except ShardLoomProtocolError:
                        self._process.wait(timeout=max(0.001, deadline - time.monotonic()))
                        break
            except (OSError, ValueError, subprocess.TimeoutExpired, ShardLoomProtocolError) as error:
                if self._process.poll() is not None:
                    return
                self._process.terminate()
                try:
                    self._process.wait(timeout=0.5)
                except subprocess.TimeoutExpired:
                    self._process.kill()
                    self._process.wait(timeout=0.5)
                raise ShardLoomProtocolError(
                    "native batch cancellation required forced process termination; "
                    "staged output cleanup could not be confirmed"
                ) from error

    def _finish_close(self):
        self._closed = True
        cleanup_error = None
        owners = list(self._iterators.values())
        if self._process is not None:
            owners.extend((self._process.stdin, self._process.stdout))
        self._iterators.clear()
        self._buffer.clear()
        for owner in owners:
            try:
                close = getattr(owner, "close", None)
                if close is not None:
                    close()
            except BaseException as error:
                if cleanup_error is None:
                    cleanup_error = error
        if cleanup_error is not None:
            raise cleanup_error

    def __del__(self):
        try:
            self.close()
        except Exception:
            pass
