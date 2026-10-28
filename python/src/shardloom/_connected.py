"""Inert relation binding and context-local names for the shared native engine."""

from __future__ import annotations

import re
import threading
from collections.abc import Mapping
from dataclasses import replace
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from ._temporal import temporal_parts


def identifier(value, *, label="relation name"):
    if not isinstance(value, str) or re.fullmatch(r"[A-Za-z_][A-Za-z_0-9]*", value) is None:
        raise ValueError(f"{label} must be an ordinary SQL identifier")
    return value.lower()


def normalize_relations(relations, client):
    from .query import LazyFrame

    if relations is None:
        return ()
    if not isinstance(relations, Mapping):
        raise TypeError("relations must map names to lazy DataFrames")
    values = {}
    for name, frame in relations.items():
        name = identifier(name)
        if name in values:
            raise ValueError(f"duplicate relation name {name!r}")
        if not isinstance(frame, LazyFrame):
            raise TypeError(f"relation {name!r} must be a lazy DataFrame")
        if frame.client is not client:
            raise ValueError("relation belongs to another context; use the same execution owner")
        values[name] = frame
    return tuple(values.items())


def normalize_parameters(parameters):
    from .types import _ScalarParameter
    if parameters is None:
        return ()
    if not isinstance(parameters, Mapping):
        raise TypeError("parameters must map names to typed scalar values")
    values = []
    for name, value in parameters.items():
        identifier(name, label="parameter name")
        scalar = value.value if type(value) is _ScalarParameter else value
        temporal_parameter = type(value) is _ScalarParameter and temporal_parts(value.dtype.name) is not None
        if type(scalar) not in (type(None), bool, int, float, str, bytes, date, datetime, Decimal) and not (temporal_parameter and type(scalar) in (time, timedelta)):
            raise TypeError(f"parameter {name!r} requires a supported scalar value")
        if type(scalar) is datetime and scalar.tzinfo is not None and not temporal_parameter:
            raise ValueError("timestamp parameters currently require a naive microsecond timestamp; timezone conversion is explicit")
        if type(scalar) is Decimal and not scalar.is_finite():
            raise ValueError("decimal parameters must be finite")
        values.append((name, value))
    return tuple(values)


def native_parameter(module, value):
    from .types import _ScalarParameter
    expression = module.Expression
    if type(value) is _ScalarParameter:
        dtype = value.dtype
        name = f"decimal128({dtype.parameters[0]},{dtype.parameters[1]})" if dtype.name == "decimal128" else dtype.name
        if value.value is not None and temporal_parts(name) is not None:
            from ._input_schema import temporal_input_value
            ticks = temporal_input_value(name, value.value)
            if name == "date32":
                return expression.date32(ticks)
            if name == "timestamp_micros":
                return expression.timestamp_micros(ticks)
            return expression.temporal(ticks, name)
        return native_parameter(module, value.value).cast(name)
    if type(value) is Decimal:
        sign, digits, exponent = value.as_tuple()
        scale = max(0, -exponent)
        precision = max(len(digits) + max(0, exponent), scale, 1)
        if precision > 38 or scale > 38:
            raise ValueError("decimal parameter exceeds Decimal128 precision/scale")
        unscaled = 0
        for digit in digits:
            unscaled = unscaled * 10 + digit
        unscaled *= 10 ** max(0, exponent)
        return expression.decimal128(-unscaled if sign else unscaled, precision, scale)
    if type(value) is datetime:
        delta = value - datetime(1970, 1, 1)
        micros = (delta.days * 86_400 + delta.seconds) * 1_000_000 + delta.microseconds
        return expression.timestamp_micros(micros)
    if type(value) is date:
        return expression.date32((value - date(1970, 1, 1)).days)
    return expression.literal(value)


class Catalog:
    """Temporary immutable plan snapshots owned by one explicit context.

    Registering a view never reads data. Replacement and drop affect future
    bindings; already constructed queries keep their relation/source owners.
    """

    def __init__(self, client):
        self._client = client
        self._views = {}
        self._lock = threading.RLock()

    def register(self, name, frame, *, replace=False):
        if not isinstance(replace, bool):
            raise TypeError("replace must be a bool")
        name, frame = normalize_relations({name: frame}, self._client)[0]
        self._client._ensure_open()
        if self._client.transport != "native":
            raise ValueError("temporary catalogs require the in-process native transport")
        # Parsing and binding are inert; no schema/payload or producer is opened.
        self._client._native_backend_for().declaration(frame)
        with self._lock:
            if name in self._views and not replace:
                raise ValueError(f"relation {name!r} already exists; use replace=True")
            self._views[name] = frame
        return frame

    def table(self, name):
        name = identifier(name)
        self._client._ensure_open()
        with self._lock:
            try:
                return self._views[name]
            except KeyError:
                raise KeyError(f"unknown relation {name!r}") from None

    def list(self):
        self._client._ensure_open()
        with self._lock:
            return tuple(sorted(self._views))

    def drop(self, name, *, if_exists=False):
        name = identifier(name)
        self._client._ensure_open()
        if not isinstance(if_exists, bool):
            raise TypeError("if_exists must be a bool")
        with self._lock:
            if name not in self._views:
                if if_exists:
                    return False
                raise KeyError(f"unknown relation {name!r}")
            del self._views[name]
        return True

    def schema(self, name, **resources):
        """Explicit schema inspection under the selected action's resources."""
        return self.table(name).schema(**resources)

    def bindings(self, relations=None):
        local = normalize_relations(relations, self._client)
        with self._lock:
            result = dict(self._views)
        result.update(local)
        return result

    def clear(self):
        with self._lock:
            self._views.clear()


def _input_arguments(backend, sources, *, relations=()):
    from ._native_backend import _native_fields
    from .client import _schema_command_arg
    from .query import _workflow_source_bindings, _public_workflow_input_format

    # Validate conflicting declarations before constructing native handles.
    _workflow_source_bindings(sources)
    files, owners, batches, ranges = [], [], [], []
    relations = list(relations)
    seen = set()
    for source in sources:
        if source.uri in seen:
            continue
        seen.add(source.uri)
        if source._bound_frame is not None:
            relations.append((source.uri, source._bound_frame._native_plan.plan))
        elif source.native_input is not None:
            owners.append((source.uri, source.native_input))
        elif source.memory_input:
            memory = dict(source.memory_input)
            if memory["kind"] == "range":
                start, end, step = memory["start"], memory["end"], memory["step"]
                if memory.get("inclusive"):
                    end += 1 if step > 0 else -1
                ranges.append((source.uri, memory["column"], start, step, len(range(start, end, step))))
            elif memory["kind"] == "batches":
                producer = source.batch_input
                batches.append((source.uri, producer, _native_fields(producer._types), memory.get("streaming", False)))
            else:
                raise ValueError("native object input requires an owned native source")
        else:
            files.append((source.uri, _public_workflow_input_format(source), _schema_command_arg(source.schema or None)))
    return dict(files=files, sources=owners, batches=batches, ranges=ranges, relations=relations)


def _dependencies(frame):
    root = frame.source.relation
    if root is not None:
        yield from (relation for _, relation in root.relations)
    yield from (operation.right_frame for operation in frame.operations if operation.right_frame is not None)
    yield from (source._bound_frame for source in frame._declared_sources() if source._bound_frame is not None)


def native_declaration(backend, frame, spill=None):
    """Bind a DAG iteratively, without rendering child plans into parent SQL."""
    from ._native_backend import _checked_native_call
    from ._relational_sql import _render_stages
    from .query import _quote_sql_local_source_path

    key = (id(backend), spill)
    pending = [(frame, False)]
    active = set()
    while pending:
        current, ready = pending.pop()
        state = current._native_plan
        with state.lock:
            cached = state.key == key and state.plan is not None
        if cached:
            continue
        if not ready:
            if id(current) in active:
                raise ValueError("cyclic relation declaration is not supported")
            active.add(id(current))
            pending.append((current, True))
            for dependency in reversed(tuple(_dependencies(current))):
                if dependency.client is not frame.client:
                    raise ValueError("relation belongs to another context")
                pending.append((dependency, False))
            continue
        active.remove(id(current))
        source = current.source
        root = source.relation
        # Preserve the existing complete-statement provider for ordinary local
        # plans. Only actual bound children/values require declaration composition;
        # this never expands a child relation or executes an intermediate.
        if len(current.operations) <= 8 and (root is None or not root.relations and not root.parameters) and all(
            operation.right_frame is None and operation.kind != "observe" for operation in current.operations
        ):
            statement = current._relation_statement()
            if statement is None:
                raise ValueError("DataFrame stage has no admitted native relational lowering")
            sources = current._declared_sources()
            plan = _checked_native_call(backend.session.sql, statement,
                **_input_arguments(backend, sources), spill=spill)
            with state.lock:
                state.key, state.plan = key, plan
                state.references = frozenset(plan.source_references)
            with backend._plans_lock:
                backend._plans.add(state)
            continue
        with source._native_plan.lock:
            if source._native_plan.key == key:
                plan = source._native_plan.plan
                references = set(source._native_plan.references)
            else:
                sources = root.source_bindings if root is not None else (source,)
                relations = [] if root is None else [(name, value._native_plan.plan) for name, value in root.relations]
                statement = root.statement if root is not None else f"SELECT * FROM {_quote_sql_local_source_path(source.uri)}"
                parameters = [] if root is None else [(name, native_parameter(backend.module, value)) for name, value in root.parameters]
                plan = _checked_native_call(backend.session.sql, statement,
                    **_input_arguments(backend, sources, relations=relations), parameters=parameters, spill=spill)
                references = set(plan.source_references)
                source._native_plan.key, source._native_plan.plan = key, plan
                source._native_plan.references = frozenset(references)
        with backend._plans_lock:
            backend._plans.add(source._native_plan)
        columns = tuple(name for name, _ in source.schema) or None
        operations = iter(current.operations)
        queued = None
        while True:
            operation = queued if queued is not None else next(operations, None)
            queued = None
            if operation is None:
                break
            if operation.kind == "observe":
                plan = _checked_native_call(plan.observe, operation.values[0], list(operation.values[1:]))
                continue
            group = [operation]
            if operation.kind == "group_by":
                aggregate = next(operations, None)
                if aggregate is None or aggregate.kind != "aggregate":
                    raise ValueError("group_by requires an aggregate before execution")
                group.append(aggregate)
            if group[-1].kind == "aggregate":
                queued = next(operations, None)
                if queued is not None and queued.kind == "having":
                    group.append(queued)
                    queued = None
            relations = [("__sl_input", plan)]
            if operation.right_frame is not None:
                child = operation.right_frame
                relations.append(("__sl_right", child._native_plan.plan))
                references.update(child._native_plan.references)
                group[0] = replace(operation, right_statement="SELECT * FROM __sl_right")
            rendered = _render_stages("SELECT * FROM __sl_input", tuple(group), columns)
            if rendered is None:
                raise ValueError("DataFrame stage has no admitted native relational lowering")
            extra = tuple(item for op in group for item in op.source_bindings if item.uri not in references)
            plan = _checked_native_call(backend.session.sql, rendered.statement,
                **_input_arguments(backend, extra, relations=relations), spill=spill)
            references = set(plan.source_references)
            columns = rendered.columns
        with state.lock:
            state.key, state.plan, state.references = key, plan, frozenset(references)
        with backend._plans_lock:
            backend._plans.add(state)
    return frame._native_plan.plan
