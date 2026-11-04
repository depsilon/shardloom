"""Deferred implementations for query declarations.

Public classes, annotations and signatures remain in the canonical module.
Module-qualified lookups preserve its current shared helpers and patches.
"""

from __future__ import annotations

from . import query as _api


def WorkflowOperation_to_summary(self):
    if self.kind == "filter":
        return f"filter({self.values[0]})"
    if self.kind == "having":
        return f"having({self.values[0]})"
    if self.kind == "group_by" and self.grouping_clause is not None:
        return f"group_by({self.grouping_clause})"
    if self.kind == "select":
        return f"select({','.join(self.values)})"
    if self.kind == "limit":
        limit_parts = _api._limit_operation_parts(self.values)
        if limit_parts is None:
            return f"limit({','.join(self.values)})"
        count, offset = limit_parts
        if offset != "0":
            return f"limit({count}, offset={offset})"
        return f"limit({count})"
    return f"{self.kind}({','.join(self.values)})"


def LazyFrame_from_sql(cls, statement, client, input_uri, input_format, source_bindings, *, relations, parameters, resources, resource_limits):
    from ._connected import normalize_relations, normalize_parameters

    statement = _api._require_non_empty("SQL statement", statement)
    bound = normalize_relations(relations, client)
    values = normalize_parameters(parameters)
    if (bound or values) and getattr(client, "transport", "subprocess") != "native":
        raise ValueError("named relations and typed parameters require the in-process native transport")
    sources = tuple(source_bindings)
    if input_uri is not None and input_format is not None:
        primary = _api.WorkflowSource(input_format, input_uri)
        matching = tuple(source for source in sources if source.uri == input_uri)
        if any(_api._public_workflow_input_format(source) != input_format for source in matching):
            raise ValueError(f"conflicting format or schema declarations for source {input_uri!r}")
        if not matching:
            sources = (primary, *sources)
    frame = cls(
        _api.WorkflowSource("sql", "", relation=_api.SqlRelation(statement, sources, bound, values, input_uri, input_format)),
        client, resources=resources, resource_limits=resource_limits,
    )
    if getattr(client, "transport", "subprocess") == "native":
        plan = client._native_backend_for().declaration(frame)
        if bound:
            # Native binding accounts for lexical CTE scopes and source-free
            # relations. Physical source paths cannot identify these policies.
            used = frozenset(plan.bound_input_indices)
            limits = _api.merge_resource_limits(frame.resource_limits, _api._predicate_limits(
                tuple(value for index, (_, value) in enumerate(bound) if index in used)
            ))
            if limits != frame.resource_limits:
                frame = _api._dataclass_replace(frame, resource_limits=limits)
    return frame


def LazyFrame__declared_input_kwargs(self):
    if type(self) is _api.LazyFrame:
        return _copy_description(self._native_plan.describe("input_kwargs", lambda: _input_kwargs(self)))
    return _input_kwargs(self)


def _copy_description(value):
    # Public callers can mutate their request dictionaries. Copy only the
    # description's containers; native source and producer identities stay owned.
    if isinstance(value, dict):
        return {key: _copy_description(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_copy_description(item) for item in value]
    if isinstance(value, tuple):
        return tuple(_copy_description(item) for item in value)
    return value


def _input_kwargs(self):
    bindings = _api._workflow_source_bindings(self._declared_sources())
    payload: dict[str, Any] = {"source_bindings": bindings} if bindings else {}
    if self.source.relation is None:
        payload.update(input_uri=self.source.uri, input_format=_api._public_workflow_input_format(self.source))
    elif self.source.relation.input_uri is not None:
        payload.update(input_uri=self.source.relation.input_uri, input_format=self.source.relation.input_format)
    elif not payload and (embedded := _api._embedded_vortex_input_uri(self.source.relation.statement)) is not None:
        payload.update(input_uri=embedded, input_format="vortex")
    return payload


def LazyFrame___post_init__(self):
    limits = _api.merge_resource_limits(
        self.resource_limits, getattr(self.client, "resource_limits", None),
        _api._predicate_limits(self.operations),
    )
    resources = _api.optional_resources(
        resources=self.resources, inherited=getattr(self.client, "resources", None),
        limits=limits,
    )
    object.__setattr__(self, "resources", resources)
    object.__setattr__(self, "resource_limits", _api.merge_resource_limits(
        limits, resources.limits if resources is not None else None,
    ))
