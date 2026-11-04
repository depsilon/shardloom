"""Deferred implementations for query legacy lowering.

Public classes, annotations and signatures remain in the canonical module.
Module-qualified lookups preserve its current shared helpers and patches.
"""

from __future__ import annotations

from . import query as _api


def LazyFrame__has_structured_binary_export_shape(self):
    if self.source.source_format != "vortex":
        return False
    operations = [op for op in self.operations if op.kind != "set_index"]
    if len(operations) < 1:
        return False
    if operations[-1].kind == "limit":
        limit_parts = _api._limit_operation_parts(operations[-1].values)
        if limit_parts is None or limit_parts[1] != "0":
            return False
        if int(limit_parts[0]) <= 0:
            return False
        operations = operations[:-1]
    if not operations or operations[0].kind != "select":
        return False
    if not all(_api._is_sql_identifier(column) and column.lower() not in {"null", "true", "false"}
               for column in operations[0].values):
        return False
    for operation in operations[1:]:
        if operation.kind != "with_column" or len(operation.values) != 2:
            return False
        name, expression = operation.values
        if not _api._is_sql_identifier(name):
            return False
        expression_text = str(expression).strip()
        if expression_text.startswith("ARRAY[") and expression_text.endswith("]"):
            continue
        if expression_text.startswith("STRUCT(") and expression_text.endswith(")"):
            inner = expression_text[len("STRUCT(") : -1].strip()
            try:
                columns = _api._split_projection_function_args(inner)
            except ValueError:
                return False
            if columns and all(_api._is_sql_identifier(column) for column in columns):
                continue
        return False
    return True


def LazyFrame__expression_project_projection_columns(self, required_columns):
    projection_columns = self._unary_projection_columns()
    if projection_columns is None:
        return None
    required = tuple(dict.fromkeys(required_columns))
    if any(not _api._is_sql_identifier(column) for column in required):
        return None
    if projection_columns != ("*",) and any(column not in projection_columns for column in required):
        return None
    return projection_columns


def LazyFrame__duplicate_mask_projection_columns(self, subset):
    projection_columns = self._unary_projection_columns()
    if projection_columns is None:
        return None
    if subset is None:
        return projection_columns
    subset_columns = _api._normalize_columns((subset,))
    if not subset_columns or any(not _api._is_sql_identifier(column) for column in subset_columns):
        return None
    missing = tuple(column for column in subset_columns if column not in projection_columns) if projection_columns != ("*",) else ()
    if missing:
        raise ValueError(
            "duplicated subset referenced unknown declared/projection column(s): "
            + ", ".join(missing)
        )
    return subset_columns


def LazyFrame__expression_project_payload(self, rewrite_specs, *, required_columns):
    projection_columns = self._expression_project_projection_columns(required_columns)
    if projection_columns is None:
        return None
    # Declared source types remain hints only while stages preserve those
    # fields. Derived expressions bind their types in the native executor.
    schema = self.source.schema_map if all(
        operation.kind in {"filter", "select", "set_index", "sort", "limit", "tail", "sample", "distinct", "drop_duplicates"}
        and (operation.kind != "select" or all(_api._is_sql_identifier(value) for value in operation.values))
        for operation in self.operations
    ) else {}
    rewrites: list[dict[str, object]] = []
    for spec in rewrite_specs:
        kind = str(spec.get("kind", "")).strip()
        target_column = str(spec.get("target_column", "")).strip()
        if not kind or not target_column or not _api._is_sql_identifier(target_column):
            return None
        if kind == "row_number":
            start = spec.get("start", 0)
            if isinstance(start, bool) or not isinstance(start, int) or start < 0:
                return None
            rewrites.append(
                {
                    "kind": "row_number",
                    "target_column": target_column,
                    "start": start,
                }
            )
            continue
        target_dtype = schema.get(target_column)
        if kind == "mask_scalar":
            predicate = str(spec.get("predicate", "")).strip()
            replacement = _api._vortex_expression_scalar_payload(
                spec.get("replacement"),
                target_dtype=target_dtype,
            )
            if not predicate or replacement is None:
                return None
            rewrites.append(
                {
                    "kind": "mask_scalar",
                    "target_column": target_column,
                    "predicate": predicate,
                    "replacement": replacement,
                }
            )
        elif kind == "replace_scalar":
            to_replace = _api._vortex_expression_scalar_payload(
                spec.get("to_replace"),
                target_dtype=target_dtype,
                allow_null=False,
            )
            replacement = _api._vortex_expression_scalar_payload(
                spec.get("replacement"),
                target_dtype=target_dtype,
            )
            if to_replace is None or replacement is None:
                return None
            rewrites.append(
                {
                    "kind": "replace_scalar",
                    "target_column": target_column,
                    "to_replace": to_replace,
                    "replacement": replacement,
                }
            )
        elif kind == "string_replace_scalar":
            dtype = target_dtype.strip().lower().replace("-", "_") if target_dtype else None
            needle = spec.get("needle")
            replacement = spec.get("replacement")
            if (
                dtype not in {None, "utf8", "string", "str"}
                or not isinstance(needle, str)
                or needle == ""
                or not isinstance(replacement, str)
            ):
                return None
            rewrites.append(
                {
                    "kind": "string_replace_scalar",
                    "target_column": target_column,
                    "needle": needle,
                    "replacement": replacement,
                }
            )
        elif kind == "regex_replace_scalar":
            dtype = target_dtype.strip().lower().replace("-", "_") if target_dtype else None
            pattern = spec.get("pattern")
            replacement = spec.get("replacement")
            if (
                dtype not in {None, "utf8", "string", "str"}
                or not isinstance(pattern, str)
                or pattern == ""
                or not isinstance(replacement, str)
            ):
                return None
            rewrites.append(
                {
                    "kind": "regex_replace_scalar",
                    "target_column": target_column,
                    "pattern": pattern,
                    "replacement": replacement,
                }
            )
        elif kind == "numeric_scalar_arithmetic":
            operator = str(spec.get("operator", "")).strip()
            if operator not in {"+", "-", "*", "/"}:
                return None
            operand = _api._vortex_expression_scalar_payload(
                spec.get("operand"),
                target_dtype=target_dtype,
                allow_null=False,
            )
            if operand is None:
                return None
            rewrites.append(
                {
                    "kind": "numeric_scalar_arithmetic",
                    "target_column": target_column,
                    "operator": operator,
                    "operand": operand,
                }
            )
        elif kind == "forward_fill_null":
            limit = spec.get("limit")
            rewrite: dict[str, object] = {
                "kind": "forward_fill_null",
                "target_column": target_column,
            }
            if limit is not None:
                if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0:
                    return None
                rewrite["limit"] = limit
            rewrites.append(rewrite)
        else:
            return None
    if not rewrites:
        return None
    payload = {
        "columns": "*" if projection_columns == ("*",) else list(projection_columns),
        "rewrites": rewrites,
    }
    return _api.json.dumps(payload, sort_keys=True, separators=(",", ":"))


def LazyFrame__row_number_expression_project_payload(self, target_column):
    projection_columns = self._expression_project_projection_columns(())
    if projection_columns is None:
        return None
    return self._expression_project_payload(
        (
            {
                "kind": "row_number",
                "target_column": target_column,
                "start": 0,
            },
        ),
        required_columns=(),
    )


def LazyFrame__forward_fill_null_expression_project_payload(self, limit):
    projection_columns = self._expression_project_projection_columns(())
    if projection_columns is None or projection_columns == ("*",):
        return None
    return self._expression_project_payload(
        tuple(
            {
                "kind": "forward_fill_null",
                "target_column": column,
                "limit": limit,
            }
            if limit is not None
            else {
                "kind": "forward_fill_null",
                "target_column": column,
            }
            for column in projection_columns
        ),
        required_columns=projection_columns,
    )


def LazyFrame__replace_rewrite_specs(self, to_replace, value):
    projection_columns = self._expression_project_projection_columns(())
    if projection_columns is None:
        return None
    specs: list[Mapping[str, object]] = []
    if isinstance(to_replace, _api.Mapping):
        if not to_replace:
            return None
        replacement_map = value if isinstance(value, _api.Mapping) else None
        for raw_column, old_value in to_replace.items():
            column = str(raw_column).strip()
            if not _api._is_sql_identifier(column) or column not in projection_columns:
                return None
            if isinstance(old_value, _api.Mapping) and value is None:
                if not old_value:
                    return None
                for nested_old, nested_new in old_value.items():
                    specs.append(
                        {
                            "kind": "replace_scalar",
                            "target_column": column,
                            "to_replace": nested_old,
                            "replacement": nested_new,
                        }
                    )
                continue
            if replacement_map is not None:
                if raw_column not in replacement_map and column not in replacement_map:
                    return None
                new_value = replacement_map.get(raw_column, replacement_map.get(column))
            else:
                new_value = value
            specs.append(
                {
                    "kind": "replace_scalar",
                    "target_column": column,
                    "to_replace": old_value,
                    "replacement": new_value,
                }
            )
        return tuple(specs)
    if len(projection_columns) != 1:
        return None
    return (
        {
            "kind": "replace_scalar",
            "target_column": projection_columns[0],
            "to_replace": to_replace,
            "replacement": value,
        },
    )


def LazyFrame__regex_replace_rewrite_specs(self, to_replace, value):
    projection_columns = self._expression_project_projection_columns(())
    if projection_columns is None:
        return None
    specs: list[Mapping[str, object]] = []
    if isinstance(to_replace, _api.Mapping):
        if not to_replace:
            return None
        replacement_map = value if isinstance(value, _api.Mapping) else None
        for raw_column, pattern in to_replace.items():
            column = str(raw_column).strip()
            if not _api._is_sql_identifier(column) or column not in projection_columns:
                return None
            if not isinstance(pattern, str) or pattern == "":
                return None
            if replacement_map is not None:
                if raw_column not in replacement_map and column not in replacement_map:
                    return None
                replacement = replacement_map.get(raw_column, replacement_map.get(column))
            else:
                replacement = value
            if not isinstance(replacement, str):
                return None
            specs.append(
                {
                    "kind": "regex_replace_scalar",
                    "target_column": column,
                    "pattern": pattern,
                    "replacement": replacement,
                }
            )
        return tuple(specs)
    if (
        len(projection_columns) != 1
        or not isinstance(to_replace, str)
        or to_replace == ""
        or not isinstance(value, str)
    ):
        return None
    return (
        {
            "kind": "regex_replace_scalar",
            "target_column": projection_columns[0],
            "pattern": to_replace,
            "replacement": value,
        },
    )


def LazyFrame__string_replace_expression_project_payload(self, column_name, expression_sql):
    parsed = _api._vortex_string_replace_expression_parts(expression_sql)
    if parsed is None:
        return None
    source_column, needle, replacement = parsed
    if source_column != column_name:
        return None
    return self._expression_project_payload(
        (
            {
                "kind": "string_replace_scalar",
                "target_column": column_name,
                "needle": needle,
                "replacement": replacement,
            },
        ),
        required_columns=(column_name,),
    )


def LazyFrame__numeric_scalar_expression_project_payload(self, column_name, expression_sql):
    rewrite = _api._numeric_scalar_assignment_rewrite_from_expression(
        column_name,
        expression_sql,
    )
    if rewrite is None:
        return None
    return self._expression_project_payload(
        (rewrite,),
        required_columns=(column_name,),
    )


def LazyFrame__declared_projection_columns(self, *, allowed_operations):
    if not self.source.schema:
        return None
    declared_columns = tuple(name for name, _dtype in self.source.schema)
    if not declared_columns or any(not _api._is_sql_identifier(name) for name in declared_columns):
        return None
    projection_columns = declared_columns
    for operation in self.operations:
        if operation.kind == "select":
            if any(not _api._is_sql_identifier(value) for value in operation.values):
                return None
            projection_columns = operation.values
        elif operation.kind == "set_index":
            continue
        elif allowed_operations is not None and operation.kind in allowed_operations:
            continue
        else:
            return None
    return projection_columns


def LazyFrame__bounded_materialization_report(self, *, limit, check, memory_gb, memory_bytes, max_parallelism, resources):
    allocation = _api.resolve_resources(
        memory_gb=memory_gb, memory_bytes=memory_bytes,
        max_parallelism=max_parallelism, resources=resources,
        inherited=self.resources, limits=self.resource_limits,
    )
    if limit is not None:
        _api._validate_positive_row_count("materialization limit", limit)
    report = self.collect(limit=limit, check=check, resources=allocation)
    if isinstance(report, _api.UnsupportedWorkflowOperationReport):
        return report
    if report.status != "success":
        return _api.UnsupportedWorkflowOperationReport(report.workflow, "collect", report.envelope)
    return report
