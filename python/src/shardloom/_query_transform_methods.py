"""Deferred implementations for query transform methods.

Public classes, annotations and signatures remain in the canonical module.
Module-qualified lookups preserve its current shared helpers and patches.
"""

from __future__ import annotations

from . import query as _api


def LazyFrame_dropna(self, *, subset, how, axis, thresh, check, **kwargs):

    target_ref = _api._normalize_dropna_target(
        subset=subset,
        how=how,
        axis=axis,
        thresh=thresh,
        kwargs=kwargs,
    )
    predicate = self._schema_declared_dropna_predicate(
        subset,
        how=how,
        axis=axis,
        thresh=thresh,
        kwargs=kwargs,
    )
    if predicate is not None:
        if predicate == "":
            return self
        return self._with_filter_condition(predicate)
    if self.source.source_format == "vortex" and not kwargs:
        normalized_how = _api._normalize_dropna_how(how)
        if normalized_how == "any" and subset is not None and thresh is None:
            target_columns = _api._normalize_columns((subset,))
            if target_columns and all(_api._is_sql_identifier(column) for column in target_columns):
                return self._with_filter_condition(
                    " AND ".join(f"{column} IS NOT NULL" for column in target_columns)
                )
    return self._unsupported_operation("dropna", target_ref, check=check)


def LazyFrame_astype(self, dtype, *, errors, check, **kwargs):

    target_ref = _api._normalize_astype_target(dtype=dtype, errors=errors, kwargs=kwargs)
    projection = self._schema_declared_astype_projection(
        dtype,
        errors=errors,
        kwargs=kwargs,
    )
    if projection is not None:
        return self._with_rewritten_projection(projection)
    if self.source.source_format == "vortex":
        return self._unsupported_operation("native-vortex-cast", target_ref, check=check)
    return self._unsupported_operation("astype", target_ref, check=check)


def LazyFrame_sample(self, n, fraction, seed, *, frac, random_state, weights, replace, check):

    effective_fraction = _api._normalize_sample_fraction_alias(
        fraction=fraction,
        frac=frac,
    )
    effective_seed = _api._normalize_sample_seed_alias(
        seed=seed,
        random_state=random_state,
    )
    target_ref = _api._normalize_sample_target(
        n=n,
        fraction=effective_fraction,
        seed=effective_seed,
        weights=weights,
        replace=replace,
    )
    if effective_seed is not None and (
        isinstance(effective_seed, bool) or not isinstance(effective_seed, int)
    ):
        return self._unsupported_operation("sample", target_ref, check=check)
    sample_weight_column = _api._normalize_sample_weight_column(weights)
    if weights is not None and sample_weight_column is None:
        return self._unsupported_operation("sample", target_ref, check=check)
    sample_seed = (
        0
        if effective_seed is None
        else _api._normalize_non_negative_int("sample seed", effective_seed)
    )
    if effective_fraction is not None:
        sample_fraction = _api._normalize_sample_fraction_value(effective_fraction)
        values = ["fraction", f"{sample_fraction:.12g}", str(sample_seed)]
        if sample_weight_column is not None:
            values = [
                f"fraction={sample_fraction:.12g}",
                f"seed={sample_seed}",
                f"weights={sample_weight_column}",
            ]
        if replace is True:
            values.append("replacement")
        return self._append(
            _api.WorkflowOperation(
                "sample",
                tuple(values),
            )
        )
    sample_n = 1 if n is None else _api._normalize_non_negative_int("sample n", n)
    _api._validate_positive_row_count("sample n", sample_n)
    if sample_weight_column is not None:
        values = [f"n={sample_n}", f"seed={sample_seed}", f"weights={sample_weight_column}"]
        if replace is True:
            values.append("replacement")
        return self._append(_api.WorkflowOperation("sample", tuple(values)))
    if replace is True:
        return self._append(
            _api.WorkflowOperation("sample", (str(sample_n), str(sample_seed), "replacement"))
        )
    return self._append(_api.WorkflowOperation("sample", (str(sample_n), str(sample_seed))))


def LazyFrame_merge(self, other, *, on, left_on, right_on, how, check, **kwargs):

    target_ref = _api._normalize_merge_target(
        other,
        on=on,
        left_on=left_on,
        right_on=right_on,
        how=how,
        kwargs=kwargs,
    )
    if (
        not kwargs
        and on is not None
        and left_on is None
        and right_on is None
        and self._can_lower_merge_to_join(other, on=on, how=how)
    ):
        joined = self.join(other, on=on, how=how, check=check)
        if isinstance(joined, _api.LazyFrame):
            return joined
    return self._unsupported_operation("merge", target_ref, check=check)


def LazyFrame_concat(self, others, *, axis, join, check, **kwargs):

    target_ref = _api._normalize_concat_target(others, axis=axis, join=join, kwargs=kwargs)
    other = _api._single_lazyframe_target(others)
    projection = self._explicit_set_operation_projection_columns()
    other_projection = (
        other._explicit_set_operation_projection_columns() if other is not None else None
    )
    if (
        axis == 0
        and not kwargs
        and other is not None
        and projection == other_projection
        and projection is not None
    ):
        return self.union_all(other, check=check)
    return self._unsupported_operation("concat", target_ref, check=check)


def LazyFrame_pivot(self, *, index, columns, values, check, **kwargs):

    target_ref = _api._normalize_pivot_target(
        index=index,
        columns=columns,
        values=values,
        kwargs=kwargs,
    )
    payload = _api._vortex_pivot_projection_payload(
        index=index,
        columns=columns,
        values=values,
        aggregate="first_unique",
        kwargs=kwargs,
    )
    if payload is not None:
        return self._append(_api.WorkflowOperation("pivot", (payload,)))
    return self._unsupported_operation("pivot", target_ref, check=check)


def LazyFrame_pivot_table(self, *, values, index, columns, aggfunc, check, **kwargs):

    target_ref = _api._normalize_pivot_table_target(
        values=values,
        index=index,
        columns=columns,
        aggfunc=aggfunc,
        kwargs=kwargs,
    )
    aggregate = _api._normalize_pivot_table_single_aggregate(
        aggfunc,
        values=values,
    )
    payload = _api._vortex_pivot_projection_payload(
        index=index,
        columns=columns,
        values=values,
        aggregate=aggregate or "",
        kwargs=kwargs,
    )
    if payload is not None:
        return self._append(_api.WorkflowOperation("pivot", (payload,)))
    return self._unsupported_operation("pivot-table", target_ref, check=check)


def LazyFrame_melt(self, *, id_vars, value_vars, var_name, value_name, ignore_index, check, **kwargs):

    target_ref = _api._normalize_melt_target(
        id_vars=id_vars,
        value_vars=value_vars,
        var_name=var_name,
        value_name=value_name,
        ignore_index=ignore_index,
        kwargs=kwargs,
    )
    effective_value_vars = value_vars
    if effective_value_vars is None and not kwargs and ignore_index:
        effective_value_vars = self._schema_declared_melt_value_columns(id_vars)
    if not ignore_index and not kwargs:
        try:
            original_id_columns = _api._normalize_optional_columns(id_vars)
            value_columns = (
                _api._normalize_columns((value_vars,))
                if value_vars is not None
                else self._schema_declared_melt_value_columns(id_vars)
            )
        except (TypeError, ValueError):
            value_columns = None
        if value_columns:
            indexed = self.reset_index()
            if isinstance(indexed, _api.LazyFrame):
                id_columns = ("index", *original_id_columns)
                payload = _api._vortex_melt_projection_payload(
                    id_vars=id_columns,
                    value_vars=value_columns,
                    var_name=var_name,
                    value_name=value_name,
                    kwargs=kwargs,
                )
                if payload is not None:
                    candidate = indexed._append(_api.WorkflowOperation("melt", (payload,)))
                    if candidate._relation_statement() is not None:
                        return candidate
    if ignore_index and (
        payload := _api._vortex_melt_projection_payload(
            id_vars=id_vars,
            value_vars=effective_value_vars,
            var_name=var_name,
            value_name=value_name,
            kwargs=kwargs,
        )
    ) is not None:
        return self._append(_api.WorkflowOperation("melt", (payload,)))
    return self._unsupported_operation("melt", target_ref, check=check)


def LazyFrame_rolling(self, window, *, min_periods, center, check, **kwargs):

    target_ref = _api._normalize_rolling_target(
        window,
        min_periods=min_periods,
        center=center,
        kwargs=kwargs,
    )
    if kwargs:
        return self._unsupported_operation("rolling", target_ref, check=check)
    try:
        window_size = _api._normalize_positive_int("rolling window", window)
        min_window_periods = (
            window_size
            if min_periods is None
            else _api._normalize_positive_int("rolling min_periods", min_periods)
        )
    except (TypeError, ValueError):
        return self._unsupported_operation("rolling", target_ref, check=check)
    if min_window_periods > window_size:
        return self._unsupported_operation("rolling", target_ref, check=check)
    return _api.RollingFrame(
        frame=self,
        window=window_size,
        min_periods=min_window_periods,
        center=bool(center),
    )


def LazyFrame_duplicated(self, subset, *, keep, check, **kwargs):

    target_ref = _api._normalize_duplicated_target(subset=subset, keep=keep, kwargs=kwargs)
    normalized_keep = _api._normalize_duplicate_keep_value(keep)
    if not kwargs and normalized_keep in {"first", "last", "false"}:
        columns = self._duplicate_mask_projection_columns(subset)
        if columns is not None:
            return self._append(
                _api.WorkflowOperation("duplicate_mask", (*columns, f"keep={normalized_keep}"))
            )
    return self._unsupported_operation("duplicated", target_ref, check=check)


def LazyFrame_nunique(self, *columns, dropna, check, **kwargs):

    target_ref = _api._normalize_distinct_count_target(
        columns,
        dropna=dropna,
        kwargs=kwargs,
    )
    normalized_columns = _api._normalize_columns(columns)
    if (
        not kwargs
        and dropna is True
        and len(normalized_columns) == 1
        and self._can_append_nunique(normalized_columns[0])
    ):
        return self.agg(unique_count=_api.count_distinct(normalized_columns[0]))
    return self._unsupported_operation("nunique", target_ref, check=check)


def LazyFrame_value_counts(self, *columns, sort, dropna, check, **kwargs):

    target_ref = _api._normalize_value_counts_target(
        columns,
        sort=sort,
        dropna=dropna,
        kwargs=kwargs,
    )
    normalized_columns = _api._normalize_columns(columns)
    if not kwargs and self._can_append_value_counts(normalized_columns):
        frame = self
        if dropna:
            frame = self._with_filter_condition(
                " AND ".join(
                    f"{column} IS NOT NULL" for column in normalized_columns
                )
            )
        grouped = frame.group_by(*normalized_columns).count(alias="rows")
        if isinstance(grouped, _api.LazyFrame):
            return grouped.sort("rows", descending=True) if sort else grouped
    return self._unsupported_operation("value-counts", target_ref, check=check)


def LazyFrame__top_n_by_columns(self, operation, n, columns, *, descending, keep, check):
    normalized_n = _api._normalize_top_n_count(operation, n)
    normalized_columns = _api._normalize_columns((columns,))
    normalized_keep = _api._normalize_top_n_keep(operation, keep)
    target_ref = _api._normalize_top_n_target(
        n=normalized_n,
        columns=normalized_columns,
        keep=normalized_keep,
    )
    if normalized_keep in {"first", "last", "all"} and self._can_append_sort(
        normalized_columns
    ):
        sort_values = (
            ("desc" if descending else "asc", *normalized_columns)
            if normalized_keep == "first"
            else (
                "desc" if descending else "asc",
                f"keep={normalized_keep}",
                *normalized_columns,
            )
        )
        sorted_frame = self._append(
            _api.WorkflowOperation(
                "sort",
                sort_values,
            )
        )
        return sorted_frame.limit(normalized_n)
    if self.source.source_format == "vortex":
        sort_values = (
            ("desc" if descending else "asc", *normalized_columns)
            if normalized_keep == "first"
            else (
                "desc" if descending else "asc",
                f"keep={normalized_keep}",
                *normalized_columns,
            )
        )
        sorted_frame = self._append(
            _api.WorkflowOperation(
                "sort",
                sort_values,
            )
        )
        return sorted_frame.limit(normalized_n)
    return self._unsupported_operation(operation, target_ref, check=check)


def LazyFrame_fillna(self, value, *, axis, inplace, method, limit, check, **kwargs):

    target_ref = _api._normalize_fillna_target(
        value,
        axis=axis,
        inplace=inplace,
        method=method,
        limit=limit,
        kwargs=kwargs,
    )
    if (
        not kwargs
        and value is None
        and _api._normalize_dropna_axis(axis) == "rows"
        and not inplace
        and (method_name := _api._normalize_fill_method(method)) == "ffill"
    ):
        fill_limit = _api._normalize_optional_positive_int("fillna limit", limit)
        payload = self._forward_fill_null_expression_project_payload(fill_limit)
        if payload is not None:
            return self._append(_api.WorkflowOperation("expression_project", (payload,)))
    if (
        not kwargs
        and method is None
        and limit is None
        and _api._normalize_dropna_axis(axis) in {"rows", "columns"}
        and not inplace
        and (projection := self._schema_declared_fillna_projection(value))
    ):
        return self._with_rewritten_projection(projection)
    return self._unsupported_operation("fillna", target_ref, check=check)


def LazyFrame_fill_null(self, value, *, axis, inplace, method, limit, check, **kwargs):

    return self.fillna(
        value,
        axis=axis,
        inplace=inplace,
        method=method,
        limit=limit,
        check=check,
        **kwargs,
    )


def LazyFrame_mask(self, cond, other, *, axis, inplace, level, check, **kwargs):

    target_ref = _api._normalize_mask_target(
        cond=cond,
        other=other,
        axis=axis,
        inplace=inplace,
        level=level,
        kwargs=kwargs,
    )
    if (
        not kwargs
        and _api._normalize_dropna_axis(axis) == "rows"
        and not inplace
        and level is None
    ):
        predicate = _api._vortex_tiny_predicate_from_sql(_api._predicate_sql(cond))
        target_column = (
            _api._vortex_tiny_predicate_column(predicate) if predicate is not None else None
        )
        if target_column is not None:
            payload = self._expression_project_payload(
                (
                    {
                        "kind": "mask_scalar",
                        "target_column": target_column,
                        "predicate": predicate,
                        "replacement": other,
                    },
                ),
                required_columns=(target_column,),
            )
            if payload is not None:
                return self._append(_api.WorkflowOperation("expression_project", (payload,)))
    return self._unsupported_operation("mask", target_ref, check=check)


def LazyFrame_replace(self, to_replace, value, *, regex, inplace, method, limit, check, **kwargs):

    target_ref = _api._normalize_replace_target(
        to_replace=to_replace,
        value=value,
        regex=regex,
        inplace=inplace,
        method=method,
        limit=limit,
        kwargs=kwargs,
    )
    if (
        not kwargs
        and not inplace
        and method is None
        and limit is None
        and to_replace is not None
    ):
        rewrite_specs = (
            self._regex_replace_rewrite_specs(to_replace, value)
            if regex
            else self._replace_rewrite_specs(to_replace, value)
        )
        if rewrite_specs is not None:
            payload = self._expression_project_payload(
                rewrite_specs,
                required_columns=tuple(
                    str(spec["target_column"]) for spec in rewrite_specs
                ),
            )
            if payload is not None:
                return self._append(_api.WorkflowOperation("expression_project", (payload,)))
    return self._unsupported_operation("replace", target_ref, check=check)


def LazyFrame_apply(self, function, *args, check, **kwargs):

    if isinstance(function, _api.WorkflowPlanTransform):
        result = function.apply(self, *args, **kwargs)
        if isinstance(result, _api.LazyFrame):
            return result
        return self._unsupported_operation(
            "apply",
            f"plan_transform={function.name};return_type={type(result).__name__}",
            check=check,
        )
    target_ref = _api._normalize_callable_transform_target(
        "apply",
        function,
        args,
        kwargs,
    )
    return self._unsupported_operation("apply", target_ref, check=check)


def LazyFrame_pipe(self, function, *args, check, **kwargs):

    if isinstance(function, _api.WorkflowPlanTransform):
        result = function.apply(self, *args, **kwargs)
        if isinstance(result, _api.LazyFrame):
            return result
        return self._unsupported_operation(
            "pipe",
            f"plan_transform={function.name};return_type={type(result).__name__}",
            check=check,
        )
    target_ref = _api._normalize_callable_transform_target(
        "pipe",
        function,
        args,
        kwargs,
    )
    return self._unsupported_operation("pipe", target_ref, check=check)


def LazyFrame_transform(self, function, *args, check, **kwargs):

    if isinstance(function, _api.WorkflowPlanTransform):
        return self.pipe(function, *args, check=check, **kwargs)
    if isinstance(function, _api.WorkflowColumnTransform) and not args and not kwargs:
        return self.with_columns(function.items, check=check)
    if not args and not kwargs and isinstance(function, _api.Mapping):
        return self.with_columns(function, check=check)
    target_ref = _api._normalize_callable_transform_target(
        "transform",
        function,
        args,
        kwargs,
    )
    return self._unsupported_operation("transform", target_ref, check=check)


def LazyFrame_applymap(self, function, *args, check, **kwargs):

    if isinstance(function, _api.WorkflowColumnTransform) and not args and not kwargs:
        return self.with_columns(function.items, check=check)
    target_ref = _api._normalize_callable_transform_target(
        "applymap",
        function,
        args,
        kwargs,
    )
    return self._unsupported_operation("applymap", target_ref, check=check)


def LazyFrame_map_rows(self, function, *args, check, **kwargs):

    if isinstance(function, _api.WorkflowRowTransform) and not args and not kwargs:
        return self.with_columns(function.items, check=check)
    target_ref = _api._normalize_callable_transform_target(
        "map_rows",
        function,
        args,
        kwargs,
    )
    return self._unsupported_operation("map-rows", target_ref, check=check)


def LazyFrame_eval(self, expr, *, check, **kwargs):

    target_ref = _api._normalize_eval_target(expr, kwargs)
    if not kwargs:
        rewrite_specs = _api._eval_numeric_scalar_assignment_rewrites(
            expr, allow_zero_division=self.client.float_policy == "extended"
        )
        if rewrite_specs is not None:
            target_columns = tuple(
                str(rewrite_spec["target_column"])
                for rewrite_spec in rewrite_specs
            )
            payload = self._expression_project_payload(
                rewrite_specs,
                required_columns=target_columns,
            )
            if payload is not None:
                return self._append(_api.WorkflowOperation("expression_project", (payload,)))
    return self._unsupported_operation("eval", target_ref, check=check)


def LazyFrame_drop_duplicates(self, subset, *, keep, check, **kwargs):

    if not kwargs:
        normalized_keep = _api._normalize_duplicate_keep_value(keep)
        projection_columns = self._unary_projection_columns()
        try:
            subset_columns = (
                projection_columns
                if subset is None and projection_columns is not None
                else _api._normalize_columns((subset,))
            )
        except (TypeError, ValueError):
            subset_columns = ()
        if (
            normalized_keep in {"first", "last", "false"}
            and projection_columns is not None
            and subset_columns
            and (subset_columns == ("*",) or all(_api._is_sql_identifier(column) for column in subset_columns))
            and all(column == "*" or _api._is_sql_identifier(column) for column in projection_columns)
            and (projection_columns == ("*",) or all(column in projection_columns for column in subset_columns))
        ):
            return self._append(
                _api.WorkflowOperation(
                    "drop_duplicates",
                    (*subset_columns, f"keep={normalized_keep}"),
                )
            )
    target_ref = _api._normalize_duplicated_target(
        subset=subset,
        keep=keep,
        kwargs=kwargs,
    )
    return self._unsupported_operation("drop-duplicates", target_ref, check=check)


def LazyFrame_set_index(self, keys, *, drop, check, **kwargs):

    target_ref = _api._normalize_index_target(
        "set_index",
        keys=keys,
        drop=drop,
        kwargs=kwargs,
    )
    index_columns = _api._normalize_index_columns(keys)
    if drop is False and not kwargs:
        return self._append(_api.WorkflowOperation("set_index", index_columns))
    return self._unsupported_operation("set-index", target_ref, check=check)


def LazyFrame_reset_index(self, *, check, **kwargs):

    if kwargs == {"drop": True}:
        if _api._workflow_index_columns(self.operations):
            return _api.LazyFrame(
                source=self.source,
                client=self.client,
                operations=_api._strip_index_metadata_operations(self.operations),
                engine_mode=self.engine_mode,
                resources=self.resources, resource_limits=self.resource_limits,
            )
        return self
    if not kwargs or kwargs == {"drop": False}:
        base = _api.LazyFrame(
            source=self.source,
            client=self.client,
            operations=_api._strip_index_metadata_operations(self.operations),
            engine_mode=self.engine_mode,
            resources=self.resources, resource_limits=self.resource_limits,
        )
        projection_columns = base._expression_project_projection_columns(())
        if projection_columns is not None:
            target_column = (
                "index"
                if "index" not in projection_columns
                else "__shardloom_row_number"
            )
            payload = base._row_number_expression_project_payload(target_column)
            if payload is not None:
                return base._append(_api.WorkflowOperation("expression_project", (payload,)))
        if _api._workflow_index_columns(self.operations):
            return base
    target_ref = _api._normalize_index_target("reset_index", keys=None, kwargs=kwargs)
    return self._unsupported_operation("reset-index", target_ref, check=check)


def LazyFrame_with_column(self, name, expression, *, check):

    column_name = _api._normalize_output_column_name(name)
    expression_sources = _api._predicate_sources(expression)
    expression_limits = _api._predicate_limits(expression)
    try:
        literal = (
            expression
            if isinstance(expression, (_api.Decimal, bytes, bytearray, _api.date))
            else _api._generated_literal_expression(expression)
        )
        expression_sql = "NULL" if literal is None else _api._sql_literal(literal)
    except (TypeError, ValueError):
        try:
            expression_sql = _api._sql_computed_projection_expression(expression)
        except (TypeError, ValueError):
            expression_text = _api._require_non_empty("column expression", expression)
            if self.source.source_format == "vortex" and _api._sql_text_looks_like_cast(
                expression_text
            ):
                return self._append(
                    _api.WorkflowOperation("with_column", (column_name, expression_text), expression_sources, resource_limits=expression_limits)
                )
            return self._unsupported_operation(
                "with-column",
                f"{column_name}={expression_text}",
                check=check,
            )
    if expression_project_payload := self._numeric_scalar_expression_project_payload(
        column_name,
        expression_sql,
    ):
        return self._append(
            self._computed_projection_operation(
                expression_project_payload, column_name, expression_sql,
                source_bindings=expression_sources,
                resource_limits=expression_limits,
            )
        )
    if expression_project_payload := self._string_replace_expression_project_payload(
        column_name,
        expression_sql,
    ):
        return self._append(
            self._computed_projection_operation(
                expression_project_payload, column_name, expression_sql,
                source_bindings=expression_sources,
                resource_limits=expression_limits,
            )
        )
    if (self.source.source_format == "vortex"
            and isinstance(expression, _api.ComplexProjectionExpression)
            and self._can_append_projection_column(column_name, allow_vortex=True)):
        projected = self._append(_api.WorkflowOperation(
            "with_column", (column_name, expression_sql), expression_sources,
            resource_limits=expression_limits,
        ))
        if projected._has_structured_binary_export_shape():
            return projected
    if self._can_append_projection_column(column_name, allow_vortex=True):
        return self._append(_api.WorkflowOperation(
            "with_column", (column_name, expression_sql), expression_sources,
            resource_limits=expression_limits,
        ))
    if self.source.source_format == "vortex" and _api._sql_text_looks_like_cast(expression_sql):
        return self._append(_api.WorkflowOperation(
            "with_column", (column_name, expression_sql), expression_sources,
            resource_limits=expression_limits,
        ))
    return self._unsupported_operation(
        "with-column",
        f"{column_name}={expression_sql}",
        check=check,
    )


def LazyFrame__computed_projection_operation(self, payload, name, expression, *, source_bindings, resource_limits):
    from ._relational_sql import computed_projection

    columns = self._expression_project_projection_columns(())
    if columns == ("*",):
        return _api.WorkflowOperation("with_column", (name, expression), source_bindings, resource_limits=resource_limits)
    return _api.WorkflowOperation(
        "expression_project", (payload,),
        source_bindings=source_bindings,
        projection_sql=computed_projection(columns, name, expression) if columns is not None else None,
        resource_limits=resource_limits,
    )


def LazyFrame_with_columns(self, columns, *, check, **named_expressions):

    items = _api._normalize_named_projection_items(
        "with_columns",
        columns,
        named_expressions,
    )
    workflow: LazyFrame | UnsupportedWorkflowOperationReport = self
    for name, expression in items:
        if isinstance(workflow, _api.UnsupportedWorkflowOperationReport):
            return workflow
        workflow = workflow.with_column(name, expression, check=check)
    return workflow
