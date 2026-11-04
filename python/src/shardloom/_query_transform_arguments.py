"""Deferred implementations for query transform arguments.

Public classes, annotations and signatures remain in the canonical module.
Module-qualified lookups preserve its current shared helpers and patches.
"""

from __future__ import annotations

from . import query as _api


def _client_from_config(client, client_config):
    if client is not None:
        if client_config:
            raise ValueError("client cannot be combined with client configuration arguments")
        if isinstance(client, _api.ShardLoomClient):
            client._ensure_open()
        return client
    config = dict(client_config)
    binary = config.pop("binary", None)
    env = config.pop("env", None)
    cwd = config.pop("cwd", None)
    repo_root = config.pop("repo_root", None)
    profile_order = config.pop("profile_order", None)
    timeout = config.pop("timeout", None)
    from ._native_backend import application_transport
    transport = application_transport(config.pop("transport", None), binary=binary, repo_root=repo_root, env=env)
    if config:
        unknown = ", ".join(sorted(str(key) for key in config))
        raise TypeError(f"unknown client configuration argument(s): {unknown}")
    if repo_root is not None:
        return _api.ShardLoomClient.from_repo(
            repo_root,
            binary=_api._optional_binary(binary),
            env=_api._optional_env(env),
            cwd=_api._optional_path(cwd),
            profile_order=_api._optional_profile_order(profile_order) or _api.DEFAULT_PROFILE_ORDER,
            timeout=_api._optional_timeout(timeout),
            transport=transport,
        )
    return _api.ShardLoomClient.from_env(
        env=_api._optional_env(env),
        binary=_api._optional_binary(binary),
        cwd=_api._optional_path(cwd),
        profile_order=_api._optional_profile_order(profile_order),
        timeout=_api._optional_timeout(timeout),
        transport=transport,
    )


def _prepared_vortex_target_path(source_uri, *, target_vortex_path, workspace):
    if target_vortex_path is not None and workspace is not None:
        raise ValueError(
            "prepare_vortex accepts either target_vortex_path or workspace, not both"
        )
    if target_vortex_path is not None:
        return target_vortex_path
    if workspace is None:
        raise ValueError(
            "prepare_vortex requires target_vortex_path or workspace=... so the "
            "caller-owned VortexPreparedState artifact location is explicit"
        )
    source_name = _api.Path(source_uri).name or "source"
    stem = _api.Path(source_name).stem or "source"
    return _api.Path(workspace).expanduser() / f"{stem}.vortex"


def _generated_prepared_vortex_target_path(stem, *, target_vortex_path, workspace):
    if target_vortex_path is not None and workspace is not None:
        raise ValueError(
            "generated prepare_vortex accepts either target_vortex_path or workspace, not both"
        )
    if target_vortex_path is not None:
        return target_vortex_path
    if workspace is None:
        raise ValueError(
            "generated prepare_vortex requires target_vortex_path or workspace=... so the "
            "caller-owned VortexPreparedState artifact location is explicit"
        )
    return _api.Path(workspace).expanduser() / f"{_api._safe_generated_vortex_stem(stem)}.vortex"


def _normalize_named_projection_items(context, columns, named_expressions):

    raw_items: list[tuple[object, object]] = []
    if columns is not None:
        if isinstance(columns, _api.Mapping):
            raw_items.extend(columns.items())
        elif _api._is_non_string_sequence(columns):
            for item in columns:
                if not _api._is_non_string_sequence(item) or len(item) != 2:
                    raise ValueError(
                        f"{context} sequence entries must be (name, expression) pairs"
                    )
                name, expression = item
                raw_items.append((name, expression))
        else:
            raise TypeError(
                f"{context} columns must be a mapping or sequence of (name, expression) pairs"
            )
    raw_items.extend(named_expressions.items())
    if not raw_items:
        raise ValueError(f"{context} expressions must not be empty")

    normalized: list[tuple[str, object]] = []
    seen: set[str] = set()
    for name, expression in raw_items:
        column_name = _api._normalize_output_column_name(name)
        if column_name in seen:
            raise ValueError(f"{context} output column names must be unique")
        seen.add(column_name)
        normalized.append((column_name, expression))
    return tuple(normalized)


def _normalize_rename_items(context, columns, named_columns):

    raw_items: list[tuple[object, object]] = []
    if columns is not None:
        if isinstance(columns, _api.Mapping):
            raw_items.extend(columns.items())
        elif _api._is_non_string_sequence(columns):
            for item in columns:
                if not _api._is_non_string_sequence(item) or len(item) != 2:
                    raise ValueError(f"{context} sequence entries must be (source, target) pairs")
                source, target = item
                raw_items.append((source, target))
        else:
            raise TypeError(
                f"{context} columns must be a mapping or sequence of (source, target) pairs"
            )
    raw_items.extend(named_columns.items())
    if not raw_items:
        raise ValueError(f"{context} columns must not be empty")

    normalized: list[tuple[str, str]] = []
    seen_sources: set[str] = set()
    seen_targets: set[str] = set()
    for source, target in raw_items:
        source_name = _api._require_non_empty("source column name", source)
        target_name = _api._normalize_output_column_name(target)
        if source_name in seen_sources:
            raise ValueError(f"{context} source column names must be unique")
        if target_name in seen_targets:
            raise ValueError(f"{context} target column names must be unique")
        seen_sources.add(source_name)
        seen_targets.add(target_name)
        normalized.append((source_name, target_name))
    return tuple(normalized)


def _normalize_drop_columns(labels, columns):
    raw_columns: list[object] = []
    if len(labels) == 1 and _api._is_non_string_sequence(labels[0]):
        raw_columns.extend(labels[0])
    else:
        raw_columns.extend(labels)
    if columns is not None:
        if _api._is_non_string_sequence(columns):
            raw_columns.extend(columns)
        else:
            raw_columns.append(columns)
    values = [_api._require_non_empty("drop column", column) for column in raw_columns]
    if not values:
        raise ValueError("drop columns must not be empty")
    duplicates = [column for column in dict.fromkeys(values) if values.count(column) > 1]
    if duplicates:
        raise ValueError("drop columns must be unique")
    return tuple(values)


def _normalize_sample_target(*, n, fraction, seed, weights, replace):
    if n is not None and fraction is not None:
        raise ValueError("sample accepts either n or fraction, not both")
    if not isinstance(replace, bool):
        raise TypeError("sample replace must be boolean")
    parts: list[str] = []
    if fraction is None:
        sample_n = 1 if n is None else _api._normalize_non_negative_int("sample n", n)
        parts.append(f"n={sample_n}")
    else:
        if isinstance(fraction, bool) or not isinstance(fraction, (int, float)):
            raise TypeError("sample fraction must be numeric")
        fraction_value = float(fraction)
        if not _api.math.isfinite(fraction_value) or fraction_value <= 0:
            raise ValueError("sample fraction must be positive and finite")
        parts.append(f"fraction={fraction_value:.12g}")
    if seed is not None:
        if isinstance(seed, bool) or not isinstance(seed, int):
            parts.append(f"seed={_api._stable_target_value(seed)}")
        else:
            parts.append(f"seed={_api._normalize_non_negative_int('sample seed', seed)}")
    if weights is not None:
        parts.append(f"weights={_api._stable_target_value(weights)}")
    if replace:
        parts.append("replace=true")
    return ",".join(parts)


def _sample_operation_parts(values):
    sample_count = None
    sample_fraction = None
    sample_seed = 0
    sample_with_replacement = False
    sample_weight_column = None
    try:
        if any("=" in value for value in values):
            parsed_sample: int | None = None
            parsed_fraction: float | None = None
            parsed_seed = 0
            parsed_replacement = False
            parsed_weight_column: str | None = None
            for value in values:
                if value in {"replacement", "replace=true"}:
                    if parsed_replacement:
                        return None
                    parsed_replacement = True
                elif value.startswith("n="):
                    if parsed_sample is not None:
                        return None
                    parsed_sample = int(value.removeprefix("n="))
                elif value.startswith("fraction=") or value.startswith("frac="):
                    if parsed_fraction is not None:
                        return None
                    _, raw_fraction = value.split("=", 1)
                    parsed_fraction = float(raw_fraction)
                elif value.startswith("seed=") or value.startswith("random_state="):
                    _, raw_seed = value.split("=", 1)
                    parsed_seed = int(raw_seed)
                elif value.startswith("weights=") or value.startswith("weight="):
                    if parsed_weight_column is not None:
                        return None
                    _, raw_weight_column = value.split("=", 1)
                    if not _api._is_sql_identifier(raw_weight_column):
                        return None
                    parsed_weight_column = raw_weight_column
                else:
                    return None
            if (parsed_sample is None) == (parsed_fraction is None):
                return None
            if parsed_sample is not None and parsed_sample <= 0:
                return None
            if parsed_fraction is not None and (
                not _api.math.isfinite(parsed_fraction)
                or parsed_fraction <= 0
                or parsed_fraction > 1
            ):
                return None
            if parsed_seed < 0:
                return None
            sample_count = parsed_sample
            sample_fraction = parsed_fraction
            sample_seed = parsed_seed
            sample_with_replacement = parsed_replacement
            sample_weight_column = parsed_weight_column
        elif values and values[0] == "fraction":
            if len(values) < 2:
                return None
            parsed_fraction = float(values[1])
            parsed_seed = int(values[2]) if len(values) > 2 else 0
            parsed_replacement = (
                len(values) > 3 and values[3] == "replacement"
            )
            if len(values) > (4 if parsed_replacement else 3):
                return None
            if (
                not _api.math.isfinite(parsed_fraction)
                or parsed_fraction <= 0
                or parsed_fraction > 1
                or parsed_seed < 0
            ):
                return None
            sample_fraction = parsed_fraction
            sample_seed = parsed_seed
            sample_with_replacement = parsed_replacement
        else:
            parsed_sample = int(values[0])
            parsed_seed = int(values[1]) if len(values) > 1 else 0
            parsed_replacement = len(values) > 2 and values[2] == "replacement"
            if len(values) > (3 if parsed_replacement else 2):
                return None
            if parsed_sample <= 0 or parsed_seed < 0:
                return None
            sample_count = parsed_sample
            sample_seed = parsed_seed
            sample_with_replacement = parsed_replacement
    except (ValueError, TypeError, IndexError):
        return None
    return sample_count, sample_fraction, sample_seed, sample_with_replacement, sample_weight_column


def _normalize_sample_seed_alias(*, seed, random_state):
    if seed is None:
        return random_state
    if random_state is None:
        return seed
    normalized_seed = _api._normalize_non_negative_int("sample seed", seed)
    normalized_random_state = _api._normalize_non_negative_int(
        "sample random_state",
        random_state,
    )
    if normalized_seed != normalized_random_state:
        raise ValueError("sample seed and random_state must match when both are provided")
    return normalized_seed


def _normalize_merge_target(other, *, on, left_on, right_on, how, kwargs):
    normalized_how = _api._normalize_join_how(how)
    if on is not None and (left_on is not None or right_on is not None):
        raise ValueError("merge accepts either on= or left_on=/right_on=, not both")
    parts = [f"how={normalized_how}"]
    if on is not None:
        parts.append(f"on={_api._join_columns_for_target('merge on', on)}")
    elif left_on is not None or right_on is not None:
        if left_on is None or right_on is None:
            raise ValueError("merge requires both left_on and right_on when using sided keys")
        parts.append(f"left_on={_api._join_columns_for_target('merge left_on', left_on)}")
        parts.append(f"right_on={_api._join_columns_for_target('merge right_on', right_on)}")
    else:
        parts.append("on=implicit_common_columns")
    parts.extend(_api._normalize_extra_kwargs("merge", kwargs))
    parts.append(_api._workflow_target_summary(other))
    return ";".join(parts)


def _normalize_concat_target(others, *, axis, join, kwargs):
    if isinstance(axis, bool) or axis not in (0, 1):
        raise ValueError("concat axis must be 0 or 1")
    normalized_join = _api._require_non_empty("concat join", join).lower()
    if normalized_join not in {"inner", "outer"}:
        raise ValueError("concat join must be inner or outer")
    targets = _api._normalize_workflow_targets(others)
    parts = [f"axis={axis}", f"join={normalized_join}"]
    parts.extend(_api._normalize_extra_kwargs("concat", kwargs))
    parts.extend(targets)
    return ";".join(parts)


def _normalize_melt_target(*, id_vars, value_vars, var_name, value_name, ignore_index, kwargs):
    parts = [
        f"id_vars={_api._optional_columns_for_target(id_vars)}",
        f"value_vars={_api._optional_columns_for_target(value_vars)}",
        f"ignore_index={str(bool(ignore_index)).lower()}",
    ]
    if var_name is not None:
        parts.append(f"var_name={_api._require_non_empty('melt var_name', var_name)}")
    if value_name is not None:
        parts.append(f"value_name={_api._require_non_empty('melt value_name', value_name)}")
    parts.extend(_api._normalize_extra_kwargs("melt", kwargs))
    return ";".join(parts)


def _normalize_fillna_target(value, *, axis, inplace, method, limit, kwargs):
    parts = [
        f"value={_api._stable_target_value(value)}",
        f"axis={_api._normalize_dropna_axis(axis)}",
        f"inplace={str(bool(inplace)).lower()}",
    ]
    if method is not None:
        parts.append(f"method={_api._stable_target_value(method)}")
    if limit is not None:
        parts.append(f"limit={_api._stable_target_value(limit)}")
    parts.extend(_api._normalize_extra_kwargs("fillna", kwargs))
    return ";".join(parts)


def _dropna_threshold_predicate(columns, threshold):
    if not columns:
        return None
    if threshold > len(columns):
        column = columns[0]
        return f"{column} IS NOT NULL AND {column} IS NULL"
    if threshold == 1:
        return " OR ".join(f"{column} IS NOT NULL" for column in columns)
    if threshold == len(columns):
        return " AND ".join(f"{column} IS NOT NULL" for column in columns)
    if _api.math.comb(len(columns), threshold) > 128:
        return None
    clauses = []
    for group in _api.combinations(columns, threshold):
        clauses.append(
            "(" + " AND ".join(f"{column} IS NOT NULL" for column in group) + ")"
        )
    return " OR ".join(clauses)


def _normalize_dropna_target(*, subset, how, axis, thresh, kwargs):
    parts = [
        f"subset={_api._optional_columns_for_target(subset)}",
        f"how={_api._normalize_dropna_how(how)}",
        f"axis={_api._normalize_dropna_axis(axis)}",
    ]
    if thresh is not None:
        parts.append(f"thresh={_api._normalize_non_negative_int('dropna thresh', thresh)}")
    parts.extend(_api._normalize_extra_kwargs("dropna", kwargs))
    return ";".join(parts)


def _normalize_astype_dtype_map(dtype, projection_columns):
    if isinstance(dtype, _api.Mapping):
        if not dtype:
            return None
        dtype_map: dict[str, str] = {}
        for raw_column, raw_dtype in dtype.items():
            column = _api._require_non_empty("astype column", raw_column)
            if not _api._is_sql_identifier(column):
                raise ValueError("astype column names admit only bare SQL identifiers")
            dtype_map[column] = _api._normalize_cast_dtype(raw_dtype)
        return dtype_map
    normalized_dtype = _api._normalize_cast_dtype(dtype)
    return {column: normalized_dtype for column in projection_columns}


def _normalize_mask_target(*, cond, other, axis, inplace, level, kwargs):
    parts = [
        f"cond={_api._require_non_empty('mask condition', cond)}",
        f"other={_api._stable_target_value(other)}",
        f"axis={_api._normalize_dropna_axis(axis)}",
        f"inplace={str(bool(inplace)).lower()}",
        f"level={_api._stable_target_value(level)}",
    ]
    parts.extend(_api._normalize_extra_kwargs("mask", kwargs))
    return ";".join(parts)


def _normalize_replace_target(*, to_replace, value, regex, inplace, method, limit, kwargs):
    parts = [
        f"to_replace={_api._stable_target_value(to_replace)}",
        f"value={_api._stable_target_value(value)}",
        f"regex={str(bool(regex)).lower()}",
        f"inplace={str(bool(inplace)).lower()}",
        f"method={_api._stable_target_value(method)}",
        f"limit={_api._stable_target_value(limit)}",
    ]
    parts.extend(_api._normalize_extra_kwargs("replace", kwargs))
    return ";".join(parts)


def _eval_numeric_scalar_assignment_rewrites(expr, *, allow_zero_division):
    if not isinstance(expr, str):
        return None
    try:
        parsed = _api.ast.parse(_api._require_non_empty("eval expression", expr), mode="exec")
    except SyntaxError:
        return None
    if not parsed.body:
        return None
    rewrites: list[dict[str, object]] = []
    target_columns: set[str] = set()
    for statement in parsed.body:
        if not isinstance(statement, _api.ast.Assign):
            return None
        if len(statement.targets) != 1 or not isinstance(statement.targets[0], _api.ast.Name):
            return None
        target_column = _api._normalize_output_column_name(statement.targets[0].id)
        if target_column in target_columns:
            return None
        target_columns.add(target_column)
        value = statement.value
        if not isinstance(value, _api.ast.BinOp) or not isinstance(value.left, _api.ast.Name):
            return None
        if _api._normalize_output_column_name(value.left.id) != target_column:
            return None
        rewrite = _api._numeric_scalar_assignment_rewrite_from_binop(
            target_column, value, allow_zero_division=allow_zero_division
        )
        if rewrite is None:
            return None
        rewrites.append(rewrite)
    return tuple(rewrites) if rewrites else None


def _numeric_scalar_assignment_rewrite_from_expression(target_column, expression_sql):
    target_column = _api._normalize_output_column_name(target_column)
    try:
        parsed = _api.ast.parse(
            _api._require_non_empty("numeric assignment expression", expression_sql),
            mode="eval",
        )
    except SyntaxError:
        return None
    value = parsed.body
    if not isinstance(value, _api.ast.BinOp) or not isinstance(value.left, _api.ast.Name):
        return None
    if _api._normalize_output_column_name(value.left.id) != target_column:
        return None
    return _api._numeric_scalar_assignment_rewrite_from_binop(target_column, value)


def _numeric_scalar_assignment_rewrite_from_binop(target_column, value, *, allow_zero_division):
    operator = _api._eval_numeric_scalar_operator(value.op)
    if operator is None:
        return None
    operand = _api._eval_numeric_scalar_literal(value.right)
    if operand is None:
        return None
    if operator == "/" and float(operand) == 0.0 and not allow_zero_division:
        return None
    return {
        "kind": "numeric_scalar_arithmetic",
        "target_column": target_column,
        "operator": operator,
        "operand": operand,
    }


def _eval_numeric_scalar_operator(operator):
    if isinstance(operator, _api.ast.Add):
        return "+"
    if isinstance(operator, _api.ast.Sub):
        return "-"
    if isinstance(operator, _api.ast.Mult):
        return "*"
    if isinstance(operator, _api.ast.Div):
        return "/"
    return None


def _eval_numeric_scalar_literal(node):
    if isinstance(node, _api.ast.Constant) and not isinstance(node.value, bool):
        if isinstance(node.value, (int, float)) and _api.math.isfinite(float(node.value)):
            return node.value
        return None
    if (
        isinstance(node, _api.ast.UnaryOp)
        and isinstance(node.op, _api.ast.USub)
        and isinstance(node.operand, _api.ast.Constant)
        and not isinstance(node.operand.value, bool)
        and isinstance(node.operand.value, (int, float))
    ):
        value = -node.operand.value
        return value if _api.math.isfinite(float(value)) else None
    return None


def _stable_callable_name(function):
    if isinstance(function, str):
        return _api._require_non_empty("callable expression", function)
    name = getattr(function, "__name__", None)
    if isinstance(name, str) and name.strip():
        return name.strip()
    type_name = type(function).__name__
    if type_name:
        return type_name
    return _api._require_non_empty("callable", function)


def _normalize_workflow_targets(value):
    if isinstance(value, _api.LazyFrame) or isinstance(value, (str, _api.os.PathLike)):
        targets = (_api._workflow_target_summary(value),)
    elif _api._is_non_string_sequence(value):
        targets = tuple(_api._workflow_target_summary(item) for item in value)
    else:
        raise TypeError("concat others must be a workflow, path, or sequence of workflows/paths")
    if not targets:
        raise ValueError("concat others must not be empty")
    return targets


def _normalize_join_how(value):
    normalized = _api._require_non_empty("join how", value).lower().replace("-", "_")
    aliases = {
        "inner": "inner",
        "inner_equi": "inner",
        "left": "left",
        "left_outer": "left",
        "right": "right",
        "right_outer": "right",
        "full": "full",
        "full_outer": "full",
        "outer": "full",
        "semi": "semi",
        "left_semi": "semi",
        "anti": "anti",
        "left_anti": "anti",
        "cross": "cross",
    }
    try:
        return aliases[normalized]
    except KeyError as exc:
        raise ValueError(
            "join how must be one of inner, left, right, full, semi, anti, or cross"
        ) from exc


def _normalize_temporal_grid_keys(by, time_column):
    if isinstance(by, str):
        raw_keys: tuple[object, ...] = (by,)
    elif by is None:
        raise TypeError("temporal grid partition keys must be a string or iterable of strings")
    else:
        try:
            raw_keys = tuple(_api.cast(_api.Iterable[object], by))
        except TypeError as exc:
            raise TypeError(
                "temporal grid partition keys must be a string or iterable of strings"
            ) from exc
    keys = tuple(_api._normalize_temporal_grid_column(key, "partition key") for key in raw_keys)
    if len(set(keys)) != len(keys):
        raise ValueError("temporal grid partition keys must be unique")
    if time_column in keys:
        raise ValueError("temporal grid partition keys must not include the time column")
    return keys


def _normalize_temporal_grid_policies(month_end, ambiguity):
    if not isinstance(month_end, str):
        raise TypeError("temporal month_end must be a string literal")
    if month_end not in {"reject", "clamp"}:
        raise ValueError("temporal month_end must be 'reject' or 'clamp'")
    if not isinstance(ambiguity, str):
        raise TypeError("temporal ambiguity must be a string literal")
    if ambiguity not in {"reject", "earlier", "later"}:
        raise ValueError("temporal ambiguity must be 'reject', 'earlier', or 'later'")
    return month_end, ambiguity


def _normalize_cast_dtype(value):
    if isinstance(value, _api.ResultType):
        value = "boolean" if value.name == "bool" else value.label
    raw_dtype = _api._require_non_empty("cast dtype", value)
    temporal = _api.parse_temporal_declaration(raw_dtype)
    if temporal is not None:
        return temporal
    dtype = raw_dtype.lower()
    if dtype == "timestamp":
        dtype = "timestamp_micros"
    if dtype in {"blob", "varbinary"}:
        dtype = "binary"
    decimal_dtype = _api._normalize_decimal_cast_dtype(dtype)
    if decimal_dtype is not None:
        return decimal_dtype
    if dtype not in {
        "int64",
        "uint64",
        "float64",
        "utf8",
        "boolean",
        "date32",
        "timestamp_micros",
        "binary",
    }:
        raise ValueError(
            "cast dtype must be one of ('int64', 'uint64', 'float64', 'utf8', 'boolean', 'date32', 'timestamp_micros', 'binary', 'decimal128(p,s)')"
        )
    return dtype
