//! AST-bound window function admission and lowering for canonical native SQL plans.

use super::{
    CanonicalLowering, ColumnRef, Expression, NativeResult, NativeWindowFunction, Plan, Project,
    ScopeColumn, ScopedRelation, Window, WindowExpression, agg_call_admitted, column,
    fresh_col_names, make_order_key, rewrite_agg_and_group_expr, unsupported_sql_error,
};
use shardloom_core::ScalarValue;
use shardloom_plan::sql_binder::{
    LogicalExpr, LogicalProjectionItem, LogicalRelation, LogicalSortKey, LogicalWindowFrame,
    LogicalWindowSpec, LogicalWindowUnits,
};
use shardloom_vortex::{
    query_primitive::VortexSimpleAggregateMeasure,
    relational_query::{
        VortexRelationalFrameFunction as FrameFunction, VortexRelationalWindowFrame,
    },
};
use std::collections::BTreeSet;

/// Returns whether a bound SQL window expression (`OVER (...)`) is admitted natively.
pub(super) fn window_call_admitted(
    name: &str,
    args: &[LogicalExpr],
    distinct: bool,
    filter: Option<&LogicalExpr>,
    order_by: &[LogicalSortKey],
    spec: &LogicalWindowSpec,
    forbidden_sources: &BTreeSet<String>,
) -> bool {
    if filter.is_some() || !order_by.is_empty() {
        return false;
    }
    if !args
        .iter()
        .all(|arg| super::expr_supported_with_sources(arg, forbidden_sources))
        || !spec
            .partition_by
            .iter()
            .all(|expr| super::expr_supported_with_sources(expr, forbidden_sources))
        || !spec
            .order_by
            .iter()
            .all(|k| super::expr_supported_with_sources(&k.expr, forbidden_sources))
    {
        return false;
    }
    if let Some(frame) = &spec.frame
        && convert_window_frame(frame).is_err()
    {
        return false;
    }
    let lower_name = name.to_ascii_lowercase();
    match lower_name.as_str() {
        "row_number" | "rank" | "dense_rank" | "percent_rank" | "cume_dist" => {
            !distinct && args.is_empty()
        }
        "ntile" => !distinct && args.len() == 1 && positive_window_literal(&args[0]).is_some(),
        "lag" | "lead" => {
            !distinct
                && (args.len() == 1
                    || (args.len() == 2 && positive_window_literal(&args[1]).is_some()))
                && !matches!(args[0], LogicalExpr::Wildcard { .. })
        }
        "first_value" | "last_value" => {
            !distinct && args.len() == 1 && !matches!(args[0], LogicalExpr::Wildcard { .. })
        }
        "nth_value" => {
            !distinct
                && args.len() == 2
                && !matches!(args[0], LogicalExpr::Wildcard { .. })
                && positive_window_literal(&args[1]).is_some()
        }
        "count" => {
            args.len() == 1
                && ((matches!(args[0], LogicalExpr::Wildcard { qualifier: None }) && !distinct)
                    || !matches!(args[0], LogicalExpr::Wildcard { .. }))
        }
        "sum" | "avg" | "min" | "max" => {
            !distinct && args.len() == 1 && !matches!(args[0], LogicalExpr::Wildcard { .. })
        }
        _ => {
            !args
                .iter()
                .any(|arg| matches!(arg, LogicalExpr::Wildcard { .. }))
                && agg_call_admitted(&lower_name, args, distinct)
                && !matches!(
                    lower_name.as_str(),
                    "group_concat" | "string_agg" | "percentile_cont" | "percentile_disc"
                )
        }
    }
}

fn positive_window_literal(expr: &LogicalExpr) -> Option<u64> {
    match expr {
        LogicalExpr::Literal(ScalarValue::Int64(v)) if *v >= 1 => u64::try_from(*v).ok(),
        LogicalExpr::Literal(ScalarValue::UInt64(v)) if *v >= 1 => Some(*v),
        _ => None,
    }
}

pub(super) fn convert_window_frame(
    frame: &LogicalWindowFrame,
) -> NativeResult<VortexRelationalWindowFrame> {
    let unit = match frame.units {
        LogicalWindowUnits::Rows => "ROWS",
        LogicalWindowUnits::Range => "RANGE",
        LogicalWindowUnits::Groups => "GROUPS",
    };
    let raw = match (&frame.end, &frame.exclusion) {
        (Some(end), Some(excl)) => {
            format!("{unit} BETWEEN {} AND {end} EXCLUDE {excl}", frame.start)
        }
        (Some(end), None) => format!("{unit} BETWEEN {} AND {end}", frame.start),
        (None, Some(excl)) => format!("{unit} {} EXCLUDE {excl}", frame.start),
        (None, None) => format!("{unit} {}", frame.start),
    };
    super::super::super::window_frames::parse_frame(&raw)
}

fn collect_expr_window_calls(expr: &LogicalExpr, out: &mut Vec<LogicalExpr>) {
    match expr {
        LogicalExpr::Function {
            args,
            filter,
            order_by,
            over: Some(spec),
            ..
        } => {
            for arg in args {
                collect_expr_window_calls(arg, out);
            }
            if let Some(f) = filter {
                collect_expr_window_calls(f, out);
            }
            for key in order_by {
                collect_expr_window_calls(&key.expr, out);
            }
            for p in &spec.partition_by {
                collect_expr_window_calls(p, out);
            }
            for k in &spec.order_by {
                collect_expr_window_calls(&k.expr, out);
            }
            if !out.iter().any(|existing| existing == expr) {
                out.push(expr.clone());
            }
        }
        LogicalExpr::Function {
            args,
            filter,
            order_by,
            ..
        } => {
            for arg in args {
                collect_expr_window_calls(arg, out);
            }
            if let Some(f) = filter {
                collect_expr_window_calls(f, out);
            }
            for key in order_by {
                collect_expr_window_calls(&key.expr, out);
            }
        }
        LogicalExpr::Unary { expr: inner, .. } | LogicalExpr::Cast { expr: inner, .. } => {
            collect_expr_window_calls(inner, out);
        }
        LogicalExpr::Binary { left, right, .. }
        | LogicalExpr::Compare { left, right, .. }
        | LogicalExpr::IsDistinctFrom { left, right, .. }
        | LogicalExpr::Like {
            expr: left,
            pattern: right,
            ..
        } => {
            collect_expr_window_calls(left, out);
            collect_expr_window_calls(right, out);
        }
        LogicalExpr::Between {
            expr, low, high, ..
        } => {
            collect_expr_window_calls(expr, out);
            collect_expr_window_calls(low, out);
            collect_expr_window_calls(high, out);
        }
        LogicalExpr::InList { expr, list, .. } => {
            collect_expr_window_calls(expr, out);
            for item in list {
                collect_expr_window_calls(item, out);
            }
        }
        LogicalExpr::Case {
            operand,
            when_then,
            else_expr,
        } => {
            if let Some(op) = operand {
                collect_expr_window_calls(op, out);
            }
            for (w, t) in when_then {
                collect_expr_window_calls(w, out);
                collect_expr_window_calls(t, out);
            }
            if let Some(e) = else_expr {
                collect_expr_window_calls(e, out);
            }
        }
        LogicalExpr::Array(items) | LogicalExpr::Tuple(items) => {
            for item in items {
                collect_expr_window_calls(item, out);
            }
        }
        _ => {}
    }
}

fn rewrite_window_calls(expr: &LogicalExpr, win_map: &[(LogicalExpr, String)]) -> LogicalExpr {
    for (target, col) in win_map {
        if expr == target {
            return LogicalExpr::Column {
                qualifier: None,
                name: col.clone(),
            };
        }
    }
    super::map_logical_children(expr, &|child| rewrite_window_calls(child, win_map))
}

impl CanonicalLowering<'_, '_, '_> {
    /// Lowers a bare `LogicalRelation::Window` node (used if nested outside `Project`).
    pub(super) fn lower_bare_window(
        &mut self,
        input: &LogicalRelation,
        windows: &[(String, LogicalWindowSpec)],
    ) -> NativeResult<ScopedRelation> {
        let base_scope = self.lower_relation(input)?;
        let synthetic_items: Vec<LogicalProjectionItem> = windows
            .iter()
            .enumerate()
            .map(|(idx, (name, spec))| LogicalProjectionItem::Expr {
                expr: Box::new(LogicalExpr::Function {
                    name: name.clone(),
                    args: Vec::new(),
                    distinct: false,
                    filter: None,
                    order_by: Vec::new(),
                    over: Some(spec.clone()),
                }),
                alias: Some(format!("win_{idx}")),
            })
            .collect();
        let (win_scope, _) = self.apply_window_stage(base_scope, &synthetic_items, None)?;
        Ok(win_scope)
    }

    /// Lowers `Project -> Window -> Aggregate` in canonical SQL evaluation order.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn lower_window_aggregate_project(
        &mut self,
        agg_input: &LogicalRelation,
        group_by: &[LogicalExpr],
        having: Option<&LogicalExpr>,
        items: &[LogicalProjectionItem],
        distinct: bool,
        outer_sort: Option<&[LogicalSortKey]>,
    ) -> NativeResult<ScopedRelation> {
        let (mut post_scope, agg_map, group_map, alias_exprs) =
            self.lower_aggregate_prefix(agg_input, group_by, having, items, outer_sort, &[])?;
        post_scope =
            self.materialize_having(post_scope, having, &alias_exprs, &agg_map, &group_map)?;
        let rewritten_items: Vec<LogicalProjectionItem> = items
            .iter()
            .enumerate()
            .map(|(idx, item)| match item {
                LogicalProjectionItem::Wildcard { .. } => {
                    LogicalProjectionItem::Wildcard { qualifier: None }
                }
                LogicalProjectionItem::Expr { expr, alias } => {
                    let out_alias = alias.clone().unwrap_or_else(|| match expr.as_ref() {
                        LogicalExpr::Column { name, .. } => name.clone(),
                        _ => format!("col_{idx}"),
                    });
                    LogicalProjectionItem::Expr {
                        expr: Box::new(rewrite_agg_and_group_expr(expr, &agg_map, &group_map)),
                        alias: Some(out_alias),
                    }
                }
            })
            .collect();
        let rewritten_sort = outer_sort.map(|keys| {
            keys.iter()
                .map(|key| LogicalSortKey {
                    expr: if let LogicalExpr::Column {
                        qualifier: None,
                        name,
                    } = &key.expr
                        && super::items_has_alias(items, name)
                    {
                        key.expr.clone()
                    } else {
                        let aliased = super::substitute_alias_expr(&key.expr, &alias_exprs);
                        rewrite_agg_and_group_expr(&aliased, &agg_map, &group_map)
                    },
                    descending: key.descending,
                    nulls_first: key.nulls_first,
                })
                .collect::<Vec<_>>()
        });
        self.lower_window_project_over_scope(
            post_scope,
            &rewritten_items,
            distinct,
            rewritten_sort.as_deref(),
        )
    }

    /// Lowers a `Project` stage over an already-lowered input scope that may include window calls.
    pub(super) fn lower_window_project_over_scope(
        &mut self,
        mut scope: ScopedRelation,
        items: &[LogicalProjectionItem],
        distinct: bool,
        outer_sort: Option<&[LogicalSortKey]>,
    ) -> NativeResult<ScopedRelation> {
        let mut materialized_items = items.to_vec();
        for item in &mut materialized_items {
            if let LogicalProjectionItem::Expr { expr, .. } = item {
                scope = super::subquery::materialize_expr_subqueries(self, scope, expr)?;
            }
        }
        let mut materialized_sort = outer_sort.map(ToOwned::to_owned);
        if let Some(keys) = materialized_sort.as_mut() {
            for key in keys {
                scope = super::subquery::materialize_expr_subqueries(self, scope, &mut key.expr)?;
            }
        }
        let (win_scope, win_map) =
            self.apply_window_stage(scope, &materialized_items, materialized_sort.as_deref())?;
        let final_items: Vec<LogicalProjectionItem> = materialized_items
            .iter()
            .map(|item| match item {
                LogicalProjectionItem::Wildcard { qualifier } => LogicalProjectionItem::Wildcard {
                    qualifier: qualifier.clone(),
                },
                LogicalProjectionItem::Expr { expr, alias } => LogicalProjectionItem::Expr {
                    expr: Box::new(rewrite_window_calls(expr, &win_map)),
                    alias: alias.clone(),
                },
            })
            .collect();
        let final_sort = materialized_sort.map(|keys| {
            keys.iter()
                .map(|key| LogicalSortKey {
                    expr: rewrite_window_calls(&key.expr, &win_map),
                    descending: key.descending,
                    nulls_first: key.nulls_first,
                })
                .collect::<Vec<_>>()
        });
        let (expanded, expressions, visible_entries) =
            self.expand_projection_items(&final_items, &win_scope)?;
        self.finish_projection(
            win_scope,
            &expanded,
            expressions,
            visible_entries,
            distinct,
            final_sort.as_deref(),
        )
    }

    fn apply_window_stage(
        &mut self,
        mut scope: ScopedRelation,
        items: &[LogicalProjectionItem],
        outer_sort: Option<&[LogicalSortKey]>,
    ) -> NativeResult<(ScopedRelation, Vec<(LogicalExpr, String)>)> {
        let mut calls = Vec::new();
        for item in items {
            if let LogicalProjectionItem::Expr { expr, .. } = item {
                collect_expr_window_calls(expr, &mut calls);
            }
        }
        if let Some(keys) = outer_sort {
            for key in keys {
                collect_expr_window_calls(&key.expr, &mut calls);
            }
        }
        if calls.is_empty() {
            return Ok((scope, Vec::new()));
        }

        let mut pre_names: Vec<String> = scope.entries.iter().map(|e| e.internal.clone()).collect();
        let mut pre_exprs = Vec::<(String, Expression)>::new();
        let retained_refs: Vec<ColumnRef> = pre_names
            .iter()
            .map(|name| ColumnRef::new(name.clone()))
            .collect::<NativeResult<_>>()?;
        let mut window_exprs = Vec::with_capacity(calls.len());
        let mut win_map = Vec::with_capacity(calls.len());

        for call in calls {
            let LogicalExpr::Function {
                name,
                args,
                distinct,
                filter,
                order_by,
                over: Some(spec),
            } = &call
            else {
                continue;
            };
            let expr = self.build_window_expression(
                name,
                args,
                *distinct,
                filter.as_deref(),
                order_by,
                spec,
                &scope,
                &mut pre_names,
                &mut pre_exprs,
            )?;
            let out_name = expr.output_column.clone();
            window_exprs.push(expr);
            win_map.push((call, out_name));
        }

        if !pre_exprs.is_empty() {
            let mut full_pre = scope
                .entries
                .iter()
                .map(|e| Ok((e.internal.clone(), column(&e.internal)?)))
                .collect::<NativeResult<Vec<_>>>()?;
            full_pre.extend(pre_exprs);
            scope.plan = Plan::Project(Box::new(Project {
                input: scope.plan,
                expressions: full_pre,
            }));
        }

        scope.plan = Plan::Window(Box::new(Window {
            input: scope.plan,
            columns: retained_refs,
            expressions: window_exprs,
        }));
        for (_, out_col) in &win_map {
            scope.entries.push(ScopeColumn {
                internal: out_col.clone(),
                qualifier: None,
                output: out_col.clone(),
            });
        }
        Ok((scope, win_map))
    }

    #[allow(clippy::too_many_arguments)]
    fn build_window_expression(
        &mut self,
        name: &str,
        args: &[LogicalExpr],
        distinct: bool,
        filter: Option<&LogicalExpr>,
        agg_order_by: &[LogicalSortKey],
        spec: &LogicalWindowSpec,
        scope: &ScopedRelation,
        pre_names: &mut Vec<String>,
        pre_exprs: &mut Vec<(String, Expression)>,
    ) -> NativeResult<WindowExpression> {
        if filter.is_some() || !agg_order_by.is_empty() {
            return Err(unsupported_sql_error(
                "window FILTER and WITHIN GROUP ORDER BY clauses are not admitted",
            ));
        }
        let mut resolve_input_col = |expr: &LogicalExpr| -> NativeResult<ColumnRef> {
            match expr {
                LogicalExpr::Column { qualifier, name } => {
                    Ok(ColumnRef::new(scope.resolve(qualifier.as_deref(), name)?)?)
                }
                other => {
                    let lowered = self.lower_expr(other, scope)?;
                    let col = fresh_col_names(pre_names, &mut self.lowerer.serial);
                    pre_names.push(col.clone());
                    pre_exprs.push((col.clone(), lowered));
                    Ok(ColumnRef::new(col)?)
                }
            }
        };

        let partition_by = spec
            .partition_by
            .iter()
            .map(&mut resolve_input_col)
            .collect::<NativeResult<Vec<_>>>()?;
        let order_by = spec
            .order_by
            .iter()
            .map(|key| {
                let col = resolve_input_col(&key.expr)?;
                make_order_key(col.as_str(), key)
            })
            .collect::<NativeResult<Vec<_>>>()?;
        let frame = spec.frame.as_ref().map(convert_window_frame).transpose()?;

        let function = build_window_function(name, args, distinct, &mut resolve_input_col)?;
        let out_col = fresh_col_names(pre_names, &mut self.lowerer.serial);
        pre_names.push(out_col.clone());

        Ok(WindowExpression {
            output_column: out_col,
            function,
            partition_by,
            order_by,
            frame,
        })
    }
}

fn build_window_function(
    name: &str,
    args: &[LogicalExpr],
    distinct: bool,
    resolve_input_col: &mut dyn FnMut(&LogicalExpr) -> NativeResult<ColumnRef>,
) -> NativeResult<NativeWindowFunction> {
    let lower_name = name.to_ascii_lowercase();
    match lower_name.as_str() {
        "row_number" => Ok(NativeWindowFunction::RowNumber),
        "rank" => Ok(NativeWindowFunction::Rank),
        "dense_rank" => Ok(NativeWindowFunction::DenseRank),
        "percent_rank" => Ok(NativeWindowFunction::PercentRank),
        "cume_dist" => Ok(NativeWindowFunction::CumeDist),
        "ntile" => {
            let buckets =
                positive_window_literal(args.first().ok_or_else(|| {
                    unsupported_sql_error("NTILE requires a bucket count argument")
                })?)
                .and_then(|v| usize::try_from(v).ok())
                .ok_or_else(|| unsupported_sql_error("NTILE bucket count must be >= 1"))?;
            Ok(NativeWindowFunction::Ntile { buckets })
        }
        "lag" | "lead" => {
            let col = resolve_input_col(args.first().ok_or_else(|| {
                unsupported_sql_error(&format!("{name} requires an expression argument"))
            })?)?;
            let offset = if let Some(offset_expr) = args.get(1) {
                positive_window_literal(offset_expr)
                    .and_then(|v| usize::try_from(v).ok())
                    .ok_or_else(|| unsupported_sql_error(&format!("{name} offset must be >= 1")))?
            } else {
                1
            };
            Ok(if lower_name == "lag" {
                NativeWindowFunction::Lag {
                    column: col,
                    offset,
                }
            } else {
                NativeWindowFunction::Lead {
                    column: col,
                    offset,
                }
            })
        }
        "first_value" => Ok(NativeWindowFunction::Framed(FrameFunction::FirstValue(
            resolve_input_col(
                args.first()
                    .ok_or_else(|| unsupported_sql_error("FIRST_VALUE requires one argument"))?,
            )?,
        ))),
        "last_value" => Ok(NativeWindowFunction::Framed(FrameFunction::LastValue(
            resolve_input_col(
                args.first()
                    .ok_or_else(|| unsupported_sql_error("LAST_VALUE requires one argument"))?,
            )?,
        ))),
        "nth_value" => {
            let col = resolve_input_col(args.first().ok_or_else(|| {
                unsupported_sql_error("NTH_VALUE requires expression and index arguments")
            })?)?;
            let index =
                positive_window_literal(args.get(1).ok_or_else(|| {
                    unsupported_sql_error("NTH_VALUE requires index argument >= 1")
                })?)
                .ok_or_else(|| unsupported_sql_error("NTH_VALUE index must be >= 1"))?;
            Ok(NativeWindowFunction::Framed(FrameFunction::NthValue {
                column: col,
                index,
            }))
        }
        "count" | "sum" | "avg" | "min" | "max" => {
            build_basic_framed_window_op(&lower_name, args, distinct, resolve_input_col)
        }
        other => build_framed_window_aggregate(other, name, args, distinct, resolve_input_col),
    }
}

fn build_basic_framed_window_op(
    lower_name: &str,
    args: &[LogicalExpr],
    distinct: bool,
    resolve_input_col: &mut dyn FnMut(&LogicalExpr) -> NativeResult<ColumnRef>,
) -> NativeResult<NativeWindowFunction> {
    match lower_name {
        "count" => {
            if args.len() == 1 && matches!(args[0], LogicalExpr::Wildcard { qualifier: None }) {
                Ok(NativeWindowFunction::Framed(FrameFunction::CountAll))
            } else if args.len() == 1 {
                let col = resolve_input_col(&args[0])?;
                Ok(NativeWindowFunction::Framed(if distinct {
                    FrameFunction::CountDistinct(col)
                } else {
                    FrameFunction::Count(col)
                }))
            } else {
                Err(unsupported_sql_error("window COUNT requires one argument"))
            }
        }
        "sum" => Ok(NativeWindowFunction::Framed(FrameFunction::Sum(
            resolve_input_col(
                args.first()
                    .ok_or_else(|| unsupported_sql_error("window SUM requires one argument"))?,
            )?,
        ))),
        "avg" => Ok(NativeWindowFunction::Framed(FrameFunction::Avg(
            resolve_input_col(
                args.first()
                    .ok_or_else(|| unsupported_sql_error("window AVG requires one argument"))?,
            )?,
        ))),
        "min" => Ok(NativeWindowFunction::Framed(FrameFunction::Min(
            resolve_input_col(
                args.first()
                    .ok_or_else(|| unsupported_sql_error("window MIN requires one argument"))?,
            )?,
        ))),
        _ => Ok(NativeWindowFunction::Framed(FrameFunction::Max(
            resolve_input_col(
                args.first()
                    .ok_or_else(|| unsupported_sql_error("window MAX requires one argument"))?,
            )?,
        ))),
    }
}

fn build_framed_window_aggregate(
    lower_name: &str,
    display_name: &str,
    args: &[LogicalExpr],
    distinct: bool,
    resolve_input_col: &mut dyn FnMut(&LogicalExpr) -> NativeResult<ColumnRef>,
) -> NativeResult<NativeWindowFunction> {
    let effective_distinct = if lower_name == "count_distinct" {
        true
    } else {
        distinct
    };
    let base = if lower_name == "count_distinct" {
        "count"
    } else {
        lower_name
    };
    let agg_fn = super::AggregateFunction::parse(base).ok_or_else(|| {
        unsupported_sql_error(&format!("unsupported window function {display_name:?}"))
    })?;
    let native_name = agg_fn.native_name(effective_distinct)?;
    let col = if args.len() == 1 && matches!(args[0], LogicalExpr::Wildcard { .. }) {
        None
    } else {
        args.first().map(&mut *resolve_input_col).transpose()?
    };
    let mut measure =
        VortexSimpleAggregateMeasure::new(native_name, col, format!("__win_agg_{native_name}"));
    if let Some(second) = args.get(1) {
        measure.secondary_column = Some(resolve_input_col(second)?);
    }
    Ok(NativeWindowFunction::Framed(FrameFunction::Aggregate(
        Box::new(measure),
    )))
}
