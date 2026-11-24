//! Canonical subquery inspection, semi/anti/scalar-aggregate decorrelation, and native lowering.

use super::{
    Aggregate, CanonicalLowering, ColumnRef, ExprId, Expression, ExpressionKind, Filter,
    InspectState, Join, JoinColumn, JoinKind, NativeResult, Plan, Project, ScopeColumn,
    ScopedRelation, Side, Subquery, SubqueryKind, column, fresh_col_names, inspect_relation,
    unsupported_sql_error,
};
use shardloom_core::{BinaryOp, ComparisonOp, ScalarValue, UnaryOp};
use shardloom_plan::sql_binder::{LogicalExpr, LogicalProjectionItem, LogicalRelation};
use shardloom_vortex::relational_query::VortexRelationalJoinKey as JoinKey;
use std::collections::{BTreeMap, BTreeSet};

/// Walks `expr` so subqueries register all bound sources, inputs, and nesting depths.
pub(super) fn inspect_expr_sources(
    expr: &LogicalExpr,
    ctes: &BTreeSet<String>,
    depth: usize,
    state: &mut InspectState,
) -> NativeResult<()> {
    match expr {
        LogicalExpr::Unary { expr, .. } | LogicalExpr::Cast { expr, .. } => {
            inspect_expr_sources(expr, ctes, depth, state)
        }
        LogicalExpr::Binary { left, right, .. }
        | LogicalExpr::Compare { left, right, .. }
        | LogicalExpr::IsDistinctFrom { left, right, .. }
        | LogicalExpr::Like {
            expr: left,
            pattern: right,
            ..
        } => {
            inspect_expr_sources(left, ctes, depth, state)?;
            inspect_expr_sources(right, ctes, depth, state)
        }
        LogicalExpr::Between {
            expr, low, high, ..
        } => {
            inspect_expr_sources(expr, ctes, depth, state)?;
            inspect_expr_sources(low, ctes, depth, state)?;
            inspect_expr_sources(high, ctes, depth, state)
        }
        LogicalExpr::InList { expr, list, .. } => {
            inspect_expr_sources(expr, ctes, depth, state)?;
            for item in list {
                inspect_expr_sources(item, ctes, depth, state)?;
            }
            Ok(())
        }
        LogicalExpr::InSubquery { expr, subquery, .. } => {
            inspect_expr_sources(expr, ctes, depth, state)?;
            inspect_relation(subquery, ctes, depth + 1, state)
        }
        LogicalExpr::Exists { subquery, .. } | LogicalExpr::ScalarSubquery(subquery) => {
            inspect_relation(subquery, ctes, depth + 1, state)
        }
        LogicalExpr::Case {
            operand,
            when_then,
            else_expr,
        } => {
            if let Some(op) = operand {
                inspect_expr_sources(op, ctes, depth, state)?;
            }
            for (w, t) in when_then {
                inspect_expr_sources(w, ctes, depth, state)?;
                inspect_expr_sources(t, ctes, depth, state)?;
            }
            if let Some(el) = else_expr {
                inspect_expr_sources(el, ctes, depth, state)?;
            }
            Ok(())
        }
        LogicalExpr::Function {
            args,
            filter,
            order_by,
            over,
            ..
        } => {
            for arg in args {
                inspect_expr_sources(arg, ctes, depth, state)?;
            }
            if let Some(f) = filter {
                inspect_expr_sources(f, ctes, depth, state)?;
            }
            for k in order_by {
                inspect_expr_sources(&k.expr, ctes, depth, state)?;
            }
            if let Some(spec) = over {
                for p in &spec.partition_by {
                    inspect_expr_sources(p, ctes, depth, state)?;
                }
                for k in &spec.order_by {
                    inspect_expr_sources(&k.expr, ctes, depth, state)?;
                }
            }
            Ok(())
        }
        LogicalExpr::Array(items) | LogicalExpr::Tuple(items) => {
            for item in items {
                inspect_expr_sources(item, ctes, depth, state)?;
            }
            Ok(())
        }
        LogicalExpr::Literal(_)
        | LogicalExpr::Column { .. }
        | LogicalExpr::Parameter(_)
        | LogicalExpr::Wildcard { .. }
        | LogicalExpr::Interval { .. } => Ok(()),
    }
}

/// Counts CTE name references inside expression subqueries (respecting inner shadowing).
pub(super) fn count_expr_cte_refs(
    expr: &LogicalExpr,
    shadowed: &BTreeSet<String>,
    counts: &mut BTreeMap<String, usize>,
) {
    match expr {
        LogicalExpr::InSubquery { expr, subquery, .. } => {
            count_expr_cte_refs(expr, shadowed, counts);
            super::count_rel_cte_refs(subquery, shadowed, counts);
        }
        LogicalExpr::Exists { subquery, .. } | LogicalExpr::ScalarSubquery(subquery) => {
            super::count_rel_cte_refs(subquery, shadowed, counts);
        }
        LogicalExpr::Unary { expr, .. } | LogicalExpr::Cast { expr, .. } => {
            count_expr_cte_refs(expr, shadowed, counts);
        }
        LogicalExpr::Binary { left, right, .. }
        | LogicalExpr::Compare { left, right, .. }
        | LogicalExpr::IsDistinctFrom { left, right, .. }
        | LogicalExpr::Like {
            expr: left,
            pattern: right,
            ..
        } => {
            count_expr_cte_refs(left, shadowed, counts);
            count_expr_cte_refs(right, shadowed, counts);
        }
        LogicalExpr::Between {
            expr, low, high, ..
        } => {
            count_expr_cte_refs(expr, shadowed, counts);
            count_expr_cte_refs(low, shadowed, counts);
            count_expr_cte_refs(high, shadowed, counts);
        }
        LogicalExpr::InList { expr, list, .. } => {
            count_expr_cte_refs(expr, shadowed, counts);
            for item in list {
                count_expr_cte_refs(item, shadowed, counts);
            }
        }
        LogicalExpr::Case {
            operand,
            when_then,
            else_expr,
        } => {
            if let Some(op) = operand {
                count_expr_cte_refs(op, shadowed, counts);
            }
            for (w, t) in when_then {
                count_expr_cte_refs(w, shadowed, counts);
                count_expr_cte_refs(t, shadowed, counts);
            }
            if let Some(el) = else_expr {
                count_expr_cte_refs(el, shadowed, counts);
            }
        }
        LogicalExpr::Function { args, filter, .. } => {
            for arg in args {
                count_expr_cte_refs(arg, shadowed, counts);
            }
            if let Some(f) = filter {
                count_expr_cte_refs(f, shadowed, counts);
            }
        }
        LogicalExpr::Array(items) | LogicalExpr::Tuple(items) => {
            for item in items {
                count_expr_cte_refs(item, shadowed, counts);
            }
        }
        _ => {}
    }
}

pub(super) fn expr_has_subquery(expr: &LogicalExpr) -> bool {
    match expr {
        LogicalExpr::Exists { .. }
        | LogicalExpr::InSubquery { .. }
        | LogicalExpr::ScalarSubquery(_) => true,
        LogicalExpr::Unary { expr, .. } | LogicalExpr::Cast { expr, .. } => expr_has_subquery(expr),
        LogicalExpr::Binary { left, right, .. }
        | LogicalExpr::Compare { left, right, .. }
        | LogicalExpr::IsDistinctFrom { left, right, .. }
        | LogicalExpr::Like {
            expr: left,
            pattern: right,
            ..
        } => expr_has_subquery(left) || expr_has_subquery(right),
        LogicalExpr::Between {
            expr, low, high, ..
        } => expr_has_subquery(expr) || expr_has_subquery(low) || expr_has_subquery(high),
        LogicalExpr::InList { expr, list, .. } => {
            expr_has_subquery(expr) || list.iter().any(expr_has_subquery)
        }
        LogicalExpr::Case {
            operand,
            when_then,
            else_expr,
        } => {
            operand.as_deref().is_some_and(expr_has_subquery)
                || when_then
                    .iter()
                    .any(|(w, t)| expr_has_subquery(w) || expr_has_subquery(t))
                || else_expr.as_deref().is_some_and(expr_has_subquery)
        }
        LogicalExpr::Function {
            args,
            filter,
            order_by,
            over,
            ..
        } => {
            args.iter().any(expr_has_subquery)
                || filter.as_deref().is_some_and(expr_has_subquery)
                || order_by.iter().any(|k| expr_has_subquery(&k.expr))
                || over.as_ref().is_some_and(|spec| {
                    spec.partition_by.iter().any(expr_has_subquery)
                        || spec.order_by.iter().any(|k| expr_has_subquery(&k.expr))
                })
        }
        LogicalExpr::Array(items) | LogicalExpr::Tuple(items) => {
            items.iter().any(expr_has_subquery)
        }
        _ => false,
    }
}

fn split_and_conjuncts<'a>(expr: &'a LogicalExpr, out: &mut Vec<&'a LogicalExpr>) {
    if let LogicalExpr::Binary {
        left,
        op: BinaryOp::And,
        right,
    } = expr
    {
        split_and_conjuncts(left, out);
        split_and_conjuncts(right, out);
    } else {
        out.push(expr);
    }
}

fn conjoin_logical(exprs: &[LogicalExpr]) -> Option<LogicalExpr> {
    let mut iter = exprs.iter().cloned();
    let first = iter.next()?;
    Some(iter.fold(first, |acc, rhs| LogicalExpr::Binary {
        left: Box::new(acc),
        op: BinaryOp::And,
        right: Box::new(rhs),
    }))
}

fn scope_matches_column(entries: &[ScopeColumn], qualifier: Option<&str>, name: &str) -> bool {
    if let Some(q) = qualifier {
        let exact = format!("{q}.{name}");
        entries
            .iter()
            .any(|e| (e.qualifier.as_deref() == Some(q) && e.output == name) || e.internal == exact)
    } else {
        entries.iter().any(|e| e.output == name)
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ExprScope {
    Constant,
    InnerOnly,
    OuterOnly,
    Mixed,
}

fn classify_expr_scope(
    expr: &LogicalExpr,
    inner: &ScopedRelation,
    outer: &ScopedRelation,
) -> NativeResult<ExprScope> {
    let mut has_inner = false;
    let mut has_outer = false;
    visit_col_refs(expr, &mut |qualifier, name| {
        if scope_matches_column(&inner.entries, qualifier, name) {
            has_inner = true;
            Ok(())
        } else if scope_matches_column(&outer.entries, qualifier, name) {
            has_outer = true;
            Ok(())
        } else {
            Err(unsupported_sql_error(&format!(
                "column {:?} is not present in subquery or outer schema",
                qualifier.map_or_else(|| name.to_string(), |q| format!("{q}.{name}"))
            )))
        }
    })?;
    Ok(match (has_inner, has_outer) {
        (false, false) => ExprScope::Constant,
        (true, false) => ExprScope::InnerOnly,
        (false, true) => ExprScope::OuterOnly,
        (true, true) => ExprScope::Mixed,
    })
}

fn visit_col_refs(
    expr: &LogicalExpr,
    visit: &mut impl FnMut(Option<&str>, &str) -> NativeResult<()>,
) -> NativeResult<()> {
    match expr {
        LogicalExpr::Column { qualifier, name } => visit(qualifier.as_deref(), name),
        LogicalExpr::Unary { expr, .. } | LogicalExpr::Cast { expr, .. } => {
            visit_col_refs(expr, visit)
        }
        LogicalExpr::Binary { left, right, .. }
        | LogicalExpr::Compare { left, right, .. }
        | LogicalExpr::IsDistinctFrom { left, right, .. }
        | LogicalExpr::Like {
            expr: left,
            pattern: right,
            ..
        } => {
            visit_col_refs(left, visit)?;
            visit_col_refs(right, visit)
        }
        LogicalExpr::Between {
            expr, low, high, ..
        } => {
            visit_col_refs(expr, visit)?;
            visit_col_refs(low, visit)?;
            visit_col_refs(high, visit)
        }
        LogicalExpr::InList { expr, list, .. } => {
            visit_col_refs(expr, visit)?;
            for item in list {
                visit_col_refs(item, visit)?;
            }
            Ok(())
        }
        LogicalExpr::Case {
            operand,
            when_then,
            else_expr,
        } => {
            if let Some(op) = operand {
                visit_col_refs(op, visit)?;
            }
            for (w, t) in when_then {
                visit_col_refs(w, visit)?;
                visit_col_refs(t, visit)?;
            }
            if let Some(el) = else_expr {
                visit_col_refs(el, visit)?;
            }
            Ok(())
        }
        LogicalExpr::Function { args, filter, .. } => {
            for arg in args {
                visit_col_refs(arg, visit)?;
            }
            if let Some(f) = filter {
                visit_col_refs(f, visit)?;
            }
            Ok(())
        }
        LogicalExpr::Array(items) | LogicalExpr::Tuple(items) => {
            for item in items {
                visit_col_refs(item, visit)?;
            }
            Ok(())
        }
        LogicalExpr::InSubquery { .. }
        | LogicalExpr::Exists { .. }
        | LogicalExpr::ScalarSubquery(_) => Err(unsupported_sql_error(
            "nested subquery correlation inside filter predicate is not admitted in this rewrite",
        )),
        _ => Ok(()),
    }
}

fn unwrap_project_filter(
    mut rel: &LogicalRelation,
) -> Option<(Option<&LogicalExpr>, &LogicalRelation)> {
    loop {
        match rel {
            LogicalRelation::Subquery { input, .. } => rel = input,
            LogicalRelation::Project {
                input,
                items,
                distinct: _,
            } => {
                let single = if items.len() == 1 {
                    match &items[0] {
                        LogicalProjectionItem::Expr { expr, .. } => Some(expr.as_ref()),
                        LogicalProjectionItem::Wildcard { .. } => None,
                    }
                } else {
                    None
                };
                return Some((single, input));
            }
            _ => return Some((None, rel)),
        }
    }
}

/// Lowers `LogicalRelation::Filter`, decorrelating semi/anti/scalar subqueries when possible.
pub(super) fn lower_filter_with_subqueries(
    ctx: &mut CanonicalLowering<'_, '_, '_>,
    input: &LogicalRelation,
    predicate: &LogicalExpr,
) -> NativeResult<ScopedRelation> {
    let mut scope = ctx.lower_relation(input)?;
    let mut conjuncts = Vec::new();
    split_and_conjuncts(predicate, &mut conjuncts);
    let mut pending_scalar = Vec::new();

    for conj in conjuncts {
        if !expr_has_subquery(conj) {
            pending_scalar.push(conj.clone());
            continue;
        }
        if !pending_scalar.is_empty() {
            scope = apply_scalar_filter(ctx, scope, &pending_scalar)?;
            pending_scalar.clear();
        }
        if let LogicalExpr::Exists { subquery, negated } = conj
            && let Some(next_scope) = try_decorrelate_exists(ctx, &scope, subquery, *negated)?
        {
            scope = next_scope;
            continue;
        }
        if let LogicalExpr::InSubquery {
            expr,
            subquery,
            negated: false,
        } = conj
            && let Some(next_scope) = try_decorrelate_in(ctx, &scope, expr, subquery)?
        {
            scope = next_scope;
            continue;
        }
        if let LogicalExpr::Compare { left, op, right } = conj
            && let Some(next_scope) = try_decorrelate_scalar_cmp(ctx, &scope, left, *op, right)?
        {
            scope = next_scope;
            continue;
        }
        let mut owned_conj = conj.clone();
        let saved_entries = scope.entries.clone();
        scope = materialize_expr_subqueries(ctx, scope, &mut owned_conj)?;
        let lowered_pred = ctx.lower_expr(&owned_conj, &scope)?;
        scope.plan = Plan::Filter(Box::new(Filter {
            input: scope.plan,
            predicate: lowered_pred,
        }));
        if scope.entries.len() != saved_entries.len() {
            scope.entries = saved_entries;
            scope = project_entries_passthrough(scope)?;
        }
    }
    if !pending_scalar.is_empty() {
        scope = apply_scalar_filter(ctx, scope, &pending_scalar)?;
    }
    Ok(scope)
}

fn apply_scalar_filter(
    ctx: &mut CanonicalLowering<'_, '_, '_>,
    mut scope: ScopedRelation,
    exprs: &[LogicalExpr],
) -> NativeResult<ScopedRelation> {
    let Some(combined) = conjoin_logical(exprs) else {
        return Ok(scope);
    };
    let lowered = ctx.lower_expr(&combined, &scope)?;
    scope.plan = Plan::Filter(Box::new(Filter {
        input: scope.plan,
        predicate: lowered,
    }));
    Ok(scope)
}

fn project_entries_passthrough(mut scope: ScopedRelation) -> NativeResult<ScopedRelation> {
    let exprs = scope
        .entries
        .iter()
        .map(|entry| Ok((entry.internal.clone(), column(&entry.internal)?)))
        .collect::<NativeResult<Vec<_>>>()?;
    scope.plan = Plan::Project(Box::new(Project {
        input: scope.plan,
        expressions: exprs,
    }));
    Ok(scope)
}

type CorrelationSplit = (Vec<(LogicalExpr, LogicalExpr)>, Vec<LogicalExpr>);

fn split_correlation_conjuncts(
    pred: &LogicalExpr,
    outer: &ScopedRelation,
    inner: &ScopedRelation,
) -> Option<CorrelationSplit> {
    let mut raw = Vec::new();
    split_and_conjuncts(pred, &mut raw);
    let mut corr_pairs = Vec::new();
    let mut pure_inner = Vec::new();
    for part in raw {
        match classify_expr_scope(part, inner, outer) {
            Ok(ExprScope::InnerOnly | ExprScope::Constant) => pure_inner.push(part.clone()),
            Ok(ExprScope::Mixed) => {
                let LogicalExpr::Compare {
                    left,
                    op: ComparisonOp::Eq,
                    right,
                } = part
                else {
                    return None;
                };
                let Ok(ls) = classify_expr_scope(left, inner, outer) else {
                    return None;
                };
                let Ok(rs) = classify_expr_scope(right, inner, outer) else {
                    return None;
                };
                match (ls, rs) {
                    (ExprScope::OuterOnly, ExprScope::InnerOnly) => {
                        corr_pairs.push((left.as_ref().clone(), right.as_ref().clone()));
                    }
                    (ExprScope::InnerOnly, ExprScope::OuterOnly) => {
                        corr_pairs.push((right.as_ref().clone(), left.as_ref().clone()));
                    }
                    _ => return None,
                }
            }
            Ok(ExprScope::OuterOnly) | Err(_) => return None,
        }
    }
    Some((corr_pairs, pure_inner))
}

fn resolve_or_project_key(
    ctx: &mut CanonicalLowering<'_, '_, '_>,
    scope: &mut ScopedRelation,
    expr: &LogicalExpr,
) -> NativeResult<ColumnRef> {
    if let LogicalExpr::Column { qualifier, name } = expr
        && let Ok(resolved) = scope.resolve(qualifier.as_deref(), name)
    {
        return ColumnRef::new(resolved);
    }
    let lowered = ctx.lower_expr(expr, scope)?;
    let existing: Vec<String> = scope.entries.iter().map(|e| e.internal.clone()).collect();
    let fresh = fresh_col_names(&existing, &mut ctx.lowerer.serial);
    let mut exprs = scope
        .entries
        .iter()
        .map(|entry| Ok((entry.internal.clone(), column(&entry.internal)?)))
        .collect::<NativeResult<Vec<_>>>()?;
    exprs.push((fresh.clone(), lowered));
    scope.plan = Plan::Project(Box::new(Project {
        input: scope.plan.clone(),
        expressions: exprs,
    }));
    scope.entries.push(ScopeColumn {
        internal: fresh.clone(),
        qualifier: None,
        output: fresh.clone(),
    });
    ColumnRef::new(fresh)
}

fn semi_anti_columns(entries: &[ScopeColumn]) -> NativeResult<Vec<JoinColumn>> {
    entries
        .iter()
        .map(|entry| {
            Ok(JoinColumn {
                side: Side::Left,
                column: ColumnRef::new(entry.internal.clone())?,
                output_column: entry.internal.clone(),
            })
        })
        .collect()
}

fn try_decorrelate_exists(
    ctx: &mut CanonicalLowering<'_, '_, '_>,
    outer: &ScopedRelation,
    subquery: &LogicalRelation,
    negated: bool,
) -> NativeResult<Option<ScopedRelation>> {
    let Some((_, inner_after_proj)) = unwrap_project_filter(subquery) else {
        return Ok(None);
    };
    let LogicalRelation::Filter {
        input: inner_base,
        predicate: inner_pred,
    } = inner_after_proj
    else {
        return Ok(None);
    };
    if rel_has_outer_references(inner_base) {
        return Ok(None);
    }
    let Ok(mut inner_scope) = ctx.lower_relation(inner_base) else {
        return Ok(None);
    };
    let Some((corr_pairs, pure_inner)) =
        split_correlation_conjuncts(inner_pred, outer, &inner_scope)
    else {
        return Ok(None);
    };
    if corr_pairs.is_empty() {
        return Ok(None);
    }
    if !pure_inner.is_empty() {
        inner_scope = apply_scalar_filter(ctx, inner_scope, &pure_inner)?;
    }
    let mut left_scope = outer.clone();
    let saved_left_entries = outer.entries.clone();
    let mut keys = Vec::with_capacity(corr_pairs.len());
    for (outer_expr, inner_expr) in &corr_pairs {
        let left_ref = resolve_or_project_key(ctx, &mut left_scope, outer_expr)?;
        let right_ref = resolve_or_project_key(ctx, &mut inner_scope, inner_expr)?;
        keys.push(JoinKey {
            left: left_ref,
            right: right_ref,
        });
    }
    let columns = semi_anti_columns(&saved_left_entries)?;
    let needs_trim = left_scope.entries.len() != saved_left_entries.len();
    let join = Plan::Join(Box::new(Join {
        left: left_scope.plan,
        right: inner_scope.plan,
        kind: if negated {
            JoinKind::LeftAnti
        } else {
            JoinKind::LeftSemi
        },
        keys,
        condition: None,
        columns,
    }));
    let joined = ScopedRelation {
        plan: join,
        entries: saved_left_entries,
    };
    if needs_trim {
        Ok(Some(project_entries_passthrough(joined)?))
    } else {
        Ok(Some(joined))
    }
}

fn try_decorrelate_in(
    ctx: &mut CanonicalLowering<'_, '_, '_>,
    outer: &ScopedRelation,
    lhs_expr: &LogicalExpr,
    subquery: &LogicalRelation,
) -> NativeResult<Option<ScopedRelation>> {
    if expr_has_subquery(lhs_expr) || matches!(lhs_expr, LogicalExpr::Tuple(_)) {
        return Ok(None);
    }
    let mut rel = subquery;
    while let LogicalRelation::Subquery { input, .. } = rel {
        rel = input;
    }
    let (proj_expr, after_proj) = match rel {
        LogicalRelation::Project { items, input, .. } if items.len() == 1 => {
            let LogicalProjectionItem::Expr { expr, .. } = &items[0] else {
                return Ok(None);
            };
            (Some(expr.as_ref()), input.as_ref())
        }
        _ => (None, rel),
    };
    let (inner_base, inner_pred) = match after_proj {
        LogicalRelation::Filter { input, predicate } => (input.as_ref(), Some(predicate)),
        other => (other, None),
    };
    if rel_has_outer_references(inner_base) {
        return Ok(None);
    }
    let Ok(mut inner_scope) = ctx.lower_relation(inner_base) else {
        return Ok(None);
    };
    if let Some(proj) = proj_expr
        && inner_scope.entries.len() == 1
        && matches!(
            classify_expr_scope(proj, &inner_scope, outer),
            Ok(ExprScope::OuterOnly) | Err(_)
        )
    {
        return Ok(None);
    }
    let mut corr_pairs = Vec::new();
    if let Some(pred) = inner_pred {
        let Some((corr, pure_inner)) = split_correlation_conjuncts(pred, outer, &inner_scope)
        else {
            return Ok(None);
        };
        corr_pairs = corr;
        if !pure_inner.is_empty() {
            inner_scope = apply_scalar_filter(ctx, inner_scope, &pure_inner)?;
        }
    }
    let target_inner_expr = if let Some(expr) = proj_expr {
        if !matches!(
            classify_expr_scope(expr, &inner_scope, outer),
            Ok(ExprScope::InnerOnly | ExprScope::Constant)
        ) {
            return Ok(None);
        }
        expr.clone()
    } else {
        if inner_scope.entries.len() != 1 {
            return Ok(None);
        }
        LogicalExpr::Column {
            qualifier: None,
            name: inner_scope.entries[0].output.clone(),
        }
    };
    let mut left_scope = outer.clone();
    let saved_left_entries = outer.entries.clone();
    let left_in_col = resolve_or_project_key(ctx, &mut left_scope, lhs_expr)?;
    let right_in_col = resolve_or_project_key(ctx, &mut inner_scope, &target_inner_expr)?;
    let mut keys = vec![JoinKey {
        left: left_in_col,
        right: right_in_col,
    }];
    for (outer_corr, inner_corr) in &corr_pairs {
        let lk = resolve_or_project_key(ctx, &mut left_scope, outer_corr)?;
        let rk = resolve_or_project_key(ctx, &mut inner_scope, inner_corr)?;
        keys.push(JoinKey {
            left: lk,
            right: rk,
        });
    }
    let columns = semi_anti_columns(&saved_left_entries)?;
    Ok(Some(ScopedRelation {
        plan: Plan::Join(Box::new(Join {
            left: left_scope.plan,
            right: inner_scope.plan,
            kind: JoinKind::LeftSemi,
            keys,
            condition: None,
            columns,
        })),
        entries: saved_left_entries,
    }))
}

fn try_decorrelate_scalar_cmp(
    ctx: &mut CanonicalLowering<'_, '_, '_>,
    outer: &ScopedRelation,
    left: &LogicalExpr,
    op: ComparisonOp,
    right: &LogicalExpr,
) -> NativeResult<Option<ScopedRelation>> {
    let (lhs_expr, sub_rel, cmp_op) = match (left, right) {
        (_, LogicalExpr::ScalarSubquery(sub)) if !expr_has_subquery(left) => {
            (left, sub.as_ref(), op)
        }
        (LogicalExpr::ScalarSubquery(sub), _) if !expr_has_subquery(right) => {
            (right, sub.as_ref(), flip_comparison(op))
        }
        _ => return Ok(None),
    };
    let mut rel = sub_rel;
    while let LogicalRelation::Subquery { input, .. } = rel {
        rel = input;
    }
    let LogicalRelation::Project {
        input: proj_in,
        items,
        ..
    } = rel
    else {
        return Ok(None);
    };
    let [
        LogicalProjectionItem::Expr {
            expr: scalar_expr, ..
        },
    ] = items.as_slice()
    else {
        return Ok(None);
    };
    let LogicalRelation::Aggregate {
        input: agg_in,
        group_by,
        grouping_sets,
        having,
        ..
    } = proj_in.as_ref()
    else {
        return Ok(None);
    };
    if !group_by.is_empty() || grouping_sets.is_some() || having.is_some() {
        return Ok(None);
    }
    let LogicalRelation::Filter {
        input: inner_base,
        predicate: inner_pred,
    } = agg_in.as_ref()
    else {
        return Ok(None);
    };
    if rel_has_outer_references(inner_base) || expr_has_subquery(scalar_expr) {
        return Ok(None);
    }
    let mut aggs = Vec::new();
    super::collect_expr_aggregates(scalar_expr, &mut aggs);
    if aggs.is_empty() {
        return Ok(None);
    }
    let Ok(mut inner_scope) = ctx.lower_relation(inner_base) else {
        return Ok(None);
    };
    let Some((corr_pairs, pure_inner)) =
        split_correlation_conjuncts(inner_pred, outer, &inner_scope)
    else {
        return Ok(None);
    };
    if corr_pairs.is_empty() {
        return Ok(None);
    }
    if !pure_inner.is_empty() {
        inner_scope = apply_scalar_filter(ctx, inner_scope, &pure_inner)?;
    }
    build_decorrelated_scalar_cmp(
        ctx,
        outer,
        inner_scope,
        &corr_pairs,
        &aggs,
        scalar_expr,
        lhs_expr,
        cmp_op,
    )
}

#[allow(clippy::too_many_arguments)]
fn build_decorrelated_scalar_cmp(
    ctx: &mut CanonicalLowering<'_, '_, '_>,
    outer: &ScopedRelation,
    inner_scope: ScopedRelation,
    corr_pairs: &[(LogicalExpr, LogicalExpr)],
    aggs: &[super::CollectedAgg],
    scalar_expr: &LogicalExpr,
    lhs_expr: &LogicalExpr,
    cmp_op: ComparisonOp,
) -> NativeResult<Option<ScopedRelation>> {
    let mut left_scope = outer.clone();
    let saved_left_entries = outer.entries.clone();
    let mut pre_names: Vec<String> = inner_scope
        .entries
        .iter()
        .map(|e| e.internal.clone())
        .collect();
    let mut pre_proj = Vec::new();
    let mut inner_corr_cols = Vec::with_capacity(corr_pairs.len());
    let mut left_corr_cols = Vec::with_capacity(corr_pairs.len());
    for (outer_corr, inner_corr) in corr_pairs {
        left_corr_cols.push(resolve_or_project_key(ctx, &mut left_scope, outer_corr)?);
        let rk = if let LogicalExpr::Column { qualifier, name } = inner_corr
            && let Ok(resolved) = inner_scope.resolve(qualifier.as_deref(), name)
        {
            resolved
        } else {
            let lowered = ctx.lower_expr(inner_corr, &inner_scope)?;
            let col_name = fresh_col_names(&pre_names, &mut ctx.lowerer.serial);
            pre_names.push(col_name.clone());
            pre_proj.push((col_name.clone(), lowered));
            col_name
        };
        inner_corr_cols.push(rk);
    }

    let mut measures = Vec::with_capacity(aggs.len());
    let mut agg_map = Vec::with_capacity(aggs.len());
    let mut count_cols = Vec::new();
    for spec in aggs {
        let alias = fresh_col_names(&pre_names, &mut ctx.lowerer.serial);
        pre_names.push(alias.clone());
        let measure =
            ctx.lower_single_measure(spec, &alias, &inner_scope, &mut pre_names, &mut pre_proj)?;
        if matches!(
            spec.name.to_ascii_lowercase().as_str(),
            "count" | "count_distinct"
        ) {
            count_cols.push(alias.clone());
        }
        measures.push(measure);
        agg_map.push((spec.expr.clone(), alias));
    }

    let agg_scope =
        build_grouped_corr_agg(inner_scope, &pre_proj, &inner_corr_cols, measures, &agg_map)?;
    join_decorrelated_scalar_agg(
        ctx,
        left_scope,
        saved_left_entries,
        &agg_scope,
        &left_corr_cols,
        &inner_corr_cols,
        &agg_map,
        &count_cols,
        scalar_expr,
        lhs_expr,
        cmp_op,
    )
}

fn build_grouped_corr_agg(
    inner_scope: ScopedRelation,
    pre_proj: &[(String, shardloom_core::Expression)],
    inner_corr_cols: &[String],
    measures: Vec<shardloom_vortex::query_primitive::VortexSimpleAggregateMeasure>,
    agg_map: &[(LogicalExpr, String)],
) -> NativeResult<ScopedRelation> {
    let mut inner_plan = inner_scope.plan;
    if !pre_proj.is_empty() {
        let mut full_pre = inner_scope
            .entries
            .iter()
            .map(|e| Ok((e.internal.clone(), column(&e.internal)?)))
            .collect::<NativeResult<Vec<_>>>()?;
        full_pre.extend(pre_proj.iter().cloned());
        inner_plan = Plan::Project(Box::new(Project {
            input: inner_plan,
            expressions: full_pre,
        }));
    }
    let group_refs = inner_corr_cols
        .iter()
        .map(|c| ColumnRef::new(c.clone()))
        .collect::<NativeResult<Vec<_>>>()?;
    inner_plan = Plan::Aggregate(Box::new(Aggregate {
        grouping_sets: None,
        input: inner_plan,
        group_by: group_refs,
        measures,
    }));
    let agg_entries = inner_corr_cols
        .iter()
        .chain(agg_map.iter().map(|(_, a)| a))
        .map(|name| ScopeColumn {
            internal: name.clone(),
            qualifier: None,
            output: name.clone(),
        })
        .collect();
    Ok(ScopedRelation {
        plan: inner_plan,
        entries: agg_entries,
    })
}

#[allow(clippy::too_many_arguments)]
fn join_decorrelated_scalar_agg(
    ctx: &mut CanonicalLowering<'_, '_, '_>,
    left_scope: ScopedRelation,
    saved_left_entries: Vec<ScopeColumn>,
    agg_scope: &ScopedRelation,
    left_corr_cols: &[ColumnRef],
    inner_corr_cols: &[String],
    agg_map: &[(LogicalExpr, String)],
    count_cols: &[String],
    scalar_expr: &LogicalExpr,
    lhs_expr: &LogicalExpr,
    cmp_op: ComparisonOp,
) -> NativeResult<Option<ScopedRelation>> {
    let mut keys = Vec::with_capacity(left_corr_cols.len());
    let mut join_cols = semi_anti_columns(&left_scope.entries)?;
    let mut joined_names: Vec<String> = left_scope
        .entries
        .iter()
        .map(|e| e.internal.clone())
        .collect();
    for (lk, rk) in left_corr_cols.iter().zip(inner_corr_cols) {
        keys.push(JoinKey {
            left: lk.clone(),
            right: ColumnRef::new(rk.clone())?,
        });
    }
    let mut joined_entries = left_scope.entries.clone();
    let mut agg_joined_map = Vec::with_capacity(agg_map.len());
    for (agg_expr, alias) in agg_map {
        let right_out = fresh_col_names(&joined_names, &mut ctx.lowerer.serial);
        joined_names.push(right_out.clone());
        join_cols.push(JoinColumn {
            side: Side::Right,
            column: ColumnRef::new(alias.clone())?,
            output_column: right_out.clone(),
        });
        joined_entries.push(ScopeColumn {
            internal: right_out.clone(),
            qualifier: None,
            output: right_out.clone(),
        });
        let replacement = if count_cols.contains(alias) {
            LogicalExpr::Function {
                name: "coalesce".into(),
                args: vec![
                    LogicalExpr::Column {
                        qualifier: None,
                        name: right_out,
                    },
                    LogicalExpr::Literal(ScalarValue::UInt64(0)),
                ],
                distinct: false,
                filter: None,
                order_by: Vec::new(),
                over: None,
            }
        } else {
            LogicalExpr::Column {
                qualifier: None,
                name: right_out,
            }
        };
        agg_joined_map.push((agg_expr.clone(), replacement));
    }

    let joined_scope = ScopedRelation {
        plan: Plan::Join(Box::new(Join {
            left: left_scope.plan,
            right: agg_scope.plan.clone(),
            kind: JoinKind::Left,
            keys,
            condition: None,
            columns: join_cols,
        })),
        entries: joined_entries,
    };
    let rewritten_scalar = substitute_expr_map(scalar_expr, &agg_joined_map);
    let cmp_logical = LogicalExpr::Compare {
        left: Box::new(lhs_expr.clone()),
        op: cmp_op,
        right: Box::new(rewritten_scalar),
    };
    let pred = ctx.lower_expr(&cmp_logical, &joined_scope)?;
    let filtered = ScopedRelation {
        plan: Plan::Filter(Box::new(Filter {
            input: joined_scope.plan,
            predicate: pred,
        })),
        entries: saved_left_entries,
    };
    Ok(Some(project_entries_passthrough(filtered)?))
}

fn substitute_expr_map(expr: &LogicalExpr, map: &[(LogicalExpr, LogicalExpr)]) -> LogicalExpr {
    for (target, replacement) in map {
        if expr == target {
            return replacement.clone();
        }
    }
    super::map_logical_children(expr, &|child| substitute_expr_map(child, map))
}

const fn flip_comparison(op: ComparisonOp) -> ComparisonOp {
    match op {
        ComparisonOp::Lt => ComparisonOp::Gt,
        ComparisonOp::LtEq => ComparisonOp::GtEq,
        ComparisonOp::Gt => ComparisonOp::Lt,
        ComparisonOp::GtEq => ComparisonOp::LtEq,
        other => other,
    }
}

fn rel_has_outer_references(rel: &LogicalRelation) -> bool {
    match rel {
        LogicalRelation::Subquery { input, .. }
        | LogicalRelation::Project { input, .. }
        | LogicalRelation::Aggregate { input, .. }
        | LogicalRelation::Sort { input, .. }
        | LogicalRelation::Limit { input, .. }
        | LogicalRelation::Window { input, .. } => rel_has_outer_references(input),
        LogicalRelation::Filter { input, predicate } => {
            expr_has_subquery(predicate) || rel_has_outer_references(input)
        }
        LogicalRelation::Join {
            left, right, on, ..
        } => {
            on.as_ref().is_some_and(expr_has_subquery)
                || rel_has_outer_references(left)
                || rel_has_outer_references(right)
        }
        LogicalRelation::WithCte { ctes, body, .. } => {
            ctes.iter().any(|cte| rel_has_outer_references(&cte.query))
                || rel_has_outer_references(body)
        }
        LogicalRelation::SetOperation { left, right, .. } => {
            rel_has_outer_references(left) || rel_has_outer_references(right)
        }
        LogicalRelation::Source { .. }
        | LogicalRelation::Values { .. }
        | LogicalRelation::Unit
        | LogicalRelation::TableFunction { .. } => false,
    }
}

/// Replaces any `Exists`, `InSubquery`, or `ScalarSubquery` inside `expr` with `Plan::Subquery` columns.
pub(super) fn materialize_expr_subqueries(
    ctx: &mut CanonicalLowering<'_, '_, '_>,
    scope: ScopedRelation,
    expr: &mut LogicalExpr,
) -> NativeResult<ScopedRelation> {
    materialize_expr_subqueries_guarded(ctx, scope, expr, None)
}

fn demand_guard(
    guard: Option<&Expression>,
    condition: Expression,
    selected: bool,
) -> NativeResult<Expression> {
    let boolean = |value| {
        Expression::literal(
            ExprId::new("canonical.scalar.demand").expect("fixed identifier"),
            ScalarValue::Boolean(value),
        )
    };
    let branch = Expression::new(
        ExprId::new("canonical.scalar.branch")?,
        ExpressionKind::FunctionCall {
            name: "case_when".into(),
            args: vec![condition, boolean(selected), boolean(!selected)],
        },
    );
    Ok(if let Some(guard) = guard {
        Expression::new(
            ExprId::new("canonical.scalar.guard")?,
            ExpressionKind::FunctionCall {
                name: "case_when".into(),
                args: vec![guard.clone(), branch, boolean(false)],
            },
        )
    } else {
        branch
    })
}

fn materialize_expr_subqueries_guarded(
    ctx: &mut CanonicalLowering<'_, '_, '_>,
    mut scope: ScopedRelation,
    expr: &mut LogicalExpr,
    guard: Option<&Expression>,
) -> NativeResult<ScopedRelation> {
    if !expr_has_subquery(expr) {
        return Ok(scope);
    }
    if matches!(
        expr,
        LogicalExpr::InSubquery { .. }
            | LogicalExpr::Exists { .. }
            | LogicalExpr::ScalarSubquery(_)
    ) {
        return materialize_direct_subquery(ctx, scope, expr, guard);
    }
    match expr {
        LogicalExpr::Unary { expr: inner, .. } | LogicalExpr::Cast { expr: inner, .. } => {
            scope = materialize_expr_subqueries_guarded(ctx, scope, inner, guard)?;
        }
        LogicalExpr::Binary { left, right, .. }
        | LogicalExpr::Compare { left, right, .. }
        | LogicalExpr::IsDistinctFrom { left, right, .. }
        | LogicalExpr::Like {
            expr: left,
            pattern: right,
            ..
        } => {
            scope = materialize_expr_subqueries_guarded(ctx, scope, left, guard)?;
            scope = materialize_expr_subqueries_guarded(ctx, scope, right, guard)?;
        }
        LogicalExpr::Between {
            expr: target,
            low,
            high,
            ..
        } => {
            scope = materialize_expr_subqueries_guarded(ctx, scope, target, guard)?;
            scope = materialize_expr_subqueries_guarded(ctx, scope, low, guard)?;
            scope = materialize_expr_subqueries_guarded(ctx, scope, high, guard)?;
        }
        LogicalExpr::InList {
            expr: target, list, ..
        } => {
            scope = materialize_expr_subqueries_guarded(ctx, scope, target, guard)?;
            scope = materialize_expr_slice_subqueries(ctx, scope, list, guard)?;
        }
        LogicalExpr::Case {
            operand,
            when_then,
            else_expr,
        } => {
            scope = materialize_case_subqueries(
                ctx,
                scope,
                operand.as_deref_mut(),
                when_then,
                else_expr.as_deref_mut(),
                guard,
            )?;
        }
        LogicalExpr::Function {
            name,
            args,
            filter,
            order_by,
            over,
            ..
        } => {
            scope = materialize_function_subqueries(
                ctx,
                scope,
                name,
                args,
                filter.as_deref_mut(),
                order_by,
                over.as_mut(),
                guard,
            )?;
        }
        LogicalExpr::Array(items) | LogicalExpr::Tuple(items) => {
            scope = materialize_expr_slice_subqueries(ctx, scope, items, guard)?;
        }
        _ => {}
    }
    Ok(scope)
}

fn materialize_direct_subquery(
    ctx: &mut CanonicalLowering<'_, '_, '_>,
    mut scope: ScopedRelation,
    expr: &mut LogicalExpr,
    guard: Option<&Expression>,
) -> NativeResult<ScopedRelation> {
    let out_col = match expr {
        LogicalExpr::InSubquery {
            expr: lhs,
            subquery,
            negated,
        } => {
            scope = materialize_expr_subqueries_guarded(ctx, scope, lhs, guard)?;
            append_subquery_node(
                ctx,
                &mut scope,
                SubqueryTarget::In(lhs),
                subquery,
                *negated,
                guard,
            )?
        }
        LogicalExpr::Exists { subquery, negated } => append_subquery_node(
            ctx,
            &mut scope,
            SubqueryTarget::Exists,
            subquery,
            *negated,
            guard,
        )?,
        LogicalExpr::ScalarSubquery(subquery) => append_subquery_node(
            ctx,
            &mut scope,
            SubqueryTarget::Scalar,
            subquery,
            false,
            guard,
        )?,
        _ => return Ok(scope),
    };
    *expr = LogicalExpr::Column {
        qualifier: None,
        name: out_col,
    };
    Ok(scope)
}

fn materialize_expr_slice_subqueries(
    ctx: &mut CanonicalLowering<'_, '_, '_>,
    mut scope: ScopedRelation,
    items: &mut [LogicalExpr],
    guard: Option<&Expression>,
) -> NativeResult<ScopedRelation> {
    for item in items {
        scope = materialize_expr_subqueries_guarded(ctx, scope, item, guard)?;
    }
    Ok(scope)
}

fn materialize_case_subqueries(
    ctx: &mut CanonicalLowering<'_, '_, '_>,
    mut scope: ScopedRelation,
    operand: Option<&mut LogicalExpr>,
    when_then: &mut [(LogicalExpr, LogicalExpr)],
    else_expr: Option<&mut LogicalExpr>,
    guard: Option<&Expression>,
) -> NativeResult<ScopedRelation> {
    let lowered_op = if let Some(op) = operand {
        scope = materialize_expr_subqueries_guarded(ctx, scope, op, guard)?;
        Some(ctx.lower_expr(op, &scope)?)
    } else {
        None
    };
    let mut remaining_guard: Option<Expression> = guard.cloned();
    for idx in 0..when_then.len() {
        scope = materialize_expr_subqueries_guarded(
            ctx,
            scope,
            &mut when_then[idx].0,
            remaining_guard.as_ref(),
        )?;
        let later_has_subquery = expr_has_subquery(&when_then[idx].1)
            || when_then[idx + 1..]
                .iter()
                .any(|(nw, nt)| expr_has_subquery(nw) || expr_has_subquery(nt))
            || else_expr.as_deref().is_some_and(expr_has_subquery);
        if later_has_subquery {
            let when_val = ctx.lower_expr(&when_then[idx].0, &scope)?;
            let cond_expr = if let Some(ref op_val) = lowered_op {
                Expression::new(
                    ExprId::new("canonical.case.eq")?,
                    ExpressionKind::Compare {
                        left: Box::new(op_val.clone()),
                        op: ComparisonOp::Eq,
                        right: Box::new(when_val),
                    },
                )
            } else {
                when_val
            };
            let then_guard = demand_guard(remaining_guard.as_ref(), cond_expr.clone(), true)?;
            let next_remaining = demand_guard(remaining_guard.as_ref(), cond_expr, false)?;
            scope = materialize_expr_subqueries_guarded(
                ctx,
                scope,
                &mut when_then[idx].1,
                Some(&then_guard),
            )?;
            remaining_guard = Some(next_remaining);
        }
    }
    if let Some(el) = else_expr {
        scope = materialize_expr_subqueries_guarded(ctx, scope, el, remaining_guard.as_ref())?;
    }
    Ok(scope)
}

#[allow(clippy::too_many_arguments)]
fn materialize_function_subqueries(
    ctx: &mut CanonicalLowering<'_, '_, '_>,
    mut scope: ScopedRelation,
    fn_name: &str,
    args: &mut [LogicalExpr],
    filter: Option<&mut LogicalExpr>,
    order_by: &mut [shardloom_plan::sql_binder::LogicalSortKey],
    over: Option<&mut shardloom_plan::sql_binder::LogicalWindowSpec>,
    guard: Option<&Expression>,
) -> NativeResult<ScopedRelation> {
    if over.is_none()
        && filter.is_none()
        && order_by.is_empty()
        && fn_name.eq_ignore_ascii_case("coalesce")
    {
        let mut coalesce_guard: Option<Expression> = guard.cloned();
        for idx in 0..args.len() {
            scope = materialize_expr_subqueries_guarded(
                ctx,
                scope,
                &mut args[idx],
                coalesce_guard.as_ref(),
            )?;
            if args[idx + 1..].iter().any(expr_has_subquery) {
                let lowered_arg = ctx.lower_expr(&args[idx], &scope)?;
                let is_null = Expression::new(
                    ExprId::new("canonical.coalesce.null")?,
                    ExpressionKind::Unary {
                        op: UnaryOp::IsNull,
                        expr: Box::new(lowered_arg),
                    },
                );
                coalesce_guard = Some(demand_guard(coalesce_guard.as_ref(), is_null, true)?);
            }
        }
        return Ok(scope);
    }
    let mut arg_guard = guard.cloned();
    if let Some(f) = filter {
        scope = materialize_expr_subqueries_guarded(ctx, scope, f, guard)?;
        if over.is_none()
            && (args.iter().any(expr_has_subquery)
                || order_by.iter().any(|k| expr_has_subquery(&k.expr)))
        {
            let filter_pred = ctx.lower_expr(f, &scope)?;
            arg_guard = Some(demand_guard(guard, filter_pred, true)?);
        }
    }
    for arg in args {
        scope = materialize_expr_subqueries_guarded(ctx, scope, arg, arg_guard.as_ref())?;
    }
    for k in order_by {
        scope = materialize_expr_subqueries_guarded(ctx, scope, &mut k.expr, arg_guard.as_ref())?;
    }
    if let Some(spec) = over {
        for p in &mut spec.partition_by {
            scope = materialize_expr_subqueries_guarded(ctx, scope, p, guard)?;
        }
        for k in &mut spec.order_by {
            scope = materialize_expr_subqueries_guarded(ctx, scope, &mut k.expr, guard)?;
        }
    }
    Ok(scope)
}

#[derive(Clone, Copy)]
enum SubqueryTarget<'a> {
    Exists,
    Scalar,
    In(&'a LogicalExpr),
}

fn append_subquery_node(
    ctx: &mut CanonicalLowering<'_, '_, '_>,
    outer: &mut ScopedRelation,
    target: SubqueryTarget<'_>,
    subquery: &LogicalRelation,
    negated: bool,
    guard: Option<&Expression>,
) -> NativeResult<String> {
    let is_scalar = matches!(target, SubqueryTarget::Scalar);
    if !is_scalar
        && let Ok((inner_rel, corr_pairs)) =
            extract_inner_correlation_for_subquery(ctx, outer, subquery)
        && let Ok(mut inner_scope) = ctx.lower_relation(&inner_rel)
    {
        let mut correlation = Vec::with_capacity(corr_pairs.len());
        for (outer_expr, inner_expr) in &corr_pairs {
            let lk = resolve_or_project_key(ctx, outer, outer_expr)?;
            let rk = resolve_or_project_key(ctx, &mut inner_scope, inner_expr)?;
            correlation.push(JoinKey {
                left: lk,
                right: rk,
            });
        }
        return finish_subquery_node(
            ctx,
            outer,
            target,
            inner_scope,
            correlation,
            negated,
            false,
            guard,
        );
    }
    if let Ok(inner_scope) = ctx.lower_relation(subquery) {
        return finish_subquery_node(
            ctx,
            outer,
            target,
            inner_scope,
            Vec::new(),
            negated,
            false,
            guard,
        );
    }
    let mut attached_outer = None;
    let inner_scope = lower_correlated_relation(ctx, outer, subquery, &mut attached_outer)?;
    finish_subquery_node(
        ctx,
        outer,
        target,
        inner_scope,
        Vec::new(),
        negated,
        true,
        guard,
    )
}

#[allow(clippy::too_many_arguments)]
fn finish_subquery_node(
    ctx: &mut CanonicalLowering<'_, '_, '_>,
    outer: &mut ScopedRelation,
    target: SubqueryTarget<'_>,
    inner_scope: ScopedRelation,
    correlation: Vec<JoinKey>,
    negated: bool,
    parameterized: bool,
    guard: Option<&Expression>,
) -> NativeResult<String> {
    let kind = match target {
        SubqueryTarget::Exists => SubqueryKind::Exists,
        SubqueryTarget::Scalar => {
            if inner_scope.entries.len() != 1 {
                return Err(unsupported_sql_error(
                    "scalar subquery must project exactly one column",
                ));
            }
            SubqueryKind::Scalar
        }
        SubqueryTarget::In(lhs) => {
            if inner_scope.entries.len() != 1 {
                return Err(unsupported_sql_error(
                    "IN subquery must project exactly one column",
                ));
            }
            let lk = resolve_or_project_key(ctx, outer, lhs)?;
            let rk = ColumnRef::new(inner_scope.entries[0].internal.clone())?;
            SubqueryKind::In {
                columns: vec![JoinKey {
                    left: lk,
                    right: rk,
                }],
            }
        }
    };
    let relation = if parameterized {
        strip_materialized_in_plan(inner_scope.plan)
    } else {
        inner_scope.plan
    };
    let existing: Vec<String> = outer.entries.iter().map(|e| e.internal.clone()).collect();
    let out_col = fresh_col_names(&existing, &mut ctx.lowerer.serial);
    let node = Box::new(Subquery {
        input: outer.plan.clone(),
        relation,
        kind,
        evaluation_guard: guard.cloned(),
        correlation,
        output_column: out_col.clone(),
        negated,
    });
    outer.plan = if parameterized {
        Plan::CorrelatedSubquery(node)
    } else {
        Plan::Subquery(node)
    };
    outer.entries.push(ScopeColumn {
        internal: out_col.clone(),
        qualifier: None,
        output: out_col.clone(),
    });
    Ok(out_col)
}

fn strip_materialized_in_plan(plan: Plan) -> Plan {
    match plan {
        Plan::Materialized(inner) => strip_materialized_in_plan(inner.as_ref().clone()),
        Plan::Join(mut node) => {
            node.left = strip_materialized_in_plan(node.left);
            node.right = strip_materialized_in_plan(node.right);
            Plan::Join(node)
        }
        Plan::AsOfJoin(mut node) => {
            node.left = strip_materialized_in_plan(node.left);
            node.right = strip_materialized_in_plan(node.right);
            Plan::AsOfJoin(node)
        }
        Plan::Set(mut node) => {
            node.left = strip_materialized_in_plan(node.left);
            node.right = strip_materialized_in_plan(node.right);
            Plan::Set(node)
        }
        Plan::Subquery(mut node) => {
            node.input = strip_materialized_in_plan(node.input);
            node.relation = strip_materialized_in_plan(node.relation);
            Plan::Subquery(node)
        }
        Plan::CorrelatedSubquery(mut node) => {
            node.input = strip_materialized_in_plan(node.input);
            node.relation = strip_materialized_in_plan(node.relation);
            Plan::CorrelatedSubquery(node)
        }
        Plan::Window(mut node) => {
            node.input = strip_materialized_in_plan(node.input);
            Plan::Window(node)
        }
        Plan::GapFill(mut node) => {
            node.input = strip_materialized_in_plan(node.input);
            Plan::GapFill(node)
        }
        Plan::Project(mut node) => {
            node.input = strip_materialized_in_plan(node.input);
            Plan::Project(node)
        }
        Plan::Filter(mut node) => {
            node.input = strip_materialized_in_plan(node.input);
            Plan::Filter(node)
        }
        Plan::Sort(mut node) => {
            node.input = strip_materialized_in_plan(node.input);
            Plan::Sort(node)
        }
        Plan::Limit(mut node) => {
            node.input = strip_materialized_in_plan(node.input);
            Plan::Limit(node)
        }
        Plan::Pipeline(mut node) => {
            node.input = strip_materialized_in_plan(node.input);
            Plan::Pipeline(node)
        }
        Plan::Aggregate(mut node) => {
            node.input = strip_materialized_in_plan(node.input);
            Plan::Aggregate(node)
        }
        Plan::Unary(mut node) => {
            node.input = strip_materialized_in_plan(node.input);
            Plan::Unary(node)
        }
        other => other,
    }
}

fn attach_outer_scope(
    ctx: &mut CanonicalLowering<'_, '_, '_>,
    inner: &mut ScopedRelation,
    outer: &ScopedRelation,
    eq_keys: &[(ColumnRef, ColumnRef)],
) -> NativeResult<Vec<ScopeColumn>> {
    let mut used: Vec<String> = inner.entries.iter().map(|e| e.internal.clone()).collect();
    let mut join_cols = Vec::with_capacity(inner.entries.len() + outer.entries.len());
    for entry in &inner.entries {
        join_cols.push(JoinColumn {
            side: Side::Left,
            column: ColumnRef::new(entry.internal.clone())?,
            output_column: entry.internal.clone(),
        });
    }
    let mut outer_cols = Vec::with_capacity(outer.entries.len());
    for entry in &outer.entries {
        let fresh = fresh_col_names(&used, &mut ctx.lowerer.serial);
        used.push(fresh.clone());
        join_cols.push(JoinColumn {
            side: Side::Right,
            column: ColumnRef::new(entry.internal.clone())?,
            output_column: fresh.clone(),
        });
        outer_cols.push(ScopeColumn {
            internal: fresh,
            qualifier: entry.qualifier.clone(),
            output: entry.output.clone(),
        });
    }
    let keys = eq_keys
        .iter()
        .map(|(lk, rk)| JoinKey {
            left: lk.clone(),
            right: rk.clone(),
        })
        .collect();
    inner.plan = Plan::Join(Box::new(Join {
        left: inner.plan.clone(),
        right: Plan::Outer,
        kind: if eq_keys.is_empty() {
            JoinKind::Cross
        } else {
            JoinKind::Inner
        },
        keys,
        condition: None,
        columns: join_cols,
    }));
    Ok(outer_cols)
}

fn rewrite_outer_refs_in_expr(
    expr: &LogicalExpr,
    inner_entries: &[ScopeColumn],
    outer_cols: &[ScopeColumn],
) -> LogicalExpr {
    if let LogicalExpr::Column { qualifier, name } = expr
        && !scope_matches_column(inner_entries, qualifier.as_deref(), name)
        && let Some(matched) = resolve_outer_entry(outer_cols, qualifier.as_deref(), name)
    {
        return LogicalExpr::Column {
            qualifier: None,
            name: matched.to_owned(),
        };
    }
    super::map_logical_children(expr, &|child| {
        rewrite_outer_refs_in_expr(child, inner_entries, outer_cols)
    })
}

fn resolve_outer_entry<'a>(
    outer_cols: &'a [ScopeColumn],
    qualifier: Option<&str>,
    name: &str,
) -> Option<&'a str> {
    if let Some(q) = qualifier {
        outer_cols
            .iter()
            .find(|e| e.qualifier.as_deref() == Some(q) && e.output == name)
            .map(|e| e.internal.as_str())
    } else {
        let matches: Vec<&str> = outer_cols
            .iter()
            .filter(|e| e.output == name)
            .map(|e| e.internal.as_str())
            .collect();
        (matches.len() == 1).then(|| matches[0])
    }
}

fn combined_scope_with_outer(
    inner: &ScopedRelation,
    outer_cols: Option<&[ScopeColumn]>,
) -> ScopedRelation {
    let mut entries = inner.entries.clone();
    if let Some(extras) = outer_cols {
        for extra in extras {
            entries.push(ScopeColumn {
                internal: extra.internal.clone(),
                qualifier: None,
                output: extra.internal.clone(),
            });
        }
    }
    ScopedRelation {
        plan: inner.plan.clone(),
        entries,
    }
}

fn lower_correlated_relation(
    ctx: &mut CanonicalLowering<'_, '_, '_>,
    outer: &ScopedRelation,
    rel: &LogicalRelation,
    attached_outer: &mut Option<Vec<ScopeColumn>>,
) -> NativeResult<ScopedRelation> {
    if attached_outer.is_none()
        && let Ok(standalone) = ctx.lower_relation(rel)
    {
        return Ok(standalone);
    }
    match rel {
        LogicalRelation::Subquery { input, alias } => {
            let mut inner = lower_correlated_relation(ctx, outer, input, attached_outer)?;
            for entry in &mut inner.entries {
                entry.qualifier.clone_from(alias);
            }
            Ok(inner)
        }
        LogicalRelation::Filter { input, predicate } => {
            lower_correlated_filter(ctx, outer, input, predicate, attached_outer)
        }
        LogicalRelation::Project {
            input,
            items,
            distinct,
        } => lower_correlated_project(ctx, outer, input, items, *distinct, attached_outer),
        _ => ctx.lower_relation(rel),
    }
}

fn lower_correlated_filter(
    ctx: &mut CanonicalLowering<'_, '_, '_>,
    outer: &ScopedRelation,
    input: &LogicalRelation,
    predicate: &LogicalExpr,
    attached_outer: &mut Option<Vec<ScopeColumn>>,
) -> NativeResult<ScopedRelation> {
    let mut inner = lower_correlated_relation(ctx, outer, input, attached_outer)?;
    if attached_outer.is_none()
        && !expr_has_subquery(predicate)
        && let Some((corr_pairs, pure_inner)) =
            split_correlation_conjuncts(predicate, outer, &inner)
        && !corr_pairs.is_empty()
    {
        if !pure_inner.is_empty() {
            inner = apply_scalar_filter(ctx, inner, &pure_inner)?;
        }
        let mut eq_keys = Vec::with_capacity(corr_pairs.len());
        for (outer_expr, inner_expr) in &corr_pairs {
            let LogicalExpr::Column { qualifier, name } = outer_expr else {
                eq_keys.clear();
                break;
            };
            let Some(outer_internal) =
                resolve_outer_entry(&outer.entries, qualifier.as_deref(), name)
            else {
                eq_keys.clear();
                break;
            };
            let inner_ref = resolve_or_project_key(ctx, &mut inner, inner_expr)?;
            eq_keys.push((inner_ref, ColumnRef::new(outer_internal.to_owned())?));
        }
        if eq_keys.len() == corr_pairs.len() {
            *attached_outer = Some(attach_outer_scope(ctx, &mut inner, outer, &eq_keys)?);
            return Ok(inner);
        }
    }
    if attached_outer.is_none() {
        *attached_outer = Some(attach_outer_scope(ctx, &mut inner, outer, &[])?);
    }
    let outer_cols = attached_outer.as_deref().unwrap_or(&[]);
    let mut combined = combined_scope_with_outer(&inner, Some(outer_cols));
    let mut rewritten = rewrite_outer_refs_in_expr(predicate, &inner.entries, outer_cols);
    let saved_len = combined.entries.len();
    combined = materialize_expr_subqueries(ctx, combined, &mut rewritten)?;
    let lowered_pred = ctx.lower_expr(&rewritten, &combined)?;
    combined.plan = Plan::Filter(Box::new(Filter {
        input: combined.plan,
        predicate: lowered_pred,
    }));
    if combined.entries.len() != saved_len {
        combined.entries.truncate(saved_len);
        combined = project_entries_passthrough(combined)?;
    }
    inner.plan = combined.plan;
    Ok(inner)
}

fn lower_correlated_project(
    ctx: &mut CanonicalLowering<'_, '_, '_>,
    outer: &ScopedRelation,
    input: &LogicalRelation,
    items: &[LogicalProjectionItem],
    distinct: bool,
    attached_outer: &mut Option<Vec<ScopeColumn>>,
) -> NativeResult<ScopedRelation> {
    if let LogicalRelation::Aggregate {
        input: agg_input,
        group_by,
        having,
        ..
    } = input
    {
        return lower_correlated_agg_project(
            ctx,
            outer,
            agg_input,
            group_by,
            having.as_ref(),
            items,
            distinct,
            attached_outer,
        );
    }
    let mut inner = lower_correlated_relation(ctx, outer, input, attached_outer)?;
    let needs_outer = items.iter().any(|item| match item {
        LogicalProjectionItem::Expr { expr, .. } => expr_needs_outer(expr, &inner.entries, outer),
        LogicalProjectionItem::Wildcard { .. } => false,
    });
    if needs_outer && attached_outer.is_none() {
        *attached_outer = Some(attach_outer_scope(ctx, &mut inner, outer, &[])?);
    }
    let outer_cols = attached_outer.take().unwrap_or_default();
    let combined = combined_scope_with_outer(&inner, Some(&outer_cols));
    let rw_items = rewrite_outer_refs_in_items(items, &inner.entries, &outer_cols);
    ctx.lower_window_project_over_scope(combined, &rw_items, distinct, None)
}

#[allow(clippy::too_many_arguments)]
fn lower_correlated_agg_project(
    ctx: &mut CanonicalLowering<'_, '_, '_>,
    outer: &ScopedRelation,
    agg_input: &LogicalRelation,
    group_by: &[LogicalExpr],
    having: Option<&LogicalExpr>,
    items: &[LogicalProjectionItem],
    distinct: bool,
    attached_outer: &mut Option<Vec<ScopeColumn>>,
) -> NativeResult<ScopedRelation> {
    let inner_base = lower_correlated_relation(ctx, outer, agg_input, attached_outer)?;
    let outer_cols = attached_outer.take().unwrap_or_default();
    let agg_scope = combined_scope_with_outer(&inner_base, Some(&outer_cols));
    let rw_group: Vec<LogicalExpr> = group_by
        .iter()
        .map(|g| rewrite_outer_refs_in_expr(g, &inner_base.entries, &outer_cols))
        .collect();
    let rw_having = having.map(|h| rewrite_outer_refs_in_expr(h, &inner_base.entries, &outer_cols));
    let rw_items = rewrite_outer_refs_in_items(items, &inner_base.entries, &outer_cols);
    let (mut post, agg_map, group_map, alias_exprs) = ctx.lower_aggregate_prefix_from_scope(
        &agg_scope,
        &rw_group,
        rw_having.as_ref(),
        &rw_items,
        None,
        &[],
    )?;
    post = ctx.materialize_having(post, rw_having.as_ref(), &alias_exprs, &agg_map, &group_map)?;
    if !rw_group.is_empty()
        && outer_cols
            .iter()
            .any(|o| rw_items_or_having_uses(o, &rw_items, rw_having.as_ref()))
    {
        return Err(unsupported_sql_error(
            "correlated grouped subquery projecting ungrouped outer columns is not admitted",
        ));
    }
    if rw_group.is_empty()
        && items.iter().any(|item| match item {
            LogicalProjectionItem::Expr { expr, .. } => expr_needs_outer(expr, &[], outer),
            LogicalProjectionItem::Wildcard { .. } => false,
        })
    {
        let re_attached = attach_outer_scope(ctx, &mut post, outer, &[])?;
        let post_combined = combined_scope_with_outer(&post, Some(&re_attached));
        let post_items = rewrite_outer_refs_in_items(items, &post.entries, &re_attached);
        return ctx.finish_aggregate_project(
            post_combined,
            &post_items,
            distinct,
            None,
            &alias_exprs,
            &agg_map,
            &group_map,
        );
    }
    ctx.finish_aggregate_project(
        post,
        &rw_items,
        distinct,
        None,
        &alias_exprs,
        &agg_map,
        &group_map,
    )
}

fn rewrite_outer_refs_in_items(
    items: &[LogicalProjectionItem],
    inner_entries: &[ScopeColumn],
    outer_cols: &[ScopeColumn],
) -> Vec<LogicalProjectionItem> {
    items
        .iter()
        .map(|item| match item {
            LogicalProjectionItem::Wildcard { qualifier } => LogicalProjectionItem::Wildcard {
                qualifier: qualifier.clone(),
            },
            LogicalProjectionItem::Expr { expr, alias } => LogicalProjectionItem::Expr {
                expr: Box::new(rewrite_outer_refs_in_expr(expr, inner_entries, outer_cols)),
                alias: alias.clone(),
            },
        })
        .collect()
}

fn expr_needs_outer(
    expr: &LogicalExpr,
    inner_entries: &[ScopeColumn],
    outer: &ScopedRelation,
) -> bool {
    let mut needs = false;
    let _ = visit_col_refs(expr, &mut |q, name| {
        if !scope_matches_column(inner_entries, q, name)
            && scope_matches_column(&outer.entries, q, name)
        {
            needs = true;
        }
        Ok(())
    });
    needs
}

fn rw_items_or_having_uses(
    col: &ScopeColumn,
    items: &[LogicalProjectionItem],
    having: Option<&LogicalExpr>,
) -> bool {
    let mut used = false;
    for item in items {
        if let LogicalProjectionItem::Expr { expr, .. } = item {
            let _ = visit_col_refs(expr, &mut |q, n| {
                if q.is_none() && n == col.internal {
                    used = true;
                }
                Ok(())
            });
        }
    }
    if let Some(h) = having {
        let _ = visit_col_refs(h, &mut |q, n| {
            if q.is_none() && n == col.internal {
                used = true;
            }
            Ok(())
        });
    }
    used
}

fn extract_inner_correlation_for_subquery(
    ctx: &mut CanonicalLowering<'_, '_, '_>,
    outer: &ScopedRelation,
    rel: &LogicalRelation,
) -> NativeResult<(LogicalRelation, Vec<(LogicalExpr, LogicalExpr)>)> {
    match rel {
        LogicalRelation::Subquery { input, alias } => {
            let (inner, corr) = extract_inner_correlation_for_subquery(ctx, outer, input)?;
            Ok((
                LogicalRelation::Subquery {
                    input: Box::new(inner),
                    alias: alias.clone(),
                },
                corr,
            ))
        }
        LogicalRelation::Project {
            input,
            items,
            distinct,
        } => {
            let (inner, corr) = extract_inner_correlation_for_subquery(ctx, outer, input)?;
            Ok((
                LogicalRelation::Project {
                    input: Box::new(inner),
                    items: items.clone(),
                    distinct: *distinct,
                },
                corr,
            ))
        }
        LogicalRelation::Filter { input, predicate } => {
            let inner_scope = ctx.lower_relation(input)?;
            let Some((corr_pairs, pure_inner)) =
                split_correlation_conjuncts(predicate, outer, &inner_scope)
            else {
                return Ok((rel.clone(), Vec::new()));
            };
            if corr_pairs.is_empty() {
                return Ok((rel.clone(), Vec::new()));
            }
            if let Some(rest) = conjoin_logical(&pure_inner) {
                Ok((
                    LogicalRelation::Filter {
                        input: input.clone(),
                        predicate: rest,
                    },
                    corr_pairs,
                ))
            } else {
                Ok((*input.clone(), corr_pairs))
            }
        }
        _ => Ok((rel.clone(), Vec::new())),
    }
}
