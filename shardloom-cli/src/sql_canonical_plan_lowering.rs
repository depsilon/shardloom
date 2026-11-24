//! Canonical AST-bound SQL plan lowering onto the shared native relational engine.
//!
//! Handles relational SQL queries bound by `shardloom_plan::sql_binder` (multiway
//! joins, comma joins, subqueries, AST-bound windows, set operations, CTE
//! materialization, and canonical scalar/aggregate expressions) without external
//! engine fallback.

#[path = "sql_canonical_subquery.rs"]
mod subquery;
#[path = "sql_canonical_window.rs"]
mod window;

use super::{
    Aggregate, AggregateFunction, ColumnRef, ExprId, Expression, ExpressionKind, Filter, Join,
    JoinColumn, JoinKind, Limit, Lowered, Lowerer, NativeResult, NativeWindowFunction, NullOrder,
    OrderKey, ParsedRelationLeaf, Plan, Project, Set, SetKind, Side, Sort, Subquery, SubqueryKind,
    VortexSimpleAggregateMeasure, Window, WindowExpression, append_equality_keys, column,
    unsupported_sql_error, validate_sql_identifier,
};
use shardloom_core::{BinaryOp, ComparisonOp, ScalarValue, UnaryOp};
use shardloom_plan::sql_binder::{
    BoundSqlPlan, LogicalAggregateSpec, LogicalCte, LogicalExpr, LogicalJoinType,
    LogicalProjectionItem, LogicalRelation, LogicalSetOp, LogicalSortKey, LogicalWindowSpec,
    bind_sql, expr::parse_core_dtype_label,
};
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::Arc;

/// Metadata summary extracted from an AST-bound canonical plan before execution.
pub(super) struct CanonicalPlanSummary {
    pub(super) metadata_bytes: usize,
    pub(super) sources: BTreeMap<ParsedRelationLeaf, u8>,
    pub(super) input_uses: BTreeMap<usize, usize>,
    pub(super) depth: usize,
}

#[derive(Clone)]
struct ScopeColumn {
    internal: String,
    qualifier: Option<String>,
    output: String,
}

type ExpandedProjection = (
    Vec<LogicalProjectionItem>,
    Vec<(String, Expression)>,
    Vec<ScopeColumn>,
);
type GroupByColumns = (Vec<String>, Vec<(LogicalExpr, String)>);
type AggregateMeasures = (
    Vec<VortexSimpleAggregateMeasure>,
    Vec<(LogicalExpr, String)>,
);

#[derive(Clone)]
struct ScopedRelation {
    plan: Plan,
    entries: Vec<ScopeColumn>,
}

impl ScopedRelation {
    fn from_columns(plan: Plan, columns: Vec<String>, qualifier: Option<&str>) -> Self {
        let qualifier = qualifier.map(str::to_owned);
        let entries = columns
            .into_iter()
            .map(|name| ScopeColumn {
                internal: name.clone(),
                qualifier: qualifier.clone(),
                output: name,
            })
            .collect();
        Self { plan, entries }
    }

    fn with_qualifier(mut self, qualifier: Option<&str>) -> Self {
        let qualifier = qualifier.map(str::to_owned);
        for entry in &mut self.entries {
            entry.qualifier.clone_from(&qualifier);
        }
        self
    }

    fn resolve(&self, qualifier: Option<&str>, name: &str) -> NativeResult<String> {
        let mut matches = self.entries.iter().filter(|entry| match qualifier {
            Some(q) => entry.qualifier.as_deref() == Some(q) && entry.output == name,
            None => entry.output == name,
        });
        if let Some(first) = matches.next() {
            if matches.next().is_some() {
                let label = match qualifier {
                    Some(q) => format!("{q}.{name}"),
                    None => name.to_string(),
                };
                return Err(unsupported_sql_error(&format!(
                    "column {label:?} is ambiguous; qualify it with a source alias"
                )));
            }
            return Ok(first.internal.clone());
        }
        if let Some(q) = qualifier {
            let exact_name = format!("{q}.{name}");
            let mut exact = self
                .entries
                .iter()
                .filter(|entry| entry.internal == exact_name);
            if let Some(first) = exact.next()
                && exact.next().is_none()
            {
                return Ok(first.internal.clone());
            }
            return Err(unsupported_sql_error(&format!(
                "column \"{exact_name}\" is not present in the native input schema"
            )));
        }
        Err(unsupported_sql_error(&format!(
            "column {name:?} is not present in the native input schema"
        )))
    }

    fn into_lowered(self) -> Lowered {
        let mut qualifiers = BTreeMap::new();
        for entry in &self.entries {
            if let Some(q) = &entry.qualifier {
                qualifiers.insert(format!("{q}.{}", entry.output), entry.internal.clone());
            }
        }
        Lowered {
            plan: self.plan,
            columns: self.entries.into_iter().map(|e| e.output).collect(),
            qualifiers,
        }
    }
}

/// Parses SQL with the canonical `shardloom-plan` binder if this slice can execute it natively.
#[inline(never)]
pub(super) fn parse_canonical_plan(raw: &str) -> Option<Box<BoundSqlPlan>> {
    let bound = bind_sql(raw).ok()?;
    can_lower_canonically(&bound).then(|| Box::new(bound))
}

/// Checks whether a canonical `BoundSqlPlan` lies within the canonical native lowering slice.
pub(super) fn can_lower_canonically(bound: &BoundSqlPlan) -> bool {
    rel_supported(&bound.root, &BTreeSet::new())
        && !has_unresolved_outer_qualifier(&bound.root, &BTreeSet::new())
}

/// Returns whether a canonical plan should take precedence over the legacy SQL parser.
///
/// Restricts precedence to unparameterized-relation queries (`bound_names.is_empty()`) that use
/// canonical-converged features (multi-ref/explicit CTEs, AST windows, set operations, or
/// subqueries) without legacy `outer.*` unresolved correlation qualifiers.
pub(super) fn prefers_canonical_lowering(bound: &BoundSqlPlan, bound_names: &[String]) -> bool {
    bound_names.is_empty()
        && rel_has_canonical_priority(&bound.root)
        && !has_unresolved_outer_qualifier(&bound.root, &BTreeSet::new())
}

fn rel_has_canonical_priority(rel: &LogicalRelation) -> bool {
    match rel {
        LogicalRelation::WithCte {
            ctes,
            recursive: false,
            body,
        } => with_cte_has_canonical_priority(ctes, body),
        LogicalRelation::Window { .. } | LogicalRelation::SetOperation { .. } => true,
        LogicalRelation::Subquery { input, .. } | LogicalRelation::Limit { input, .. } => {
            rel_has_canonical_priority(input)
        }
        LogicalRelation::Filter { input, predicate } => {
            expr_has_canonical_priority(predicate) || rel_has_canonical_priority(input)
        }
        LogicalRelation::Project { input, items, .. } => {
            items.iter().any(|item| {
                matches!(
                    item,
                    LogicalProjectionItem::Expr { expr, .. } if expr_has_canonical_priority(expr)
                )
            }) || rel_has_canonical_priority(input)
        }
        LogicalRelation::Aggregate {
            input,
            group_by,
            aggregates,
            having,
            ..
        } => {
            group_by.iter().any(expr_has_canonical_priority)
                || aggregates.iter().any(agg_has_canonical_priority)
                || having.as_ref().is_some_and(expr_has_canonical_priority)
                || rel_has_canonical_priority(input)
        }
        LogicalRelation::Sort { input, keys } => {
            keys.iter().any(|k| expr_has_canonical_priority(&k.expr))
                || rel_has_canonical_priority(input)
        }
        LogicalRelation::Join {
            left, right, on, ..
        } => {
            on.as_ref().is_some_and(expr_has_canonical_priority)
                || rel_has_canonical_priority(left)
                || rel_has_canonical_priority(right)
        }
        LogicalRelation::Source { .. }
        | LogicalRelation::Values { .. }
        | LogicalRelation::Unit
        | LogicalRelation::TableFunction { .. }
        | LogicalRelation::WithCte {
            recursive: true, ..
        } => false,
    }
}

fn with_cte_has_canonical_priority(ctes: &[LogicalCte], body: &LogicalRelation) -> bool {
    for (idx, cte) in ctes.iter().enumerate() {
        let mut counts = BTreeMap::new();
        let mut shadowed = BTreeSet::new();
        for later in &ctes[idx + 1..] {
            count_rel_cte_refs(&later.query, &shadowed, &mut counts);
            shadowed.insert(later.name.to_ascii_lowercase());
        }
        count_rel_cte_refs(body, &shadowed, &mut counts);
        let refs = counts
            .get(&cte.name.to_ascii_lowercase())
            .copied()
            .unwrap_or(0);
        if cte.materialized == Some(true) || refs >= 2 || rel_has_canonical_priority(&cte.query) {
            return true;
        }
    }
    rel_has_canonical_priority(body)
}

fn agg_has_canonical_priority(spec: &LogicalAggregateSpec) -> bool {
    spec.args.iter().any(expr_has_canonical_priority)
        || spec
            .filter
            .as_ref()
            .is_some_and(expr_has_canonical_priority)
        || spec
            .order_by
            .iter()
            .any(|k| expr_has_canonical_priority(&k.expr))
}

fn expr_has_canonical_priority(expr: &LogicalExpr) -> bool {
    match expr {
        LogicalExpr::Exists { .. }
        | LogicalExpr::InSubquery { .. }
        | LogicalExpr::ScalarSubquery(_)
        | LogicalExpr::Function { over: Some(_), .. } => true,
        LogicalExpr::Unary { expr, .. } | LogicalExpr::Cast { expr, .. } => {
            expr_has_canonical_priority(expr)
        }
        LogicalExpr::Binary { left, right, .. }
        | LogicalExpr::Compare { left, right, .. }
        | LogicalExpr::IsDistinctFrom { left, right, .. }
        | LogicalExpr::Like {
            expr: left,
            pattern: right,
            ..
        } => expr_has_canonical_priority(left) || expr_has_canonical_priority(right),
        LogicalExpr::Between {
            expr, low, high, ..
        } => {
            expr_has_canonical_priority(expr)
                || expr_has_canonical_priority(low)
                || expr_has_canonical_priority(high)
        }
        LogicalExpr::InList { expr, list, .. } => {
            expr_has_canonical_priority(expr) || list.iter().any(expr_has_canonical_priority)
        }
        LogicalExpr::Case {
            operand,
            when_then,
            else_expr,
        } => {
            operand.as_deref().is_some_and(expr_has_canonical_priority)
                || when_then
                    .iter()
                    .any(|(w, t)| expr_has_canonical_priority(w) || expr_has_canonical_priority(t))
                || else_expr
                    .as_deref()
                    .is_some_and(expr_has_canonical_priority)
        }
        LogicalExpr::Function {
            args,
            filter,
            order_by,
            over: None,
            ..
        } => {
            args.iter().any(expr_has_canonical_priority)
                || filter.as_deref().is_some_and(expr_has_canonical_priority)
                || order_by
                    .iter()
                    .any(|k| expr_has_canonical_priority(&k.expr))
        }
        LogicalExpr::Array(items) | LogicalExpr::Tuple(items) => {
            items.iter().any(expr_has_canonical_priority)
        }
        LogicalExpr::Literal(_)
        | LogicalExpr::Column { .. }
        | LogicalExpr::Parameter(_)
        | LogicalExpr::Wildcard { .. }
        | LogicalExpr::Interval { .. } => false,
    }
}

fn rel_output_qualifiers(rel: &LogicalRelation, out: &mut BTreeSet<String>) {
    match rel {
        LogicalRelation::Source {
            reference,
            quoted_path,
            alias,
        } => {
            if let Some(a) = alias {
                out.insert(a.to_ascii_lowercase());
            } else if !quoted_path {
                out.insert(reference.to_ascii_lowercase());
            }
        }
        LogicalRelation::Subquery { alias: Some(a), .. } => {
            out.insert(a.to_ascii_lowercase());
        }
        LogicalRelation::Subquery { input, alias: None }
        | LogicalRelation::Filter { input, .. }
        | LogicalRelation::Project { input, .. }
        | LogicalRelation::Aggregate { input, .. }
        | LogicalRelation::Window { input, .. }
        | LogicalRelation::Sort { input, .. }
        | LogicalRelation::Limit { input, .. } => rel_output_qualifiers(input, out),
        LogicalRelation::Join {
            left,
            right,
            join_type,
            ..
        } => {
            rel_output_qualifiers(left, out);
            if !matches!(join_type, LogicalJoinType::Semi | LogicalJoinType::Anti) {
                rel_output_qualifiers(right, out);
            }
        }
        LogicalRelation::WithCte { body, .. } => rel_output_qualifiers(body, out),
        LogicalRelation::SetOperation { .. }
        | LogicalRelation::Values { .. }
        | LogicalRelation::Unit
        | LogicalRelation::TableFunction { .. } => {}
    }
}

fn has_unresolved_outer_qualifier(rel: &LogicalRelation, outer_visible: &BTreeSet<String>) -> bool {
    match rel {
        LogicalRelation::Source { .. } | LogicalRelation::Values { .. } | LogicalRelation::Unit => {
            false
        }
        LogicalRelation::Subquery { input, .. } | LogicalRelation::Limit { input, .. } => {
            has_unresolved_outer_qualifier(input, outer_visible)
        }
        LogicalRelation::Filter { input, predicate } => {
            if has_unresolved_outer_qualifier(input, outer_visible) {
                return true;
            }
            let mut scope = outer_visible.clone();
            rel_output_qualifiers(input, &mut scope);
            expr_has_unresolved_qualifier(predicate, &scope)
        }
        LogicalRelation::Project { input, items, .. } => {
            if has_unresolved_outer_qualifier(input, outer_visible) {
                return true;
            }
            let mut scope = outer_visible.clone();
            rel_output_qualifiers(input, &mut scope);
            items.iter().any(|item| {
                matches!(
                    item,
                    LogicalProjectionItem::Expr { expr, .. }
                        if expr_has_unresolved_qualifier(expr, &scope)
                )
            })
        }
        LogicalRelation::Aggregate {
            input,
            group_by,
            aggregates,
            having,
            ..
        } => agg_has_unresolved_qualifier(
            input,
            group_by,
            aggregates,
            having.as_ref(),
            outer_visible,
        ),
        LogicalRelation::Window { input, windows } => {
            if has_unresolved_outer_qualifier(input, outer_visible) {
                return true;
            }
            let mut scope = outer_visible.clone();
            rel_output_qualifiers(input, &mut scope);
            windows.iter().any(|(_, spec)| {
                spec.partition_by
                    .iter()
                    .any(|p| expr_has_unresolved_qualifier(p, &scope))
                    || spec
                        .order_by
                        .iter()
                        .any(|k| expr_has_unresolved_qualifier(&k.expr, &scope))
            })
        }
        LogicalRelation::Sort { input, keys } => {
            if has_unresolved_outer_qualifier(input, outer_visible) {
                return true;
            }
            let mut scope = outer_visible.clone();
            rel_output_qualifiers(input, &mut scope);
            keys.iter()
                .any(|k| expr_has_unresolved_qualifier(&k.expr, &scope))
        }
        LogicalRelation::Join {
            left, right, on, ..
        } => {
            if has_unresolved_outer_qualifier(left, outer_visible)
                || has_unresolved_outer_qualifier(right, outer_visible)
            {
                return true;
            }
            on.as_ref().is_some_and(|cond| {
                let mut scope = outer_visible.clone();
                rel_output_qualifiers(left, &mut scope);
                rel_output_qualifiers(right, &mut scope);
                expr_has_unresolved_qualifier(cond, &scope)
            })
        }
        LogicalRelation::SetOperation { left, right, .. } => {
            has_unresolved_outer_qualifier(left, outer_visible)
                || has_unresolved_outer_qualifier(right, outer_visible)
        }
        LogicalRelation::WithCte { ctes, body, .. } => {
            ctes.iter()
                .any(|cte| has_unresolved_outer_qualifier(&cte.query, outer_visible))
                || has_unresolved_outer_qualifier(body, outer_visible)
        }
        LogicalRelation::TableFunction { .. } => true,
    }
}

fn agg_has_unresolved_qualifier(
    input: &LogicalRelation,
    group_by: &[LogicalExpr],
    aggregates: &[LogicalAggregateSpec],
    having: Option<&LogicalExpr>,
    outer_visible: &BTreeSet<String>,
) -> bool {
    if has_unresolved_outer_qualifier(input, outer_visible) {
        return true;
    }
    let mut scope = outer_visible.clone();
    rel_output_qualifiers(input, &mut scope);
    group_by
        .iter()
        .any(|g| expr_has_unresolved_qualifier(g, &scope))
        || aggregates.iter().any(|spec| {
            spec.args
                .iter()
                .any(|a| expr_has_unresolved_qualifier(a, &scope))
                || spec
                    .filter
                    .as_ref()
                    .is_some_and(|f| expr_has_unresolved_qualifier(f, &scope))
                || spec
                    .order_by
                    .iter()
                    .any(|k| expr_has_unresolved_qualifier(&k.expr, &scope))
        })
        || having
            .as_ref()
            .is_some_and(|h| expr_has_unresolved_qualifier(h, &scope))
}

fn expr_has_unresolved_qualifier(expr: &LogicalExpr, scope: &BTreeSet<String>) -> bool {
    match expr {
        LogicalExpr::Column {
            qualifier: Some(q), ..
        } => !scope.contains(&q.to_ascii_lowercase()),
        LogicalExpr::Column {
            qualifier: None, ..
        }
        | LogicalExpr::Literal(_)
        | LogicalExpr::Parameter(_)
        | LogicalExpr::Wildcard { .. }
        | LogicalExpr::Interval { .. } => false,
        LogicalExpr::InSubquery { expr, subquery, .. } => {
            expr_has_unresolved_qualifier(expr, scope)
                || has_unresolved_outer_qualifier(subquery, scope)
        }
        LogicalExpr::Exists { subquery, .. } | LogicalExpr::ScalarSubquery(subquery) => {
            has_unresolved_outer_qualifier(subquery, scope)
        }
        LogicalExpr::Unary { expr, .. } | LogicalExpr::Cast { expr, .. } => {
            expr_has_unresolved_qualifier(expr, scope)
        }
        LogicalExpr::Binary { left, right, .. }
        | LogicalExpr::Compare { left, right, .. }
        | LogicalExpr::IsDistinctFrom { left, right, .. }
        | LogicalExpr::Like {
            expr: left,
            pattern: right,
            ..
        } => {
            expr_has_unresolved_qualifier(left, scope)
                || expr_has_unresolved_qualifier(right, scope)
        }
        LogicalExpr::Between {
            expr, low, high, ..
        } => {
            expr_has_unresolved_qualifier(expr, scope)
                || expr_has_unresolved_qualifier(low, scope)
                || expr_has_unresolved_qualifier(high, scope)
        }
        LogicalExpr::InList { expr, list, .. } => {
            expr_has_unresolved_qualifier(expr, scope)
                || list
                    .iter()
                    .any(|item| expr_has_unresolved_qualifier(item, scope))
        }
        LogicalExpr::Case {
            operand,
            when_then,
            else_expr,
        } => {
            operand
                .as_deref()
                .is_some_and(|op| expr_has_unresolved_qualifier(op, scope))
                || when_then.iter().any(|(w, t)| {
                    expr_has_unresolved_qualifier(w, scope)
                        || expr_has_unresolved_qualifier(t, scope)
                })
                || else_expr
                    .as_deref()
                    .is_some_and(|el| expr_has_unresolved_qualifier(el, scope))
        }
        LogicalExpr::Function {
            args,
            filter,
            order_by,
            over,
            ..
        } => {
            args.iter()
                .any(|arg| expr_has_unresolved_qualifier(arg, scope))
                || filter
                    .as_deref()
                    .is_some_and(|f| expr_has_unresolved_qualifier(f, scope))
                || order_by
                    .iter()
                    .any(|k| expr_has_unresolved_qualifier(&k.expr, scope))
                || over.as_ref().is_some_and(|spec| {
                    spec.partition_by
                        .iter()
                        .any(|p| expr_has_unresolved_qualifier(p, scope))
                        || spec
                            .order_by
                            .iter()
                            .any(|k| expr_has_unresolved_qualifier(&k.expr, scope))
                })
        }
        LogicalExpr::Array(items) | LogicalExpr::Tuple(items) => items
            .iter()
            .any(|item| expr_has_unresolved_qualifier(item, scope)),
    }
}

fn rel_supported(rel: &LogicalRelation, forbidden_sources: &BTreeSet<String>) -> bool {
    match rel {
        LogicalRelation::Source {
            reference,
            quoted_path,
            ..
        } => *quoted_path || !forbidden_sources.contains(&reference.to_ascii_lowercase()),
        LogicalRelation::Subquery { input, .. } => rel_supported(input, forbidden_sources),
        LogicalRelation::Filter { input, predicate } => {
            rel_supported(input, forbidden_sources)
                && expr_supported_with_sources(predicate, forbidden_sources)
        }
        LogicalRelation::Project { input, items, .. } => {
            rel_supported(input, forbidden_sources)
                && items.iter().all(|item| match item {
                    LogicalProjectionItem::Wildcard { .. } => true,
                    LogicalProjectionItem::Expr { expr, .. } => {
                        expr_supported_with_sources(expr, forbidden_sources)
                    }
                })
        }
        LogicalRelation::Aggregate {
            input,
            group_by,
            grouping_sets,
            aggregates,
            having,
        } => aggregate_supported(
            input,
            group_by,
            grouping_sets.is_some(),
            aggregates,
            having.as_ref(),
            forbidden_sources,
        ),
        LogicalRelation::Window { input, windows } => {
            rel_supported(input, forbidden_sources)
                && windows
                    .iter()
                    .all(|(_, spec)| window_spec_supported(spec, forbidden_sources))
        }
        LogicalRelation::Join {
            left,
            right,
            join_type,
            on,
            match_condition,
            tolerance,
        } => {
            match_condition.is_none()
                && tolerance.is_none()
                && join_supported(left, right, *join_type, on.as_ref(), forbidden_sources)
        }
        LogicalRelation::SetOperation {
            left,
            right,
            op,
            distinct,
        } => {
            (*distinct || matches!(op, LogicalSetOp::Union))
                && rel_supported(left, forbidden_sources)
                && rel_supported(right, forbidden_sources)
        }
        LogicalRelation::Sort { input, keys } => {
            rel_supported(input, forbidden_sources)
                && keys
                    .iter()
                    .all(|k| expr_supported_with_sources(&k.expr, forbidden_sources))
        }
        LogicalRelation::Limit {
            input,
            limit,
            offset,
        } => {
            rel_supported(input, forbidden_sources)
                && limit
                    .as_ref()
                    .is_none_or(|expr| expr_supported_with_sources(expr, forbidden_sources))
                && offset
                    .as_ref()
                    .is_none_or(|expr| expr_supported_with_sources(expr, forbidden_sources))
        }
        LogicalRelation::WithCte {
            ctes,
            recursive: false,
            body,
        } => with_cte_supported(ctes, body, forbidden_sources),
        LogicalRelation::Values { .. }
        | LogicalRelation::Unit
        | LogicalRelation::TableFunction { .. }
        | LogicalRelation::WithCte {
            recursive: true, ..
        } => false,
    }
}

fn aggregate_supported(
    input: &LogicalRelation,
    group_by: &[LogicalExpr],
    has_grouping_sets: bool,
    aggregates: &[LogicalAggregateSpec],
    having: Option<&LogicalExpr>,
    forbidden_sources: &BTreeSet<String>,
) -> bool {
    !has_grouping_sets
        && rel_supported(input, forbidden_sources)
        && group_by
            .iter()
            .all(|expr| expr_supported_with_sources(expr, forbidden_sources))
        && aggregates
            .iter()
            .all(|spec| agg_spec_supported(spec, forbidden_sources))
        && having
            .as_ref()
            .is_none_or(|expr| expr_supported_with_sources(expr, forbidden_sources))
}

fn join_supported(
    left: &LogicalRelation,
    right: &LogicalRelation,
    join_type: LogicalJoinType,
    on: Option<&LogicalExpr>,
    forbidden_sources: &BTreeSet<String>,
) -> bool {
    matches!(
        join_type,
        LogicalJoinType::Inner
            | LogicalJoinType::LeftOuter
            | LogicalJoinType::RightOuter
            | LogicalJoinType::FullOuter
            | LogicalJoinType::Cross
            | LogicalJoinType::Semi
            | LogicalJoinType::Anti
    ) && rel_supported(left, forbidden_sources)
        && rel_supported(right, forbidden_sources)
        && on.as_ref().is_none_or(|expr| {
            !expr_contains_complex_constructor(expr)
                && expr_supported_with_sources(expr, forbidden_sources)
        })
}

fn with_cte_supported(
    ctes: &[LogicalCte],
    body: &LogicalRelation,
    forbidden_sources: &BTreeSet<String>,
) -> bool {
    let mut remaining = BTreeSet::new();
    for cte in ctes {
        if !remaining.insert(cte.name.to_ascii_lowercase()) {
            return false;
        }
    }
    let mut inner_forbidden: BTreeSet<String> =
        forbidden_sources.difference(&remaining).cloned().collect();
    inner_forbidden.extend(remaining.iter().cloned());
    for cte in ctes {
        if !rel_supported(&cte.query, &inner_forbidden) {
            return false;
        }
        inner_forbidden.remove(&cte.name.to_ascii_lowercase());
    }
    rel_supported(body, &inner_forbidden)
}

fn window_spec_supported(spec: &LogicalWindowSpec, forbidden_sources: &BTreeSet<String>) -> bool {
    spec.partition_by
        .iter()
        .all(|expr| expr_supported_with_sources(expr, forbidden_sources))
        && spec
            .order_by
            .iter()
            .all(|k| expr_supported_with_sources(&k.expr, forbidden_sources))
        && spec
            .frame
            .as_ref()
            .is_none_or(|frame| window::convert_window_frame(frame).is_ok())
}

fn agg_call_admitted(name: &str, args: &[LogicalExpr], distinct: bool) -> bool {
    let lower_name = name.to_ascii_lowercase();
    let (fn_name, effective_distinct) = match lower_name.as_str() {
        "count_distinct" => ("count", true),
        "group_concat" => ("string_agg", distinct),
        _ => (lower_name.as_str(), distinct),
    };
    let Some(agg_fn) = AggregateFunction::parse(fn_name) else {
        return false;
    };
    if agg_fn.native_name(effective_distinct).is_err() {
        return false;
    }
    let has_wildcard = args
        .iter()
        .any(|arg| matches!(arg, LogicalExpr::Wildcard { .. }));
    if has_wildcard && (effective_distinct || !matches!(agg_fn, AggregateFunction::Count)) {
        return false;
    }
    !args.is_empty() && args.len() <= 2
}

fn agg_spec_supported(spec: &LogicalAggregateSpec, forbidden_sources: &BTreeSet<String>) -> bool {
    agg_call_admitted(&spec.name, &spec.args, spec.distinct)
        && spec
            .args
            .iter()
            .all(|expr| expr_supported_with_sources(expr, forbidden_sources))
        && spec
            .filter
            .as_ref()
            .is_none_or(|expr| expr_supported_with_sources(expr, forbidden_sources))
        && spec
            .order_by
            .iter()
            .all(|k| expr_supported_with_sources(&k.expr, forbidden_sources))
}

fn expr_contains_complex_constructor(expr: &LogicalExpr) -> bool {
    match expr {
        LogicalExpr::Array(_) | LogicalExpr::Tuple(_) => true,
        LogicalExpr::Unary { expr, .. } | LogicalExpr::Cast { expr, .. } => {
            expr_contains_complex_constructor(expr)
        }
        LogicalExpr::Binary { left, right, .. }
        | LogicalExpr::Compare { left, right, .. }
        | LogicalExpr::IsDistinctFrom { left, right, .. }
        | LogicalExpr::Like {
            expr: left,
            pattern: right,
            ..
        } => expr_contains_complex_constructor(left) || expr_contains_complex_constructor(right),
        LogicalExpr::Between {
            expr, low, high, ..
        } => {
            expr_contains_complex_constructor(expr)
                || expr_contains_complex_constructor(low)
                || expr_contains_complex_constructor(high)
        }
        LogicalExpr::InList { expr, list, .. } => {
            expr_contains_complex_constructor(expr)
                || list.iter().any(expr_contains_complex_constructor)
        }
        _ => false,
    }
}

fn in_subquery_projection_admitted(subquery: &LogicalRelation) -> bool {
    let mut cur = subquery;
    loop {
        match cur {
            LogicalRelation::Subquery { input, .. }
            | LogicalRelation::Sort { input, .. }
            | LogicalRelation::Limit { input, .. } => cur = input,
            LogicalRelation::WithCte { body, .. } => cur = body,
            LogicalRelation::Project { items, .. } => {
                if items.len() != 1 {
                    return false;
                }
                return match &items[0] {
                    LogicalProjectionItem::Wildcard { .. } => false,
                    LogicalProjectionItem::Expr { expr, .. } => {
                        !matches!(expr.as_ref(), LogicalExpr::Array(_) | LogicalExpr::Tuple(_))
                    }
                };
            }
            _ => return true,
        }
    }
}

fn canonical_scalar_fn_admitted(name: &str) -> bool {
    let lower_name = name.to_ascii_lowercase();
    let mapped = match lower_name.as_str() {
        "substring" | "substr" => "substr",
        "char_length" | "character_length" => "length",
        "lcase" => "lower",
        "ucase" => "upper",
        other => other,
    };
    shardloom_core::expression::builtins::scalar(mapped).is_some()
}

pub(super) fn expr_supported_with_sources(
    expr: &LogicalExpr,
    forbidden_sources: &BTreeSet<String>,
) -> bool {
    match expr {
        LogicalExpr::Literal(_)
        | LogicalExpr::Column { .. }
        | LogicalExpr::Parameter(_)
        | LogicalExpr::Wildcard { .. } => true,
        LogicalExpr::Unary { expr, .. } => expr_supported_with_sources(expr, forbidden_sources),
        LogicalExpr::Cast {
            expr, target_dtype, ..
        } => {
            parse_core_dtype_label(target_dtype).is_ok()
                && expr_supported_with_sources(expr, forbidden_sources)
        }
        LogicalExpr::Binary { left, right, .. }
        | LogicalExpr::Compare { left, right, .. }
        | LogicalExpr::IsDistinctFrom { left, right, .. } => {
            expr_supported_with_sources(left, forbidden_sources)
                && expr_supported_with_sources(right, forbidden_sources)
        }
        LogicalExpr::Like {
            expr,
            pattern,
            case_insensitive,
            ..
        } => {
            !*case_insensitive
                && expr_supported_with_sources(expr, forbidden_sources)
                && expr_supported_with_sources(pattern, forbidden_sources)
        }
        LogicalExpr::Between {
            expr, low, high, ..
        } => {
            expr_supported_with_sources(expr, forbidden_sources)
                && expr_supported_with_sources(low, forbidden_sources)
                && expr_supported_with_sources(high, forbidden_sources)
        }
        LogicalExpr::InList { expr, list, .. } => {
            expr_supported_with_sources(expr, forbidden_sources)
                && list
                    .iter()
                    .all(|item| expr_supported_with_sources(item, forbidden_sources))
        }
        LogicalExpr::InSubquery { expr, subquery, .. } => {
            !matches!(expr.as_ref(), LogicalExpr::Tuple(_))
                && in_subquery_projection_admitted(subquery)
                && expr_supported_with_sources(expr, forbidden_sources)
                && rel_supported(subquery, forbidden_sources)
        }
        LogicalExpr::Exists { subquery, .. } | LogicalExpr::ScalarSubquery(subquery) => {
            rel_supported(subquery, forbidden_sources)
        }
        LogicalExpr::Case {
            operand,
            when_then,
            else_expr,
        } => case_expr_supported(
            operand.as_deref(),
            when_then,
            else_expr.as_deref(),
            forbidden_sources,
        ),
        LogicalExpr::Function {
            name,
            args,
            distinct,
            filter,
            order_by,
            over: Some(spec),
        } => window::window_call_admitted(
            name,
            args,
            *distinct,
            filter.as_deref(),
            order_by,
            spec,
            forbidden_sources,
        ),
        LogicalExpr::Function {
            name,
            args,
            distinct,
            filter,
            order_by,
            over: None,
        } => function_expr_supported(
            name,
            args,
            *distinct,
            filter.as_deref(),
            order_by,
            forbidden_sources,
        ),
        LogicalExpr::Array(items) => items
            .iter()
            .all(|item| expr_supported_with_sources(item, forbidden_sources)),
        LogicalExpr::Interval { .. } | LogicalExpr::Tuple(_) => false,
    }
}

fn case_expr_supported(
    operand: Option<&LogicalExpr>,
    when_then: &[(LogicalExpr, LogicalExpr)],
    else_expr: Option<&LogicalExpr>,
    forbidden_sources: &BTreeSet<String>,
) -> bool {
    operand.is_none_or(|op| expr_supported_with_sources(op, forbidden_sources))
        && when_then.iter().all(|(w, t)| {
            expr_supported_with_sources(w, forbidden_sources)
                && expr_supported_with_sources(t, forbidden_sources)
        })
        && else_expr.is_none_or(|el| expr_supported_with_sources(el, forbidden_sources))
}

fn function_expr_supported(
    name: &str,
    args: &[LogicalExpr],
    distinct: bool,
    filter: Option<&LogicalExpr>,
    order_by: &[LogicalSortKey],
    forbidden_sources: &BTreeSet<String>,
) -> bool {
    let admitted = if is_agg_name(name) {
        agg_call_admitted(name, args, distinct)
    } else {
        !distinct && filter.is_none() && order_by.is_empty() && canonical_scalar_fn_admitted(name)
    };
    admitted
        && args
            .iter()
            .all(|arg| expr_supported_with_sources(arg, forbidden_sources))
        && filter.is_none_or(|f| expr_supported_with_sources(f, forbidden_sources))
        && order_by
            .iter()
            .all(|k| expr_supported_with_sources(&k.expr, forbidden_sources))
}

/// Inspects source declarations, bound input usage, nesting depth, and metadata size.
pub(super) fn inspect_canonical_plan(
    bound: &BoundSqlPlan,
    bound_names: &[String],
) -> NativeResult<CanonicalPlanSummary> {
    let mut bound_lookup = BTreeMap::new();
    for (index, name) in bound_names.iter().enumerate() {
        validate_sql_identifier(name)?;
        if bound_lookup
            .insert(name.to_ascii_lowercase(), index)
            .is_some()
        {
            return Err(unsupported_sql_error(
                "relation bindings must be distinct ignoring case",
            ));
        }
    }
    let mut state = InspectState {
        bound_lookup,
        sources: BTreeMap::new(),
        input_uses: BTreeMap::new(),
        max_depth: 0,
    };
    let ctes = BTreeSet::new();
    inspect_relation(&bound.root, &ctes, 0, &mut state)?;
    if state.max_depth > 24 {
        return Err(unsupported_sql_error(
            "recursive SQL lowering exceeds 24 levels",
        ));
    }
    let metadata_bytes = bound
        .normalized_sql
        .len()
        .checked_mul(1024)
        .and_then(|bytes| bytes.checked_add(65_536))
        .ok_or_else(|| unsupported_sql_error("SQL declaration metadata size overflow"))?;
    Ok(CanonicalPlanSummary {
        metadata_bytes,
        sources: state.sources,
        input_uses: state.input_uses,
        depth: state.max_depth,
    })
}

/// Extracts all unique declared source leaves from a canonical plan.
pub(super) fn extract_source_leaves(
    bound: &BoundSqlPlan,
) -> NativeResult<BTreeSet<ParsedRelationLeaf>> {
    Ok(inspect_canonical_plan(bound, &[])?
        .sources
        .into_keys()
        .collect())
}

struct InspectState {
    bound_lookup: BTreeMap<String, usize>,
    sources: BTreeMap<ParsedRelationLeaf, u8>,
    input_uses: BTreeMap<usize, usize>,
    max_depth: usize,
}

fn inspect_relation(
    rel: &LogicalRelation,
    ctes: &BTreeSet<String>,
    depth: usize,
    state: &mut InspectState,
) -> NativeResult<()> {
    state.max_depth = state.max_depth.max(depth);
    match rel {
        LogicalRelation::Source {
            reference,
            quoted_path,
            ..
        } => {
            if !quoted_path && ctes.contains(&reference.to_ascii_lowercase()) {
                return Ok(());
            }
            if !quoted_path
                && let Some(&index) = state.bound_lookup.get(&reference.to_ascii_lowercase())
            {
                *state.input_uses.entry(index).or_insert(0) += 1;
                return Ok(());
            }
            let leaf = make_relation_leaf(reference, *quoted_path)?;
            let count = state.sources.entry(leaf).or_insert(0);
            *count = (*count + 1).min(2);
            Ok(())
        }
        LogicalRelation::Subquery { input, .. } => inspect_relation(input, ctes, depth + 1, state),
        LogicalRelation::Filter { input, predicate } => {
            inspect_relation(input, ctes, depth, state)?;
            subquery::inspect_expr_sources(predicate, ctes, depth, state)
        }
        LogicalRelation::Project { input, items, .. } => {
            inspect_relation(input, ctes, depth, state)?;
            for item in items {
                if let LogicalProjectionItem::Expr { expr, .. } = item {
                    subquery::inspect_expr_sources(expr, ctes, depth, state)?;
                }
            }
            Ok(())
        }
        LogicalRelation::Aggregate {
            input,
            group_by,
            aggregates,
            having,
            ..
        } => inspect_aggregate_sources(
            input,
            group_by,
            aggregates,
            having.as_ref(),
            ctes,
            depth,
            state,
        ),
        LogicalRelation::Window { input, windows } => {
            inspect_relation(input, ctes, depth, state)?;
            for (_, spec) in windows {
                for p in &spec.partition_by {
                    subquery::inspect_expr_sources(p, ctes, depth, state)?;
                }
                for k in &spec.order_by {
                    subquery::inspect_expr_sources(&k.expr, ctes, depth, state)?;
                }
            }
            Ok(())
        }
        LogicalRelation::Sort { input, keys } => {
            inspect_relation(input, ctes, depth, state)?;
            for k in keys {
                subquery::inspect_expr_sources(&k.expr, ctes, depth, state)?;
            }
            Ok(())
        }
        LogicalRelation::Limit { input, .. } => inspect_relation(input, ctes, depth, state),
        LogicalRelation::Join {
            left, right, on, ..
        } => {
            inspect_relation(left, ctes, depth, state)?;
            inspect_relation(right, ctes, depth, state)?;
            if let Some(cond) = on {
                subquery::inspect_expr_sources(cond, ctes, depth, state)?;
            }
            Ok(())
        }
        LogicalRelation::SetOperation { left, right, .. } => {
            inspect_relation(left, ctes, depth + 1, state)?;
            inspect_relation(right, ctes, depth + 1, state)
        }
        LogicalRelation::WithCte {
            ctes: defs, body, ..
        } => {
            let mut inner_ctes = ctes.clone();
            for cte in defs {
                inspect_relation(&cte.query, &inner_ctes, depth + 1, state)?;
                inner_ctes.insert(cte.name.to_ascii_lowercase());
            }
            inspect_relation(body, &inner_ctes, depth, state)
        }
        _ => Err(unsupported_sql_error(
            "relation node is not admitted in canonical native lowering",
        )),
    }
}

fn inspect_aggregate_sources(
    input: &LogicalRelation,
    group_by: &[LogicalExpr],
    aggregates: &[LogicalAggregateSpec],
    having: Option<&LogicalExpr>,
    ctes: &BTreeSet<String>,
    depth: usize,
    state: &mut InspectState,
) -> NativeResult<()> {
    inspect_relation(input, ctes, depth, state)?;
    for g in group_by {
        subquery::inspect_expr_sources(g, ctes, depth, state)?;
    }
    for agg in aggregates {
        for a in &agg.args {
            subquery::inspect_expr_sources(a, ctes, depth, state)?;
        }
        if let Some(f) = &agg.filter {
            subquery::inspect_expr_sources(f, ctes, depth, state)?;
        }
        for k in &agg.order_by {
            subquery::inspect_expr_sources(&k.expr, ctes, depth, state)?;
        }
    }
    if let Some(h) = having {
        subquery::inspect_expr_sources(h, ctes, depth, state)?;
    }
    Ok(())
}

fn make_relation_leaf(reference: &str, quoted_path: bool) -> NativeResult<ParsedRelationLeaf> {
    if reference.trim_start().starts_with(':') {
        return Err(unsupported_sql_error(
            "scalar parameters cannot name a relation; use an explicit relation binding",
        ));
    }
    Ok(ParsedRelationLeaf {
        path: PathBuf::from(reference),
        declared_identifier: !quoted_path && validate_sql_identifier(reference).is_ok(),
        memory_input: None,
    })
}

/// Lowers a canonical `BoundSqlPlan` into the native `Lowered` relational plan.
pub(super) fn lower_bound_sql_plan(
    bound: &BoundSqlPlan,
    bound_names: &[String],
    lowerer: &mut Lowerer<'_, '_>,
) -> NativeResult<Lowered> {
    let mut ctx = CanonicalLowering {
        lowerer,
        bound_lookup: bound_names
            .iter()
            .enumerate()
            .map(|(idx, name)| (name.to_ascii_lowercase(), idx))
            .collect(),
        ctes: BTreeMap::new(),
        serial: 0,
    };
    let scoped = ctx.lower_relation(&bound.root)?;
    let mut result = scoped.into_lowered();
    result.plan = result.plan.compact_streaming();
    Ok(result)
}

struct CanonicalLowering<'a, 'b, 'session> {
    lowerer: &'a mut Lowerer<'b, 'session>,
    bound_lookup: BTreeMap<String, usize>,
    ctes: BTreeMap<String, ScopedRelation>,
    serial: usize,
}

impl CanonicalLowering<'_, '_, '_> {
    fn next_id(&mut self, prefix: &str) -> NativeResult<ExprId> {
        let id = format!("{prefix}.{}", self.serial);
        self.serial += 1;
        ExprId::new(id)
    }

    fn lower_relation(&mut self, rel: &LogicalRelation) -> NativeResult<ScopedRelation> {
        self.lowerer.cancellation.check()?;
        match rel {
            LogicalRelation::WithCte {
                ctes,
                recursive: false,
                body,
            } => self.lower_with_cte(ctes, body),
            LogicalRelation::Source {
                reference,
                quoted_path,
                alias,
            } => self.lower_source(reference, *quoted_path, alias.as_deref()),
            LogicalRelation::Subquery { input, alias } => {
                let lowered = self.lower_relation(input)?;
                Ok(lowered.with_qualifier(alias.as_deref()))
            }
            LogicalRelation::Filter { input, predicate } => {
                subquery::lower_filter_with_subqueries(self, input, predicate)
            }
            LogicalRelation::Join {
                left,
                right,
                join_type,
                on,
                ..
            } => self.lower_join(left, right, *join_type, on.as_ref()),
            LogicalRelation::Window { input, windows } => self.lower_bare_window(input, windows),
            LogicalRelation::SetOperation {
                left,
                right,
                op,
                distinct,
            } => self.lower_set_operation(left, right, *op, *distinct),
            LogicalRelation::Project {
                input,
                items,
                distinct,
            } => self.lower_project(input, items, *distinct, None),
            LogicalRelation::Aggregate {
                input,
                group_by,
                aggregates,
                having,
                ..
            } => self.lower_aggregate_only(input, group_by, aggregates, having.as_ref()),
            LogicalRelation::Sort { input, keys } => self.lower_sort(input, keys),
            LogicalRelation::Limit {
                input,
                limit,
                offset,
            } => self.lower_limit(input, limit.as_ref(), offset.as_ref()),
            _ => Err(unsupported_sql_error(
                "relation node is not admitted in canonical native lowering",
            )),
        }
    }

    fn lower_set_operation(
        &mut self,
        left: &LogicalRelation,
        right: &LogicalRelation,
        op: LogicalSetOp,
        distinct: bool,
    ) -> NativeResult<ScopedRelation> {
        let mut left_rel = self.lower_relation(left)?;
        let right_rel = self.lower_relation(right)?;
        if left_rel.entries.len() != right_rel.entries.len() {
            return Err(unsupported_sql_error(&format!(
                "set operation expects matching branch widths, found {} and {}",
                left_rel.entries.len(),
                right_rel.entries.len()
            )));
        }
        let kind = match (op, distinct) {
            (LogicalSetOp::Union, false) => SetKind::UnionAll,
            (LogicalSetOp::Union, true) => SetKind::UnionDistinct,
            (LogicalSetOp::Intersect, true) => SetKind::Intersect,
            (LogicalSetOp::Except, true) => SetKind::Except,
            (LogicalSetOp::Intersect | LogicalSetOp::Except, false) => {
                return Err(unsupported_sql_error(
                    "bag INTERSECT ALL / EXCEPT ALL are not admitted natively",
                ));
            }
        };
        if left_rel
            .entries
            .iter()
            .any(|entry| entry.internal != entry.output)
        {
            let norm = left_rel
                .entries
                .iter()
                .map(|entry| Ok((entry.output.clone(), column(&entry.internal)?)))
                .collect::<NativeResult<Vec<_>>>()?;
            left_rel.plan = Plan::Project(Box::new(Project {
                input: left_rel.plan,
                expressions: norm,
            }));
        }
        let entries = left_rel
            .entries
            .into_iter()
            .map(|entry| ScopeColumn {
                internal: entry.output.clone(),
                qualifier: None,
                output: entry.output,
            })
            .collect();
        Ok(ScopedRelation {
            plan: Plan::Set(Box::new(Set {
                left: left_rel.plan,
                right: right_rel.plan,
                kind,
            })),
            entries,
        })
    }

    fn lower_with_cte(
        &mut self,
        ctes: &[LogicalCte],
        body: &LogicalRelation,
    ) -> NativeResult<ScopedRelation> {
        let saved = self.ctes.clone();
        for (idx, cte) in ctes.iter().enumerate() {
            let mut cte_rel = self.lower_relation(&cte.query)?;
            if !cte.columns.is_empty() {
                if cte.columns.len() != cte_rel.entries.len() {
                    self.ctes = saved;
                    return Err(unsupported_sql_error(&format!(
                        "CTE {:?} column list has {} names for {} output columns",
                        cte.name,
                        cte.columns.len(),
                        cte_rel.entries.len()
                    )));
                }
                let mut expressions = Vec::with_capacity(cte.columns.len());
                let mut renamed = Vec::with_capacity(cte.columns.len());
                for (entry, alias) in cte_rel.entries.iter().zip(&cte.columns) {
                    expressions.push((alias.clone(), column(&entry.internal)?));
                    renamed.push(ScopeColumn {
                        internal: alias.clone(),
                        qualifier: None,
                        output: alias.clone(),
                    });
                }
                cte_rel = ScopedRelation {
                    plan: Plan::Project(Box::new(Project {
                        input: cte_rel.plan,
                        expressions,
                    })),
                    entries: renamed,
                };
            }
            let cte_key = cte.name.to_ascii_lowercase();
            let mut ref_counts = BTreeMap::new();
            let mut shadowed = BTreeSet::new();
            for later in &ctes[idx + 1..] {
                count_rel_cte_refs(&later.query, &shadowed, &mut ref_counts);
                shadowed.insert(later.name.to_ascii_lowercase());
            }
            count_rel_cte_refs(body, &shadowed, &mut ref_counts);
            let refs = ref_counts.get(&cte_key).copied().unwrap_or(0);
            if cte.materialized == Some(true) || (cte.materialized != Some(false) && refs >= 2) {
                cte_rel.plan = Plan::Materialized(Arc::new(cte_rel.plan));
            }
            self.ctes.insert(cte_key, cte_rel);
        }
        let result = self.lower_relation(body);
        self.ctes = saved;
        result
    }

    fn lower_source(
        &mut self,
        reference: &str,
        quoted_path: bool,
        alias: Option<&str>,
    ) -> NativeResult<ScopedRelation> {
        let qualifier = alias
            .map(str::to_owned)
            .or_else(|| (!quoted_path).then(|| reference.to_string()));
        if !quoted_path && let Some(cte) = self.ctes.get(&reference.to_ascii_lowercase()).cloned() {
            return Ok(cte.with_qualifier(qualifier.as_deref()));
        }
        if !quoted_path && let Some(&index) = self.bound_lookup.get(&reference.to_ascii_lowercase())
        {
            let lowered = self.lowerer.bound_input(index)?;
            return Ok(ScopedRelation::from_columns(
                lowered.plan,
                lowered.columns,
                qualifier.as_deref(),
            ));
        }
        let leaf = make_relation_leaf(reference, quoted_path)?;
        let lowered = self.lowerer.scan(&leaf)?;
        Ok(ScopedRelation::from_columns(
            lowered.plan,
            lowered.columns,
            qualifier.as_deref(),
        ))
    }

    fn lower_join(
        &mut self,
        left: &LogicalRelation,
        right: &LogicalRelation,
        join_type: LogicalJoinType,
        on: Option<&LogicalExpr>,
    ) -> NativeResult<ScopedRelation> {
        let left_rel = self.lower_relation(left)?;
        let right_rel = self.lower_relation(right)?;
        let kind = match join_type {
            LogicalJoinType::Inner => JoinKind::Inner,
            LogicalJoinType::LeftOuter => JoinKind::Left,
            LogicalJoinType::RightOuter => JoinKind::Right,
            LogicalJoinType::FullOuter => JoinKind::Full,
            LogicalJoinType::Cross => JoinKind::Cross,
            LogicalJoinType::Semi => JoinKind::LeftSemi,
            LogicalJoinType::Anti => JoinKind::LeftAnti,
            LogicalJoinType::AsOfInner | LogicalJoinType::AsOfLeft => {
                return Err(unsupported_sql_error(
                    "ASOF joins continue through the specialized ASOF lowering path",
                ));
            }
        };
        let mut used = BTreeSet::new();
        let mut columns = Vec::with_capacity(left_rel.entries.len() + right_rel.entries.len());
        let mut entries = Vec::with_capacity(columns.capacity());
        push_join_side(
            Side::Left,
            &left_rel.entries,
            &mut used,
            &mut columns,
            &mut entries,
        )?;
        if !matches!(kind, JoinKind::LeftSemi | JoinKind::LeftAnti) {
            push_join_side(
                Side::Right,
                &right_rel.entries,
                &mut used,
                &mut columns,
                &mut entries,
            )?;
        }
        let mut keys = Vec::new();
        let condition = match on {
            Some(expr) => {
                let lowered_on = self.lower_join_on_expr(expr, &left_rel, &right_rel)?;
                append_equality_keys(&lowered_on, &mut keys)?;
                if !keys.is_empty() && is_pure_equi_conjunction(&lowered_on) {
                    None
                } else {
                    Some(lowered_on)
                }
            }
            None => None,
        };
        if !matches!(kind, JoinKind::Cross) && keys.is_empty() && condition.is_none() {
            return Err(unsupported_sql_error("JOIN requires an ON condition"));
        }
        Ok(ScopedRelation {
            plan: Plan::Join(Box::new(Join {
                left: left_rel.plan,
                right: right_rel.plan,
                kind,
                keys,
                condition,
                columns,
            })),
            entries,
        })
    }

    fn lower_sort(
        &mut self,
        input: &LogicalRelation,
        keys: &[LogicalSortKey],
    ) -> NativeResult<ScopedRelation> {
        if let LogicalRelation::Project {
            input: proj_input,
            items,
            distinct,
        } = input
        {
            return self.lower_project(proj_input, items, *distinct, Some(keys));
        }
        let mut rel = self.lower_relation(input)?;
        let saved_entries = rel.entries.clone();
        let mut owned_keys = keys.to_vec();
        for key in &mut owned_keys {
            rel = subquery::materialize_expr_subqueries(self, rel, &mut key.expr)?;
        }
        let mut extra_exprs = Vec::new();
        let mut order_keys = Vec::with_capacity(owned_keys.len());
        for key in &owned_keys {
            if let LogicalExpr::Column { qualifier, name } = &key.expr {
                let resolved = rel.resolve(qualifier.as_deref(), name)?;
                order_keys.push(make_order_key(&resolved, key)?);
            } else {
                let hidden = fresh_col(&rel.entries, &mut self.lowerer.serial);
                let lowered = self.lower_expr(&key.expr, &rel)?;
                extra_exprs.push((hidden.clone(), lowered));
                order_keys.push(make_order_key(&hidden, key)?);
            }
        }
        let added_extra = !extra_exprs.is_empty() || rel.entries.len() != saved_entries.len();
        if !extra_exprs.is_empty() {
            let mut exprs = rel
                .entries
                .iter()
                .map(|e| Ok((e.internal.clone(), column(&e.internal)?)))
                .collect::<NativeResult<Vec<_>>>()?;
            exprs.extend(extra_exprs);
            rel.plan = Plan::Project(Box::new(Project {
                input: rel.plan,
                expressions: exprs,
            }));
        }
        rel.plan = Plan::Sort(Box::new(Sort {
            input: rel.plan,
            keys: order_keys,
        }));
        if added_extra {
            rel.entries = saved_entries;
            let restore = rel
                .entries
                .iter()
                .map(|e| Ok((e.internal.clone(), column(&e.internal)?)))
                .collect::<NativeResult<Vec<_>>>()?;
            rel.plan = Plan::Project(Box::new(Project {
                input: rel.plan,
                expressions: restore,
            }));
        }
        Ok(rel)
    }

    fn lower_limit(
        &mut self,
        input: &LogicalRelation,
        limit: Option<&LogicalExpr>,
        offset: Option<&LogicalExpr>,
    ) -> NativeResult<ScopedRelation> {
        let mut rel = self.lower_relation(input)?;
        let offset_val = offset
            .map(|expr| self.eval_non_negative_usize(expr, "OFFSET"))
            .transpose()?
            .unwrap_or(0);
        let Some(limit_expr) = limit else {
            if offset_val > 0 {
                return Err(unsupported_sql_error(
                    "native SQL OFFSET requires an outer LIMIT",
                ));
            }
            return Ok(rel);
        };
        let count = self.eval_non_negative_usize(limit_expr, "LIMIT")?;
        rel.plan = Plan::Limit(Box::new(Limit {
            input: rel.plan,
            offset: offset_val,
            count,
        }));
        Ok(rel)
    }

    fn eval_non_negative_usize(&self, expr: &LogicalExpr, clause: &str) -> NativeResult<usize> {
        let literal = match expr {
            LogicalExpr::Literal(v) => v.clone(),
            LogicalExpr::Parameter(name) => match self.lowerer.parameters.get(name) {
                Some(param) => match &param.kind {
                    ExpressionKind::Literal(v) => v.clone(),
                    _ => {
                        return Err(unsupported_sql_error(&format!(
                            "{clause} parameter :{name} must be an integer literal"
                        )));
                    }
                },
                None => {
                    return Err(unsupported_sql_error(&format!(
                        "unbound SQL parameter :{name}"
                    )));
                }
            },
            _ => {
                return Err(unsupported_sql_error(&format!(
                    "{clause} admits a single non-negative integer literal only"
                )));
            }
        };
        match literal {
            ScalarValue::Int64(v) => usize::try_from(v).map_err(|_| {
                unsupported_sql_error(&format!(
                    "{clause} admits a single non-negative integer literal only"
                ))
            }),
            ScalarValue::UInt64(v) => usize::try_from(v).map_err(|_| {
                unsupported_sql_error(&format!("{clause} value exceeds supported platform range"))
            }),
            _ => Err(unsupported_sql_error(&format!(
                "{clause} admits a single non-negative integer literal only"
            ))),
        }
    }

    fn lower_project(
        &mut self,
        input: &LogicalRelation,
        items: &[LogicalProjectionItem],
        distinct: bool,
        outer_sort: Option<&[LogicalSortKey]>,
    ) -> NativeResult<ScopedRelation> {
        match input {
            LogicalRelation::Aggregate {
                input: agg_input,
                group_by,
                aggregates,
                having,
                ..
            } => self.lower_aggregate_project(
                agg_input,
                group_by,
                aggregates,
                having.as_ref(),
                items,
                distinct,
                outer_sort,
            ),
            LogicalRelation::Window { input: win_in, .. } => {
                if let LogicalRelation::Aggregate {
                    input: agg_input,
                    group_by,
                    having,
                    ..
                } = win_in.as_ref()
                {
                    return self.lower_window_aggregate_project(
                        agg_input,
                        group_by,
                        having.as_ref(),
                        items,
                        distinct,
                        outer_sort,
                    );
                }
                let rel = self.lower_relation(win_in)?;
                self.lower_window_project_over_scope(rel, items, distinct, outer_sort)
            }
            other => {
                let rel = self.lower_relation(other)?;
                self.lower_window_project_over_scope(rel, items, distinct, outer_sort)
            }
        }
    }

    fn count_wildcard_duplicate_outputs(
        items: &[LogicalProjectionItem],
        wildcard_entries: &[ScopeColumn],
    ) -> BTreeMap<String, usize> {
        let mut duplicate_outputs = BTreeMap::<String, usize>::new();
        for item in items {
            if let LogicalProjectionItem::Wildcard { qualifier } = item {
                for entry in wildcard_entries {
                    if qualifier
                        .as_deref()
                        .is_none_or(|q| entry.qualifier.as_deref() == Some(q))
                    {
                        *duplicate_outputs.entry(entry.output.clone()).or_insert(0) += 1;
                    }
                }
            }
        }
        duplicate_outputs
    }

    fn expand_projection_items(
        &mut self,
        items: &[LogicalProjectionItem],
        wildcard_entries: &[ScopeColumn],
        scope: &ScopedRelation,
    ) -> NativeResult<ExpandedProjection> {
        let mut expanded = Vec::new();
        let mut expressions = Vec::new();
        let mut visible = Vec::new();
        let duplicate_outputs = Self::count_wildcard_duplicate_outputs(items, wildcard_entries);
        let mut used_names = BTreeSet::new();
        for (idx, item) in items.iter().enumerate() {
            match item {
                LogicalProjectionItem::Wildcard { qualifier } => {
                    let mut matched = 0usize;
                    for entry in wildcard_entries {
                        if qualifier
                            .as_deref()
                            .is_some_and(|q| entry.qualifier.as_deref() != Some(q))
                        {
                            continue;
                        }
                        matched += 1;
                        let out_name = if qualifier.is_none()
                            && duplicate_outputs.get(&entry.output).copied().unwrap_or(0) > 1
                        {
                            entry.internal.clone()
                        } else {
                            entry.output.clone()
                        };
                        if !used_names.insert(out_name.clone()) {
                            return Err(unsupported_sql_error(&format!(
                                "duplicate projected column name {out_name:?}; assign distinct aliases"
                            )));
                        }
                        let expr = LogicalExpr::Column {
                            qualifier: entry.qualifier.clone(),
                            name: entry.output.clone(),
                        };
                        expanded.push(LogicalProjectionItem::Expr {
                            expr: Box::new(expr),
                            alias: Some(out_name.clone()),
                        });
                        expressions.push((out_name.clone(), column(&entry.internal)?));
                        visible.push(ScopeColumn {
                            internal: out_name.clone(),
                            qualifier: entry.qualifier.clone(),
                            output: out_name,
                        });
                    }
                    if let Some(q) = qualifier
                        && matched == 0
                    {
                        return Err(unsupported_sql_error(&format!(
                            "wildcard qualifier {q:?} matches no columns"
                        )));
                    }
                }
                LogicalProjectionItem::Expr { expr, alias } => {
                    let out_name = match alias {
                        Some(a) => a.clone(),
                        None => match expr.as_ref() {
                            LogicalExpr::Column { name, .. } => name.clone(),
                            _ => format!("col_{idx}"),
                        },
                    };
                    if !used_names.insert(out_name.clone()) {
                        return Err(unsupported_sql_error(&format!(
                            "duplicate projected column name {out_name:?}; assign distinct aliases"
                        )));
                    }
                    let lowered = self.lower_expr(expr, scope)?;
                    expanded.push(LogicalProjectionItem::Expr {
                        expr: expr.clone(),
                        alias: Some(out_name.clone()),
                    });
                    expressions.push((out_name.clone(), lowered));
                    visible.push(ScopeColumn {
                        internal: out_name.clone(),
                        qualifier: None,
                        output: out_name,
                    });
                }
            }
        }
        Ok((expanded, expressions, visible))
    }

    fn finish_projection(
        &mut self,
        source_scope: ScopedRelation,
        expanded_items: &[LogicalProjectionItem],
        mut expressions: Vec<(String, Expression)>,
        visible_entries: Vec<ScopeColumn>,
        distinct: bool,
        outer_sort: Option<&[LogicalSortKey]>,
    ) -> NativeResult<ScopedRelation> {
        let mut order_keys = Vec::new();
        let visible_names: Vec<String> = visible_entries
            .iter()
            .map(|entry| entry.output.clone())
            .collect();
        let mut projected_names = visible_names.clone();
        if let Some(keys) = outer_sort {
            for key in keys {
                let target_col = self.resolve_sort_key_column(
                    &key.expr,
                    expanded_items,
                    &visible_names,
                    &source_scope,
                    distinct,
                    &mut projected_names,
                    &mut expressions,
                )?;
                order_keys.push(make_order_key(&target_col, key)?);
            }
        }
        let mut plan = Plan::Project(Box::new(Project {
            input: source_scope.plan,
            expressions,
        }));
        if distinct {
            let group_by = projected_names
                .iter()
                .map(|name| ColumnRef::new(name.clone()))
                .collect::<NativeResult<Vec<_>>>()?;
            plan = Plan::Aggregate(Box::new(Aggregate {
                grouping_sets: None,
                input: plan,
                group_by,
                measures: Vec::new(),
            }));
        }
        if !order_keys.is_empty() {
            plan = Plan::Sort(Box::new(Sort {
                input: plan,
                keys: order_keys,
            }));
        }
        if projected_names != visible_names {
            let trim = visible_names
                .into_iter()
                .map(|name| Ok((name.clone(), column(&name)?)))
                .collect::<NativeResult<Vec<_>>>()?;
            plan = Plan::Project(Box::new(Project {
                input: plan,
                expressions: trim,
            }));
        }
        Ok(ScopedRelation {
            plan,
            entries: visible_entries,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn resolve_sort_key_column(
        &mut self,
        expr: &LogicalExpr,
        expanded_items: &[LogicalProjectionItem],
        visible_names: &[String],
        source_scope: &ScopedRelation,
        distinct: bool,
        projected_names: &mut Vec<String>,
        expressions: &mut Vec<(String, Expression)>,
    ) -> NativeResult<String> {
        if let LogicalExpr::Column {
            qualifier: None,
            name,
        } = expr
            && visible_names.contains(name)
        {
            return Ok(name.clone());
        }
        for item in expanded_items {
            if let LogicalProjectionItem::Expr {
                expr: item_expr,
                alias: Some(alias),
            } = item
                && item_expr.as_ref() == expr
            {
                return Ok(alias.clone());
            }
        }
        if distinct {
            return Err(unsupported_sql_error(
                "SELECT DISTINCT requires ORDER BY columns to be projected",
            ));
        }
        let lowered = self.lower_expr(expr, source_scope)?;
        let hidden = fresh_col_names(projected_names, &mut self.lowerer.serial);
        projected_names.push(hidden.clone());
        expressions.push((hidden.clone(), lowered));
        Ok(hidden)
    }

    fn lower_aggregate_only(
        &mut self,
        agg_input: &LogicalRelation,
        group_by: &[LogicalExpr],
        aggregates: &[LogicalAggregateSpec],
        having: Option<&LogicalExpr>,
    ) -> NativeResult<ScopedRelation> {
        let mut items = Vec::with_capacity(group_by.len() + aggregates.len());
        for (idx, expr) in group_by.iter().enumerate() {
            items.push(LogicalProjectionItem::Expr {
                expr: Box::new(expr.clone()),
                alias: Some(format!("group_{idx}")),
            });
        }
        for (idx, spec) in aggregates.iter().enumerate() {
            let expr = LogicalExpr::Function {
                name: spec.name.clone(),
                args: spec.args.clone(),
                distinct: spec.distinct,
                filter: spec.filter.clone().map(Box::new),
                order_by: spec.order_by.clone(),
                over: None,
            };
            items.push(LogicalProjectionItem::Expr {
                expr: Box::new(expr),
                alias: spec.alias.clone().or_else(|| Some(format!("agg_{idx}"))),
            });
        }
        self.lower_aggregate_project(agg_input, group_by, aggregates, having, &items, false, None)
    }

    #[allow(clippy::too_many_arguments)]
    fn lower_aggregate_project(
        &mut self,
        agg_input: &LogicalRelation,
        group_by: &[LogicalExpr],
        _binder_aggs: &[LogicalAggregateSpec],
        having: Option<&LogicalExpr>,
        items: &[LogicalProjectionItem],
        distinct: bool,
        outer_sort: Option<&[LogicalSortKey]>,
    ) -> NativeResult<ScopedRelation> {
        let (mut post_scope, agg_map, group_map, alias_exprs) =
            self.lower_aggregate_prefix(agg_input, group_by, having, items, outer_sort, &[])?;
        post_scope =
            self.materialize_having(post_scope, having, &alias_exprs, &agg_map, &group_map)?;
        self.finish_aggregate_project(
            post_scope,
            items,
            distinct,
            outer_sort,
            &alias_exprs,
            &agg_map,
            &group_map,
        )
    }

    #[allow(clippy::type_complexity)]
    pub(super) fn lower_aggregate_prefix(
        &mut self,
        agg_input: &LogicalRelation,
        group_by: &[LogicalExpr],
        having: Option<&LogicalExpr>,
        items: &[LogicalProjectionItem],
        outer_sort: Option<&[LogicalSortKey]>,
        extra_aggs: &[CollectedAgg],
    ) -> NativeResult<(
        ScopedRelation,
        Vec<(LogicalExpr, String)>,
        Vec<(LogicalExpr, String)>,
        BTreeMap<String, LogicalExpr>,
    )> {
        let raw_scope = self.lower_relation(agg_input)?;
        self.lower_aggregate_prefix_from_scope(
            &raw_scope, group_by, having, items, outer_sort, extra_aggs,
        )
    }

    #[allow(clippy::type_complexity)]
    pub(super) fn lower_aggregate_prefix_from_scope(
        &mut self,
        raw_scope: &ScopedRelation,
        group_by: &[LogicalExpr],
        having: Option<&LogicalExpr>,
        items: &[LogicalProjectionItem],
        outer_sort: Option<&[LogicalSortKey]>,
        extra_aggs: &[CollectedAgg],
    ) -> NativeResult<(
        ScopedRelation,
        Vec<(LogicalExpr, String)>,
        Vec<(LogicalExpr, String)>,
        BTreeMap<String, LogicalExpr>,
    )> {
        let alias_exprs = collect_select_alias_map(items);
        let resolved_group_by = resolve_group_by_aliases(group_by, raw_scope, &alias_exprs);
        let agg_specs =
            collect_all_aggregate_specs(items, having, outer_sort, &alias_exprs, extra_aggs);
        if resolved_group_by.is_empty() && agg_specs.is_empty() && having.is_some() {
            return Err(unsupported_sql_error(
                "HAVING requires grouped or scalar aggregation",
            ));
        }

        let mut pre_names: Vec<String> = raw_scope
            .entries
            .iter()
            .map(|entry| entry.internal.clone())
            .collect();
        let mut pre_projections = Vec::<(String, Expression)>::new();
        let (group_cols, group_map) = self.lower_group_by_columns(
            &resolved_group_by,
            raw_scope,
            &mut pre_names,
            &mut pre_projections,
        )?;
        let (measures, agg_map) = self.lower_aggregate_measures(
            &agg_specs,
            group_cols.is_empty(),
            raw_scope,
            &mut pre_names,
            &mut pre_projections,
        )?;

        let mut plan = raw_scope.plan.clone();
        if !pre_projections.is_empty() {
            let mut full_pre = raw_scope
                .entries
                .iter()
                .map(|entry| Ok((entry.internal.clone(), column(&entry.internal)?)))
                .collect::<NativeResult<Vec<_>>>()?;
            full_pre.extend(pre_projections);
            plan = Plan::Project(Box::new(Project {
                input: plan,
                expressions: full_pre,
            }));
        }
        let group_by_refs = group_cols
            .iter()
            .map(|c| ColumnRef::new(c.clone()))
            .collect::<NativeResult<Vec<_>>>()?;
        plan = Plan::Aggregate(Box::new(Aggregate {
            grouping_sets: None,
            input: plan,
            group_by: group_by_refs,
            measures,
        }));

        let post_entries = group_map
            .iter()
            .map(|(_, col)| col)
            .chain(agg_map.iter().map(|(_, col)| col))
            .map(|col| ScopeColumn {
                internal: col.clone(),
                qualifier: None,
                output: col.clone(),
            })
            .collect();
        Ok((
            ScopedRelation {
                plan,
                entries: post_entries,
            },
            agg_map,
            group_map,
            alias_exprs,
        ))
    }

    fn lower_group_by_columns(
        &mut self,
        resolved_group_by: &[LogicalExpr],
        raw_scope: &ScopedRelation,
        pre_names: &mut Vec<String>,
        pre_projections: &mut Vec<(String, Expression)>,
    ) -> NativeResult<GroupByColumns> {
        let mut group_cols = Vec::with_capacity(resolved_group_by.len());
        let mut group_map = Vec::with_capacity(resolved_group_by.len());
        for gb in resolved_group_by {
            let col_name = if let LogicalExpr::Column { qualifier, name } = gb {
                raw_scope.resolve(qualifier.as_deref(), name)?
            } else {
                let expr = self.lower_expr(gb, raw_scope)?;
                let col = fresh_col_names(pre_names, &mut self.lowerer.serial);
                pre_names.push(col.clone());
                pre_projections.push((col.clone(), expr));
                col
            };
            group_cols.push(col_name.clone());
            group_map.push((gb.clone(), col_name));
        }
        Ok((group_cols, group_map))
    }

    fn lower_aggregate_measures(
        &mut self,
        agg_specs: &[CollectedAgg],
        group_empty: bool,
        raw_scope: &ScopedRelation,
        pre_names: &mut Vec<String>,
        pre_projections: &mut Vec<(String, Expression)>,
    ) -> NativeResult<AggregateMeasures> {
        let mut measures = Vec::with_capacity(agg_specs.len());
        let mut agg_map = Vec::with_capacity(agg_specs.len());
        for spec in agg_specs {
            let alias = fresh_col_names(pre_names, &mut self.lowerer.serial);
            pre_names.push(alias.clone());
            let measure =
                self.lower_single_measure(spec, &alias, raw_scope, pre_names, pre_projections)?;
            measures.push(measure);
            agg_map.push((spec.expr.clone(), alias));
        }
        if group_empty && measures.is_empty() {
            let dummy = fresh_col_names(pre_names, &mut self.lowerer.serial);
            pre_names.push(dummy.clone());
            measures.push(VortexSimpleAggregateMeasure::new("count", None, dummy));
        }
        Ok((measures, agg_map))
    }

    pub(super) fn materialize_having(
        &mut self,
        mut post_scope: ScopedRelation,
        having: Option<&LogicalExpr>,
        alias_exprs: &BTreeMap<String, LogicalExpr>,
        agg_map: &[(LogicalExpr, String)],
        group_map: &[(LogicalExpr, String)],
    ) -> NativeResult<ScopedRelation> {
        let Some(having_expr) = having else {
            return Ok(post_scope);
        };
        let with_aliases = substitute_alias_expr(having_expr, alias_exprs);
        let mut rewritten = rewrite_agg_and_group_expr(&with_aliases, agg_map, group_map);
        let saved_entries = post_scope.entries.clone();
        post_scope = subquery::materialize_expr_subqueries(self, post_scope, &mut rewritten)?;
        let pred = self.lower_expr(&rewritten, &post_scope)?;
        post_scope.plan = Plan::Filter(Box::new(Filter {
            input: post_scope.plan,
            predicate: pred,
        }));
        if post_scope.entries.len() != saved_entries.len() {
            let exprs = saved_entries
                .iter()
                .map(|entry| Ok((entry.internal.clone(), column(&entry.internal)?)))
                .collect::<NativeResult<Vec<_>>>()?;
            post_scope.entries = saved_entries;
            post_scope.plan = Plan::Project(Box::new(Project {
                input: post_scope.plan,
                expressions: exprs,
            }));
        }
        Ok(post_scope)
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn finish_aggregate_project(
        &mut self,
        post_scope: ScopedRelation,
        items: &[LogicalProjectionItem],
        distinct: bool,
        outer_sort: Option<&[LogicalSortKey]>,
        alias_exprs: &BTreeMap<String, LogicalExpr>,
        agg_map: &[(LogicalExpr, String)],
        group_map: &[(LogicalExpr, String)],
    ) -> NativeResult<ScopedRelation> {
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
                        expr: Box::new(rewrite_agg_and_group_expr(expr, agg_map, group_map)),
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
                        && items_has_alias(items, name)
                    {
                        key.expr.clone()
                    } else {
                        let with_aliases = substitute_alias_expr(&key.expr, alias_exprs);
                        rewrite_agg_and_group_expr(&with_aliases, agg_map, group_map)
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

    fn lower_single_measure(
        &mut self,
        spec: &CollectedAgg,
        alias: &str,
        raw_scope: &ScopedRelation,
        pre_names: &mut Vec<String>,
        pre_projections: &mut Vec<(String, Expression)>,
    ) -> NativeResult<VortexSimpleAggregateMeasure> {
        let lower_name = spec.name.to_ascii_lowercase();
        let (fn_name, effective_distinct) = if lower_name == "count_distinct" {
            ("count", true)
        } else if lower_name == "group_concat" {
            ("string_agg", spec.distinct)
        } else {
            (lower_name.as_str(), spec.distinct)
        };
        let agg_fn = AggregateFunction::parse(fn_name).ok_or_else(|| {
            unsupported_sql_error(&format!(
                "aggregate function {:?} is not admitted in canonical native lowering",
                spec.name
            ))
        })?;
        let native_name = agg_fn.native_name(effective_distinct)?;

        let mut arg_col = None;
        let mut secondary_col = None;
        let mut separator = None;
        let mut lower_arg = |expr: &LogicalExpr| -> NativeResult<Option<ColumnRef>> {
            if matches!(expr, LogicalExpr::Wildcard { qualifier: None }) {
                return Ok(None);
            }
            if let LogicalExpr::Column { qualifier, name } = expr {
                return Ok(Some(ColumnRef::new(
                    raw_scope.resolve(qualifier.as_deref(), name)?,
                )?));
            }
            let lowered = self.lower_expr(expr, raw_scope)?;
            let col = fresh_col_names(pre_names, &mut self.lowerer.serial);
            pre_names.push(col.clone());
            pre_projections.push((col.clone(), lowered));
            Ok(Some(ColumnRef::new(col)?))
        };

        if let Some(first) = spec.args.first() {
            arg_col = lower_arg(first)?;
        }
        if let Some(second) = spec.args.get(1) {
            if matches!(agg_fn, AggregateFunction::StringAgg) {
                separator = Some(self.extract_utf8_literal(second, "STRING_AGG separator")?);
            } else {
                secondary_col = lower_arg(second)?;
            }
        }
        if spec.args.len() > 2 {
            return Err(unsupported_sql_error(&format!(
                "aggregate function {:?} accepts at most two arguments",
                spec.name
            )));
        }

        let filter_col = if let Some(filter) = &spec.filter {
            let lowered = self.lower_expr(filter, raw_scope)?;
            let col = fresh_col_names(pre_names, &mut self.lowerer.serial);
            pre_names.push(col.clone());
            pre_projections.push((col.clone(), lowered));
            Some(ColumnRef::new(col)?)
        } else {
            None
        };

        let mut order_keys = Vec::with_capacity(spec.order_by.len());
        for key in &spec.order_by {
            let target = match &key.expr {
                LogicalExpr::Column { qualifier, name } => {
                    raw_scope.resolve(qualifier.as_deref(), name)?
                }
                other => {
                    let lowered = self.lower_expr(other, raw_scope)?;
                    let col = fresh_col_names(pre_names, &mut self.lowerer.serial);
                    pre_names.push(col.clone());
                    pre_projections.push((col.clone(), lowered));
                    col
                }
            };
            order_keys.push(make_order_key(&target, key)?);
        }

        let mut measure =
            VortexSimpleAggregateMeasure::new(native_name, arg_col, alias.to_string());
        measure.secondary_column = secondary_col;
        measure.filter_column = filter_col;
        measure.separator = separator;
        measure.order_by = order_keys;
        Ok(measure)
    }

    fn lower_join_on_expr(
        &mut self,
        expr: &LogicalExpr,
        left: &ScopedRelation,
        right: &ScopedRelation,
    ) -> NativeResult<Expression> {
        let resolve_join_col = |qualifier: Option<&str>, name: &str| -> NativeResult<String> {
            let in_left = left.resolve(qualifier, name);
            let in_right = right.resolve(qualifier, name);
            match (in_left, in_right) {
                (Ok(l), Err(_)) => Ok(format!("left.{l}")),
                (Err(_), Ok(r)) => Ok(format!("right.{r}")),
                (Ok(_), Ok(_)) => {
                    let label = match qualifier {
                        Some(q) => format!("{q}.{name}"),
                        None => name.to_string(),
                    };
                    Err(unsupported_sql_error(&format!(
                        "column {label:?} is ambiguous in JOIN ON; qualify it with a source alias"
                    )))
                }
                (Err(err), Err(_)) => Err(err),
            }
        };
        self.lower_expr_with_resolver(expr, &resolve_join_col)
    }

    fn lower_expr(
        &mut self,
        expr: &LogicalExpr,
        scope: &ScopedRelation,
    ) -> NativeResult<Expression> {
        self.lower_expr_with_resolver(expr, &|qualifier, name| scope.resolve(qualifier, name))
    }

    #[allow(clippy::too_many_lines)]
    fn lower_expr_with_resolver(
        &mut self,
        expr: &LogicalExpr,
        resolve_col: &dyn Fn(Option<&str>, &str) -> NativeResult<String>,
    ) -> NativeResult<Expression> {
        match expr {
            LogicalExpr::Literal(v) => {
                Ok(Expression::literal(self.next_id("canon.lit")?, v.clone()))
            }
            LogicalExpr::Parameter(name) => self
                .lowerer
                .parameters
                .get(name)
                .cloned()
                .ok_or_else(|| unsupported_sql_error(&format!("unbound SQL parameter :{name}"))),
            LogicalExpr::Column { qualifier, name } => {
                let col = resolve_col(qualifier.as_deref(), name)?;
                Ok(Expression::column(
                    self.next_id("canon.col")?,
                    ColumnRef::new(col)?,
                ))
            }
            LogicalExpr::Unary { op, expr } => Ok(Expression::new(
                self.next_id("canon.unary")?,
                ExpressionKind::Unary {
                    op: *op,
                    expr: Box::new(self.lower_expr_with_resolver(expr, resolve_col)?),
                },
            )),
            LogicalExpr::Binary { left, op, right } => Ok(Expression::new(
                self.next_id("canon.bin")?,
                ExpressionKind::Binary {
                    left: Box::new(self.lower_expr_with_resolver(left, resolve_col)?),
                    op: *op,
                    right: Box::new(self.lower_expr_with_resolver(right, resolve_col)?),
                },
            )),
            LogicalExpr::Compare { left, op, right } => Ok(Expression::new(
                self.next_id("canon.cmp")?,
                ExpressionKind::Compare {
                    left: Box::new(self.lower_expr_with_resolver(left, resolve_col)?),
                    op: *op,
                    right: Box::new(self.lower_expr_with_resolver(right, resolve_col)?),
                },
            )),
            LogicalExpr::IsDistinctFrom {
                left,
                right,
                negated,
            } => {
                let l = self.lower_expr_with_resolver(left, resolve_col)?;
                let r = self.lower_expr_with_resolver(right, resolve_col)?;
                self.build_is_distinct_from(l, r, *negated)
            }
            LogicalExpr::Between {
                expr,
                low,
                high,
                negated,
            } => {
                let e = self.lower_expr_with_resolver(expr, resolve_col)?;
                let lo = self.lower_expr_with_resolver(low, resolve_col)?;
                let hi = self.lower_expr_with_resolver(high, resolve_col)?;
                let ge = Expression::new(
                    self.next_id("canon.ge")?,
                    ExpressionKind::Compare {
                        left: Box::new(e.clone()),
                        op: ComparisonOp::GtEq,
                        right: Box::new(lo),
                    },
                );
                let le = Expression::new(
                    self.next_id("canon.le")?,
                    ExpressionKind::Compare {
                        left: Box::new(e),
                        op: ComparisonOp::LtEq,
                        right: Box::new(hi),
                    },
                );
                let inside = Expression::new(
                    self.next_id("canon.between")?,
                    ExpressionKind::Binary {
                        left: Box::new(ge),
                        op: BinaryOp::And,
                        right: Box::new(le),
                    },
                );
                if *negated {
                    Ok(Expression::new(
                        self.next_id("canon.not_between")?,
                        ExpressionKind::Unary {
                            op: UnaryOp::Not,
                            expr: Box::new(inside),
                        },
                    ))
                } else {
                    Ok(inside)
                }
            }
            LogicalExpr::InList {
                expr,
                list,
                negated,
            } => {
                if list.is_empty() {
                    return Ok(Expression::literal(
                        self.next_id("canon.in_empty")?,
                        ScalarValue::Boolean(*negated),
                    ));
                }
                let lhs = self.lower_expr_with_resolver(expr, resolve_col)?;
                let mut acc: Option<Expression> = None;
                for item in list {
                    let rhs = self.lower_expr_with_resolver(item, resolve_col)?;
                    let eq = Expression::new(
                        self.next_id("canon.in_eq")?,
                        ExpressionKind::Compare {
                            left: Box::new(lhs.clone()),
                            op: ComparisonOp::Eq,
                            right: Box::new(rhs),
                        },
                    );
                    acc = Some(match acc {
                        None => eq,
                        Some(prev) => Expression::new(
                            self.next_id("canon.in_or")?,
                            ExpressionKind::Binary {
                                left: Box::new(prev),
                                op: BinaryOp::Or,
                                right: Box::new(eq),
                            },
                        ),
                    });
                }
                let disjunction = acc.expect("non-empty IN list produces disjunction");
                if *negated {
                    Ok(Expression::new(
                        self.next_id("canon.not_in")?,
                        ExpressionKind::Unary {
                            op: UnaryOp::Not,
                            expr: Box::new(disjunction),
                        },
                    ))
                } else {
                    Ok(disjunction)
                }
            }
            LogicalExpr::Like {
                expr,
                pattern,
                escape,
                negated,
                case_insensitive,
            } => {
                let target = self.lower_expr_with_resolver(expr, resolve_col)?;
                let pat = self.extract_utf8_literal(pattern, "LIKE pattern")?;
                let esc = match escape.as_deref() {
                    None => None,
                    Some(s) => {
                        let mut chars = s.chars();
                        match (chars.next(), chars.next()) {
                            (Some(ch), None) => Some(ch),
                            _ => {
                                return Err(unsupported_sql_error(
                                    "LIKE ESCAPE must be a single character string",
                                ));
                            }
                        }
                    }
                };
                let regex = canonical_like_to_regex(&pat, esc, *case_insensitive)?;
                let call = Expression::new(
                    self.next_id("canon.like")?,
                    ExpressionKind::FunctionCall {
                        name: "utf8_regex_match".into(),
                        args: vec![
                            target,
                            Expression::literal(
                                self.next_id("canon.like_pat")?,
                                ScalarValue::Utf8(regex),
                            ),
                        ],
                    },
                );
                if *negated {
                    Ok(Expression::new(
                        self.next_id("canon.not_like")?,
                        ExpressionKind::Unary {
                            op: UnaryOp::Not,
                            expr: Box::new(call),
                        },
                    ))
                } else {
                    Ok(call)
                }
            }
            LogicalExpr::Cast {
                expr,
                target_dtype,
                try_cast,
            } => {
                let inner = self.lower_expr_with_resolver(expr, resolve_col)?;
                let dtype = parse_core_dtype_label(target_dtype)?;
                let kind = if *try_cast {
                    ExpressionKind::TryCast {
                        expr: Box::new(inner),
                        target_dtype: dtype,
                    }
                } else {
                    ExpressionKind::Cast {
                        expr: Box::new(inner),
                        target_dtype: dtype,
                    }
                };
                Ok(Expression::new(self.next_id("canon.cast")?, kind))
            }
            LogicalExpr::Case {
                operand,
                when_then,
                else_expr,
            } => {
                let op_expr = operand
                    .as_deref()
                    .map(|op| self.lower_expr_with_resolver(op, resolve_col))
                    .transpose()?;
                let mut acc = match else_expr.as_deref() {
                    Some(el) => self.lower_expr_with_resolver(el, resolve_col)?,
                    None => {
                        Expression::literal(self.next_id("canon.case_null")?, ScalarValue::Null)
                    }
                };
                for (when_expr, then_expr) in when_then.iter().rev() {
                    let when_low = self.lower_expr_with_resolver(when_expr, resolve_col)?;
                    let cond = match &op_expr {
                        Some(op) => Expression::new(
                            self.next_id("canon.case_eq")?,
                            ExpressionKind::Compare {
                                left: Box::new(op.clone()),
                                op: ComparisonOp::Eq,
                                right: Box::new(when_low),
                            },
                        ),
                        None => when_low,
                    };
                    let then_low = self.lower_expr_with_resolver(then_expr, resolve_col)?;
                    acc = Expression::new(
                        self.next_id("canon.case")?,
                        ExpressionKind::FunctionCall {
                            name: "case_when".into(),
                            args: vec![cond, then_low, acc],
                        },
                    );
                }
                Ok(acc)
            }
            LogicalExpr::Array(items) => {
                let values = items
                    .iter()
                    .map(|item| self.lower_expr_with_resolver(item, resolve_col))
                    .collect::<NativeResult<Vec<_>>>()?;
                Ok(Expression::new(
                    self.next_id("canon.list")?,
                    ExpressionKind::List { values },
                ))
            }
            LogicalExpr::Function {
                name,
                args,
                over: None,
                ..
            } => {
                let lower_name = name.to_ascii_lowercase();
                let mut lowered_args = args
                    .iter()
                    .map(|arg| self.lower_expr_with_resolver(arg, resolve_col))
                    .collect::<NativeResult<Vec<_>>>()?;
                let mapped_name = match lower_name.as_str() {
                    "substring" | "substr" => {
                        if lowered_args.len() == 2 {
                            let len_arg = Expression::new(
                                self.next_id("canon.substr_len")?,
                                ExpressionKind::FunctionCall {
                                    name: "length".into(),
                                    args: vec![lowered_args[0].clone()],
                                },
                            );
                            lowered_args.push(len_arg);
                        }
                        "substr".to_string()
                    }
                    "char_length" | "character_length" => "length".to_string(),
                    "lcase" => "lower".to_string(),
                    "ucase" => "upper".to_string(),
                    _ => lower_name,
                };
                Ok(Expression::new(
                    self.next_id("canon.fn")?,
                    ExpressionKind::FunctionCall {
                        name: mapped_name,
                        args: lowered_args,
                    },
                ))
            }
            other => Err(unsupported_sql_error(&format!(
                "expression `{other:?}` is not admitted in canonical native lowering"
            ))),
        }
    }

    fn build_is_distinct_from(
        &mut self,
        left: Expression,
        right: Expression,
        negated: bool,
    ) -> NativeResult<Expression> {
        let l_is_null = Expression::new(
            self.next_id("canon.l_null")?,
            ExpressionKind::Unary {
                op: UnaryOp::IsNull,
                expr: Box::new(left.clone()),
            },
        );
        let r_is_null = Expression::new(
            self.next_id("canon.r_null")?,
            ExpressionKind::Unary {
                op: UnaryOp::IsNull,
                expr: Box::new(right.clone()),
            },
        );
        let not_distinct = {
            let eq = Expression::new(
                self.next_id("canon.ndf_eq")?,
                ExpressionKind::Compare {
                    left: Box::new(left),
                    op: ComparisonOp::Eq,
                    right: Box::new(right),
                },
            );
            let inner = Expression::new(
                self.next_id("canon.ndf_inner")?,
                ExpressionKind::FunctionCall {
                    name: "case_when".into(),
                    args: vec![
                        r_is_null.clone(),
                        Expression::literal(
                            self.next_id("canon.ndf_false")?,
                            ScalarValue::Boolean(false),
                        ),
                        eq,
                    ],
                },
            );
            Expression::new(
                self.next_id("canon.ndf")?,
                ExpressionKind::FunctionCall {
                    name: "case_when".into(),
                    args: vec![l_is_null, r_is_null, inner],
                },
            )
        };
        if negated {
            Ok(not_distinct)
        } else {
            Ok(Expression::new(
                self.next_id("canon.df")?,
                ExpressionKind::Unary {
                    op: UnaryOp::Not,
                    expr: Box::new(not_distinct),
                },
            ))
        }
    }

    fn extract_utf8_literal(&self, expr: &LogicalExpr, context: &str) -> NativeResult<String> {
        let scalar = match expr {
            LogicalExpr::Literal(v) => v.clone(),
            LogicalExpr::Parameter(name) => match self.lowerer.parameters.get(name) {
                Some(param) => match &param.kind {
                    ExpressionKind::Literal(v) => v.clone(),
                    _ => {
                        return Err(unsupported_sql_error(&format!(
                            "{context} parameter :{name} must be a UTF8 string literal"
                        )));
                    }
                },
                None => {
                    return Err(unsupported_sql_error(&format!(
                        "unbound SQL parameter :{name}"
                    )));
                }
            },
            _ => {
                return Err(unsupported_sql_error(&format!(
                    "{context} requires a literal string value"
                )));
            }
        };
        match scalar {
            ScalarValue::Utf8(s) => Ok(s),
            _ => Err(unsupported_sql_error(&format!(
                "{context} requires a UTF8 string literal"
            ))),
        }
    }
}

#[derive(Clone)]
struct CollectedAgg {
    expr: LogicalExpr,
    name: String,
    args: Vec<LogicalExpr>,
    distinct: bool,
    filter: Option<LogicalExpr>,
    order_by: Vec<LogicalSortKey>,
}

fn push_join_side(
    side: Side,
    side_entries: &[ScopeColumn],
    used: &mut BTreeSet<String>,
    columns: &mut Vec<JoinColumn>,
    out_entries: &mut Vec<ScopeColumn>,
) -> NativeResult<()> {
    for entry in side_entries {
        let out_name = allocate_internal_name(used, entry.qualifier.as_deref(), &entry.output);
        columns.push(JoinColumn {
            side,
            column: ColumnRef::new(entry.internal.clone())?,
            output_column: out_name.clone(),
        });
        out_entries.push(ScopeColumn {
            internal: out_name,
            qualifier: entry.qualifier.clone(),
            output: entry.output.clone(),
        });
    }
    Ok(())
}

fn allocate_internal_name(
    used: &mut BTreeSet<String>,
    qualifier: Option<&str>,
    output: &str,
) -> String {
    let base = match qualifier {
        Some(q) => format!("{q}.{output}"),
        None => output.to_string(),
    };
    if used.insert(base.clone()) {
        return base;
    }
    let mut suffix = 1usize;
    loop {
        let candidate = format!("{base}__{suffix}");
        if used.insert(candidate.clone()) {
            return candidate;
        }
        suffix += 1;
    }
}

fn is_pure_equi_conjunction(expr: &Expression) -> bool {
    match &expr.kind {
        ExpressionKind::Binary {
            left,
            op: BinaryOp::And,
            right,
        } => is_pure_equi_conjunction(left) && is_pure_equi_conjunction(right),
        ExpressionKind::Compare {
            left,
            op: ComparisonOp::Eq,
            right,
        } => matches!(
            (&left.kind, &right.kind),
            (ExpressionKind::Column(l), ExpressionKind::Column(r))
                if (l.as_str().starts_with("left.") && r.as_str().starts_with("right."))
                    || (r.as_str().starts_with("left.") && l.as_str().starts_with("right."))
        ),
        _ => false,
    }
}

fn make_order_key(column: &str, key: &LogicalSortKey) -> NativeResult<OrderKey> {
    Ok(OrderKey {
        column: ColumnRef::new(column)?,
        descending: key.descending,
        nulls: key.nulls_first.map(|first| {
            if first {
                NullOrder::First
            } else {
                NullOrder::Last
            }
        }),
    })
}

fn fresh_col(entries: &[ScopeColumn], serial: &mut usize) -> String {
    let names: Vec<String> = entries.iter().map(|e| e.internal.clone()).collect();
    fresh_col_names(&names, serial)
}

fn fresh_col_names(existing: &[String], serial: &mut usize) -> String {
    loop {
        let name = format!("__shardloom_expression_{}", *serial);
        *serial += 1;
        if !existing.contains(&name) {
            return name;
        }
    }
}

fn is_agg_name(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "count"
            | "count_distinct"
            | "sum"
            | "avg"
            | "mean"
            | "min"
            | "max"
            | "var_pop"
            | "var_samp"
            | "variance"
            | "var"
            | "stddev_pop"
            | "stddev_samp"
            | "stddev"
            | "std"
            | "covar_pop"
            | "covar_samp"
            | "corr"
            | "median"
            | "quantile_cont"
            | "quantile_disc"
            | "first"
            | "last"
            | "arg_min"
            | "arg_max"
            | "list"
            | "array_agg"
            | "string_agg"
            | "group_concat"
    )
}

fn collect_expr_aggregates(expr: &LogicalExpr, out: &mut Vec<CollectedAgg>) {
    match expr {
        LogicalExpr::Function {
            name,
            args,
            distinct,
            filter,
            order_by,
            over: None,
        } if is_agg_name(name) => {
            if !out.iter().any(|existing| existing.expr == *expr) {
                out.push(CollectedAgg {
                    expr: expr.clone(),
                    name: name.clone(),
                    args: args.clone(),
                    distinct: *distinct,
                    filter: filter.as_deref().cloned(),
                    order_by: order_by.clone(),
                });
            }
        }
        LogicalExpr::Function {
            args,
            filter,
            order_by,
            over,
            ..
        } => collect_fn_aggregates(args, filter.as_deref(), order_by, over.as_ref(), out),
        LogicalExpr::InSubquery { expr, .. } => {
            collect_expr_aggregates(expr, out);
        }
        LogicalExpr::Unary { expr: inner, .. } | LogicalExpr::Cast { expr: inner, .. } => {
            collect_expr_aggregates(inner, out);
        }
        LogicalExpr::Binary { left, right, .. }
        | LogicalExpr::Compare { left, right, .. }
        | LogicalExpr::IsDistinctFrom { left, right, .. }
        | LogicalExpr::Like {
            expr: left,
            pattern: right,
            ..
        } => {
            collect_expr_aggregates(left, out);
            collect_expr_aggregates(right, out);
        }
        LogicalExpr::Between {
            expr, low, high, ..
        } => {
            collect_expr_aggregates(expr, out);
            collect_expr_aggregates(low, out);
            collect_expr_aggregates(high, out);
        }
        LogicalExpr::InList { expr, list, .. } => {
            collect_expr_aggregates(expr, out);
            for item in list {
                collect_expr_aggregates(item, out);
            }
        }
        LogicalExpr::Case {
            operand,
            when_then,
            else_expr,
        } => {
            if let Some(op) = operand {
                collect_expr_aggregates(op, out);
            }
            for (w, t) in when_then {
                collect_expr_aggregates(w, out);
                collect_expr_aggregates(t, out);
            }
            if let Some(e) = else_expr {
                collect_expr_aggregates(e, out);
            }
        }
        LogicalExpr::Array(items) => {
            for item in items {
                collect_expr_aggregates(item, out);
            }
        }
        _ => {}
    }
}

fn collect_fn_aggregates(
    args: &[LogicalExpr],
    filter: Option<&LogicalExpr>,
    order_by: &[LogicalSortKey],
    over: Option<&LogicalWindowSpec>,
    out: &mut Vec<CollectedAgg>,
) {
    for arg in args {
        collect_expr_aggregates(arg, out);
    }
    if let Some(f) = filter {
        collect_expr_aggregates(f, out);
    }
    for k in order_by {
        collect_expr_aggregates(&k.expr, out);
    }
    if let Some(spec) = over {
        for p in &spec.partition_by {
            collect_expr_aggregates(p, out);
        }
        for k in &spec.order_by {
            collect_expr_aggregates(&k.expr, out);
        }
    }
}

fn collect_all_aggregate_specs(
    items: &[LogicalProjectionItem],
    having: Option<&LogicalExpr>,
    outer_sort: Option<&[LogicalSortKey]>,
    alias_exprs: &BTreeMap<String, LogicalExpr>,
    extra_aggs: &[CollectedAgg],
) -> Vec<CollectedAgg> {
    let mut agg_specs = Vec::<CollectedAgg>::new();
    for item in items {
        if let LogicalProjectionItem::Expr { expr, .. } = item {
            collect_expr_aggregates(expr, &mut agg_specs);
        }
    }
    if let Some(having_expr) = having {
        let substituted = substitute_alias_expr(having_expr, alias_exprs);
        collect_expr_aggregates(&substituted, &mut agg_specs);
    }
    if let Some(keys) = outer_sort {
        for key in keys {
            let substituted = substitute_alias_expr(&key.expr, alias_exprs);
            collect_expr_aggregates(&substituted, &mut agg_specs);
        }
    }
    for spec in extra_aggs {
        if !agg_specs.iter().any(|existing| existing.expr == spec.expr) {
            agg_specs.push(spec.clone());
        }
    }
    agg_specs
}

fn collect_select_alias_map(items: &[LogicalProjectionItem]) -> BTreeMap<String, LogicalExpr> {
    let mut map = BTreeMap::new();
    for item in items {
        if let LogicalProjectionItem::Expr {
            expr,
            alias: Some(alias),
        } = item
        {
            map.insert(alias.clone(), expr.as_ref().clone());
        }
    }
    map
}

fn items_has_alias(items: &[LogicalProjectionItem], target: &str) -> bool {
    items.iter().any(|item| {
        matches!(
            item,
            LogicalProjectionItem::Expr { alias: Some(a), .. } if a == target
        )
    })
}

fn resolve_group_by_aliases(
    group_by: &[LogicalExpr],
    raw_scope: &ScopedRelation,
    aliases: &BTreeMap<String, LogicalExpr>,
) -> Vec<LogicalExpr> {
    group_by
        .iter()
        .map(|gb| {
            if let LogicalExpr::Column {
                qualifier: None,
                name,
            } = gb
                && raw_scope.resolve(None, name).is_err()
                && let Some(expr) = aliases.get(name)
            {
                return expr.clone();
            }
            gb.clone()
        })
        .collect()
}

fn substitute_alias_expr(
    expr: &LogicalExpr,
    aliases: &BTreeMap<String, LogicalExpr>,
) -> LogicalExpr {
    if let LogicalExpr::Column {
        qualifier: None,
        name,
    } = expr
        && let Some(replacement) = aliases.get(name)
    {
        return replacement.clone();
    }
    map_logical_children(expr, &|child| substitute_alias_expr(child, aliases))
}

fn rewrite_agg_and_group_expr(
    expr: &LogicalExpr,
    agg_map: &[(LogicalExpr, String)],
    group_map: &[(LogicalExpr, String)],
) -> LogicalExpr {
    for (agg_expr, col) in agg_map {
        if expr == agg_expr {
            return LogicalExpr::Column {
                qualifier: None,
                name: col.clone(),
            };
        }
    }
    for (gb_expr, col) in group_map {
        if expr == gb_expr {
            return LogicalExpr::Column {
                qualifier: None,
                name: col.clone(),
            };
        }
    }
    if let LogicalExpr::Column { qualifier, name } = expr {
        for (gb_expr, col) in group_map {
            if let LogicalExpr::Column {
                qualifier: gb_q,
                name: gb_name,
            } = gb_expr
                && gb_name == name
                && (qualifier.is_none() || qualifier == gb_q)
            {
                return LogicalExpr::Column {
                    qualifier: None,
                    name: col.clone(),
                };
            }
        }
    }
    map_logical_children(expr, &|child| {
        rewrite_agg_and_group_expr(child, agg_map, group_map)
    })
}

fn map_logical_children(
    expr: &LogicalExpr,
    f: &dyn Fn(&LogicalExpr) -> LogicalExpr,
) -> LogicalExpr {
    match expr {
        LogicalExpr::Unary { op, expr } => LogicalExpr::Unary {
            op: *op,
            expr: Box::new(f(expr)),
        },
        LogicalExpr::Binary { left, op, right } => LogicalExpr::Binary {
            left: Box::new(f(left)),
            op: *op,
            right: Box::new(f(right)),
        },
        LogicalExpr::Compare { left, op, right } => LogicalExpr::Compare {
            left: Box::new(f(left)),
            op: *op,
            right: Box::new(f(right)),
        },
        LogicalExpr::IsDistinctFrom {
            left,
            right,
            negated,
        } => LogicalExpr::IsDistinctFrom {
            left: Box::new(f(left)),
            right: Box::new(f(right)),
            negated: *negated,
        },
        LogicalExpr::Between {
            expr,
            low,
            high,
            negated,
        } => LogicalExpr::Between {
            expr: Box::new(f(expr)),
            low: Box::new(f(low)),
            high: Box::new(f(high)),
            negated: *negated,
        },
        LogicalExpr::InList {
            expr,
            list,
            negated,
        } => LogicalExpr::InList {
            expr: Box::new(f(expr)),
            list: list.iter().map(f).collect(),
            negated: *negated,
        },
        LogicalExpr::InSubquery {
            expr,
            subquery,
            negated,
        } => LogicalExpr::InSubquery {
            expr: Box::new(f(expr)),
            subquery: subquery.clone(),
            negated: *negated,
        },
        expr @ LogicalExpr::Like { .. } => map_logical_like(expr, f),
        LogicalExpr::Cast {
            expr,
            target_dtype,
            try_cast,
        } => LogicalExpr::Cast {
            expr: Box::new(f(expr)),
            target_dtype: target_dtype.clone(),
            try_cast: *try_cast,
        },
        expr @ LogicalExpr::Case { .. } => map_logical_case(expr, f),
        LogicalExpr::Function {
            name,
            args,
            distinct,
            filter,
            order_by,
            over,
        } => map_logical_function(
            name,
            args,
            *distinct,
            filter.as_deref(),
            order_by,
            over.as_ref(),
            f,
        ),
        LogicalExpr::Array(items) => LogicalExpr::Array(items.iter().map(f).collect()),
        LogicalExpr::Tuple(items) => LogicalExpr::Tuple(items.iter().map(f).collect()),
        other => other.clone(),
    }
}

fn map_logical_like(expr: &LogicalExpr, f: &dyn Fn(&LogicalExpr) -> LogicalExpr) -> LogicalExpr {
    let LogicalExpr::Like {
        expr,
        pattern,
        escape,
        negated,
        case_insensitive,
    } = expr
    else {
        return expr.clone();
    };
    LogicalExpr::Like {
        expr: Box::new(f(expr)),
        pattern: Box::new(f(pattern)),
        escape: escape.clone(),
        negated: *negated,
        case_insensitive: *case_insensitive,
    }
}

fn map_logical_case(expr: &LogicalExpr, f: &dyn Fn(&LogicalExpr) -> LogicalExpr) -> LogicalExpr {
    let LogicalExpr::Case {
        operand,
        when_then,
        else_expr,
    } = expr
    else {
        return expr.clone();
    };
    LogicalExpr::Case {
        operand: operand.as_deref().map(f).map(Box::new),
        when_then: when_then.iter().map(|(w, t)| (f(w), f(t))).collect(),
        else_expr: else_expr.as_deref().map(f).map(Box::new),
    }
}

fn map_logical_function(
    name: &str,
    args: &[LogicalExpr],
    distinct: bool,
    filter: Option<&LogicalExpr>,
    order_by: &[LogicalSortKey],
    over: Option<&LogicalWindowSpec>,
    f: &dyn Fn(&LogicalExpr) -> LogicalExpr,
) -> LogicalExpr {
    LogicalExpr::Function {
        name: name.to_owned(),
        args: args.iter().map(f).collect(),
        distinct,
        filter: filter.map(f).map(Box::new),
        order_by: order_by
            .iter()
            .map(|k| LogicalSortKey {
                expr: f(&k.expr),
                descending: k.descending,
                nulls_first: k.nulls_first,
            })
            .collect(),
        over: over.map(|spec| LogicalWindowSpec {
            name: spec.name.clone(),
            partition_by: spec.partition_by.iter().map(f).collect(),
            order_by: spec
                .order_by
                .iter()
                .map(|k| LogicalSortKey {
                    expr: f(&k.expr),
                    descending: k.descending,
                    nulls_first: k.nulls_first,
                })
                .collect(),
            frame: spec.frame.clone(),
        }),
    }
}

fn count_rel_cte_refs(
    rel: &LogicalRelation,
    shadowed: &BTreeSet<String>,
    counts: &mut BTreeMap<String, usize>,
) {
    match rel {
        LogicalRelation::Source {
            reference,
            quoted_path: false,
            ..
        } => {
            let key = reference.to_ascii_lowercase();
            if !shadowed.contains(&key) {
                *counts.entry(key).or_insert(0) += 1;
            }
        }
        LogicalRelation::Subquery { input, .. } | LogicalRelation::Limit { input, .. } => {
            count_rel_cte_refs(input, shadowed, counts);
        }
        LogicalRelation::Filter { input, predicate } => {
            count_rel_cte_refs(input, shadowed, counts);
            subquery::count_expr_cte_refs(predicate, shadowed, counts);
        }
        LogicalRelation::Project { input, items, .. } => {
            count_rel_cte_refs(input, shadowed, counts);
            for item in items {
                if let LogicalProjectionItem::Expr { expr, .. } = item {
                    subquery::count_expr_cte_refs(expr, shadowed, counts);
                }
            }
        }
        LogicalRelation::Aggregate {
            input,
            group_by,
            aggregates,
            having,
            ..
        } => {
            count_rel_cte_refs(input, shadowed, counts);
            for g in group_by {
                subquery::count_expr_cte_refs(g, shadowed, counts);
            }
            for agg in aggregates {
                for a in &agg.args {
                    subquery::count_expr_cte_refs(a, shadowed, counts);
                }
                if let Some(f) = &agg.filter {
                    subquery::count_expr_cte_refs(f, shadowed, counts);
                }
            }
            if let Some(h) = having {
                subquery::count_expr_cte_refs(h, shadowed, counts);
            }
        }
        LogicalRelation::Window { input, windows } => {
            count_rel_cte_refs(input, shadowed, counts);
            for (_, spec) in windows {
                for p in &spec.partition_by {
                    subquery::count_expr_cte_refs(p, shadowed, counts);
                }
                for k in &spec.order_by {
                    subquery::count_expr_cte_refs(&k.expr, shadowed, counts);
                }
            }
        }
        LogicalRelation::Sort { input, keys } => {
            count_rel_cte_refs(input, shadowed, counts);
            for k in keys {
                subquery::count_expr_cte_refs(&k.expr, shadowed, counts);
            }
        }
        LogicalRelation::Join {
            left, right, on, ..
        } => {
            count_rel_cte_refs(left, shadowed, counts);
            count_rel_cte_refs(right, shadowed, counts);
            if let Some(cond) = on {
                subquery::count_expr_cte_refs(cond, shadowed, counts);
            }
        }
        LogicalRelation::SetOperation { left, right, .. } => {
            count_rel_cte_refs(left, shadowed, counts);
            count_rel_cte_refs(right, shadowed, counts);
        }
        LogicalRelation::WithCte { ctes, body, .. } => {
            let mut inner_shadowed = shadowed.clone();
            for cte in ctes {
                count_rel_cte_refs(&cte.query, &inner_shadowed, counts);
                inner_shadowed.insert(cte.name.to_ascii_lowercase());
            }
            count_rel_cte_refs(body, &inner_shadowed, counts);
        }
        LogicalRelation::Source {
            quoted_path: true, ..
        }
        | LogicalRelation::Values { .. }
        | LogicalRelation::Unit
        | LogicalRelation::TableFunction { .. } => {}
    }
}

fn canonical_like_to_regex(
    pattern: &str,
    escape: Option<char>,
    case_insensitive: bool,
) -> NativeResult<String> {
    let mut regex = String::from(if case_insensitive { r"\A(?i)" } else { r"\A" });
    let mut chars = pattern.chars();
    while let Some(ch) = chars.next() {
        if escape.is_some_and(|esc| ch == esc) {
            let Some(next) = chars.next() else {
                return Err(unsupported_sql_error(
                    "LIKE ESCAPE pattern cannot end with the escape character",
                ));
            };
            if next != '%' && next != '_' && Some(next) != escape {
                return Err(unsupported_sql_error(
                    "LIKE ESCAPE may only escape %, _, or the escape character",
                ));
            }
            push_regex_escaped(&mut regex, next);
            continue;
        }
        match ch {
            '%' => regex.push_str("(?s:.*)"),
            '_' => regex.push_str("(?s:.)"),
            ch => push_regex_escaped(&mut regex, ch),
        }
    }
    regex.push_str(r"\z");
    regex::Regex::new(&regex)
        .map_err(|_| unsupported_sql_error("LIKE pattern could not be compiled as regex"))?;
    Ok(regex)
}

fn push_regex_escaped(regex: &mut String, ch: char) {
    match ch {
        '\\' | '.' | '+' | '*' | '?' | '(' | ')' | '|' | '[' | ']' | '{' | '}' | '^' | '$' => {
            regex.push('\\');
            regex.push(ch);
        }
        ch => regex.push(ch),
    }
}
