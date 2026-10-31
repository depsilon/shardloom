//! Validate declaration recursion and give expressions deterministic tree IDs.

use super::{NATIVE_RECURSION_LIMIT, Plan, Result, failed};
use shardloom_core::{ExprId, Expression, ExpressionKind, PredicateExpr, ScalarValue};
use shardloom_vortex::relational_query::VortexRelationalStage as Stage;

pub(super) fn visit(plan: &mut Plan, nodes: &mut usize) -> Result<()> {
    match plan {
        Plan::Scan(scan) => {
            if let Some(value) = &scan.predicate {
                predicate(value, 0)?;
            }
        }
        Plan::Project(node) => {
            visit(&mut node.input, nodes)?;
            for (_, value) in &mut node.expressions {
                expression(value, 0, nodes)?;
            }
        }
        Plan::Filter(node) => {
            visit(&mut node.input, nodes)?;
            expression(&mut node.predicate, 0, nodes)?;
        }
        Plan::Join(node) => {
            visit(&mut node.left, nodes)?;
            visit(&mut node.right, nodes)?;
            if let Some(value) = &mut node.condition {
                expression(value, 0, nodes)?;
            }
        }
        Plan::AsOfJoin(node) => {
            visit(&mut node.left, nodes)?;
            visit(&mut node.right, nodes)?;
        }
        Plan::Set(node) => {
            visit(&mut node.left, nodes)?;
            visit(&mut node.right, nodes)?;
        }
        Plan::Sort(node) => visit(&mut node.input, nodes)?,
        Plan::Limit(node) => visit(&mut node.input, nodes)?,
        Plan::Pipeline(node) => {
            visit(&mut node.input, nodes)?;
            for stage in &mut node.stages {
                *nodes = nodes
                    .checked_add(1)
                    .ok_or_else(|| failed("native plan size overflow"))?;
                match stage {
                    Stage::Project(projected) => {
                        for (_, value) in projected {
                            expression(value, 0, nodes)?;
                        }
                    }
                    Stage::Filter(value) => expression(value, 0, nodes)?,
                    Stage::ApplicationBatch(_) => {}
                    Stage::Observe(observation) => {
                        for (_, value) in &mut observation.projection {
                            expression(value, 0, nodes)?;
                        }
                    }
                    Stage::Limit { offset, count } => {
                        offset
                            .checked_add(*count)
                            .ok_or_else(|| failed("output range overflow"))?;
                    }
                }
            }
        }
        Plan::Aggregate(node) => visit(&mut node.input, nodes)?,
        Plan::Window(node) => visit(&mut node.input, nodes)?,
        Plan::GapFill(node) => visit(&mut node.input, nodes)?,
        Plan::Unary(node) => visit(&mut node.input, nodes)?,
        Plan::Subquery(node) | Plan::CorrelatedSubquery(node) => {
            visit(&mut node.input, nodes)?;
            visit(&mut node.relation, nodes)?;
            if let Some(value) = &mut node.evaluation_guard {
                expression(value, 0, nodes)?;
            }
        }
        // Validate shared definitions without rewriting their immutable identity.
        // Their cloned native lowering still binds every expression and domain.
        Plan::Materialized(input) => visit(&mut input.as_ref().clone(), nodes)?,
        Plan::Recursive(input) => {
            visit(&mut input.anchor.clone(), nodes)?;
            visit(&mut input.step.clone(), nodes)?;
        }
        Plan::Input(_) | Plan::Outer | Plan::RecursiveInput(_) => {}
        Plan::ExecutionResult(_) | Plan::DeferredSubquery(_) => {
            return Err(failed(
                "execution-scoped relation is not a reusable declaration",
            ));
        }
    }
    Ok(())
}

fn expression(value: &mut Expression, depth: usize, nodes: &mut usize) -> Result<()> {
    check_depth(depth)?;
    *nodes = nodes
        .checked_add(1)
        .ok_or_else(|| failed("native expression size overflow"))?;
    value.id = ExprId::new(format!("native_expression_{nodes}"))?;
    match &mut value.kind {
        ExpressionKind::Literal(value) => scalar(value, 0)?,
        ExpressionKind::Column(_) | ExpressionKind::Unsupported { .. } => {}
        ExpressionKind::RelationalValue { .. } => {
            return Err(failed(
                "relational value has no resolved native query binding",
            ));
        }
        ExpressionKind::Alias { expr, .. }
        | ExpressionKind::Cast { expr, .. }
        | ExpressionKind::TryCast { expr, .. }
        | ExpressionKind::Unary { expr, .. } => expression(expr, depth + 1, nodes)?,
        ExpressionKind::List { values } | ExpressionKind::FunctionCall { args: values, .. } => {
            for child in values {
                expression(child, depth + 1, nodes)?;
            }
        }
        ExpressionKind::Struct { fields } => {
            for (_, child) in fields {
                expression(child, depth + 1, nodes)?;
            }
        }
        ExpressionKind::Binary { left, right, .. }
        | ExpressionKind::Compare { left, right, .. } => {
            expression(left, depth + 1, nodes)?;
            expression(right, depth + 1, nodes)?;
        }
    }
    Ok(())
}

fn check_depth(depth: usize) -> Result<()> {
    if depth > NATIVE_RECURSION_LIMIT {
        return Err(failed("recursive scalar expression exceeds 24 levels"));
    }
    Ok(())
}

pub(super) fn scalar(value: &ScalarValue, depth: usize) -> Result<()> {
    check_depth(depth)?;
    match value {
        ScalarValue::List(values) => {
            for child in values {
                scalar(child, depth + 1)?;
            }
        }
        ScalarValue::Struct(fields) => {
            for (_, child) in fields {
                scalar(child, depth + 1)?;
            }
        }
        _ => {}
    }
    Ok(())
}

pub(super) fn parameter(value: &Expression, depth: usize) -> Result<()> {
    check_depth(depth)?;
    match &value.kind {
        ExpressionKind::Literal(value) => scalar(value, depth),
        ExpressionKind::Cast { expr, .. } => parameter(expr, depth + 1),
        _ => Err(failed(
            "SQL parameter must be a typed scalar literal, not an expression or identifier",
        )),
    }
}

fn predicate(value: &PredicateExpr, depth: usize) -> Result<()> {
    check_depth(depth)?;
    if let PredicateExpr::And(values) = value {
        for child in values {
            predicate(child, depth + 1)?;
        }
    }
    Ok(())
}
