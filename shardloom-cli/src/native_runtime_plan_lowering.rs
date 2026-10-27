//! Schema names and source rebinding for typed templates. The native binder
//! remains responsible for all expression, type and operator semantics.

use super::{
    CancellationToken, DatasetUri, ParsedRelationLeaf, Plan, ResolvedSources, Result, failed,
};
use shardloom_plan::ProjectionRequest;
use shardloom_vortex::{
    VortexQueryPrimitiveKind,
    local_primitives::prepared_relational::VortexRelationalPreparation,
    relational_query::{VortexRelationalSet, VortexRelationalSetKind},
};

pub(super) fn leaf(uri: &DatasetUri) -> ParsedRelationLeaf {
    ParsedRelationLeaf {
        path: uri.as_str().into(),
        declared_identifier: false,
        memory_input: None,
    }
}

pub(super) fn rebind(
    plan: &mut Plan,
    sources: &ResolvedSources,
    cancellation: &CancellationToken,
) -> Result<()> {
    let mut pending = vec![plan];
    while let Some(plan) = pending.pop() {
        cancellation.check()?;
        match plan {
            Plan::Input(_) => return Err(failed("native declaration input is not bound")),
            Plan::Scan(scan) => {
                let Some(uris) = sources.get(&leaf(&scan.source_uri)) else {
                    // A SQL child already resolved its physical source through
                    // this same map. It must remain inside that admitted set.
                    if sources.values().any(|uris| uris.contains(&scan.source_uri)) {
                        continue;
                    }
                    return Err(failed("native source was not declared"));
                };
                if let [uri] = uris.as_slice() {
                    scan.source_uri = uri.clone();
                } else {
                    let mut parts = uris
                        .iter()
                        .map(|uri| {
                            let mut part = scan.clone();
                            part.source_uri = uri.clone();
                            Plan::Scan(part)
                        })
                        .collect::<Vec<_>>();
                    while parts.len() > 1 {
                        let mut joined = Vec::with_capacity(parts.len().div_ceil(2));
                        let mut iter = parts.into_iter();
                        while let Some(left) = iter.next() {
                            joined.push(if let Some(right) = iter.next() {
                                Plan::Set(Box::new(VortexRelationalSet {
                                    left,
                                    right,
                                    kind: VortexRelationalSetKind::UnionAll,
                                }))
                            } else {
                                left
                            });
                        }
                        parts = joined;
                    }
                    *plan = parts
                        .pop()
                        .ok_or_else(|| failed("native source collection is empty"))?;
                }
            }
            Plan::Project(node) => pending.push(&mut node.input),
            Plan::Filter(node) => pending.push(&mut node.input),
            Plan::Sort(node) => pending.push(&mut node.input),
            Plan::Limit(node) => pending.push(&mut node.input),
            Plan::Pipeline(node) => pending.push(&mut node.input),
            Plan::Aggregate(node) => pending.push(&mut node.input),
            Plan::Window(node) => pending.push(&mut node.input),
            Plan::Unary(node) => pending.push(&mut node.input),
            Plan::Join(node) => {
                pending.push(&mut node.right);
                pending.push(&mut node.left);
            }
            Plan::Set(node) => {
                pending.push(&mut node.right);
                pending.push(&mut node.left);
            }
            Plan::Subquery(node) | Plan::CorrelatedSubquery(node) => {
                pending.push(&mut node.relation);
                pending.push(&mut node.input);
            }
            Plan::Outer | Plan::ExecutionResult(_) | Plan::DeferredSubquery(_) => {}
        }
    }
    Ok(())
}

pub(super) fn columns(
    mut plan: &mut Plan,
    schemas: &mut VortexRelationalPreparation<'_>,
    cancellation: &CancellationToken,
) -> Result<Vec<String>> {
    loop {
        cancellation.check()?;
        match plan {
            Plan::Scan(scan) => {
                return match &scan.projection {
                    ProjectionRequest::All => schemas.source_columns(&scan.source_uri),
                    ProjectionRequest::Columns(columns) => Ok(columns
                        .iter()
                        .map(|column| column.as_str().to_owned())
                        .collect()),
                };
            }
            Plan::Project(node) => {
                return Ok(node
                    .expressions
                    .iter()
                    .map(|(name, _)| name.clone())
                    .collect());
            }
            Plan::Filter(node) => plan = &mut node.input,
            Plan::Sort(node) => plan = &mut node.input,
            Plan::Limit(node) => plan = &mut node.input,
            Plan::Pipeline(node) => {
                for stage in node.stages.iter().rev() {
                    if let shardloom_vortex::relational_query::VortexRelationalStage::Project(
                        expressions,
                    ) = stage
                    {
                        return Ok(expressions.iter().map(|(name, _)| name.clone()).collect());
                    }
                }
                plan = &mut node.input;
            }
            Plan::Aggregate(node) => {
                return Ok(node.output_columns().map(str::to_owned).collect());
            }
            Plan::Join(node) => {
                return Ok(node
                    .columns
                    .iter()
                    .map(|column| column.output_column.clone())
                    .collect());
            }
            Plan::Set(node) => plan = &mut node.left,
            Plan::Window(node) => {
                return Ok(node
                    .columns
                    .iter()
                    .map(|column| column.as_str().to_owned())
                    .chain(
                        node.expressions
                            .iter()
                            .map(|expression| expression.output_column.clone()),
                    )
                    .collect());
            }
            Plan::Subquery(node) | Plan::CorrelatedSubquery(node) => {
                let mut names = columns(&mut node.input, schemas, cancellation)?;
                names.push(node.output_column.clone());
                return Ok(names);
            }
            Plan::Unary(node) if node.request.kind != VortexQueryPrimitiveKind::PivotRows => {
                let selected = match &node.request.projection {
                    ProjectionRequest::All => columns(&mut node.input, schemas, cancellation)?,
                    ProjectionRequest::Columns(columns) => columns
                        .iter()
                        .map(|column| column.as_str().to_owned())
                        .collect(),
                };
                return Ok(if let Some(melt) = &node.request.melt_projection {
                    melt.output_columns()
                } else if let Some(rolling) = &node.request.rolling_window {
                    rolling.output_columns()
                } else if let Some(rewrites) = &node.request.expression_projection {
                    rewrites.output_columns(&selected)
                } else if let Some(explode) = &node.request.explode_projection {
                    explode.output_columns(&selected)
                } else if node.request.kind == VortexQueryPrimitiveKind::DuplicateMaskRows {
                    vec!["duplicated".into()]
                } else {
                    selected
                });
            }
            Plan::Unary(_) | Plan::ExecutionResult(_) => {
                let (resolved, columns) = schemas.resolve_output(plan)?;
                *plan = resolved;
                return Ok(columns);
            }
            Plan::Input(_) | Plan::Outer | Plan::DeferredSubquery(_) => {
                return Err(failed("relation output schema requires a bound input"));
            }
        }
    }
}
