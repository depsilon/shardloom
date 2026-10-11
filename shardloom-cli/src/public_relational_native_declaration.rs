//! Typed declarations reuse the SQL source resolver and ordinary native binder.

use super::{
    CancellationToken, DatasetUri, PathBuf, PreparedVortexRelational, PublicWorkflowRouteRequest,
    ShardLoomError, Sources, failed, native_relational, native_vortex_materializing_policy,
};
use shardloom_vortex::{
    local_primitives::prepared_relational::{
        VortexRelationalPreparation, prepare_relational_with_dynamic_inputs_in_session_controlled,
        prepare_relational_with_schema_in_session_controlled, validate_session_policy,
    },
    relational_query::{
        VortexRelationalPlan as Plan, VortexRelationalSet, VortexRelationalSetKind,
    },
    resident_session::ResidentVortexSession,
};
use std::{collections::BTreeMap, sync::Arc};

type Result<T> = std::result::Result<T, ShardLoomError>;

pub(crate) fn prepare_native_in_session(
    declaration: &crate::native::NativeRelationDeclaration,
    request: &PublicWorkflowRouteRequest,
    session: &ResidentVortexSession,
    inputs: impl FnOnce(&mut VortexRelationalPreparation<'_>) -> Result<()>,
    cancellation: &CancellationToken,
) -> Result<(PreparedVortexRelational, bool)> {
    cancellation.check()?;
    let policy = native_vortex_materializing_policy(request)?;
    validate_session_policy(policy, session)?;
    let mut metadata = session.reserve_input_scratch(
        u64::try_from(declaration.metadata_bytes).map_err(|_| failed("plan metadata overflow"))?,
    )?;
    for reference in request.source_bindings.keys() {
        if !declaration
            .sources
            .iter()
            .any(|uri| uri.as_str() == reference)
        {
            return Err(failed(
                "source declaration is not referenced by the native plan",
            ));
        }
    }
    let mut sources = Sources {
        cancellation: cancellation.clone(),
        ..Sources::default()
    };
    let mut resolved = BTreeMap::new();
    for reference in &declaration.sources {
        cancellation.check()?;
        let leaf = native_relational::ParsedRelationLeaf {
            path: PathBuf::from(reference.as_str()),
            declared_identifier: false,
            memory_input: None,
        };
        let uris = sources.resolve(&leaf, request, Some(&session.shared_memory_pool()))?;
        let bytes = uris.iter().try_fold(0_u64, |sum, uri| {
            let bytes = u64::try_from(uri.as_str().len())
                .ok()
                .and_then(|n| n.checked_mul(8))
                .and_then(|n| n.checked_add(8192));
            bytes
                .and_then(|n| sum.checked_add(n))
                .ok_or_else(|| failed("source metadata overflow"))
        })?;
        metadata.resize(
            metadata
                .bytes()
                .checked_add(bytes)
                .ok_or_else(|| failed("source metadata overflow"))?,
        )?;
        resolved.insert(reference.as_str().to_owned(), uris);
    }
    let mut plan = declaration.plan().clone();
    rebind(&mut plan, &resolved, cancellation)?;
    let operation = if declaration.dynamic {
        let uris = resolved.values().flatten().cloned().collect::<Vec<_>>();
        let plan = Arc::new(plan);
        prepare_relational_with_dynamic_inputs_in_session_controlled(
            &uris,
            policy,
            session,
            usize::try_from(metadata.bytes()).map_err(|_| failed("plan metadata overflow"))?,
            inputs,
            move |_| Ok((*plan).clone()),
            cancellation,
        )?
    } else {
        prepare_relational_with_schema_in_session_controlled(
            policy,
            session,
            |schemas| {
                inputs(schemas)?;
                Ok(plan)
            },
            cancellation,
        )?
    };
    #[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
    let operation = operation.with_preparation_sources(sources.preparations)?;
    #[cfg(feature = "vortex-write")]
    let operation = if let Some(spill) = &request.spill {
        operation.with_spill(spill.relational()?)?
    } else {
        operation
    };
    cancellation.check()?;
    Ok((operation, !sources.has_collections))
}

fn rebind(
    plan: &mut Plan,
    sources: &BTreeMap<String, Vec<DatasetUri>>,
    cancellation: &CancellationToken,
) -> Result<()> {
    cancellation.check()?;
    match plan {
        Plan::Scan(scan) => {
            let uris = sources
                .get(scan.source_uri.as_str())
                .ok_or_else(|| failed("native source was not declared"))?;
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
        Plan::Project(node) => rebind(&mut node.input, sources, cancellation)?,
        Plan::Filter(node) => rebind(&mut node.input, sources, cancellation)?,
        Plan::Sort(node) => rebind(&mut node.input, sources, cancellation)?,
        Plan::Limit(node) => rebind(&mut node.input, sources, cancellation)?,
        Plan::Aggregate(node) => rebind(&mut node.input, sources, cancellation)?,
        Plan::Window(node) => rebind(&mut node.input, sources, cancellation)?,
        Plan::Unary(node) => rebind(&mut node.input, sources, cancellation)?,
        Plan::Join(node) => {
            rebind(&mut node.left, sources, cancellation)?;
            rebind(&mut node.right, sources, cancellation)?;
        }
        Plan::Set(node) => {
            rebind(&mut node.left, sources, cancellation)?;
            rebind(&mut node.right, sources, cancellation)?;
        }
        Plan::Subquery(node) | Plan::CorrelatedSubquery(node) => {
            rebind(&mut node.input, sources, cancellation)?;
            rebind(&mut node.relation, sources, cancellation)?;
        }
        Plan::Outer => {}
        Plan::ExecutionResult(_) | Plan::DeferredSubquery(_) => {
            return Err(failed(
                "execution-scoped relation is not a reusable declaration",
            ));
        }
    }
    Ok(())
}
