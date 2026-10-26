//! SQL and typed declarations share source admission and native preparation.

use super::{
    CancellationToken, DatasetUri, PreparedVortexRelational, PublicWorkflowRouteRequest,
    ShardLoomError, Sources, failed, native_relational, native_vortex_materializing_policy,
};
use shardloom_vortex::{
    local_primitives::prepared_relational::{
        VortexRelationalPreparation, prepare_relational_with_dynamic_inputs_in_session_controlled,
        prepare_relational_with_schema_in_session_controlled, validate_session_policy,
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
    mut metadata: shardloom_exec::live_memory::MemoryLease,
    schema_aliases: Option<&BTreeMap<String, DatasetUri>>,
) -> Result<(PreparedVortexRelational, bool)> {
    cancellation.check()?;
    let policy = native_vortex_materializing_policy(request)?;
    validate_session_policy(policy, session)?;
    if declaration.recursive_depth() > shardloom_vortex::relational_query::NATIVE_RECURSION_LIMIT {
        return Err(failed("recursive plan exceeds 24 levels"));
    }
    if !session.shared_memory_pool().owns(&metadata)
        || metadata.bytes()
            < u64::try_from(declaration.metadata_bytes)
                .map_err(|_| failed("plan metadata overflow"))?
    {
        return Err(failed(
            "native declaration admission has a foreign owner or insufficient capacity",
        ));
    }
    let ResolvedDeclaration {
        sources,
        resolved,
        uses,
    } = resolve_sources(
        declaration,
        request,
        session,
        cancellation,
        &mut metadata,
        schema_aliases,
    )?;
    let memory_inputs = declaration
        .sources
        .keys()
        .filter_map(|leaf| {
            leaf.memory_input
                .as_ref()
                .map(|input| (leaf.path.clone(), input.clone()))
        })
        .collect::<Vec<_>>();
    let initialize = |schemas: &mut VortexRelationalPreparation<'_>| {
        for (path, input) in memory_inputs {
            schemas
                .register_memory_source(DatasetUri::new(path.to_string_lossy())?, |session| {
                    input.build(session)
                })?;
        }
        inputs(schemas)
    };
    let operation = if declaration.dynamic {
        let uris = uses
            .values()
            .map(|(uri, _)| uri.clone())
            .collect::<Vec<_>>();
        let declaration = declaration.clone();
        prepare_relational_with_dynamic_inputs_in_session_controlled(
            &uris,
            policy,
            session,
            usize::try_from(metadata.bytes()).map_err(|_| failed("plan metadata overflow"))?,
            initialize,
            move |schemas| {
                let token = schemas.cancellation_token();
                for (uri, count) in uses.values() {
                    if *count > 1 {
                        schemas.declare_source_reuse(uri)?;
                    }
                }
                let mut lowered = declaration.lower(schemas, &resolved, &token)?;
                native_relational::prune_bound_plan(&mut lowered.plan, schemas, &token)?;
                Ok(lowered.plan)
            },
            cancellation,
        )?
    } else {
        prepare_relational_with_schema_in_session_controlled(
            policy,
            session,
            |schemas| {
                initialize(schemas)?;
                let mut lowered = declaration.lower(schemas, &resolved, cancellation)?;
                native_relational::prune_bound_plan(&mut lowered.plan, schemas, cancellation)?;
                Ok(lowered.plan)
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

struct ResolvedDeclaration {
    sources: Sources,
    resolved: Arc<native_relational::ResolvedSources>,
    uses: BTreeMap<String, (DatasetUri, u8)>,
}

fn resolve_sources(
    declaration: &crate::native::NativeRelationDeclaration,
    request: &PublicWorkflowRouteRequest,
    session: &ResidentVortexSession,
    cancellation: &CancellationToken,
    metadata: &mut shardloom_exec::live_memory::MemoryLease,
    schema_aliases: Option<&BTreeMap<String, DatasetUri>>,
) -> Result<ResolvedDeclaration> {
    for reference in request.source_bindings.keys() {
        if !declaration
            .sources
            .keys()
            .any(|leaf| leaf.path.to_str() == Some(reference))
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
    let mut uses = BTreeMap::<String, (DatasetUri, u8)>::new();
    for (leaf, count) in &declaration.sources {
        cancellation.check()?;
        let uris = if let Some(uri) =
            schema_aliases.and_then(|aliases| aliases.get(leaf.path.to_string_lossy().as_ref()))
        {
            vec![uri.clone()]
        } else if leaf.memory_input.is_some() {
            vec![DatasetUri::new(leaf.path.to_string_lossy())?]
        } else {
            sources.resolve(leaf, request, Some(&session.shared_memory_pool()))?
        };
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
        for uri in &uris {
            let (_, total) = uses
                .entry(uri.as_str().to_owned())
                .or_insert_with(|| (uri.clone(), 0));
            *total = (*total + count).min(2);
        }
        resolved.insert(leaf.clone(), uris);
    }
    Ok(ResolvedDeclaration {
        sources,
        resolved: Arc::new(resolved),
        uses,
    })
}
