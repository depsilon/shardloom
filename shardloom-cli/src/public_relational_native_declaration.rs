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
    metadata: shardloom_exec::live_memory::MemoryLease,
    schema_aliases: Option<&BTreeMap<String, DatasetUri>>,
) -> Result<(PreparedVortexRelational, bool)> {
    prepare_native_with_sources(
        declaration,
        request,
        session,
        inputs,
        cancellation,
        metadata,
        schema_aliases,
        super::PublicSourcePreparations::default(),
    )
    .map(|(operation, reusable, _)| (operation, reusable))
}

#[allow(clippy::too_many_arguments)] // Existing admission plus externally held preparation identities.
pub(super) fn prepare_native_with_sources(
    declaration: &crate::native::NativeRelationDeclaration,
    request: &PublicWorkflowRouteRequest,
    session: &ResidentVortexSession,
    inputs: impl FnOnce(&mut VortexRelationalPreparation<'_>) -> Result<()>,
    cancellation: &CancellationToken,
    mut metadata: shardloom_exec::live_memory::MemoryLease,
    schema_aliases: Option<&BTreeMap<String, DatasetUri>>,
    preparations: super::PublicSourcePreparations,
) -> Result<(PreparedVortexRelational, bool, usize)> {
    cancellation.check()?;
    let policy = native_vortex_materializing_policy(request)?;
    validate_session_policy(policy, session)?;
    validate_admission(declaration, session, &metadata)?;
    let initialize_inputs = |schemas: &mut VortexRelationalPreparation<'_>| {
        for leaf in declaration
            .sources
            .keys()
            .filter(|leaf| leaf.memory_input.is_some())
        {
            let input = leaf.memory_input.as_ref().expect("filtered inline input");
            schemas.register_memory_source(
                DatasetUri::new(leaf.path.to_string_lossy())?,
                |session| input.build(session),
            )?;
        }
        inputs(schemas)
    };
    let mut sources = Sources::default();
    let operation = if declaration.dynamic {
        let resolved = resolve_sources(
            declaration,
            request,
            session,
            cancellation,
            &mut metadata,
            schema_aliases,
        )?;
        sources = resolved.sources;
        let uses = resolved.uses;
        let resolved = resolved.resolved;
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
            |schemas| {
                register_local_sources(&sources, schemas)?;
                initialize_inputs(schemas)
            },
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
                // Static callers historically establish their input ownership
                // and cancellation before file admission. Preserve that order
                // while sharing the same declaration/lowering path.
                initialize_inputs(schemas)?;
                cancellation.check()?;
                let resolved = resolve_sources(
                    declaration,
                    request,
                    session,
                    cancellation,
                    &mut metadata,
                    schema_aliases,
                )?;
                sources = resolved.sources;
                register_local_sources(&sources, schemas)?;
                let resolved = resolved.resolved;
                let mut lowered = declaration.lower(schemas, &resolved, cancellation)?;
                native_relational::prune_bound_plan(&mut lowered.plan, schemas, cancellation)?;
                Ok(lowered.plan)
            },
            cancellation,
        )?
    };
    let result = finish_source_preparation(operation, sources, request, preparations)?;
    cancellation.check()?;
    Ok(result)
}

fn validate_admission(
    declaration: &crate::native::NativeRelationDeclaration,
    session: &ResidentVortexSession,
    metadata: &shardloom_exec::live_memory::MemoryLease,
) -> Result<()> {
    if declaration.recursive_depth() > shardloom_vortex::relational_query::NATIVE_RECURSION_LIMIT {
        return Err(failed("recursive plan exceeds 24 levels"));
    }
    if !session.shared_memory_pool().owns(metadata)
        || metadata.bytes()
            < u64::try_from(declaration.metadata_bytes)
                .map_err(|_| failed("plan metadata overflow"))?
    {
        return Err(failed(
            "native declaration admission has a foreign owner or insufficient capacity",
        ));
    }
    Ok(())
}

fn finish_source_preparation(
    operation: PreparedVortexRelational,
    sources: Sources,
    request: &PublicWorkflowRouteRequest,
    preparations: super::PublicSourcePreparations,
) -> Result<(PreparedVortexRelational, bool, usize)> {
    let normalized = sources.normalized
        + request
            .source_bindings
            .values()
            .filter(|binding| binding.memory_input.is_some())
            .count();
    #[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
    let operation = {
        let mut retained = sources.preparations;
        retained.extend(preparations.sources);
        operation.with_preparation_sources(retained)?
    };
    #[cfg(not(all(feature = "vortex-write", feature = "universal-format-io")))]
    let _ = preparations;
    #[cfg(feature = "vortex-write")]
    let operation = if let Some(spill) = &request.spill {
        operation.with_spill(spill.relational()?)?
    } else {
        operation
    };
    Ok((operation, !sources.has_collections, normalized))
}

fn register_local_sources(
    sources: &Sources,
    schemas: &mut VortexRelationalPreparation<'_>,
) -> Result<()> {
    #[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
    for (uri, source) in &sources.local {
        schemas.register_local_source(uri.clone(), Arc::clone(source))?;
    }
    #[cfg(not(all(feature = "vortex-write", feature = "universal-format-io")))]
    let _ = (sources, schemas);
    Ok(())
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
    let declared = declaration
        .sources
        .keys()
        .map(|leaf| super::declared_path(leaf, request))
        .collect::<Result<std::collections::BTreeSet<_>>>()?;
    for reference in request.source_bindings.keys() {
        if !declared.contains(std::path::Path::new(reference)) {
            return Err(failed(
                "source declaration is not referenced by the native plan",
            ));
        }
    }
    let mut sources = Sources {
        cancellation: cancellation.clone(),
        float_policy: session.float_policy(),
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
