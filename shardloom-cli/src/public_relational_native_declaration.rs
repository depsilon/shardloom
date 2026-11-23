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
use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    sync::Arc,
};

type Result<T> = std::result::Result<T, ShardLoomError>;

pub(crate) type SourceCacheFn<'a> = &'a mut dyn FnMut(
    &DatasetUri,
) -> Result<
    Option<shardloom_vortex::resident_session::PreparedVortexSource>,
>;

pub(super) fn mark_seeded_uri(
    already_seeded: &mut BTreeSet<PathBuf>,
    uri: &DatasetUri,
) -> Result<bool> {
    let Some(path) =
        super::super::super::PublicExecutionSession::resolve_local_vortex_path_for_uri(uri)
    else {
        return Ok(true);
    };
    let abs = std::path::absolute(path).map_err(|error| failed(&error.to_string()))?;
    Ok(already_seeded.insert(abs))
}

fn seed_cached_vortex_sources(
    sources: &Sources,
    request: &PublicWorkflowRouteRequest,
    preparations: &super::PublicSourcePreparations,
    already_seeded: &mut BTreeSet<PathBuf>,
    schemas: &mut VortexRelationalPreparation<'_>,
    cache: &mut Option<SourceCacheFn<'_>>,
) -> Result<()> {
    let Some(resolve) = cache.as_deref_mut() else {
        return Ok(());
    };
    if !preparations.sources.is_empty()
        || !sources.preparations.is_empty()
        || sources.has_collections
        || request.execution_policy == "prepare_once"
    {
        return Ok(());
    }
    for uri in &sources.single_file_vortex_uris {
        if !mark_seeded_uri(already_seeded, uri)? {
            continue;
        }
        if let Some(cached) = resolve(uri)? {
            schemas.register_prepared_source(uri, cached)?;
        }
    }
    Ok(())
}

/// Primitive payloads share the same source resolver and native plan owner.
pub(crate) fn prepare_primitive_in_session(
    request: &PublicWorkflowRouteRequest,
    session: &ResidentVortexSession,
    cancellation: &CancellationToken,
) -> Result<(PreparedVortexRelational, bool, usize)> {
    prepare_primitive_in_session_with_cache(request, session, cancellation, None)
}

pub(crate) fn prepare_primitive_in_session_with_cache(
    request: &PublicWorkflowRouteRequest,
    session: &ResidentVortexSession,
    cancellation: &CancellationToken,
    mut cache: Option<SourceCacheFn<'_>>,
) -> Result<(PreparedVortexRelational, bool, usize)> {
    use shardloom_vortex::{
        VortexQueryPrimitiveKind as Kind,
        relational_query::{VortexRelationalPlan, VortexRelationalScan},
    };
    let workflow = request;
    let uri = DatasetUri::new(
        request
            .input_uri
            .as_deref()
            .ok_or_else(|| failed("primitive source is absent"))?,
    )?;
    let primitive = super::super::super::normalized_vortex_primitive(request)
        .ok_or_else(|| failed("primitive declaration is absent"))?;
    let (request, _) =
        super::super::super::native_vortex_request_and_arg(request, primitive, uri.clone())?;
    let declaration = crate::native::NativeRelationDeclaration::new(VortexRelationalPlan::Scan(
        VortexRelationalScan {
            source_uri: uri,
            projection: if matches!(
                request.kind,
                Kind::ProjectColumns | Kind::FilterPredicate | Kind::FilterAndProject
            ) {
                request.projection.clone()
            } else {
                shardloom_plan::ProjectionRequest::All
            },
            predicate: request.predicate.clone(),
        },
    ))?;
    let policy = native_vortex_materializing_policy(workflow)?;
    validate_session_policy(policy, session)?;
    let declaration_bytes = declaration
        .metadata_bytes
        .checked_add(
            format!("{request:?}")
                .len()
                .checked_mul(128)
                .ok_or_else(|| failed("primitive metadata overflow"))?,
        )
        .ok_or_else(|| failed("primitive metadata overflow"))?;
    let mut metadata = session.reserve_input_scratch(declaration_bytes as u64)?;
    let resolved = resolve_sources(
        &declaration,
        workflow,
        session,
        cancellation,
        &mut metadata,
        None,
    )?;
    let sources = resolved.sources;
    let uris = resolved
        .uses
        .values()
        .map(|(uri, _)| uri.clone())
        .collect::<Vec<_>>();
    let mut request = request;
    request.source_uri = uris.first().cloned();
    let empty_preparations = super::PublicSourcePreparations::default();
    let mut already_seeded = BTreeSet::new();
    let operation = prepare_relational_with_dynamic_inputs_in_session_controlled(
        &uris,
        policy,
        session,
        usize::try_from(metadata.bytes()).map_err(|_| failed("primitive metadata overflow"))?,
        |schemas| {
            register_local_sources(&sources, schemas)?;
            seed_cached_vortex_sources(
                &sources,
                workflow,
                &empty_preparations,
                &mut already_seeded,
                schemas,
                &mut cache,
            )
        },
        move |schemas| {
            let token = schemas.cancellation_token();
            let scan = declaration.lower(schemas, &resolved.resolved, &token)?.plan;
            let mut plan = schemas.primitive_plan(&request, scan)?;
            native_relational::prune_bound_plan(&mut plan, schemas, &token)?;
            Ok(plan)
        },
        cancellation,
    )?;
    finish_source_preparation(operation, sources, workflow, empty_preparations)
}

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
        None,
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
    metadata: shardloom_exec::live_memory::MemoryLease,
    schema_aliases: Option<&BTreeMap<String, DatasetUri>>,
    preparations: super::PublicSourcePreparations,
    sql_statement: Option<&str>,
) -> Result<(PreparedVortexRelational, bool, usize)> {
    prepare_native_with_sources_and_cache(
        declaration,
        request,
        session,
        inputs,
        cancellation,
        metadata,
        schema_aliases,
        preparations,
        sql_statement,
        None,
        None,
    )
}

#[allow(clippy::too_many_arguments)] // Existing admission plus externally held preparation identities.
pub(super) fn prepare_native_with_sources_and_cache(
    declaration: &crate::native::NativeRelationDeclaration,
    request: &PublicWorkflowRouteRequest,
    session: &ResidentVortexSession,
    inputs: impl FnOnce(&mut VortexRelationalPreparation<'_>) -> Result<()>,
    cancellation: &CancellationToken,
    mut metadata: shardloom_exec::live_memory::MemoryLease,
    schema_aliases: Option<&BTreeMap<String, DatasetUri>>,
    preparations: super::PublicSourcePreparations,
    sql_statement: Option<&str>,
    preseeded_uri: Option<&DatasetUri>,
    mut cache: Option<SourceCacheFn<'_>>,
) -> Result<(PreparedVortexRelational, bool, usize)> {
    cancellation.check()?;
    let policy = native_vortex_materializing_policy(request)?;
    validate_session_policy(policy, session)?;
    validate_admission(declaration, session, &metadata)?;
    let initialize_inputs = |schemas: &mut VortexRelationalPreparation<'_>| {
        register_declaration_memory_inputs(declaration, schemas)?;
        inputs(schemas)
    };
    let mut sources = Sources::default();
    let mut already_seeded = BTreeSet::new();
    if let Some(uri) = preseeded_uri {
        let _ = mark_seeded_uri(&mut already_seeded, uri)?;
    }
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
                initialize_inputs(schemas)?;
                seed_cached_vortex_sources(
                    &sources,
                    request,
                    &preparations,
                    &mut already_seeded,
                    schemas,
                    &mut cache,
                )
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
        prepare_static_native_operation(
            declaration,
            request,
            session,
            policy,
            initialize_inputs,
            cancellation,
            &mut metadata,
            schema_aliases,
            &preparations,
            sql_statement,
            &mut sources,
            &mut already_seeded,
            &mut cache,
        )?
    };
    let result = finish_source_preparation(operation, sources, request, preparations)?;
    cancellation.check()?;
    Ok(result)
}

fn register_declaration_memory_inputs(
    declaration: &crate::native::NativeRelationDeclaration,
    schemas: &mut VortexRelationalPreparation<'_>,
) -> Result<()> {
    for leaf in declaration
        .sources
        .keys()
        .filter(|leaf| leaf.memory_input.is_some())
    {
        let input = leaf.memory_input.as_ref().expect("filtered inline input");
        schemas
            .register_memory_source(DatasetUri::new(leaf.path.to_string_lossy())?, |session| {
                input.build(session)
            })?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn prepare_static_native_operation(
    declaration: &crate::native::NativeRelationDeclaration,
    request: &PublicWorkflowRouteRequest,
    session: &ResidentVortexSession,
    policy: shardloom_vortex::VortexLocalPrimitiveExecutionPolicy,
    initialize_inputs: impl FnOnce(&mut VortexRelationalPreparation<'_>) -> Result<()>,
    cancellation: &CancellationToken,
    metadata: &mut shardloom_exec::live_memory::MemoryLease,
    schema_aliases: Option<&BTreeMap<String, DatasetUri>>,
    preparations: &super::PublicSourcePreparations,
    sql_statement: Option<&str>,
    sources: &mut Sources,
    already_seeded: &mut BTreeSet<PathBuf>,
    cache: &mut Option<SourceCacheFn<'_>>,
) -> Result<PreparedVortexRelational> {
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
                metadata,
                schema_aliases,
            )?;
            *sources = resolved.sources;
            register_local_sources(sources, schemas)?;
            let resolved = resolved.resolved;
            let named = super::named_file_sql_declaration(
                sql_statement,
                request,
                declaration,
                sources,
                session,
                schemas,
                cancellation,
                metadata,
                already_seeded,
                cache,
            )?;
            seed_cached_vortex_sources(
                sources,
                request,
                preparations,
                already_seeded,
                schemas,
                cache,
            )?;
            let mut lowered =
                named
                    .as_ref()
                    .unwrap_or(declaration)
                    .lower(schemas, &resolved, cancellation)?;
            native_relational::prune_bound_plan(&mut lowered.plan, schemas, cancellation)?;
            Ok(lowered.plan)
        },
        cancellation,
    )
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
