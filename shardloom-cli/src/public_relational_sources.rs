//! Every parsed source leaf normalizes once; strings in SQL values are untouched.

use super::super::PublicSourcePreparations;
use super::super::{
    declared_sql_source_identifier_matches, infer_input_format_from_ref,
    native_vortex_input_binding_for_uri,
};
use super::{
    PreparedVortexRelational, PublicWorkflowRouteRequest, ShardLoomError, native_relational,
    native_vortex_materializing_policy,
};
use shardloom_core::DatasetUri;
use shardloom_exec::compute_pool::CancellationToken;
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

#[path = "public_relational_native_declaration.rs"]
mod typed;
pub(crate) use typed::prepare_native_in_session;

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
use super::super::{
    PreparationFacadeError, auto_prepared_vortex_target_path_with_schema,
    prepare_local_source_for_public_workflow_controlled,
    public_workflow_effective_resource_envelope,
};

#[cfg(test)]
fn prepare_with_source(
    statement: &str,
    request: &PublicWorkflowRouteRequest,
    source: Option<shardloom_vortex::resident_session::PreparedVortexSource>,
    preparations: PublicSourcePreparations,
) -> Result<(PreparedVortexRelational, usize), ShardLoomError> {
    let policy = native_vortex_materializing_policy(request)?;
    let session = shardloom_vortex::resident_session::ResidentVortexSession::new(
        policy.resource_envelope.memory_budget_bytes,
        policy.max_parallelism,
    )?;
    prepare_with_source_in_session(
        statement,
        request,
        source,
        preparations,
        &session,
        &CancellationToken::default(),
    )
}

/// Embedded callers use the same source resolver and SQL lowerer while retaining
/// their existing session. Native input handles never pass through JSON cells.
pub(crate) fn prepare_in_session(
    statement: &str,
    request: &PublicWorkflowRouteRequest,
    session: &shardloom_vortex::resident_session::ResidentVortexSession,
    inputs: impl FnOnce(
        &mut shardloom_vortex::local_primitives::prepared_relational::VortexRelationalPreparation<
            '_,
        >,
    ) -> Result<(), ShardLoomError>,
    cancellation: &CancellationToken,
) -> Result<PreparedVortexRelational, ShardLoomError> {
    prepare_in_session_with_reuse(statement, request, session, inputs, cancellation)
        .map(|(operation, _)| operation)
}

/// Resolve reuse from the source admission that prepared the operation. A second
/// frontend parse must not decide which already-admitted provider can execute.
pub(crate) fn prepare_in_session_with_reuse(
    statement: &str,
    request: &PublicWorkflowRouteRequest,
    session: &shardloom_vortex::resident_session::ResidentVortexSession,
    inputs: impl FnOnce(
        &mut shardloom_vortex::local_primitives::prepared_relational::VortexRelationalPreparation<
            '_,
        >,
    ) -> Result<(), ShardLoomError>,
    cancellation: &CancellationToken,
) -> Result<(PreparedVortexRelational, bool), ShardLoomError> {
    cancellation.check()?;
    let statement = super::super::resident_sql::typed_statement(statement, request, false);
    let statement = statement.as_ref();
    prepare_sql_in_session(
        statement,
        request,
        session,
        inputs,
        cancellation,
        PublicSourcePreparations::default(),
    )
    .map(|(operation, reusable, _)| (operation, reusable))
}

fn prepare_sql_in_session(
    statement: &str,
    request: &PublicWorkflowRouteRequest,
    session: &shardloom_vortex::resident_session::ResidentVortexSession,
    inputs: impl FnOnce(
        &mut shardloom_vortex::local_primitives::prepared_relational::VortexRelationalPreparation<
            '_,
        >,
    ) -> Result<(), ShardLoomError>,
    cancellation: &CancellationToken,
    preparations: PublicSourcePreparations,
) -> Result<(PreparedVortexRelational, bool, usize), ShardLoomError> {
    cancellation.check()?;
    let bytes = statement
        .len()
        .checked_mul(1024)
        .and_then(|n| n.checked_add(65_536))
        .ok_or_else(|| failed("SQL parser metadata size overflow"))?;
    let mut metadata = session.reserve_input_scratch(bytes as u64)?;
    validate_bindings(statement, request)?;
    let declaration = crate::native::NativeRelationDeclaration::sql(statement, Vec::new())?;
    metadata.resize((bytes.max(declaration.metadata_bytes)) as u64)?;
    typed::prepare_native_with_sources(
        &declaration,
        request,
        session,
        inputs,
        cancellation,
        metadata,
        None,
        preparations,
    )
}

/// General commands reuse the same owner as embedded plans and retained results.
// Preparation owners transfer into the plan when format preparation is enabled.
#[cfg_attr(
    not(all(feature = "vortex-write", feature = "universal-format-io")),
    allow(clippy::needless_pass_by_value)
)]
pub(super) fn prepare_with_source_in_session(
    statement: &str,
    request: &PublicWorkflowRouteRequest,
    source: Option<shardloom_vortex::resident_session::PreparedVortexSource>,
    preparations: PublicSourcePreparations,
    session: &shardloom_vortex::resident_session::ResidentVortexSession,
    cancellation: &CancellationToken,
) -> Result<(PreparedVortexRelational, usize), ShardLoomError> {
    cancellation.check()?;
    let statement = super::super::resident_sql::typed_statement_for_source(
        statement,
        request,
        source.as_ref(),
    )?;
    let statement = statement.as_ref();
    let (operation, _, normalized) = prepare_sql_in_session(
        statement,
        request,
        session,
        |schemas| {
            if let Some(source) = source {
                let uri = request
                    .input_uri
                    .as_ref()
                    .ok_or_else(|| failed("source URI is absent"))?;
                schemas.register_prepared_source(&DatasetUri::new(uri)?, source)?;
            }
            register_memory_inputs(schemas, request, &mut |_, input, owner| {
                cancellation.check()?;
                let result = input.build(owner)?;
                cancellation.check()?;
                Ok(result)
            })
        },
        cancellation,
        preparations,
    )?;
    Ok((operation, normalized))
}

/// Establish the input control owner before any source resolution. Dynamic SQL
/// resolves its source declarations before registering inputs, so initialization
/// inside the registration closure would miss compatibility preparation.
pub(super) fn prepare_with_input_adapter(
    statement: &str,
    request: &PublicWorkflowRouteRequest,
    initialize: impl FnOnce(
        &shardloom_vortex::resident_session::ResidentVortexSession,
    ) -> Result<(), ShardLoomError>,
    mut build: impl FnMut(
        &str,
        &crate::native_memory_input::MemoryInput,
        &shardloom_vortex::resident_session::ResidentVortexSession,
    ) -> Result<
        shardloom_vortex::resident_memory_source::ResidentMemorySource,
        ShardLoomError,
    >,
    cancellation: &CancellationToken,
) -> Result<(PreparedVortexRelational, usize), ShardLoomError> {
    cancellation.check()?;
    validate_bindings(statement, request)?;
    let policy = native_vortex_materializing_policy(request)?;
    let session = shardloom_vortex::resident_session::ResidentVortexSession::new(
        policy.resource_envelope.memory_budget_bytes,
        policy.max_parallelism,
    )?;
    cancellation.check()?;
    initialize(&session)?;
    cancellation.check()?;
    let (operation, _, normalized) = prepare_sql_in_session(
        statement,
        request,
        &session,
        |schemas| {
            register_memory_inputs(schemas, request, &mut |uri, input, owner| {
                cancellation.check()?;
                let result = build(uri, input, owner)?;
                cancellation.check()?;
                Ok(result)
            })
        },
        cancellation,
        PublicSourcePreparations::default(),
    )?;
    Ok((operation, normalized))
}

fn register_memory_inputs(
    schemas: &mut shardloom_vortex::local_primitives::prepared_relational::VortexRelationalPreparation<'_>,
    request: &PublicWorkflowRouteRequest,
    build: &mut impl FnMut(
        &str,
        &crate::native_memory_input::MemoryInput,
        &shardloom_vortex::resident_session::ResidentVortexSession,
    ) -> Result<
        shardloom_vortex::resident_memory_source::ResidentMemorySource,
        ShardLoomError,
    >,
) -> Result<(), ShardLoomError> {
    for (uri, binding) in &request.source_bindings {
        if let Some(input) = &binding.memory_input {
            if matches!(
                input,
                crate::native_memory_input::MemoryInput::Batches {
                    streaming: true,
                    ..
                }
            ) {
                schemas.register_batch_source(DatasetUri::new(uri)?, |session| {
                    build(uri, input, session)
                })?;
            } else {
                schemas.register_memory_source(DatasetUri::new(uri)?, |session| {
                    build(uri, input, session)
                })?;
            }
        }
    }
    Ok(())
}

pub(super) fn validate_bindings(
    statement: &str,
    request: &PublicWorkflowRouteRequest,
) -> Result<(), ShardLoomError> {
    if request.source_bindings.is_empty() {
        return Ok(());
    }
    let paths = native_relational::source_leaves(statement)?
        .iter()
        .map(|leaf| declared_path(leaf, request))
        .collect::<Result<std::collections::BTreeSet<_>, _>>()?;
    for (uri, binding) in &request.source_bindings {
        if !paths.contains(Path::new(uri)) {
            return Err(failed(
                "source declaration is not referenced by the SQL statement",
            ));
        }
        if request.input_uri.as_deref() == Some(uri.as_str())
            && (request
                .input_format
                .as_ref()
                .is_some_and(|format| format != &binding.input_format)
                || request
                    .source_schema
                    .as_ref()
                    .is_some_and(|schema| binding.source_schema.as_ref() != Some(schema)))
        {
            return Err(failed(
                "source declaration conflicts with the primary input format or schema",
            ));
        }
    }
    Ok(())
}

pub(super) fn normalization_required(
    statement: &str,
    request: &PublicWorkflowRouteRequest,
) -> Result<bool, ShardLoomError> {
    let mut required = false;
    for leaf in native_relational::source_leaves(statement)? {
        if leaf.memory_input.is_some() {
            continue;
        }
        let path = declared_path(&leaf, request)?;
        let raw = path
            .to_str()
            .ok_or_else(|| failed("source paths must be UTF8"))?;
        let format = declared_format(raw, request)
            .ok_or_else(|| failed("source format is not declared or recognized"))?;
        required |= format != "vortex" && format != "memory";
    }
    Ok(required)
}

/// Resolve only parsed, unquoted identifiers against explicit input contracts.
/// Quoted source paths and SQL values are never rewritten or rebound.
fn declared_path(
    leaf: &native_relational::ParsedRelationLeaf,
    request: &PublicWorkflowRouteRequest,
) -> Result<PathBuf, ShardLoomError> {
    if !leaf.declared_identifier {
        return Ok(leaf.path.clone());
    }
    let name = leaf
        .path
        .to_str()
        .ok_or_else(|| failed("source paths must be UTF8"))?;
    let candidates = request
        .input_uri
        .as_deref()
        .into_iter()
        .chain(request.source_bindings.keys().map(String::as_str))
        .filter(|uri| *uri == name || declared_sql_source_identifier_matches(name, uri))
        .collect::<std::collections::BTreeSet<_>>();
    if candidates.len() > 1 {
        return Err(failed(
            "declared SQL source identifier is ambiguous; use an exact quoted source path",
        ));
    }
    Ok(candidates
        .first()
        .map_or_else(|| leaf.path.clone(), PathBuf::from))
}

fn declared_format<'a>(source: &str, request: &'a PublicWorkflowRouteRequest) -> Option<&'a str> {
    request
        .source_bindings
        .get(source)
        .map(|binding| binding.input_format.as_str())
        .or_else(|| {
            (request.input_uri.as_deref() == Some(source))
                .then_some(request.input_format.as_deref())
                .flatten()
        })
        .or_else(|| infer_input_format_from_ref(source))
}

/// File collections are rebound on each execution so adding, removing or
/// reordering a manifest entry cannot silently reuse an earlier declaration.
pub(crate) fn has_file_collection(
    statement: &str,
    request: &PublicWorkflowRouteRequest,
    memory: &shardloom_exec::live_memory::LiveMemoryPool,
    cancellation: &CancellationToken,
) -> Result<bool, ShardLoomError> {
    cancellation.check()?;
    for leaf in native_relational::source_leaves(statement)? {
        if leaf.memory_input.is_some() {
            continue;
        }
        let path = declared_path(&leaf, request)?;
        let raw = path
            .to_str()
            .ok_or_else(|| failed("source paths must be UTF8"))?;
        if declared_format(raw, request) == Some("vortex")
            && super::super::input_binding::is_collection(raw, memory, cancellation)?
        {
            return Ok(true);
        }
    }
    Ok(false)
}

#[derive(Default)]
struct Sources {
    bound: BTreeMap<PathBuf, Vec<DatasetUri>>,
    metadata: Option<shardloom_exec::live_memory::MemoryLease>,
    normalized: usize,
    has_collections: bool,
    cancellation: CancellationToken,
    #[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
    local: Vec<(
        DatasetUri,
        std::sync::Arc<shardloom_vortex::universal_format_io::demanded::LocalColumnarSource>,
    )>,
    #[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
    preparations:
        Vec<std::sync::Arc<shardloom_vortex::prepared_source_binding::LocalPreparationIdentity>>,
}

impl Sources {
    fn resolve(
        &mut self,
        leaf: &native_relational::ParsedRelationLeaf,
        request: &PublicWorkflowRouteRequest,
        memory: Option<&shardloom_exec::live_memory::LiveMemoryPool>,
    ) -> Result<Vec<DatasetUri>, ShardLoomError> {
        self.cancellation.check()?;
        let memory = memory
            .ok_or_else(|| failed("source expansion requires the native preparation owner"))?;
        let path = declared_path(leaf, request)?;
        if let Some(uri) = self.bound.get(&path) {
            let bytes = source_metadata_bytes(&path, uri.iter().map(DatasetUri::as_str))?;
            self.reserve_metadata(memory, bytes)?;
            let uri = self.bound.get(&path).expect("source remains cached");
            return Ok(uri.clone());
        }
        if self.bound.len() >= 128 {
            return Err(failed("SQL exceeds 128 source leaves"));
        }
        let raw = path
            .to_str()
            .ok_or_else(|| failed("source paths must be UTF8"))?;
        let format = declared_format(raw, request)
            .ok_or_else(|| failed("source format is not declared or recognized"))?;
        let uris = if format == "vortex" {
            let binding = native_vortex_input_binding_for_uri(raw, memory, &self.cancellation)?;
            self.has_collections |= binding.mode != "single_file";
            self.reserve_metadata(
                memory,
                source_metadata_bytes(&path, binding.sources.iter().map(String::as_str))?,
            )?;
            binding
                .sources
                .into_iter()
                .map(DatasetUri::new)
                .collect::<Result<Vec<_>, _>>()?
        } else if format == "memory" {
            self.reserve_metadata(memory, source_metadata_bytes(&path, std::iter::once(raw))?)?;
            vec![DatasetUri::new(raw)?]
        } else {
            let uri = self.prepare_compatibility(raw, format, request, Some(memory))?;
            self.reserve_metadata(
                memory,
                source_metadata_bytes(&path, std::iter::once(uri.as_str()))?,
            )?;
            vec![uri]
        };
        self.cancellation.check()?;
        if self.bound.values().map(Vec::len).sum::<usize>() + uris.len() > 128 {
            return Err(failed("SQL exceeds 128 source files"));
        }
        self.bound.insert(path, uris.clone());
        Ok(uris)
    }

    fn reserve_metadata(
        &mut self,
        memory: &shardloom_exec::live_memory::LiveMemoryPool,
        bytes: u64,
    ) -> Result<(), ShardLoomError> {
        if self.metadata.is_none() {
            self.metadata = Some(memory.reserve(0)?);
        }
        let lease = self.metadata.as_mut().expect("metadata owner initialized");
        lease.resize(
            lease
                .bytes()
                .checked_add(bytes)
                .ok_or_else(|| failed("source metadata overflow"))?,
        )
    }

    #[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
    fn prepare_compatibility(
        &mut self,
        source: &str,
        format: &str,
        request: &PublicWorkflowRouteRequest,
        memory: Option<&shardloom_exec::live_memory::LiveMemoryPool>,
    ) -> Result<DatasetUri, ShardLoomError> {
        let schema = request
            .source_bindings
            .get(source)
            .and_then(|binding| binding.source_schema.as_deref())
            .or_else(|| {
                (request.input_uri.as_deref() == Some(source))
                    .then_some(request.source_schema.as_deref())
                    .flatten()
            });
        let target = auto_prepared_vortex_target_path_with_schema(source, format, schema);
        if format == "parquet"
            // Directory intake and explicit schema validation retain their
            // existing full-preparation contract in this first file adapter.
            && schema.is_none()
            && !Path::new(source).is_dir()
            && request.execution_policy != "prepare_once"
            && request
                .source_fingerprint_policy
                .as_deref()
                .is_none_or(|policy| policy == "metadata_only")
        {
            let memory =
                memory.ok_or_else(|| failed("source metadata requires the native owner"))?;
            match std::fs::metadata(&target) {
                Ok(_) => {
                    // Reuse is read-only: a missing/replaced artifact after this
                    // check fails; it never triggers a fresh full preparation.
                    use shardloom_vortex::prepared_source_binding::{
                        local_preparation_binding_with_schema_controlled,
                        local_preparation_identity_controlled,
                    };
                    let expected = local_preparation_binding_with_schema_controlled(
                        Path::new(source),
                        format,
                        "metadata_only",
                        schema,
                        &self.cancellation,
                    )?;
                    self.preparations.push(std::sync::Arc::new(
                        local_preparation_identity_controlled(
                            &target,
                            &expected,
                            memory,
                            &self.cancellation,
                        )?,
                    ));
                    self.normalized += 1;
                    return DatasetUri::new(target.to_string_lossy().into_owned());
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(failed(&format!("prepared source metadata: {error}"))),
            }
            let uri = DatasetUri::new(source)?;
            let descriptor =
                shardloom_vortex::universal_format_io::demanded::LocalColumnarSource::admit(
                    Path::new(source),
                    format,
                    memory,
                    &self.cancellation,
                )?;
            self.local.push((uri.clone(), descriptor));
            self.normalized += 1;
            return Ok(uri);
        }
        let resources = public_workflow_effective_resource_envelope(request)?;
        let preparation = prepare_local_source_for_public_workflow_controlled(
            source,
            format,
            &target,
            false,
            resources,
            memory,
            request.source_fingerprint_policy.as_deref(),
            schema,
            &self.cancellation,
        )
        .map_err(|error| match error {
            PreparationFacadeError::Runtime(error) => error,
            PreparationFacadeError::FeatureGated => {
                failed("compatibility preparation is feature gated")
            }
        })?;
        self.preparations.push(
            preparation
                .identity
                .ok_or_else(|| failed("preparation source generation proof is absent"))?,
        );
        self.normalized += 1;
        DatasetUri::new(preparation.target_path.to_string_lossy().into_owned())
    }

    #[cfg(not(all(feature = "vortex-write", feature = "universal-format-io")))]
    #[allow(clippy::unused_self)]
    fn prepare_compatibility(
        &mut self,
        _source: &str,
        _format: &str,
        _request: &PublicWorkflowRouteRequest,
        _memory: Option<&shardloom_exec::live_memory::LiveMemoryPool>,
    ) -> Result<DatasetUri, ShardLoomError> {
        Err(failed(
            "compatibility normalization requires vortex-write and universal-format-io",
        ))
    }
}

// Cover the resolver's cache, returned URI copies and the temporary lowering
// declaration until the prepared plan retains its own metadata reservation.
fn source_metadata_bytes<'a>(
    path: &Path,
    mut sources: impl Iterator<Item = &'a str>,
) -> Result<u64, ShardLoomError> {
    let bytes = sources
        .try_fold(path.as_os_str().len(), |sum, source| {
            sum.checked_add(source.len())
                .and_then(|n| n.checked_add(std::mem::size_of::<DatasetUri>()))
        })
        .and_then(|n| n.checked_mul(8))
        .and_then(|n| n.checked_add(4096))
        .and_then(|n| u64::try_from(n).ok())
        .ok_or_else(|| failed("source metadata overflow"))?;
    Ok(bytes)
}

fn failed(message: &str) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "native SQL source preparation: {message}; no fallback execution was attempted"
    ))
}

#[cfg(all(test, feature = "vortex-write", feature = "universal-format-io", unix))]
#[path = "public_relational_source_demand_tests.rs"]
mod source_demand_tests;

#[cfg(test)]
mod tests {
    use super::super::super::PublicSourceBinding;
    use super::*;

    #[test]
    fn batch_preparation_cancellation_precedes_static_and_dynamic_source_resolution() {
        let mut request = PublicWorkflowRouteRequest::new("sql".into());
        request.resources.memory_bytes = Some((8_u64 << 20).to_string());
        request.resources.max_parallelism = Some("1".into());
        request.input_uri = Some("/missing/batch-source-cancel.csv".into());
        request.input_format = Some("csv".into());
        request.source_schema = Some("value:int64".into());
        request.bounded = true;
        for statement in [
            "SELECT value FROM '/missing/batch-source-cancel.csv'",
            r#"SELECT * FROM PIVOT((SELECT value AS entity,'a' AS category,value AS amount FROM '/missing/batch-source-cancel.csv'), '{"index":"entity","columns":"category","values":"amount","aggregate":"sum"}') AS p"#,
        ] {
            for already_cancelled in [false, true] {
                let token = CancellationToken::default();
                if already_cancelled {
                    token.cancel();
                }
                let mut initialized = false;
                let mut memory = None;
                let error = prepare_with_input_adapter(
                    statement,
                    &request,
                    |session| {
                        initialized = true;
                        memory = Some(session.shared_memory_pool());
                        token.cancel();
                        Ok(())
                    },
                    |_, _, _| panic!("cancelled preparation must not demand producer input"),
                    &token,
                )
                .err()
                .expect("cancel before opening or preparing an absent compatibility source");
                assert!(error.to_string().contains("cancelled"), "{error}");
                assert_eq!(initialized, !already_cancelled);
                if let Some(memory) = memory {
                    assert_eq!(memory.snapshot().reserved_bytes, 0);
                }
            }
        }
    }

    #[test]
    fn batch_preparation_releases_built_input_when_the_producer_cancels() {
        let mut request = PublicWorkflowRouteRequest::new("sql".into());
        request.resources.memory_bytes = Some((8_u64 << 20).to_string());
        request.resources.max_parallelism = Some("1".into());
        request.input_uri = Some("memory://cancel-built".into());
        request.input_format = Some("memory".into());
        request.bounded = true;
        request.source_bindings = super::super::super::parse_public_source_bindings(
            &serde_json::json!({"memory://cancel-built":{"input_format":"memory","memory_input":{"kind":"range","start":1,"end":4,"step":1,"column":"n"}}}).to_string(),
        ).unwrap();
        let token = CancellationToken::default();
        let mut memory = None;
        let mut builds = 0;
        let error = prepare_with_input_adapter(
            "SELECT n FROM 'memory://cancel-built'",
            &request,
            |session| {
                memory = Some(session.shared_memory_pool());
                Ok(())
            },
            |_, input, session| {
                builds += 1;
                let built = input.build(session)?;
                token.cancel();
                Ok(built)
            },
            &token,
        )
        .err()
        .expect("a producer cancellation must not publish the prepared plan");
        assert!(error.to_string().contains("cancelled"), "{error}");
        assert_eq!(builds, 1);
        assert_eq!(memory.unwrap().snapshot().reserved_bytes, 0);
    }

    #[test]
    fn native_source_resolution_observes_cancellation_after_input_registration() {
        let mut request = PublicWorkflowRouteRequest::new("sql".into());
        request.resources.memory_bytes = Some((8_u64 << 20).to_string());
        request.resources.max_parallelism = Some("1".into());
        request.input_uri = Some("/missing/native-source-cancel.csv".into());
        request.input_format = Some("csv".into());
        request.source_schema = Some("value:int64".into());
        request.bounded = true;
        let session =
            shardloom_vortex::resident_session::ResidentVortexSession::new(8 << 20, 1).unwrap();
        let retained = session.shared_memory_pool().reserve(17).unwrap();
        let token = CancellationToken::default();
        let error = prepare_in_session(
            "SELECT value FROM '/missing/native-source-cancel.csv'",
            &request,
            &session,
            |_| {
                token.cancel();
                Ok(())
            },
            &token,
        )
        .err()
        .expect("cancel before source resolution");
        assert!(error.to_string().contains("cancelled"), "{error}");
        assert_eq!(session.shared_memory_pool().snapshot().reserved_bytes, 17);
        drop(retained);
        assert_eq!(session.shared_memory_pool().snapshot().reserved_bytes, 0);
    }

    #[cfg(all(feature = "vortex-write", feature = "universal-format-io", unix))]
    #[test]
    #[allow(clippy::too_many_lines)]
    fn compatibility_preparation_competes_with_retained_native_inputs() {
        use shardloom_exec::compute_pool::CancellationToken;
        struct Directory(PathBuf);
        impl Drop for Directory {
            fn drop(&mut self) {
                let _ = std::fs::remove_dir_all(&self.0);
            }
        }
        let fixture = Directory(std::env::temp_dir().join(format!(
            "shardloom-shared-prepare-{}-{}",
            std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos(),
        )));
        std::fs::create_dir(&fixture.0).unwrap();
        let source = fixture.0.join("input.csv");
        std::fs::write(&source, "value\n1\n2\n").unwrap();
        let source_uri = source.to_str().unwrap();
        let target =
            auto_prepared_vortex_target_path_with_schema(source_uri, "csv", Some("value:int64"));
        let mut request = PublicWorkflowRouteRequest::new("sql".into());
        request.resources.memory_bytes = Some((8_u64 << 20).to_string());
        request.resources.max_parallelism = Some("1".into());
        request.input_uri = Some("memory://held".into());
        request.input_format = Some("memory".into());
        request.bounded = true;
        request.source_bindings = super::super::super::parse_public_source_bindings(&serde_json::json!({
            "memory://held":{"input_format":"memory","memory_input":{"kind":"range","start":1,"end":2,"step":1,"column":"n"}},
            source_uri:{"input_format":"csv","source_schema":"value:int64"},
        }).to_string()).unwrap();
        let sql = format!(
            "SELECT r.n AS n,c.value AS value FROM 'memory://held' AS r JOIN '{source_uri}' AS c ON r.n = c.value"
        );
        request.sql_statement = Some(sql.clone());
        let mut retained = None;
        let mut shared_memory = None;
        let error = prepare_with_input_adapter(
            &sql,
            &request,
            |session| {
                retained = Some(session.reserve_input_scratch(7 << 20)?);
                shared_memory = Some(session.shared_memory_pool());
                Ok(())
            },
            |_, input, session| input.build(session),
            &CancellationToken::default(),
        )
        .err()
        .expect("retained native state must reduce available preparation memory");
        assert!(
            error.to_string().contains("memory reservation denied"),
            "{error}"
        );
        assert!(!target.exists());
        let memory = shared_memory.unwrap();
        assert_eq!(
            memory.snapshot().reserved_bytes,
            retained.as_ref().unwrap().bytes()
        );
        drop(retained);
        assert_eq!(memory.snapshot().reserved_bytes, 0);

        let (operation, normalized) =
            prepare_with_source(&sql, &request, None, PublicSourcePreparations::default()).unwrap();
        assert_eq!(normalized, 2);
        assert_eq!(operation.snapshot().memory.limit_bytes, 8 << 20);
        let result = operation
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        assert_eq!(result.result_jsonl.value(), "{\"n\":1,\"value\":1}\n");
        assert!(
            !result
                .execution
                .native_io_certificate
                .source_capability_report
                .fallback_attempted
        );
        assert!(target.exists());
        let mut fields = Vec::new();
        super::super::append_execution(&mut fields, &result.execution, false);
        let evidence = fields
            .into_iter()
            .collect::<std::collections::BTreeMap<_, _>>();
        assert_eq!(
            evidence["execution_resource_declared_memory_bytes"],
            (8_u64 << 20).to_string()
        );
        assert_eq!(
            evidence["execution_resource_declared_memory_gb"],
            "unavailable"
        );
        assert_eq!(
            evidence["execution_resource_memory_origin"],
            "execution_call"
        );
        assert_eq!(
            evidence["execution_resource_admitted_memory_bytes"],
            (8_u64 << 20).to_string()
        );
        assert_eq!(evidence["execution_resource_admitted_max_parallelism"], "1");
        assert_eq!(
            evidence["execution_resource_observed_native_peak_reserved_bytes"],
            result
                .execution
                .runtime
                .memory
                .peak_reserved_bytes
                .to_string()
        );
        assert!(
            evidence["execution_resource_memory_observation_scope"]
                .contains("session_pool_lifetime")
        );
        assert_eq!(
            evidence["execution_resource_observed_peak_active_lanes"],
            "unavailable"
        );
        assert_eq!(
            evidence["execution_resource_whole_process_memory_limit_enforced"],
            "false"
        );
    }

    #[test]
    fn native_streaming_join_source_admission_allows_resident_on_either_side() {
        use crate::native_memory_input::{MemoryInput, MemoryRow};
        use shardloom_exec::compute_pool::CancellationToken;
        let streaming =
            serde_json::json!({"kind":"batches","schema":[["n","int64"]],"streaming":true});
        for stream_right in [false, true] {
            let mut request = PublicWorkflowRouteRequest::new("sql".into());
            request.resources.memory_gb = Some("4".into());
            request.resources.max_parallelism = Some("2".into());
            request.input_uri = Some("memory://stream".into());
            request.input_format = Some("memory".into());
            request.bounded = true;
            request.source_bindings = super::super::super::parse_public_source_bindings(&serde_json::json!({
                "memory://stream":{"input_format":"memory","memory_input":streaming},
                "memory://ordinary":{"input_format":"memory","memory_input":{"kind":"range","start":1,"end":4,"step":1,"column":"n"}},
            }).to_string()).unwrap();
            let (left, right) = if stream_right {
                ("memory://ordinary", "memory://stream")
            } else {
                ("memory://stream", "memory://ordinary")
            };
            let sql = format!(
                "SELECT a.n AS left_n, b.n AS right_n FROM '{left}' AS a FULL JOIN '{right}' AS b ON a.n = b.n"
            );
            request.sql_statement = Some(sql.clone());
            let (operation, normalized) = prepare_with_input_adapter(
                &sql,
                &request,
                |_| Ok(()),
                |_, input, session| {
                    if let MemoryInput::Batches { schema, .. } = input {
                        crate::native_memory_rows::build_batch(schema, &[], session)
                    } else {
                        input.build(session)
                    }
                },
                &CancellationToken::default(),
            )
            .unwrap();
            assert_eq!(normalized, 2);
            let MemoryInput::Batches { schema, .. } = request.source_bindings["memory://stream"]
                .memory_input
                .as_ref()
                .unwrap()
            else {
                unreachable!()
            };
            let mut calls = 0;
            let mut producer =
                |session: &shardloom_vortex::resident_session::ResidentVortexSession| {
                    calls += 1;
                    if calls == 1 {
                        crate::native_memory_rows::build_batch(
                            schema,
                            &[MemoryRow(vec![Some("2".into())]), MemoryRow(vec![None])],
                            session,
                        )
                        .map(Some)
                    } else {
                        assert_eq!(calls, 2);
                        Ok(None)
                    }
                };
            let result = operation
                .with_batch_input(&mut producer)
                .unwrap()
                .collect_jsonl(&CancellationToken::default())
                .unwrap();
            let expected = if stream_right {
                "{\"left_n\":1,\"right_n\":null}\n{\"left_n\":2,\"right_n\":2}\n{\"left_n\":3,\"right_n\":null}\n{\"left_n\":null,\"right_n\":null}\n"
            } else {
                "{\"left_n\":2,\"right_n\":2}\n{\"left_n\":null,\"right_n\":null}\n{\"left_n\":null,\"right_n\":1}\n{\"left_n\":null,\"right_n\":3}\n"
            };
            assert_eq!(result.result_jsonl.value(), expected);
            assert_eq!(calls, 2);
            assert_eq!(result.execution.input.as_ref().unwrap().rows, 2);
            assert_eq!(result.execution.prepared_sources, 2);
        }
    }

    #[test]
    fn native_streaming_join_source_admission_allows_multiple_and_resident_producers() {
        use crate::native_memory_input::{MemoryInput, MemoryRow};
        let streaming =
            serde_json::json!({"kind":"batches","schema":[["n","int64"]],"streaming":true});
        for second_streams in [false, true] {
            let mut request = PublicWorkflowRouteRequest::new("sql".into());
            request.resources.memory_gb = Some("4".into());
            request.resources.max_parallelism = Some("2".into());
            request.input_uri = Some("memory://stream".into());
            request.input_format = Some("memory".into());
            request.bounded = true;
            request.source_bindings = super::super::super::parse_public_source_bindings(&serde_json::json!({
                "memory://stream":{"input_format":"memory","memory_input":streaming},
                "memory://other":{"input_format":"memory","memory_input":{"kind":"batches","schema":[["n","int64"]],"streaming":second_streams}},
            }).to_string()).unwrap();
            let sql = "SELECT a.n AS a,b.n AS b FROM 'memory://stream' AS a CROSS JOIN 'memory://other' AS b";
            request.sql_statement = Some(sql.into());
            let mut calls = 0;
            let (operation, normalized) = prepare_with_input_adapter(
                sql,
                &request,
                |_| Ok(()),
                |_, input, session| {
                    calls += 1;
                    let MemoryInput::Batches { schema, streaming } = input else {
                        unreachable!()
                    };
                    if *streaming {
                        crate::native_memory_rows::build_batch(schema, &[], session)
                    } else {
                        crate::native_memory_rows::build(
                            schema,
                            &[MemoryRow(vec![Some("2".into())])],
                            session,
                        )
                    }
                },
                &CancellationToken::default(),
            )
            .unwrap();
            assert_eq!(normalized, 2);
            assert_eq!(calls, 2);
            let mut demands = [0_u64; 2];
            let mut producer = |uri: &DatasetUri, ordinal: u64, session: &shardloom_vortex::resident_session::ResidentVortexSession| {
                let index = usize::from(uri.as_str() == "memory://other");
                assert_eq!(demands[index], ordinal);
                demands[index] += 1;
                if ordinal == 1 { return Ok(None); }
                let MemoryInput::Batches { schema, streaming } = request.source_bindings[uri.as_str()]
                    .memory_input.as_ref().unwrap() else { unreachable!() };
                assert!(*streaming);
                crate::native_memory_rows::build_batch(schema, &[MemoryRow(vec![Some((index + 1).to_string())])], session).map(Some)
            };
            let result = operation
                .with_batch_inputs(&mut producer)
                .unwrap()
                .collect_jsonl(&CancellationToken::default())
                .unwrap();
            assert_eq!(result.result_jsonl.value(), "{\"a\":1,\"b\":2}\n");
            assert_eq!(demands, [2, if second_streams { 2 } else { 0 }]);
            assert_eq!(result.execution.prepared_sources, 2);
            assert_eq!(
                result.execution.input.as_ref().unwrap().sources,
                if second_streams { 2 } else { 1 }
            );
            assert!(result.execution.native_io_certificate.is_certified());
        }
    }

    #[test]
    fn native_memory_input_declarations_use_complete_shared_sql() {
        for declaration in [
            serde_json::json!({"kind":"rows","schema":[["n","int64"]],"rows":[["1"],["3"],["2"]]}),
            serde_json::json!({"kind":"range","start":1,"end":4,"step":1,"column":"n"}),
        ] {
            let mut request = PublicWorkflowRouteRequest::new("sql".into());
            request.resources.memory_gb = Some("4".into());
            request.resources.max_parallelism = Some("2".into());
            request.input_uri = Some("memory://rows".into());
            request.input_format = Some("memory".into());
            request.bounded = true;
            request.source_bindings = super::super::super::parse_public_source_bindings(
                &serde_json::json!({"memory://rows":{"input_format":"memory","memory_input":declaration}}).to_string(),
            ).unwrap();
            let sql = "SELECT n * 2 AS doubled FROM 'memory://rows' WHERE n > 1 ORDER BY doubled DESC LIMIT 2";
            request.sql_statement = Some(sql.into());
            validate_bindings(sql, &request).unwrap();
            assert!(!normalization_required(sql, &request).unwrap());
            let (operation, normalized) =
                prepare_with_source(sql, &request, None, PublicSourcePreparations::default())
                    .unwrap();
            assert_eq!(normalized, 1);
            for _ in 0..2 {
                let result = operation
                    .collect_jsonl(&shardloom_exec::compute_pool::CancellationToken::default())
                    .unwrap();
                assert_eq!(
                    result.result_jsonl.value(),
                    "{\"doubled\":6}\n{\"doubled\":4}\n"
                );
                assert_eq!(result.execution.runtime.prepared_source_opens, 0);
            }
        }
    }

    #[test]
    fn native_relational_declared_identifiers_resolve_inertly_across_nested_sources() {
        let mut request = PublicWorkflowRouteRequest::new("sql".into());
        request.input_uri = Some("missing/cargo.vortex".into());
        request.input_format = Some("vortex".into());
        request.source_bindings.insert(
            "missing/cargo.vortex".into(),
            PublicSourceBinding {
                input_format: "vortex".into(),
                source_schema: None,
                memory_input: None,
            },
        );
        let sql = "SELECT cargo_id FROM (SELECT cargo_id FROM cargo LIMIT 2) AS q WHERE cargo_id IN (SELECT cargo_id FROM cargo)";
        validate_bindings(sql, &request).unwrap();
        assert!(!normalization_required(sql, &request).unwrap());
        let leaves = native_relational::source_leaves(sql).unwrap();
        assert_eq!(leaves.len(), 1);
        assert_eq!(
            declared_path(leaves.first().unwrap(), &request).unwrap(),
            Path::new("missing/cargo.vortex")
        );

        let quoted = "SELECT * FROM (SELECT cargo_id FROM 'cargo') AS q";
        assert!(
            validate_bindings(quoted, &request)
                .unwrap_err()
                .to_string()
                .contains("not referenced")
        );
        let literal = native_relational::source_leaves(quoted).unwrap();
        assert!(!literal.first().unwrap().declared_identifier);
        assert_eq!(
            declared_path(literal.first().unwrap(), &request).unwrap(),
            Path::new("cargo")
        );

        request.source_bindings.insert(
            "another/cargo.csv".into(),
            PublicSourceBinding {
                input_format: "csv".into(),
                source_schema: Some("cargo_id:utf8".into()),
                memory_input: None,
            },
        );
        assert!(
            validate_bindings(sql, &request)
                .unwrap_err()
                .to_string()
                .contains("ambiguous")
        );
        let exact = "SELECT * FROM (SELECT cargo_id FROM 'missing/cargo.vortex' UNION ALL SELECT cargo_id FROM 'another/cargo.csv') AS q";
        validate_bindings(exact, &request).unwrap();
        assert!(normalization_required(exact, &request).unwrap());
    }
}
