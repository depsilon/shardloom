//! Public workflow route facade.
//!
//! This module is the side-effect-free narrow waist for user-facing route
//! inspection. It resolves a declared SQL/Python/DataFrame/CLI workflow into a
//! ShardLoom-internal command route or a deterministic blocker before any
//! runtime command is allowed to execute.

use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
    process::ExitCode,
};

use shardloom_core::{
    CommandStatus, DatasetUri, Diagnostic, DiagnosticCategory, DiagnosticCode, DiagnosticSeverity,
    ExecutionResources, FallbackStatus, OutputFormat, ShardLoomError,
};

use crate::{
    cli_output::{emit, emit_error},
    cli_unknown_arg_error,
    command_context::CommandAdmission,
    execution_resources::ResourceArguments,
    sql_local_source_runtime, vortex_planning, vortex_primitive_execution,
};

const ROUTE_SCHEMA_VERSION: &str = "shardloom.public_workflow_route.v1";
const FACADE_SCHEMA_VERSION: &str = "shardloom.public_workflow_execution_facade.v1";
const NATIVE_VORTEX_USER_ROUTE_CONTRACT_SCHEMA_VERSION: &str =
    "shardloom.native_vortex_user_route_contract.v1";
const NATIVE_VORTEX_PLAN_CONTRACT_SCHEMA_VERSION: &str =
    "shardloom.native_vortex_unified_plan_contract.v1";
const TYPED_RESULT_SINK_CONTRACT_SCHEMA_VERSION: &str = "shardloom.typed_result_sink_contract.v1";
const ROUTE_REPORT_ID: &str = "gar-runtime-impl-6d.public_workflow_route_facade";
const ROUTE_DOCS_REF: &str = "docs/status/cli-command-registry.md#public-route-facade-command";
const FALLBACK_BOUNDARY: &str =
    "route inspection is side-effect-free and never invokes fallback or external engines";
const CLAIM_BOUNDARY: &str = "simplified public facade over admitted ShardLoom routes only; not broad SQL/DataFrame support, production readiness, or performance superiority";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NativeOperatorOptions {
    Automatic,
    Explicit,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PublicWorkflowRouteRequest {
    surface: String,
    input_uri: Option<String>,
    input_format: Option<String>,
    source_schema: Option<String>,
    source_bindings: std::collections::BTreeMap<String, PublicSourceBinding>,
    raw_source_bindings: Option<String>,
    sql_statement: Option<String>,
    plan_summary: Option<String>,
    requested_output: String,
    output_ref: Option<String>,
    execution_policy: String,
    materialization_policy: String,
    evidence_level: String,
    bounded: bool,
    allow_overwrite: bool,
    fanout_outputs: Vec<String>,
    source_fingerprint_policy: Option<String>,
    // Explicit operation payloads must never be discarded in favor of SQL.
    native_operator_options: NativeOperatorOptions,
    native_vortex_operation_family: Option<String>,
    native_vortex_right_input: Option<String>,
    vortex_primitive: Option<String>,
    vortex_predicate: Option<String>,
    vortex_columns: Option<String>,
    vortex_source_order_limit: Option<String>,
    vortex_sample_seed: Option<String>,
    vortex_sample_fraction: Option<String>,
    vortex_sample_replacement: bool,
    vortex_sample_weight_column: Option<String>,
    vortex_duplicate_keep: Option<String>,
    vortex_deduplicate_key_columns: Option<String>,
    vortex_expression_projection: Option<String>,
    vortex_melt_projection: Option<String>,
    vortex_explode_projection: Option<String>,
    vortex_pivot_projection: Option<String>,
    vortex_rolling_window: Option<String>,
    vortex_aggregate: Option<String>,
    vortex_sort_rows: Option<String>,
    resources: ResourceArguments,
    spill: Option<spill::Options>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct PublicSourceBinding {
    input_format: String,
    source_schema: Option<String>,
    memory_input: Option<crate::native_memory_input::MemoryInput>,
    #[serde(default)]
    table_snapshot: Option<PublicTableSnapshot>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct PublicTableSnapshot {
    pub(super) snapshot_id: Option<i64>,
    pub(super) version: Option<u64>,
    pub(super) instant: Option<String>,
}

impl PublicTableSnapshot {
    pub(super) fn to_selection(&self) -> shardloom_core::input::LocalTableSelection {
        shardloom_core::input::LocalTableSelection {
            snapshot_id: self.snapshot_id,
            version: self.version,
            instant: self.instant.clone(),
        }
    }

    #[cfg(all(feature = "vortex-local-primitives", unix))]
    pub(super) fn from_selection(selection: &shardloom_core::input::LocalTableSelection) -> Self {
        Self {
            snapshot_id: selection.snapshot_id,
            version: selection.version,
            instant: selection.instant.clone(),
        }
    }
}

fn parse_public_source_bindings(
    raw: &str,
) -> Result<std::collections::BTreeMap<String, PublicSourceBinding>, ShardLoomError> {
    let invalid = |message: &str| {
        ShardLoomError::InvalidOperation(format!(
            "source bindings: {message}; no fallback execution was attempted"
        ))
    };
    if raw.len() > 8 * 1024 * 1024 {
        return Err(invalid("source declarations and inline input exceed 8 MiB"));
    }
    let mut bindings: std::collections::BTreeMap<String, PublicSourceBinding> =
        serde_json::from_str(raw).map_err(|error| invalid(&error.to_string()))?;
    if bindings.len() > 128 {
        return Err(invalid("declarations exceed 128 sources"));
    }
    if raw.len() > 256 * 1024
        && bindings
            .values()
            .all(|binding| binding.memory_input.is_none())
    {
        return Err(invalid("file declarations exceed 256 KiB"));
    }
    for (uri, binding) in &mut bindings {
        if uri.trim().is_empty() || uri.len() > 16_384 {
            return Err(invalid("source URI must contain 1..=16384 bytes"));
        }
        binding.input_format = normalize_input_format(&binding.input_format)?;
        if let Some(snapshot) = &binding.table_snapshot {
            if !matches!(binding.input_format.as_str(), "iceberg" | "delta" | "hudi") {
                return Err(invalid(
                    "table_snapshot requires an Iceberg, Delta, or Hudi source",
                ));
            }
            snapshot
                .to_selection()
                .validate_for(&binding.input_format)?;
        } else if matches!(binding.input_format.as_str(), "iceberg" | "delta" | "hudi") {
            shardloom_core::input::LocalTableSelection::default()
                .validate_for(&binding.input_format)?;
        }
        if matches!(binding.input_format.as_str(), "iceberg" | "delta" | "hudi")
            && (binding.source_schema.is_some() || binding.memory_input.is_some())
        {
            return Err(invalid(
                "table sources cannot declare source_schema or memory_input",
            ));
        }
        if binding.input_format == "memory" {
            if !uri.starts_with("memory://") || binding.source_schema.is_some() {
                return Err(invalid(
                    "memory inputs require a memory URI and their own typed declaration",
                ));
            }
            binding
                .memory_input
                .as_ref()
                .ok_or_else(|| invalid("memory input declaration is absent"))?
                .validate()?;
        } else if binding.memory_input.is_some() {
            return Err(invalid(
                "inline memory input cannot be attached to a file source",
            ));
        }
        if let Some(schema) = &binding.source_schema {
            if binding.input_format == "vortex" {
                return Err(invalid(
                    "native Vortex retains its authoritative schema; source schema hints require compatibility input",
                ));
            }
            sql_local_source_runtime::parse_vortex_ingest_schema_hints(schema)?;
        }
    }
    Ok(bindings)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PublicWorkflowRoutePlan {
    status: CommandStatus,
    route_status: &'static str,
    route_id: &'static str,
    resolved_internal_command: &'static str,
    start_state: &'static str,
    vortex_normalization_point: &'static str,
    execution_mode: &'static str,
    preparation_included: bool,
    query_timing_starts_after_preparation: bool,
    blocker_id: &'static str,
    blocker_reason: &'static str,
    diagnostics: Vec<Diagnostic>,
}

type PublicWorkflowRoutePlanResult<T> = Result<T, Box<PublicWorkflowRoutePlan>>;

#[path = "public_workflow_spill.rs"]
mod spill;

#[cfg(all(feature = "vortex-local-primitives", unix))]
#[path = "public_resident_count.rs"]
mod resident_count;

#[cfg(all(feature = "vortex-local-primitives", unix))]
#[path = "public_resident_sql.rs"]
mod resident_sql;

#[cfg(all(feature = "vortex-local-primitives", unix))]
#[path = "public_resident_count_where.rs"]
mod resident_count_where;

#[cfg(all(feature = "vortex-local-primitives", unix))]
#[path = "public_resident_aggregate.rs"]
mod resident_aggregate;

#[cfg(all(feature = "vortex-local-primitives", unix))]
#[path = "public_resident_unary.rs"]
mod resident_unary;

#[cfg(all(feature = "vortex-local-primitives", unix))]
#[path = "public_resident_relational.rs"]
mod resident_relational;

#[cfg(all(feature = "vortex-local-primitives", unix))]
#[path = "native_runtime.rs"]
pub mod native;

#[cfg(all(feature = "vortex-local-primitives", unix))]
const MAX_CACHED_PREPARED_SOURCES: usize = 16;

#[cfg(all(feature = "vortex-local-primitives", unix))]
#[derive(Clone)]
struct CachedPreparedSource {
    uri_key: String,
    canonical_path: PathBuf,
    source: shardloom_vortex::resident_session::PreparedVortexSource,
}

/// Caller-owned prepared execution. Only the latest admitted operation is retained;
/// results are never cached and every execution validates its source generation.
#[derive(Default)]
pub(crate) struct PublicExecutionSession {
    #[cfg(all(feature = "vortex-local-primitives", unix))]
    command_owner: Option<std::sync::Arc<native::EmbeddedExecutionOwner>>,
    #[cfg(all(feature = "vortex-local-primitives", unix))]
    cache_owner: Option<shardloom_vortex::resident_session::ResidentVortexSession>,
    #[cfg(all(feature = "vortex-local-primitives", unix))]
    cache_resources: Option<(u64, usize)>,
    #[cfg(all(feature = "vortex-local-primitives", unix))]
    prepared_sources: Vec<CachedPreparedSource>,
    #[cfg(all(feature = "vortex-local-primitives", unix))]
    collect: Option<PreparedPublicCollect>,
    #[cfg(all(feature = "vortex-local-primitives", unix))]
    count: Option<PreparedPublicCount>,
    #[cfg(all(feature = "vortex-local-primitives", unix))]
    count_where: Option<PreparedPublicCountWhere>,
    #[cfg(all(feature = "vortex-local-primitives", unix))]
    aggregate: Option<resident_aggregate::PreparedPublicAggregate>,
    #[cfg(all(feature = "vortex-local-primitives", unix))]
    unary: Option<resident_unary::PreparedPublicUnary>,
    #[cfg(all(feature = "vortex-local-primitives", unix))]
    relational: Option<resident_relational::PreparedPublicRelational>,
}

#[cfg(all(feature = "vortex-local-primitives", unix))]
struct PreparedPublicCollect {
    request: PublicWorkflowRouteRequest,
    operation: shardloom_vortex::local_primitives::collect::PreparedVortexCollect,
}

#[cfg(all(feature = "vortex-local-primitives", unix))]
struct PreparedPublicCount {
    request: PublicWorkflowRouteRequest,
    operation: shardloom_vortex::resident_session::PreparedVortexCount,
    session: shardloom_vortex::resident_session::ResidentVortexSession,
}

#[cfg(all(feature = "vortex-local-primitives", unix))]
struct PreparedPublicCountWhere {
    request: PublicWorkflowRouteRequest,
    operation: shardloom_vortex::local_primitives::prepared_count::PreparedVortexCountWhere,
}

impl PublicExecutionSession {
    // Reduced builds have no prepared native fields to release.
    #[cfg_attr(
        not(all(feature = "vortex-local-primitives", unix)),
        allow(clippy::unused_self)
    )]
    pub(crate) fn clear_operation_slots(&mut self) {
        // Release per-operation prepared lowers while retaining opened
        // immutable file sources owned by the current native session.
        #[cfg(all(feature = "vortex-local-primitives", unix))]
        {
            self.collect = None;
            self.count = None;
            self.count_where = None;
            self.aggregate = None;
            self.unary = None;
            self.relational = None;
        }
    }

    // Reduced builds have no prepared native fields to release.
    #[cfg_attr(
        not(all(feature = "vortex-local-primitives", unix)),
        allow(clippy::unused_self, dead_code)
    )]
    pub(crate) fn clear(&mut self) {
        // Discard prepared work without replacing the runtime that owns its
        // credits. The worker's next explicit allocation reconfigures that same
        // owner; a previous call never becomes implicit resource configuration.
        self.clear_operation_slots();
        #[cfg(all(feature = "vortex-local-primitives", unix))]
        {
            self.prepared_sources.clear();
            self.cache_resources = None;
        }
    }

    #[cfg(all(feature = "vortex-local-primitives", unix))]
    fn canonical_source_key(path: &Path) -> PathBuf {
        std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf())
    }

    #[cfg(all(feature = "vortex-local-primitives", unix))]
    fn resolve_local_path_for_uri_str(uri: &str) -> Option<PathBuf> {
        if let Ok(parsed) = DatasetUri::new(uri) {
            return Self::resolve_local_vortex_path_for_uri(&parsed);
        }
        if uri.contains("://") && !uri.starts_with("file://") {
            return None;
        }
        Some(PathBuf::from(uri.strip_prefix("file://").unwrap_or(uri)))
    }

    #[cfg(all(feature = "vortex-local-primitives", unix))]
    fn resolve_local_vortex_path_for_uri(uri: &DatasetUri) -> Option<PathBuf> {
        use shardloom_core::UriScheme;
        match uri.scheme() {
            UriScheme::LocalPath => Some(PathBuf::from(uri.as_str())),
            UriScheme::File => Some(PathBuf::from(
                uri.as_str()
                    .strip_prefix("file://")
                    .unwrap_or_else(|| uri.as_str()),
            )),
            UriScheme::S3 | UriScheme::Gcs | UriScheme::Adls | UriScheme::Other => None,
        }
    }

    #[cfg_attr(
        not(all(feature = "vortex-local-primitives", unix)),
        allow(clippy::unused_self, dead_code)
    )]
    fn evict_prepared_path(&mut self, path: &Path) {
        #[cfg(all(feature = "vortex-local-primitives", unix))]
        {
            let canonical = Self::canonical_source_key(path);
            let uri_key = path.to_string_lossy();
            self.prepared_sources.retain(|entry| {
                entry.canonical_path != canonical
                    && entry.canonical_path != path
                    && entry.uri_key != uri_key.as_ref()
                    && Path::new(&entry.uri_key) != path
            });
        }
        #[cfg(not(all(feature = "vortex-local-primitives", unix)))]
        let _ = path;
    }

    #[cfg_attr(
        not(all(feature = "vortex-local-primitives", unix)),
        allow(clippy::unused_self, dead_code)
    )]
    fn evict_prepared_uri(&mut self, uri: &str) {
        #[cfg(all(feature = "vortex-local-primitives", unix))]
        {
            self.prepared_sources.retain(|entry| entry.uri_key != uri);
            if let Some(path) = Self::resolve_local_path_for_uri_str(uri) {
                self.evict_prepared_path(&path);
            }
        }
        #[cfg(not(all(feature = "vortex-local-primitives", unix)))]
        let _ = uri;
    }

    #[cfg_attr(
        not(all(feature = "vortex-local-primitives", unix)),
        allow(clippy::unused_self)
    )]
    fn evict_prepared_binding(&mut self, binding: &NativeVortexInputBinding) {
        #[cfg(all(feature = "vortex-local-primitives", unix))]
        for source in &binding.sources {
            self.evict_prepared_uri(source);
        }
        #[cfg(not(all(feature = "vortex-local-primitives", unix)))]
        let _ = binding;
    }

    #[cfg_attr(
        not(all(feature = "vortex-local-primitives", unix)),
        allow(clippy::unused_self)
    )]
    fn evict_prepared_request_inputs(&mut self, request: &PublicWorkflowRouteRequest) {
        #[cfg(all(feature = "vortex-local-primitives", unix))]
        {
            if let Some(uri) = request.input_uri.as_deref() {
                self.evict_prepared_uri(uri);
            }
            for key in request.source_bindings.keys() {
                self.evict_prepared_uri(key);
            }
        }
        #[cfg(not(all(feature = "vortex-local-primitives", unix)))]
        let _ = request;
    }

    #[cfg(all(feature = "vortex-local-primitives", unix))]
    fn remember_prepared_file_source_inner(
        &mut self,
        uri_key: String,
        canonical_path: PathBuf,
        source: shardloom_vortex::resident_session::PreparedVortexSource,
    ) {
        self.prepared_sources
            .retain(|entry| entry.canonical_path != canonical_path && entry.uri_key != uri_key);
        if self.prepared_sources.len() >= MAX_CACHED_PREPARED_SOURCES {
            self.prepared_sources.remove(0);
        }
        self.prepared_sources.push(CachedPreparedSource {
            uri_key,
            canonical_path,
            source,
        });
    }

    #[cfg(all(feature = "vortex-local-primitives", unix))]
    fn get_or_prepare_file_source(
        &mut self,
        path: &Path,
        session: &shardloom_vortex::resident_session::ResidentVortexSession,
        cancellation: &shardloom_exec::compute_pool::CancellationToken,
    ) -> Result<
        (
            shardloom_vortex::resident_session::PreparedVortexSource,
            bool,
        ),
        ShardLoomError,
    > {
        cancellation.check()?;
        if self
            .cache_owner
            .as_ref()
            .is_some_and(|owner| !owner.same_owner(session))
        {
            self.clear();
        }
        if self.cache_owner.is_none() {
            self.cache_owner = Some(session.clone());
        }
        let uri_key = path.to_string_lossy().into_owned();
        let canonical_path = Self::canonical_source_key(path);
        if let Some(pos) = self
            .prepared_sources
            .iter()
            .position(|entry| entry.canonical_path == canonical_path || entry.uri_key == uri_key)
        {
            let candidate = self.prepared_sources[pos].source.clone();
            if candidate.same_session_owner(session) && candidate.validate_generation().is_ok() {
                let entry = self.prepared_sources.remove(pos);
                self.prepared_sources.push(entry);
                return Ok((candidate, true));
            }
            self.prepared_sources.remove(pos);
        }
        let source = session.prepare_file_with_cancellation(path, cancellation)?;
        self.remember_prepared_file_source_inner(uri_key, canonical_path, source.clone());
        Ok((source, false))
    }

    #[cfg(all(feature = "vortex-local-primitives", unix))]
    fn get_or_prepare_uri_source(
        &mut self,
        uri: &DatasetUri,
        session: &shardloom_vortex::resident_session::ResidentVortexSession,
        cancellation: &shardloom_exec::compute_pool::CancellationToken,
    ) -> Result<
        Option<(
            shardloom_vortex::resident_session::PreparedVortexSource,
            bool,
        )>,
        ShardLoomError,
    > {
        let Some(path) = Self::resolve_local_vortex_path_for_uri(uri) else {
            return Ok(None);
        };
        let (source, reused) = self.get_or_prepare_file_source(&path, session, cancellation)?;
        Ok(Some((source, reused)))
    }

    #[cfg(all(test, feature = "vortex-local-primitives", unix))]
    pub(crate) fn command_owner(
        &mut self,
        requested: Option<shardloom_core::FloatPolicy>,
    ) -> Result<std::sync::Arc<native::EmbeddedExecutionOwner>, ShardLoomError> {
        self.command_owner_with_policies(requested, None)
    }

    #[cfg(all(feature = "vortex-local-primitives", unix))]
    pub(crate) fn command_owner_with_policies(
        &mut self,
        requested: Option<shardloom_core::FloatPolicy>,
        recursion_limit: Option<shardloom_core::RecursionLimit>,
    ) -> Result<std::sync::Arc<native::EmbeddedExecutionOwner>, ShardLoomError> {
        if let Some(owner) = &self.command_owner
            && requested.is_some_and(|policy| policy != owner.float_policy())
        {
            return Err(ShardLoomError::InvalidOperation(
                "float_policy cannot change within a native command session; create a new session; no fallback execution was attempted".into(),
            ));
        }
        if let Some(owner) = &self.command_owner
            && recursion_limit.is_some_and(|limit| limit != owner.recursion_limit())
        {
            return Err(ShardLoomError::InvalidOperation(
                "recursion_limit cannot change within a native command session; create a new session; no fallback execution was attempted".into(),
            ));
        }
        if self.command_owner.is_none() {
            self.command_owner = Some(std::sync::Arc::new(
                native::EmbeddedExecutionOwner::new_with_policies(
                    None,
                    None,
                    requested.unwrap_or_default(),
                    recursion_limit.unwrap_or_default(),
                )?,
            ));
        }
        Ok(std::sync::Arc::clone(
            self.command_owner
                .as_ref()
                .expect("command owner initialized"),
        ))
    }

    #[cfg(all(feature = "vortex-local-primitives", unix))]
    fn bind_native_owner(
        &mut self,
        admission: &crate::command_context::CommandAdmission<'_>,
    ) -> Result<(), ShardLoomError> {
        let native = admission.native_session()?;
        let resources = admission.resources();
        let resource_key = (resources.memory_bytes(), resources.max_parallelism());
        if self
            .cache_owner
            .as_ref()
            .is_none_or(|owner| !owner.same_owner(native))
            || self.cache_resources != Some(resource_key)
        {
            self.clear();
            self.cache_owner = Some(native.clone());
            self.cache_resources = Some(resource_key);
        }
        Ok(())
    }

    #[cfg(all(feature = "vortex-local-primitives", unix))]
    fn invalidate_changed_request(
        &mut self,
        request: &PublicWorkflowRouteRequest,
        plan: &PublicWorkflowRoutePlan,
    ) {
        // Prepared routes retain the normalized native request. Source preparation
        // still validates original and prepared generations before every call.
        let prepared = (plan.route_id == "local_file_prepare_once_first_query")
            .then(|| prepared_local_workflow_native_request(request).ok())
            .flatten()
            .map(|prepared| prepared.request);
        let retained = prepared.as_ref().unwrap_or(request);
        if self
            .collect
            .as_ref()
            .is_some_and(|entry| &entry.request != retained)
            || self
                .count
                .as_ref()
                .is_some_and(|entry| &entry.request != retained)
            || self
                .count_where
                .as_ref()
                .is_some_and(|entry| &entry.request != retained)
            || self
                .aggregate
                .as_ref()
                .is_some_and(|entry| &entry.request != retained)
            || self
                .unary
                .as_ref()
                .is_some_and(|entry| &entry.request != retained)
            || self
                .relational
                .as_ref()
                .is_some_and(|entry| &entry.request != retained)
        {
            self.clear_operation_slots();
        }
    }
}

pub(crate) fn handle_public_workflow_route(
    args: impl Iterator<Item = String>,
    format: OutputFormat,
) -> ExitCode {
    let request = match PublicWorkflowRouteRequest::parse(args) {
        Ok(request) => request,
        Err(error) => {
            return emit_error("route", format, "public workflow route failed", &error);
        }
    };
    let request = effective_public_workflow_request(&request);
    let plan = plan_public_workflow_route(&request);
    emit(
        "route",
        format,
        plan.status,
        "public workflow route".to_string(),
        String::new(),
        plan.diagnostics.clone(),
        route_fields(&request, &plan),
    );
    ExitCode::SUCCESS
}

#[allow(clippy::too_many_lines)] // Keep request admission, semantic routing and operation lifetime in one dispatcher.
pub(crate) fn handle_public_workflow_run(
    args: impl Iterator<Item = String>,
    format: OutputFormat,
    execution_session: &mut PublicExecutionSession,
    context: &crate::command_context::CommandContext<'_>,
) -> ExitCode {
    let arguments = args.collect::<Vec<_>>();
    #[cfg(all(feature = "vortex-local-primitives", unix))]
    let explicit_primitive = arguments.iter().any(|argument| {
        argument.starts_with("--vortex-") || argument == "--native-vortex-right-input"
    });
    let args = arguments.into_iter();
    let request = match PublicWorkflowRouteRequest::parse_execution_in_context(args, context) {
        Ok(request) => request,
        Err(error) => {
            execution_session.clear_operation_slots();
            return emit_error("run", format, "public workflow run failed", &error);
        }
    };
    let request = effective_public_workflow_request(&request);
    let plan = plan_public_workflow_route(&request);
    #[cfg(all(feature = "vortex-local-primitives", unix))]
    let plan = if context.requested_float_policy(None).ok().flatten()
        == Some(shardloom_core::FloatPolicy::Extended)
    {
        if explicit_primitive {
            return emit_error("run", format, "floating policy is not admitted for explicit primitive options", &ShardLoomError::InvalidOperation(
                "extended float_policy requires a complete shared SQL or typed relational declaration; no fallback execution was attempted".into(),
            ));
        }
        let Some(shared) = resident_relational::route_admitted_statement(&request) else {
            return emit_error("run", format, "floating policy requires a shared declaration", &ShardLoomError::InvalidOperation(
                "extended float_policy requires a complete SQL statement; no fallback execution was attempted".into(),
            ));
        };
        spill::validate_route(&request, shared)
    } else {
        plan
    };
    #[cfg(all(feature = "vortex-local-primitives", unix))]
    execution_session.invalidate_changed_request(&request, &plan);
    if plan.status != CommandStatus::Success {
        execution_session.clear_operation_slots();
        return emit_blocked_facade("run", format, &request, &plan);
    }
    let resources = match request.resources.resolve() {
        Ok(resources) => resources,
        Err(error) => return emit_error("run", format, "resource declaration failed", &error),
    };
    let admission = match context.admit_for_command(resources, "run", format) {
        Ok(admission) => admission,
        Err(exit) => return exit,
    };
    #[cfg(all(feature = "vortex-local-primitives", unix))]
    if let Err(error) = execution_session.bind_native_owner(&admission) {
        return emit_error("run", format, "native command ownership failed", &error);
    }

    match plan.route_id {
        #[cfg(all(feature = "vortex-local-primitives", unix))]
        "native_vortex_relational_collect" | "native_vortex_relational_write" => {
            resident_relational::run(&request, &plan, format, execution_session, &admission)
        }
        "native_vortex_count_all"
        | "native_vortex_count_where"
        | "native_vortex_filter"
        | "native_vortex_project"
        | "native_vortex_filter_project"
        | "native_vortex_distinct"
        | "native_vortex_drop_duplicates"
        | "native_vortex_duplicate_mask"
        | "native_vortex_tail"
        | "native_vortex_sample"
        | "native_vortex_expression_project"
        | "native_vortex_melt"
        | "native_vortex_explode"
        | "native_vortex_pivot"
        | "native_vortex_rolling_window"
        | "native_vortex_aggregate"
        | "native_vortex_sort_rows" => execute_native_vortex_primitive_run(
            &request,
            &plan,
            format,
            execution_session,
            &admission,
        ),
        "native_vortex_user_profile" => {
            execute_native_vortex_profile_run(&request, &plan, format, &admission)
        }
        "native_vortex_primitive_row_export" => {
            execute_native_vortex_primitive_row_export_run(&request, &plan, format, &admission)
        }
        "local_file_prepare_once_first_query" => {
            let result = execute_local_file_prepare_once_first_query_run(
                &request,
                &plan,
                format,
                execution_session,
                &admission,
            );
            if result != ExitCode::SUCCESS {
                execution_session.clear_operation_slots();
                execution_session.evict_prepared_request_inputs(&request);
            }
            result
        }
        _ => {
            let blocked = run_route_not_executable_yet(&plan);
            emit_blocked_facade("run", format, &request, &blocked)
        }
    }
}

#[cfg(all(feature = "vortex-local-primitives", unix))]
pub(crate) fn handle_batch_workflow(
    args: impl Iterator<Item = String>,
    transport: &mut crate::python_batch_protocol::Transport,
) -> ExitCode {
    let (args, recursion_limit) = match crate::parse_recursion_limit(args.collect()) {
        Ok((args, limit)) => (args, limit.unwrap_or_default()),
        Err(error) => {
            return emit_error("run", OutputFormat::Json, "invalid recursion limit", &error);
        }
    };
    let (args, float_policy) = match crate::parse_float_policy(args) {
        Ok((args, policy)) => (args, policy.unwrap_or_default()),
        Err(error) => {
            return emit_error("run", OutputFormat::Json, "invalid floating policy", &error);
        }
    };
    if float_policy == shardloom_core::FloatPolicy::Extended
        && args.iter().any(|argument| {
            argument.starts_with("--vortex-") || argument == "--native-vortex-right-input"
        })
    {
        return emit_error("run", OutputFormat::Json, "floating policy is not admitted for explicit primitive options", &ShardLoomError::InvalidOperation("extended float_policy requires a complete shared SQL declaration; no fallback execution was attempted".into()));
    }
    let args = args.into_iter();
    let request = match PublicWorkflowRouteRequest::parse_execution(args) {
        Ok(request) => effective_public_workflow_request(&request),
        Err(error) => {
            return emit_error(
                "run",
                OutputFormat::Json,
                "batch declaration rejected",
                &error,
            );
        }
    };
    let Some(plan) = resident_relational::route_admitted_statement(&request) else {
        return emit_error(
            "run",
            OutputFormat::Json,
            "batch declaration rejected",
            &ShardLoomError::InvalidOperation(
                "batch consumption requires a complete SQL declaration".into(),
            ),
        );
    };
    let plan = spill::validate_route(&request, plan);
    if plan.status != CommandStatus::Success {
        return emit_blocked_facade("run", OutputFormat::Json, &request, &plan);
    }
    resident_relational::run_batches(&request, &plan, transport, float_policy, recursion_limit)
}

fn execute_native_vortex_profile_run(
    request: &PublicWorkflowRouteRequest,
    plan: &PublicWorkflowRoutePlan,
    format: OutputFormat,
    admission: &CommandAdmission<'_>,
) -> ExitCode {
    execute_native_vortex_profile_run_with_extra(request, plan, format, Vec::new(), admission)
}

fn execute_native_vortex_profile_run_with_extra(
    request: &PublicWorkflowRouteRequest,
    plan: &PublicWorkflowRoutePlan,
    format: OutputFormat,
    mut extra_fields: Vec<(String, String)>,
    admission: &CommandAdmission<'_>,
) -> ExitCode {
    if let Err(error) = admission.check() {
        return emit_error("run", format, "native profile cancelled", &error);
    }
    let Some(input_uri) = request.input_uri.clone() else {
        let blocked = input_not_declared_route();
        return emit_blocked_facade("run", format, request, &blocked);
    };
    let mut attachment_fields = execution_attachment_fields("run", request, plan);
    attachment_fields.extend(native_vortex_profile_projection_attachment_fields(request));
    attachment_fields.append(&mut extra_fields);
    vortex_planning::handle_vortex_metadata_summary_with_facade(
        vec![input_uri].into_iter(),
        format,
        "run",
        attachment_fields,
    )
}

fn execute_native_vortex_primitive_row_export_run(
    request: &PublicWorkflowRouteRequest,
    plan: &PublicWorkflowRoutePlan,
    format: OutputFormat,
    admission: &CommandAdmission<'_>,
) -> ExitCode {
    execute_native_vortex_primitive_row_export_run_with_extra(
        request,
        plan,
        format,
        Vec::new(),
        admission,
    )
}

fn execute_native_vortex_primitive_row_export_run_with_extra(
    request: &PublicWorkflowRouteRequest,
    plan: &PublicWorkflowRoutePlan,
    format: OutputFormat,
    extra_fields: Vec<(String, String)>,
    admission: &CommandAdmission<'_>,
) -> ExitCode {
    #[cfg(all(feature = "vortex-local-primitives", feature = "vortex-write", unix))]
    let mut extra_fields = extra_fields;
    #[cfg(all(feature = "vortex-local-primitives", feature = "vortex-write", unix))]
    if let Some(exit) = resident_sql::write_if_needed(
        request,
        plan,
        format,
        &mut extra_fields,
        PublicSourcePreparations::default(),
        admission,
    ) {
        return exit;
    }
    let Some(input_uri) = request.input_uri.clone() else {
        let blocked = input_not_declared_route();
        return emit_blocked_facade("run", format, request, &blocked);
    };
    let targets = match native_vortex_primitive_row_export_targets(request, "run") {
        Ok(targets) => targets,
        Err(blocked) => return emit_blocked_facade("run", format, request, blocked.as_ref()),
    };
    let Some(primitive) = normalized_vortex_primitive(request) else {
        let blocked = native_vortex_operation_blocked_route(NativeVortexOperationFamily::Sink);
        return emit_blocked_facade("run", format, request, &blocked);
    };
    let execution =
        match native_vortex_primitive_row_export_execution(request, input_uri, primitive) {
            Ok(execution) => execution,
            Err(error) => {
                return emit_error(
                    "run",
                    format,
                    "native Vortex primitive row export failed",
                    &error,
                );
            }
        };
    let written = match execute_native_vortex_primitive_row_export_reports(
        request, &targets, &execution, admission,
    ) {
        Ok(reports) => reports,
        Err(error) => {
            return emit_error(
                "run",
                format,
                "native Vortex primitive row export failed",
                &error,
            );
        }
    };
    #[cfg(all(feature = "vortex-local-primitives", feature = "vortex-write", unix))]
    resident_relational::append_output_replay_fields(&mut extra_fields, written.replay.as_ref());
    emit_native_vortex_primitive_row_export(
        request,
        plan,
        format,
        extra_fields,
        &targets,
        &written.reports,
    )
}

fn emit_native_vortex_primitive_row_export(
    request: &PublicWorkflowRouteRequest,
    plan: &PublicWorkflowRoutePlan,
    format: OutputFormat,
    mut extra_fields: Vec<(String, String)>,
    targets: &[NativeVortexPrimitiveRowExportTarget],
    reports: &[shardloom_vortex::VortexLocalPrimitiveRowExportReport],
) -> ExitCode {
    let mut fields = execution_attachment_fields("run", request, plan);
    fields.append(&mut extra_fields);
    let primary_report = reports
        .first()
        .expect("native Vortex primitive row export has a primary target");
    append_native_vortex_primitive_row_export_fields(&mut fields, primary_report);
    append_native_vortex_primitive_row_export_target_fields(&mut fields, targets, reports);
    let has_errors = reports
        .iter()
        .any(shardloom_vortex::VortexLocalPrimitiveRowExportReport::has_errors);
    let diagnostics = reports
        .iter()
        .flat_map(|report| report.diagnostics.clone())
        .collect::<Vec<_>>();
    emit(
        "run",
        format,
        if has_errors {
            CommandStatus::Unsupported
        } else {
            CommandStatus::Success
        },
        "native Vortex primitive row export".to_string(),
        format!(
            "native Vortex primitive row export wrote {} {} rows to {} across {} target(s)",
            primary_report.rows_written,
            primary_report.output_format,
            primary_report.output_path,
            reports.len()
        ),
        diagnostics,
        fields,
    );
    if has_errors {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

struct NativeVortexPrimitiveRowExportExecution {
    primitive_request: shardloom_vortex::VortexQueryPrimitiveRequest,
    policy: shardloom_vortex::VortexLocalPrimitiveExecutionPolicy,
}

struct NativeVortexPrimitiveRowExports {
    reports: Vec<shardloom_vortex::VortexLocalPrimitiveRowExportReport>,
    #[cfg(all(feature = "vortex-local-primitives", feature = "vortex-write", unix))]
    replay: Option<
        shardloom_vortex::local_primitives::prepared_relational::VortexRelationalOutputReplay,
    >,
}

fn native_vortex_primitive_row_export_execution(
    request: &PublicWorkflowRouteRequest,
    input_uri: String,
    primitive: PublicVortexPrimitive,
) -> Result<NativeVortexPrimitiveRowExportExecution, ShardLoomError> {
    let uri = shardloom_core::DatasetUri::new(input_uri)?;
    let (primitive_request, _) = native_vortex_request_and_arg(request, primitive, uri)?;
    let policy = native_vortex_materializing_policy(request)?;
    Ok(NativeVortexPrimitiveRowExportExecution {
        primitive_request,
        policy,
    })
}

fn execute_native_vortex_primitive_row_export_reports(
    request: &PublicWorkflowRouteRequest,
    targets: &[NativeVortexPrimitiveRowExportTarget],
    execution: &NativeVortexPrimitiveRowExportExecution,
    admission: &CommandAdmission<'_>,
) -> Result<NativeVortexPrimitiveRowExports, ShardLoomError> {
    admission.check()?;
    if targets.len() > 1 {
        return execute_native_vortex_primitive_row_export_reports_staged(
            request, targets, execution, admission,
        );
    }
    targets
        .iter()
        .map(|target| {
            #[cfg(all(feature = "vortex-local-primitives", feature = "vortex-write", unix))]
            {
                shardloom_vortex::local_primitives::execution::export_in_session(
                    &execution.primitive_request,
                    &target.path,
                    target.format,
                    request.allow_overwrite,
                    execution.policy,
                    admission.native_session()?,
                    &admission.cancellation(),
                )
            }
            #[cfg(not(all(feature = "vortex-local-primitives", feature = "vortex-write", unix)))]
            shardloom_vortex::execute_vortex_local_primitive_row_export_with_policy(
                &execution.primitive_request,
                &target.path,
                target.format,
                request.allow_overwrite,
                execution.policy,
            )
        })
        .collect::<Result<Vec<_>, _>>()
        .map(|reports| NativeVortexPrimitiveRowExports {
            reports,
            #[cfg(all(feature = "vortex-local-primitives", feature = "vortex-write", unix))]
            replay: None,
        })
}

fn execute_native_vortex_primitive_row_export_reports_staged(
    request: &PublicWorkflowRouteRequest,
    targets: &[NativeVortexPrimitiveRowExportTarget],
    execution: &NativeVortexPrimitiveRowExportExecution,
    admission: &CommandAdmission<'_>,
) -> Result<NativeVortexPrimitiveRowExports, ShardLoomError> {
    #[cfg(all(feature = "vortex-local-primitives", feature = "vortex-write", unix))]
    {
        shardloom_vortex::local_primitives::output_fanout::write_primitive_with_replay_in_session(
            &execution.primitive_request,
            &targets
                .iter()
                .map(|target| (target.path.clone(), target.format))
                .collect::<Vec<_>>(),
            request.allow_overwrite,
            execution.policy,
            admission.native_session()?,
            &admission.cancellation(),
        )
        .map(|written| NativeVortexPrimitiveRowExports {
            reports: written.outputs,
            replay: written.replay,
        })
    }
    #[cfg(not(all(feature = "vortex-local-primitives", feature = "vortex-write", unix)))]
    {
        let _ = (request, targets, execution, admission);
        Err(ShardLoomError::InvalidOperation("native output fanout requires the native writer features; no fallback execution was attempted".into()))
    }
}

#[derive(Debug, Clone)]
struct NativeVortexPrimitiveRowExportTarget {
    role: &'static str,
    format: shardloom_vortex::VortexLocalPrimitiveRowExportFormat,
    path: PathBuf,
}

fn native_vortex_primitive_row_export_targets(
    request: &PublicWorkflowRouteRequest,
    command: &'static str,
) -> PublicWorkflowRoutePlanResult<Vec<NativeVortexPrimitiveRowExportTarget>> {
    let Some(output_ref) = request.output_ref.as_deref() else {
        return Err(Box::new(output_required_route(
            command,
            "native Vortex primitive row export",
        )));
    };
    if output_ref.trim().is_empty() {
        return Err(Box::new(output_required_route(
            command,
            "native Vortex primitive row export",
        )));
    }
    let primary_format = native_vortex_row_export_format_for_output_request(request)?;
    let mut targets = vec![NativeVortexPrimitiveRowExportTarget {
        role: "primary",
        format: primary_format,
        path: PathBuf::from(output_ref),
    }];
    for fanout_output in &request.fanout_outputs {
        targets.push(native_vortex_parse_row_export_fanout_target(fanout_output)?);
    }
    native_vortex_preflight_row_export_targets(&targets, request.allow_overwrite)?;
    Ok(targets)
}

fn native_vortex_row_export_format_for_output_request(
    request: &PublicWorkflowRouteRequest,
) -> PublicWorkflowRoutePlanResult<shardloom_vortex::VortexLocalPrimitiveRowExportFormat> {
    match request.requested_output.as_str() {
        "write_json" => Ok(shardloom_vortex::VortexLocalPrimitiveRowExportFormat::Json),
        "write_jsonl" => Ok(shardloom_vortex::VortexLocalPrimitiveRowExportFormat::Jsonl),
        "write_csv" => Ok(shardloom_vortex::VortexLocalPrimitiveRowExportFormat::Csv),
        "write_vortex" => Ok(shardloom_vortex::VortexLocalPrimitiveRowExportFormat::Vortex),
        "write_parquet" => Ok(shardloom_vortex::VortexLocalPrimitiveRowExportFormat::Parquet),
        "write_arrow_ipc" => Ok(shardloom_vortex::VortexLocalPrimitiveRowExportFormat::ArrowIpc),
        "write_avro" => Ok(shardloom_vortex::VortexLocalPrimitiveRowExportFormat::Avro),
        "write_orc" => Ok(shardloom_vortex::VortexLocalPrimitiveRowExportFormat::Orc),
        _ => Err(Box::new(native_vortex_sink_format_blocked_route(request))),
    }
}

fn native_vortex_parse_row_export_fanout_target(
    value: &str,
) -> PublicWorkflowRoutePlanResult<NativeVortexPrimitiveRowExportTarget> {
    let Some((format_raw, path_raw)) = value.split_once('=') else {
        return Err(native_vortex_row_export_fanout_blocked_route(
            "py-vortex-route-unify-1.native_vortex_fanout_payload_invalid",
            "native Vortex primitive row-export fanout requires format=local-path",
            format!("fanout_payload={value}"),
            "pass --fanout-output jsonl=path or --fanout-output csv=path",
        ));
    };
    if path_raw.trim().is_empty() {
        return Err(native_vortex_row_export_fanout_blocked_route(
            "py-vortex-route-unify-1.native_vortex_fanout_payload_invalid",
            "native Vortex primitive row-export fanout path is empty",
            format!("fanout_payload={value}"),
            "pass --fanout-output jsonl=path or --fanout-output csv=path",
        ));
    }
    let format = match format_raw.trim().to_ascii_lowercase().as_str() {
        "json" => shardloom_vortex::VortexLocalPrimitiveRowExportFormat::Json,
        "jsonl" => shardloom_vortex::VortexLocalPrimitiveRowExportFormat::Jsonl,
        "csv" => shardloom_vortex::VortexLocalPrimitiveRowExportFormat::Csv,
        "vortex" => shardloom_vortex::VortexLocalPrimitiveRowExportFormat::Vortex,
        "parquet" => shardloom_vortex::VortexLocalPrimitiveRowExportFormat::Parquet,
        "arrow_ipc" | "arrow-ipc" => {
            shardloom_vortex::VortexLocalPrimitiveRowExportFormat::ArrowIpc
        }
        "avro" => shardloom_vortex::VortexLocalPrimitiveRowExportFormat::Avro,
        "orc" => shardloom_vortex::VortexLocalPrimitiveRowExportFormat::Orc,
        _ => {
            return Err(native_vortex_row_export_fanout_blocked_route(
                "py-vortex-route-unify-1.native_vortex_fanout_sink_format_missing",
                "native output fanout requires an admitted output format",
                format!(
                    "fanout_format={format_raw} admitted_fanout_formats=vortex,json,jsonl,csv,parquet,arrow_ipc,avro,orc"
                ),
                "use an admitted format=path fanout target",
            ));
        }
    };
    Ok(NativeVortexPrimitiveRowExportTarget {
        role: "fanout",
        format,
        path: PathBuf::from(path_raw),
    })
}

fn native_vortex_preflight_row_export_targets(
    targets: &[NativeVortexPrimitiveRowExportTarget],
    allow_overwrite: bool,
) -> PublicWorkflowRoutePlanResult<()> {
    let mut seen = BTreeSet::new();
    for target in targets {
        let workspace_root = shardloom_core::infer_local_output_workspace_root(&target.path)
            .map_err(|error| native_vortex_row_export_output_safety_blocked_route(&error))?;
        let write_plan = shardloom_core::plan_workspace_safe_local_output(
            workspace_root,
            &target.path,
            allow_overwrite,
        )
        .map_err(|error| native_vortex_row_export_output_safety_blocked_route(&error))?;
        let key = write_plan
            .target_path
            .to_string_lossy()
            .replace('\\', "/")
            .to_ascii_lowercase();
        if !seen.insert(key) {
            return Err(native_vortex_row_export_fanout_blocked_route(
                "py-vortex-route-unify-1.native_vortex_fanout_duplicate_output",
                "native Vortex primitive row-export fanout output path is duplicated",
                format!("duplicate_output_path={}", target.path.display()),
                "choose distinct primary and fanout output paths",
            ));
        }
    }
    Ok(())
}

fn native_vortex_row_export_fanout_blocked_route(
    blocker_id: &'static str,
    message: &'static str,
    reason: String,
    next_action: &'static str,
) -> Box<PublicWorkflowRoutePlan> {
    Box::new(blocked_route(
        blocker_id,
        message,
        Diagnostic::new(
            DiagnosticCode::InvalidInput,
            DiagnosticSeverity::Error,
            DiagnosticCategory::InvalidInput,
            message.to_string(),
            Some("public_workflow_route.fanout_outputs".to_string()),
            Some(reason),
            Some(next_action.to_string()),
            FallbackStatus::disabled_by_policy(),
        ),
    ))
}

fn native_vortex_row_export_output_safety_blocked_route(
    error: &ShardLoomError,
) -> Box<PublicWorkflowRoutePlan> {
    Box::new(blocked_route(
        "py-vortex-route-unify-1.native_vortex_row_export_output_path_unsafe",
        "native Vortex primitive row-export output path failed workspace-safety validation",
        Diagnostic::new(
            DiagnosticCode::InvalidInput,
            DiagnosticSeverity::Error,
            DiagnosticCategory::InvalidInput,
            "native Vortex primitive row-export output path failed workspace-safety validation"
                .to_string(),
            Some("public_workflow_route.output_ref".to_string()),
            Some(error.to_string()),
            Some("choose a local output path within the inferred workspace and retry".to_string()),
            FallbackStatus::disabled_by_policy(),
        ),
    ))
}

#[allow(clippy::too_many_lines)]
fn native_vortex_primitive_arg_for_request(
    request: &PublicWorkflowRouteRequest,
    primitive: PublicVortexPrimitive,
) -> Result<String, ShardLoomError> {
    match primitive {
        PublicVortexPrimitive::Count => Ok("count".to_string()),
        PublicVortexPrimitive::CountWhere => Ok(format!(
            "count-where:{}",
            required_native_vortex_payload(request.vortex_predicate.as_ref(), "vortex predicate")?
        )),
        PublicVortexPrimitive::Filter => Ok(format!(
            "filter:{}",
            required_native_vortex_payload(request.vortex_predicate.as_ref(), "vortex predicate")?
        )),
        PublicVortexPrimitive::Project => Ok(format!(
            "project:{}",
            required_native_vortex_payload(request.vortex_columns.as_ref(), "vortex columns")?
        )),
        PublicVortexPrimitive::FilterProject => Ok(format!(
            "filter-project:{}|{}",
            required_native_vortex_payload(request.vortex_predicate.as_ref(), "vortex predicate")?,
            required_native_vortex_payload(request.vortex_columns.as_ref(), "vortex columns")?
        )),
        PublicVortexPrimitive::Distinct => {
            let columns = request.vortex_columns.as_ref().map_or("*", String::as_str);
            if let Some(predicate) = request.vortex_predicate.as_ref() {
                Ok(format!("distinct-filter-project:{predicate}|{columns}"))
            } else {
                Ok(format!("distinct:{columns}"))
            }
        }
        PublicVortexPrimitive::DropDuplicates => {
            let output_columns = request.vortex_columns.as_ref().map_or("*", String::as_str);
            let key_columns = required_native_vortex_payload(
                request.vortex_deduplicate_key_columns.as_ref(),
                "vortex deduplicate key columns",
            )?;
            if let Some(predicate) = request.vortex_predicate.as_ref() {
                Ok(format!(
                    "drop-duplicates-filter-project:{predicate}|{output_columns}|{key_columns}"
                ))
            } else {
                Ok(format!("drop-duplicates:{output_columns}|{key_columns}"))
            }
        }
        PublicVortexPrimitive::DuplicateMask => Ok(format!(
            "duplicate-mask:{}",
            required_native_vortex_payload(request.vortex_columns.as_ref(), "vortex columns")?
        )),
        PublicVortexPrimitive::Tail => {
            let columns = request.vortex_columns.as_ref().map_or("*", String::as_str);
            Ok(format!("tail:{columns}"))
        }
        PublicVortexPrimitive::Sample => {
            let columns = request.vortex_columns.as_ref().map_or("*", String::as_str);
            if let Some(predicate) = request.vortex_predicate.as_ref() {
                Ok(format!("sample-filter-project:{predicate}|{columns}"))
            } else {
                Ok(format!("sample:{columns}"))
            }
        }
        PublicVortexPrimitive::ExpressionProject => Ok(format!(
            "expression-project:{}",
            required_native_vortex_payload(
                request.vortex_expression_projection.as_ref(),
                "vortex expression projection"
            )?
        )),
        PublicVortexPrimitive::Melt => Ok(format!(
            "melt:{}",
            required_native_vortex_payload(
                request.vortex_melt_projection.as_ref(),
                "vortex melt projection"
            )?
        )),
        PublicVortexPrimitive::Explode => {
            let explode_projection = required_native_vortex_payload(
                request.vortex_explode_projection.as_ref(),
                "vortex explode projection",
            )?;
            Ok(format!(
                "explode:{}",
                json_payload_with_columns(
                    &explode_projection,
                    request.vortex_columns.as_deref(),
                    "native Vortex explode projection",
                )?
            ))
        }
        PublicVortexPrimitive::Pivot => Ok(format!(
            "pivot:{}",
            required_native_vortex_payload(
                request.vortex_pivot_projection.as_ref(),
                "vortex pivot projection"
            )?
        )),
        PublicVortexPrimitive::RollingWindow => Ok(format!(
            "rolling:{}",
            required_native_vortex_payload(
                request.vortex_rolling_window.as_ref(),
                "vortex rolling window"
            )?
        )),
        PublicVortexPrimitive::Aggregate => {
            let aggregate = required_native_vortex_payload(
                request.vortex_aggregate.as_ref(),
                "vortex aggregate",
            )?;
            if let Some(predicate) = request.vortex_predicate.as_ref() {
                Ok(format!("aggregate-filter:{predicate}|{aggregate}"))
            } else {
                Ok(format!("aggregate:{aggregate}"))
            }
        }
        PublicVortexPrimitive::SortRows => {
            let sort_rows = required_native_vortex_payload(
                request.vortex_sort_rows.as_ref(),
                "vortex sort rows",
            )?;
            let columns = request.vortex_columns.as_ref().map_or("*", String::as_str);
            if let Some(predicate) = request.vortex_predicate.as_ref() {
                Ok(format!(
                    "sort-filter-project:{predicate}|{columns}|{sort_rows}"
                ))
            } else {
                Ok(format!(
                    "sort-rows:{}",
                    json_payload_with_columns(
                        &sort_rows,
                        request.vortex_columns.as_deref(),
                        "native Vortex sort rows",
                    )?
                ))
            }
        }
    }
}

// Computed unary payloads describe the operation, not the source predicate.
// Preserve the separate public predicate before computation and LIMIT.
fn attach_native_unary_source_predicate(
    request: &PublicWorkflowRouteRequest,
    primitive_request: &mut shardloom_vortex::VortexQueryPrimitiveRequest,
) -> Result<(), ShardLoomError> {
    use shardloom_vortex::VortexQueryPrimitiveKind as Kind;
    let Some(predicate) = request.vortex_predicate.as_deref() else {
        return Ok(());
    };
    match primitive_request.kind {
        Kind::TailRows | Kind::DuplicateMaskRows => {
            return Err(ShardLoomError::InvalidOperation(
                "source predicates are not admitted for tail or duplicate_mask; no fallback execution was attempted".to_owned(),
            ));
        }
        Kind::ExpressionProjectRows
        | Kind::MeltRows
        | Kind::ExplodeRows
        | Kind::PivotRows
        | Kind::RollingWindowRows => {}
        _ => return Ok(()),
    }
    // Constructed lists and structs use the same shared scan/filter/limit/project
    // plan as other native expressions. The predicate belongs to the source and
    // must remain attached even when its columns are absent from the output.
    primitive_request.predicate =
        Some(vortex_primitive_execution::parse_tiny_predicate(predicate)?);
    Ok(())
}

fn json_payload_with_columns(
    payload: &str,
    columns: Option<&str>,
    label: &str,
) -> Result<String, ShardLoomError> {
    let Some(columns) = columns else {
        return Ok(payload.to_string());
    };
    let mut value = serde_json::from_str::<serde_json::Value>(payload).map_err(|error| {
        ShardLoomError::InvalidOperation(format!("{label} payload must be valid JSON: {error}"))
    })?;
    let object = value.as_object_mut().ok_or_else(|| {
        ShardLoomError::InvalidOperation(format!("{label} payload must be a JSON object"))
    })?;
    object.insert(
        "columns".to_string(),
        serde_json::Value::String(columns.to_string()),
    );
    Ok(value.to_string())
}

#[allow(clippy::too_many_lines)]
fn append_native_vortex_primitive_row_export_fields(
    fields: &mut Vec<(String, String)>,
    report: &shardloom_vortex::VortexLocalPrimitiveRowExportReport,
) {
    append_hot_runtime_timing_surface_fields(fields);
    if report.status == shardloom_vortex::VortexLocalPrimitiveExecutionStatus::Executed {
        crate::execution_resources::append_admission_fields(
            fields,
            report.resource_envelope.memory_budget_bytes,
            report.resource_envelope.max_parallelism,
            "native_operator_policy_ceiling;runtime_may_use_fewer_lanes",
        );
        crate::execution_resources::append_spill_observation_fields(
            fields,
            report.evidence.side_effects.spill_io_performed,
            report
                .state_budget
                .native_weighted_count_spill
                .as_ref()
                .map(|spill| spill.native_bytes_written),
        );
    }
    push_field(fields, "mode", "native_vortex_primitive_row_export");
    push_field(
        fields,
        "execution",
        "native_vortex_primitive_row_export_performed",
    );
    push_field(
        fields,
        "native_vortex_result_export_kind",
        if report.evidence.native_array_sink.is_some() {
            "owned_native_array_stream"
        } else {
            "primitive_row_stream"
        },
    );
    push_field(
        fields,
        "native_vortex_result_export_format",
        report.output_format,
    );
    push_field(
        fields,
        "native_vortex_result_export_path",
        &report.output_path,
    );
    push_field(
        fields,
        "native_vortex_result_export_rows_written",
        report.rows_written.to_string(),
    );
    push_field(
        fields,
        "native_vortex_result_export_projected_columns",
        report.projected_columns.join(","),
    );
    push_field(
        fields,
        "typed_sink_contract",
        if report.evidence.native_array_sink.is_some() && report.output_format == "arrow-ipc" {
            "native_vortex_arrays_to_arrow_ipc_compatibility_sink"
        } else if report.evidence.native_array_sink.is_some() && report.output_format == "parquet" {
            "native_vortex_arrays_to_parquet_compatibility_sink"
        } else {
            native_vortex_primitive_row_export_typed_sink_contract(report.output_format)
        },
    );
    push_field(
        fields,
        "decode_materialization_boundary",
        if report.evidence.native_array_sink.is_some() && report.output_format == "vortex" {
            if report
                .evidence
                .native_array_sink
                .as_ref()
                .is_some_and(|sink| sink.scalar_values_materialized > 0)
            {
                "native_scan_arrays_to_native_flat_writer;bounded_adapter_scalar_copy;no_arrow_conversion;provider_decode_may_occur"
            } else {
                "native_scan_arrays_to_native_flat_writer;no_adapter_scalar_or_arrow_conversion;provider_decode_may_occur"
            }
        } else if report.evidence.native_array_sink.is_some() {
            "native_scan_arrays_to_explicit_arrow_compatibility_writer;no_scalar_row_bridge;provider_decode_or_copy_may_occur"
        } else {
            "native_vortex_scan_pushdown_then_selected_column_decode_at_compatibility_sink"
        },
    );
    push_bool_field(fields, "data_read", report.evidence.side_effects.data_read);
    push_bool_field(
        fields,
        "data_decoded",
        report.evidence.side_effects.data_decoded,
    );
    push_bool_field(
        fields,
        "data_materialized",
        report.evidence.side_effects.data_materialized,
    );
    push_bool_field(
        fields,
        "upstream_vortex_scan_called",
        report.evidence.upstream_scan_called,
    );
    push_bool_field(fields, "row_read", report.evidence.side_effects.row_read);
    push_bool_field(
        fields,
        "arrow_converted",
        report.evidence.side_effects.arrow_converted,
    );
    push_bool_field(fields, "runtime_execution", true);
    push_bool_field(
        fields,
        "output_io_performed",
        report.evidence.side_effects.write_io,
    );
    push_bool_field(fields, "write_io", report.evidence.side_effects.write_io);
    push_bool_field(fields, "fallback_attempted", false);
    push_bool_field(fields, "external_engine_invoked", false);
    push_bool_field(
        fields,
        "materialization_boundary_reported",
        report.evidence.materialization_boundary_reported,
    );
    append_local_primitive_resource_envelope_fields(fields, &report.resource_envelope);
    append_local_primitive_physical_policy_fields(fields, &report.physical_policy);
    append_local_primitive_state_budget_fields(fields, &report.state_budget);
    append_local_primitive_memory_admission_fields(
        fields,
        &report.resource_envelope,
        &report.state_budget,
    );
    if let Some(native) = &report.evidence.native_array_sink {
        append_native_array_sink_fields(fields, native);
    }
    push_field(fields, "claim_gate_status", "not_claim_grade");
}

fn append_native_array_sink_fields(
    fields: &mut Vec<(String, String)>,
    evidence: &shardloom_vortex::VortexNativeArraySinkEvidence,
) {
    crate::execution_resources::append_memory_observation_fields(
        fields,
        None,
        evidence.peak_reserved_bytes,
        "session_pool_lifetime_including_retained_owners;excludes_provider_bypass_allocations_and_process_rss",
    );
    for (name, value) in [
        (
            "arrays_submitted",
            evidence.native_arrays_submitted.to_string(),
        ),
        (
            "array_logical_bytes",
            evidence.native_array_logical_bytes.to_string(),
        ),
        (
            "adapter_payload_bytes_copied",
            evidence.adapter_payload_bytes_copied.to_string(),
        ),
        (
            "scalar_values_materialized",
            evidence.scalar_values_materialized.to_string(),
        ),
        ("scan_row_bound", evidence.scan_row_bound.to_string()),
        (
            "writer_input_batch_bound",
            evidence.writer_input_batch_bound.to_string(),
        ),
        (
            "peak_reserved_bytes",
            evidence.peak_reserved_bytes.to_string(),
        ),
        (
            "metadata_reserved_bytes",
            evidence.metadata_reserved_bytes.to_string(),
        ),
        (
            "pre_limit_result_row_count",
            evidence.pre_limit_result_row_count.to_string(),
        ),
        (
            "pre_limit_result_row_count_status",
            if evidence.pre_limit_result_row_count_exact {
                "exact"
            } else {
                "lower_bound_scan_stopped_at_limit"
            }
            .to_string(),
        ),
        (
            "source_generation_validated",
            evidence.source_generation_validated.to_string(),
        ),
        (
            "dtype_and_row_count_validated",
            evidence.dtype_and_row_count_validated.to_string(),
        ),
        ("output_sha256", evidence.output_sha256.clone()),
        ("metadata_fidelity", evidence.metadata_fidelity.to_string()),
    ] {
        push_field(fields, format!("native_vortex_array_sink_{name}"), value);
    }
    push_field(
        fields,
        "native_vortex_array_sink_memory_scope",
        if evidence.compatibility.is_some() {
            "session_host_allocator_buffers_and_reserved_arrow_writer_envelopes;excludes_provider_bypass_allocations_and_process_rss"
        } else {
            "session_host_allocator_buffers_and_reserved_layout_metadata;excludes_provider_bypass_allocations_and_process_rss"
        },
    );
    push_field(
        fields,
        "native_vortex_array_sink_byte_work_scope",
        "logical_array_bytes_may_share_buffers;zero_adapter_copies_does_not_assert_zero_provider_decode_or_copy",
    );
    if let Some(compatibility) = &evidence.compatibility {
        append_columnar_compatibility_sink_fields(fields, compatibility);
    }
}

fn append_columnar_compatibility_sink_fields(
    fields: &mut Vec<(String, String)>,
    evidence: &shardloom_vortex::VortexColumnarCompatibilitySinkEvidence,
) {
    for (name, value) in [
        ("native_batches", evidence.native_batches),
        ("native_logical_bytes", evidence.native_logical_bytes),
        ("arrow_batches", evidence.arrow_batches),
        (
            "admitted_arrow_expansion_bytes",
            evidence.admitted_arrow_expansion_bytes,
        ),
        ("max_arrow_batch_bytes", evidence.max_arrow_batch_bytes),
        (
            "max_observed_parquet_in_progress_bytes",
            evidence.max_observed_parquet_in_progress_bytes,
        ),
        (
            "max_observed_orc_in_progress_bytes",
            evidence.max_observed_orc_in_progress_bytes,
        ),
        ("writer_flushes", evidence.writer_flushes),
        ("pressure_flushes", evidence.pressure_flushes),
        ("output_file_units", evidence.output_file_units),
        ("conversion_splits", evidence.conversion_splits),
        ("large_value_batches", evidence.large_value_batches),
        ("file_unit_target_bytes", evidence.file_unit_target_bytes),
        ("writer_reserved_bytes", evidence.writer_reserved_bytes),
        ("output_bytes", evidence.output_bytes),
    ] {
        push_field(
            fields,
            format!("native_vortex_columnar_compatibility_sink_{name}"),
            value.to_string(),
        );
    }
    push_field(
        fields,
        "native_vortex_columnar_compatibility_sink_work_scope",
        "read_decode_materialize_flags_conservative_scan_scope;arrow_batches_observed;expansion_admission_is_not_copy_bytes;parquet_state_sample_excludes_transients;no_process_rss_bound",
    );
}

#[allow(clippy::too_many_lines)]
fn append_local_primitive_resource_envelope_fields(
    fields: &mut Vec<(String, String)>,
    resource_envelope: &shardloom_vortex::VortexLocalPrimitiveResourceEnvelope,
) {
    crate::execution_resources::append_declaration_fields(
        fields,
        resource_envelope.declared_resources,
    );
    push_field(
        fields,
        "local_primitive_resource_envelope_schema_version",
        shardloom_vortex::VortexLocalPrimitiveResourceEnvelope::SCHEMA_VERSION,
    );
    push_field(
        fields,
        "local_primitive_resource_envelope_summary",
        resource_envelope.compact_summary(),
    );
    push_field(
        fields,
        "local_primitive_resource_memory_gb",
        resource_envelope
            .memory_gb
            .map_or_else(|| "unavailable".into(), |value| value.to_string()),
    );
    push_field(
        fields,
        "local_primitive_resource_memory_budget_bytes",
        resource_envelope.memory_budget_bytes.to_string(),
    );
    push_field(
        fields,
        "local_primitive_resource_max_parallelism",
        resource_envelope.max_parallelism.to_string(),
    );
    push_field(
        fields,
        "local_primitive_resource_scan_concurrency_per_worker",
        resource_envelope.scan_concurrency_per_worker.to_string(),
    );
    push_field(
        fields,
        "local_primitive_resource_capillary_unit_target_rows",
        resource_envelope.capillary_unit_target_rows.to_string(),
    );
    push_field(
        fields,
        "local_primitive_resource_group_state_soft_item_budget",
        resource_envelope.group_state_soft_item_budget.to_string(),
    );
    push_field(
        fields,
        "local_primitive_resource_string_topk_heavy_hitter_capacity",
        resource_envelope
            .string_topk_heavy_hitter_capacity
            .to_string(),
    );
    push_field(
        fields,
        "local_primitive_resource_numeric_utf8_topk_heavy_hitter_capacity",
        resource_envelope
            .numeric_utf8_topk_heavy_hitter_capacity
            .to_string(),
    );
    push_field(
        fields,
        "local_primitive_resource_spill_threshold_bytes",
        resource_envelope.spill_threshold_bytes.to_string(),
    );
    push_field(
        fields,
        "local_primitive_resource_sort_retention_flush_multiplier",
        resource_envelope
            .sort_retention_flush_multiplier
            .to_string(),
    );
    push_field(
        fields,
        "local_primitive_resource_sort_retention_flush_slack_rows",
        resource_envelope
            .sort_retention_flush_slack_rows
            .to_string(),
    );
    push_field(
        fields,
        "local_primitive_resource_writer_row_block_target_rows",
        resource_envelope.writer_row_block_target_rows.to_string(),
    );
    push_field(
        fields,
        "local_primitive_resource_writer_coalescing_target_bytes",
        resource_envelope.writer_coalescing_target_bytes.to_string(),
    );
}

fn append_local_primitive_physical_policy_fields(
    fields: &mut Vec<(String, String)>,
    physical_policy: &shardloom_vortex::VortexLocalPrimitivePhysicalPolicyReport,
) {
    push_field(
        fields,
        "local_primitive_physical_policy_schema_version",
        &physical_policy.schema_version,
    );
    push_field(
        fields,
        "local_primitive_physical_policy_summary",
        physical_policy.compact_summary(),
    );
    push_field(
        fields,
        "local_primitive_physical_policy_id",
        &physical_policy.policy_id,
    );
    push_field(
        fields,
        "local_primitive_physical_policy_route_family",
        &physical_policy.route_family,
    );
    push_field(
        fields,
        "local_primitive_physical_policy_state_pressure_reason",
        &physical_policy.state_pressure_reason,
    );
    push_field(
        fields,
        "local_primitive_physical_policy_requested_max_parallelism",
        physical_policy.requested_max_parallelism.to_string(),
    );
    push_field(
        fields,
        "local_primitive_physical_policy_selected_max_parallelism",
        physical_policy.selected_max_parallelism.to_string(),
    );
    push_field(
        fields,
        "local_primitive_physical_policy_selected_scan_concurrency_per_worker",
        physical_policy
            .selected_scan_concurrency_per_worker
            .to_string(),
    );
    push_field(
        fields,
        "local_primitive_physical_policy_selected_group_state_soft_item_budget",
        physical_policy
            .selected_group_state_soft_item_budget
            .to_string(),
    );
    push_field(
        fields,
        "local_primitive_physical_policy_selected_string_topk_heavy_hitter_capacity",
        physical_policy
            .selected_string_topk_heavy_hitter_capacity
            .to_string(),
    );
    push_field(
        fields,
        "local_primitive_physical_policy_selected_numeric_utf8_topk_heavy_hitter_capacity",
        physical_policy
            .selected_numeric_utf8_topk_heavy_hitter_capacity
            .to_string(),
    );
    push_field(
        fields,
        "local_primitive_physical_policy_rejected_alternatives",
        physical_policy.rejected_alternatives.join(","),
    );
    push_bool_field(
        fields,
        "local_primitive_physical_policy_row_ref_retention_enabled",
        physical_policy.row_ref_retention_enabled,
    );
    push_bool_field(
        fields,
        "local_primitive_physical_policy_writer_coalescing_enabled",
        physical_policy.writer_coalescing_enabled,
    );
}

fn append_local_primitive_state_budget_fields(
    fields: &mut Vec<(String, String)>,
    state_budget: &shardloom_vortex::VortexLocalPrimitiveStateBudgetReport,
) {
    push_field(
        fields,
        "local_primitive_state_budget_schema_version",
        &state_budget.schema_version,
    );
    push_bool_field(
        fields,
        "local_primitive_state_budget_required",
        state_budget.state_budget_required,
    );
    push_field(
        fields,
        "local_primitive_state_budget_status",
        &state_budget.state_budget_status,
    );
    push_field(
        fields,
        "local_primitive_state_pressure_class",
        &state_budget.state_pressure_class,
    );
    push_field(
        fields,
        "local_primitive_state_family",
        &state_budget.state_family,
    );
    push_field(
        fields,
        "local_primitive_capillary_work_units",
        state_budget.capillary_work_units.join(","),
    );
    push_field(
        fields,
        "local_primitive_pulseweave_pressure_signals",
        state_budget.pulseweave_pressure_signals.join(","),
    );
    push_field(
        fields,
        "local_primitive_observed_state_items",
        state_budget.observed_state_items.to_string(),
    );
    push_field(
        fields,
        "local_primitive_estimated_state_items",
        state_budget
            .estimated_state_items
            .map_or_else(|| "none".to_string(), |value| value.to_string()),
    );
    push_field(
        fields,
        "local_primitive_budget_scope",
        &state_budget.budget_scope,
    );
    push_field(
        fields,
        "local_primitive_spill_policy",
        &state_budget.spill_policy,
    );
    push_bool_field(
        fields,
        "local_primitive_spill_required",
        state_budget.spill_required,
    );
    push_bool_field(
        fields,
        "local_primitive_spill_supported",
        state_budget.spill_supported,
    );
    push_bool_field(
        fields,
        "local_primitive_spill_io_performed",
        state_budget.spill_io_performed,
    );
    push_bool_field(
        fields,
        "local_primitive_fail_closed_if_spill_required",
        state_budget.fail_closed_if_spill_required,
    );
    push_field(
        fields,
        "local_primitive_state_budget_diagnostic_code",
        &state_budget.diagnostic_code,
    );
    push_field(
        fields,
        "local_primitive_state_budget_next_action",
        &state_budget.next_action,
    );
    if let Some(spill) = &state_budget.native_aggregate_spill {
        append_native_aggregate_spill_fields(fields, spill);
    }
    if let Some(spill) = &state_budget.native_weighted_count_spill {
        append_native_weighted_count_spill_fields(fields, spill);
    }
    if let Some(spill) = &state_budget.native_sort_spill {
        append_native_sort_spill_fields(fields, spill);
    }
}

fn append_native_sort_spill_fields(
    fields: &mut Vec<(String, String)>,
    spill: &shardloom_vortex::VortexSortSpillReport,
) {
    let prefix = "local_primitive_native_sort_spill_";
    push_field(
        fields,
        format!("{prefix}workspace"),
        spill.workspace.display().to_string(),
    );
    for (name, value) in [
        ("quota_bytes", spill.quota_bytes),
        ("memory_bytes", spill.memory_bytes),
        ("peak_reserved_bytes", spill.peak_reserved_bytes),
        ("peak_disk_bytes", spill.peak_disk_bytes),
        ("runs_written", spill.runs_written),
        ("runs_validated", spill.runs_validated),
        ("merge_passes", spill.merge_passes),
    ] {
        push_field(fields, format!("{prefix}{name}"), value.to_string());
    }
    for (name, value) in [
        ("run_block_rows", spill.run_block_rows),
        ("merge_fan_in", spill.merge_fan_in),
        ("max_open_runs", spill.max_open_runs),
    ] {
        push_field(fields, format!("{prefix}{name}"), value.to_string());
    }
    push_bool_field(
        fields,
        format!("{prefix}owned_cleanup_completed"),
        spill.owned_cleanup_completed,
    );
}

fn append_native_weighted_count_spill_fields(
    fields: &mut Vec<(String, String)>,
    spill: &shardloom_vortex::VortexWeightedCountSpillReport,
) {
    let prefix = "local_primitive_native_weighted_count_spill_";
    push_field(fields, format!("{prefix}family"), &spill.family);
    push_field(fields, format!("{prefix}key_order"), &spill.key_order);
    push_field(
        fields,
        format!("{prefix}workspace"),
        spill.workspace.display().to_string(),
    );
    for (name, value) in [
        ("quota_bytes", spill.quota_bytes),
        ("memory_bytes", spill.memory_bytes),
        ("peak_reserved_bytes", spill.peak_reserved_bytes),
        ("peak_disk_bytes", spill.peak_disk_bytes),
        ("runs_written", spill.runs_written),
        ("runs_validated", spill.runs_validated),
        ("merge_passes", spill.merge_passes),
        ("source_rows", spill.source_rows),
        ("source_records", spill.source_records),
        ("initial_run_records", spill.initial_run_records),
        ("native_records_written", spill.native_records_written),
        ("native_bytes_written", spill.native_bytes_written),
        ("groups", spill.groups),
        ("source_text_bytes_copied", spill.source_text_bytes_copied),
        ("encoded_text_bytes_copied", spill.encoded_text_bytes_copied),
        (
            "merge_head_text_bytes_copied",
            spill.merge_head_text_bytes_copied,
        ),
        (
            "selection_text_bytes_copied",
            spill.selection_text_bytes_copied,
        ),
    ] {
        push_field(fields, format!("{prefix}{name}"), value.to_string());
    }
    for (name, value) in [
        ("buffer_capacity_records", spill.buffer_capacity_records),
        (
            "buffer_capacity_text_bytes",
            spill.buffer_capacity_text_bytes,
        ),
        ("min_run_block_rows", spill.min_run_block_rows),
        ("max_run_block_rows", spill.max_run_block_rows),
        ("max_run_key_bytes", spill.max_run_key_bytes),
        ("max_admitted_key_bytes", spill.max_admitted_key_bytes),
        ("merge_fan_in", spill.merge_fan_in),
    ] {
        push_field(fields, format!("{prefix}{name}"), value.to_string());
    }
    push_bool_field(
        fields,
        format!("{prefix}owned_cleanup_completed"),
        spill.owned_cleanup_completed,
    );
    push_field(
        fields,
        format!("{prefix}scope"),
        "explicit_weighted_complete_key_count_policy;query_pool_parent_envelope_through_source_validation;bounded_records_utf8_arena_run_metadata_merge_and_eof_selection;actual_per_run_geometry;lazy_owned_workspace;source_provider_JSON_and_RSS_separate",
    );
}

fn append_native_aggregate_spill_fields(
    fields: &mut Vec<(String, String)>,
    spill: &shardloom_vortex::VortexAggregateSpillReport,
) {
    let prefix = "local_primitive_native_aggregate_spill_";
    push_field(fields, format!("{prefix}family"), &spill.family);
    push_field(
        fields,
        format!("{prefix}workspace"),
        spill.workspace.display().to_string(),
    );
    for (name, value) in [
        ("quota_bytes", spill.quota_bytes),
        ("memory_bytes", spill.memory_bytes),
        ("peak_reserved_bytes", spill.peak_reserved_bytes),
        ("peak_disk_bytes", spill.peak_disk_bytes),
        ("runs_written", spill.runs_written),
        ("runs_validated", spill.runs_validated),
        ("merge_passes", spill.merge_passes),
        ("source_rows", spill.source_rows),
        ("complete_pairs", spill.complete_pairs),
    ] {
        push_field(fields, format!("{prefix}{name}"), value.to_string());
    }
    for (name, value) in [
        ("groups", spill.groups),
        ("buffer_capacity_pairs", spill.buffer_capacity_pairs),
        ("run_block_rows", spill.run_block_rows),
        ("merge_fan_in", spill.merge_fan_in),
    ] {
        push_field(fields, format!("{prefix}{name}"), value.to_string());
    }
    push_bool_field(
        fields,
        format!("{prefix}owned_cleanup_completed"),
        spill.owned_cleanup_completed,
    );
    push_field(
        fields,
        format!("{prefix}scope"),
        "explicit_exact_integer_distinct_policy;query_pool_parent_envelope_retained_through_result;child_pool_owned_buffer_runs_merge_and_selected_state;workspace_created_only_when_buffer_flush_requires_it;provider_transients_and_process_rss_not_bounded",
    );
}

#[allow(clippy::too_many_lines)]
fn append_local_primitive_memory_admission_fields(
    fields: &mut Vec<(String, String)>,
    resource_envelope: &shardloom_vortex::VortexLocalPrimitiveResourceEnvelope,
    state_budget: &shardloom_vortex::VortexLocalPrimitiveStateBudgetReport,
) {
    push_field(
        fields,
        "local_primitive_memory_admission_schema_version",
        "shardloom.memory_admission.v1",
    );
    push_field(
        fields,
        "local_primitive_memory_admission_scope",
        "public_local_vortex_primitive_state_budget",
    );
    push_bool_field(
        fields,
        "local_primitive_memory_reservation_required",
        state_budget.state_budget_required,
    );
    push_field(
        fields,
        "local_primitive_memory_reservation_owner_class",
        local_primitive_memory_owner_class(&state_budget.state_family).as_str(),
    );
    push_field(
        fields,
        "local_primitive_memory_reservation_owner_label",
        &state_budget.state_family,
    );

    let requested_bytes = local_primitive_memory_reservation_request_bytes(state_budget);
    push_field(
        fields,
        "local_primitive_memory_admission_measurement_basis",
        if state_budget.native_sort_spill.is_some() {
            "post_execution_descriptor_check_of_measured_sort_spill_peak;runtime_scope_owned_candidates_merge_batches_run_metadata_checksum_scratch"
        } else if state_budget.native_aggregate_spill.is_some()
            || state_budget.native_weighted_count_spill.is_some()
        {
            "post_execution_descriptor_check_of_measured_aggregate_spill_peak;runtime_query_pool_parent_envelope_and_child_pool_owned_state_reported_separately"
        } else {
            "post_execution_descriptor_check_not_runtime_allocation_enforcement"
        },
    );
    push_field(
        fields,
        "local_primitive_memory_reservation_requested_bytes",
        requested_bytes.to_string(),
    );

    if requested_bytes == 0 {
        push_field(
            fields,
            "local_primitive_memory_reservation_status",
            "not_required",
        );
        push_field(
            fields,
            "local_primitive_memory_admission_decision",
            "not_requested",
        );
        push_field(fields, "local_primitive_memory_pressure_before", "normal");
        push_field(fields, "local_primitive_memory_pressure_after", "normal");
        push_field(fields, "local_primitive_memory_reserved_before_bytes", "0");
        push_field(fields, "local_primitive_memory_reserved_after_bytes", "0");
        push_field(fields, "local_primitive_memory_granted_bytes", "0");
        push_bool_field(fields, "local_primitive_memory_fail_before_oom", false);
        push_bool_field(
            fields,
            "local_primitive_memory_reservation_release_performed",
            false,
        );
        push_field(
            fields,
            "local_primitive_memory_reserved_after_release_bytes",
            "0",
        );
        push_bool_field(fields, "local_primitive_memory_fallback_attempted", false);
        return;
    }

    let Ok(budget) = shardloom_exec::MemoryBudget::new(shardloom_exec::ByteSize::from_bytes(
        resource_envelope.memory_budget_bytes,
    )) else {
        append_local_primitive_memory_admission_error_fields(fields, "invalid_budget");
        return;
    };
    let owner_class = local_primitive_memory_owner_class(&state_budget.state_family);
    let Ok(owner) = shardloom_exec::MemoryOwner::new(
        owner_class,
        format!("local_vortex_primitive.{}", state_budget.state_family),
    ) else {
        append_local_primitive_memory_admission_error_fields(fields, "invalid_owner");
        return;
    };
    let Ok(reservation_id) =
        shardloom_exec::MemoryReservationId::new("public_local_vortex_primitive_state")
    else {
        append_local_primitive_memory_admission_error_fields(fields, "invalid_reservation_id");
        return;
    };

    let mut pool = shardloom_exec::MemoryPoolPlan::new(budget);
    let Ok(report) = pool.admit_reservation(
        reservation_id.clone(),
        owner,
        shardloom_exec::ByteSize::from_bytes(requested_bytes),
    ) else {
        append_local_primitive_memory_admission_error_fields(fields, "admission_error");
        return;
    };

    push_field(
        fields,
        "local_primitive_memory_reservation_status",
        report.reservation.status.as_str(),
    );
    push_field(
        fields,
        "local_primitive_memory_admission_decision",
        report.decision.as_str(),
    );
    push_field(
        fields,
        "local_primitive_memory_pressure_before",
        report.pressure_before.as_str(),
    );
    push_field(
        fields,
        "local_primitive_memory_pressure_after",
        report.pressure_after.as_str(),
    );
    push_field(
        fields,
        "local_primitive_memory_reserved_before_bytes",
        report.reserved_before.as_bytes().to_string(),
    );
    push_field(
        fields,
        "local_primitive_memory_reserved_after_bytes",
        report.reserved_after.as_bytes().to_string(),
    );
    push_field(
        fields,
        "local_primitive_memory_granted_bytes",
        report.reservation.granted.as_bytes().to_string(),
    );
    push_bool_field(
        fields,
        "local_primitive_memory_fail_before_oom",
        report.fail_before_oom,
    );
    push_bool_field(
        fields,
        "local_primitive_memory_fallback_attempted",
        report.fallback_attempted,
    );

    let release_performed =
        report.granted_decision() && pool.release_reservation(&reservation_id).is_ok();
    push_bool_field(
        fields,
        "local_primitive_memory_reservation_release_performed",
        release_performed,
    );
    push_field(
        fields,
        "local_primitive_memory_reserved_after_release_bytes",
        pool.reserved_bytes().as_bytes().to_string(),
    );
}

fn append_local_primitive_memory_admission_error_fields(
    fields: &mut Vec<(String, String)>,
    status: &str,
) {
    push_field(fields, "local_primitive_memory_reservation_status", status);
    push_field(
        fields,
        "local_primitive_memory_admission_decision",
        "not_requested",
    );
    push_field(fields, "local_primitive_memory_pressure_before", "unknown");
    push_field(fields, "local_primitive_memory_pressure_after", "unknown");
    push_field(fields, "local_primitive_memory_reserved_before_bytes", "0");
    push_field(fields, "local_primitive_memory_reserved_after_bytes", "0");
    push_field(fields, "local_primitive_memory_granted_bytes", "0");
    push_bool_field(fields, "local_primitive_memory_fail_before_oom", true);
    push_bool_field(
        fields,
        "local_primitive_memory_reservation_release_performed",
        false,
    );
    push_field(
        fields,
        "local_primitive_memory_reserved_after_release_bytes",
        "0",
    );
    push_bool_field(fields, "local_primitive_memory_fallback_attempted", false);
}

fn local_primitive_memory_reservation_request_bytes(
    state_budget: &shardloom_vortex::VortexLocalPrimitiveStateBudgetReport,
) -> u64 {
    if let Some(spill) = &state_budget.native_sort_spill {
        return spill.peak_reserved_bytes;
    }
    if let Some(spill) = &state_budget.native_aggregate_spill {
        return spill.peak_reserved_bytes;
    }
    if let Some(spill) = &state_budget.native_weighted_count_spill {
        return spill.peak_reserved_bytes;
    }
    if !state_budget.state_budget_required {
        return 0;
    }
    let item_count = state_budget
        .estimated_state_items
        .unwrap_or(state_budget.observed_state_items)
        .max(state_budget.observed_state_items)
        .max(1);
    item_count.saturating_mul(128)
}

fn local_primitive_memory_owner_class(state_family: &str) -> shardloom_exec::OperatorMemoryClass {
    if state_family.contains("aggregate") || state_family.contains("group") {
        shardloom_exec::OperatorMemoryClass::Aggregate
    } else if state_family.contains("sort") || state_family.contains("top") {
        shardloom_exec::OperatorMemoryClass::Sort
    } else if state_family.contains("join") {
        shardloom_exec::OperatorMemoryClass::Join
    } else if state_family.contains("rolling") || state_family.contains("window") {
        shardloom_exec::OperatorMemoryClass::Window
    } else if state_family.contains("row_export") || state_family.contains("sink") {
        shardloom_exec::OperatorMemoryClass::Sink
    } else {
        shardloom_exec::OperatorMemoryClass::Unknown
    }
}

#[allow(clippy::too_many_lines)]
fn append_local_primitive_embedded_layout_fields(
    fields: &mut Vec<(String, String)>,
    embedded_layout: &shardloom_vortex::VortexLocalPrimitiveEmbeddedLayoutReport,
) {
    push_field(
        fields,
        "local_primitive_embedded_layout_schema_version",
        &embedded_layout.schema_version,
    );
    push_field(
        fields,
        "local_primitive_embedded_layout_status",
        &embedded_layout.status,
    );
    push_field(
        fields,
        "local_primitive_footer_row_count",
        embedded_layout.footer_row_count.to_string(),
    );
    push_field(
        fields,
        "local_primitive_footer_segment_count",
        embedded_layout.footer_segment_count.to_string(),
    );
    push_bool_field(
        fields,
        "local_primitive_footer_statistics_available",
        embedded_layout.footer_statistics_available,
    );
    push_field(
        fields,
        "local_primitive_footer_statistics_status",
        &embedded_layout.footer_statistics_status,
    );
    push_field(
        fields,
        "local_primitive_footer_approx_bytes",
        embedded_layout
            .footer_approx_bytes
            .map_or_else(|| "none".to_string(), |value| value.to_string()),
    );
    push_field(
        fields,
        "local_primitive_footer_dtype_summary",
        &embedded_layout.footer_dtype_summary,
    );
    push_field(
        fields,
        "local_primitive_footer_layout_summary",
        &embedded_layout.footer_layout_summary,
    );
    push_field(
        fields,
        "local_primitive_root_layout_encoding",
        &embedded_layout.root_layout_encoding,
    );
    push_field(
        fields,
        "local_primitive_layout_encoding_inventory",
        &embedded_layout.layout_encoding_inventory,
    );
    push_field(
        fields,
        "local_primitive_layout_inventory_scope",
        &embedded_layout.layout_inventory_scope,
    );
    push_field(
        fields,
        "local_primitive_layout_inventory_nodes_inspected",
        embedded_layout.layout_inventory_nodes_inspected.to_string(),
    );
    push_field(
        fields,
        "local_primitive_per_column_metadata_contract",
        &embedded_layout.per_column_metadata_contract,
    );
    push_field(
        fields,
        "local_primitive_segment_membership_status",
        &embedded_layout.segment_membership_status,
    );
    push_field(
        fields,
        "local_primitive_domain_dictionary_status",
        &embedded_layout.domain_dictionary_status,
    );
    push_field(
        fields,
        "local_primitive_derived_layout_stats_status",
        &embedded_layout.derived_layout_stats_status,
    );
    push_field(
        fields,
        "local_primitive_row_position_locality_status",
        &embedded_layout.row_position_locality_status,
    );
    push_field(
        fields,
        "local_primitive_layout_reader_cache_status",
        &embedded_layout.layout_reader_cache_status,
    );
    push_bool_field(
        fields,
        "local_primitive_metadata_persisted_in_artifact",
        embedded_layout.metadata_persisted_in_artifact,
    );
    push_bool_field(
        fields,
        "local_primitive_metadata_first_pruning_available",
        embedded_layout.metadata_first_pruning_available,
    );
    push_field(
        fields,
        "local_primitive_metadata_elimination_stage",
        "after_vortex_normalization_before_operator_execution",
    );
    push_field(
        fields,
        "local_primitive_metadata_elimination_outcome",
        local_primitive_metadata_elimination_outcome(embedded_layout),
    );
    push_bool_field(
        fields,
        "local_primitive_metadata_elimination_attempted",
        embedded_layout.metadata_persisted_in_artifact,
    );
    push_bool_field(
        fields,
        "local_primitive_metadata_first_pruning_consulted",
        embedded_layout.metadata_first_pruning_consulted,
    );
    push_bool_field(
        fields,
        "local_primitive_metadata_pruned_entire_input",
        embedded_layout.metadata_pruned_entire_input,
    );
    push_field(
        fields,
        "local_primitive_selected_segment_count",
        embedded_layout.selected_segment_count.to_string(),
    );
    push_field(
        fields,
        "local_primitive_skipped_segment_count",
        embedded_layout.skipped_segment_count.to_string(),
    );
    push_field(
        fields,
        "local_primitive_rows_avoided_by_metadata",
        if embedded_layout.metadata_pruned_entire_input {
            embedded_layout.footer_row_count.to_string()
        } else {
            "0".to_string()
        },
    );
    push_bool_field(
        fields,
        "local_primitive_decode_avoided_by_metadata",
        embedded_layout.metadata_pruned_entire_input
            || embedded_layout
                .planner_consumption_status
                .contains("metadata_row_count"),
    );
    push_bool_field(
        fields,
        "local_primitive_materialization_avoided_by_metadata",
        embedded_layout.metadata_pruned_entire_input,
    );
    push_bool_field(
        fields,
        "local_primitive_vortex_file_stats_reader_available",
        embedded_layout.vortex_file_stats_reader_available,
    );
    push_field(
        fields,
        "local_primitive_planner_consumption_status",
        &embedded_layout.planner_consumption_status,
    );
    push_field(
        fields,
        "local_primitive_operator_selection_metadata_status",
        &embedded_layout.operator_selection_metadata_status,
    );
    push_field(
        fields,
        "local_primitive_dictionary_encoding_policy",
        &embedded_layout.dictionary_encoding_policy,
    );
    push_field(
        fields,
        "local_primitive_late_materialization_status",
        &embedded_layout.late_materialization_status,
    );
    push_bool_field(
        fields,
        "local_primitive_no_query_answer_cache",
        embedded_layout.no_query_answer_cache,
    );
}

fn local_primitive_metadata_elimination_outcome(
    embedded_layout: &shardloom_vortex::VortexLocalPrimitiveEmbeddedLayoutReport,
) -> &'static str {
    if !embedded_layout.metadata_persisted_in_artifact {
        return "metadata_unavailable_operator_execution_required";
    }
    if embedded_layout.metadata_pruned_entire_input {
        return "metadata_pruned_entire_input";
    }
    if embedded_layout
        .planner_consumption_status
        .contains("metadata_row_count")
    {
        return "metadata_answered_without_row_scan";
    }
    if embedded_layout.metadata_first_pruning_consulted {
        return "metadata_consulted_operator_execution_required";
    }
    if embedded_layout.metadata_first_pruning_available {
        return "metadata_available_not_consulted_for_this_shape";
    }
    "metadata_recorded_for_operator_selection"
}

fn native_vortex_primitive_row_export_typed_sink_contract(output_format: &str) -> &'static str {
    match output_format {
        "vortex" => "native_vortex_structured_row_stream_to_vortex_sink",
        "parquet" | "arrow-ipc" | "avro" => {
            "native_vortex_structured_row_stream_to_parquet_arrow_avro_compatibility_sink"
        }
        "orc" => "native_vortex_result_to_checked_orc_compatibility_sink",
        "json" => "native_vortex_row_stream_to_json_array_compatibility_sink",
        _ => "native_vortex_primitive_row_stream_to_jsonl_csv_compatibility_sink",
    }
}

fn append_native_vortex_primitive_row_export_target_fields(
    fields: &mut Vec<(String, String)>,
    targets: &[NativeVortexPrimitiveRowExportTarget],
    reports: &[shardloom_vortex::VortexLocalPrimitiveRowExportReport],
) {
    append_native_vortex_primitive_row_export_target_fields_with_execution(
        fields,
        targets,
        reports,
        1,
        if targets.len() > 1 {
            "native_query_once_with_credited_result_replay"
        } else {
            "native_query_once_direct_output"
        },
    );
}

fn append_native_vortex_primitive_row_export_target_fields_with_execution(
    fields: &mut Vec<(String, String)>,
    targets: &[NativeVortexPrimitiveRowExportTarget],
    reports: &[shardloom_vortex::VortexLocalPrimitiveRowExportReport],
    execution_count: usize,
    execution_model: &str,
) {
    append_native_vortex_primitive_row_export_target_summary_fields(
        fields,
        targets,
        reports,
        execution_count,
        execution_model,
    );
    append_native_vortex_primitive_row_export_target_commit_fields(fields, targets, reports);
}

fn append_native_vortex_primitive_row_export_target_summary_fields(
    fields: &mut Vec<(String, String)>,
    targets: &[NativeVortexPrimitiveRowExportTarget],
    reports: &[shardloom_vortex::VortexLocalPrimitiveRowExportReport],
    execution_count: usize,
    execution_model: &str,
) {
    push_field(
        fields,
        "native_vortex_result_export_target_count",
        targets.len().to_string(),
    );
    push_field(
        fields,
        "native_vortex_result_export_fanout_count",
        targets.len().saturating_sub(1).to_string(),
    );
    push_bool_field(
        fields,
        "native_vortex_result_export_fanout_performed",
        targets.len() > 1,
    );
    push_field(
        fields,
        "native_vortex_result_export_execution_count",
        execution_count.to_string(),
    );
    push_field(
        fields,
        "native_vortex_result_export_execution_model",
        execution_model,
    );
    push_field(
        fields,
        "native_vortex_result_export_target_roles",
        targets
            .iter()
            .map(|target| target.role)
            .collect::<Vec<_>>()
            .join(","),
    );
    push_field(
        fields,
        "native_vortex_result_export_target_formats",
        targets
            .iter()
            .map(|target| target.format.as_str())
            .collect::<Vec<_>>()
            .join(","),
    );
    push_field(
        fields,
        "native_vortex_result_export_target_paths",
        targets
            .iter()
            .map(|target| target.path.display().to_string())
            .collect::<Vec<_>>()
            .join(","),
    );
    push_field(
        fields,
        "native_vortex_result_export_target_rows_written",
        reports
            .iter()
            .map(|report| report.rows_written.to_string())
            .collect::<Vec<_>>()
            .join(","),
    );
}

fn append_native_vortex_primitive_row_export_target_commit_fields(
    fields: &mut Vec<(String, String)>,
    targets: &[NativeVortexPrimitiveRowExportTarget],
    reports: &[shardloom_vortex::VortexLocalPrimitiveRowExportReport],
) {
    let all_targets_committed =
        reports.len() == targets.len() && reports.iter().all(|report| !report.has_errors());
    let multi_target = targets.len() > 1;
    push_field(
        fields,
        "native_vortex_result_export_target_commit_modes",
        row_export_target_report_evidence_field(targets, reports, |report| {
            if multi_target || report.evidence.native_array_sink.is_some() {
                "atomic_create_if_absent_hard_link_same_directory"
            } else {
                "atomic_rename_same_directory"
            }
        }),
    );
    push_field(
        fields,
        "native_vortex_result_export_target_commit_statuses",
        row_export_target_report_evidence_field(targets, reports, |report| {
            if report.has_errors() {
                "not_committed"
            } else {
                "committed"
            }
        }),
    );
    push_field(
        fields,
        "native_vortex_result_export_target_temp_cleanup_statuses",
        row_export_target_report_evidence_field(targets, reports, |report| {
            if report.has_errors() {
                "cleanup_required"
            } else if multi_target {
                "staging_removed_after_commit"
            } else {
                "temp_removed_after_commit"
            }
        }),
    );
    push_field(
        fields,
        "native_vortex_result_export_target_replay_statuses",
        row_export_target_report_evidence_field(targets, reports, |report| {
            if report.has_errors() {
                "not_replayed"
            } else {
                "committed_row_count_recorded"
            }
        }),
    );
    push_field(
        fields,
        "native_vortex_result_export_target_fidelity_statuses",
        row_export_target_report_evidence_field(targets, reports, |report| {
            native_vortex_row_export_fidelity_status(report.output_format)
        }),
    );
    push_bool_field(
        fields,
        "native_vortex_result_export_all_targets_committed",
        all_targets_committed,
    );
    push_field(
        fields,
        "native_vortex_result_export_fanout_atomicity_contract",
        if targets.len() > 1 {
            "all_targets_staged_before_create_if_absent_publication_partial_commits_preserved"
        } else if reports
            .first()
            .is_some_and(|report| report.evidence.native_array_sink.is_some())
        {
            "single_target_atomic_create_if_absent_hard_link_same_directory"
        } else {
            "single_target_same_directory_atomic_rename"
        },
    );
    push_field(
        fields,
        "native_vortex_result_export_partial_write_cleanup_status",
        if all_targets_committed {
            if targets.len() > 1 {
                "not_needed_all_targets_committed"
            } else {
                "not_needed_single_target_committed"
            }
        } else {
            "owned_staging_cleanup_partial_commits_preserved"
        },
    );
}

fn row_export_target_report_evidence_field(
    targets: &[NativeVortexPrimitiveRowExportTarget],
    reports: &[shardloom_vortex::VortexLocalPrimitiveRowExportReport],
    value: impl Fn(&shardloom_vortex::VortexLocalPrimitiveRowExportReport) -> &'static str,
) -> String {
    targets
        .iter()
        .enumerate()
        .map(|(index, target)| {
            let evidence = match reports.get(index) {
                Some(report) => value(report),
                None => "not_reported",
            };
            format!("{}:{}:{evidence}", target.role, target.format.as_str())
        })
        .collect::<Vec<_>>()
        .join(",")
}

fn native_vortex_row_export_fidelity_status(output_format: &str) -> &'static str {
    match output_format {
        "vortex" => "native_vortex_logical_types_preserved_physical_fidelity_reported",
        "parquet" | "arrow-ipc" | "avro" | "orc" => "typed_compatibility_binary_export",
        "json" | "jsonl" | "csv" => "compatibility_text_row_export_type_metadata_not_preserved",
        _ => "unknown_output_format",
    }
}

fn execute_native_vortex_primitive_run(
    request: &PublicWorkflowRouteRequest,
    plan: &PublicWorkflowRoutePlan,
    format: OutputFormat,
    execution_session: &mut PublicExecutionSession,
    admission: &CommandAdmission<'_>,
) -> ExitCode {
    execute_native_vortex_primitive_run_with_extra(
        request,
        plan,
        format,
        Vec::new(),
        execution_session,
        admission,
    )
}

#[path = "native_vortex_input_binding.rs"]
mod input_binding;
use input_binding::NativeVortexInputBinding;

fn native_vortex_input_binding_for_request(
    request: &PublicWorkflowRouteRequest,
    admission: &CommandAdmission<'_>,
) -> Result<NativeVortexInputBinding, ShardLoomError> {
    let input_uri = request.input_uri.as_ref().ok_or_else(|| {
        ShardLoomError::InvalidOperation("public native Vortex run requires --input".to_string())
    })?;
    native_vortex_input_binding_for_uri(input_uri, admission, &admission.cancellation())
}

fn native_vortex_input_binding_for_uri(
    input_uri: &str,
    memory: &shardloom_exec::live_memory::LiveMemoryPool,
    cancellation: &shardloom_exec::compute_pool::CancellationToken,
) -> Result<NativeVortexInputBinding, ShardLoomError> {
    input_binding::bind(input_uri, memory, cancellation)
}

fn native_vortex_primitive_materializes(primitive: PublicVortexPrimitive) -> bool {
    matches!(
        primitive,
        PublicVortexPrimitive::Distinct
            | PublicVortexPrimitive::DropDuplicates
            | PublicVortexPrimitive::DuplicateMask
            | PublicVortexPrimitive::Tail
            | PublicVortexPrimitive::Sample
            | PublicVortexPrimitive::ExpressionProject
            | PublicVortexPrimitive::Melt
            | PublicVortexPrimitive::Explode
            | PublicVortexPrimitive::Pivot
            | PublicVortexPrimitive::RollingWindow
            | PublicVortexPrimitive::Aggregate
            | PublicVortexPrimitive::SortRows
    )
}

#[allow(clippy::too_many_lines)]
fn execute_native_vortex_primitive_run_with_extra(
    request: &PublicWorkflowRouteRequest,
    plan: &PublicWorkflowRoutePlan,
    format: OutputFormat,
    mut extra_fields: Vec<(String, String)>,
    execution_session: &mut PublicExecutionSession,
    admission: &CommandAdmission<'_>,
) -> ExitCode {
    #[cfg(all(feature = "vortex-local-primitives", unix))]
    if let Some(exit) = resident_sql::run_if_needed(
        request,
        plan,
        format,
        &mut extra_fields,
        execution_session,
        PublicSourcePreparations::default(),
        admission,
    ) {
        return exit;
    }
    if request.requested_output != "collect" {
        execution_session.clear_operation_slots();
    }
    let Some(primitive) = normalized_vortex_primitive(request) else {
        let blocked = native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_primitive",
            "public native Vortex run requires a primitive payload",
            "pass --vortex-primitive with count, count_where, filter, project, filter_project, distinct, drop_duplicates, duplicate_mask, tail, sample, expression_project, melt, explode, pivot, rolling_window, aggregate, or sort_rows",
        );
        return emit_blocked_facade("run", format, request, &blocked);
    };
    if native_vortex_primitive_materializes(primitive) {
        return execute_native_vortex_materializing_with_session(
            request,
            plan,
            format,
            extra_fields,
            execution_session,
            primitive,
            admission,
        );
    }
    let binding = match native_vortex_input_binding_for_request(request, admission) {
        Ok(binding) => binding,
        Err(error) => {
            execution_session.clear_operation_slots();
            execution_session.evict_prepared_request_inputs(request);
            return emit_error(
                "run",
                format,
                "public native Vortex input binding failed",
                &error,
            );
        }
    };
    if binding.mode != "single_file" {
        execution_session.clear_operation_slots();
        execution_session.evict_prepared_request_inputs(request);
        execution_session.evict_prepared_binding(&binding);
        return execute_native_vortex_bound_primitive_run_with_extra(
            request,
            plan,
            format,
            extra_fields,
            primitive,
            &binding,
            admission,
        );
    }
    #[cfg(all(feature = "vortex-local-primitives", unix))]
    if request.requested_output == "collect" && primitive == PublicVortexPrimitive::Count {
        return resident_count::execute_native_vortex_resident_count(
            request,
            plan,
            format,
            extra_fields,
            &binding,
            execution_session,
            admission,
        );
    }
    #[cfg(all(feature = "vortex-local-primitives", unix))]
    if request.requested_output == "collect" && primitive == PublicVortexPrimitive::CountWhere {
        return resident_count_where::run(
            request,
            plan,
            format,
            extra_fields,
            &binding,
            execution_session,
            admission,
        );
    }
    #[cfg(all(feature = "vortex-local-primitives", unix))]
    if request.requested_output == "collect"
        && matches!(
            primitive,
            PublicVortexPrimitive::Filter
                | PublicVortexPrimitive::Project
                | PublicVortexPrimitive::FilterProject
        )
    {
        return execute_native_vortex_owned_collect(
            request,
            plan,
            format,
            extra_fields,
            primitive,
            &binding,
            execution_session,
            admission,
        );
    }
    let runtime_args = match native_vortex_primitive_runtime_args(request, primitive) {
        Ok(args) => args,
        Err(error) => {
            return emit_error("run", format, "public native Vortex run failed", &error);
        }
    };
    let mut attachment_fields = execution_attachment_fields("run", request, plan);
    attachment_fields.append(&mut extra_fields);
    execute_native_vortex_single_primitive_run(
        primitive,
        runtime_args,
        format,
        attachment_fields,
        admission,
    )
}

fn execute_native_vortex_materializing_with_session(
    request: &PublicWorkflowRouteRequest,
    plan: &PublicWorkflowRoutePlan,
    format: OutputFormat,
    extra_fields: Vec<(String, String)>,
    execution_session: &mut PublicExecutionSession,
    primitive: PublicVortexPrimitive,
    admission: &CommandAdmission<'_>,
) -> ExitCode {
    #[cfg(all(feature = "vortex-local-primitives", unix))]
    if request.requested_output == "collect" {
        if primitive == PublicVortexPrimitive::Aggregate {
            return resident_aggregate::run(
                request,
                plan,
                format,
                extra_fields,
                execution_session,
                admission,
            );
        }
        if primitive != PublicVortexPrimitive::SortRows {
            return resident_unary::run(
                request,
                plan,
                format,
                extra_fields,
                execution_session,
                primitive,
                admission,
            );
        }
    }
    execution_session.clear_operation_slots();
    execute_native_vortex_materializing_primitive_run_with_extra_and_session(
        request,
        plan,
        format,
        extra_fields,
        Some(execution_session),
        primitive,
        admission,
    )
}

#[cfg(all(feature = "vortex-local-primitives", unix))]
#[allow(clippy::too_many_arguments)] // The existing collect facade now borrows the complete command admission.
fn execute_native_vortex_owned_collect(
    request: &PublicWorkflowRouteRequest,
    plan: &PublicWorkflowRoutePlan,
    format: OutputFormat,
    extra_fields: Vec<(String, String)>,
    primitive: PublicVortexPrimitive,
    binding: &NativeVortexInputBinding,
    execution_session: &mut PublicExecutionSession,
    admission: &CommandAdmission<'_>,
) -> ExitCode {
    execute_native_vortex_owned_collect_with_source(
        request,
        plan,
        format,
        extra_fields,
        primitive,
        binding,
        execution_session,
        None,
        admission,
    )
}

#[cfg(all(feature = "vortex-local-primitives", unix))]
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
fn execute_native_vortex_owned_collect_with_source(
    request: &PublicWorkflowRouteRequest,
    plan: &PublicWorkflowRoutePlan,
    format: OutputFormat,
    mut extra_fields: Vec<(String, String)>,
    primitive: PublicVortexPrimitive,
    binding: &NativeVortexInputBinding,
    execution_session: &mut PublicExecutionSession,
    source: Option<shardloom_vortex::resident_session::PreparedVortexSource>,
    admission: &CommandAdmission<'_>,
) -> ExitCode {
    let result = (|| {
        admission.check()?;
        if request.materialization_policy == "zero_decode" {
            return Err(ShardLoomError::InvalidOperation(
                "JSON collect requires scalar decoding at its sink; use --materialization-policy bounded or the native Rust array result API; no fallback execution was attempted".to_string(),
            ));
        }
        let (primitive_request, _, _) =
            native_vortex_bound_request_and_arg(request, primitive, binding)?;
        native_vortex_materializing_policy(request)?;
        if !execution_session
            .collect
            .as_ref()
            .is_some_and(|entry| entry.request == *request)
        {
            // Release the previous reader/runtime before admitting the next budget.
            execution_session.clear_operation_slots();
            let native_session = admission.native_session()?;
            let cancellation = admission.cancellation();
            let operation = if let Some(source) = source {
                shardloom_vortex::local_primitives::collect::prepare_rows_from_source_controlled(
                    &primitive_request,
                    source,
                    &cancellation,
                )?
            } else if let Some(uri) = primitive_request.source_uri.as_ref()
                && let Some((source, _)) = execution_session.get_or_prepare_uri_source(
                    uri,
                    native_session,
                    &cancellation,
                )?
            {
                shardloom_vortex::local_primitives::collect::prepare_rows_from_source_controlled(
                    &primitive_request,
                    source,
                    &cancellation,
                )?
            } else {
                shardloom_vortex::local_primitives::collect::prepare_rows_in_session_controlled(
                    &primitive_request,
                    native_session,
                    &cancellation,
                )?
            };
            execution_session.collect = Some(PreparedPublicCollect {
                request: request.clone(),
                operation,
            });
        }
        execution_session
            .collect
            .as_ref()
            .ok_or_else(|| {
                ShardLoomError::InvalidOperation("prepared collect was not admitted".to_string())
            })?
            .operation
            .execute_with_cancellation(&admission.cancellation())
    })();
    let result = match result {
        Ok(result) => result,
        Err(error) => {
            execution_session.clear_operation_slots();
            execution_session.evict_prepared_request_inputs(request);
            execution_session.evict_prepared_binding(binding);
            return native_vortex_materializing_error(format, primitive, &error);
        }
    };
    let mut fields = execution_attachment_fields("run", request, plan);
    fields.append(&mut extra_fields);
    fields.extend(binding.evidence_fields());
    append_owned_collect_fields(&mut fields, primitive, &result);
    crate::execution_resources::append_query_timing_fields(&mut fields, result.timing);
    let (schema, _schema_ownership) = result.result_schema_json.into_parts();
    let (values_json, _json_ownership) = result.values_json.into_parts();
    append_native_result_schema_fields(&mut fields, schema);
    fields.extend([
        ("result_values_json".into(), values_json),
        ("result_payload_complete".into(), "true".into()),
        ("result_known".to_string(), "true".to_string()),
        ("output_row_count".to_string(), result.rows.to_string()),
        ("fallback_attempted".to_string(), "false".to_string()),
        ("external_engine_invoked".to_string(), "false".to_string()),
        ("data_read".to_string(), "true".to_string()),
        ("data_decoded".to_string(), "true".to_string()),
        ("data_materialized".to_string(), "true".to_string()),
        ("row_read".to_string(), "true".to_string()),
        (
            "local_primitive_no_query_answer_cache".to_string(),
            "true".to_string(),
        ),
        (
            "native_result_payload".to_string(),
            "owned_vortex_arrays_to_bounded_json_sink".to_string(),
        ),
        (
            "resident_source_opens".to_string(),
            result.runtime.prepared_source_opens.to_string(),
        ),
        (
            "resident_completed_executions".to_string(),
            result.runtime.completed_executions.to_string(),
        ),
        (
            "resident_peak_reserved_buffer_bytes".to_string(),
            result.runtime.memory.peak_reserved_bytes.to_string(),
        ),
    ]);
    emit(
        "run",
        format,
        CommandStatus::Success,
        "native Vortex bounded row collect".to_string(),
        format!("collected {} native rows", result.rows),
        Vec::new(),
        fields,
    );
    ExitCode::SUCCESS
}

#[cfg(all(feature = "vortex-local-primitives", unix))]
fn append_native_result_schema_fields(fields: &mut Vec<(String, String)>, schema: String) {
    fields.extend([
        ("result_schema_json".into(), schema),
        (
            "result_schema_format".into(),
            "vortex.dtype.serde.v1".into(),
        ),
    ]);
}

#[cfg(all(feature = "vortex-local-primitives", unix))]
fn append_owned_collect_fields(
    fields: &mut Vec<(String, String)>,
    primitive: PublicVortexPrimitive,
    result: &shardloom_vortex::local_primitives::collect::CollectedVortexRows,
) {
    let (prefix, kind) = match primitive {
        PublicVortexPrimitive::Project => ("project", "project_columns"),
        PublicVortexPrimitive::FilterProject => ("filter_project", "filter_and_project"),
        _ => ("filter", "filter_predicate"),
    };
    fields.extend([
        ("mode".into(), format!("vortex_{prefix}")),
        ("primitive".into(), kind.into()),
        (
            "execution".into(),
            "resident_vortex_scan_to_json_sink".into(),
        ),
        ("rows_projected".into(), result.rows.to_string()),
        ("columns".into(), result.projected_columns.join(",")),
        ("resident_provider_crate".into(), "vortex".into()),
        (
            "resident_provider_version".into(),
            shardloom_vortex::UPSTREAM_VORTEX_PROVIDER_VERSION.into(),
        ),
        (
            "resident_decoded_byte_accounting".into(),
            "not_instrumented".into(),
        ),
    ]);
    for (key, value) in [
        ("status", "executed".into()),
        ("result_known", "true".into()),
        ("rows_projected", result.rows.to_string()),
        ("projected_columns", result.projected_columns.join(",")),
        (
            "source_order_limit_requested",
            result
                .source_order_limit
                .map_or_else(|| "none".into(), |limit| limit.to_string()),
        ),
        (
            "source_order_limit_applied",
            result.source_order_limit.is_some().to_string(),
        ),
        ("data_read", "true".into()),
        ("data_decoded", (result.rows > 0).to_string()),
        ("data_materialized", (result.rows > 0).to_string()),
        ("fallback_attempted", "false".into()),
        ("external_engine_invoked", "false".into()),
    ] {
        fields.push((format!("{prefix}_local_execution_{key}"), value));
    }
    vortex_primitive_execution::append_vortex_local_primitive_native_io_certificate_fields(
        fields,
        Some(&result.native_io_certificate),
    );
    // No independent correctness oracle is run as part of ordinary execution.
    vortex_primitive_execution::append_vortex_local_primitive_execution_certificate_fields(
        fields, None,
    );
}

fn execute_native_vortex_single_primitive_run(
    primitive: PublicVortexPrimitive,
    runtime_args: Vec<String>,
    format: OutputFormat,
    attachment_fields: Vec<(String, String)>,
    admission: &CommandAdmission<'_>,
) -> ExitCode {
    if let Err(error) = admission.check() {
        return native_vortex_materializing_error(format, primitive, &error);
    }
    match primitive {
        PublicVortexPrimitive::Count => vortex_primitive_execution::handle_vortex_run_with_facade(
            runtime_args.into_iter(),
            format,
            "run",
            attachment_fields,
            admission,
        ),
        PublicVortexPrimitive::CountWhere => {
            vortex_primitive_execution::handle_vortex_count_where_with_facade(
                runtime_args.into_iter(),
                format,
                "run",
                attachment_fields,
                admission,
            )
        }
        PublicVortexPrimitive::Filter => {
            vortex_primitive_execution::handle_vortex_filter_with_facade(
                runtime_args.into_iter(),
                format,
                "run",
                attachment_fields,
                admission,
            )
        }
        PublicVortexPrimitive::Project => {
            vortex_primitive_execution::handle_vortex_project_with_facade(
                runtime_args.into_iter(),
                format,
                "run",
                attachment_fields,
                admission,
            )
        }
        PublicVortexPrimitive::FilterProject => {
            vortex_primitive_execution::handle_vortex_filter_project_with_facade(
                runtime_args.into_iter(),
                format,
                "run",
                attachment_fields,
                admission,
            )
        }
        PublicVortexPrimitive::Distinct
        | PublicVortexPrimitive::DropDuplicates
        | PublicVortexPrimitive::DuplicateMask
        | PublicVortexPrimitive::Tail
        | PublicVortexPrimitive::Sample
        | PublicVortexPrimitive::ExpressionProject
        | PublicVortexPrimitive::Melt
        | PublicVortexPrimitive::Explode
        | PublicVortexPrimitive::Pivot
        | PublicVortexPrimitive::RollingWindow
        | PublicVortexPrimitive::Aggregate
        | PublicVortexPrimitive::SortRows => {
            unreachable!("handled before runtime args")
        }
    }
}

fn execute_admitted_native_primitive(
    request: &shardloom_vortex::VortexQueryPrimitiveRequest,
    sources: Option<&[shardloom_core::DatasetUri]>,
    policy: shardloom_vortex::VortexLocalPrimitiveExecutionPolicy,
    admission: &CommandAdmission<'_>,
) -> Result<shardloom_vortex::VortexLocalPrimitiveExecutionReport, ShardLoomError> {
    admission.check()?;
    #[cfg(all(feature = "vortex-local-primitives", unix))]
    {
        let session = admission.native_session()?;
        let cancellation = admission.cancellation();
        match sources {
            Some(sources) => {
                shardloom_vortex::local_primitives::execution::execute_partitioned_in_session(
                    request,
                    sources,
                    policy,
                    session,
                    &cancellation,
                )
            }
            None => shardloom_vortex::local_primitives::execution::execute_in_session(
                request,
                policy,
                session,
                &cancellation,
            ),
        }
    }
    #[cfg(not(all(feature = "vortex-local-primitives", unix)))]
    match sources {
        Some(sources) => shardloom_vortex::execute_vortex_local_partitioned_primitive_with_policy(
            request, sources, policy,
        ),
        None => shardloom_vortex::execute_vortex_local_primitive_with_policy(request, policy),
    }
}

fn execute_native_vortex_bound_primitive_run_with_extra(
    request: &PublicWorkflowRouteRequest,
    plan: &PublicWorkflowRoutePlan,
    format: OutputFormat,
    mut extra_fields: Vec<(String, String)>,
    primitive: PublicVortexPrimitive,
    binding: &NativeVortexInputBinding,
    admission: &CommandAdmission<'_>,
) -> ExitCode {
    if let Err(error) = admission.check() {
        return native_vortex_materializing_error(format, primitive, &error);
    }
    let (primitive_request, primitive_arg, source_uris) =
        match native_vortex_bound_request_and_arg(request, primitive, binding) {
            Ok(value) => value,
            Err(error) => return native_vortex_materializing_error(format, primitive, &error),
        };
    let policy = match native_vortex_materializing_policy(request) {
        Ok(policy) => policy,
        Err(error) => return native_vortex_materializing_error(format, primitive, &error),
    };
    let report = match execute_admitted_native_primitive(
        &primitive_request,
        Some(&source_uris),
        policy,
        admission,
    ) {
        Ok(report) => report,
        Err(error) => return native_vortex_materializing_error(format, primitive, &error),
    };
    let native_io_certificate =
        shardloom_vortex::local_primitive_native_io_certificate(&primitive_request, &report).ok();
    let execution_certificate =
        native_vortex_materializing_execution_certificate(&primitive_request, &report);
    let mut fields = execution_attachment_fields("run", request, plan);
    fields.append(&mut extra_fields);
    fields.extend(binding.evidence_fields());
    append_native_vortex_materializing_primitive_fields(
        &mut fields,
        &report,
        &primitive_arg,
        native_io_certificate.as_ref(),
        execution_certificate.as_ref(),
    );
    emit(
        "run",
        format,
        if report.has_errors() {
            CommandStatus::Unsupported
        } else {
            CommandStatus::Success
        },
        format!("native Vortex {} primitive", primitive.as_str()),
        report.to_human_text(),
        report.diagnostics.clone(),
        fields,
    );
    if report.has_errors() {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

#[cfg(all(feature = "vortex-local-primitives", unix))]
#[allow(clippy::too_many_arguments)]
fn execute_native_vortex_sort_collect(
    request: &PublicWorkflowRouteRequest,
    plan: &PublicWorkflowRoutePlan,
    format: OutputFormat,
    mut extra_fields: Vec<(String, String)>,
    primitive_request: &shardloom_vortex::VortexQueryPrimitiveRequest,
    binding: &NativeVortexInputBinding,
    execution_session: Option<&mut PublicExecutionSession>,
    source: Option<shardloom_vortex::resident_session::PreparedVortexSource>,
    admission: &CommandAdmission<'_>,
) -> ExitCode {
    use shardloom_vortex::local_primitives::{prepared_dispatch, prepared_relational};
    let mut execution_session = execution_session;
    let result = (|| {
        admission.check()?;
        let policy = native_vortex_materializing_policy(request)?;
        let native_session = admission.native_session()?;
        let cancellation = admission.cancellation();
        prepared_relational::validate_session_policy(policy, native_session)?;
        let source = match source {
            Some(source) => source,
            None => {
                if let (Some(session), Some(uri)) = (
                    execution_session.as_deref_mut(),
                    primitive_request.source_uri.as_ref(),
                ) && let Some((cached, _)) =
                    session.get_or_prepare_uri_source(uri, native_session, &cancellation)?
                {
                    cached
                } else {
                    prepared_dispatch::prepare_source_in_session(
                        primitive_request,
                        policy,
                        native_session,
                        &cancellation,
                    )?
                }
            }
        };
        prepared_dispatch::collect_sort_controlled(
            primitive_request,
            policy,
            &source,
            &cancellation,
        )
    })();
    let result = match result {
        Ok(result) => result,
        Err(error) => {
            if let Some(session) = execution_session {
                session.clear_operation_slots();
                session.evict_prepared_request_inputs(request);
                session.evict_prepared_binding(binding);
            }
            return native_vortex_materializing_error(
                format,
                PublicVortexPrimitive::SortRows,
                &error,
            );
        }
    };
    let mut fields = execution_attachment_fields("run", request, plan);
    fields.append(&mut extra_fields);
    fields.extend(binding.evidence_fields());
    append_native_vortex_materializing_primitive_fields(
        &mut fields,
        &result.report,
        "sort_rows",
        Some(&result.native_io_certificate),
        None,
    );
    crate::execution_resources::append_query_timing_fields(&mut fields, Some(result.timing));
    let (jsonl, _json_ownership) = result.result_jsonl.into_parts();
    let (schema, _schema_ownership) = result.result_schema_json.into_parts();
    append_native_result_schema_fields(&mut fields, schema);
    fields.extend([
        ("result_jsonl".into(), jsonl),
        ("result_payload_complete".into(), "true".into()),
    ]);
    emit(
        "run",
        format,
        CommandStatus::Success,
        "native Vortex sorted rows".into(),
        "complete bounded native sort result".into(),
        Vec::new(),
        fields,
    );
    ExitCode::SUCCESS
}

#[cfg(all(feature = "vortex-local-primitives", unix))]
fn execute_native_vortex_materializing_primitive_run_with_extra(
    request: &PublicWorkflowRouteRequest,
    plan: &PublicWorkflowRoutePlan,
    format: OutputFormat,
    extra_fields: Vec<(String, String)>,
    primitive: PublicVortexPrimitive,
    admission: &CommandAdmission<'_>,
) -> ExitCode {
    execute_native_vortex_materializing_primitive_run_with_extra_and_session(
        request,
        plan,
        format,
        extra_fields,
        None,
        primitive,
        admission,
    )
}

fn execute_native_vortex_materializing_primitive_run_with_extra_and_session(
    request: &PublicWorkflowRouteRequest,
    plan: &PublicWorkflowRoutePlan,
    format: OutputFormat,
    extra_fields: Vec<(String, String)>,
    execution_session: Option<&mut PublicExecutionSession>,
    primitive: PublicVortexPrimitive,
    admission: &CommandAdmission<'_>,
) -> ExitCode {
    execute_native_vortex_materializing_with_source_and_session(
        request,
        plan,
        format,
        extra_fields,
        execution_session,
        primitive,
        #[cfg(all(feature = "vortex-local-primitives", unix))]
        None,
        admission,
    )
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)] // Keep feature-gated source dispatch and its certificate emission together.
fn execute_native_vortex_materializing_with_source_and_session(
    request: &PublicWorkflowRouteRequest,
    plan: &PublicWorkflowRoutePlan,
    format: OutputFormat,
    mut extra_fields: Vec<(String, String)>,
    mut execution_session: Option<&mut PublicExecutionSession>,
    primitive: PublicVortexPrimitive,
    #[cfg(all(feature = "vortex-local-primitives", unix))] source: Option<
        shardloom_vortex::resident_session::PreparedVortexSource,
    >,
    admission: &CommandAdmission<'_>,
) -> ExitCode {
    #[cfg(not(all(feature = "vortex-local-primitives", unix)))]
    let _ = &mut execution_session;
    if let Err(error) = admission.check() {
        return native_vortex_materializing_error(format, primitive, &error);
    }
    if primitive == PublicVortexPrimitive::Aggregate
        && request.materialization_policy == "zero_decode"
    {
        return native_vortex_materializing_error(format, primitive, &ShardLoomError::InvalidOperation(
            "aggregate compute requires admitted native array decoding and typed materialization; use --materialization-policy bounded or the metadata-only count primitive; no fallback execution was attempted".to_string(),
        ));
    }
    if native_vortex_primitive_materializes(primitive)
        && request.materialization_policy == "zero_decode"
    {
        return native_vortex_materializing_error(format, primitive, &ShardLoomError::InvalidOperation(
            "native row computation and collection require admitted materialization; use --materialization-policy bounded; no fallback execution was attempted".to_string(),
        ));
    }
    let binding = match native_vortex_input_binding_for_request(request, admission) {
        Ok(binding) => binding,
        Err(error) => {
            if let Some(session) = execution_session {
                session.clear_operation_slots();
                session.evict_prepared_request_inputs(request);
            }
            return native_vortex_materializing_error(format, primitive, &error);
        }
    };
    if binding.mode != "single_file"
        && let Some(session) = execution_session.as_deref_mut()
    {
        session.clear_operation_slots();
        session.evict_prepared_request_inputs(request);
        session.evict_prepared_binding(&binding);
    }
    let (primitive_request, primitive_arg, source_uris) =
        match native_vortex_bound_request_and_arg(request, primitive, &binding) {
            Ok(value) => value,
            Err(error) => return native_vortex_materializing_error(format, primitive, &error),
        };
    let policy = match native_vortex_materializing_policy(request) {
        Ok(policy) => policy,
        Err(error) => return native_vortex_materializing_error(format, primitive, &error),
    };
    #[cfg(all(feature = "vortex-local-primitives", unix))]
    if primitive == PublicVortexPrimitive::SortRows
        && request.requested_output == "collect"
        && binding.mode == "single_file"
    {
        return execute_native_vortex_sort_collect(
            request,
            plan,
            format,
            extra_fields,
            &primitive_request,
            &binding,
            execution_session,
            source,
            admission,
        );
    }
    #[cfg(all(feature = "vortex-local-primitives", unix))]
    let resolved_sort_source = if primitive == PublicVortexPrimitive::SortRows
        && binding.mode == "single_file"
        && source.is_none()
        && let (Some(session), Some(uri)) = (
            execution_session.as_deref_mut(),
            primitive_request.source_uri.as_ref(),
        ) {
        let native_session = match admission.native_session() {
            Ok(s) => s,
            Err(error) => return native_vortex_materializing_error(format, primitive, &error),
        };
        if let Err(error) =
            shardloom_vortex::local_primitives::prepared_relational::validate_session_policy(
                policy,
                native_session,
            )
        {
            return native_vortex_materializing_error(format, primitive, &error);
        }
        match session.get_or_prepare_uri_source(uri, native_session, &admission.cancellation()) {
            Ok(opt) => opt.map(|(s, _)| s),
            Err(error) => {
                session.clear_operation_slots();
                session.evict_prepared_request_inputs(request);
                session.evict_prepared_binding(&binding);
                return native_vortex_materializing_error(format, primitive, &error);
            }
        }
    } else {
        source
    };
    #[cfg(all(feature = "vortex-local-primitives", unix))]
    let prepared = resolved_sort_source.map(|source| {
        shardloom_vortex::local_primitives::prepared_dispatch::execute_sort_controlled(
            &primitive_request,
            policy,
            &source,
            &admission.cancellation(),
        )
    });
    #[cfg(not(all(feature = "vortex-local-primitives", unix)))]
    let prepared: Option<
        Result<shardloom_vortex::VortexLocalPrimitiveExecutionReport, ShardLoomError>,
    > = None;
    let report = if let Some(report) = prepared {
        match report {
            Ok(report) => report,
            Err(error) => {
                if let Some(session) = execution_session {
                    session.clear_operation_slots();
                    session.evict_prepared_request_inputs(request);
                    session.evict_prepared_binding(&binding);
                }
                return native_vortex_materializing_error(format, primitive, &error);
            }
        }
    } else if binding.mode == "single_file" {
        match execute_admitted_native_primitive(&primitive_request, None, policy, admission) {
            Ok(report) => report,
            Err(error) => return native_vortex_materializing_error(format, primitive, &error),
        }
    } else {
        match execute_admitted_native_primitive(
            &primitive_request,
            Some(&source_uris),
            policy,
            admission,
        ) {
            Ok(report) => report,
            Err(error) => return native_vortex_materializing_error(format, primitive, &error),
        }
    };
    let native_io_certificate =
        shardloom_vortex::local_primitive_native_io_certificate(&primitive_request, &report).ok();
    let execution_certificate =
        native_vortex_materializing_execution_certificate(&primitive_request, &report);
    let mut fields = execution_attachment_fields("run", request, plan);
    fields.append(&mut extra_fields);
    fields.extend(binding.evidence_fields());
    append_native_vortex_materializing_primitive_fields(
        &mut fields,
        &report,
        &primitive_arg,
        native_io_certificate.as_ref(),
        execution_certificate.as_ref(),
    );
    emit(
        "run",
        format,
        if report.has_errors() {
            CommandStatus::Unsupported
        } else {
            CommandStatus::Success
        },
        format!("native Vortex {} primitive", primitive.as_str()),
        report.to_human_text(),
        report.diagnostics.clone(),
        fields,
    );
    if report.has_errors() {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

fn native_vortex_materializing_error(
    format: OutputFormat,
    primitive: PublicVortexPrimitive,
    error: &ShardLoomError,
) -> ExitCode {
    let summary = format!("public native Vortex {} failed", primitive.as_str());
    emit_error("run", format, &summary, error)
}

fn native_vortex_bound_request_and_arg(
    request: &PublicWorkflowRouteRequest,
    primitive: PublicVortexPrimitive,
    binding: &NativeVortexInputBinding,
) -> Result<
    (
        shardloom_vortex::VortexQueryPrimitiveRequest,
        String,
        Vec<DatasetUri>,
    ),
    ShardLoomError,
> {
    let input_uri = binding.sources.first().ok_or_else(|| {
        ShardLoomError::InvalidOperation(format!(
            "public native Vortex {} requires --input",
            primitive.as_str()
        ))
    })?;
    let uri = DatasetUri::new(input_uri.clone())?;
    let source_uris = binding
        .sources
        .iter()
        .map(|source| DatasetUri::new(source.clone()))
        .collect::<Result<Vec<_>, _>>()?;
    let (primitive_request, primitive_arg) =
        native_vortex_request_and_arg(request, primitive, uri)?;
    Ok((primitive_request, primitive_arg, source_uris))
}

fn native_vortex_request_and_arg(
    request: &PublicWorkflowRouteRequest,
    primitive: PublicVortexPrimitive,
    uri: DatasetUri,
) -> Result<(shardloom_vortex::VortexQueryPrimitiveRequest, String), ShardLoomError> {
    let primitive_arg = native_vortex_primitive_arg_for_request(request, primitive)?;
    let mut primitive_request =
        vortex_primitive_execution::parse_vortex_primitive_request(uri, &primitive_arg)?;
    spill::attach(request, &mut primitive_request)?;
    attach_native_unary_source_predicate(request, &mut primitive_request)?;
    if let Some(limit) = request.vortex_source_order_limit.as_ref() {
        primitive_request = primitive_request
            .with_source_order_limit(positive_usize_arg("source-order limit", limit)?);
    }
    if let Some(seed) = request.vortex_sample_seed.as_ref() {
        primitive_request =
            primitive_request.with_sample_seed(non_negative_u64_arg("sample seed", seed)?);
    }
    if let Some(fraction) = request.vortex_sample_fraction.as_ref() {
        primitive_request = primitive_request
            .with_sample_fraction(sample_fraction_arg("sample fraction", fraction)?);
    }
    primitive_request =
        primitive_request.with_sample_replacement(request.vortex_sample_replacement);
    if let Some(column) = request.vortex_sample_weight_column.as_ref() {
        primitive_request =
            primitive_request.with_sample_weight_column(shardloom_core::ColumnRef::new(column)?);
    }
    if let Some(columns) = request.vortex_deduplicate_key_columns.as_ref() {
        primitive_request = primitive_request.with_deduplicate_key_projection(
            vortex_primitive_execution::parse_projection_columns(columns)?,
        );
    }
    primitive_request = primitive_request.with_duplicate_keep(duplicate_keep_policy_arg(
        request.vortex_duplicate_keep.as_deref(),
    )?);
    Ok((primitive_request, primitive_arg))
}

fn native_vortex_materializing_policy(
    request: &PublicWorkflowRouteRequest,
) -> Result<shardloom_vortex::VortexLocalPrimitiveExecutionPolicy, ShardLoomError> {
    shardloom_vortex::VortexLocalPrimitiveExecutionPolicy::from_resources(
        request.resources.resolve()?,
    )
}

fn public_workflow_effective_memory_gb_label(request: &PublicWorkflowRouteRequest) -> String {
    request.resources.optional().map_or_else(
        |_| "invalid".into(),
        |resources| {
            resources
                .and_then(ExecutionResources::whole_gib)
                .map_or_else(|| "unavailable".into(), |value| value.to_string())
        },
    )
}

fn public_workflow_requested_max_parallelism(request: &PublicWorkflowRouteRequest) -> String {
    public_workflow_effective_max_parallelism_label(request)
}

fn public_workflow_effective_resource_envelope(
    request: &PublicWorkflowRouteRequest,
) -> Result<ExecutionResources, ShardLoomError> {
    request.resources.resolve()
}

fn public_workflow_dynamic_parallelism_floor_applied(
    _request: &PublicWorkflowRouteRequest,
) -> bool {
    false
}

fn native_vortex_materializing_execution_certificate(
    primitive_request: &shardloom_vortex::VortexQueryPrimitiveRequest,
    report: &shardloom_vortex::VortexLocalPrimitiveExecutionReport,
) -> Option<shardloom_core::ExecutionCertificate> {
    vortex_primitive_execution::local_primitive_correctness_fixture_for_request(
        primitive_request,
        report,
    )
    .and_then(|fixture| {
        shardloom_vortex::local_primitive_execution_certificate(&fixture, primitive_request, report)
            .ok()
    })
}

fn append_native_vortex_materializing_primitive_fields(
    fields: &mut Vec<(String, String)>,
    report: &shardloom_vortex::VortexLocalPrimitiveExecutionReport,
    primitive_arg: &str,
    native_io_certificate: Option<&shardloom_core::NativeIoCertificate>,
    execution_certificate: Option<&shardloom_core::ExecutionCertificate>,
) {
    append_hot_runtime_timing_surface_fields(fields);
    if report.status == shardloom_vortex::VortexLocalPrimitiveExecutionStatus::Executed {
        crate::execution_resources::append_admission_fields(
            fields,
            report.resource_envelope.memory_budget_bytes,
            report.resource_envelope.max_parallelism,
            "native_operator_policy_ceiling;runtime_may_use_fewer_lanes",
        );
        crate::execution_resources::append_spill_observation_fields(
            fields,
            report.spill_io_performed,
            report
                .state_budget
                .native_weighted_count_spill
                .as_ref()
                .map(|spill| spill.native_bytes_written),
        );
    }
    append_native_vortex_materializing_identity_fields(fields, report, primitive_arg);
    append_native_vortex_materializing_row_fields(fields, report);
    append_native_vortex_materializing_side_effect_fields(fields, report);
    append_native_vortex_materializing_limit_fields(fields, report);
    append_native_scan_column_fields(fields, report);
    append_local_primitive_result_summary_evidence_fields(fields, report.result_summary.as_deref());
    append_local_primitive_resource_envelope_fields(fields, &report.resource_envelope);
    append_local_primitive_physical_policy_fields(fields, &report.physical_policy);
    append_local_primitive_state_budget_fields(fields, &report.state_budget);
    append_local_primitive_memory_admission_fields(
        fields,
        &report.resource_envelope,
        &report.state_budget,
    );
    append_local_primitive_embedded_layout_fields(fields, &report.embedded_layout);
    vortex_primitive_execution::append_vortex_local_primitive_native_io_certificate_fields(
        fields,
        native_io_certificate,
    );
    vortex_primitive_execution::append_vortex_local_primitive_execution_certificate_fields(
        fields,
        execution_certificate,
    );
}

fn append_native_vortex_materializing_identity_fields(
    fields: &mut Vec<(String, String)>,
    report: &shardloom_vortex::VortexLocalPrimitiveExecutionReport,
    primitive_arg: &str,
) {
    let primitive = native_vortex_materializing_public_primitive_name(report.primitive_kind);
    push_field(fields, "fallback_execution_allowed", "false");
    push_field(fields, "fallback_attempted", "false");
    push_field(fields, "external_engine_invoked", "false");
    push_field(fields, "mode", "native_vortex_primitive");
    push_field(fields, "primitive", primitive);
    push_field(fields, "vortex_primitive_arg", primitive_arg);
    push_field(
        fields,
        "execution",
        if report.has_errors() {
            format!("local_vortex_{primitive}_primitive_not_performed")
        } else {
            format!("local_vortex_{primitive}_primitive_performed")
        },
    );
    push_field(fields, "local_primitive_report_present", "true");
    push_field(
        fields,
        "local_primitive_status",
        report.status.as_str().to_string(),
    );
    push_field(
        fields,
        "local_primitive_mode",
        report.mode.as_str().to_string(),
    );
}

fn native_vortex_materializing_public_primitive_name(
    kind: shardloom_vortex::VortexQueryPrimitiveKind,
) -> &'static str {
    match kind {
        shardloom_vortex::VortexQueryPrimitiveKind::DistinctRows => "distinct",
        shardloom_vortex::VortexQueryPrimitiveKind::DropDuplicateRows => "drop_duplicates",
        shardloom_vortex::VortexQueryPrimitiveKind::DuplicateMaskRows => "duplicate_mask",
        shardloom_vortex::VortexQueryPrimitiveKind::TailRows => "tail",
        shardloom_vortex::VortexQueryPrimitiveKind::SampleRows => "sample",
        shardloom_vortex::VortexQueryPrimitiveKind::ExpressionProjectRows => "expression_project",
        shardloom_vortex::VortexQueryPrimitiveKind::MeltRows => "melt",
        shardloom_vortex::VortexQueryPrimitiveKind::ExplodeRows => "explode",
        shardloom_vortex::VortexQueryPrimitiveKind::PivotRows => "pivot",
        shardloom_vortex::VortexQueryPrimitiveKind::RollingWindowRows => "rolling_window",
        shardloom_vortex::VortexQueryPrimitiveKind::SimpleAggregate => "aggregate",
        shardloom_vortex::VortexQueryPrimitiveKind::SortRows => "sort_rows",
        _ => kind.as_str(),
    }
}

fn append_native_vortex_materializing_row_fields(
    fields: &mut Vec<(String, String)>,
    report: &shardloom_vortex::VortexLocalPrimitiveExecutionReport,
) {
    push_field(
        fields,
        "local_primitive_rows_scanned",
        report.rows_scanned.to_string(),
    );
    push_field(
        fields,
        "local_primitive_rows_selected",
        report
            .rows_selected
            .map_or_else(|| "unknown".to_string(), |rows| rows.to_string()),
    );
    push_field(
        fields,
        "local_primitive_rows_projected",
        report
            .rows_projected
            .map_or_else(|| "unknown".to_string(), |rows| rows.to_string()),
    );
    push_field(
        fields,
        "rows_selected",
        report
            .rows_selected
            .map_or_else(|| "unknown".to_string(), |rows| rows.to_string()),
    );
    push_field(
        fields,
        "rows_projected",
        report
            .rows_projected
            .map_or_else(|| "unknown".to_string(), |rows| rows.to_string()),
    );
    push_field(
        fields,
        "output_row_count",
        report
            .rows_projected
            .or(report.rows_selected)
            .map_or_else(|| "0".to_string(), |rows| rows.to_string()),
    );
    push_bool_field(
        fields,
        "result_known",
        report.rows_projected.or(report.rows_selected).is_some(),
    );
    push_field(
        fields,
        "selected_input_row_count",
        report
            .rows_selected
            .map_or_else(|| "unknown".to_string(), |rows| rows.to_string()),
    );
    push_field(
        fields,
        "local_primitive_projected_columns",
        report.projected_columns.join(","),
    );
    push_field(
        fields,
        "local_primitive_arrays_read_count",
        report.arrays_read_count.to_string(),
    );
    push_field(
        fields,
        "local_primitive_max_chunk_rows",
        report.max_chunk_rows.to_string(),
    );
    push_field(
        fields,
        "local_primitive_max_parallelism_requested",
        report.max_parallelism_requested.to_string(),
    );
    push_field(
        fields,
        "local_primitive_scan_concurrency_per_worker",
        report.scan_concurrency_per_worker.to_string(),
    );
}

fn append_native_vortex_materializing_side_effect_fields(
    fields: &mut Vec<(String, String)>,
    report: &shardloom_vortex::VortexLocalPrimitiveExecutionReport,
) {
    push_bool_field(fields, "data_read", report.data_read);
    push_bool_field(fields, "data_decoded", report.data_decoded);
    push_bool_field(fields, "data_materialized", report.data_materialized);
    push_bool_field(fields, "row_read", report.row_read);
    push_bool_field(fields, "arrow_converted", report.arrow_converted);
    push_bool_field(fields, "object_store_io", report.object_store_io);
    push_bool_field(fields, "write_io", report.write_io);
    push_bool_field(fields, "spill_io_performed", report.spill_io_performed);
    push_bool_field(
        fields,
        "upstream_vortex_scan_called",
        report.upstream_scan_called,
    );
    push_bool_field(
        fields,
        "local_primitive_filter_pushdown_applied",
        report.filter_pushdown_applied,
    );
    push_bool_field(
        fields,
        "local_primitive_projection_pushdown_applied",
        report.projection_pushdown_applied,
    );
    push_bool_field(
        fields,
        "local_primitive_materialization_boundary_reported",
        report.materialization_boundary_reported,
    );
}

fn append_native_vortex_materializing_limit_fields(
    fields: &mut Vec<(String, String)>,
    report: &shardloom_vortex::VortexLocalPrimitiveExecutionReport,
) {
    push_field(
        fields,
        "local_primitive_source_order_limit_requested",
        report
            .source_order_limit_requested
            .map_or_else(|| "none".to_string(), |limit| limit.to_string()),
    );
    push_bool_field(
        fields,
        "local_primitive_source_order_limit_applied",
        report.source_order_limit_applied,
    );
    push_field(
        fields,
        "local_primitive_source_order_limit_rows_output",
        report
            .source_order_limit_rows_output
            .map_or_else(|| "none".to_string(), |rows| rows.to_string()),
    );
}

pub(crate) fn append_native_scan_column_fields(
    fields: &mut Vec<(String, String)>,
    report: &shardloom_vortex::VortexLocalPrimitiveExecutionReport,
) {
    let Some(columns) = &report.scan_columns else {
        return;
    };
    fields.extend([
        ("local_primitive_scan_projection_columns".into(), serde_json::json!(columns.projection_columns).to_string()),
        ("local_primitive_scan_predicate_columns".into(), serde_json::json!(columns.predicate_columns).to_string()),
        ("local_primitive_scan_metadata_completed".into(), columns.metadata_completed.to_string()),
        ("local_primitive_scan_column_scope".into(),
         "executed_provider_rewritten_scan_contract;not_per_column_payload_io_or_decode_measurement".into()),
    ]);
}

#[allow(clippy::too_many_lines)]
pub(crate) fn append_local_primitive_result_summary_evidence_fields(
    fields: &mut Vec<(String, String)>,
    result_summary: Option<&str>,
) {
    let Some(result_summary) = result_summary else {
        return;
    };
    let Some(payload) = local_primitive_result_summary_payload(result_summary) else {
        return;
    };
    let Some(object) = payload.as_object() else {
        return;
    };
    append_native_numeric_accessor_evidence_fields(fields, object);
    append_encoded_numeric_reduction_evidence_fields(fields, object);
    append_scan_segment_reuse_evidence_fields(fields, object);
    if let Some(regional) = object
        .get("regional_aggregate")
        .and_then(serde_json::Value::as_object)
    {
        for (key, value) in regional {
            push_field(
                fields,
                format!("local_primitive_regional_aggregate_{key}"),
                json_value_to_field_string(value),
            );
        }
    }
    for key in [
        "aggregate_result_boundary",
        "aggregate_result_serialized_json_round_trip",
        "aggregate_first_pass_scan_next_nanos",
        "aggregate_first_pass_reader_evidence_nanos",
        "aggregate_first_pass_accessor_nanos",
        "aggregate_first_pass_group_update_nanos",
        "aggregate_first_pass_accessor_chunks",
        "aggregate_first_pass_accessor_rows",
        "aggregate_result_finalization_nanos",
        "aggregate_timing_scope",
        "aggregate_provider_background_workers",
        "aggregate_provider_cpu_scope",
        "aggregate_worker_selection",
        "aggregate_worker_selection_source_rows",
        "aggregate_worker_selection_max_rows",
        "aggregate_workers_rows",
        "aggregate_workers_partial_entries",
        "aggregate_workers_submitted_chunks",
        "aggregate_workers_completed_chunks",
        "aggregate_workers_outstanding_chunks",
        "aggregate_workers_peak_outstanding_chunks",
        "aggregate_workers_cpu_ceiling",
        "aggregate_workers_compute_threads",
        "aggregate_workers_provider_background_workers",
        "aggregate_workers_peak_active_workers",
        "aggregate_workers_worker_busy_elapsed_nanos",
        "aggregate_workers_inline_busy_elapsed_nanos",
        "aggregate_workers_canonicalization_work_nanos",
        "aggregate_workers_count_work_nanos",
        "aggregate_workers_caller_merge_nanos",
        "aggregate_workers_caller_join_wait_nanos",
        "aggregate_workers_caller_submit_elapsed_nanos",
        "aggregate_workers_utf8_bytes_hashed",
        "aggregate_workers_equality_comparisons",
        "aggregate_workers_native_dictionary_chunks",
        "aggregate_workers_native_dictionary_values",
        "aggregate_workers_new_global_strings",
        "aggregate_workers_peak_estimated_global_string_bytes",
        "aggregate_workers_native_constant_chunks",
        "aggregate_workers_peak_partial_capacity_bytes",
        "aggregate_workers_shared_live_peak_bytes",
        "aggregate_workers_shared_live_limit_bytes",
        "aggregate_workers_string_pressure_transitions",
        "aggregate_workers_released_histogram_entries",
        "aggregate_workers_retained_interner_values_on_pressure",
        "aggregate_workers_partition_count",
        "aggregate_workers_partition_complete_groups",
        "aggregate_workers_partition_committed_rows",
        "aggregate_workers_partition_lock_wait_nanos",
        "aggregate_workers_partition_reconcile_work_nanos",
        "aggregate_workers_partition_arrange_work_nanos",
        "aggregate_workers_partition_selection_work_nanos",
        "aggregate_workers_partition_equality_comparisons",
        "aggregate_workers_partition_native_handoffs",
        "aggregate_workers_partition_selection_jobs",
        "aggregate_workers_partition_retry_jobs",
        "aggregate_workers_partition_source_replays",
        "aggregate_workers_partition_discarded_input_rows",
        "aggregate_workers_partition_discarded_attempt_nanos",
        "aggregate_workers_partition_source_replay_nanos",
        "aggregate_workers_partition_entry_credit_claim_calls",
        "aggregate_workers_partition_entry_credit_return_calls",
        "aggregate_workers_compound_partition_strings",
        "aggregate_workers_compound_partition_utf8_bytes_copied",
        "aggregate_workers_compound_key_scope",
        "aggregate_workers_distinct_group_reduction_jobs",
        "aggregate_workers_exact_distinct_complete_pairs",
        "aggregate_workers_exact_distinct_committed_rows",
        "aggregate_workers_exact_distinct_partition_comparisons",
        "aggregate_workers_exact_distinct_partition_lock_wait_nanos",
        "aggregate_workers_exact_distinct_partition_reconcile_nanos",
        "aggregate_workers_exact_distinct_group_reduce_nanos",
        "aggregate_workers_exact_distinct_entry_credit_claims",
        "aggregate_workers_exact_distinct_entry_credit_returns",
        "aggregate_workers_exact_distinct_scope",
        "exact_distinct_final_reserved_bytes",
        "exact_distinct_final_reservation_scope",
        "aggregate_workers_scope",
    ] {
        if let Some(value) = object.get(key) {
            fields.push((
                format!("local_primitive_{key}"),
                value
                    .as_str()
                    .map_or_else(|| value.to_string(), str::to_string),
            ));
        }
    }
    for (summary_key, field_key) in [
        (
            "group_output_strategy",
            "local_primitive_group_output_strategy",
        ),
        (
            "group_admission_strategy",
            "local_primitive_group_admission_strategy",
        ),
        ("candidate_groups", "local_primitive_candidate_groups"),
        (
            "peak_buffered_order_candidates",
            "local_primitive_peak_buffered_order_candidates",
        ),
        (
            "retained_candidate_groups",
            "local_primitive_retained_candidate_groups",
        ),
        (
            "topk_retention_after_update",
            "local_primitive_group_topk_retention_after_update",
        ),
        (
            "materialized_group_value_count",
            "local_primitive_materialized_group_value_count",
        ),
        (
            "decoded_string_count",
            "local_primitive_decoded_string_count",
        ),
        (
            "aggregate_key_encoding_mode",
            "local_primitive_aggregate_key_encoding_mode",
        ),
        (
            "aggregate_update_strategy",
            "local_primitive_aggregate_update_strategy",
        ),
        (
            "numeric_minute_string_direct_slice_updates",
            "local_primitive_numeric_minute_string_direct_slice_updates",
        ),
        (
            "numeric_minute_string_direct_slice_update_rows",
            "local_primitive_numeric_minute_string_direct_slice_update_rows",
        ),
        (
            "expression_fusion_strategy",
            "local_primitive_expression_fusion_strategy",
        ),
        (
            "expression_plan_fingerprint_status",
            "local_primitive_expression_plan_fingerprint_status",
        ),
        (
            "transformed_dictionary_lazy_utf8_minmax_updates",
            "local_primitive_transformed_dictionary_lazy_utf8_minmax_updates",
        ),
        (
            "embedded_derived_column_rewrite_status",
            "local_primitive_embedded_derived_column_rewrite_status",
        ),
        (
            "embedded_derived_column_rewrites",
            "local_primitive_embedded_derived_column_rewrites",
        ),
        (
            "aggregate_accessor_summary",
            "local_primitive_aggregate_accessor_summary",
        ),
        (
            "aggregate_accessor_materialization_status",
            "local_primitive_aggregate_accessor_materialization_status",
        ),
        (
            "aggregate_vortex_dictionary_accessor_columns",
            "local_primitive_aggregate_vortex_dictionary_accessor_columns",
        ),
        (
            "aggregate_chunk_dictionary_accessor_columns",
            "local_primitive_aggregate_chunk_dictionary_accessor_columns",
        ),
        (
            "aggregate_primitive_accessor_columns",
            "local_primitive_aggregate_primitive_accessor_columns",
        ),
        (
            "aggregate_materialized_accessor_columns",
            "local_primitive_aggregate_materialized_accessor_columns",
        ),
        (
            "aggregate_accessor_blockers",
            "local_primitive_aggregate_accessor_blockers",
        ),
        (
            "aggregate_accessor_layout_correlation_status",
            "local_primitive_aggregate_accessor_layout_correlation_status",
        ),
        (
            "aggregate_accessor_layout_correlation_columns",
            "local_primitive_aggregate_accessor_layout_correlation_columns",
        ),
        (
            "aggregate_accessor_layout_correlation_blockers",
            "local_primitive_aggregate_accessor_layout_correlation_blockers",
        ),
        (
            "aggregate_accessor_layout_correlation_dictionary_status",
            "local_primitive_aggregate_accessor_layout_correlation_dictionary_status",
        ),
        (
            "aggregate_accessor_layout_correlation_encoding_inventory",
            "local_primitive_aggregate_accessor_layout_correlation_encoding_inventory",
        ),
        (
            "compact_group_state_strategy",
            "local_primitive_compact_group_state_strategy",
        ),
        ("group_state_mode", "local_primitive_group_state_mode"),
        ("group_key_storage", "local_primitive_group_key_storage"),
        (
            "group_key_comparison_strategy",
            "local_primitive_group_key_comparison_strategy",
        ),
        (
            "source_order_key_retention",
            "local_primitive_source_order_key_retention",
        ),
        (
            "estimated_group_key_storage_bytes",
            "local_primitive_estimated_group_key_storage_bytes",
        ),
        (
            "estimated_group_string_storage_bytes",
            "local_primitive_estimated_group_string_storage_bytes",
        ),
        (
            "distinct_state_strategy",
            "local_primitive_distinct_state_strategy",
        ),
        (
            "uniqueness_proof_status",
            "local_primitive_uniqueness_proof_status",
        ),
        ("spill_state", "local_primitive_spill_state"),
        (
            "sort_provider_background_workers",
            "local_primitive_sort_provider_background_workers",
        ),
        (
            "sort_provider_cpu_scope",
            "local_primitive_sort_provider_cpu_scope",
        ),
        (
            "sort_scan_split_policy",
            "local_primitive_sort_scan_split_policy",
        ),
        ("candidate_rows_seen", "local_primitive_candidate_rows_seen"),
        (
            "retained_candidate_rows",
            "local_primitive_retained_candidate_rows",
        ),
        (
            "retention_selection_strategy",
            "local_primitive_retention_selection_strategy",
        ),
        (
            "retention_flush_threshold",
            "local_primitive_retention_flush_threshold",
        ),
        (
            "late_materialization_chunks_scanned",
            "local_primitive_late_materialization_chunks_scanned",
        ),
        (
            "late_materialization_early_stop_applied",
            "local_primitive_late_materialization_early_stop_applied",
        ),
        (
            "late_materialization_row_index_selection_applied",
            "local_primitive_late_materialization_row_index_selection_applied",
        ),
        (
            "late_materialization_requested_row_indices",
            "local_primitive_late_materialization_requested_row_indices",
        ),
        (
            "late_materialization_min_selected_source_ordinal",
            "local_primitive_late_materialization_min_selected_source_ordinal",
        ),
        (
            "late_materialization_max_selected_source_ordinal",
            "local_primitive_late_materialization_max_selected_source_ordinal",
        ),
        (
            "late_materialization_selected_row_refs_used",
            "local_primitive_late_materialization_selected_row_refs_used",
        ),
        (
            "late_materialization_source_row_id_projection_applied",
            "local_primitive_late_materialization_source_row_id_projection_applied",
        ),
        (
            "sort_predicate_strategy",
            "local_primitive_sort_predicate_strategy",
        ),
        ("predicate_strategy", "local_primitive_predicate_strategy"),
        (
            "string_count_topk_heavy_hitter_second_pass",
            "local_primitive_string_count_topk_heavy_hitter_second_pass",
        ),
        (
            "string_count_topk_late_measure_second_pass",
            "local_primitive_string_count_topk_late_measure_second_pass",
        ),
        (
            "string_count_topk_first_pass_late_measures",
            "local_primitive_string_count_topk_first_pass_late_measures",
        ),
        (
            "string_count_topk_first_pass_late_measure_rows",
            "local_primitive_string_count_topk_first_pass_late_measure_rows",
        ),
        (
            "string_count_topk_first_pass_late_measure_row_cap",
            "local_primitive_string_count_topk_first_pass_late_measure_row_cap",
        ),
        (
            "string_count_topk_first_pass_late_measure_group_cap",
            "local_primitive_string_count_topk_first_pass_late_measure_group_cap",
        ),
        (
            "string_count_topk_first_pass_late_measure_disabled",
            "local_primitive_string_count_topk_first_pass_late_measure_disabled",
        ),
        (
            "string_count_topk_late_measure_candidate_groups",
            "local_primitive_string_count_topk_late_measure_candidate_groups",
        ),
        (
            "string_count_topk_heavy_hitter_candidate_groups",
            "local_primitive_string_count_topk_heavy_hitter_candidate_groups",
        ),
        (
            "string_count_topk_heavy_hitter_capacity",
            "local_primitive_string_count_topk_heavy_hitter_capacity",
        ),
        (
            "string_count_topk_heavy_hitter_threshold",
            "local_primitive_string_count_topk_heavy_hitter_threshold",
        ),
        (
            "string_count_topk_heavy_hitter_exact_proof",
            "local_primitive_string_count_topk_heavy_hitter_exact_proof",
        ),
        (
            "string_count_topk_heavy_hitter_exact_counts_source",
            "local_primitive_string_count_topk_heavy_hitter_exact_counts_source",
        ),
        (
            "string_count_topk_first_pass_exact_histogram_enabled",
            "local_primitive_string_count_topk_first_pass_exact_histogram_enabled",
        ),
        (
            "string_count_topk_first_pass_exact_histogram_promoted",
            "local_primitive_string_count_topk_first_pass_exact_histogram_promoted",
        ),
        (
            "string_count_topk_first_pass_exact_histogram_disabled",
            "local_primitive_string_count_topk_first_pass_exact_histogram_disabled",
        ),
        (
            "string_count_topk_first_pass_exact_histogram_entries",
            "local_primitive_string_count_topk_first_pass_exact_histogram_entries",
        ),
        (
            "string_count_topk_first_pass_exact_histogram_entry_budget",
            "local_primitive_string_count_topk_first_pass_exact_histogram_entry_budget",
        ),
        (
            "string_count_topk_first_pass_exact_histogram_input_rows",
            "local_primitive_string_count_topk_first_pass_exact_histogram_input_rows",
        ),
        (
            "string_count_topk_first_pass_exact_histogram_input_values",
            "local_primitive_string_count_topk_first_pass_exact_histogram_input_values",
        ),
        (
            "string_count_topk_candidate_signature_prefilter",
            "local_primitive_string_count_topk_candidate_signature_prefilter",
        ),
        (
            "string_count_topk_candidate_code_prefilter",
            "local_primitive_string_count_topk_candidate_code_prefilter",
        ),
        (
            "string_count_topk_candidate_free_chunks_skipped",
            "local_primitive_string_count_topk_candidate_free_chunks_skipped",
        ),
        (
            "string_count_topk_candidate_free_rows_skipped",
            "local_primitive_string_count_topk_candidate_free_rows_skipped",
        ),
        (
            "numeric_utf8_topk_heavy_hitter_second_pass",
            "local_primitive_numeric_utf8_topk_heavy_hitter_second_pass",
        ),
        (
            "numeric_utf8_topk_heavy_hitter_candidate_groups",
            "local_primitive_numeric_utf8_topk_heavy_hitter_candidate_groups",
        ),
        (
            "numeric_utf8_topk_heavy_hitter_capacity",
            "local_primitive_numeric_utf8_topk_heavy_hitter_capacity",
        ),
        (
            "numeric_utf8_topk_heavy_hitter_threshold",
            "local_primitive_numeric_utf8_topk_heavy_hitter_threshold",
        ),
        (
            "numeric_utf8_topk_heavy_hitter_exact_proof",
            "local_primitive_numeric_utf8_topk_heavy_hitter_exact_proof",
        ),
        (
            "numeric_utf8_topk_candidate_code_prefilter",
            "local_primitive_numeric_utf8_topk_candidate_code_prefilter",
        ),
        (
            "numeric_utf8_topk_candidate_free_chunks_skipped",
            "local_primitive_numeric_utf8_topk_candidate_free_chunks_skipped",
        ),
        (
            "numeric_utf8_topk_candidate_free_rows_skipped",
            "local_primitive_numeric_utf8_topk_candidate_free_rows_skipped",
        ),
        (
            "numeric_pair_late_measure_near_unique_directory_updates",
            "local_primitive_numeric_pair_late_measure_near_unique_directory_updates",
        ),
        (
            "numeric_pair_late_measure_near_unique_rows",
            "local_primitive_numeric_pair_late_measure_near_unique_rows",
        ),
        (
            "numeric_pair_late_measure_near_unique_seen_keys",
            "local_primitive_numeric_pair_late_measure_near_unique_seen_keys",
        ),
        (
            "numeric_pair_late_measure_near_unique_duplicate_keys",
            "local_primitive_numeric_pair_late_measure_near_unique_duplicate_keys",
        ),
        (
            "numeric_pair_late_measure_near_unique_directory_released_before_second_pass",
            "local_primitive_numeric_pair_late_measure_near_unique_directory_released_before_second_pass",
        ),
        (
            "grouped_count_distinct_pair_preunion_updates",
            "local_primitive_grouped_count_distinct_pair_preunion_updates",
        ),
        (
            "grouped_count_distinct_pair_preunion_input_rows",
            "local_primitive_grouped_count_distinct_pair_preunion_input_rows",
        ),
        (
            "grouped_count_distinct_pair_preunion_unique_pairs",
            "local_primitive_grouped_count_distinct_pair_preunion_unique_pairs",
        ),
        (
            "grouped_count_distinct_pair_preunion_chunk_group_partials",
            "local_primitive_grouped_count_distinct_pair_preunion_chunk_group_partials",
        ),
        (
            "grouped_count_distinct_pair_preunion_chunk_groups",
            "local_primitive_grouped_count_distinct_pair_preunion_chunk_groups",
        ),
        (
            "grouped_count_distinct_pair_preunion_duplicate_rows_elided",
            "local_primitive_grouped_count_distinct_pair_preunion_duplicate_rows_elided",
        ),
        (
            "string_count_distinct_topk_heavy_hitter_second_pass",
            "local_primitive_string_count_distinct_topk_heavy_hitter_second_pass",
        ),
        (
            "string_count_distinct_topk_heavy_hitter_candidate_groups",
            "local_primitive_string_count_distinct_topk_heavy_hitter_candidate_groups",
        ),
        (
            "string_count_distinct_topk_heavy_hitter_capacity",
            "local_primitive_string_count_distinct_topk_heavy_hitter_capacity",
        ),
        (
            "string_count_distinct_topk_heavy_hitter_threshold",
            "local_primitive_string_count_distinct_topk_heavy_hitter_threshold",
        ),
        (
            "string_count_distinct_topk_heavy_hitter_exact_proof",
            "local_primitive_string_count_distinct_topk_heavy_hitter_exact_proof",
        ),
        (
            "string_count_distinct_topk_exact_sets_source",
            "local_primitive_string_count_distinct_topk_exact_sets_source",
        ),
        (
            "string_count_distinct_topk_first_pass_exact_sets",
            "local_primitive_string_count_distinct_topk_first_pass_exact_sets",
        ),
        (
            "string_count_distinct_topk_first_pass_exact_sets_disabled",
            "local_primitive_string_count_distinct_topk_first_pass_exact_sets_disabled",
        ),
        (
            "string_count_distinct_topk_first_pass_exact_set_entries",
            "local_primitive_string_count_distinct_topk_first_pass_exact_set_entries",
        ),
        (
            "string_count_distinct_topk_first_pass_exact_set_entry_budget",
            "local_primitive_string_count_distinct_topk_first_pass_exact_set_entry_budget",
        ),
        (
            "string_count_distinct_topk_candidate_signature_prefilter",
            "local_primitive_string_count_distinct_topk_candidate_signature_prefilter",
        ),
        (
            "string_count_distinct_topk_candidate_code_prefilter",
            "local_primitive_string_count_distinct_topk_candidate_code_prefilter",
        ),
        (
            "string_count_distinct_topk_candidate_free_chunks_skipped",
            "local_primitive_string_count_distinct_topk_candidate_free_chunks_skipped",
        ),
        (
            "string_count_distinct_topk_candidate_free_rows_skipped",
            "local_primitive_string_count_distinct_topk_candidate_free_rows_skipped",
        ),
    ] {
        if let Some(value) = object.get(summary_key) {
            push_field(fields, field_key, json_value_to_field_string(value));
        }
    }
}

fn append_native_numeric_accessor_evidence_fields(
    fields: &mut Vec<(String, String)>,
    summary: &serde_json::Map<String, serde_json::Value>,
) {
    let Some(work) = summary
        .get("aggregate_native_numeric_accessor")
        .and_then(serde_json::Value::as_object)
    else {
        return;
    };
    for key in [
        "native_decode_calls",
        "rows",
        "source_array_nbytes_estimate",
        "canonical_array_nbytes_estimate",
        "typed_value_bytes_copied",
        "decode_and_typed_copy_nanos",
        "max_source_array_rows",
        "columns",
        "scope",
    ] {
        if let Some(value) = work.get(key) {
            push_field(
                fields,
                format!("local_primitive_aggregate_native_numeric_accessor_{key}"),
                json_value_to_field_string(value),
            );
        }
    }
}

fn append_encoded_numeric_reduction_evidence_fields(
    fields: &mut Vec<(String, String)>,
    summary: &serde_json::Map<String, serde_json::Value>,
) {
    let Some(evidence) = summary
        .get("aggregate_encoded_numeric_reduction")
        .and_then(serde_json::Value::as_object)
    else {
        return;
    };
    if let Some(work) = evidence.get("work").and_then(serde_json::Value::as_object) {
        for key in [
            "calls",
            "constant_arrays",
            "run_end_arrays",
            "logical_rows",
            "child_primitive_executions",
            "child_rows",
            "max_child_rows",
            "weighted_value_visits",
            "elapsed_nanos",
            "structural_resolutions",
        ] {
            if let Some(value) = work.get(key) {
                push_field(
                    fields,
                    format!("local_primitive_aggregate_encoded_numeric_reduction_{key}"),
                    json_value_to_field_string(value),
                );
            }
        }
    }
    if let Some(scope) = evidence.get("scope") {
        push_field(
            fields,
            "local_primitive_aggregate_encoded_numeric_reduction_scope",
            json_value_to_field_string(scope),
        );
    }
}

fn append_scan_segment_reuse_evidence_fields(
    fields: &mut Vec<(String, String)>,
    summary: &serde_json::Map<String, serde_json::Value>,
) {
    let Some(work) = summary
        .get("scan_segment_reuse")
        .and_then(serde_json::Value::as_object)
    else {
        return;
    };
    for key in [
        "scope",
        "hits",
        "shared_requests",
        "downstream_requests",
        "completed_segments",
        "completed_segment_bytes",
        "completed_segment_bytes_scope",
        "copied_bytes",
        "copy_nanos",
        "copy_timing_scope",
        "evictions",
        "pressure_bypasses",
        "entry_bypasses",
        "oversized_or_empty_bypasses",
        "cancelled_requests",
        "failed_requests",
        "invalidations",
        "peak_entries",
        "closed",
        "retained_entries",
        "in_flight",
        "table_reserved_bytes",
        "retention_limit_bytes",
        "retention_live_owned_bytes",
        "retention_peak_owned_bytes",
        "retention_denied_reservations",
        "session_limit_bytes",
        "session_live_owned_bytes",
        "session_peak_owned_bytes",
        "session_denied_reservations",
        "memory_scope",
        "uncached_replays",
        "discarded_attempt_nanos",
        "uncached_replay_nanos",
        "provider_background_workers",
        "admission_skipped",
        "replay_scope",
    ] {
        if let Some(value) = work.get(key) {
            push_field(
                fields,
                format!("local_primitive_scan_segment_reuse_{key}"),
                json_value_to_field_string(value),
            );
        }
    }
}

fn local_primitive_result_summary_payload(result_summary: &str) -> Option<serde_json::Value> {
    let result_summary = result_summary
        .strip_prefix("partitioned_vortex_sources=")
        .and_then(|summary| {
            let (count, summary) = summary.split_once(';')?;
            count.parse::<usize>().ok()?;
            summary.strip_prefix("partitioned_execution_strategy=sequential_capillary_parts;")
        })
        .unwrap_or(result_summary);
    if let Ok(payload) = serde_json::from_str::<serde_json::Value>(result_summary) {
        return Some(payload);
    }
    let values_index = result_summary.rfind(" values=")?;
    serde_json::from_str::<serde_json::Value>(&result_summary[values_index + " values=".len()..])
        .ok()
}

fn json_value_to_field_string(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::Null => "null".to_string(),
        serde_json::Value::Bool(value) => value.to_string(),
        serde_json::Value::Number(value) => value.to_string(),
        serde_json::Value::String(value) => value.clone(),
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => value.to_string(),
    }
}

#[derive(Debug, Clone)]
struct PreparedLocalWorkflowRun {
    request: PublicWorkflowRouteRequest,
    left_source_uri: String,
    left_source_format: String,
    left_target: PathBuf,
    right_source: Option<PreparedLocalWorkflowRightSource>,
}

#[derive(Debug, Clone)]
struct PreparedLocalWorkflowRightSource {
    source_uri: String,
    source_format: String,
    target: PathBuf,
}

#[derive(Clone, Default)]
struct PublicSourcePreparations {
    #[cfg(all(feature = "vortex-write", feature = "universal-format-io", unix))]
    sources:
        Vec<std::sync::Arc<shardloom_vortex::prepared_source_binding::LocalPreparationIdentity>>,
}

impl PublicSourcePreparations {
    #[cfg(all(feature = "vortex-local-primitives", unix))]
    #[cfg_attr(
        not(all(feature = "vortex-write", feature = "universal-format-io")),
        allow(clippy::unused_self, unused_variables)
    )]
    fn same_generations(&self, other: &Self) -> bool {
        #[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
        return self.sources.len() == other.sources.len()
            && self
                .sources
                .iter()
                .zip(&other.sources)
                .all(|(left, right)| {
                    left.source_digest == right.source_digest
                        && left.prepared_digest == right.prepared_digest
                });
        #[cfg(not(all(feature = "vortex-write", feature = "universal-format-io")))]
        true
    }
}

#[derive(Clone, Copy)]
struct PublicWorkflowPreparationInput<'a> {
    source_uri: &'a str,
    source_format: &'a str,
    target: &'a Path,
    source_schema: Option<&'a str>,
    error_title: &'static str,
}

#[allow(clippy::too_many_lines)]
fn execute_local_file_prepare_once_first_query_run(
    request: &PublicWorkflowRouteRequest,
    plan: &PublicWorkflowRoutePlan,
    format: OutputFormat,
    execution_session: &mut PublicExecutionSession,
    admission: &CommandAdmission<'_>,
) -> ExitCode {
    if is_write_request(request)
        && !matches!(
            request.requested_output.as_str(),
            "write_vortex"
                | "write_json"
                | "write_jsonl"
                | "write_csv"
                | "write_parquet"
                | "write_arrow_ipc"
                | "write_avro"
                | "write_orc"
        )
    {
        let blocked = local_file_compatibility_sink_contract_missing_route(request);
        return emit_blocked_facade("run", format, request, &blocked);
    }

    let prepared_run = match prepared_local_workflow_native_request(request) {
        Ok(prepared_run) => prepared_run,
        Err(blocked) => return emit_blocked_facade("run", format, request, &blocked),
    };
    let resources = match public_workflow_effective_resource_envelope(request) {
        Ok(values) => values,
        Err(error) => {
            return emit_error(
                "run",
                format,
                "public workflow resource validation failed",
                &error,
            );
        }
    };

    let native_plan = plan_public_workflow_route(&prepared_run.request);
    if native_plan.status != CommandStatus::Success {
        return emit_blocked_facade("run", format, &prepared_run.request, &native_plan);
    }

    let left_input = PublicWorkflowPreparationInput {
        source_uri: &prepared_run.left_source_uri,
        source_format: &prepared_run.left_source_format,
        target: &prepared_run.left_target,
        source_schema: request.source_schema.as_deref(),
        error_title: "public local Vortex preparation failed",
    };
    let left_preparation = match prepare_local_source_for_public_workflow_or_emit(
        left_input, request, format, resources, admission,
    ) {
        Ok(preparation) => preparation,
        Err(exit) => return exit,
    };

    let mut extra_fields = local_prepared_vortex_execution_attachment_fields(
        request,
        plan,
        &left_preparation,
        prepared_run.right_source.is_some(),
    );
    let right_preparation = if let Some(right) = &prepared_run.right_source {
        let right_input = PublicWorkflowPreparationInput {
            source_uri: &right.source_uri,
            source_format: &right.source_format,
            target: &right.target,
            source_schema: None,
            error_title: "public local Vortex right-input preparation failed",
        };
        let right_preparation = match prepare_local_source_for_public_workflow_or_emit(
            right_input,
            request,
            format,
            resources,
            admission,
        ) {
            Ok(preparation) => preparation,
            Err(exit) => return exit,
        };
        extra_fields.extend(local_prepared_vortex_right_execution_attachment_fields(
            &right_preparation,
        ));
        Some(right_preparation)
    } else {
        None
    };

    with_prepared_source_generations(&left_preparation, right_preparation.as_ref(), || {
        let preparations = PublicSourcePreparations {
            #[cfg(all(feature = "vortex-write", feature = "universal-format-io", unix))]
            sources: std::iter::once(&left_preparation)
                .chain(right_preparation.as_ref())
                .filter_map(|preparation| preparation.identity.clone())
                .collect(),
        };
        execute_prepared_local_native_route(
            &prepared_run.request,
            &native_plan,
            format,
            extra_fields,
            execution_session,
            &preparations,
            admission,
        )
    })
    .unwrap_or_else(|error| {
        emit_error(
            "run",
            format,
            "prepared source generation changed during execution",
            &error,
        )
    })
}

pub(crate) fn with_prepared_source_generations<T>(
    left: &sql_local_source_runtime::PublicWorkflowVortexPreparation,
    right: Option<&sql_local_source_runtime::PublicWorkflowVortexPreparation>,
    execute: impl FnOnce() -> T,
) -> Result<T, ShardLoomError> {
    crate::cli_output::with_validated_output(
        || {
            left.validate_generation()?;
            if let Some(right) = right {
                right.validate_generation()?;
            }
            Ok(())
        },
        execute,
    )
}

fn execute_prepared_local_native_route(
    request: &PublicWorkflowRouteRequest,
    native_plan: &PublicWorkflowRoutePlan,
    format: OutputFormat,
    extra_fields: Vec<(String, String)>,
    execution_session: &mut PublicExecutionSession,
    preparations: &PublicSourcePreparations,
    admission: &CommandAdmission<'_>,
) -> ExitCode {
    #[cfg(all(feature = "vortex-local-primitives", unix))]
    let mut extra_fields = extra_fields;
    #[cfg(not(all(feature = "vortex-local-primitives", unix)))]
    let _ = preparations;
    // Resolve the same strategy with the original preparation owners attached.
    // Native source/plan owners carry them through final writer commit checks.
    #[cfg(all(feature = "vortex-local-primitives", feature = "vortex-write", unix))]
    if is_write_request(request)
        && let Some(exit) = resident_sql::write_if_needed(
            request,
            native_plan,
            format,
            &mut extra_fields,
            preparations.clone(),
            admission,
        )
    {
        return exit;
    }
    #[cfg(all(feature = "vortex-local-primitives", unix))]
    if let Some(exit) = resident_sql::run_if_needed(
        request,
        native_plan,
        format,
        &mut extra_fields,
        execution_session,
        preparations.clone(),
        admission,
    ) {
        return exit;
    }
    match native_plan.route_id {
        #[cfg(all(feature = "vortex-local-primitives", unix))]
        "native_vortex_relational_collect" | "native_vortex_relational_write" => {
            resident_relational::run_with_source(
                request,
                native_plan,
                format,
                execution_session,
                extra_fields,
                None,
                preparations.clone(),
                admission,
            )
        }
        "native_vortex_count_all"
        | "native_vortex_count_where"
        | "native_vortex_filter"
        | "native_vortex_project"
        | "native_vortex_filter_project"
        | "native_vortex_distinct"
        | "native_vortex_drop_duplicates"
        | "native_vortex_duplicate_mask"
        | "native_vortex_tail"
        | "native_vortex_sample"
        | "native_vortex_expression_project"
        | "native_vortex_melt"
        | "native_vortex_explode"
        | "native_vortex_pivot"
        | "native_vortex_rolling_window"
        | "native_vortex_aggregate"
        | "native_vortex_sort_rows" => execute_native_vortex_primitive_run_with_extra(
            request,
            native_plan,
            format,
            extra_fields,
            execution_session,
            admission,
        ),
        "native_vortex_user_profile" => execute_native_vortex_profile_run_with_extra(
            request,
            native_plan,
            format,
            extra_fields,
            admission,
        ),
        "native_vortex_primitive_row_export" => {
            execute_native_vortex_primitive_row_export_run_with_extra(
                request,
                native_plan,
                format,
                extra_fields,
                admission,
            )
        }
        _ => {
            let blocked = run_route_not_executable_yet(native_plan);
            emit_blocked_facade("run", format, request, &blocked)
        }
    }
}

enum PreparationFacadeError {
    FeatureGated,
    Runtime(ShardLoomError),
}

fn prepare_local_source_for_public_workflow_or_emit(
    input: PublicWorkflowPreparationInput<'_>,
    request: &PublicWorkflowRouteRequest,
    format: OutputFormat,
    resources: ExecutionResources,
    admission: &CommandAdmission<'_>,
) -> Result<sql_local_source_runtime::PublicWorkflowVortexPreparation, ExitCode> {
    match prepare_local_source_for_public_workflow_controlled(
        input.source_uri,
        input.source_format,
        input.target,
        false,
        resources,
        Some(admission),
        request.source_fingerprint_policy.as_deref(),
        input.source_schema,
        &admission.cancellation(),
    ) {
        Ok(preparation) => Ok(preparation),
        Err(PreparationFacadeError::FeatureGated) => {
            let blocked = local_file_vortex_ingest_feature_gated_route(request);
            Err(emit_blocked_facade("run", format, request, &blocked))
        }
        Err(PreparationFacadeError::Runtime(error)) => {
            Err(emit_error("run", format, input.error_title, &error))
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn prepare_local_source_for_public_workflow_controlled(
    source_uri: &str,
    source_format: &str,
    target: &Path,
    allow_overwrite: bool,
    resources: ExecutionResources,
    shared_memory_pool: Option<&shardloom_exec::live_memory::LiveMemoryPool>,
    source_fingerprint_policy: Option<&str>,
    source_schema: Option<&str>,
    cancellation: &shardloom_exec::compute_pool::CancellationToken,
) -> Result<sql_local_source_runtime::PublicWorkflowVortexPreparation, PreparationFacadeError> {
    prepare_local_source_for_public_workflow_with_float_policy(
        source_uri,
        source_format,
        target,
        allow_overwrite,
        resources,
        shared_memory_pool,
        source_fingerprint_policy,
        source_schema,
        cancellation,
        shardloom_core::FloatPolicy::Strict,
    )
}

#[allow(clippy::too_many_arguments)]
fn prepare_local_source_for_public_workflow_with_float_policy(
    source_uri: &str,
    source_format: &str,
    target: &Path,
    allow_overwrite: bool,
    resources: ExecutionResources,
    shared_memory_pool: Option<&shardloom_exec::live_memory::LiveMemoryPool>,
    source_fingerprint_policy: Option<&str>,
    source_schema: Option<&str>,
    cancellation: &shardloom_exec::compute_pool::CancellationToken,
    float_policy: shardloom_core::FloatPolicy,
) -> Result<sql_local_source_runtime::PublicWorkflowVortexPreparation, PreparationFacadeError> {
    sql_local_source_runtime::prepare_local_source_as_vortex_for_public_workflow_with_float_policy(
        source_uri,
        target,
        Some(source_format),
        allow_overwrite,
        resources,
        shared_memory_pool,
        source_fingerprint_policy,
        source_schema,
        cancellation,
        float_policy,
    )
    .map_err(|error| match error {
        ShardLoomError::NotImplemented(feature)
            if feature == "vortex_ingest feature gate is not enabled" =>
        {
            PreparationFacadeError::FeatureGated
        }
        other => PreparationFacadeError::Runtime(other),
    })
}

fn prepare_profile_runtime_requested(request: &PublicWorkflowRouteRequest) -> bool {
    if request.sql_statement.is_some() {
        return true;
    }
    request
        .plan_summary
        .as_deref()
        .and_then(parse_plan_summary_operations)
        .is_some_and(|operations| operations.len() > 1)
}

fn prepared_profile_native_runtime_request(
    request: &PublicWorkflowRouteRequest,
    prepared_target: &Path,
) -> Result<PublicWorkflowRouteRequest, Box<PublicWorkflowRoutePlan>> {
    let mut native_request = request.clone();
    if let Some(plan_summary) = prepared_local_workflow_plan_summary(request, prepared_target, None)
    {
        native_request.surface = "dataframe".to_string();
        native_request.input_uri = Some(prepared_target.display().to_string());
        native_request.input_format = Some("vortex".to_string());
        native_request.sql_statement =
            prepared_local_workflow_sql_statement(request, prepared_target, None);
        native_request.plan_summary = Some(plan_summary);
    } else if let Some(statement) =
        prepared_local_workflow_sql_statement(request, prepared_target, None)
    {
        native_request.surface = "sql".to_string();
        native_request.input_uri = Some(prepared_target.display().to_string());
        native_request.input_format = Some("vortex".to_string());
        native_request.sql_statement = Some(statement);
        native_request.plan_summary = Some("sql(statement)".to_string());
    } else {
        return Err(Box::new(local_file_vortex_middle_required_route(request)));
    }

    native_request.source_schema = None;
    native_request.requested_output = "collect".to_string();
    native_request.output_ref = None;
    native_request.fanout_outputs.clear();
    native_request.execution_policy = "native_vortex".to_string();
    native_request
        .materialization_policy
        .clone_from(&request.materialization_policy);
    native_request.bounded = true;
    Ok(effective_public_workflow_request(&native_request))
}

#[allow(clippy::too_many_lines)]
fn prepared_profile_runtime_plan_fields(
    request: &PublicWorkflowRouteRequest,
    preparation: &sql_local_source_runtime::PublicWorkflowVortexPreparation,
) -> Vec<(String, String)> {
    let mut fields = vec![
        (
            "public_workflow_preparation_profile_runtime_schema_version".to_string(),
            "shardloom.public_workflow_preparation_profile_runtime.v1".to_string(),
        ),
        (
            "public_workflow_preparation_profile_runtime_fallback_attempted".to_string(),
            "false".to_string(),
        ),
        (
            "public_workflow_preparation_profile_runtime_external_engine_invoked".to_string(),
            "false".to_string(),
        ),
        (
            "public_workflow_preparation_profile_runtime_contract".to_string(),
            "single_vortex_artifact_native_runtime_no_query_answer_sidecar".to_string(),
        ),
    ];
    if !prepare_profile_runtime_requested(request) {
        fields.push((
            "public_workflow_preparation_profile_runtime_plan_status".to_string(),
            "not_declared".to_string(),
        ));
        fields.push((
            "public_workflow_preparation_profile_runtime_next_action".to_string(),
            "pass --sql or a DataFrame plan summary when prepare should record native Vortex profile planning evidence"
                .to_string(),
        ));
        return fields;
    }

    let native_request =
        match prepared_profile_native_runtime_request(request, &preparation.target_path) {
            Ok(native_request) => native_request,
            Err(blocked) => {
                fields.extend([
                    (
                        "public_workflow_preparation_profile_runtime_plan_status".to_string(),
                        "not_admitted".to_string(),
                    ),
                    (
                        "public_workflow_preparation_profile_runtime_blocker_id".to_string(),
                        blocked.blocker_id.to_string(),
                    ),
                    (
                        "public_workflow_preparation_profile_runtime_next_action".to_string(),
                        blocked
                            .diagnostics
                            .first()
                            .and_then(|diagnostic| diagnostic.suggested_next_step.clone())
                            .unwrap_or_else(|| {
                                "use an admitted native Vortex aggregate or bounded sort profile"
                                    .to_string()
                            }),
                    ),
                ]);
                return fields;
            }
        };
    let native_plan = plan_public_workflow_route(&native_request);
    fields.extend([
        (
            "public_workflow_preparation_profile_runtime_route_id".to_string(),
            native_plan.route_id.to_string(),
        ),
        (
            "public_workflow_preparation_profile_runtime_route_status".to_string(),
            native_plan.status.as_str().to_string(),
        ),
    ]);
    if native_plan.status != CommandStatus::Success {
        fields.extend([
            (
                "public_workflow_preparation_profile_runtime_plan_status".to_string(),
                "not_admitted".to_string(),
            ),
            (
                "public_workflow_preparation_profile_runtime_blocker_id".to_string(),
                native_plan.blocker_id.to_string(),
            ),
        ]);
        return fields;
    }

    let Some(primitive) = normalized_vortex_primitive(&native_request) else {
        fields.extend([
            (
                "public_workflow_preparation_profile_runtime_plan_status".to_string(),
                "not_reusable_family".to_string(),
            ),
            (
                "public_workflow_preparation_profile_runtime_next_action".to_string(),
                "profile resolved without a native Vortex primitive payload".to_string(),
            ),
        ]);
        return fields;
    };
    fields.extend([
        (
            "public_workflow_preparation_profile_runtime_plan_status".to_string(),
            "planned_single_vortex_artifact_native_route".to_string(),
        ),
        (
            "public_workflow_preparation_profile_runtime_primitive".to_string(),
            primitive.as_str().to_string(),
        ),
        (
            "public_workflow_preparation_profile_runtime_execution_status".to_string(),
            "not_executed_during_prepare".to_string(),
        ),
        (
            "public_workflow_preparation_profile_runtime_query_answer_sidecar_status".to_string(),
            "disabled_public_runtime_single_vortex_artifact_policy".to_string(),
        ),
        (
            "public_workflow_preparation_profile_runtime_prepared_target".to_string(),
            preparation.target_path.display().to_string(),
        ),
    ]);
    fields
}

fn prepared_local_workflow_native_request(
    request: &PublicWorkflowRouteRequest,
) -> Result<PreparedLocalWorkflowRun, Box<PublicWorkflowRoutePlan>> {
    let Some(input_uri) = request.input_uri.clone() else {
        return Err(Box::new(input_not_declared_route()));
    };
    let Some(source_format) = request.input_format.clone() else {
        return Err(Box::new(input_format_not_admitted_route("not_declared")));
    };
    let left_target = auto_prepared_vortex_target_path_with_schema(
        &input_uri,
        &source_format,
        request.source_schema.as_deref(),
    );
    let right_source = prepared_local_workflow_right_source(request)?;
    let mut native_request = request.clone();
    // Normalization changes both the SQL source leaf and its declaration. The
    // original request still owns the compatibility schema and preparation proof;
    // the native request must bind only the prepared artifact's authoritative type.
    for (source, target) in std::iter::once((input_uri.as_str(), left_target.as_path())).chain(
        right_source
            .as_ref()
            .map(|right| (right.source_uri.as_str(), right.target.as_path())),
    ) {
        if native_request.source_bindings.remove(source).is_some() {
            native_request.source_bindings.insert(
                target.display().to_string(),
                PublicSourceBinding {
                    input_format: "vortex".into(),
                    source_schema: None,
                    memory_input: None,
                    table_snapshot: None,
                },
            );
        }
    }
    if let Some(plan_summary) =
        prepared_local_workflow_plan_summary(request, &left_target, right_source.as_ref())
    {
        native_request.surface = "dataframe".to_string();
        native_request.input_uri = Some(left_target.display().to_string());
        native_request.input_format = Some("vortex".to_string());
        // Python supplies executable SQL alongside its descriptive operation
        // summary. Preserve that lowering when rebinding to the prepared file;
        // summaries do not encode every admitted aggregate or expression.
        native_request.sql_statement =
            prepared_local_workflow_sql_statement(request, &left_target, right_source.as_ref());
        native_request.source_schema = None;
        native_request.plan_summary = Some(plan_summary);
    } else if let Some(statement) =
        prepared_local_workflow_sql_statement(request, &left_target, right_source.as_ref())
    {
        native_request.surface = "sql".to_string();
        native_request.input_uri = Some(left_target.display().to_string());
        native_request.input_format = Some("vortex".to_string());
        native_request.sql_statement = Some(statement);
        native_request.source_schema = None;
        native_request.plan_summary = Some("sql(statement)".to_string());
        if let Some(payload) = infer_native_vortex_sql_payload(&native_request) {
            payload.apply(&mut native_request);
        } else if plan_public_workflow_route(&native_request).status != CommandStatus::Success {
            // Native relational SQL, including LIMIT 0, has no positive-limit
            // primitive payload. Admit it through the same inert route planner.
            let blocked = if is_write_request(request) {
                local_file_compatibility_sink_contract_missing_route(request)
            } else {
                local_file_vortex_middle_required_route(request)
            };
            return Err(Box::new(blocked));
        }
    } else {
        return Err(Box::new(local_file_vortex_middle_required_route(request)));
    }

    native_request.execution_policy = "native_vortex".to_string();
    native_request
        .materialization_policy
        .clone_from(&request.materialization_policy);
    native_request.bounded = true;
    Ok(PreparedLocalWorkflowRun {
        request: effective_public_workflow_request(&native_request),
        left_source_uri: input_uri,
        left_source_format: source_format,
        left_target,
        right_source,
    })
}

fn prepared_local_workflow_right_source(
    request: &PublicWorkflowRouteRequest,
) -> Result<Option<PreparedLocalWorkflowRightSource>, Box<PublicWorkflowRoutePlan>> {
    let right_uri = if let Some(summary) = request.plan_summary.as_deref() {
        parse_plan_summary_operations(summary).and_then(|operations| {
            operations
                .iter()
                .find(|operation| operation.kind == "join")
                .map(|operation| operation.arg)
                .and_then(|join_arg| join_arg.split(',').next())
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_string)
        })
    } else if let Some(statement) = request.sql_statement.as_deref() {
        quoted_source_refs(statement)
            .into_iter()
            .skip(1)
            .find(|ref_| {
                ref_ != request.input_uri.as_deref().unwrap_or("")
                    && infer_input_format_from_ref(ref_).is_some()
            })
    } else {
        None
    };
    let Some(right_uri) = right_uri else {
        return Ok(None);
    };
    if infer_input_format_from_ref(&right_uri) == Some("vortex") {
        return Ok(None);
    }
    let Some(source_format) = infer_input_format_from_ref(&right_uri) else {
        return Err(Box::new(local_file_vortex_middle_required_route(request)));
    };
    let target = auto_prepared_vortex_target_path(&right_uri, source_format);
    Ok(Some(PreparedLocalWorkflowRightSource {
        source_uri: right_uri,
        source_format: source_format.to_string(),
        target,
    }))
}

fn prepared_local_workflow_plan_summary(
    request: &PublicWorkflowRouteRequest,
    left_target: &Path,
    right_source: Option<&PreparedLocalWorkflowRightSource>,
) -> Option<String> {
    let operations = parse_plan_summary_operations(request.plan_summary.as_deref()?)?;
    let input_uri = request.input_uri.as_deref()?;
    let first = operations.first()?;
    if !first.kind.starts_with("read_") || first.arg.trim() != input_uri {
        return None;
    }
    let mut rewritten = Vec::with_capacity(operations.len());
    for (index, operation) in operations.iter().enumerate() {
        if index == 0 {
            rewritten.push(format!("read_vortex({})", left_target.display()));
        } else if operation.kind == "join" {
            let arg = prepared_local_join_arg(operation.arg, right_source);
            rewritten.push(format!("{}({arg})", operation.kind));
        } else {
            rewritten.push(format!("{}({})", operation.kind, operation.arg));
        }
    }
    Some(rewritten.join(" -> "))
}

fn prepared_local_workflow_sql_statement(
    request: &PublicWorkflowRouteRequest,
    left_target: &Path,
    right_source: Option<&PreparedLocalWorkflowRightSource>,
) -> Option<String> {
    let statement = request.sql_statement.as_deref()?;
    let input_uri = request.input_uri.as_deref()?;
    let source_refs = sql_source_ref_ranges(statement);
    let mut rewritten = statement.to_string();
    for source_ref in source_refs.iter().rev() {
        let matches = |uri: &str| {
            source_ref.value == uri
                || (!source_ref.quoted
                    && declared_sql_source_identifier_matches(&source_ref.value, uri))
        };
        let target = if matches(input_uri) {
            Some(left_target)
        } else {
            right_source
                .filter(|right| matches(&right.source_uri))
                .map(|right| right.target.as_path())
        };
        if let Some(target) = target {
            rewritten.replace_range(
                source_ref.start..source_ref.end,
                &sql_string_literal(&target.display().to_string()),
            );
        }
    }
    (rewritten != statement).then_some(rewritten)
}

struct SqlSourceRefRange {
    value: String,
    start: usize,
    end: usize,
    quoted: bool,
}

#[derive(Default)]
struct SqlSourceScope<'a> {
    select: bool,
    source_clause: bool,
    expect_source: bool,
    previous_word: &'a str,
}

/// Locate source tokens without touching values, comments or expression FROMs.
/// Each parenthesized scope must introduce its own SELECT before admitting a
/// source. The complete SQL parser still owns syntax and semantic admission.
fn sql_source_ref_ranges(statement: &str) -> Vec<SqlSourceRefRange> {
    let mut ranges = Vec::new();
    let mut scopes = vec![SqlSourceScope::default()];
    let mut index = 0;
    while index < statement.len() {
        let remaining = trim_sql_leading_comments_and_whitespace(&statement[index..]);
        index = statement.len() - remaining.len();
        let Some(byte) = remaining.as_bytes().first().copied() else {
            break;
        };
        let scope = scopes.last_mut().expect("root SQL scope is retained");
        if byte == b'\'' || byte == b'"' {
            let Some((value, consumed)) = leading_quoted_sql_literal_with_consumed(remaining)
            else {
                break;
            };
            if scope.expect_source {
                ranges.push(SqlSourceRefRange {
                    value,
                    start: index,
                    end: index + consumed,
                    quoted: true,
                });
            }
            scope.expect_source = false;
            scope.previous_word = "";
            index += consumed;
        } else if byte.is_ascii_alphabetic() || byte == b'_' {
            let consumed = remaining
                .bytes()
                .take_while(|byte| byte.is_ascii_alphanumeric() || *byte == b'_')
                .count();
            let word = &remaining[..consumed];
            if scope.expect_source {
                ranges.push(SqlSourceRefRange {
                    value: word.to_string(),
                    start: index,
                    end: index + consumed,
                    quoted: false,
                });
                scope.expect_source = false;
            } else if word.eq_ignore_ascii_case("select") {
                scope.select = true;
                scope.source_clause = false;
            } else if scope.select
                && ((word.eq_ignore_ascii_case("from")
                    && !scope.previous_word.eq_ignore_ascii_case("distinct"))
                    || (word.eq_ignore_ascii_case("join") && scope.source_clause))
            {
                scope.source_clause = true;
                scope.expect_source = true;
            } else if ["where", "group", "having", "order", "limit", "offset"]
                .iter()
                .any(|keyword| word.eq_ignore_ascii_case(keyword))
            {
                scope.source_clause = false;
            }
            scope.previous_word = word;
            index += consumed;
        } else {
            scope.expect_source = false;
            scope.previous_word = "";
            match byte {
                b'(' => scopes.push(SqlSourceScope::default()),
                b')' if scopes.len() > 1 => {
                    scopes.pop();
                }
                _ => {}
            }
            index += remaining
                .chars()
                .next()
                .expect("nonempty SQL tail")
                .len_utf8();
        }
    }
    ranges
}

fn declared_sql_source_identifier_matches(source_ref: &str, old_ref: &str) -> bool {
    if !is_summary_identifier(source_ref) {
        return false;
    }
    declared_sql_source_identifier_aliases(old_ref)
        .into_iter()
        .any(|alias| source_ref.eq_ignore_ascii_case(&alias))
}

fn declared_sql_source_identifier_aliases(source_ref: &str) -> Vec<String> {
    let path = Path::new(source_ref);
    let mut aliases = Vec::new();
    if let Some(stem) = path.file_stem().and_then(|stem| stem.to_str()) {
        aliases.push(stem.to_string());
    }
    if let Some(name) = path.file_name().and_then(|name| name.to_str()) {
        aliases.push(name.to_string());
    }
    aliases
        .into_iter()
        .filter(|alias| is_summary_identifier(alias))
        .collect()
}

fn sql_string_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn prepared_local_join_arg(
    arg: &str,
    right_source: Option<&PreparedLocalWorkflowRightSource>,
) -> String {
    let Some(right_source) = right_source else {
        return arg.to_string();
    };
    let Some((_old_right, rest)) = arg.split_once(',') else {
        return arg.to_string();
    };
    format!("{},{}", right_source.target.display(), rest)
}

fn local_prepared_vortex_execution_attachment_fields(
    request: &PublicWorkflowRouteRequest,
    plan: &PublicWorkflowRoutePlan,
    preparation: &sql_local_source_runtime::PublicWorkflowVortexPreparation,
    has_right_input: bool,
) -> Vec<(String, String)> {
    let mut fields = vec![
        (
            "public_workflow_local_source_route_id".to_string(),
            plan.route_id.to_string(),
        ),
        (
            "public_workflow_local_source_resolved_internal_command".to_string(),
            plan.resolved_internal_command.to_string(),
        ),
        (
            "public_workflow_local_source_format".to_string(),
            request
                .input_format
                .as_deref()
                .unwrap_or("not_declared")
                .to_string(),
        ),
        (
            "public_workflow_local_source_vortex_normalization_point".to_string(),
            "VortexPreparedState".to_string(),
        ),
        (
            "public_workflow_local_source_execution_mode".to_string(),
            "prepared_vortex_then_native_vortex".to_string(),
        ),
        (
            "public_workflow_local_source_preparation_included".to_string(),
            "true".to_string(),
        ),
        (
            "public_workflow_local_source_query_timing_starts_after_preparation".to_string(),
            "true".to_string(),
        ),
        (
            "public_workflow_local_source_vortex_ingest_performed".to_string(),
            preparation
                .fields
                .iter()
                .find(|(key, _)| key == "public_workflow_preparation_vortex_ingest_performed")
                .map_or_else(|| "not_reported".to_string(), |(_, value)| value.clone()),
        ),
        (
            "public_workflow_local_source_prepared_vortex_path".to_string(),
            preparation.target_path.display().to_string(),
        ),
        (
            "public_workflow_local_source_right_input_prepared".to_string(),
            has_right_input.to_string(),
        ),
    ];
    let olap_fields = local_prepared_olap_query_attachment_fields(&preparation.fields);
    fields.extend(preparation.fields.clone());
    fields.extend(olap_fields);
    fields
}

#[allow(clippy::too_many_lines)]
fn local_prepared_olap_query_attachment_fields(
    preparation_fields: &[(String, String)],
) -> Vec<(String, String)> {
    let status = preparation_olap_field_value(preparation_fields, "status", "not_reported");
    let query_time_contract =
        preparation_olap_field_value(preparation_fields, "query_time_contract", "not_reported");
    let sidecar_family_count =
        preparation_olap_field_value(preparation_fields, "exact_sidecar_family_count", "0");
    let artifact_model =
        preparation_olap_field_value(preparation_fields, "artifact_model", "not_reported");
    let metadata_pruning_contract = preparation_olap_field_value(
        preparation_fields,
        "metadata_pruning_contract",
        "not_reported",
    );
    let embedded_layout_statistics_contract = preparation_olap_field_value(
        preparation_fields,
        "embedded_layout_statistics_contract",
        "not_reported",
    );
    let query_answer_sidecar_status = preparation_olap_field_value(
        preparation_fields,
        "query_answer_sidecar_status",
        "not_reported",
    );
    let evidence_persistence =
        preparation_olap_field_value(preparation_fields, "evidence_persistence", "not_reported");
    let external_manifest_written = preparation_olap_field_value(
        preparation_fields,
        "external_manifest_written",
        "not_reported",
    );
    let layout_inventory_status = preparation_olap_field_value(
        preparation_fields,
        "layout_inventory_status",
        "not_reported",
    );
    let layout_inventory_digest = preparation_olap_field_value(
        preparation_fields,
        "layout_inventory_digest",
        "not_reported",
    );
    let layout_footer_row_count = preparation_olap_field_value(
        preparation_fields,
        "layout_footer_row_count",
        "not_reported",
    );
    let layout_footer_segment_count = preparation_olap_field_value(
        preparation_fields,
        "layout_footer_segment_count",
        "not_reported",
    );
    let layout_footer_statistics_status = preparation_olap_field_value(
        preparation_fields,
        "layout_footer_statistics_status",
        "not_reported",
    );
    let layout_footer_encoding_layout_status = preparation_olap_field_value(
        preparation_fields,
        "layout_footer_encoding_layout_status",
        "not_reported",
    );
    let layout_footer_approx_bytes = preparation_olap_field_value(
        preparation_fields,
        "layout_footer_approx_bytes",
        "not_reported",
    );
    let layout_footer_dtype_summary = preparation_olap_field_value(
        preparation_fields,
        "layout_footer_dtype_summary",
        "not_reported",
    );
    let layout_per_column_metadata_contract = preparation_olap_field_value(
        preparation_fields,
        "layout_per_column_metadata_contract",
        "not_reported",
    );
    let layout_operator_selection_metadata_status = preparation_olap_field_value(
        preparation_fields,
        "layout_operator_selection_metadata_status",
        "not_reported",
    );
    let layout_metadata_persisted_in_artifact = preparation_olap_field_value(
        preparation_fields,
        "layout_metadata_persisted_in_artifact",
        "not_reported",
    );
    let admitted = matches!(
        status,
        "prepared_olap_state_ready" | "prepared_olap_state_read_through_hit"
    ) && query_time_contract
        == "single_vortex_artifact_native_runtime_no_query_answer_sidecar";
    let embedded_layout_metadata_consumed =
        admitted && layout_metadata_persisted_in_artifact == "true";
    vec![
        (
            "public_workflow_prepared_olap_state_attached".to_string(),
            admitted.to_string(),
        ),
        (
            "public_workflow_prepared_olap_state_consumed".to_string(),
            "false".to_string(),
        ),
        (
            "public_workflow_prepared_olap_state_consumption_status".to_string(),
            if admitted {
                "attached_to_prepared_native_vortex_route_embedded_layout_metadata_available"
            } else {
                "not_admitted_for_query_time_attachment"
            }
            .to_string(),
        ),
        (
            "public_workflow_prepared_olap_embedded_layout_metadata_consumed".to_string(),
            embedded_layout_metadata_consumed.to_string(),
        ),
        (
            "public_workflow_prepared_olap_state_read_through_status".to_string(),
            if admitted {
                "attached_to_prepared_native_vortex_route_embedded_layout_metadata_available"
            } else {
                "not_admitted_for_query_time_read_through"
            }
            .to_string(),
        ),
        (
            "public_workflow_prepared_olap_state_query_time_contract".to_string(),
            query_time_contract.to_string(),
        ),
        (
            "public_workflow_prepared_olap_state_artifact_model".to_string(),
            artifact_model.to_string(),
        ),
        (
            "public_workflow_prepared_olap_state_metadata_pruning_contract".to_string(),
            metadata_pruning_contract.to_string(),
        ),
        (
            "public_workflow_prepared_olap_state_embedded_layout_statistics_contract".to_string(),
            embedded_layout_statistics_contract.to_string(),
        ),
        (
            "public_workflow_prepared_olap_state_query_answer_sidecar_status".to_string(),
            query_answer_sidecar_status.to_string(),
        ),
        (
            "public_workflow_prepared_olap_state_evidence_persistence".to_string(),
            evidence_persistence.to_string(),
        ),
        (
            "public_workflow_prepared_olap_state_external_manifest_written".to_string(),
            external_manifest_written.to_string(),
        ),
        (
            "public_workflow_prepared_olap_state_layout_inventory_status".to_string(),
            layout_inventory_status.to_string(),
        ),
        (
            "public_workflow_prepared_olap_state_layout_inventory_digest".to_string(),
            layout_inventory_digest.to_string(),
        ),
        (
            "public_workflow_prepared_olap_state_layout_footer_row_count".to_string(),
            layout_footer_row_count.to_string(),
        ),
        (
            "public_workflow_prepared_olap_state_layout_footer_segment_count".to_string(),
            layout_footer_segment_count.to_string(),
        ),
        (
            "public_workflow_prepared_olap_state_layout_footer_statistics_status".to_string(),
            layout_footer_statistics_status.to_string(),
        ),
        (
            "public_workflow_prepared_olap_state_layout_footer_encoding_layout_status".to_string(),
            layout_footer_encoding_layout_status.to_string(),
        ),
        (
            "public_workflow_prepared_olap_state_layout_footer_approx_bytes".to_string(),
            layout_footer_approx_bytes.to_string(),
        ),
        (
            "public_workflow_prepared_olap_state_layout_footer_dtype_summary".to_string(),
            layout_footer_dtype_summary.to_string(),
        ),
        (
            "public_workflow_prepared_olap_state_layout_per_column_metadata_contract".to_string(),
            layout_per_column_metadata_contract.to_string(),
        ),
        (
            "public_workflow_prepared_olap_state_layout_operator_selection_metadata_status"
                .to_string(),
            layout_operator_selection_metadata_status.to_string(),
        ),
        (
            "public_workflow_prepared_olap_state_layout_metadata_persisted_in_artifact".to_string(),
            layout_metadata_persisted_in_artifact.to_string(),
        ),
        (
            "public_workflow_prepared_olap_state_exact_sidecar_family_count".to_string(),
            sidecar_family_count.to_string(),
        ),
        (
            "public_workflow_prepared_olap_state_status".to_string(),
            status.to_string(),
        ),
    ]
}

fn preparation_olap_field_value<'a>(
    fields: &'a [(String, String)],
    suffix: &str,
    default: &'static str,
) -> &'a str {
    for prefix in [
        "public_workflow_preparation_vortex_prepared_olap_state_",
        "public_workflow_preparation_prepared_olap_state_",
    ] {
        let key = format!("{prefix}{suffix}");
        if let Some(value) = preparation_field_value(fields, &key) {
            return value;
        }
    }
    default
}

fn preparation_field_value<'a>(fields: &'a [(String, String)], key: &str) -> Option<&'a str> {
    fields
        .iter()
        .find(|(field, _)| field == key)
        .map(|(_, value)| value.as_str())
}

fn local_prepared_vortex_right_execution_attachment_fields(
    preparation: &sql_local_source_runtime::PublicWorkflowVortexPreparation,
) -> Vec<(String, String)> {
    let mut fields = vec![
        (
            "public_workflow_right_source_vortex_ingest_performed".to_string(),
            "true".to_string(),
        ),
        (
            "public_workflow_right_source_prepared_vortex_path".to_string(),
            preparation.target_path.display().to_string(),
        ),
    ];
    fields.extend(preparation.fields.iter().map(|(key, value)| {
        (
            key.replacen(
                "public_workflow_preparation_",
                "public_workflow_right_preparation_",
                1,
            ),
            value.clone(),
        )
    }));
    fields
}

#[allow(clippy::too_many_lines)] // Keep validation, admission and dispatch in one command boundary.
pub(crate) fn handle_public_workflow_prepare(
    args: impl Iterator<Item = String>,
    format: OutputFormat,
    context: &crate::command_context::CommandContext<'_>,
) -> ExitCode {
    let mut request = match PublicWorkflowRouteRequest::parse_execution_in_context(args, context) {
        Ok(request) => request,
        Err(error) => {
            return emit_error("prepare", format, "public workflow prepare failed", &error);
        }
    };
    request.requested_output = "prepare".to_string();
    request.execution_policy = "prepare_once".to_string();
    request.bounded = true;

    let plan = plan_public_workflow_route(&request);
    if plan.status != CommandStatus::Success {
        return emit_blocked_facade("prepare", format, &request, &plan);
    }
    let Some(input_uri) = request.input_uri.clone() else {
        let blocked = input_not_declared_route();
        return emit_blocked_facade("prepare", format, &request, &blocked);
    };
    let Some(output_ref) = request.output_ref.clone() else {
        let blocked = output_required_route("prepare", "prepared Vortex target");
        return emit_blocked_facade("prepare", format, &request, &blocked);
    };
    let source_format = request
        .input_format
        .clone()
        .unwrap_or_else(|| "csv".to_string());
    let resources = match public_workflow_effective_resource_envelope(&request) {
        Ok(values) => values,
        Err(error) => {
            return emit_error(
                "prepare",
                format,
                "public workflow resource validation failed",
                &error,
            );
        }
    };
    let admission = match context.admit_for_command(resources, "prepare", format) {
        Ok(admission) => admission,
        Err(code) => return code,
    };
    let preparation = match prepare_local_source_for_public_workflow_controlled(
        &input_uri,
        &source_format,
        Path::new(&output_ref),
        request.allow_overwrite,
        admission.resources(),
        Some(&admission),
        request.source_fingerprint_policy.as_deref(),
        request.source_schema.as_deref(),
        &admission.cancellation(),
    ) {
        Ok(preparation) => preparation,
        Err(PreparationFacadeError::FeatureGated) => {
            let blocked = local_file_vortex_ingest_feature_gated_route(&request);
            return emit_blocked_facade("prepare", format, &request, &blocked);
        }
        Err(PreparationFacadeError::Runtime(error)) => {
            return emit_error(
                "prepare",
                format,
                "public local Vortex preparation failed",
                &error,
            );
        }
    };

    let mut fields = execution_attachment_fields("prepare", &request, &plan);
    fields.extend([
        ("runtime_execution".to_string(), "true".to_string()),
        ("source_io_performed".to_string(), "true".to_string()),
        ("output_io_performed".to_string(), "true".to_string()),
        ("execution".to_string(), "performed".to_string()),
        ("fallback_attempted".to_string(), "false".to_string()),
        ("external_engine_invoked".to_string(), "false".to_string()),
        (
            "claim_gate_status".to_string(),
            "local_workflow_runtime_supported".to_string(),
        ),
        (
            "public_workflow_prepared_vortex_path".to_string(),
            preparation.target_path.display().to_string(),
        ),
    ]);
    let profile_runtime_fields = prepared_profile_runtime_plan_fields(&request, &preparation);
    if let Err(error) = preparation.validate_generation() {
        return emit_error(
            "prepare",
            format,
            "prepared source generation changed",
            &error,
        );
    }
    fields.extend(preparation.fields);
    fields.extend(profile_runtime_fields);
    emit(
        "prepare",
        format,
        CommandStatus::Success,
        "public workflow prepare completed".to_string(),
        String::new(),
        Vec::new(),
        fields,
    );
    ExitCode::SUCCESS
}

impl PublicWorkflowRouteRequest {
    fn parse(args: impl Iterator<Item = String>) -> Result<Self, ShardLoomError> {
        Self::parse_with_resource_requirement(args, false, None)
    }

    #[cfg(all(feature = "vortex-local-primitives", unix))]
    fn parse_execution(args: impl Iterator<Item = String>) -> Result<Self, ShardLoomError> {
        Self::parse_with_resource_requirement(args, true, None)
    }

    fn parse_execution_in_context(
        args: impl Iterator<Item = String>,
        context: &crate::command_context::CommandContext<'_>,
    ) -> Result<Self, ShardLoomError> {
        Self::parse_with_resource_requirement(args, true, Some(context))
    }

    fn parse_with_resource_requirement(
        args: impl Iterator<Item = String>,
        required: bool,
        context: Option<&crate::command_context::CommandContext<'_>>,
    ) -> Result<Self, ShardLoomError> {
        let mut args = args.peekable();
        let Some(surface) = args.next() else {
            return Err(ShardLoomError::InvalidOperation(
                "usage: shardloom route <sql|python|dataframe|cli> [--input <uri>] [--input-format <format>] [--source-schema <name:dtype,...>] [--source-bindings <json-uri-to-format-and-schema>] [--sql <statement>] [--plan <summary>] [--request <collect|prepare|write_vortex|write_parquet|write_arrow_ipc|write_avro|write_orc|write_csv|write_json|write_jsonl|explain|route|evidence>] [--output <ref>] [--fanout-output <format=local-path>]... [--execution-policy <vortex_middle|native_vortex|prepare_once>] [--materialization-policy <bounded|materialized|zero_decode|explicit>] [--evidence-level <report_only|runtime_smoke|production_admitted_local_workflow|claim_grade>] [--bounded true|false] [--allow-overwrite] [--source-fingerprint-policy <metadata_only|content_digest>] [--native-vortex-operation-family <family>] [--vortex-primitive <count|count_where|filter|project|filter_project|distinct|tail|sample|expression_project|melt|explode|pivot|rolling_window|aggregate|sort_rows>] [--vortex-predicate <tiny-predicate>] [--vortex-columns <columns>] [--vortex-source-order-limit <rows>] [--vortex-sample-fraction <fraction>] [--vortex-sample-seed <seed>] [--vortex-sample-replacement] [--vortex-expression-projection <json>] [--vortex-melt-projection <json>] [--vortex-explode-projection <json>] [--vortex-pivot-projection <json>] [--vortex-rolling-window <json>] [--vortex-aggregate <json>] [--vortex-sort-rows <json>] [--spill <workspace-quota-buffer-json>] [--memory-gb <n>] [--max-parallelism <n>]"
                    .to_string(),
            ));
        };
        let surface = normalize_surface(&surface)?;
        let mut request = Self::new(surface);
        let mut native_operator_options = false;

        while let Some(flag) = args.next() {
            native_operator_options |= flag.starts_with("--vortex-")
                || flag.starts_with("--native-vortex-")
                || matches!(flag.as_str(), "--operation-family" | "--right-input");
            request.parse_flag(&flag, &mut args)?;
        }
        request.native_operator_options = if native_operator_options {
            NativeOperatorOptions::Explicit
        } else {
            NativeOperatorOptions::Automatic
        };

        // Resolve before decoding caller-owned input cells or preparing a source.
        if required {
            if let Some(context) = context {
                request.resources = crate::execution_resources::ResourceArguments::from_resources(
                    context.resolve(&request.resources)?,
                );
            } else {
                request.resources.resolve()?;
            }
        } else {
            request.resources.optional()?;
        }
        if let Some(raw) = request.raw_source_bindings.take() {
            request.source_bindings = parse_public_source_bindings(&raw)?;
        }

        if native_operator_options
            && request
                .source_bindings
                .values()
                .any(|binding| binding.input_format == "memory")
        {
            return Err(ShardLoomError::InvalidOperation(
                "declared memory inputs require a complete SQL declaration; separate native operator options are not consumed; no fallback execution was attempted".into(),
            ));
        }

        request.infer_defaults();
        if !request.source_bindings.is_empty() {
            if request.requested_output == "prepare" {
                return Err(ShardLoomError::InvalidOperation(
                    "source bindings require an admitted SQL statement, not a preparation-only request; no fallback execution was attempted".into(),
                ));
            }
            if request
                .sql_statement
                .as_deref()
                .is_none_or(|sql| !native_relational_sql_candidate(sql))
            {
                request.bind_flat_source()?;
            }
        }
        if request.source_schema.is_some() && request.input_format.as_deref() == Some("vortex") {
            return Err(ShardLoomError::InvalidOperation(
                "--source-schema is supported only for compatibility input; native Vortex does not accept source schema hints; no fallback execution was attempted".into(),
            ));
        }
        Ok(request)
    }

    /// Reuse a single flat SQL declaration as the existing normalization input.
    /// All checks are syntactic: inspection must not open the source or target.
    fn bind_flat_source(&mut self) -> Result<(), ShardLoomError> {
        let invalid = || {
            ShardLoomError::InvalidOperation(
            "flat SQL source bindings require exactly one used source with matching input, format and schema declarations; no declaration is silently ignored and no fallback execution was attempted".into(),
        )
        };
        let shape = self
            .sql_statement
            .as_deref()
            .and_then(parse_native_vortex_sql_single_source_shape)
            .ok_or_else(invalid)?;
        if self.source_bindings.len() != 1
            || self
                .input_uri
                .as_ref()
                .is_some_and(|uri| uri != &shape.source_ref)
        {
            return Err(invalid());
        }
        let binding = self
            .source_bindings
            .get(&shape.source_ref)
            .ok_or_else(invalid)?;
        if self
            .input_format
            .as_ref()
            .is_some_and(|format| format != &binding.input_format)
            || self
                .source_schema
                .as_ref()
                .is_some_and(|schema| Some(schema) != binding.source_schema.as_ref())
        {
            return Err(invalid());
        }
        self.input_uri = Some(shape.source_ref);
        self.input_format = Some(binding.input_format.clone());
        self.source_schema.clone_from(&binding.source_schema);
        Ok(())
    }

    fn new(surface: String) -> Self {
        Self {
            surface,
            input_uri: None,
            input_format: None,
            source_schema: None,
            source_bindings: std::collections::BTreeMap::new(),
            raw_source_bindings: None,
            sql_statement: None,
            plan_summary: None,
            requested_output: "collect".to_string(),
            output_ref: None,
            execution_policy: "vortex_middle".to_string(),
            materialization_policy: "bounded".to_string(),
            evidence_level: "runtime_smoke".to_string(),
            bounded: false,
            allow_overwrite: false,
            fanout_outputs: Vec::new(),
            source_fingerprint_policy: None,
            native_operator_options: NativeOperatorOptions::Automatic,
            native_vortex_operation_family: None,
            native_vortex_right_input: None,
            vortex_primitive: None,
            vortex_predicate: None,
            vortex_columns: None,
            vortex_source_order_limit: None,
            vortex_sample_seed: None,
            vortex_sample_fraction: None,
            vortex_sample_replacement: false,
            vortex_sample_weight_column: None,
            vortex_duplicate_keep: None,
            vortex_deduplicate_key_columns: None,
            vortex_expression_projection: None,
            vortex_melt_projection: None,
            vortex_explode_projection: None,
            vortex_pivot_projection: None,
            vortex_rolling_window: None,
            vortex_aggregate: None,
            vortex_sort_rows: None,
            resources: ResourceArguments::default(),
            spill: None,
        }
    }

    #[allow(clippy::too_many_lines)]
    fn parse_flag(
        &mut self,
        flag: &str,
        args: &mut std::iter::Peekable<impl Iterator<Item = String>>,
    ) -> Result<(), ShardLoomError> {
        if self.resources.parse_flag(flag, args)? {
            return Ok(());
        }
        match flag {
            "--spill" => {
                if self.spill.is_some() {
                    return Err(ShardLoomError::InvalidOperation(
                        "--spill may be declared only once; no fallback execution was attempted"
                            .into(),
                    ));
                }
                self.spill = Some(spill::Options::parse(&required_value(args, flag)?)?);
            }
            "--input" => self.input_uri = Some(required_value(args, "--input")?),
            "--input-format" => {
                self.input_format = Some(normalize_input_format(&required_value(
                    args,
                    "--input-format",
                )?)?);
            }
            "--source-schema" => {
                let value = required_value(args, "--source-schema")?;
                sql_local_source_runtime::parse_vortex_ingest_schema_hints(&value)?;
                self.source_schema = Some(value);
            }
            "--source-bindings" => {
                if self.raw_source_bindings.is_some() {
                    return Err(ShardLoomError::InvalidOperation(
                        "--source-bindings may be declared only once".into(),
                    ));
                }
                self.raw_source_bindings = Some(required_value(args, flag)?);
            }
            "--sql" => {
                if self.sql_statement.is_some() {
                    return Err(ShardLoomError::InvalidOperation(
                        "--sql may be declared only once; no fallback execution was attempted"
                            .into(),
                    ));
                }
                self.sql_statement = Some(required_value(args, "--sql")?);
            }
            "--plan" => self.plan_summary = Some(required_value(args, "--plan")?),
            "--request" | "--requested-output" => {
                self.requested_output = normalize_requested_output(&required_value(args, flag)?)?;
            }
            "--output" => self.output_ref = Some(required_value(args, "--output")?),
            "--execution-policy" => {
                self.execution_policy =
                    normalize_execution_policy(&required_value(args, "--execution-policy")?)?;
            }
            "--materialization-policy" | "--decode-policy" => {
                self.materialization_policy =
                    normalize_materialization_policy(&required_value(args, flag)?)?;
            }
            "--evidence-level" => {
                self.evidence_level =
                    normalize_evidence_level(&required_value(args, "--evidence-level")?)?;
            }
            "--bounded" => {
                self.bounded = parse_bool_flag("--bounded", &required_value(args, "--bounded")?)?;
            }
            "--allow-overwrite" => self.allow_overwrite = true,
            "--source-fingerprint-policy" => {
                self.source_fingerprint_policy = Some(normalize_source_fingerprint_policy(
                    &required_value(args, "--source-fingerprint-policy")?,
                )?);
            }
            "--source-content-fingerprint" => {
                self.source_fingerprint_policy = Some("content_digest".to_string());
            }
            "--fanout-output" => self
                .fanout_outputs
                .push(required_value(args, "--fanout-output")?),
            "--native-vortex-operation-family" | "--operation-family" => {
                self.native_vortex_operation_family = Some(required_value(args, flag)?);
            }
            "--native-vortex-right-input" | "--right-input" => {
                self.native_vortex_right_input = Some(required_value(args, flag)?);
            }
            "--vortex-primitive" => {
                self.vortex_primitive = Some(required_value(args, "--vortex-primitive")?);
            }
            "--vortex-predicate" => {
                self.vortex_predicate = Some(required_value(args, "--vortex-predicate")?);
            }
            "--vortex-columns" => {
                self.vortex_columns = Some(required_value(args, "--vortex-columns")?);
            }
            "--vortex-source-order-limit" => {
                self.vortex_source_order_limit =
                    Some(required_value(args, "--vortex-source-order-limit")?);
            }
            "--vortex-sample-seed" => {
                self.vortex_sample_seed = Some(required_value(args, "--vortex-sample-seed")?);
            }
            "--vortex-sample-fraction" => {
                self.vortex_sample_fraction =
                    Some(required_value(args, "--vortex-sample-fraction")?);
            }
            "--vortex-sample-replacement" => {
                self.vortex_sample_replacement = true;
            }
            "--vortex-sample-weight-column" => {
                self.vortex_sample_weight_column =
                    Some(required_value(args, "--vortex-sample-weight-column")?);
            }
            "--vortex-duplicate-keep" => {
                self.vortex_duplicate_keep = Some(normalize_duplicate_keep(&required_value(
                    args,
                    "--vortex-duplicate-keep",
                )?)?);
            }
            "--vortex-deduplicate-key-columns" | "--vortex-drop-duplicate-key-columns" => {
                self.vortex_deduplicate_key_columns = Some(required_value(args, flag)?);
            }
            "--vortex-expression-projection" => {
                self.vortex_expression_projection =
                    Some(required_value(args, "--vortex-expression-projection")?);
            }
            "--vortex-melt-projection" => {
                self.vortex_melt_projection =
                    Some(required_value(args, "--vortex-melt-projection")?);
            }
            "--vortex-explode-projection" => {
                self.vortex_explode_projection =
                    Some(required_value(args, "--vortex-explode-projection")?);
            }
            "--vortex-pivot-projection" => {
                self.vortex_pivot_projection =
                    Some(required_value(args, "--vortex-pivot-projection")?);
            }
            "--vortex-rolling-window" => {
                self.vortex_rolling_window = Some(required_value(args, "--vortex-rolling-window")?);
            }
            "--vortex-aggregate" => {
                self.vortex_aggregate = Some(required_value(args, "--vortex-aggregate")?);
            }
            "--vortex-sort-rows" => {
                self.vortex_sort_rows = Some(required_value(args, "--vortex-sort-rows")?);
            }
            extra => return Err(cli_unknown_arg_error("route", extra)),
        }
        Ok(())
    }

    fn infer_defaults(&mut self) {
        if self.sql_statement.is_none() && self.plan_summary.is_none() {
            self.plan_summary = Some(format!("{} workflow", self.surface));
        }
        if self.input_uri.is_none() {
            self.input_uri = self.sql_statement.as_deref().and_then(|statement| {
                extract_first_declared_sql_source_ref(statement)
                    .filter(|source| self.source_bindings.contains_key(source))
                    .or_else(|| extract_first_quoted_source_ref(statement))
                    .or_else(|| {
                        self.input_format
                            .as_deref()
                            .filter(|format| is_local_file_format(format) || *format == "vortex")
                            .and_then(|_| extract_first_declared_sql_source_ref(statement))
                    })
            });
        }
        if self.input_format.is_none() {
            self.input_format = self.input_uri.as_deref().and_then(|uri| {
                self.source_bindings
                    .get(uri)
                    .map(|binding| binding.input_format.clone())
                    .or_else(|| infer_input_format_from_ref(uri).map(str::to_string))
            });
        }
        self.bind_single_source_alias();
        if !self.bounded {
            self.bounded = self
                .sql_statement
                .as_deref()
                .is_some_and(sql_statement_has_limit)
                || self
                    .plan_summary
                    .as_deref()
                    .is_some_and(plan_summary_has_limit)
                || !matches!(self.requested_output.as_str(), "collect");
        }
    }

    /// Preserve the native single --input shorthand for an unquoted SQL table
    /// name. Bind before selection so compatibility and extended-type providers
    /// receive the same exact source; explicit binding maps remain authoritative.
    fn bind_single_source_alias(&mut self) {
        if !self.source_bindings.is_empty()
            || self.native_vortex_right_input.is_some()
            || !self
                .input_format
                .as_deref()
                .is_some_and(|format| format == "vortex" || is_local_file_format(format))
        {
            return;
        }
        let (Some(input), Some(statement)) =
            (self.input_uri.as_deref(), self.sql_statement.as_deref())
        else {
            return;
        };
        if parse_native_vortex_sql_single_source_shape(statement).is_none() {
            return;
        }
        let sources = sql_source_ref_ranges(statement);
        let [source] = sources.as_slice() else {
            return;
        };
        if source.quoted || !is_summary_identifier(&source.value) {
            return;
        }
        let mut rebound = statement.to_string();
        rebound.replace_range(source.start..source.end, &sql_string_literal(input));
        self.sql_statement = Some(rebound);
    }
}

fn plan_public_workflow_route(request: &PublicWorkflowRouteRequest) -> PublicWorkflowRoutePlan {
    spill::validate_route(request, plan_public_workflow_route_inner(request))
}

fn plan_public_workflow_route_inner(
    request: &PublicWorkflowRouteRequest,
) -> PublicWorkflowRoutePlan {
    if matches!(request.requested_output.as_str(), "collect") && !request.fanout_outputs.is_empty()
    {
        return collect_fanout_blocked_route();
    }
    if matches!(request.requested_output.as_str(), "collect") && !request.bounded {
        return unbounded_collect_blocked_route();
    }

    #[cfg(all(feature = "vortex-local-primitives", unix))]
    if uses_source_demand(request)
        && let Some(plan) = resident_relational::route_admitted_statement(request)
    {
        return plan;
    }

    if request
        .source_bindings
        .values()
        .any(|binding| binding.input_format == "memory")
    {
        #[cfg(all(feature = "vortex-local-primitives", unix))]
        if let Some(plan) = resident_relational::route_admitted_statement(request) {
            return plan;
        }
        return native_vortex_materializing_primitive_feature_gated_route();
    }

    #[cfg(all(feature = "vortex-local-primitives", unix))]
    if let Some(plan) = resident_sql::route(request) {
        return plan;
    }

    if request.requested_output != "prepare"
        && (request.input_uri.is_none()
            || request
                .sql_statement
                .as_deref()
                .is_some_and(native_relational_sql_candidate))
    {
        #[cfg(all(feature = "vortex-local-primitives", unix))]
        if let Some(plan) = resident_relational::route(request) {
            return plan;
        }
        #[cfg(not(all(feature = "vortex-local-primitives", unix)))]
        return native_vortex_materializing_primitive_feature_gated_route();
    }

    if request.execution_policy == "native_vortex"
        && request.input_format.as_deref() != Some("vortex")
    {
        return native_vortex_input_required_route();
    }

    if request.input_format.as_deref() == Some("vortex") && request.requested_output == "prepare" {
        return native_vortex_artifact_prepare_route(request);
    }

    if is_native_vortex_route(request) {
        return native_vortex_route(request);
    }

    match request.input_format.as_deref() {
        Some(format) if is_local_file_format(format) => local_file_route(request),
        Some(other) => input_format_not_admitted_route(other),
        None => input_not_declared_route(),
    }
}

fn unbounded_collect_blocked_route() -> PublicWorkflowRoutePlan {
    blocked_route(
        "cg21.route.unbounded_collect_blocked",
        "bounded collect requires an explicit LIMIT, bounded=true, or a write/materialized output request",
        Diagnostic::materialization_required(
            "public_workflow_route.collect",
            "unbounded public collect is not admitted through the facade",
            "add LIMIT, pass bounded=true for a proven bounded request, or use an explicit write route",
        ),
    )
}

fn collect_fanout_blocked_route() -> PublicWorkflowRoutePlan {
    blocked_route(
        "cg21.route.collect_fanout_blocked",
        "collect routes cannot carry fanout output payloads",
        Diagnostic::new(
            DiagnosticCode::InvalidInput,
            DiagnosticSeverity::Error,
            DiagnosticCategory::InvalidInput,
            "public collect routes do not admit fanout outputs".to_string(),
            Some("public_workflow_route.fanout_outputs".to_string()),
            Some("fanout outputs require an explicit write request".to_string()),
            Some(
                "use --request write_jsonl/write_csv/etc. with --output and --fanout-output"
                    .to_string(),
            ),
            FallbackStatus::disabled_by_policy(),
        ),
    )
}

fn is_native_vortex_route(request: &PublicWorkflowRouteRequest) -> bool {
    request.input_format.as_deref() == Some("vortex")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PublicVortexPrimitive {
    Count,
    CountWhere,
    Filter,
    Project,
    FilterProject,
    Distinct,
    DropDuplicates,
    DuplicateMask,
    Tail,
    Sample,
    ExpressionProject,
    Melt,
    Explode,
    Pivot,
    RollingWindow,
    Aggregate,
    SortRows,
}

impl PublicVortexPrimitive {
    fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "count" | "count_all" => Some(Self::Count),
            "count_where" | "count-where" | "filter_count" | "filtered_count" => {
                Some(Self::CountWhere)
            }
            "filter" | "filter_predicate" => Some(Self::Filter),
            "project" | "project_columns" => Some(Self::Project),
            "filter_project" | "filter-project" | "filter_and_project" => Some(Self::FilterProject),
            "distinct" | "distinct_rows" | "unique" => Some(Self::Distinct),
            "drop_duplicates"
            | "drop-duplicates"
            | "drop_duplicate_rows"
            | "drop-duplicate-rows"
            | "deduplicate"
            | "dedup" => Some(Self::DropDuplicates),
            "duplicate_mask" | "duplicate-mask" | "duplicate_mask_rows" | "duplicated" => {
                Some(Self::DuplicateMask)
            }
            "tail" | "tail_rows" | "source_order_tail" => Some(Self::Tail),
            "sample" | "sample_rows" | "deterministic_sample" => Some(Self::Sample),
            "expression_project"
            | "expression-project"
            | "expression_project_rows"
            | "mask"
            | "replace" => Some(Self::ExpressionProject),
            "melt" | "melt_rows" | "melt-rows" | "unpivot" => Some(Self::Melt),
            "explode" | "explode_rows" | "explode-rows" | "list_explode" | "list-explode" => {
                Some(Self::Explode)
            }
            "pivot" | "pivot_rows" | "pivot-rows" | "pivot_table" | "pivot-table" => {
                Some(Self::Pivot)
            }
            "rolling" | "rolling_window" | "rolling-window" | "rolling_rows" | "rolling-rows"
            | "rolling_sum" | "rolling-sum" | "rolling_mean" | "rolling-mean" | "rolling_count"
            | "rolling-count" => Some(Self::RollingWindow),
            "aggregate" | "aggregation" | "simple_aggregate" | "simple-aggregate"
            | "scalar_aggregate" | "scalar-aggregate" => Some(Self::Aggregate),
            "sort" | "sort_rows" | "sort-rows" | "order_by" | "order-rows" | "order_rows" => {
                Some(Self::SortRows)
            }
            _ => None,
        }
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::Count => "count",
            Self::CountWhere => "count_where",
            Self::Filter => "filter",
            Self::Project => "project",
            Self::FilterProject => "filter_project",
            Self::Distinct => "distinct",
            Self::DropDuplicates => "drop_duplicates",
            Self::DuplicateMask => "duplicate_mask",
            Self::Tail => "tail",
            Self::Sample => "sample",
            Self::ExpressionProject => "expression_project",
            Self::Melt => "melt",
            Self::Explode => "explode",
            Self::Pivot => "pivot",
            Self::RollingWindow => "rolling_window",
            Self::Aggregate => "aggregate",
            Self::SortRows => "sort_rows",
        }
    }

    const fn route_id(self) -> &'static str {
        match self {
            Self::Count => "native_vortex_count_all",
            Self::CountWhere => "native_vortex_count_where",
            Self::Filter => "native_vortex_filter",
            Self::Project => "native_vortex_project",
            Self::FilterProject => "native_vortex_filter_project",
            Self::Distinct => "native_vortex_distinct",
            Self::DropDuplicates => "native_vortex_drop_duplicates",
            Self::DuplicateMask => "native_vortex_duplicate_mask",
            Self::Tail => "native_vortex_tail",
            Self::Sample => "native_vortex_sample",
            Self::ExpressionProject => "native_vortex_expression_project",
            Self::Melt => "native_vortex_melt",
            Self::Explode => "native_vortex_explode",
            Self::Pivot => "native_vortex_pivot",
            Self::RollingWindow => "native_vortex_rolling_window",
            Self::Aggregate => "native_vortex_aggregate",
            Self::SortRows => "native_vortex_sort_rows",
        }
    }

    const fn resolved_internal_command(self) -> &'static str {
        match self {
            Self::CountWhere => "vortex-count-where",
            Self::Filter => "vortex-filter",
            Self::Project => "vortex-project",
            Self::FilterProject => "vortex-filter-project",
            Self::Count
            | Self::Distinct
            | Self::DropDuplicates
            | Self::DuplicateMask
            | Self::Tail
            | Self::Sample
            | Self::ExpressionProject
            | Self::Melt
            | Self::Explode
            | Self::Pivot
            | Self::RollingWindow
            | Self::Aggregate
            | Self::SortRows => "vortex-run",
        }
    }

    const fn requires_predicate(self) -> bool {
        matches!(self, Self::CountWhere | Self::Filter | Self::FilterProject)
    }

    const fn requires_columns(self) -> bool {
        matches!(
            self,
            Self::Project
                | Self::FilterProject
                | Self::DuplicateMask
                | Self::ExpressionProject
                | Self::Explode
        )
    }

    const fn requires_deduplicate_key_columns(self) -> bool {
        matches!(self, Self::DropDuplicates)
    }

    const fn requires_expression_projection(self) -> bool {
        matches!(self, Self::ExpressionProject)
    }

    const fn requires_melt_projection(self) -> bool {
        matches!(self, Self::Melt)
    }

    const fn requires_explode_projection(self) -> bool {
        matches!(self, Self::Explode)
    }

    const fn requires_pivot_projection(self) -> bool {
        matches!(self, Self::Pivot)
    }

    const fn requires_rolling_window(self) -> bool {
        matches!(self, Self::RollingWindow)
    }

    const fn requires_aggregate(self) -> bool {
        matches!(self, Self::Aggregate)
    }

    const fn requires_sort_rows(self) -> bool {
        matches!(self, Self::SortRows)
    }

    const fn allows_source_order_limit(self) -> bool {
        matches!(
            self,
            Self::Filter
                | Self::Project
                | Self::FilterProject
                | Self::Distinct
                | Self::DropDuplicates
                | Self::DuplicateMask
                | Self::Tail
                | Self::Sample
                | Self::ExpressionProject
                | Self::Melt
                | Self::Explode
                | Self::Pivot
                | Self::RollingWindow
                | Self::Aggregate
                | Self::SortRows
        )
    }

    const fn requires_local_primitives_feature(self) -> bool {
        matches!(
            self,
            Self::Distinct
                | Self::DropDuplicates
                | Self::DuplicateMask
                | Self::Tail
                | Self::Sample
                | Self::ExpressionProject
                | Self::Melt
                | Self::Explode
                | Self::Pivot
                | Self::RollingWindow
                | Self::Aggregate
                | Self::SortRows
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NativeVortexOperationFamily {
    Count,
    FilterProjectLimit,
    Aggregate,
    Join,
    TopN,
    Cast,
    Contains,
    Distinct,
    DuplicateMask,
    Sample,
    ExpressionProject,
    Melt,
    Explode,
    Pivot,
    RollingWindow,
    Profile,
    Sink,
    GeneralQuery,
}

impl NativeVortexOperationFamily {
    fn parse(value: &str) -> Option<Self> {
        let normalized = value.trim().to_ascii_lowercase().replace('-', "_");
        match normalized.as_str() {
            "count" | "count_where" | "filter_count" | "primitive_count" => Some(Self::Count),
            "filter"
            | "project"
            | "filter_project"
            | "filter_project_limit"
            | "filter_project_limit_collect"
            | "primitive_filter_project" => Some(Self::FilterProjectLimit),
            "aggregate"
            | "aggregation"
            | "group_by"
            | "grouped_count_sum"
            | "null_heavy_aggregate" => Some(Self::Aggregate),
            "join" | "hash_join" | "join_state" | "multi_input_join" => Some(Self::Join),
            "top_n" | "topn" | "global_top_n" | "nlargest" | "nsmallest" | "tail" | "tail_rows" => {
                Some(Self::TopN)
            }
            "cast" | "try_cast" | "trycast" => Some(Self::Cast),
            "contains" | "substring_contains" | "string_contains" => Some(Self::Contains),
            "distinct" | "unique" => Some(Self::Distinct),
            "deduplicate" | "dedup" | "drop_duplicates" | "drop_duplicate_rows" => {
                Some(Self::Distinct)
            }
            "duplicate_mask" | "duplicate_mask_rows" | "duplicated" => Some(Self::DuplicateMask),
            "sample" | "sampling" | "sample_rows" | "deterministic_sample" => Some(Self::Sample),
            "expression_project"
            | "expression_project_rows"
            | "mask"
            | "replace"
            | "conditional_rewrite"
            | "value_rewrite" => Some(Self::ExpressionProject),
            "melt" | "melt_rows" | "reshape" | "unpivot" => Some(Self::Melt),
            "explode" | "explode_rows" | "list_explode" | "nested_expansion" => Some(Self::Explode),
            "pivot" | "pivot_rows" | "pivot_table" | "pivot_wide_reshape" => Some(Self::Pivot),
            "rolling" | "rolling_window" | "rolling_rows" | "rolling_sum" | "rolling_mean"
            | "rolling_count" | "window" => Some(Self::RollingWindow),
            "profile" | "schema_profile" | "bounded_profile" => Some(Self::Profile),
            "sink" | "write" | "write_vortex" | "write_json" | "write_jsonl" | "write_csv"
            | "write_parquet" | "write_arrow_ipc" => Some(Self::Sink),
            "query" | "general" | "general_query" | "unshaped_query" => Some(Self::GeneralQuery),
            _ => None,
        }
    }

    const fn from_primitive(primitive: PublicVortexPrimitive) -> Self {
        match primitive {
            PublicVortexPrimitive::Count | PublicVortexPrimitive::CountWhere => Self::Count,
            PublicVortexPrimitive::Filter
            | PublicVortexPrimitive::Project
            | PublicVortexPrimitive::FilterProject => Self::FilterProjectLimit,
            PublicVortexPrimitive::Distinct | PublicVortexPrimitive::DropDuplicates => {
                Self::Distinct
            }
            PublicVortexPrimitive::DuplicateMask => Self::DuplicateMask,
            PublicVortexPrimitive::Tail | PublicVortexPrimitive::SortRows => Self::TopN,
            PublicVortexPrimitive::Sample => Self::Sample,
            PublicVortexPrimitive::ExpressionProject => Self::ExpressionProject,
            PublicVortexPrimitive::Melt => Self::Melt,
            PublicVortexPrimitive::Explode => Self::Explode,
            PublicVortexPrimitive::Pivot => Self::Pivot,
            PublicVortexPrimitive::RollingWindow => Self::RollingWindow,
            PublicVortexPrimitive::Aggregate => Self::Aggregate,
        }
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::Count => "count",
            Self::FilterProjectLimit => "filter_project_limit",
            Self::Aggregate => "aggregate",
            Self::Join => "join",
            Self::TopN => "top_n",
            Self::Cast => "cast",
            Self::Contains => "contains",
            Self::Distinct => "distinct",
            Self::DuplicateMask => "duplicate_mask",
            Self::Sample => "sample",
            Self::ExpressionProject => "expression_project",
            Self::Melt => "melt",
            Self::Explode => "explode",
            Self::Pivot => "pivot",
            Self::RollingWindow => "rolling_window",
            Self::Profile => "profile",
            Self::Sink => "sink",
            Self::GeneralQuery => "general_query",
        }
    }

    const fn allows_primitive(self, primitive: PublicVortexPrimitive) -> bool {
        matches!(
            (self, primitive),
            (
                Self::Count,
                PublicVortexPrimitive::Count | PublicVortexPrimitive::CountWhere
            ) | (
                Self::FilterProjectLimit,
                PublicVortexPrimitive::Filter
                    | PublicVortexPrimitive::Project
                    | PublicVortexPrimitive::FilterProject
            ) | (
                Self::Distinct,
                PublicVortexPrimitive::Distinct | PublicVortexPrimitive::DropDuplicates
            ) | (Self::DuplicateMask, PublicVortexPrimitive::DuplicateMask)
                | (
                    Self::TopN,
                    PublicVortexPrimitive::Tail | PublicVortexPrimitive::SortRows
                )
                | (Self::Sample, PublicVortexPrimitive::Sample)
                | (
                    Self::ExpressionProject,
                    PublicVortexPrimitive::ExpressionProject
                )
                | (Self::Melt, PublicVortexPrimitive::Melt)
                | (Self::Explode, PublicVortexPrimitive::Explode)
                | (Self::Pivot, PublicVortexPrimitive::Pivot)
                | (Self::RollingWindow, PublicVortexPrimitive::RollingWindow)
                | (Self::Aggregate, PublicVortexPrimitive::Aggregate)
        )
    }

    const fn blocker_id(self) -> &'static str {
        match self {
            Self::Aggregate => "py-vortex-route-unify-1.native_vortex_aggregate_route_missing",
            Self::Join => "py-vortex-route-unify-1.native_vortex_join_state_missing",
            Self::TopN => "py-vortex-route-unify-1.native_vortex_top_n_route_missing",
            Self::Cast => "py-vortex-route-unify-1.native_vortex_cast_route_missing",
            Self::Contains => "py-vortex-route-unify-1.native_vortex_contains_route_missing",
            Self::Distinct => "py-vortex-route-unify-1.native_vortex_distinct_route_missing",
            Self::DuplicateMask => {
                "py-vortex-route-unify-1.native_vortex_duplicate_mask_route_missing"
            }
            Self::Sample => "py-vortex-route-unify-1.native_vortex_sample_route_missing",
            Self::ExpressionProject => {
                "py-vortex-route-unify-1.native_vortex_expression_project_route_missing"
            }
            Self::Melt => "py-vortex-route-unify-1.native_vortex_melt_route_missing",
            Self::Explode => "py-vortex-route-unify-1.native_vortex_explode_route_missing",
            Self::Pivot => "py-vortex-route-unify-1.native_vortex_pivot_route_missing",
            Self::RollingWindow => {
                "py-vortex-route-unify-1.native_vortex_rolling_window_route_missing"
            }
            Self::Profile => "py-vortex-route-unify-1.native_vortex_profile_route_missing",
            Self::Sink => "py-vortex-route-unify-1.native_vortex_sink_contract_missing",
            Self::Count | Self::FilterProjectLimit | Self::GeneralQuery => {
                "py-vortex-route-unify-1.native_vortex_general_route_missing"
            }
        }
    }

    const fn blocker_reason(self) -> &'static str {
        match self {
            Self::Aggregate => "native Vortex aggregate user route is not admitted yet",
            Self::Join => "native Vortex multi-input join state is not admitted yet",
            Self::TopN => "native Vortex global top-N user route is not admitted yet",
            Self::Cast => "native Vortex cast/try-cast user route is not admitted yet",
            Self::Contains => "native Vortex substring contains user route is not admitted yet",
            Self::Distinct => "native Vortex row-level distinct/dedup route is not admitted yet",
            Self::DuplicateMask => "native Vortex duplicate-mask route is not admitted yet",
            Self::Sample => "native Vortex deterministic sample route is not admitted yet",
            Self::ExpressionProject => {
                "native Vortex expression projection route is not admitted yet"
            }
            Self::Melt => "native Vortex scoped melt route is not admitted yet",
            Self::Explode => {
                "native Vortex scoped explode route requires an admitted single-column list primitive payload"
            }
            Self::Pivot => {
                "native Vortex scoped pivot route requires an admitted single-index/single-column/single-value primitive payload"
            }
            Self::RollingWindow => "native Vortex scoped rolling-window route is not admitted yet",
            Self::Profile => "native Vortex bounded profile/statistics route is not admitted yet",
            Self::Sink => "native Vortex typed sink contract is not admitted yet",
            Self::Count | Self::FilterProjectLimit | Self::GeneralQuery => {
                "native Vortex user route requires an admitted primitive payload or promoted operator route"
            }
        }
    }

    const fn required_evidence(self) -> &'static str {
        match self {
            Self::Count | Self::FilterProjectLimit => {
                "native_vortex_input;public_facade;operator_result;decode_materialization_boundary;fallback_disabled"
            }
            Self::Aggregate => {
                "python_expression_lowering;native_vortex_group_state;decoded_reference_correctness;route_certificate"
            }
            Self::Join => {
                "multi_input_binding;native_vortex_build_probe_state;decoded_reference_correctness;route_certificate"
            }
            Self::TopN => {
                "native_vortex_ordering_semantics;bounded_result_contract;decoded_reference_correctness;route_certificate"
            }
            Self::Cast => {
                "native_vortex_cast_semantics;fail_closed_diagnostics;decoded_reference_correctness;route_certificate"
            }
            Self::Contains => {
                "native_vortex_string_contains_kernel;utf8_semantics;decoded_reference_correctness;route_certificate"
            }
            Self::Distinct => {
                "native_vortex_distinct_state;null_equality_semantics;bounded_result_contract;decoded_reference_correctness;route_certificate"
            }
            Self::DuplicateMask => {
                "native_vortex_duplicate_mask_state;keep_policy_semantics;bounded_result_contract;decoded_reference_correctness;route_certificate"
            }
            Self::Sample => {
                "native_vortex_sample_scan;deterministic_seed_policy;bounded_result_contract;decoded_reference_correctness;route_certificate"
            }
            Self::ExpressionProject => {
                "native_vortex_expression_projection;typed_scalar_rewrite;decode_materialization_boundary;decoded_reference_correctness;route_certificate"
            }
            Self::Melt => {
                "native_vortex_melt;typed_melt_projection;heterogeneous_scalar_value_representation;decode_materialization_boundary;decoded_reference_correctness;route_certificate"
            }
            Self::Explode => {
                "native_vortex_explode;typed_list_projection;list_element_scalar_contract;decode_materialization_boundary;decoded_reference_correctness;route_certificate"
            }
            Self::Pivot => {
                "native_vortex_pivot;single_index_column;single_pivot_column;single_value_column;wide_reshape_state;decode_materialization_boundary;decoded_reference_correctness;route_certificate"
            }
            Self::RollingWindow => {
                "native_vortex_rolling_window;source_order_window_state;sum_mean_count_semantics;decode_materialization_boundary;decoded_reference_correctness;route_certificate"
            }
            Self::Profile => {
                "metadata_first_profile;vortex_statistics_or_bounded_decode_contract;schema_correctness;route_certificate"
            }
            Self::Sink => {
                "typed_sink_contract;native_vortex_output_or_compatibility_sink_evidence;decode_materialization_boundary;route_certificate"
            }
            Self::GeneralQuery => {
                "declared_operation_family;native_vortex_operator_route;decoded_reference_correctness;route_certificate"
            }
        }
    }

    const fn next_action(self) -> &'static str {
        match self {
            Self::Count | Self::FilterProjectLimit => {
                "pass the supported --vortex-primitive payload for this operation family"
            }
            Self::Aggregate => {
                "implement and certify native Vortex grouped count/sum route before admitting this family"
            }
            Self::Join => {
                "implement and certify native Vortex multi-input build/probe state before admitting this family"
            }
            Self::TopN => {
                "implement and certify native Vortex global top-N route before admitting this family"
            }
            Self::Cast => {
                "implement and certify native Vortex cast/try-cast semantics before admitting this family"
            }
            Self::Contains => {
                "implement and certify native Vortex substring contains kernel before admitting this family"
            }
            Self::Distinct => {
                "implement and certify native Vortex row-level distinct/dedup state before admitting this family"
            }
            Self::DuplicateMask => {
                "route duplicated(keep='first'|'last'|False) through the admitted native Vortex duplicate-mask primitive with declared subset columns"
            }
            Self::Sample => {
                "route deterministic sample through the admitted native Vortex sample primitive with a bounded sample size and seed"
            }
            Self::ExpressionProject => {
                "route scoped mask/replace through the admitted native Vortex expression-project primitive with a typed rewrite payload"
            }
            Self::Melt => {
                "route scoped melt through the admitted native Vortex melt primitive with explicit id/value columns"
            }
            Self::Explode => {
                "route scoped explode through the admitted native Vortex explode primitive with one declared list column"
            }
            Self::Pivot => {
                "route scoped pivot/pivot_table through the admitted native Vortex pivot primitive with one index, one pivot, and one value column"
            }
            Self::RollingWindow => {
                "route scoped rolling-window sum/mean/count through the admitted native Vortex rolling-window primitive with explicit source/order/window payload"
            }
            Self::Profile => {
                "implement and certify metadata-first Vortex profile/statistics route before admitting this family"
            }
            Self::Sink => {
                "implement and certify typed native Vortex sink contract before admitting this family"
            }
            Self::GeneralQuery => {
                "declare an operation family and route only admitted native Vortex primitive or promoted operator families"
            }
        }
    }
}

fn native_vortex_route(request: &PublicWorkflowRouteRequest) -> PublicWorkflowRoutePlan {
    let mut effective_request = request.clone();
    if let Some(payload) = infer_native_vortex_route_payload(&effective_request) {
        payload.apply(&mut effective_request);
    }
    if effective_request.input_uri.is_none() {
        return input_not_declared_route();
    }
    let Ok(requested_family) = normalized_native_vortex_operation_family(&effective_request) else {
        return native_vortex_payload_blocked_route(
            "public_workflow_route.native_vortex_operation_family",
            "unsupported native Vortex operation family",
            "use count, filter_project_limit, aggregate, join, top_n, cast, contains, distinct, duplicate_mask, sample, expression_project, melt, explode, rolling_window, profile, or sink",
        );
    };
    let primitive = normalized_vortex_primitive(&effective_request);
    if is_write_request(&effective_request)
        || effective_request.output_ref.is_some()
        || !effective_request.fanout_outputs.is_empty()
    {
        if let Some(primitive) = primitive {
            return native_vortex_primitive_row_export_route(
                &effective_request,
                requested_family,
                primitive,
            );
        }
        return native_vortex_operation_blocked_route(NativeVortexOperationFamily::Sink);
    }
    if requested_family == Some(NativeVortexOperationFamily::Profile)
        || effective_request.requested_output == "profile"
    {
        return native_vortex_profile_route(&effective_request);
    }
    let Some(primitive) = primitive else {
        if effective_request.vortex_primitive.is_some() {
            return native_vortex_payload_blocked_route(
                "public_workflow_route.vortex_primitive",
                "unsupported native Vortex primitive",
                "use count, count_where, filter, project, filter_project, distinct, duplicate_mask, tail, sample, expression_project, melt, explode, pivot, rolling_window, aggregate, or sort_rows",
            );
        }
        return native_vortex_operation_blocked_route(
            requested_family.unwrap_or(NativeVortexOperationFamily::GeneralQuery),
        );
    };
    let operation_family =
        requested_family.unwrap_or_else(|| NativeVortexOperationFamily::from_primitive(primitive));
    if !operation_family.allows_primitive(primitive) {
        return native_vortex_operation_blocked_route(operation_family);
    }
    if let Some(plan) = native_vortex_primitive_payload_blocker(&effective_request, primitive) {
        return plan;
    }
    if primitive.requires_local_primitives_feature() && !cfg!(feature = "vortex-local-primitives") {
        return native_vortex_materializing_primitive_feature_gated_route();
    }
    admitted_route(
        primitive.route_id(),
        primitive.resolved_internal_command(),
        "native_vortex_file",
        "native_vortex_boundary",
        "native_vortex",
        false,
        true,
    )
}

#[allow(clippy::too_many_lines)]
fn native_vortex_primitive_payload_blocker(
    request: &PublicWorkflowRouteRequest,
    primitive: PublicVortexPrimitive,
) -> Option<PublicWorkflowRoutePlan> {
    if primitive.requires_predicate() && request.vortex_predicate.is_none() {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_predicate",
            "native Vortex primitive requires a predicate payload",
            "pass --vortex-predicate with the scoped tiny predicate expression",
        ));
    }
    if primitive.requires_columns() && request.vortex_columns.is_none() {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_columns",
            "native Vortex primitive requires a projection payload",
            "pass --vortex-columns with comma-separated projected columns",
        ));
    }
    if primitive.requires_deduplicate_key_columns()
        && request.vortex_deduplicate_key_columns.is_none()
    {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_deduplicate_key_columns",
            "native Vortex drop_duplicates requires explicit row-key columns",
            "pass --vortex-deduplicate-key-columns with comma-separated key columns",
        ));
    }
    if primitive.requires_expression_projection() && request.vortex_expression_projection.is_none()
    {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_expression_projection",
            "native Vortex expression-project primitive requires an expression projection payload",
            "pass --vortex-expression-projection with the scoped typed rewrite JSON",
        ));
    }
    if primitive.requires_melt_projection() && request.vortex_melt_projection.is_none() {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_melt_projection",
            "native Vortex melt primitive requires a melt projection payload",
            "pass --vortex-melt-projection with the scoped typed melt JSON",
        ));
    }
    if primitive.requires_explode_projection() && request.vortex_explode_projection.is_none() {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_explode_projection",
            "native Vortex explode primitive requires an explode projection payload",
            "pass --vortex-explode-projection with the scoped list explode JSON",
        ));
    }
    if primitive.requires_pivot_projection() && request.vortex_pivot_projection.is_none() {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_pivot_projection",
            "native Vortex pivot primitive requires a pivot projection payload",
            "pass --vortex-pivot-projection with the scoped wide-reshape JSON",
        ));
    }
    if primitive.requires_rolling_window() && request.vortex_rolling_window.is_none() {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_rolling_window",
            "native Vortex rolling-window primitive requires a rolling-window payload",
            "pass --vortex-rolling-window with the scoped source-order rolling JSON",
        ));
    }
    if primitive.requires_aggregate() && request.vortex_aggregate.is_none() {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_aggregate",
            "native Vortex aggregate primitive requires an aggregate payload",
            "pass --vortex-aggregate with the scoped scalar aggregate JSON",
        ));
    }
    if primitive.requires_sort_rows() && request.vortex_sort_rows.is_none() {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_sort_rows",
            "native Vortex sort-row primitive requires a sort payload",
            "pass --vortex-sort-rows with the scoped order_by JSON",
        ));
    }
    if request.vortex_source_order_limit.is_some() && !primitive.allows_source_order_limit() {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_source_order_limit",
            "native Vortex primitive does not admit a source-order limit",
            "use --vortex-source-order-limit only with filter, project, filter_project, distinct, drop_duplicates, duplicate_mask, tail, sample, expression_project, melt, explode, pivot, rolling_window, aggregate, or sort_rows",
        ));
    }
    if matches!(primitive, PublicVortexPrimitive::Tail)
        && request.vortex_source_order_limit.is_none()
    {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_source_order_limit",
            "native Vortex tail requires a bounded row count",
            "pass --vortex-source-order-limit with the requested tail row count",
        ));
    }
    if matches!(primitive, PublicVortexPrimitive::Sample)
        && request.vortex_source_order_limit.is_none()
        && request.vortex_sample_fraction.is_none()
    {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_sample_shape",
            "native Vortex sample requires a bounded row count or fraction",
            "pass --vortex-source-order-limit for sample(n=...) or --vortex-sample-fraction for sample(frac=...)",
        ));
    }
    if matches!(primitive, PublicVortexPrimitive::Sample)
        && request.vortex_source_order_limit.is_some()
        && request.vortex_sample_fraction.is_some()
    {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_sample_shape",
            "native Vortex sample accepts either row count or fraction, not both",
            "pass only one of --vortex-source-order-limit or --vortex-sample-fraction",
        ));
    }
    if request.vortex_sample_seed.is_some() && !matches!(primitive, PublicVortexPrimitive::Sample) {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_sample_seed",
            "native Vortex sample seed is only valid for sample primitives",
            "use --vortex-sample-seed only with --vortex-primitive sample",
        ));
    }
    if let Some(seed) = request.vortex_sample_seed.as_ref()
        && non_negative_u64_arg("sample seed", seed).is_err()
    {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_sample_seed",
            "native Vortex sample seed must be a non-negative integer",
            "pass --vortex-sample-seed with an unsigned integer seed",
        ));
    }
    if request.vortex_sample_fraction.is_some()
        && !matches!(primitive, PublicVortexPrimitive::Sample)
    {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_sample_fraction",
            "native Vortex sample fraction is only valid for sample primitives",
            "use --vortex-sample-fraction only with --vortex-primitive sample",
        ));
    }
    if request.vortex_sample_replacement && !matches!(primitive, PublicVortexPrimitive::Sample) {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_sample_replacement",
            "native Vortex sample replacement is only valid for sample primitives",
            "use --vortex-sample-replacement only with --vortex-primitive sample",
        ));
    }
    if request.vortex_sample_weight_column.is_some()
        && !matches!(primitive, PublicVortexPrimitive::Sample)
    {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_sample_weight_column",
            "native Vortex sample weights are only valid for sample primitives",
            "use --vortex-sample-weight-column only with --vortex-primitive sample",
        ));
    }
    if let Some(column) = request.vortex_sample_weight_column.as_ref()
        && shardloom_core::ColumnRef::new(column).is_err()
    {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_sample_weight_column",
            "native Vortex sample weight column must be a valid column name",
            "pass --vortex-sample-weight-column with one scoped numeric column name",
        ));
    }
    if request.vortex_duplicate_keep.is_some()
        && !matches!(
            primitive,
            PublicVortexPrimitive::DuplicateMask | PublicVortexPrimitive::DropDuplicates
        )
    {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_duplicate_keep",
            "native Vortex duplicate keep policy is only valid for duplicate-mask or drop-duplicates primitives",
            "use --vortex-duplicate-keep only with --vortex-primitive duplicate_mask or drop_duplicates",
        ));
    }
    if request.vortex_deduplicate_key_columns.is_some()
        && !matches!(primitive, PublicVortexPrimitive::DropDuplicates)
    {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_deduplicate_key_columns",
            "native Vortex deduplicate key columns are only valid for drop-duplicates primitives",
            "use --vortex-deduplicate-key-columns only with --vortex-primitive drop_duplicates",
        ));
    }
    if let Some(columns) = request.vortex_deduplicate_key_columns.as_ref()
        && vortex_primitive_execution::parse_projection_columns(columns).is_err()
    {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_deduplicate_key_columns",
            "native Vortex drop_duplicates key columns must be a valid projection list",
            "pass --vortex-deduplicate-key-columns with comma-separated scoped column names",
        ));
    }
    if request.vortex_expression_projection.is_some()
        && !matches!(primitive, PublicVortexPrimitive::ExpressionProject)
    {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_expression_projection",
            "native Vortex expression projection is only valid for expression-project primitives",
            "use --vortex-expression-projection only with --vortex-primitive expression_project",
        ));
    }
    if request.vortex_melt_projection.is_some() && !matches!(primitive, PublicVortexPrimitive::Melt)
    {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_melt_projection",
            "native Vortex melt projection is only valid for melt primitives",
            "use --vortex-melt-projection only with --vortex-primitive melt",
        ));
    }
    if request.vortex_explode_projection.is_some()
        && !matches!(primitive, PublicVortexPrimitive::Explode)
    {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_explode_projection",
            "native Vortex explode projection is only valid for explode primitives",
            "use --vortex-explode-projection only with --vortex-primitive explode",
        ));
    }
    if request.vortex_pivot_projection.is_some()
        && !matches!(primitive, PublicVortexPrimitive::Pivot)
    {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_pivot_projection",
            "native Vortex pivot projection is only valid for pivot primitives",
            "use --vortex-pivot-projection only with --vortex-primitive pivot",
        ));
    }
    if request.vortex_rolling_window.is_some()
        && !matches!(primitive, PublicVortexPrimitive::RollingWindow)
    {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_rolling_window",
            "native Vortex rolling-window payload is only valid for rolling-window primitives",
            "use --vortex-rolling-window only with --vortex-primitive rolling_window",
        ));
    }
    if request.vortex_aggregate.is_some() && !matches!(primitive, PublicVortexPrimitive::Aggregate)
    {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_aggregate",
            "native Vortex aggregate payload is only valid for aggregate primitives",
            "use --vortex-aggregate only with --vortex-primitive aggregate",
        ));
    }
    if request.vortex_sort_rows.is_some() && !matches!(primitive, PublicVortexPrimitive::SortRows) {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_sort_rows",
            "native Vortex sort-row payload is only valid for sort-row primitives",
            "use --vortex-sort-rows only with --vortex-primitive sort_rows",
        ));
    }
    if matches!(primitive, PublicVortexPrimitive::SortRows)
        && request.vortex_source_order_limit.is_none()
    {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_source_order_limit",
            "native Vortex sort rows require a bounded row count",
            "pass --vortex-source-order-limit with the requested top row count",
        ));
    }
    if let Some(fraction) = request.vortex_sample_fraction.as_ref()
        && sample_fraction_arg("sample fraction", fraction).is_err()
    {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_sample_fraction",
            "native Vortex sample fraction must be finite and in the range (0, 1]",
            "pass --vortex-sample-fraction with a decimal value greater than 0 and no greater than 1",
        ));
    }
    native_vortex_resource_hint_blocker(request)
}

fn native_vortex_primitive_row_export_route(
    request: &PublicWorkflowRouteRequest,
    requested_family: Option<NativeVortexOperationFamily>,
    primitive: PublicVortexPrimitive,
) -> PublicWorkflowRoutePlan {
    if !cfg!(feature = "vortex-local-primitives") {
        return native_vortex_primitive_row_export_feature_gated_route();
    }
    if !matches!(
        request.requested_output.as_str(),
        "write_vortex"
            | "write_json"
            | "write_jsonl"
            | "write_csv"
            | "write_parquet"
            | "write_arrow_ipc"
            | "write_avro"
            | "write_orc"
    ) {
        return native_vortex_sink_format_blocked_route(request);
    }
    if native_vortex_row_export_requires_structured_payload(request)
        && !(matches!(primitive, PublicVortexPrimitive::ExpressionProject)
            && request
                .vortex_expression_projection
                .as_deref()
                .is_some_and(native_vortex_expression_projection_is_structured))
    {
        return native_vortex_structured_sink_payload_blocked_route(request);
    }
    if !matches!(
        primitive,
        PublicVortexPrimitive::Count
            | PublicVortexPrimitive::CountWhere
            | PublicVortexPrimitive::Filter
            | PublicVortexPrimitive::Project
            | PublicVortexPrimitive::FilterProject
            | PublicVortexPrimitive::Distinct
            | PublicVortexPrimitive::DropDuplicates
            | PublicVortexPrimitive::DuplicateMask
            | PublicVortexPrimitive::Tail
            | PublicVortexPrimitive::Sample
            | PublicVortexPrimitive::ExpressionProject
            | PublicVortexPrimitive::Melt
            | PublicVortexPrimitive::Explode
            | PublicVortexPrimitive::Pivot
            | PublicVortexPrimitive::RollingWindow
            | PublicVortexPrimitive::Aggregate
            | PublicVortexPrimitive::SortRows
    ) {
        return native_vortex_operation_blocked_route(NativeVortexOperationFamily::Sink);
    }
    if let Err(blocked) = native_vortex_primitive_row_export_targets(request, "route") {
        return *blocked;
    }
    let operation_family =
        requested_family.unwrap_or_else(|| NativeVortexOperationFamily::from_primitive(primitive));
    if !matches!(operation_family, NativeVortexOperationFamily::Sink)
        && !operation_family.allows_primitive(primitive)
    {
        return native_vortex_operation_blocked_route(operation_family);
    }
    if let Some(plan) = native_vortex_primitive_row_export_payload_blocker(request, primitive) {
        return plan;
    }
    admitted_route(
        "native_vortex_primitive_row_export",
        "vortex-local-primitive-row-export",
        "native_vortex_file",
        "native_vortex_primitive_row_export",
        "native_vortex",
        false,
        true,
    )
}

#[allow(clippy::too_many_lines)]
fn native_vortex_primitive_row_export_payload_blocker(
    request: &PublicWorkflowRouteRequest,
    primitive: PublicVortexPrimitive,
) -> Option<PublicWorkflowRoutePlan> {
    if primitive.requires_predicate() && request.vortex_predicate.is_none() {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_predicate",
            "native Vortex primitive row export requires a predicate payload",
            "pass --vortex-predicate with the scoped tiny predicate expression",
        ));
    }
    if primitive.requires_columns() && request.vortex_columns.is_none() {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_columns",
            "native Vortex primitive row export requires a projection payload",
            "pass --vortex-columns with comma-separated projected columns",
        ));
    }
    if primitive.requires_expression_projection() && request.vortex_expression_projection.is_none()
    {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_expression_projection",
            "native Vortex expression-project row export requires an expression projection payload",
            "pass --vortex-expression-projection with the scoped typed rewrite JSON",
        ));
    }
    if primitive.requires_melt_projection() && request.vortex_melt_projection.is_none() {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_melt_projection",
            "native Vortex melt row export requires a melt projection payload",
            "pass --vortex-melt-projection with the scoped typed melt JSON",
        ));
    }
    if primitive.requires_explode_projection() && request.vortex_explode_projection.is_none() {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_explode_projection",
            "native Vortex explode row export requires an explode projection payload",
            "pass --vortex-explode-projection with the scoped list explode JSON",
        ));
    }
    if primitive.requires_pivot_projection() && request.vortex_pivot_projection.is_none() {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_pivot_projection",
            "native Vortex pivot row export requires a pivot projection payload",
            "pass --vortex-pivot-projection with the scoped wide-reshape JSON",
        ));
    }
    if primitive.requires_rolling_window() && request.vortex_rolling_window.is_none() {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_rolling_window",
            "native Vortex rolling-window row export requires a rolling-window payload",
            "pass --vortex-rolling-window with the scoped source-order rolling JSON",
        ));
    }
    if primitive.requires_aggregate() && request.vortex_aggregate.is_none() {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_aggregate",
            "native Vortex aggregate row export requires an aggregate payload",
            "pass --vortex-aggregate with the scoped scalar aggregate JSON",
        ));
    }
    if primitive.requires_sort_rows() && request.vortex_sort_rows.is_none() {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_sort_rows",
            "native Vortex sort-row export requires a sort payload",
            "pass --vortex-sort-rows with the scoped order_by JSON",
        ));
    }
    if matches!(primitive, PublicVortexPrimitive::Tail)
        && request.vortex_source_order_limit.is_none()
    {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_source_order_limit",
            "native Vortex primitive row export requires a bounded tail row count",
            "pass --vortex-source-order-limit with the requested tail row count",
        ));
    }
    if matches!(primitive, PublicVortexPrimitive::Sample)
        && request.vortex_source_order_limit.is_none()
        && request.vortex_sample_fraction.is_none()
    {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_sample_shape",
            "native Vortex sample row export requires a row count or fraction",
            "pass --vortex-source-order-limit for sample(n=...) or --vortex-sample-fraction for sample(frac=...)",
        ));
    }
    if matches!(primitive, PublicVortexPrimitive::Sample)
        && request.vortex_source_order_limit.is_some()
        && request.vortex_sample_fraction.is_some()
    {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_sample_shape",
            "native Vortex sample row export accepts either row count or fraction, not both",
            "pass only one of --vortex-source-order-limit or --vortex-sample-fraction",
        ));
    }
    if request.vortex_sample_seed.is_some() && !matches!(primitive, PublicVortexPrimitive::Sample) {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_sample_seed",
            "native Vortex sample seed is only valid for sample row export",
            "use --vortex-sample-seed only with --vortex-primitive sample",
        ));
    }
    if let Some(seed) = request.vortex_sample_seed.as_ref()
        && non_negative_u64_arg("sample seed", seed).is_err()
    {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_sample_seed",
            "native Vortex sample seed must be a non-negative integer",
            "pass --vortex-sample-seed with an unsigned integer seed",
        ));
    }
    if request.vortex_sample_fraction.is_some()
        && !matches!(primitive, PublicVortexPrimitive::Sample)
    {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_sample_fraction",
            "native Vortex sample fraction is only valid for sample row export",
            "use --vortex-sample-fraction only with --vortex-primitive sample",
        ));
    }
    if request.vortex_sample_replacement && !matches!(primitive, PublicVortexPrimitive::Sample) {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_sample_replacement",
            "native Vortex sample replacement is only valid for sample row export",
            "use --vortex-sample-replacement only with --vortex-primitive sample",
        ));
    }
    if request.vortex_sample_weight_column.is_some()
        && !matches!(primitive, PublicVortexPrimitive::Sample)
    {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_sample_weight_column",
            "native Vortex sample weights are only valid for sample row export",
            "use --vortex-sample-weight-column only with --vortex-primitive sample",
        ));
    }
    if let Some(column) = request.vortex_sample_weight_column.as_ref()
        && shardloom_core::ColumnRef::new(column).is_err()
    {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_sample_weight_column",
            "native Vortex sample weight column must be a valid column name",
            "pass --vortex-sample-weight-column with one scoped numeric column name",
        ));
    }
    if request.vortex_duplicate_keep.is_some()
        && !matches!(
            primitive,
            PublicVortexPrimitive::DuplicateMask | PublicVortexPrimitive::DropDuplicates
        )
    {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_duplicate_keep",
            "native Vortex duplicate keep policy is only valid for duplicate-mask or drop-duplicates row export",
            "use --vortex-duplicate-keep only with --vortex-primitive duplicate_mask or drop_duplicates",
        ));
    }
    if request.vortex_deduplicate_key_columns.is_some()
        && !matches!(primitive, PublicVortexPrimitive::DropDuplicates)
    {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_deduplicate_key_columns",
            "native Vortex deduplicate key columns are only valid for drop-duplicates row export",
            "use --vortex-deduplicate-key-columns only with --vortex-primitive drop_duplicates",
        ));
    }
    if matches!(primitive, PublicVortexPrimitive::DropDuplicates)
        && request.vortex_deduplicate_key_columns.is_none()
    {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_deduplicate_key_columns",
            "native Vortex drop_duplicates row export requires explicit row-key columns",
            "pass --vortex-deduplicate-key-columns with comma-separated key columns",
        ));
    }
    if let Some(columns) = request.vortex_deduplicate_key_columns.as_ref()
        && vortex_primitive_execution::parse_projection_columns(columns).is_err()
    {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_deduplicate_key_columns",
            "native Vortex drop_duplicates key columns must be a valid projection list",
            "pass --vortex-deduplicate-key-columns with comma-separated scoped column names",
        ));
    }
    if request.vortex_expression_projection.is_some()
        && !matches!(primitive, PublicVortexPrimitive::ExpressionProject)
    {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_expression_projection",
            "native Vortex expression projection is only valid for expression-project row export",
            "use --vortex-expression-projection only with --vortex-primitive expression_project",
        ));
    }
    if request.vortex_melt_projection.is_some() && !matches!(primitive, PublicVortexPrimitive::Melt)
    {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_melt_projection",
            "native Vortex melt projection is only valid for melt row export",
            "use --vortex-melt-projection only with --vortex-primitive melt",
        ));
    }
    if request.vortex_explode_projection.is_some()
        && !matches!(primitive, PublicVortexPrimitive::Explode)
    {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_explode_projection",
            "native Vortex explode projection is only valid for explode row export",
            "use --vortex-explode-projection only with --vortex-primitive explode",
        ));
    }
    if request.vortex_pivot_projection.is_some()
        && !matches!(primitive, PublicVortexPrimitive::Pivot)
    {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_pivot_projection",
            "native Vortex pivot projection is only valid for pivot row export",
            "use --vortex-pivot-projection only with --vortex-primitive pivot",
        ));
    }
    if request.vortex_rolling_window.is_some()
        && !matches!(primitive, PublicVortexPrimitive::RollingWindow)
    {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_rolling_window",
            "native Vortex rolling-window payload is only valid for rolling-window row export",
            "use --vortex-rolling-window only with --vortex-primitive rolling_window",
        ));
    }
    if request.vortex_aggregate.is_some() && !matches!(primitive, PublicVortexPrimitive::Aggregate)
    {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_aggregate",
            "native Vortex aggregate payload is only valid for aggregate primitives",
            "use --vortex-aggregate only with --vortex-primitive aggregate",
        ));
    }
    if request.vortex_sort_rows.is_some() && !matches!(primitive, PublicVortexPrimitive::SortRows) {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_sort_rows",
            "native Vortex sort-row payload is only valid for sort-row export",
            "use --vortex-sort-rows only with --vortex-primitive sort_rows",
        ));
    }
    if matches!(primitive, PublicVortexPrimitive::SortRows)
        && request.vortex_source_order_limit.is_none()
    {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_source_order_limit",
            "native Vortex sort-row export requires a bounded row count",
            "pass --vortex-source-order-limit with the requested top row count",
        ));
    }
    if let Some(fraction) = request.vortex_sample_fraction.as_ref()
        && sample_fraction_arg("sample fraction", fraction).is_err()
    {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_sample_fraction",
            "native Vortex sample fraction must be finite and in the range (0, 1]",
            "pass --vortex-sample-fraction with a decimal value greater than 0 and no greater than 1",
        ));
    }
    native_vortex_resource_hint_blocker(request)
}

fn native_vortex_primitive_row_export_feature_gated_route() -> PublicWorkflowRoutePlan {
    blocked_route(
        "py-vortex-route-unify-1.native_vortex_primitive_row_export_feature_gated",
        "native Vortex primitive row export requires the vortex-local-primitives feature",
        Diagnostic::new(
            DiagnosticCode::NotImplemented,
            DiagnosticSeverity::Error,
            DiagnosticCategory::UnsupportedFeature,
            "native Vortex primitive row export is feature-gated in this binary".to_string(),
            Some("public_workflow_route.vortex_primitive".to_string()),
            Some("compiled_without=vortex-local-primitives".to_string()),
            Some(
                "build with --features release-user-surfaces for admitted native and compatibility sinks, or vortex-local-primitives for scoped JSON/JSONL/CSV row exports".to_string(),
            ),
            FallbackStatus::disabled_by_policy(),
        ),
    )
}

fn native_vortex_materializing_primitive_feature_gated_route() -> PublicWorkflowRoutePlan {
    blocked_route(
        "py-vortex-route-unify-1.native_vortex_materializing_primitive_feature_gated",
        "native Vortex materializing primitive collect requires the vortex-local-primitives feature",
        Diagnostic::new(
            DiagnosticCode::NotImplemented,
            DiagnosticSeverity::Error,
            DiagnosticCategory::UnsupportedFeature,
                "native Vortex distinct/tail/sample/expression-project/melt/explode/rolling-window/aggregate/sort-row collect is feature-gated in this binary"
                .to_string(),
            Some("public_workflow_route.vortex_primitive".to_string()),
            Some("compiled_without=vortex-local-primitives".to_string()),
            Some(
                "build the release binary with --features release-user-surfaces or vortex-local-primitives to execute materializing native Vortex primitive collect routes".to_string(),
            ),
            FallbackStatus::disabled_by_policy(),
        ),
    )
}

fn native_vortex_profile_route(request: &PublicWorkflowRouteRequest) -> PublicWorkflowRoutePlan {
    if !native_vortex_metadata_profile_shape_admitted(request) {
        return native_vortex_operation_blocked_route(NativeVortexOperationFamily::Profile);
    }
    admitted_route(
        "native_vortex_user_profile",
        "vortex-metadata-summary",
        "native_vortex_file",
        "native_vortex_metadata_profile",
        "native_vortex",
        false,
        true,
    )
}

fn native_vortex_metadata_profile_shape_admitted(request: &PublicWorkflowRouteRequest) -> bool {
    if request.requested_output != "profile" {
        return false;
    }
    let Some(summary) = request.plan_summary.as_deref() else {
        return true;
    };
    let Some(operations) = parse_plan_summary_operations(summary) else {
        return false;
    };
    let operations = strip_index_metadata_operations(&operations);
    if !summary_read_vortex_matches_input(&operations, request.input_uri.as_deref().unwrap_or("")) {
        return false;
    }
    operations
        .iter()
        .skip(1)
        .all(|operation| matches!(operation.kind, "select" | "limit"))
}

fn native_vortex_profile_projection_attachment_fields(
    request: &PublicWorkflowRouteRequest,
) -> Vec<(String, String)> {
    let projected_columns = request
        .plan_summary
        .as_deref()
        .and_then(|summary| profile_projection_columns_from_summary(summary, request));
    let (scope, columns) = match projected_columns {
        Some(columns) => ("selected_columns", columns),
        None => ("all_columns", "all".to_string()),
    };
    vec![
        (
            "public_workflow_profile_projection_scope".to_string(),
            scope.to_string(),
        ),
        (
            "public_workflow_profile_projected_columns".to_string(),
            columns,
        ),
        (
            "metadata_summary_projection_scope".to_string(),
            scope.to_string(),
        ),
    ]
}

fn profile_projection_columns_from_summary(
    summary: &str,
    request: &PublicWorkflowRouteRequest,
) -> Option<String> {
    let operations = parse_plan_summary_operations(summary)?;
    let operations = strip_index_metadata_operations(&operations);
    if !summary_read_vortex_matches_input(&operations, request.input_uri.as_deref()?) {
        return None;
    }
    let columns = operations
        .iter()
        .rev()
        .find(|operation| operation.kind == "select")?
        .arg
        .trim();
    if columns.is_empty() || columns == "*" {
        return None;
    }
    normalize_sql_projection_columns(columns)
}

fn native_vortex_resource_hint_blocker(
    request: &PublicWorkflowRouteRequest,
) -> Option<PublicWorkflowRoutePlan> {
    if let Err(error) = request.resources.optional() {
        return Some(blocked_route(
            "cg21.route.execution_resources_invalid",
            "the execution resource declaration is invalid",
            error.to_diagnostic(),
        ));
    }
    if let Some(error) = positive_usize_option_error(
        "--vortex-source-order-limit",
        request.vortex_source_order_limit.as_deref(),
    ) {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_source_order_limit",
            error,
            "pass --vortex-source-order-limit with an integer >= 1",
        ));
    }
    None
}

fn native_vortex_input_required_route() -> PublicWorkflowRoutePlan {
    blocked_route(
        "cg21.route.native_vortex_input_required",
        "native Vortex execution policy requires declared Vortex input",
        Diagnostic::new(
            DiagnosticCode::InvalidInput,
            DiagnosticSeverity::Error,
            DiagnosticCategory::InvalidInput,
            "native Vortex public routes require input_format=vortex".to_string(),
            Some("public_workflow_route.input_format".to_string()),
            Some("execution_policy=native_vortex was requested for a non-Vortex input".to_string()),
            Some("prepare compatibility input into Vortex first, or pass input_format=vortex with an admitted native Vortex route".to_string()),
            FallbackStatus::disabled_by_policy(),
        ),
    )
}

fn native_vortex_artifact_prepare_route(
    request: &PublicWorkflowRouteRequest,
) -> PublicWorkflowRoutePlan {
    if !shardloom_vortex::vortex_ingest_write_feature_enabled() {
        return local_file_vortex_ingest_feature_gated_route(request);
    }
    admitted_route(
        "native_vortex_artifact_prepare",
        "vortex-prepare",
        "native_vortex_file",
        "VortexPreparedState",
        "prepared_vortex",
        true,
        true,
    )
}

fn native_vortex_operation_blocked_route(
    family: NativeVortexOperationFamily,
) -> PublicWorkflowRoutePlan {
    blocked_route(
        family.blocker_id(),
        family.blocker_reason(),
        Diagnostic::new(
            DiagnosticCode::NotImplemented,
            DiagnosticSeverity::Error,
            DiagnosticCategory::UnsupportedFeature,
            family.blocker_reason().to_string(),
            Some("public_workflow_route.native_vortex_operation_family".to_string()),
            Some(format!(
                "operation_family={} required_evidence={}",
                family.as_str(),
                family.required_evidence()
            )),
            Some(family.next_action().to_string()),
            FallbackStatus::disabled_by_policy(),
        ),
    )
}

fn normalized_native_vortex_operation_family(
    request: &PublicWorkflowRouteRequest,
) -> Result<Option<NativeVortexOperationFamily>, ()> {
    match request.native_vortex_operation_family.as_deref() {
        Some(value) => NativeVortexOperationFamily::parse(value)
            .map(Some)
            .ok_or(()),
        None => Ok(None),
    }
}

fn normalized_vortex_primitive(
    request: &PublicWorkflowRouteRequest,
) -> Option<PublicVortexPrimitive> {
    request
        .vortex_primitive
        .as_deref()
        .and_then(PublicVortexPrimitive::parse)
}

#[derive(Debug, Clone)]
struct InferredNativeVortexRoutePayload {
    family: NativeVortexOperationFamily,
    primitive: Option<PublicVortexPrimitive>,
    predicate: Option<String>,
    columns: Option<String>,
    source_order_limit: Option<String>,
    sample_seed: Option<String>,
    sample_fraction: Option<String>,
    sample_replacement: bool,
    sample_weight_column: Option<String>,
    duplicate_keep: Option<String>,
    deduplicate_key_columns: Option<String>,
    expression_projection: Option<String>,
    explode_projection: Option<String>,
    pivot_projection: Option<String>,
    rolling_window: Option<String>,
    aggregate: Option<String>,
    sort_rows: Option<String>,
    right_input: Option<String>,
}

impl InferredNativeVortexRoutePayload {
    fn apply(self, request: &mut PublicWorkflowRouteRequest) {
        if request.native_vortex_operation_family.is_none() {
            request.native_vortex_operation_family = Some(self.family.as_str().to_string());
        }
        if request.vortex_primitive.is_none() {
            request.vortex_primitive = self
                .primitive
                .map(|primitive| primitive.as_str().to_string());
        }
        if request.vortex_predicate.is_none() {
            request.vortex_predicate = self.predicate;
        }
        if request.vortex_columns.is_none() {
            request.vortex_columns = self.columns;
        }
        if request.vortex_source_order_limit.is_none() {
            request.vortex_source_order_limit = self.source_order_limit;
        }
        if request.vortex_sample_seed.is_none() {
            request.vortex_sample_seed = self.sample_seed;
        }
        if request.vortex_sample_fraction.is_none() {
            request.vortex_sample_fraction = self.sample_fraction;
        }
        request.vortex_sample_replacement |= self.sample_replacement;
        if request.vortex_sample_weight_column.is_none() {
            request.vortex_sample_weight_column = self.sample_weight_column;
        }
        if request.vortex_duplicate_keep.is_none() {
            request.vortex_duplicate_keep = self.duplicate_keep;
        }
        if request.vortex_deduplicate_key_columns.is_none() {
            request.vortex_deduplicate_key_columns = self.deduplicate_key_columns;
        }
        if request.vortex_expression_projection.is_none() {
            request.vortex_expression_projection = self.expression_projection;
        }
        if request.vortex_explode_projection.is_none() {
            request.vortex_explode_projection = self.explode_projection;
        }
        if request.vortex_pivot_projection.is_none() {
            request.vortex_pivot_projection = self.pivot_projection;
        }
        if request.vortex_rolling_window.is_none() {
            request.vortex_rolling_window = self.rolling_window;
        }
        if request.vortex_aggregate.is_none() {
            request.vortex_aggregate = self.aggregate;
        }
        if request.vortex_sort_rows.is_none() {
            request.vortex_sort_rows = self.sort_rows;
        }
        if request.native_vortex_right_input.is_none() {
            request.native_vortex_right_input = self.right_input;
        }
    }
}

fn infer_native_vortex_route_payload(
    request: &PublicWorkflowRouteRequest,
) -> Option<InferredNativeVortexRoutePayload> {
    if request.vortex_primitive.is_some() {
        return None;
    }
    if request.surface == "sql" || request.sql_statement.is_some() {
        return infer_native_vortex_sql_payload(request);
    }
    if request.input_format.as_deref() != Some("vortex") {
        if !request
            .input_format
            .as_deref()
            .is_some_and(is_local_file_format)
        {
            return None;
        }
        // Interpret the same inert operation summary against the original URI.
        // This adapts its source label only; it does not prepare or probe files.
        let mut native_summary = request.clone();
        native_summary.plan_summary = Some(prepared_local_workflow_plan_summary(
            request,
            Path::new(request.input_uri.as_deref()?),
            None,
        )?);
        return infer_native_vortex_primitive_payload(&native_summary);
    }
    infer_native_vortex_primitive_payload(request)
}

/// Syntactic strategy admission only; no source/cache probing during routing.
#[cfg(all(feature = "vortex-local-primitives", unix))]
fn uses_source_demand(request: &PublicWorkflowRouteRequest) -> bool {
    // Complete file SQL and direct compatibility declarations enter the same
    // query compiler. Explicit low-level primitive commands remain kernel APIs.
    if request.sql_statement.is_some()
        && request.native_operator_options == NativeOperatorOptions::Automatic
        && request.input_format.as_deref() == Some("vortex")
        && request.execution_policy != "direct"
        && request.requested_output != "prepare"
    {
        return true;
    }
    cfg!(all(
        feature = "vortex-write",
        feature = "universal-format-io"
    )) && request.execution_policy == "vortex_middle"
        && request.requested_output != "prepare"
        && (request.sql_statement.is_some() || normalized_vortex_primitive(request).is_some())
        && (request
            .input_format
            .as_deref()
            .is_some_and(demand_source_format)
            || request
                .source_bindings
                .values()
                .any(|binding| demand_source_format(&binding.input_format)))
}

#[cfg(all(feature = "vortex-local-primitives", unix))]
fn uses_primitive_source_demand(request: &PublicWorkflowRouteRequest) -> bool {
    normalized_vortex_primitive(request).is_some()
        && (request.native_operator_options == NativeOperatorOptions::Explicit
            || request.sql_statement.is_none())
}

#[cfg(all(feature = "vortex-local-primitives", unix))]
fn demand_source_format(format: &str) -> bool {
    matches!(
        format,
        "parquet" | "arrow-ipc" | "avro" | "orc" | "csv" | "tsv" | "json" | "jsonl" | "ndjson"
    )
}

fn effective_public_workflow_request(
    request: &PublicWorkflowRouteRequest,
) -> PublicWorkflowRouteRequest {
    let mut effective_request = request.clone();
    if let Some(payload) = infer_native_vortex_route_payload(&effective_request) {
        payload.apply(&mut effective_request);
    }
    effective_request
}

fn infer_native_vortex_sql_payload(
    request: &PublicWorkflowRouteRequest,
) -> Option<InferredNativeVortexRoutePayload> {
    let statement = request.sql_statement.as_deref()?;
    if native_relational_sql_candidate(statement) {
        return None;
    }
    let shape = parse_native_vortex_sql_single_source_shape(statement);
    // An empty SQL result needs the native relational limit operator. Metadata
    // count and positive-limit primitives must not omit LIMIT 0.
    if shape
        .as_ref()
        .and_then(|shape| shape.limit.as_deref())
        .is_some_and(|limit| limit.parse::<usize>() == Ok(0))
    {
        return None;
    }
    shape.and_then(|shape| infer_native_vortex_sql_primitive_payload(shape, request))
}

/// Lexical dispatch only; native parsing still admits the complete statement.
/// Quoted paths and string values cannot select an execution family.
fn native_relational_sql_candidate(statement: &str) -> bool {
    let masked = sql_without_string_literal_contents(statement);
    let mut selects = 0;
    for token in masked.split(|ch: char| !(ch.is_ascii_alphanumeric() || ch == '_')) {
        if token.eq_ignore_ascii_case("select") {
            selects += 1;
        }
        if ["join", "union", "intersect", "except", "over"]
            .iter()
            .any(|keyword| token.eq_ignore_ascii_case(keyword))
        {
            return true;
        }
    }
    selects > 1
}

#[derive(Debug, Clone)]
struct NativeVortexSqlSingleSourceShape {
    projection: String,
    source_ref: String,
    source_is_identifier: bool,
    where_clause: Option<String>,
    group_by: Option<String>,
    having: Option<String>,
    order_by: Option<String>,
    limit: Option<String>,
    offset: Option<String>,
}

#[allow(clippy::too_many_lines)]
fn infer_native_vortex_sql_primitive_payload(
    shape: NativeVortexSqlSingleSourceShape,
    request: &PublicWorkflowRouteRequest,
) -> Option<InferredNativeVortexRoutePayload> {
    if !matches!(
        request.requested_output.as_str(),
        "collect"
            | "write_vortex"
            | "write_json"
            | "write_jsonl"
            | "write_csv"
            | "write_parquet"
            | "write_arrow_ipc"
            | "write_avro"
            | "write_orc"
    ) {
        return None;
    }
    if !native_vortex_sql_source_ref_matches_request(
        &shape.source_ref,
        shape.source_is_identifier,
        request,
    ) {
        return None;
    }
    let projection = compact_ascii_lower(&shape.projection);
    // Every result boundary consumes the same metadata/encoded count producer.
    if projection == "count(*)"
        && shape.group_by.is_none()
        && shape.having.is_none()
        && shape.order_by.is_none()
        && shape.offset.is_none()
    {
        let predicate = match shape.where_clause.as_deref() {
            Some(where_clause) => Some(summary_tiny_predicate_from_sql(where_clause)?),
            None => None,
        };
        return Some(InferredNativeVortexRoutePayload {
            family: NativeVortexOperationFamily::Count,
            primitive: Some(if predicate.is_some() {
                PublicVortexPrimitive::CountWhere
            } else {
                PublicVortexPrimitive::Count
            }),
            predicate,
            columns: None,
            source_order_limit: None,
            sample_seed: None,
            sample_fraction: None,
            sample_replacement: false,
            sample_weight_column: None,
            duplicate_keep: None,

            deduplicate_key_columns: None,
            expression_projection: None,
            explode_projection: None,
            pivot_projection: None,
            rolling_window: None,
            aggregate: None,
            sort_rows: None,
            right_input: None,
        });
    }
    if let Some(aggregate_payload) = aggregate_payload_from_sql_projection(
        &shape.projection,
        shape.group_by.as_deref(),
        shape.having.as_deref(),
        shape.order_by.as_deref(),
        shape.offset.as_deref(),
    ) {
        let predicate = match shape.where_clause.as_deref() {
            Some(where_clause) => Some(summary_tiny_predicate_from_sql(where_clause)?),
            None => None,
        };
        return Some(InferredNativeVortexRoutePayload {
            family: NativeVortexOperationFamily::Aggregate,
            primitive: Some(PublicVortexPrimitive::Aggregate),
            predicate,
            columns: None,
            source_order_limit: shape.limit,
            sample_seed: None,
            sample_fraction: None,
            sample_replacement: false,
            sample_weight_column: None,
            duplicate_keep: None,

            deduplicate_key_columns: None,
            expression_projection: None,
            explode_projection: None,
            pivot_projection: None,
            rolling_window: None,
            aggregate: Some(aggregate_payload),
            sort_rows: None,
            right_input: None,
        });
    }
    if shape.group_by.is_none()
        && shape.having.is_none()
        && let (Some(order_by), Some(limit)) = (shape.order_by.as_deref(), shape.limit.as_deref())
    {
        let columns = if shape.projection.trim() == "*" {
            None
        } else {
            Some(normalize_sql_projection_columns(&shape.projection)?)
        };
        let predicate = match shape.where_clause.as_deref() {
            Some(where_clause) => Some(summary_tiny_predicate_from_sql(where_clause)?),
            None => None,
        };
        return Some(InferredNativeVortexRoutePayload {
            family: NativeVortexOperationFamily::TopN,
            primitive: Some(PublicVortexPrimitive::SortRows),
            predicate,
            columns,
            source_order_limit: Some(limit.to_string()),
            sample_seed: None,
            sample_fraction: None,
            sample_replacement: false,
            sample_weight_column: None,
            duplicate_keep: None,
            deduplicate_key_columns: None,
            expression_projection: None,
            explode_projection: None,
            pivot_projection: None,
            rolling_window: None,
            aggregate: None,
            sort_rows: Some(sort_rows_payload_from_sql_order_by(
                order_by,
                shape.offset.as_deref(),
                limit,
            )?),
            right_input: None,
        });
    }
    if shape.group_by.is_some()
        || shape.having.is_some()
        || shape.order_by.is_some()
        || shape.offset.is_some()
    {
        return None;
    }
    // A bare star is the existing native all-column projection. Keep filtered
    // stars on their established filter route and lower an unfiltered star
    // directly instead of rejecting it before reaching the shared writer.
    let columns = normalize_sql_projection_columns(&shape.projection).or_else(|| {
        (shape.projection.trim() == "*" && shape.where_clause.is_none()).then(|| "*".to_string())
    });
    // Only SELECT * may omit a projection in the filter primitive. A scalar
    // expression still needs evaluation after the predicate has selected rows.
    if columns.is_none() && shape.projection.trim() != "*" {
        return None;
    }
    let predicate = match shape.where_clause.as_deref() {
        Some(where_clause) => Some(summary_tiny_predicate_from_sql(where_clause)?),
        None => None,
    };
    let primitive = match (predicate.is_some(), columns.is_some()) {
        (true, true) => PublicVortexPrimitive::FilterProject,
        (true, false) => PublicVortexPrimitive::Filter,
        (false, true) => PublicVortexPrimitive::Project,
        (false, false) => return None,
    };
    Some(InferredNativeVortexRoutePayload {
        family: NativeVortexOperationFamily::from_primitive(primitive),
        primitive: Some(primitive),
        predicate,
        columns,
        source_order_limit: shape.limit,
        sample_seed: None,
        sample_fraction: None,
        sample_replacement: false,
        sample_weight_column: None,
        duplicate_keep: None,

        deduplicate_key_columns: None,
        expression_projection: None,
        explode_projection: None,
        pivot_projection: None,
        rolling_window: None,
        aggregate: None,
        sort_rows: None,
        right_input: None,
    })
}

fn parse_native_vortex_sql_single_source_shape(
    statement: &str,
) -> Option<NativeVortexSqlSingleSourceShape> {
    let normalized = statement.trim().trim_end_matches(';').trim();
    if !sql_keyword_prefix(normalized, "SELECT") {
        return None;
    }
    let select_body = normalized["SELECT".len()..].trim();
    let mut start = 0;
    let from_position = loop {
        let position =
            start + find_sql_keyword_outside_quotes_and_parens(&select_body[start..], "FROM")?;
        let mut preceding = select_body[..position].split_whitespace().rev();
        let distinct = preceding
            .next()
            .is_some_and(|word| word.eq_ignore_ascii_case("distinct"));
        let modifier = preceding.next();
        let null_safe = distinct
            && (modifier.is_some_and(|word| word.eq_ignore_ascii_case("is"))
                || (modifier.is_some_and(|word| word.eq_ignore_ascii_case("not"))
                    && preceding
                        .next()
                        .is_some_and(|word| word.eq_ignore_ascii_case("is"))));
        if !null_safe {
            break position;
        }
        start = position + "FROM".len();
    };
    let projection = select_body[..from_position].trim();
    let from_tail = select_body[from_position + "FROM".len()..].trim();
    let (source_ref, consumed) = leading_sql_source_ref_with_consumed(from_tail)?;
    let tail = from_tail[consumed..].trim();
    let tail = parse_native_vortex_sql_allowed_tail(tail)?;
    Some(NativeVortexSqlSingleSourceShape {
        projection: projection.to_string(),
        source_ref,
        source_is_identifier: !from_tail.starts_with(['\'', '"']),
        where_clause: tail.where_clause,
        group_by: tail.group_by,
        having: tail.having,
        order_by: tail.order_by,
        limit: tail.limit,
        offset: tail.offset,
    })
}

fn leading_sql_source_ref_with_consumed(value: &str) -> Option<(String, usize)> {
    if let Some(quoted) = leading_quoted_sql_literal_with_consumed(value) {
        return Some(quoted);
    }
    let trimmed = value.trim_start();
    let leading_ws = value.len() - trimmed.len();
    let consumed_identifier = trimmed
        .char_indices()
        .take_while(|(index, ch)| {
            if *index == 0 {
                *ch == '_' || ch.is_ascii_alphabetic()
            } else {
                *ch == '_' || ch.is_ascii_alphanumeric()
            }
        })
        .map(|(index, ch)| index + ch.len_utf8())
        .last()
        .unwrap_or(0);
    if consumed_identifier == 0 {
        return None;
    }
    Some((
        trimmed[..consumed_identifier].to_string(),
        leading_ws + consumed_identifier,
    ))
}

fn native_vortex_sql_source_ref_matches_request(
    source_ref: &str,
    source_is_identifier: bool,
    request: &PublicWorkflowRouteRequest,
) -> bool {
    request
        .input_format
        .as_deref()
        .is_some_and(|format| format == "vortex" || is_local_file_format(format))
        && request.input_uri.as_deref().is_some_and(|uri| {
            uri == source_ref
                || (source_is_identifier && declared_sql_source_identifier_matches(source_ref, uri))
        })
}

#[derive(Debug, Clone, Default)]
struct NativeVortexSqlTailClauses {
    where_clause: Option<String>,
    group_by: Option<String>,
    having: Option<String>,
    order_by: Option<String>,
    limit: Option<String>,
    offset: Option<String>,
}

fn parse_native_vortex_sql_allowed_tail(tail: &str) -> Option<NativeVortexSqlTailClauses> {
    let mut rest = tail.trim();
    let mut clauses = NativeVortexSqlTailClauses::default();
    if rest.is_empty() {
        return Some(clauses);
    }
    if sql_keyword_prefix(rest, "WHERE") {
        rest = rest["WHERE".len()..].trim();
        let (clause, tail) =
            take_sql_clause_until(rest, &["GROUP BY", "HAVING", "ORDER BY", "LIMIT", "OFFSET"]);
        if clause.is_empty() {
            return None;
        }
        clauses.where_clause = Some(clause.to_string());
        rest = tail;
    }
    if sql_keyword_prefix(rest, "GROUP BY") {
        rest = rest["GROUP BY".len()..].trim();
        let (clause, tail) =
            take_sql_clause_until(rest, &["HAVING", "ORDER BY", "LIMIT", "OFFSET"]);
        if clause.is_empty() {
            return None;
        }
        clauses.group_by = Some(clause.to_string());
        rest = tail;
    }
    if sql_keyword_prefix(rest, "HAVING") {
        rest = rest["HAVING".len()..].trim();
        let (clause, tail) = take_sql_clause_until(rest, &["ORDER BY", "LIMIT", "OFFSET"]);
        if clause.is_empty() {
            return None;
        }
        clauses.having = Some(clause.to_string());
        rest = tail;
    }
    if sql_keyword_prefix(rest, "ORDER BY") {
        rest = rest["ORDER BY".len()..].trim();
        let (clause, tail) = take_sql_clause_until(rest, &["LIMIT", "OFFSET"]);
        if clause.is_empty() {
            return None;
        }
        clauses.order_by = Some(clause.to_string());
        rest = tail;
    }
    if sql_keyword_prefix(rest, "LIMIT") {
        rest = rest["LIMIT".len()..].trim();
        let (clause, tail) = take_sql_clause_until(rest, &["OFFSET"]);
        clauses.limit = Some(parse_native_vortex_sql_limit_literal(clause)?);
        rest = tail;
    }
    if sql_keyword_prefix(rest, "OFFSET") {
        rest = rest["OFFSET".len()..].trim();
        let (clause, tail) = take_sql_clause_until(rest, &[]);
        clauses.offset = Some(parse_native_vortex_sql_limit_literal(clause)?);
        rest = tail;
    }
    rest.trim().is_empty().then_some(clauses)
}

fn take_sql_clause_until<'a>(value: &'a str, keywords: &[&str]) -> (&'a str, &'a str) {
    if keywords.is_empty() {
        return (value.trim(), "");
    }
    let Some((position, _keyword)) =
        find_first_sql_keyword_outside_quotes_and_parens(value, keywords)
    else {
        return (value.trim(), "");
    };
    (value[..position].trim(), value[position..].trim())
}

fn find_first_sql_keyword_outside_quotes_and_parens<'a>(
    value: &str,
    keywords: &'a [&str],
) -> Option<(usize, &'a str)> {
    keywords
        .iter()
        .filter_map(|keyword| {
            find_sql_keyword_outside_quotes_and_parens(value, keyword)
                .map(|position| (position, *keyword))
        })
        .min_by_key(|(position, _keyword)| *position)
}

fn parse_native_vortex_sql_limit_literal(value: &str) -> Option<String> {
    let value = value.trim();
    let digit_count = value
        .as_bytes()
        .iter()
        .take_while(|byte| byte.is_ascii_digit())
        .count();
    if digit_count == 0 || !value[digit_count..].trim().is_empty() {
        return None;
    }
    let limit = &value[..digit_count];
    limit.parse::<usize>().ok().map(|_| limit.to_string())
}

fn normalize_sql_projection_columns(projection: &str) -> Option<String> {
    let projection = projection.trim();
    if projection == "*" {
        return None;
    }
    let columns = projection
        .split(',')
        .map(str::trim)
        .map(|column| column.strip_prefix("f.").unwrap_or(column))
        .collect::<Vec<_>>();
    if columns.is_empty()
        || !columns.iter().all(|column| {
            is_summary_identifier(column)
                && !["null", "true", "false"]
                    .iter()
                    .any(|literal| column.eq_ignore_ascii_case(literal))
        })
    {
        return None;
    }
    Some(columns.join(","))
}

fn aggregate_payload_from_sql_projection(
    projection: &str,
    group_by: Option<&str>,
    having: Option<&str>,
    order_by: Option<&str>,
    offset: Option<&str>,
) -> Option<String> {
    let mut group_spec = if let Some(group_by) = group_by {
        parse_sql_group_by_spec(group_by, projection)?
    } else {
        ParsedSqlGroupSpec::default()
    };
    let mut measures = Vec::new();
    let mut parsed_measures = Vec::new();
    let mut projected_columns = Vec::new();
    for (index, item) in split_sql_projection_list(projection)
        .into_iter()
        .enumerate()
    {
        if let Some(measure) = scalar_aggregate_measure_from_sql(index, item) {
            projected_columns.push(measure.alias.clone());
            measures.push(measure.payload.clone());
            parsed_measures.push(measure);
            continue;
        }
        let projected_group_raw = strip_sql_alias(item.trim()).trim();
        let projected_group = normalize_sql_group_item(projected_group_raw);
        let projected_group_alias = sql_explicit_alias(item.trim());
        let direct_group_matches = projected_group.as_ref().is_some_and(|projected_group| {
            group_spec
                .columns
                .iter()
                .any(|column| column == projected_group)
        });
        let expression_group_matches = group_spec.expressions.iter().any(|expression| {
            expression.expression_key == compact_ascii_lower(projected_group_raw)
                || projected_group_alias.is_some_and(|alias| alias == expression.alias)
        });
        if !direct_group_matches && !expression_group_matches {
            return None;
        }
        projected_columns.push(group_spec.projected_name(
            projected_group_raw,
            projected_group_alias,
            projected_group,
        )?);
    }
    if group_spec.columns.is_empty() && group_spec.expressions.is_empty() && group_by.is_some() {
        return None;
    }
    if measures.is_empty() {
        return None;
    }
    group_spec.admit_output_layout(&projected_columns, &parsed_measures)?;
    let order_by = match order_by {
        Some(order_by) => parse_sql_aggregate_order_by(order_by, &group_spec, &parsed_measures)?,
        None => Vec::new(),
    };
    let having = match having {
        Some(having) => parse_sql_aggregate_having(having, &parsed_measures)?,
        None => Vec::new(),
    };
    let mut payload = serde_json::Map::new();
    if !group_spec.columns.is_empty() {
        payload.insert(
            "group_by".to_string(),
            serde_json::Value::Array(
                group_spec
                    .columns
                    .iter()
                    .map(|column| serde_json::Value::String(column.clone()))
                    .collect(),
            ),
        );
    }
    if !group_spec.expressions.is_empty() {
        payload.insert(
            "group_expressions".to_string(),
            serde_json::Value::Array(
                group_spec
                    .expressions
                    .iter()
                    .map(|expression| expression.payload.clone())
                    .collect(),
            ),
        );
    }
    payload.insert("measures".to_string(), serde_json::Value::Array(measures));
    if !order_by.is_empty() {
        payload.insert("order_by".to_string(), serde_json::Value::Array(order_by));
    }
    if !having.is_empty() {
        payload.insert("having".to_string(), serde_json::Value::Array(having));
    }
    if let Some(offset) = offset {
        let parsed = offset.parse::<usize>().ok()?;
        if parsed > 0 {
            payload.insert(
                "offset".to_string(),
                serde_json::Value::Number(serde_json::Number::from(parsed)),
            );
        }
    }
    Some(serde_json::Value::Object(payload).to_string())
}

fn sort_rows_payload_from_sql_order_by(
    order_by: &str,
    offset: Option<&str>,
    limit: &str,
) -> Option<String> {
    let order_by = split_sql_projection_list(order_by)
        .into_iter()
        .map(|item| {
            let (expression, descending, nulls) = parse_sql_order_item(item);
            let column = normalize_sql_group_item(strip_sql_alias(expression).trim())?;
            let mut order = serde_json::json!({
                "column": column,
                "descending": descending
            });
            if let Some(nulls) = nulls {
                order["nulls"] = nulls.into();
            }
            Some(order)
        })
        .collect::<Option<Vec<_>>>()?;
    if order_by.is_empty() {
        return None;
    }
    let parsed_limit = limit.parse::<usize>().ok()?;
    if parsed_limit == 0 {
        return None;
    }
    let mut payload = serde_json::Map::new();
    payload.insert("order_by".to_string(), serde_json::Value::Array(order_by));
    payload.insert(
        "limit".to_string(),
        serde_json::Value::Number(serde_json::Number::from(parsed_limit)),
    );
    if let Some(offset) = offset {
        let parsed = offset.parse::<usize>().ok()?;
        if parsed > 0 {
            payload.insert(
                "offset".to_string(),
                serde_json::Value::Number(serde_json::Number::from(parsed)),
            );
        }
    }
    Some(serde_json::Value::Object(payload).to_string())
}

#[derive(Debug, Clone)]
struct ParsedSqlAggregateMeasure {
    payload: serde_json::Value,
    alias: String,
    expression_key: String,
}

#[derive(Debug, Clone)]
struct ParsedSqlGroupExpression {
    payload: serde_json::Value,
    alias: String,
    expression_key: String,
}

#[derive(Debug, Clone, Default)]
struct ParsedSqlGroupSpec {
    columns: Vec<String>,
    expressions: Vec<ParsedSqlGroupExpression>,
}

impl ParsedSqlGroupSpec {
    fn projected_name(
        &self,
        raw: &str,
        alias: Option<&str>,
        column: Option<String>,
    ) -> Option<String> {
        alias
            .map(str::to_owned)
            .or_else(|| {
                self.expressions
                    .iter()
                    .find(|expression| expression.expression_key == compact_ascii_lower(raw))
                    .map(|expression| expression.alias.clone())
            })
            .or(column)
    }

    // This strategy emits every group key followed by its measures. Mixed raw
    // and computed keys use the existing identity expression to keep the SQL
    // key order in one native key vector. Other projection changes belong to
    // the shared native projection lowerer, not another result rewriter here.
    fn admit_output_layout(
        &mut self,
        projected_columns: &[String],
        measures: &[ParsedSqlAggregateMeasure],
    ) -> Option<()> {
        if projected_columns.iter().eq(self
            .columns
            .iter()
            .chain(self.expressions.iter().map(|expression| &expression.alias))
            .chain(measures.iter().map(|measure| &measure.alias)))
        {
            return Some(());
        }
        let groups = self.columns.len() + self.expressions.len();
        if self.expressions.is_empty()
            || projected_columns.len() != groups + measures.len()
            || !projected_columns[groups..]
                .iter()
                .eq(measures.iter().map(|measure| &measure.alias))
            || projected_columns[..groups]
                .iter()
                .collect::<BTreeSet<_>>()
                .len()
                != groups
        {
            return None;
        }
        let expressions = projected_columns[..groups]
            .iter()
            .map(|name| {
                if let Some(expression) = self
                    .expressions
                    .iter()
                    .find(|expression| &expression.alias == name)
                {
                    return Some(expression.clone());
                }
                self.columns.contains(name).then(|| ParsedSqlGroupExpression {
                payload: serde_json::json!({"alias":name,"column":name,"function":"identity"}),
                alias: name.clone(),
                expression_key: compact_ascii_lower(name),
            })
            })
            .collect::<Option<Vec<_>>>()?;
        self.columns.clear();
        self.expressions = expressions;
        Some(())
    }
}

fn scalar_aggregate_measure_from_sql(
    index: usize,
    item: &str,
) -> Option<ParsedSqlAggregateMeasure> {
    let raw_item = item.trim();
    let alias = sql_explicit_alias(raw_item);
    let item = strip_sql_alias(raw_item).trim();
    let open = item.find('(')?;
    if !item.ends_with(')') || open == 0 {
        return None;
    }
    let function = item[..open].trim().to_ascii_lowercase();
    if !matches!(function.as_str(), "sum" | "count" | "avg" | "min" | "max") {
        return None;
    }
    let argument = item[open + 1..item.len() - 1].trim();
    if function == "count" && argument == "*" {
        let alias = alias
            .filter(|alias| is_summary_identifier(alias))
            .map_or_else(|| format!("count_all_{index}"), str::to_string);
        return Some(ParsedSqlAggregateMeasure {
            payload: serde_json::json!({
            "function": "count",
            "alias": alias
            }),
            alias,
            expression_key: compact_ascii_lower(item),
        });
    }
    if function == "count"
        && let Some(distinct_argument) = argument
            .strip_prefix("DISTINCT ")
            .or_else(|| argument.strip_prefix("distinct "))
    {
        let column = distinct_argument
            .trim()
            .strip_prefix("f.")
            .unwrap_or(distinct_argument.trim());
        if !is_summary_identifier(column) {
            return None;
        }
        let alias = alias
            .filter(|alias| is_summary_identifier(alias))
            .map_or_else(
                || format!("count_distinct_{column}_{index}"),
                str::to_string,
            );
        return Some(ParsedSqlAggregateMeasure {
            payload: serde_json::json!({
                "function": "count_distinct",
                "column": column,
                "alias": alias
            }),
            alias,
            expression_key: compact_ascii_lower(item),
        });
    }
    let (column, argument_offset, value_transform) =
        if let Some(column) = parse_sql_length_expression(argument) {
            (column, 0, Some("length"))
        } else if function == "sum" {
            let (column, offset) = parse_sql_additive_aggregate_argument(argument)
                .unwrap_or_else(|| (argument.strip_prefix("f.").unwrap_or(argument).trim(), 0));
            (column, offset, None)
        } else {
            (
                argument.strip_prefix("f.").unwrap_or(argument).trim(),
                0,
                None,
            )
        };
    if !is_summary_identifier(column) {
        return None;
    }
    let alias = alias
        .filter(|alias| is_summary_identifier(alias))
        .map_or_else(|| format!("{function}_{column}_{index}"), str::to_string);
    let mut payload = serde_json::json!({
        "function": function,
        "column": column,
        "alias": alias
    });
    if argument_offset != 0
        && let serde_json::Value::Object(ref mut object) = payload
    {
        object.insert(
            "argument_offset".to_string(),
            serde_json::Value::Number(serde_json::Number::from(argument_offset)),
        );
    }
    if let Some(value_transform) = value_transform
        && let serde_json::Value::Object(ref mut object) = payload
    {
        object.insert(
            "value_transform".to_string(),
            serde_json::Value::String(value_transform.to_string()),
        );
    }
    Some(ParsedSqlAggregateMeasure {
        payload,
        alias,
        expression_key: compact_ascii_lower(item),
    })
}

fn parse_sql_additive_aggregate_argument(argument: &str) -> Option<(&str, i64)> {
    let argument = argument.trim();
    for operator in ['+', '-'] {
        let Some((column, literal)) = argument.rsplit_once(operator) else {
            continue;
        };
        let column = column.trim().strip_prefix("f.").unwrap_or(column.trim());
        if !is_summary_identifier(column) {
            continue;
        }
        let literal = literal.trim().parse::<i64>().ok()?;
        let offset = if operator == '-' {
            literal.checked_neg()?
        } else {
            literal
        };
        return Some((column, offset));
    }
    let column = argument.strip_prefix("f.").unwrap_or(argument).trim();
    is_summary_identifier(column).then_some((column, 0))
}

fn parse_sql_group_by_spec(group_by: &str, projection: &str) -> Option<ParsedSqlGroupSpec> {
    let mut spec = ParsedSqlGroupSpec::default();
    let projection_items = split_sql_projection_list(projection);
    let projection_expressions = split_sql_projection_list(projection)
        .into_iter()
        .filter_map(parse_sql_group_expression)
        .collect::<Vec<_>>();
    let fallback_column = split_sql_projection_list(group_by)
        .into_iter()
        .find_map(normalize_sql_group_item)
        .or_else(|| {
            projection_items
                .iter()
                .find_map(|item| normalize_sql_group_item(strip_sql_alias(item.trim()).trim()))
        });
    for item in split_sql_projection_list(group_by) {
        if let Ok(ordinal) = item.trim().parse::<usize>()
            && ordinal > 0
            && let Some(projection_item) = projection_items.get(ordinal - 1)
            && let Ok(value) = strip_sql_alias(projection_item.trim())
                .trim()
                .parse::<i64>()
        {
            let column = fallback_column.clone()?;
            let alias = format!("literal_{value}");
            spec.expressions.push(ParsedSqlGroupExpression {
                payload: serde_json::json!({
                    "alias": alias,
                    "column": column,
                    "function": "constant_int",
                    "argument_offset": value
                }),
                alias,
                expression_key: value.to_string(),
            });
            continue;
        }
        if let Some(expression) = projection_expressions
            .iter()
            .find(|expression| expression.alias.eq_ignore_ascii_case(item.trim()))
            .cloned()
        {
            if !spec
                .expressions
                .iter()
                .any(|existing| existing.alias == expression.alias)
            {
                spec.expressions.push(expression);
            }
            continue;
        }
        if let Some(column) = normalize_sql_group_item(item) {
            if !spec.columns.iter().any(|existing| existing == &column) {
                spec.columns.push(column);
            }
            continue;
        }
        let key = compact_ascii_lower(strip_sql_alias(item.trim()).trim());
        let expression = projection_expressions
            .iter()
            .find(|expression| {
                expression.expression_key == key
                    || expression.alias.eq_ignore_ascii_case(item.trim())
            })
            .cloned()
            .or_else(|| parse_sql_group_expression(item))?;
        if !spec
            .expressions
            .iter()
            .any(|existing| existing.alias == expression.alias)
        {
            spec.expressions.push(expression);
        }
    }
    Some(spec)
}

#[allow(clippy::too_many_lines)]
fn parse_sql_group_expression(item: &str) -> Option<ParsedSqlGroupExpression> {
    let alias = sql_explicit_alias(item)
        .filter(|alias| is_summary_identifier(alias))
        .map(str::to_string);
    let expression = strip_sql_alias(item.trim()).trim();
    let expression_key = compact_ascii_lower(expression);
    if let Some(alias) = alias.clone()
        && let Some(column) = normalize_sql_group_item(expression)
        && alias != column
    {
        return Some(ParsedSqlGroupExpression {
            payload: serde_json::json!({
                "alias": alias,
                "column": column,
                "function": "identity"
            }),
            alias,
            expression_key,
        });
    }
    if let Some((column, offset)) = parse_sql_additive_aggregate_argument(expression)
        && offset != 0
    {
        let alias = alias.unwrap_or_else(|| {
            format!(
                "{}_{}{}",
                column,
                if offset < 0 { "minus" } else { "plus" },
                offset.unsigned_abs()
            )
        });
        return Some(ParsedSqlGroupExpression {
            payload: serde_json::json!({
                "alias": alias,
                "column": column,
                "function": "add_offset",
                "argument_offset": offset
            }),
            alias,
            expression_key,
        });
    }
    if let Some(column) = parse_sql_length_expression(expression) {
        let alias = alias.unwrap_or_else(|| format!("length_{column}"));
        return Some(ParsedSqlGroupExpression {
            payload: serde_json::json!({
                "alias": alias,
                "column": column,
                "function": "length"
            }),
            alias,
            expression_key,
        });
    }
    if let Some(column) = parse_sql_extract_minute_expression(expression) {
        let alias = alias.unwrap_or_else(|| "minute".to_string());
        return Some(ParsedSqlGroupExpression {
            payload: serde_json::json!({
                "alias": alias,
                "column": column,
                "function": "extract_minute"
            }),
            alias,
            expression_key,
        });
    }
    if let Some(column) = parse_sql_date_trunc_minute_expression(expression) {
        let alias = alias.unwrap_or_else(|| "date_trunc_minute".to_string());
        return Some(ParsedSqlGroupExpression {
            payload: serde_json::json!({
                "alias": alias,
                "column": column,
                "function": "date_trunc_minute"
            }),
            alias,
            expression_key,
        });
    }
    if let Some(column) = parse_sql_regex_domain_expression(expression) {
        let alias = alias.unwrap_or_else(|| "domain".to_string());
        return Some(ParsedSqlGroupExpression {
            payload: serde_json::json!({
                "alias": alias,
                "column": column,
                "function": "regex_domain"
            }),
            alias,
            expression_key,
        });
    }
    if let Some((referer, search_engine, adv_engine)) =
        parse_sql_case_search_adv_zero_referer_expression(expression)
    {
        let alias = alias.unwrap_or_else(|| "traffic_source_referer".to_string());
        return Some(ParsedSqlGroupExpression {
            payload: serde_json::json!({
                "alias": alias,
                "column": referer,
                "extra_columns": [search_engine, adv_engine],
                "function": "case_search_adv_zero_referer_else_empty"
            }),
            alias,
            expression_key,
        });
    }
    None
}

fn parse_sql_length_expression(expression: &str) -> Option<&str> {
    let inner = parse_sql_function_call(expression, "length")?;
    let column = inner.trim().strip_prefix("f.").unwrap_or(inner.trim());
    is_summary_identifier(column).then_some(column)
}

fn parse_sql_extract_minute_expression(expression: &str) -> Option<&str> {
    let normalized = expression.trim();
    let inner = parse_sql_function_call(normalized, "extract")?;
    let (unit, tail) = inner
        .split_once("FROM")
        .or_else(|| inner.split_once("from"))?;
    if !unit.trim().eq_ignore_ascii_case("minute") {
        return None;
    }
    let column = tail.trim().strip_prefix("f.").unwrap_or(tail.trim());
    is_summary_identifier(column).then_some(column)
}

fn parse_sql_date_trunc_minute_expression(expression: &str) -> Option<&str> {
    let inner = parse_sql_function_call(expression, "date_trunc")?;
    let parts = split_sql_projection_list(inner);
    if parts.len() != 2 {
        return None;
    }
    if !parse_sql_single_quoted_literal(parts[0])?.eq_ignore_ascii_case("minute") {
        return None;
    }
    let column = parts[1]
        .trim()
        .strip_prefix("f.")
        .unwrap_or(parts[1].trim());
    is_summary_identifier(column).then_some(column)
}

fn parse_sql_regex_domain_expression(expression: &str) -> Option<&str> {
    let inner = parse_sql_function_call(expression, "regexp_replace")?;
    let parts = split_sql_projection_list(inner);
    if parts.len() != 3 {
        return None;
    }
    let column = parts[0]
        .trim()
        .strip_prefix("f.")
        .unwrap_or(parts[0].trim());
    let pattern = parse_sql_single_quoted_literal(parts[1])?;
    let replacement = parse_sql_single_quoted_literal(parts[2])?;
    (is_summary_identifier(column)
        && pattern == r"^https?://(?:www\.)?([^/]+)/.*$"
        && replacement == "\\1")
        .then_some(column)
}

fn parse_sql_case_search_adv_zero_referer_expression(
    expression: &str,
) -> Option<(&str, &str, &str)> {
    let lowered = expression.trim().to_ascii_lowercase();
    if !lowered.starts_with("case when ")
        || !lowered.contains(" then ")
        || !lowered.contains(" else ")
        || !lowered.ends_with(" end")
    {
        return None;
    }
    let when_tail = expression.trim()[4..].trim();
    let when_tail = when_tail
        .strip_prefix("WHEN")
        .or_else(|| when_tail.strip_prefix("when"))?
        .trim();
    let then_position = find_sql_keyword_outside_quotes_and_parens(when_tail, "THEN")?;
    let condition = when_tail[..then_position].trim();
    let then_tail = when_tail[then_position + "THEN".len()..].trim();
    let else_position = find_sql_keyword_outside_quotes_and_parens(then_tail, "ELSE")?;
    let then_value = then_tail[..else_position].trim();
    let else_tail = then_tail[else_position + "ELSE".len()..].trim();
    let else_value = else_tail
        .strip_suffix("END")
        .or_else(|| else_tail.strip_suffix("end"))?
        .trim();
    if !parse_sql_single_quoted_literal(else_value)?.is_empty() {
        return None;
    }
    let referer = then_value
        .trim()
        .strip_prefix("f.")
        .unwrap_or(then_value.trim());
    if !is_summary_identifier(referer) {
        return None;
    }
    let predicates = split_sql_conjunction_predicates(
        condition
            .strip_prefix('(')
            .and_then(|value| value.strip_suffix(')'))
            .unwrap_or(condition),
    );
    let mut zero_columns = Vec::new();
    for predicate in predicates {
        let (column, literal) = predicate.split_once('=')?;
        if summary_sql_literal_to_tiny_scalar(literal.trim())? != "0" {
            return None;
        }
        let column = column.trim().strip_prefix("f.").unwrap_or(column.trim());
        if !is_summary_identifier(column) {
            return None;
        }
        zero_columns.push(column);
    }
    let search_engine = zero_columns
        .iter()
        .find(|column| column.eq_ignore_ascii_case("SearchEngineID"))?;
    let adv_engine = zero_columns
        .iter()
        .find(|column| column.eq_ignore_ascii_case("AdvEngineID"))?;
    Some((referer, search_engine, adv_engine))
}

fn parse_sql_function_call<'a>(expression: &'a str, function: &str) -> Option<&'a str> {
    let trimmed = expression.trim();
    let open = trimmed.find('(')?;
    if !trimmed[..open].trim().eq_ignore_ascii_case(function) || !trimmed.ends_with(')') {
        return None;
    }
    Some(&trimmed[open + 1..trimmed.len() - 1])
}

fn parse_sql_aggregate_order_by(
    order_by: &str,
    group_spec: &ParsedSqlGroupSpec,
    measures: &[ParsedSqlAggregateMeasure],
) -> Option<Vec<serde_json::Value>> {
    split_sql_projection_list(order_by)
        .into_iter()
        .map(|item| {
            let (expression, descending, nulls) = parse_sql_order_item(item);
            let expression = strip_sql_alias(expression).trim();
            let column = normalize_sql_group_item(expression)
                .filter(|column| group_spec.columns.iter().any(|group| group == column))
                .or_else(|| {
                    group_spec
                        .expressions
                        .iter()
                        .find(|group_expression| {
                            group_expression.alias.eq_ignore_ascii_case(expression)
                                || group_expression.expression_key
                                    == compact_ascii_lower(expression)
                        })
                        .map(|group_expression| group_expression.alias.clone())
                })
                .or_else(|| {
                    measures
                        .iter()
                        .find(|measure| {
                            measure.alias.eq_ignore_ascii_case(expression)
                                || measure.expression_key == compact_ascii_lower(expression)
                        })
                        .map(|measure| measure.alias.clone())
                })?;
            let mut order = serde_json::json!({
                "column": column,
                "descending": descending
            });
            if let Some(nulls) = nulls {
                order["nulls"] = nulls.into();
            }
            Some(order)
        })
        .collect()
}

fn parse_sql_aggregate_having(
    having: &str,
    measures: &[ParsedSqlAggregateMeasure],
) -> Option<Vec<serde_json::Value>> {
    split_sql_conjunction_predicates(having)
        .into_iter()
        .map(|predicate| {
            for (sql_op, op) in [
                (">=", "gte"),
                ("<=", "lte"),
                ("!=", "neq"),
                ("<>", "neq"),
                (">", "gt"),
                ("<", "lt"),
                ("=", "eq"),
            ] {
                let Some((left, right)) = predicate.split_once(sql_op) else {
                    continue;
                };
                let left = left.trim();
                let column = measures
                    .iter()
                    .find(|measure| {
                        measure.alias.eq_ignore_ascii_case(left)
                            || measure.expression_key == compact_ascii_lower(left)
                    })
                    .map(|measure| measure.alias.clone())?;
                let value = summary_sql_literal_to_tiny_scalar(right.trim())?;
                return Some(serde_json::json!({
                    "column": column,
                    "op": op,
                    "value": value
                }));
            }
            None
        })
        .collect()
}

fn parse_sql_order_item(item: &str) -> (&str, bool, Option<&'static str>) {
    let item = item.trim();
    let (item, nulls) = ["first", "last"]
        .into_iter()
        .find_map(|placement| {
            let position = find_sql_trailing_keyword(item, placement)?;
            let preceding = item[..position].trim();
            let nulls_position = find_sql_trailing_keyword(preceding, "NULLS")?;
            Some((preceding[..nulls_position].trim(), Some(placement)))
        })
        .unwrap_or((item, None));
    if let Some(position) = find_sql_trailing_keyword(item, "DESC") {
        return (item[..position].trim(), true, nulls);
    }
    if let Some(position) = find_sql_trailing_keyword(item, "ASC") {
        return (item[..position].trim(), false, nulls);
    }
    (item, false, nulls)
}

fn find_sql_trailing_keyword(value: &str, keyword: &str) -> Option<usize> {
    let trimmed = value.trim_end();
    let suffix_start = trimmed.len().checked_sub(keyword.len())?;
    let suffix = &trimmed[suffix_start..];
    if !suffix.eq_ignore_ascii_case(keyword) {
        return None;
    }
    if suffix_start == 0 {
        return Some(0);
    }
    trimmed.as_bytes()[suffix_start - 1]
        .is_ascii_whitespace()
        .then_some(suffix_start - 1)
}

fn normalize_sql_group_item(item: &str) -> Option<String> {
    let item = item.trim();
    let column = item.strip_prefix("f.").unwrap_or(item);
    is_summary_identifier(column).then(|| column.to_string())
}

fn split_sql_projection_list(projection: &str) -> Vec<&str> {
    let mut items = Vec::new();
    let mut start = 0usize;
    let mut depth = 0usize;
    let mut in_single_quote = false;
    let bytes = projection.as_bytes();
    let mut index = 0usize;
    while index < bytes.len() {
        let byte = bytes[index];
        match byte {
            b'\'' => {
                in_single_quote = !in_single_quote;
            }
            b'(' if !in_single_quote => depth = depth.saturating_add(1),
            b')' if !in_single_quote => depth = depth.saturating_sub(1),
            b',' if !in_single_quote && depth == 0 => {
                items.push(projection[start..index].trim());
                start = index + 1;
            }
            _ => {}
        }
        index += 1;
    }
    items.push(projection[start..].trim());
    items.into_iter().filter(|item| !item.is_empty()).collect()
}

fn strip_sql_alias(item: &str) -> &str {
    find_sql_keyword_outside_quotes_and_parens(item, "AS")
        .map_or(item, |position| item[..position].trim())
}

fn sql_explicit_alias(item: &str) -> Option<&str> {
    let position = find_sql_keyword_outside_quotes_and_parens(item, "AS")?;
    let alias = item[position + "AS".len()..].trim();
    (!alias.is_empty()).then_some(alias)
}

#[derive(Debug, Clone, Copy)]
struct SummaryOperation<'a> {
    kind: &'a str,
    arg: &'a str,
}

#[derive(Debug, Clone)]
struct SummarySampleShape {
    source_order_limit: Option<String>,
    sample_fraction: Option<String>,
    sample_seed: String,
    sample_replacement: bool,
    sample_weight_column: Option<String>,
}

fn parse_plan_summary_operations(summary: &str) -> Option<Vec<SummaryOperation<'_>>> {
    let mut operations = Vec::new();
    for segment in summary.split(" -> ") {
        let open = segment.find('(')?;
        if !segment.ends_with(')') || open == 0 {
            return None;
        }
        let kind = &segment[..open];
        if !kind
            .chars()
            .all(|ch| ch.is_ascii_lowercase() || ch == '_' || ch.is_ascii_digit())
        {
            return None;
        }
        operations.push(SummaryOperation {
            kind,
            arg: &segment[open + 1..segment.len() - 1],
        });
    }
    (!operations.is_empty()).then_some(operations)
}

fn strip_index_metadata_operations<'a>(
    operations: &[SummaryOperation<'a>],
) -> Vec<SummaryOperation<'a>> {
    operations
        .iter()
        .copied()
        .filter(|operation| operation.kind != "set_index")
        .collect()
}

fn summary_read_vortex_matches_input(operations: &[SummaryOperation<'_>], input_uri: &str) -> bool {
    operations.first().is_some_and(|operation| {
        operation.kind == "read_vortex" && operation.arg.trim() == input_uri
    })
}

fn matches_summary_kinds(operations: &[SummaryOperation<'_>], kinds: &[&str]) -> bool {
    operations.len() == kinds.len()
        && operations
            .iter()
            .zip(kinds)
            .all(|(operation, kind)| operation.kind == *kind)
}

fn summary_positive_limit(value: &str) -> bool {
    value.trim().parse::<usize>().is_ok_and(|parsed| parsed > 0)
}

fn infer_native_vortex_primitive_payload(
    request: &PublicWorkflowRouteRequest,
) -> Option<InferredNativeVortexRoutePayload> {
    if !matches!(
        request.requested_output.as_str(),
        "collect"
            | "write_vortex"
            | "write_json"
            | "write_jsonl"
            | "write_csv"
            | "write_parquet"
            | "write_arrow_ipc"
            | "write_avro"
            | "write_orc"
    ) {
        return None;
    }
    let operations = parse_plan_summary_operations(request.plan_summary.as_deref()?)?;
    let operations = strip_index_metadata_operations(&operations);
    if !summary_read_vortex_matches_input(&operations, request.input_uri.as_deref()?) {
        return None;
    }
    infer_native_vortex_sample_primitive_payload(&operations)
        .or_else(|| infer_native_vortex_structured_export_payload(request, &operations))
        .or_else(|| infer_native_vortex_explode_primitive_payload(&operations))
        .or_else(|| infer_native_vortex_rolling_window_primitive_payload(&operations))
        .or_else(|| infer_native_vortex_sort_rows_primitive_payload(&operations))
        .or_else(|| infer_native_vortex_tail_primitive_payload(&operations))
        .or_else(|| infer_native_vortex_drop_duplicates_primitive_payload(&operations))
        .or_else(|| infer_native_vortex_duplicate_mask_primitive_payload(&operations))
        .or_else(|| infer_native_vortex_distinct_primitive_payload(&operations))
        .or_else(|| infer_native_vortex_basic_primitive_payload(&operations))
}

fn native_vortex_primitive_payload(
    primitive: PublicVortexPrimitive,
    predicate: Option<String>,
    columns: Option<String>,
    source_order_limit: Option<String>,
) -> InferredNativeVortexRoutePayload {
    native_vortex_primitive_payload_with_seed(
        primitive,
        predicate,
        columns,
        source_order_limit,
        None,
    )
}

fn native_vortex_primitive_payload_with_seed(
    primitive: PublicVortexPrimitive,
    predicate: Option<String>,
    columns: Option<String>,
    source_order_limit: Option<String>,
    sample_seed: Option<String>,
) -> InferredNativeVortexRoutePayload {
    InferredNativeVortexRoutePayload {
        family: NativeVortexOperationFamily::from_primitive(primitive),
        primitive: Some(primitive),
        predicate,
        columns,
        source_order_limit,
        sample_seed,
        sample_fraction: None,
        sample_replacement: false,
        sample_weight_column: None,
        duplicate_keep: None,

        deduplicate_key_columns: None,
        expression_projection: None,
        explode_projection: None,
        pivot_projection: None,
        rolling_window: None,
        aggregate: None,
        sort_rows: None,
        right_input: None,
    }
}

fn native_vortex_structured_export_payload(
    source_columns: &[String],
    structured_projection: &serde_json::Value,
    source_order_limit: Option<String>,
) -> InferredNativeVortexRoutePayload {
    InferredNativeVortexRoutePayload {
        family: NativeVortexOperationFamily::ExpressionProject,
        primitive: Some(PublicVortexPrimitive::ExpressionProject),
        predicate: None,
        columns: Some(source_columns.join(",")),
        source_order_limit,
        sample_seed: None,
        sample_fraction: None,
        sample_replacement: false,
        sample_weight_column: None,
        duplicate_keep: None,

        deduplicate_key_columns: None,
        expression_projection: Some(structured_projection.to_string()),
        explode_projection: None,
        pivot_projection: None,
        rolling_window: None,
        aggregate: None,
        sort_rows: None,
        right_input: None,
    }
}

fn native_vortex_duplicate_mask_primitive_payload(
    columns: String,
    source_order_limit: Option<String>,
    duplicate_keep: Option<String>,
) -> InferredNativeVortexRoutePayload {
    InferredNativeVortexRoutePayload {
        family: NativeVortexOperationFamily::DuplicateMask,
        primitive: Some(PublicVortexPrimitive::DuplicateMask),
        predicate: None,
        columns: Some(columns),
        source_order_limit,
        sample_seed: None,
        sample_fraction: None,
        sample_replacement: false,
        sample_weight_column: None,
        duplicate_keep,

        deduplicate_key_columns: None,
        expression_projection: None,
        explode_projection: None,
        pivot_projection: None,
        rolling_window: None,
        aggregate: None,
        sort_rows: None,
        right_input: None,
    }
}

fn native_vortex_drop_duplicates_primitive_payload(
    predicate: Option<String>,
    output_columns: Option<String>,
    key_columns: String,
    source_order_limit: Option<String>,
    duplicate_keep: Option<String>,
) -> InferredNativeVortexRoutePayload {
    InferredNativeVortexRoutePayload {
        family: NativeVortexOperationFamily::Distinct,
        primitive: Some(PublicVortexPrimitive::DropDuplicates),
        predicate,
        columns: output_columns,
        source_order_limit,
        sample_seed: None,
        sample_fraction: None,
        sample_replacement: false,
        sample_weight_column: None,
        duplicate_keep,
        deduplicate_key_columns: Some(key_columns),
        expression_projection: None,
        explode_projection: None,
        pivot_projection: None,
        rolling_window: None,
        aggregate: None,
        sort_rows: None,
        right_input: None,
    }
}

fn native_vortex_sample_primitive_payload(
    predicate: Option<String>,
    columns: Option<String>,
    sample_shape: SummarySampleShape,
) -> InferredNativeVortexRoutePayload {
    InferredNativeVortexRoutePayload {
        family: NativeVortexOperationFamily::Sample,
        primitive: Some(PublicVortexPrimitive::Sample),
        predicate,
        columns,
        source_order_limit: sample_shape.source_order_limit,
        sample_seed: Some(sample_shape.sample_seed),
        sample_fraction: sample_shape.sample_fraction,
        sample_replacement: sample_shape.sample_replacement,
        sample_weight_column: sample_shape.sample_weight_column,
        duplicate_keep: None,

        deduplicate_key_columns: None,
        expression_projection: None,
        explode_projection: None,
        pivot_projection: None,
        rolling_window: None,
        aggregate: None,
        sort_rows: None,
        right_input: None,
    }
}

fn infer_native_vortex_structured_export_payload(
    request: &PublicWorkflowRouteRequest,
    operations: &[SummaryOperation<'_>],
) -> Option<InferredNativeVortexRoutePayload> {
    if !matches!(
        request.requested_output.as_str(),
        "write_vortex" | "write_parquet" | "write_arrow_ipc" | "write_avro" | "write_orc"
    ) {
        return None;
    }
    if operations.len() < 2 {
        return None;
    }
    let limit = operations
        .last()
        .filter(|operation| operation.kind == "limit" && summary_positive_limit(operation.arg))
        .map(|operation| operation.arg.trim().to_string());
    let end = if limit.is_some() {
        operations.len().saturating_sub(1)
    } else {
        operations.len()
    };
    if operations.get(1)?.kind != "select" {
        return None;
    }
    if operations[2..end]
        .iter()
        .any(|operation| operation.kind != "with_column")
    {
        return None;
    }
    let mut structured_columns = Vec::new();
    let mut source_columns = Vec::new();
    for column in split_sql_projection_list(operations[1].arg) {
        if !is_summary_identifier(column) {
            return None;
        }
        push_unique_string(&mut source_columns, column.to_string());
        structured_columns.push(serde_json::json!({
            "name": column,
            "source": column
        }));
    }
    for operation in &operations[2..end] {
        let (name, expression) = operation.arg.split_once(',')?;
        let name = name.trim();
        if !is_summary_identifier(name) {
            return None;
        }
        let expression = expression.trim();
        let structured_column = if let Some(values) = parse_summary_array_literal(expression) {
            serde_json::json!({
                "name": name,
                "array": values
            })
        } else {
            let columns = parse_summary_struct_columns(expression)?;
            for column in &columns {
                push_unique_string(&mut source_columns, column.clone());
            }
            serde_json::json!({
                "name": name,
                "struct": columns
            })
        };
        structured_columns.push(structured_column);
    }
    if structured_columns.is_empty() || source_columns.is_empty() {
        return None;
    }
    let structured_projection = serde_json::json!({ "structured_columns": structured_columns });
    Some(native_vortex_structured_export_payload(
        &source_columns,
        &structured_projection,
        limit,
    ))
}

fn parse_summary_array_literal(value: &str) -> Option<Vec<serde_json::Value>> {
    let inner = value.trim().strip_prefix("ARRAY[")?.strip_suffix(']')?;
    split_sql_projection_list(inner)
        .into_iter()
        .map(parse_summary_structured_literal)
        .collect()
}

fn parse_summary_struct_columns(value: &str) -> Option<Vec<String>> {
    let inner = value.trim().strip_prefix("STRUCT(")?.strip_suffix(')')?;
    let columns = split_sql_projection_list(inner)
        .into_iter()
        .map(str::trim)
        .map(|column| is_summary_identifier(column).then(|| column.to_string()))
        .collect::<Option<Vec<_>>>()?;
    (!columns.is_empty()).then_some(columns)
}

fn parse_summary_structured_literal(value: &str) -> Option<serde_json::Value> {
    let trimmed = value.trim();
    if trimmed.eq_ignore_ascii_case("NULL") {
        return Some(serde_json::Value::Null);
    }
    if trimmed.eq_ignore_ascii_case("TRUE") {
        return Some(serde_json::Value::Bool(true));
    }
    if trimmed.eq_ignore_ascii_case("FALSE") {
        return Some(serde_json::Value::Bool(false));
    }
    if let Ok(value) = trimmed.parse::<i64>() {
        return Some(serde_json::json!(value));
    }
    if let Ok(value) = trimmed.parse::<f64>()
        && value.is_finite()
    {
        return Some(serde_json::json!(value));
    }
    parse_sql_single_quoted_literal(trimmed).map(serde_json::Value::String)
}

fn push_unique_string(out: &mut Vec<String>, value: String) {
    if !out.iter().any(|existing| existing == &value) {
        out.push(value);
    }
}

fn native_vortex_explode_primitive_payload(
    payload: String,
    columns: Option<String>,
    source_order_limit: Option<String>,
) -> InferredNativeVortexRoutePayload {
    InferredNativeVortexRoutePayload {
        family: NativeVortexOperationFamily::Explode,
        primitive: Some(PublicVortexPrimitive::Explode),
        predicate: None,
        columns,
        source_order_limit,
        sample_seed: None,
        sample_fraction: None,
        sample_replacement: false,
        sample_weight_column: None,
        duplicate_keep: None,

        deduplicate_key_columns: None,
        expression_projection: None,
        explode_projection: Some(payload),
        pivot_projection: None,
        rolling_window: None,
        aggregate: None,
        sort_rows: None,
        right_input: None,
    }
}

fn infer_native_vortex_explode_primitive_payload(
    operations: &[SummaryOperation<'_>],
) -> Option<InferredNativeVortexRoutePayload> {
    if matches_summary_kinds(operations, &["read_vortex", "explode", "limit"])
        && summary_positive_limit(operations[2].arg)
    {
        let (payload, column) = summary_explode_projection_payload(operations[1].arg)?;
        Some(native_vortex_explode_primitive_payload(
            payload,
            Some(column),
            Some(operations[2].arg.trim().to_string()),
        ))
    } else if matches_summary_kinds(operations, &["read_vortex", "explode"]) {
        let (payload, column) = summary_explode_projection_payload(operations[1].arg)?;
        Some(native_vortex_explode_primitive_payload(
            payload,
            Some(column),
            None,
        ))
    } else {
        None
    }
}

fn summary_explode_projection_payload(value: &str) -> Option<(String, String)> {
    let column = value.trim();
    if !is_summary_identifier(column) {
        return None;
    }
    let payload = serde_json::json!({ "column": column }).to_string();
    Some((payload, column.to_string()))
}

fn native_vortex_rolling_window_primitive_payload(
    payload: String,
    columns: Option<String>,
    source_order_limit: Option<String>,
) -> InferredNativeVortexRoutePayload {
    InferredNativeVortexRoutePayload {
        family: NativeVortexOperationFamily::RollingWindow,
        primitive: Some(PublicVortexPrimitive::RollingWindow),
        predicate: None,
        columns,
        source_order_limit,
        sample_seed: None,
        sample_fraction: None,
        sample_replacement: false,
        sample_weight_column: None,
        duplicate_keep: None,

        deduplicate_key_columns: None,
        expression_projection: None,
        explode_projection: None,
        pivot_projection: None,
        rolling_window: Some(payload),
        aggregate: None,
        sort_rows: None,
        right_input: None,
    }
}

fn infer_native_vortex_rolling_window_primitive_payload(
    operations: &[SummaryOperation<'_>],
) -> Option<InferredNativeVortexRoutePayload> {
    if matches_summary_kinds(operations, &["read_vortex", "rolling_window", "limit"])
        && summary_positive_limit(operations[2].arg)
    {
        let payload = summary_rolling_window_payload(operations[1].arg)?;
        let columns = summary_rolling_window_source_column(&payload)?;
        Some(native_vortex_rolling_window_primitive_payload(
            payload,
            Some(columns),
            Some(operations[2].arg.trim().to_string()),
        ))
    } else if matches_summary_kinds(operations, &["read_vortex", "rolling_window"]) {
        let payload = summary_rolling_window_payload(operations[1].arg)?;
        let columns = summary_rolling_window_source_column(&payload)?;
        Some(native_vortex_rolling_window_primitive_payload(
            payload,
            Some(columns),
            None,
        ))
    } else {
        None
    }
}

fn summary_rolling_window_payload(value: &str) -> Option<String> {
    let payload = value.trim();
    (!payload.is_empty()).then(|| payload.to_string())
}

fn summary_rolling_window_source_column(payload: &str) -> Option<String> {
    let value = serde_json::from_str::<serde_json::Value>(payload).ok()?;
    let column = value
        .get("source_column")
        .or_else(|| value.get("column"))
        .or_else(|| value.get("on"))
        .and_then(serde_json::Value::as_str)?
        .trim();
    is_summary_identifier(column).then(|| column.to_string())
}

fn native_vortex_sort_rows_primitive_payload(
    predicate: Option<String>,
    columns: Option<String>,
    source_order_limit: String,
    sort_rows: String,
) -> InferredNativeVortexRoutePayload {
    InferredNativeVortexRoutePayload {
        family: NativeVortexOperationFamily::TopN,
        primitive: Some(PublicVortexPrimitive::SortRows),
        predicate,
        columns,
        source_order_limit: Some(source_order_limit),
        sample_seed: None,
        sample_fraction: None,
        sample_replacement: false,
        sample_weight_column: None,
        duplicate_keep: None,

        deduplicate_key_columns: None,
        expression_projection: None,
        explode_projection: None,
        pivot_projection: None,
        rolling_window: None,
        aggregate: None,
        sort_rows: Some(sort_rows),
        right_input: None,
    }
}

fn infer_native_vortex_sort_rows_primitive_payload(
    operations: &[SummaryOperation<'_>],
) -> Option<InferredNativeVortexRoutePayload> {
    if matches_summary_kinds(operations, &["read_vortex", "select", "sort", "limit"])
        && summary_positive_limit(operations[3].arg)
    {
        let limit = operations[3].arg.trim().to_string();
        return Some(native_vortex_sort_rows_primitive_payload(
            None,
            Some(operations[1].arg.trim().to_string()),
            limit.clone(),
            summary_sort_rows_payload(operations[2].arg, &limit)?,
        ));
    }
    if matches_summary_kinds(
        operations,
        &["read_vortex", "filter", "select", "sort", "limit"],
    ) && summary_positive_limit(operations[4].arg)
    {
        let predicate = summary_tiny_predicate_from_sql(operations[1].arg)?;
        let limit = operations[4].arg.trim().to_string();
        return Some(native_vortex_sort_rows_primitive_payload(
            Some(predicate),
            Some(operations[2].arg.trim().to_string()),
            limit.clone(),
            summary_sort_rows_payload(operations[3].arg, &limit)?,
        ));
    }
    None
}

fn summary_sort_rows_payload(sort: &str, limit: &str) -> Option<String> {
    let parts = sort.split(',').map(str::trim).collect::<Vec<_>>();
    let (descending, column) = match parts.as_slice() {
        ["desc" | "DESC", column] | [column, "desc" | "DESC"] => (true, *column),
        ["asc" | "ASC", column] | [column, "asc" | "ASC"] | [column] => (false, *column),
        _ => return None,
    };
    if !is_summary_identifier(column) {
        return None;
    }
    let parsed_limit = limit.parse::<usize>().ok()?;
    if parsed_limit == 0 {
        return None;
    }
    Some(
        serde_json::json!({
            "order_by": [{"column": column, "descending": descending}],
            "limit": parsed_limit
        })
        .to_string(),
    )
}

fn infer_native_vortex_distinct_primitive_payload(
    operations: &[SummaryOperation<'_>],
) -> Option<InferredNativeVortexRoutePayload> {
    if matches_summary_kinds(
        operations,
        &["read_vortex", "filter", "select", "distinct", "limit"],
    ) && summary_positive_limit(operations[4].arg)
    {
        let predicate = summary_tiny_predicate_from_sql(operations[1].arg)?;
        Some(native_vortex_primitive_payload(
            PublicVortexPrimitive::Distinct,
            Some(predicate),
            Some(operations[2].arg.trim().to_string()),
            Some(operations[4].arg.trim().to_string()),
        ))
    } else if matches_summary_kinds(operations, &["read_vortex", "filter", "select", "distinct"]) {
        let predicate = summary_tiny_predicate_from_sql(operations[1].arg)?;
        Some(native_vortex_primitive_payload(
            PublicVortexPrimitive::Distinct,
            Some(predicate),
            Some(operations[2].arg.trim().to_string()),
            None,
        ))
    } else if matches_summary_kinds(operations, &["read_vortex", "filter", "distinct", "limit"])
        && summary_positive_limit(operations[3].arg)
    {
        let predicate = summary_tiny_predicate_from_sql(operations[1].arg)?;
        Some(native_vortex_primitive_payload(
            PublicVortexPrimitive::Distinct,
            Some(predicate),
            None,
            Some(operations[3].arg.trim().to_string()),
        ))
    } else if matches_summary_kinds(operations, &["read_vortex", "filter", "distinct"]) {
        let predicate = summary_tiny_predicate_from_sql(operations[1].arg)?;
        Some(native_vortex_primitive_payload(
            PublicVortexPrimitive::Distinct,
            Some(predicate),
            None,
            None,
        ))
    } else if matches_summary_kinds(operations, &["read_vortex", "select", "distinct", "limit"])
        && summary_positive_limit(operations[3].arg)
    {
        Some(native_vortex_primitive_payload(
            PublicVortexPrimitive::Distinct,
            None,
            Some(operations[1].arg.trim().to_string()),
            Some(operations[3].arg.trim().to_string()),
        ))
    } else if matches_summary_kinds(operations, &["read_vortex", "select", "distinct"]) {
        Some(native_vortex_primitive_payload(
            PublicVortexPrimitive::Distinct,
            None,
            Some(operations[1].arg.trim().to_string()),
            None,
        ))
    } else if matches_summary_kinds(operations, &["read_vortex", "distinct", "limit"])
        && summary_positive_limit(operations[2].arg)
    {
        Some(native_vortex_primitive_payload(
            PublicVortexPrimitive::Distinct,
            None,
            None,
            Some(operations[2].arg.trim().to_string()),
        ))
    } else if matches_summary_kinds(operations, &["read_vortex", "distinct"]) {
        Some(native_vortex_primitive_payload(
            PublicVortexPrimitive::Distinct,
            None,
            None,
            None,
        ))
    } else {
        None
    }
}

fn infer_native_vortex_drop_duplicates_primitive_payload(
    operations: &[SummaryOperation<'_>],
) -> Option<InferredNativeVortexRoutePayload> {
    for duplicate_kind in ["drop_duplicates", "drop-duplicates", "drop_duplicate_rows"] {
        if let Some(payload) = infer_native_vortex_drop_duplicates_variant(
            operations,
            &["read_vortex", "filter", "select", duplicate_kind, "limit"],
            Some(1),
            Some(2),
            3,
            Some(4),
        ) {
            return Some(payload);
        }
        if let Some(payload) = infer_native_vortex_drop_duplicates_variant(
            operations,
            &["read_vortex", "filter", "select", duplicate_kind],
            Some(1),
            Some(2),
            3,
            None,
        ) {
            return Some(payload);
        }
        if let Some(payload) = infer_native_vortex_drop_duplicates_variant(
            operations,
            &["read_vortex", "filter", duplicate_kind, "limit"],
            Some(1),
            None,
            2,
            Some(3),
        ) {
            return Some(payload);
        }
        if let Some(payload) = infer_native_vortex_drop_duplicates_variant(
            operations,
            &["read_vortex", "filter", duplicate_kind],
            Some(1),
            None,
            2,
            None,
        ) {
            return Some(payload);
        }
        if let Some(payload) = infer_native_vortex_drop_duplicates_variant(
            operations,
            &["read_vortex", "select", duplicate_kind, "limit"],
            None,
            Some(1),
            2,
            Some(3),
        ) {
            return Some(payload);
        }
        if let Some(payload) = infer_native_vortex_drop_duplicates_variant(
            operations,
            &["read_vortex", "select", duplicate_kind],
            None,
            Some(1),
            2,
            None,
        ) {
            return Some(payload);
        }
        if let Some(payload) = infer_native_vortex_drop_duplicates_variant(
            operations,
            &["read_vortex", duplicate_kind, "limit"],
            None,
            None,
            1,
            Some(2),
        ) {
            return Some(payload);
        }
        if let Some(payload) = infer_native_vortex_drop_duplicates_variant(
            operations,
            &["read_vortex", duplicate_kind],
            None,
            None,
            1,
            None,
        ) {
            return Some(payload);
        }
    }
    None
}

fn infer_native_vortex_drop_duplicates_variant(
    operations: &[SummaryOperation<'_>],
    kinds: &[&str],
    predicate_index: Option<usize>,
    output_index: Option<usize>,
    duplicate_index: usize,
    limit_index: Option<usize>,
) -> Option<InferredNativeVortexRoutePayload> {
    if !matches_summary_kinds(operations, kinds) {
        return None;
    }
    let source_order_limit = if let Some(index) = limit_index {
        if !summary_positive_limit(operations[index].arg) {
            return None;
        }
        Some(operations[index].arg.trim().to_string())
    } else {
        None
    };
    let predicate = if let Some(index) = predicate_index {
        Some(summary_tiny_predicate_from_sql(operations[index].arg)?)
    } else {
        None
    };
    let output_columns = if let Some(index) = output_index {
        Some(normalize_sql_projection_columns(operations[index].arg)?)
    } else {
        None
    };
    let (key_columns, duplicate_keep) =
        summary_duplicate_mask_columns_and_keep(operations[duplicate_index].arg)?;
    Some(native_vortex_drop_duplicates_primitive_payload(
        predicate,
        output_columns,
        key_columns,
        source_order_limit,
        duplicate_keep,
    ))
}

fn infer_native_vortex_duplicate_mask_primitive_payload(
    operations: &[SummaryOperation<'_>],
) -> Option<InferredNativeVortexRoutePayload> {
    for duplicate_kind in ["duplicate_mask", "duplicated"] {
        if matches_summary_kinds(
            operations,
            &["read_vortex", "select", duplicate_kind, "limit"],
        ) && summary_positive_limit(operations[3].arg)
        {
            let (columns, duplicate_keep) =
                summary_duplicate_mask_columns_and_keep(operations[2].arg)?;
            return Some(native_vortex_duplicate_mask_primitive_payload(
                columns,
                Some(operations[3].arg.trim().to_string()),
                duplicate_keep,
            ));
        }
        if matches_summary_kinds(operations, &["read_vortex", "select", duplicate_kind]) {
            let (columns, duplicate_keep) =
                summary_duplicate_mask_columns_and_keep(operations[2].arg)?;
            return Some(native_vortex_duplicate_mask_primitive_payload(
                columns,
                None,
                duplicate_keep,
            ));
        }
        if matches_summary_kinds(operations, &["read_vortex", duplicate_kind, "limit"])
            && summary_positive_limit(operations[2].arg)
        {
            let (columns, duplicate_keep) =
                summary_duplicate_mask_columns_and_keep(operations[1].arg)?;
            return Some(native_vortex_duplicate_mask_primitive_payload(
                columns,
                Some(operations[2].arg.trim().to_string()),
                duplicate_keep,
            ));
        }
        if matches_summary_kinds(operations, &["read_vortex", duplicate_kind]) {
            let (columns, duplicate_keep) =
                summary_duplicate_mask_columns_and_keep(operations[1].arg)?;
            return Some(native_vortex_duplicate_mask_primitive_payload(
                columns,
                None,
                duplicate_keep,
            ));
        }
    }
    None
}

fn summary_duplicate_mask_columns_and_keep(value: &str) -> Option<(String, Option<String>)> {
    let mut columns = Vec::new();
    let mut duplicate_keep = None;
    for part in value
        .split(',')
        .map(str::trim)
        .filter(|part| !part.is_empty())
    {
        if let Some(keep) = part.strip_prefix("keep=") {
            if duplicate_keep.is_some() || !matches!(keep, "first" | "last" | "false") {
                return None;
            }
            duplicate_keep = Some(keep.to_string());
        } else {
            if !is_summary_identifier(part) {
                return None;
            }
            columns.push(part.to_string());
        }
    }
    (!columns.is_empty()).then(|| (columns.join(","), duplicate_keep))
}

fn infer_native_vortex_sample_primitive_payload(
    operations: &[SummaryOperation<'_>],
) -> Option<InferredNativeVortexRoutePayload> {
    if matches_summary_kinds(operations, &["read_vortex", "filter", "select", "sample"]) {
        let predicate = summary_tiny_predicate_from_sql(operations[1].arg)?;
        let sample_shape = summary_sample_shape(operations[3].arg)?;
        Some(native_vortex_sample_primitive_payload(
            Some(predicate),
            Some(operations[2].arg.trim().to_string()),
            sample_shape,
        ))
    } else if matches_summary_kinds(operations, &["read_vortex", "filter", "sample"]) {
        let predicate = summary_tiny_predicate_from_sql(operations[1].arg)?;
        let sample_shape = summary_sample_shape(operations[2].arg)?;
        Some(native_vortex_sample_primitive_payload(
            Some(predicate),
            None,
            sample_shape,
        ))
    } else if matches_summary_kinds(operations, &["read_vortex", "select", "sample"]) {
        let sample_shape = summary_sample_shape(operations[2].arg)?;
        Some(native_vortex_sample_primitive_payload(
            None,
            Some(operations[1].arg.trim().to_string()),
            sample_shape,
        ))
    } else if matches_summary_kinds(operations, &["read_vortex", "sample"]) {
        let sample_shape = summary_sample_shape(operations[1].arg)?;
        Some(native_vortex_sample_primitive_payload(
            None,
            None,
            sample_shape,
        ))
    } else {
        None
    }
}

fn summary_sample_shape(value: &str) -> Option<SummarySampleShape> {
    let parts = value
        .split(',')
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>();
    if parts.is_empty() {
        return None;
    }
    if parts.first().copied() == Some("fraction") {
        let fraction = *parts.get(1)?;
        let seed = parts.get(2).copied().unwrap_or("0");
        let replacement = matches!(parts.get(3).copied(), Some("replacement" | "replace=true"));
        let valid_replacement_marker = match parts.get(3).copied() {
            None | Some("replacement" | "replace=true") => true,
            Some(_) => false,
        };
        if parts.len() <= if replacement { 4 } else { 3 }
            && sample_fraction_arg("sample fraction", fraction).is_ok()
            && seed.parse::<u64>().is_ok()
            && valid_replacement_marker
        {
            return Some(SummarySampleShape {
                source_order_limit: None,
                sample_fraction: Some(fraction.to_string()),
                sample_seed: seed.to_string(),
                sample_replacement: replacement,
                sample_weight_column: None,
            });
        }
        return None;
    }
    let mut source_order_limit = None;
    let mut sample_fraction = None;
    let mut sample_seed = "0".to_string();
    let mut sample_replacement = false;
    let mut sample_weight_column = None;
    let mut saw_seed = false;
    for (index, part) in parts.iter().enumerate() {
        if let Some(value) = part.strip_prefix("n=") {
            if source_order_limit.is_some() || !summary_positive_limit(value) {
                return None;
            }
            source_order_limit = Some(value.to_string());
        } else if let Some(value) = part
            .strip_prefix("frac=")
            .or_else(|| part.strip_prefix("fraction="))
        {
            if sample_fraction.is_some() || sample_fraction_arg("sample fraction", value).is_err() {
                return None;
            }
            sample_fraction = Some(value.to_string());
        } else if let Some(value) = part
            .strip_prefix("seed=")
            .or_else(|| part.strip_prefix("random_state="))
        {
            if saw_seed || value.parse::<u64>().is_err() {
                return None;
            }
            sample_seed = value.to_string();
            saw_seed = true;
        } else if *part == "replacement" || *part == "replace=true" {
            if sample_replacement {
                return None;
            }
            sample_replacement = true;
        } else if let Some(value) = part
            .strip_prefix("weights=")
            .or_else(|| part.strip_prefix("weight="))
        {
            if sample_weight_column.is_some() || shardloom_core::ColumnRef::new(value).is_err() {
                return None;
            }
            sample_weight_column = Some(value.to_string());
        } else if index == 0 && source_order_limit.is_none() && summary_positive_limit(part) {
            source_order_limit = Some((*part).to_string());
        } else if index == 1 && !saw_seed && part.parse::<u64>().is_ok() {
            sample_seed = (*part).to_string();
            saw_seed = true;
        } else {
            return None;
        }
    }
    if source_order_limit.is_some() == sample_fraction.is_some() {
        None
    } else {
        Some(SummarySampleShape {
            source_order_limit,
            sample_fraction,
            sample_seed,
            sample_replacement,
            sample_weight_column,
        })
    }
}

fn infer_native_vortex_tail_primitive_payload(
    operations: &[SummaryOperation<'_>],
) -> Option<InferredNativeVortexRoutePayload> {
    if matches_summary_kinds(operations, &["read_vortex", "select", "tail"])
        && summary_positive_limit(operations[2].arg)
    {
        Some(native_vortex_primitive_payload(
            PublicVortexPrimitive::Tail,
            None,
            Some(operations[1].arg.trim().to_string()),
            Some(operations[2].arg.trim().to_string()),
        ))
    } else if matches_summary_kinds(operations, &["read_vortex", "tail"])
        && summary_positive_limit(operations[1].arg)
    {
        Some(native_vortex_primitive_payload(
            PublicVortexPrimitive::Tail,
            None,
            None,
            Some(operations[1].arg.trim().to_string()),
        ))
    } else {
        None
    }
}

fn infer_native_vortex_basic_primitive_payload(
    operations: &[SummaryOperation<'_>],
) -> Option<InferredNativeVortexRoutePayload> {
    if matches_summary_kinds(operations, &["read_vortex", "filter", "select", "limit"])
        && summary_positive_limit(operations[3].arg)
    {
        let predicate = summary_tiny_predicate_from_sql(operations[1].arg)?;
        Some(native_vortex_primitive_payload(
            PublicVortexPrimitive::FilterProject,
            Some(predicate),
            Some(operations[2].arg.trim().to_string()),
            Some(operations[3].arg.trim().to_string()),
        ))
    } else if matches_summary_kinds(operations, &["read_vortex", "filter", "select"]) {
        let predicate = summary_tiny_predicate_from_sql(operations[1].arg)?;
        Some(native_vortex_primitive_payload(
            PublicVortexPrimitive::FilterProject,
            Some(predicate),
            Some(operations[2].arg.trim().to_string()),
            None,
        ))
    } else if matches_summary_kinds(operations, &["read_vortex", "filter", "limit"])
        && summary_positive_limit(operations[2].arg)
    {
        let predicate = summary_tiny_predicate_from_sql(operations[1].arg)?;
        Some(native_vortex_primitive_payload(
            PublicVortexPrimitive::Filter,
            Some(predicate),
            None,
            Some(operations[2].arg.trim().to_string()),
        ))
    } else if matches_summary_kinds(operations, &["read_vortex", "filter"]) {
        let predicate = summary_tiny_predicate_from_sql(operations[1].arg)?;
        Some(native_vortex_primitive_payload(
            PublicVortexPrimitive::Filter,
            Some(predicate),
            None,
            None,
        ))
    } else if matches_summary_kinds(operations, &["read_vortex", "select", "limit"])
        && summary_positive_limit(operations[2].arg)
    {
        Some(native_vortex_primitive_payload(
            PublicVortexPrimitive::Project,
            None,
            Some(operations[1].arg.trim().to_string()),
            Some(operations[2].arg.trim().to_string()),
        ))
    } else if matches_summary_kinds(operations, &["read_vortex", "select"]) {
        Some(native_vortex_primitive_payload(
            PublicVortexPrimitive::Project,
            None,
            Some(operations[1].arg.trim().to_string()),
            None,
        ))
    } else {
        None
    }
}

fn quoted_source_refs(value: &str) -> Vec<String> {
    let mut refs = Vec::new();
    let mut chars = value.char_indices().peekable();
    while let Some((_, ch)) = chars.next() {
        if ch != '\'' {
            continue;
        }
        let mut current = String::new();
        while let Some((_, inner)) = chars.next() {
            if inner == '\'' {
                if matches!(chars.peek(), Some((_, '\''))) {
                    current.push('\'');
                    chars.next();
                    continue;
                }
                break;
            }
            current.push(inner);
        }
        if !current.is_empty() {
            refs.push(current);
        }
    }
    refs
}

fn compact_ascii_lower(value: &str) -> String {
    value
        .chars()
        .filter(|ch| !ch.is_whitespace())
        .flat_map(char::to_lowercase)
        .collect()
}

fn sql_without_string_literal_contents(value: &str) -> String {
    let mut masked = String::with_capacity(value.len());
    let mut chars = value.char_indices().peekable();
    while let Some((_, ch)) = chars.next() {
        if ch != '\'' {
            masked.push(ch);
            continue;
        }
        masked.push('\'');
        while let Some((_, inner)) = chars.next() {
            if inner == '\'' {
                if matches!(chars.peek(), Some((_, '\''))) {
                    chars.next();
                    continue;
                }
                masked.push('\'');
                break;
            }
        }
    }
    masked
}

fn summary_tiny_predicate_from_sql(value: &str) -> Option<String> {
    let text = value.trim();
    if text.is_empty() {
        return None;
    }
    let conjuncts = split_sql_conjunction_predicates(text);
    if conjuncts.len() > 1 {
        let predicates = conjuncts
            .into_iter()
            .map(summary_tiny_predicate_from_sql)
            .collect::<Option<Vec<_>>>()?;
        return Some(format!("and({})", predicates.join(";")));
    }
    if is_summary_compact_tiny_predicate(text) {
        return Some(text.to_string());
    }
    if let Some((column, is_not)) = parse_summary_null_predicate(text) {
        return Some(format!(
            "{}:{column}",
            if is_not { "is_not_null" } else { "is_null" }
        ));
    }
    if let Some((column, needle, negated)) = parse_summary_like_predicate(text) {
        return Some(format!(
            "{}:{column}:{needle}",
            if negated { "not_contains" } else { "contains" }
        ));
    }
    if let Some((column, values, negated)) = parse_summary_in_predicate(text) {
        return Some(format!(
            "{}:{column}:{}",
            if negated { "not_in" } else { "in" },
            values.join(",")
        ));
    }
    for (sql_op, tiny_op) in [
        ("!=", "neq"),
        ("<>", "neq"),
        (">=", "gte"),
        ("<=", "lte"),
        (">", "gt"),
        ("<", "lt"),
        ("=", "eq"),
    ] {
        let Some((column, literal)) = text.split_once(sql_op) else {
            continue;
        };
        let column = column.trim();
        let literal = literal.trim();
        if is_summary_identifier(column)
            && let Some(literal) = summary_sql_literal_to_tiny_scalar(literal)
        {
            return Some(format!("{tiny_op}:{column}:{literal}"));
        }
    }
    None
}

fn is_summary_compact_tiny_predicate(value: &str) -> bool {
    if let Some(inner) = value
        .strip_prefix("and(")
        .and_then(|value| value.strip_suffix(')'))
    {
        return inner
            .split(';')
            .filter(|part| !part.trim().is_empty())
            .all(|part| is_summary_compact_tiny_predicate(part.trim()));
    }
    let parts = value.splitn(3, ':').collect::<Vec<_>>();
    match parts.as_slice() {
        ["is_null" | "is_not_null", column] => is_summary_identifier(column),
        [
            "contains" | "string_contains" | "not_contains" | "not_string_contains",
            column,
            needle,
        ] => is_summary_identifier(column) && !needle.is_empty(),
        ["in" | "not_in", column, literals] => {
            is_summary_identifier(column)
                && literals
                    .split(',')
                    .map(str::trim)
                    .any(|literal| !literal.is_empty())
        }
        [
            "eq" | "neq" | "not_eq" | "gt" | "gte" | "lt" | "lte",
            column,
            _literal,
        ] => is_summary_identifier(column),
        _ => false,
    }
}

fn parse_summary_like_predicate(value: &str) -> Option<(&str, String, bool)> {
    for (keyword, negated) in [("NOT LIKE", true), ("LIKE", false)] {
        let Some(position) = find_sql_keyword_outside_quotes_and_parens(value, keyword) else {
            continue;
        };
        let column = value[..position].trim();
        if !is_summary_identifier(column) {
            continue;
        }
        let pattern = parse_sql_single_quoted_literal(&value[position + keyword.len()..])?;
        let needle = sql_like_contains_needle(&pattern)?;
        if !tiny_predicate_preserves_text(&needle) {
            return None;
        }
        return Some((column, needle, negated));
    }
    None
}

fn sql_like_contains_needle(pattern: &str) -> Option<String> {
    if pattern.contains('_') {
        return None;
    }
    let inner = pattern.strip_prefix('%')?.strip_suffix('%')?;
    if inner.is_empty() || inner.contains('%') {
        return None;
    }
    Some(inner.to_string())
}

fn parse_summary_in_predicate(value: &str) -> Option<(&str, Vec<String>, bool)> {
    for (keyword, negated) in [("NOT IN", true), ("IN", false)] {
        let Some(position) = find_sql_keyword_outside_quotes_and_parens(value, keyword) else {
            continue;
        };
        let column = value[..position].trim();
        if !is_summary_identifier(column) {
            continue;
        }
        let tail = value[position + keyword.len()..].trim();
        let inner = tail.strip_prefix('(')?.strip_suffix(')')?;
        let values = split_sql_projection_list(inner)
            .into_iter()
            .map(summary_sql_literal_to_tiny_scalar)
            .collect::<Option<Vec<_>>>()?;
        // The compact IN grammar discards empty entries. Keep quoted empty
        // strings on the ordinary typed SQL binder instead of losing a member.
        if values.is_empty() || values.iter().any(String::is_empty) {
            return None;
        }
        return Some((column, values, negated));
    }
    None
}

fn split_sql_conjunction_predicates(value: &str) -> Vec<&str> {
    let mut out = Vec::new();
    let mut rest = value.trim();
    while let Some(position) = find_sql_keyword_outside_quotes_and_parens(rest, "AND") {
        let head = rest[..position].trim();
        if !head.is_empty() {
            out.push(head);
        }
        rest = rest[position + "AND".len()..].trim();
    }
    if !rest.is_empty() {
        out.push(rest);
    }
    out
}

fn summary_sql_literal_to_tiny_scalar(value: &str) -> Option<String> {
    if value.parse::<i64>().is_ok() {
        return Some(value.to_string());
    }
    let text = parse_sql_single_quoted_literal(value)?;
    // This compact CLI grammar trims predicates, uses unescaped delimiters and
    // infers integers. Only lower SQL strings that survive it without changing
    // their value or type. Other SQL uses the shared typed native binder.
    (tiny_predicate_preserves_text(&text) && text.parse::<i64>().is_err()).then_some(text)
}

fn tiny_predicate_preserves_text(value: &str) -> bool {
    value.trim() == value && !value.contains([',', ';', '|'])
}

fn parse_sql_single_quoted_literal(value: &str) -> Option<String> {
    let value = value.trim();
    if !value.starts_with('\'') || !value.ends_with('\'') || value.len() < 2 {
        return None;
    }
    let inner = &value[1..value.len() - 1];
    let mut out = String::new();
    let mut chars = inner.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '\'' {
            if chars.peek() == Some(&'\'') {
                chars.next();
                out.push('\'');
            } else {
                return None;
            }
        } else {
            out.push(ch);
        }
    }
    Some(out)
}

fn parse_summary_null_predicate(value: &str) -> Option<(&str, bool)> {
    let mut parts = value.split_whitespace();
    let column = parts.next()?;
    if !is_summary_identifier(column) {
        return None;
    }
    let is_token = parts.next()?;
    if !is_token.eq_ignore_ascii_case("is") {
        return None;
    }
    let third = parts.next()?;
    if third.eq_ignore_ascii_case("null") && parts.next().is_none() {
        return Some((column, false));
    }
    if third.eq_ignore_ascii_case("not") {
        let fourth = parts.next()?;
        if fourth.eq_ignore_ascii_case("null") && parts.next().is_none() {
            return Some((column, true));
        }
    }
    None
}

fn is_summary_identifier(value: &str) -> bool {
    let mut chars = value.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    (first == '_' || first.is_ascii_alphabetic())
        && chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric())
}

fn positive_usize_option_error<'a>(flag: &str, value: Option<&str>) -> Option<&'a str> {
    let value = value?;
    match value.parse::<usize>() {
        Ok(parsed) if parsed >= 1 => None,
        Ok(_) => Some(match flag {
            "--max-parallelism" => "max_parallelism must be >= 1",
            "--vortex-source-order-limit" => "source-order limit must be >= 1",
            _ => "value must be >= 1",
        }),
        Err(_) => Some(match flag {
            "--max-parallelism" => "max_parallelism must be an unsigned integer",
            "--vortex-source-order-limit" => "source-order limit must be an unsigned integer",
            _ => "value must be an unsigned integer",
        }),
    }
}

fn is_local_file_format(format: &str) -> bool {
    matches!(
        format,
        "csv" | "tsv" | "json" | "jsonl" | "ndjson" | "parquet" | "arrow-ipc" | "avro" | "orc"
    )
}

fn local_file_route(request: &PublicWorkflowRouteRequest) -> PublicWorkflowRoutePlan {
    if request.execution_policy == "direct" {
        return direct_local_file_route_blocked(request);
    }
    if is_write_request(request)
        && !matches!(
            request.requested_output.as_str(),
            "write_vortex"
                | "write_json"
                | "write_jsonl"
                | "write_csv"
                | "write_parquet"
                | "write_arrow_ipc"
                | "write_avro"
                | "write_orc"
        )
    {
        return local_file_compatibility_sink_contract_missing_route(request);
    }
    if !shardloom_vortex::vortex_ingest_write_feature_enabled() {
        return local_file_vortex_ingest_feature_gated_route(request);
    }
    if request.requested_output != "prepare"
        && request.execution_policy == "vortex_middle"
        && !cfg!(all(feature = "vortex-local-primitives", unix))
    {
        return local_file_vortex_primitive_feature_gated_route(request);
    }
    if request.requested_output == "prepare" {
        admitted_route(
            "local_file_prepare_once",
            "vortex-prepare",
            "compatibility_local_source",
            "VortexPreparedState",
            "prepared_vortex",
            true,
            true,
        )
    } else if request.execution_policy == "prepare_once" {
        let prepared_run = match prepared_local_workflow_native_request(request) {
            Ok(prepared_run) => prepared_run,
            Err(blocked) => return *blocked,
        };
        let native_plan = plan_public_workflow_route(&prepared_run.request);
        if native_plan.status != CommandStatus::Success {
            return native_plan;
        }
        if matches!(
            native_plan.route_id,
            "native_vortex_count_all"
                | "native_vortex_count_where"
                | "native_vortex_filter"
                | "native_vortex_project"
                | "native_vortex_filter_project"
                | "native_vortex_distinct"
                | "native_vortex_drop_duplicates"
                | "native_vortex_duplicate_mask"
                | "native_vortex_tail"
                | "native_vortex_sample"
                | "native_vortex_expression_project"
                | "native_vortex_melt"
                | "native_vortex_explode"
                | "native_vortex_pivot"
                | "native_vortex_rolling_window"
                | "native_vortex_aggregate"
                | "native_vortex_sort_rows"
        ) && !cfg!(feature = "vortex-local-primitives")
        {
            return local_file_vortex_primitive_feature_gated_route(request);
        }
        admitted_route(
            "local_file_prepare_once_first_query",
            "vortex-prepare->native-vortex",
            "compatibility_local_source",
            "VortexPreparedState",
            "prepared_vortex",
            true,
            true,
        )
    } else {
        local_file_vortex_middle_required_route(request)
    }
}

fn direct_local_file_route_blocked(
    request: &PublicWorkflowRouteRequest,
) -> PublicWorkflowRoutePlan {
    blocked_route(
        "cg21.route.direct_local_file_blocked",
        "direct local-file compatibility execution is not admitted as a public workflow route",
        Diagnostic::new(
            DiagnosticCode::InvalidInput,
            DiagnosticSeverity::Error,
            DiagnosticCategory::UnsupportedFeature,
            "public local-file workflows require a Vortex normalization or native Vortex execution layer"
                .to_string(),
            Some("public_workflow_route.execution_policy".to_string()),
            Some(format!(
                "execution_policy=direct input_format={} requested_output={} direct_compatibility_runtime=disabled_for_public_workflows",
                request.input_format.as_deref().unwrap_or("not_declared"),
                request.requested_output
            )),
            Some(
                "use prepare dataframe to create VortexPreparedState, then run an admitted prepared/native Vortex route; direct compatibility remains an internal diagnostic safeguard only"
                    .to_string(),
            ),
            FallbackStatus::disabled_by_policy(),
        ),
    )
}

fn local_file_vortex_primitive_feature_gated_route(
    request: &PublicWorkflowRouteRequest,
) -> PublicWorkflowRoutePlan {
    blocked_route(
        "cg21.route.local_file_vortex_primitive_feature_gated",
        "local-file public workflow requires the vortex-local-primitives feature gate before the prepared Vortex query can run",
        Diagnostic::new(
            DiagnosticCode::NotImplemented,
            DiagnosticSeverity::Error,
            DiagnosticCategory::UnsupportedFeature,
            "public local-file primitive workflows require native Vortex primitive execution, but this binary was compiled without vortex-local-primitives"
                .to_string(),
            Some("public_workflow_route.required_feature_gate".to_string()),
            Some(format!(
                "input_format={} requested_output={} required_feature_gate=vortex-local-primitives",
                request.input_format.as_deref().unwrap_or("not_declared"),
                request.requested_output
            )),
            Some(
                "use a release-user-surfaces build or rebuild shardloom-cli with --features \"vortex-write vortex-local-primitives\""
                    .to_string(),
            ),
            FallbackStatus::disabled_by_policy(),
        ),
    )
}

fn local_file_vortex_middle_required_route(
    request: &PublicWorkflowRouteRequest,
) -> PublicWorkflowRoutePlan {
    blocked_route(
        "cg21.route.local_file_vortex_middle_required",
        "local-file public workflow requires an admitted native declaration",
        Diagnostic::new(
            DiagnosticCode::NotImplemented,
            DiagnosticSeverity::Error,
            DiagnosticCategory::UnsupportedFeature,
            "local-source execution requires complete SQL, a typed plan or an admitted primitive declaration"
                .to_string(),
            Some("public_workflow_route.vortex_normalization_point".to_string()),
            Some(format!(
                "execution_policy={} input_format={} requested_output={} required_execution=shared_native_plan",
                request.execution_policy,
                request.input_format.as_deref().unwrap_or("not_declared"),
                request.requested_output
            )),
            Some(
                "supply a complete native declaration; use prepare explicitly when a durable Vortex copy is wanted"
                    .to_string(),
            ),
            FallbackStatus::disabled_by_policy(),
        )
    )
}

fn local_file_vortex_ingest_feature_gated_route(
    request: &PublicWorkflowRouteRequest,
) -> PublicWorkflowRoutePlan {
    blocked_route(
        "cg21.route.local_file_vortex_ingest_feature_gated",
        "local-file public workflow requires the vortex-write feature gate before Vortex preparation can run",
        Diagnostic::new(
            DiagnosticCode::NotImplemented,
            DiagnosticSeverity::Error,
            DiagnosticCategory::UnsupportedFeature,
            "public local-file workflows require Vortex ingest before native execution, but this binary was compiled without vortex-write"
                .to_string(),
            Some("public_workflow_route.required_feature_gate".to_string()),
            Some(format!(
                "input_format={} requested_output={} required_feature_gate=vortex-write",
                request.input_format.as_deref().unwrap_or("not_declared"),
                request.requested_output
            )),
            Some(
                "use a release-user-surfaces build or rebuild shardloom-cli with --features \"vortex-write vortex-local-primitives\" for local primitive routes"
                    .to_string(),
            ),
            FallbackStatus::disabled_by_policy(),
        ),
    )
}

fn local_file_compatibility_sink_contract_missing_route(
    request: &PublicWorkflowRouteRequest,
) -> PublicWorkflowRoutePlan {
    blocked_route(
        "cg21.route.local_file_compatibility_sink_contract_missing",
        "local-file compatibility sinks require a native Vortex-derived typed sink contract",
        Diagnostic::new(
            DiagnosticCode::NotImplemented,
            DiagnosticSeverity::Error,
            DiagnosticCategory::UnsupportedFeature,
            "public local-file writes cannot execute through direct decoded compatibility sinks"
                .to_string(),
            Some("public_workflow_route.requested_output".to_string()),
            Some(format!(
                "requested_output={} input_format={} direct_compatibility_sink=disabled_for_public_workflows",
                request.requested_output,
                request.input_format.as_deref().unwrap_or("not_declared")
            )),
            Some(
                "use write_vortex for an admitted native Vortex sink shape, or wait for the native compatibility export contract"
                    .to_string(),
            ),
            FallbackStatus::disabled_by_policy(),
        ),
    )
}

fn native_vortex_sink_format_blocked_route(
    request: &PublicWorkflowRouteRequest,
) -> PublicWorkflowRoutePlan {
    blocked_route(
        "py-vortex-route-unify-1.native_vortex_sink_format_missing",
        "native Vortex result export supports Vortex, JSONL, CSV, and scoped structured Vortex/Parquet/Arrow IPC/Avro result sinks in this runtime slice",
        Diagnostic::new(
            DiagnosticCode::NotImplemented,
            DiagnosticSeverity::Error,
            DiagnosticCategory::UnsupportedFeature,
            "native Vortex provider result export does not yet support the requested sink format"
                .to_string(),
            Some("public_workflow_route.requested_output".to_string()),
            Some(format!(
                "requested_output={} admitted_result_sink_formats=vortex,jsonl,csv,parquet,arrow-ipc,avro scoped_structured_export_requires=structured_columns_payload",
                request.requested_output
            )),
            Some(
                "use write_vortex, write_jsonl, write_csv, or a scoped structured Vortex/Parquet/Arrow IPC/Avro expression-project export"
                    .to_string(),
            ),
            FallbackStatus::disabled_by_policy(),
        ),
    )
}

fn native_vortex_structured_sink_payload_blocked_route(
    request: &PublicWorkflowRouteRequest,
) -> PublicWorkflowRoutePlan {
    blocked_route(
        "py-vortex-route-unify-1.native_vortex_structured_sink_payload_missing",
        "native Vortex structured export requires a scoped structured expression-project payload",
        Diagnostic::new(
            DiagnosticCode::NotImplemented,
            DiagnosticSeverity::Error,
            DiagnosticCategory::UnsupportedFeature,
            "this native Vortex export shape requires structured_columns payload evidence for Vortex/Parquet/Arrow IPC/Avro/ORC (subject to target type limits)"
                .to_string(),
            Some("public_workflow_route.vortex_expression_projection".to_string()),
            Some(format!(
                "requested_output={} admitted_structured_sink_formats=vortex,parquet,arrow-ipc,avro,orc required_primitive=expression_project",
                request.requested_output
            )),
            Some(
                "use select(...) with a scoped projection supported by the target format, or use write_json/write_jsonl/write_csv for scalar row streams"
                    .to_string(),
            ),
            FallbackStatus::disabled_by_policy(),
        ),
    )
}

fn native_vortex_row_export_requires_structured_payload(
    request: &PublicWorkflowRouteRequest,
) -> bool {
    if cfg!(all(feature = "vortex-write", unix))
        && !request
            .vortex_expression_projection
            .as_deref()
            .is_some_and(native_vortex_expression_projection_is_structured)
        && normalized_vortex_primitive(request).is_some_and(|primitive| {
            native_vortex_primitive_materializes(primitive)
                || matches!(
                    primitive,
                    PublicVortexPrimitive::Count
                        | PublicVortexPrimitive::CountWhere
                        | PublicVortexPrimitive::Project
                        | PublicVortexPrimitive::Filter
                        | PublicVortexPrimitive::FilterProject
                )
        })
        && (request.requested_output == "write_vortex"
            || (cfg!(feature = "universal-format-io")
                && matches!(
                    request.requested_output.as_str(),
                    "write_parquet" | "write_arrow_ipc" | "write_avro" | "write_orc"
                )))
    {
        // These native producers bind or discover their own result schema.
        // Runtime dtype admission still precedes publication; a structured
        // expression-project payload is not required for an ordinary result.
        return false;
    }
    matches!(
        request.requested_output.as_str(),
        "write_vortex" | "write_parquet" | "write_arrow_ipc" | "write_avro" | "write_orc"
    )
}

fn native_vortex_expression_projection_is_structured(value: &str) -> bool {
    serde_json::from_str::<serde_json::Value>(value).is_ok_and(|payload| {
        payload
            .get("structured_columns")
            .and_then(serde_json::Value::as_array)
            .is_some_and(|columns| !columns.is_empty())
    })
}

fn is_write_request(request: &PublicWorkflowRouteRequest) -> bool {
    matches!(
        request.requested_output.as_str(),
        "write_vortex"
            | "write_parquet"
            | "write_arrow_ipc"
            | "write_avro"
            | "write_orc"
            | "write_csv"
            | "write_json"
            | "write_jsonl"
    )
}

fn native_vortex_payload_blocked_route(
    field: &'static str,
    reason: &'static str,
    remediation: &'static str,
) -> PublicWorkflowRoutePlan {
    blocked_route(
        "cg21.route.native_vortex_payload_invalid",
        reason,
        Diagnostic::new(
            DiagnosticCode::InvalidInput,
            DiagnosticSeverity::Error,
            DiagnosticCategory::InvalidInput,
            reason,
            Some(field.to_string()),
            Some(reason.to_string()),
            Some(remediation.to_string()),
            FallbackStatus::disabled_by_policy(),
        ),
    )
}

fn input_format_not_admitted_route(format: &str) -> PublicWorkflowRoutePlan {
    blocked_route(
        "cg21.route.input_format_not_admitted",
        "input format is not admitted by the public route facade",
        Diagnostic::unsupported(
            DiagnosticCode::UnsupportedEncoding,
            "public_workflow_route.input_format",
            format!("input format {format:?} is not admitted by the public route facade"),
            Some("use csv, jsonl, parquet, arrow-ipc, avro, orc, or vortex".to_string()),
        ),
    )
}

fn input_not_declared_route() -> PublicWorkflowRoutePlan {
    blocked_route(
        "cg21.route.input_not_declared",
        "route requires a declared input or an admitted SQL statement",
        Diagnostic::new(
            DiagnosticCode::InvalidInput,
            DiagnosticSeverity::Error,
            DiagnosticCategory::InvalidInput,
            "public workflow route requires a declared input or an admitted SQL statement",
            Some("public_workflow_route.input".to_string()),
            Some("no input URI or inferable SQL source was provided".to_string()),
            Some(
                "pass --input with --input-format, --source-bindings for native memory inputs, or an admitted source-free SQL statement".to_string(),
            ),
            FallbackStatus::disabled_by_policy(),
        ),
    )
}

fn admitted_route(
    route_id: &'static str,
    resolved_internal_command: &'static str,
    start_state: &'static str,
    vortex_normalization_point: &'static str,
    execution_mode: &'static str,
    preparation_included: bool,
    query_timing_starts_after_preparation: bool,
) -> PublicWorkflowRoutePlan {
    PublicWorkflowRoutePlan {
        status: CommandStatus::Success,
        route_status: "admitted",
        route_id,
        resolved_internal_command,
        start_state,
        vortex_normalization_point,
        execution_mode,
        preparation_included,
        query_timing_starts_after_preparation,
        blocker_id: "none",
        blocker_reason: "none",
        diagnostics: Vec::new(),
    }
}

fn blocked_route(
    blocker_id: &'static str,
    blocker_reason: &'static str,
    diagnostic: Diagnostic,
) -> PublicWorkflowRoutePlan {
    PublicWorkflowRoutePlan {
        status: CommandStatus::Unsupported,
        route_status: "blocked",
        route_id: "blocked",
        resolved_internal_command: "not_resolved",
        start_state: "blocked",
        vortex_normalization_point: "not_applicable",
        execution_mode: "blocked",
        preparation_included: false,
        query_timing_starts_after_preparation: false,
        blocker_id,
        blocker_reason,
        diagnostics: vec![diagnostic],
    }
}

fn route_fields(
    request: &PublicWorkflowRouteRequest,
    plan: &PublicWorkflowRoutePlan,
) -> Vec<(String, String)> {
    let effective_request = effective_public_workflow_request(request);
    let mut fields = Vec::with_capacity(40);
    add_route_identity_fields(&mut fields, &effective_request, plan);
    add_route_request_fields(&mut fields, &effective_request, plan);
    add_route_execution_fields(&mut fields, plan);
    add_route_boundary_fields(&mut fields, plan);
    fields
}

fn add_route_identity_fields(
    fields: &mut Vec<(String, String)>,
    request: &PublicWorkflowRouteRequest,
    plan: &PublicWorkflowRoutePlan,
) {
    push_field(
        fields,
        "public_workflow_route_schema_version",
        ROUTE_SCHEMA_VERSION,
    );
    push_field(fields, "public_workflow_route_report_id", ROUTE_REPORT_ID);
    push_field(fields, "public_workflow_route_docs_ref", ROUTE_DOCS_REF);
    push_field(fields, "route_id", plan.route_id);
    push_field(fields, "route_status", plan.route_status);
    push_field(fields, "route_support_status", route_support_status(plan));
    push_field(
        fields,
        "resolved_internal_command",
        plan.resolved_internal_command,
    );
    push_field(fields, "surface", request.surface.clone());
}

fn add_route_request_fields(
    fields: &mut Vec<(String, String)>,
    request: &PublicWorkflowRouteRequest,
    plan: &PublicWorkflowRoutePlan,
) {
    push_field(
        fields,
        "declared_inputs",
        optional_or_none(request.input_uri.as_ref()),
    );
    push_field(
        fields,
        "primary_input",
        optional_or_none(request.input_uri.as_ref()),
    );
    push_field(
        fields,
        "source_format",
        request
            .input_format
            .clone()
            .unwrap_or_else(|| "not_declared".to_string()),
    );
    push_field(
        fields,
        "sql_statement_present",
        request.sql_statement.is_some().to_string(),
    );
    push_field(
        fields,
        "sql_statement",
        optional_or_none(request.sql_statement.as_ref()),
    );
    push_field(
        fields,
        "plan_summary",
        optional_or_none(request.plan_summary.as_ref()),
    );
    push_field(fields, "requested_output", request.requested_output.clone());
    push_field(
        fields,
        "output_ref",
        optional_or_none(request.output_ref.as_ref()),
    );
    push_field(
        fields,
        "fanout_output_count",
        request.fanout_outputs.len().to_string(),
    );
    push_field(fields, "fanout_outputs", fanout_outputs_field(request));
    push_field(fields, "execution_policy", request.execution_policy.clone());
    push_field(
        fields,
        "materialization_decode_policy",
        request.materialization_policy.clone(),
    );
    push_field(
        fields,
        "evidence_level",
        effective_evidence_level(request, plan),
    );
    push_field(fields, "bounded_request", request.bounded.to_string());
    push_field(
        fields,
        "allow_overwrite",
        request.allow_overwrite.to_string(),
    );
    add_route_native_vortex_request_fields(fields, request, plan);
}

fn add_route_native_vortex_request_fields(
    fields: &mut Vec<(String, String)>,
    request: &PublicWorkflowRouteRequest,
    plan: &PublicWorkflowRoutePlan,
) {
    push_native_vortex_contract_fields(fields, "", request, plan);
    add_route_native_vortex_primitive_input_fields(fields, request);
    add_route_native_vortex_extended_payload_fields(fields, request);
    add_route_native_vortex_resource_fields(fields, request);
}

fn add_route_native_vortex_primitive_input_fields(
    fields: &mut Vec<(String, String)>,
    request: &PublicWorkflowRouteRequest,
) {
    push_field(
        fields,
        "vortex_primitive",
        normalized_vortex_primitive(request).map_or("none", PublicVortexPrimitive::as_str),
    );
    push_field(
        fields,
        "vortex_predicate",
        optional_or_none(request.vortex_predicate.as_ref()),
    );
    push_field(
        fields,
        "vortex_columns",
        optional_or_none(request.vortex_columns.as_ref()),
    );
    push_field(
        fields,
        "vortex_source_order_limit",
        optional_or_none(request.vortex_source_order_limit.as_ref()),
    );
}

fn add_route_native_vortex_extended_payload_fields(
    fields: &mut Vec<(String, String)>,
    request: &PublicWorkflowRouteRequest,
) {
    push_field(
        fields,
        "vortex_sample_seed",
        optional_or_none(request.vortex_sample_seed.as_ref()),
    );
    push_field(
        fields,
        "vortex_sample_fraction",
        optional_or_none(request.vortex_sample_fraction.as_ref()),
    );
    push_field(
        fields,
        "vortex_sample_replacement",
        request.vortex_sample_replacement.to_string(),
    );
    push_field(
        fields,
        "vortex_sample_weight_column",
        optional_or_none(request.vortex_sample_weight_column.as_ref()),
    );
    push_field(
        fields,
        "vortex_duplicate_keep",
        request.vortex_duplicate_keep.as_deref().unwrap_or("first"),
    );
    push_field(
        fields,
        "vortex_deduplicate_key_columns",
        optional_or_none(request.vortex_deduplicate_key_columns.as_ref()),
    );
    push_field(
        fields,
        "vortex_expression_projection_present",
        request.vortex_expression_projection.is_some().to_string(),
    );
    push_field(
        fields,
        "vortex_expression_projection_changed_columns",
        expression_projection_changed_columns(request.vortex_expression_projection.as_ref()),
    );
    push_field(
        fields,
        "vortex_melt_projection_present",
        request.vortex_melt_projection.is_some().to_string(),
    );
    push_field(
        fields,
        "vortex_explode_projection_present",
        request.vortex_explode_projection.is_some().to_string(),
    );
    push_field(
        fields,
        "vortex_pivot_projection_present",
        request.vortex_pivot_projection.is_some().to_string(),
    );
    push_field(
        fields,
        "vortex_rolling_window_present",
        request.vortex_rolling_window.is_some().to_string(),
    );
    push_field(
        fields,
        "vortex_aggregate_present",
        request.vortex_aggregate.is_some().to_string(),
    );
    push_field(
        fields,
        "vortex_sort_rows_present",
        request.vortex_sort_rows.is_some().to_string(),
    );
    push_field(
        fields,
        "vortex_sort_tie_policy",
        sort_rows_tie_policy(request.vortex_sort_rows.as_ref()),
    );
}

fn add_route_native_vortex_resource_fields(
    fields: &mut Vec<(String, String)>,
    request: &PublicWorkflowRouteRequest,
) {
    if let Ok(Some(resources)) = request.resources.optional() {
        crate::execution_resources::append_declaration_fields(fields, resources);
    }
    spill::append_request_fields(fields, request);
    push_field(
        fields,
        "memory_gb",
        public_workflow_effective_memory_gb_label(request),
    );
    push_field(
        fields,
        "max_parallelism",
        public_workflow_effective_max_parallelism_label(request),
    );
    push_field(
        fields,
        "requested_max_parallelism",
        public_workflow_requested_max_parallelism(request),
    );
    push_field(
        fields,
        "dynamic_parallelism_floor_applied",
        public_workflow_dynamic_parallelism_floor_applied(request).to_string(),
    );
}

fn public_workflow_effective_max_parallelism_label(request: &PublicWorkflowRouteRequest) -> String {
    request.resources.optional().map_or_else(
        |_| "invalid".into(),
        |resources| {
            resources.map_or_else(
                || "unavailable".into(),
                |value| value.max_parallelism().to_string(),
            )
        },
    )
}

fn push_native_vortex_contract_fields(
    fields: &mut Vec<(String, String)>,
    prefix: &str,
    request: &PublicWorkflowRouteRequest,
    plan: &PublicWorkflowRoutePlan,
) {
    push_field(
        fields,
        format!("{prefix}native_vortex_user_route_contract_schema_version"),
        NATIVE_VORTEX_USER_ROUTE_CONTRACT_SCHEMA_VERSION,
    );
    push_field(
        fields,
        format!("{prefix}typed_result_sink_contract_schema_version"),
        TYPED_RESULT_SINK_CONTRACT_SCHEMA_VERSION,
    );
    push_native_vortex_plan_contract_fields(fields, prefix, request, plan);
    push_field(
        fields,
        format!("{prefix}native_vortex_operation_family"),
        native_vortex_operation_family_field(request, plan),
    );
    push_field(
        fields,
        format!("{prefix}native_vortex_right_input"),
        optional_or_none(request.native_vortex_right_input.as_ref()),
    );
    push_field(
        fields,
        format!("{prefix}native_vortex_capability_status"),
        native_vortex_capability_status(request, plan),
    );
    push_field(
        fields,
        format!("{prefix}native_vortex_required_feature_gate"),
        native_vortex_required_feature_gate(request, plan),
    );
    push_field(
        fields,
        format!("{prefix}native_vortex_required_evidence"),
        native_vortex_required_evidence(request, plan),
    );
    push_field(
        fields,
        format!("{prefix}native_vortex_next_action"),
        native_vortex_next_action(request, plan),
    );
    push_field(
        fields,
        format!("{prefix}typed_result_contract"),
        typed_result_contract(request, plan),
    );
    push_field(
        fields,
        format!("{prefix}typed_sink_contract"),
        typed_sink_contract(request, plan),
    );
    push_field(
        fields,
        format!("{prefix}decode_materialization_boundary"),
        decode_materialization_boundary(request, plan),
    );
}

fn push_native_vortex_plan_contract_fields(
    fields: &mut Vec<(String, String)>,
    prefix: &str,
    request: &PublicWorkflowRouteRequest,
    plan: &PublicWorkflowRoutePlan,
) {
    push_field(
        fields,
        format!("{prefix}native_vortex_plan_contract_schema_version"),
        NATIVE_VORTEX_PLAN_CONTRACT_SCHEMA_VERSION,
    );
    push_field(
        fields,
        format!("{prefix}native_vortex_plan_contract_status"),
        native_vortex_plan_contract_status(request, plan),
    );
    push_field(
        fields,
        format!("{prefix}native_vortex_plan_route_family"),
        native_vortex_plan_route_family(request, plan),
    );
    push_field(
        fields,
        format!("{prefix}native_vortex_plan_route_alias"),
        native_vortex_plan_route_alias(request, plan),
    );
    push_field(
        fields,
        format!("{prefix}native_vortex_plan_payload_kind"),
        native_vortex_plan_payload_kind(request, plan),
    );
    push_field(
        fields,
        format!("{prefix}native_vortex_plan_operator_capillaries"),
        native_vortex_plan_operator_capillaries(request, plan),
    );
    push_field(
        fields,
        format!("{prefix}native_vortex_plan_source_count"),
        native_vortex_plan_source_count(request, plan).to_string(),
    );
    push_field(
        fields,
        format!("{prefix}native_vortex_plan_result_or_sink_contract"),
        native_vortex_plan_result_or_sink_contract(request, plan),
    );
    push_field(
        fields,
        format!("{prefix}native_vortex_plan_fallback_attempted"),
        "false",
    );
    push_field(
        fields,
        format!("{prefix}native_vortex_plan_external_engine_invoked"),
        "false",
    );
}

fn native_vortex_plan_contract_applies(
    request: &PublicWorkflowRouteRequest,
    plan: &PublicWorkflowRoutePlan,
) -> bool {
    if is_native_vortex_route(request) || native_vortex_family_from_plan_blocker(plan).is_some() {
        return true;
    }
    matches!(
        plan.route_id,
        "local_file_prepare_once"
            | "local_file_prepare_once_first_query"
            | "native_vortex_artifact_prepare"
            | "native_vortex_relational_collect"
            | "native_vortex_relational_write"
    ) || matches!(
        plan.blocker_id,
        "cg21.route.local_file_vortex_ingest_feature_gated"
            | "cg21.route.local_file_vortex_primitive_feature_gated"
    )
}

fn native_vortex_plan_contract_status(
    request: &PublicWorkflowRouteRequest,
    plan: &PublicWorkflowRoutePlan,
) -> &'static str {
    if !native_vortex_plan_contract_applies(request, plan) {
        return "not_applicable";
    }
    if plan.status == CommandStatus::Success && plan.route_status == "admitted" {
        "admitted"
    } else {
        "blocked_before_execution"
    }
}

fn native_vortex_plan_route_family(
    request: &PublicWorkflowRouteRequest,
    plan: &PublicWorkflowRoutePlan,
) -> &'static str {
    if native_vortex_plan_contract_applies(request, plan) {
        "native_vortex_unified_plan"
    } else {
        "not_applicable"
    }
}

fn native_vortex_plan_route_alias(
    request: &PublicWorkflowRouteRequest,
    plan: &PublicWorkflowRoutePlan,
) -> &'static str {
    if native_vortex_plan_contract_applies(request, plan) {
        plan.route_id
    } else {
        "not_applicable"
    }
}

fn native_vortex_plan_payload_kind(
    request: &PublicWorkflowRouteRequest,
    plan: &PublicWorkflowRoutePlan,
) -> &'static str {
    if !native_vortex_plan_contract_applies(request, plan) {
        return "not_applicable";
    }
    match plan.route_id {
        "local_file_prepare_once" | "local_file_prepare_once_first_query" => {
            "prepared_compatibility_source"
        }
        "native_vortex_artifact_prepare" => "native_vortex_artifact_source",
        "native_vortex_user_profile" => "metadata_profile",
        "native_vortex_relational_collect" | "native_vortex_relational_write" => {
            "native_relational_plan"
        }
        "native_vortex_primitive_row_export" => "primitive_row_export",
        "native_vortex_count_all"
        | "native_vortex_count_where"
        | "native_vortex_filter"
        | "native_vortex_project"
        | "native_vortex_filter_project"
        | "native_vortex_distinct"
        | "native_vortex_drop_duplicates"
        | "native_vortex_duplicate_mask"
        | "native_vortex_tail"
        | "native_vortex_sample"
        | "native_vortex_expression_project"
        | "native_vortex_melt"
        | "native_vortex_explode"
        | "native_vortex_pivot"
        | "native_vortex_rolling_window"
        | "native_vortex_aggregate"
        | "native_vortex_sort_rows" => "primitive_operator",
        _ if plan.status != CommandStatus::Success => "blocked_native_vortex_operator",
        _ => "native_vortex_operator",
    }
}

fn native_vortex_plan_operator_capillaries(
    request: &PublicWorkflowRouteRequest,
    plan: &PublicWorkflowRoutePlan,
) -> String {
    if !native_vortex_plan_contract_applies(request, plan) {
        return "not_applicable".to_string();
    }
    let family = native_vortex_operation_family_field(request, plan);
    match native_vortex_plan_payload_kind(request, plan) {
        "prepared_compatibility_source" => {
            "compatibility_import,prepared_vortex_state,native_vortex_query".to_string()
        }
        "metadata_profile" => "metadata_profile,typed_profile_certificate".to_string(),
        "primitive_row_export" => format!("vortex_scan,{family},typed_sink_materialization"),
        "primitive_operator" => format!("vortex_scan,{family},bounded_materialization"),
        "blocked_native_vortex_operator" => {
            format!("admission_diagnostic,{family},no_fallback_boundary")
        }
        _ => format!("vortex_scan,{family},typed_boundary"),
    }
}

fn native_vortex_plan_source_count(
    request: &PublicWorkflowRouteRequest,
    plan: &PublicWorkflowRoutePlan,
) -> usize {
    if !native_vortex_plan_contract_applies(request, plan) {
        return 0;
    }
    #[cfg(all(feature = "vortex-local-primitives", unix))]
    if matches!(
        plan.route_id,
        "native_vortex_relational_collect" | "native_vortex_relational_write"
    ) {
        if uses_primitive_source_demand(request) {
            return usize::from(request.input_uri.is_some());
        }
        return request
            .sql_statement
            .as_deref()
            .and_then(|statement| {
                sql_local_source_runtime::native_relational::source_count(statement).ok()
            })
            .unwrap_or(0);
    }
    if request.native_vortex_right_input.is_some() {
        2
    } else {
        1
    }
}

fn native_vortex_plan_result_or_sink_contract(
    request: &PublicWorkflowRouteRequest,
    plan: &PublicWorkflowRoutePlan,
) -> &'static str {
    if !native_vortex_plan_contract_applies(request, plan) {
        return "not_applicable";
    }
    if is_write_request(request) {
        typed_sink_contract(request, plan)
    } else {
        typed_result_contract(request, plan)
    }
}

fn native_vortex_operation_family_field(
    request: &PublicWorkflowRouteRequest,
    plan: &PublicWorkflowRoutePlan,
) -> &'static str {
    if matches!(
        plan.route_id,
        "native_vortex_relational_collect" | "native_vortex_relational_write"
    ) {
        return NativeVortexOperationFamily::GeneralQuery.as_str();
    }
    if let Some(family) = native_vortex_family_from_plan_blocker(plan) {
        return family.as_str();
    }
    if matches!(
        plan.blocker_id,
        "py-vortex-route-unify-1.native_vortex_sink_contract_missing"
    ) || (is_native_vortex_route(request) && is_write_request(request))
    {
        return NativeVortexOperationFamily::Sink.as_str();
    }
    if plan.route_id == "native_vortex_user_profile" || request.requested_output == "profile" {
        return NativeVortexOperationFamily::Profile.as_str();
    }
    if let Ok(Some(family)) = normalized_native_vortex_operation_family(request) {
        return family.as_str();
    }
    if let Some(primitive) = normalized_vortex_primitive(request) {
        return NativeVortexOperationFamily::from_primitive(primitive).as_str();
    }
    if is_native_vortex_route(request) {
        return NativeVortexOperationFamily::GeneralQuery.as_str();
    }
    "not_applicable"
}

fn native_vortex_family_from_plan_blocker(
    plan: &PublicWorkflowRoutePlan,
) -> Option<NativeVortexOperationFamily> {
    match plan.blocker_id {
        "py-vortex-route-unify-1.native_vortex_aggregate_route_missing" => {
            Some(NativeVortexOperationFamily::Aggregate)
        }
        "py-vortex-route-unify-1.native_vortex_join_state_missing" => {
            Some(NativeVortexOperationFamily::Join)
        }
        "py-vortex-route-unify-1.native_vortex_top_n_route_missing" => {
            Some(NativeVortexOperationFamily::TopN)
        }
        "py-vortex-route-unify-1.native_vortex_cast_route_missing" => {
            Some(NativeVortexOperationFamily::Cast)
        }
        "py-vortex-route-unify-1.native_vortex_contains_route_missing" => {
            Some(NativeVortexOperationFamily::Contains)
        }
        "py-vortex-route-unify-1.native_vortex_distinct_route_missing" => {
            Some(NativeVortexOperationFamily::Distinct)
        }
        "py-vortex-route-unify-1.native_vortex_duplicate_mask_route_missing" => {
            Some(NativeVortexOperationFamily::DuplicateMask)
        }
        "py-vortex-route-unify-1.native_vortex_sample_route_missing" => {
            Some(NativeVortexOperationFamily::Sample)
        }
        "py-vortex-route-unify-1.native_vortex_expression_project_route_missing" => {
            Some(NativeVortexOperationFamily::ExpressionProject)
        }
        "py-vortex-route-unify-1.native_vortex_melt_route_missing" => {
            Some(NativeVortexOperationFamily::Melt)
        }
        "py-vortex-route-unify-1.native_vortex_explode_route_missing" => {
            Some(NativeVortexOperationFamily::Explode)
        }
        "py-vortex-route-unify-1.native_vortex_pivot_route_missing" => {
            Some(NativeVortexOperationFamily::Pivot)
        }
        "py-vortex-route-unify-1.native_vortex_rolling_window_route_missing" => {
            Some(NativeVortexOperationFamily::RollingWindow)
        }
        "py-vortex-route-unify-1.native_vortex_profile_route_missing" => {
            Some(NativeVortexOperationFamily::Profile)
        }
        "py-vortex-route-unify-1.native_vortex_sink_contract_missing"
        | "py-vortex-route-unify-1.native_vortex_sink_format_missing" => {
            Some(NativeVortexOperationFamily::Sink)
        }
        _ => None,
    }
}

fn native_vortex_required_feature_gate(
    request: &PublicWorkflowRouteRequest,
    plan: &PublicWorkflowRoutePlan,
) -> &'static str {
    if plan.preparation_included
        && matches!(
            plan.route_id,
            "native_vortex_relational_collect" | "native_vortex_relational_write"
        )
    {
        return "vortex-local-primitives,vortex-write,universal-format-io";
    }
    if plan.route_id == "native_vortex_relational_collect" {
        return "vortex-local-primitives";
    }
    if plan.route_id == "native_vortex_relational_write" {
        return match request.requested_output.as_str() {
            "write_parquet" | "write_arrow_ipc" | "write_avro" | "write_orc" => {
                "vortex-local-primitives,vortex-write,universal-format-io"
            }
            _ => "vortex-local-primitives,vortex-write",
        };
    }
    if !is_native_vortex_route(request) {
        return "not_applicable";
    }
    if plan.route_id == "native_vortex_primitive_row_export"
        && request.requested_output == "write_vortex"
        && !native_vortex_row_export_requires_structured_payload(request)
    {
        return "vortex-local-primitives,vortex-write";
    }
    if matches!(plan.route_id, "native_vortex_primitive_row_export")
        && native_vortex_row_export_requires_structured_payload(request)
    {
        if request.requested_output == "write_vortex" {
            return "vortex-local-primitives,vortex-write,universal-format-io";
        }
        return "vortex-local-primitives,universal-format-io";
    }
    if plan.route_id == "native_vortex_user_profile" {
        return "default";
    }
    if matches!(plan.route_id, "native_vortex_primitive_row_export")
        || plan.blocker_id
            == "py-vortex-route-unify-1.native_vortex_primitive_row_export_feature_gated"
        || plan.blocker_id
            == "py-vortex-route-unify-1.native_vortex_materializing_primitive_feature_gated"
        || normalized_vortex_primitive(request)
            .is_some_and(PublicVortexPrimitive::requires_local_primitives_feature)
    {
        return "vortex-local-primitives";
    }
    if matches!(
        plan.route_id,
        "native_vortex_count_all"
            | "native_vortex_count_where"
            | "native_vortex_filter"
            | "native_vortex_project"
            | "native_vortex_filter_project"
            | "native_vortex_distinct"
            | "native_vortex_drop_duplicates"
            | "native_vortex_duplicate_mask"
            | "native_vortex_tail"
            | "native_vortex_sample"
            | "native_vortex_expression_project"
            | "native_vortex_melt"
            | "native_vortex_explode"
            | "native_vortex_pivot"
            | "native_vortex_rolling_window"
    ) {
        return "default";
    }
    "not_applicable"
}

fn native_vortex_capability_status(
    request: &PublicWorkflowRouteRequest,
    plan: &PublicWorkflowRoutePlan,
) -> &'static str {
    if plan.status == CommandStatus::Success {
        match plan.route_id {
            "native_vortex_filter"
            | "native_vortex_project"
            | "native_vortex_filter_project"
            | "native_vortex_distinct"
            | "native_vortex_drop_duplicates"
            | "native_vortex_duplicate_mask"
            | "native_vortex_tail"
            | "native_vortex_sample"
            | "native_vortex_expression_project"
            | "native_vortex_melt"
            | "native_vortex_explode"
            | "native_vortex_pivot"
            | "native_vortex_rolling_window"
            | "native_vortex_aggregate"
            | "native_vortex_relational_collect"
            | "native_vortex_sort_rows" => "supported_with_materialization_boundary",
            "native_vortex_relational_write" | "native_vortex_primitive_row_export" => {
                "supported_with_explicit_decode_sink_boundary"
            }
            _ => "supported",
        }
    } else if matches!(
        plan.blocker_id,
        "py-vortex-route-unify-1.native_vortex_primitive_row_export_feature_gated"
            | "py-vortex-route-unify-1.native_vortex_materializing_primitive_feature_gated"
    ) {
        "feature_gated"
    } else if plan.blocker_id.starts_with("py-vortex-route-unify-1.") {
        "blocked_until_native_route_admitted"
    } else if !is_native_vortex_route(request) {
        "not_applicable"
    } else {
        "blocked_by_public_route_contract"
    }
}

fn native_vortex_required_evidence(
    request: &PublicWorkflowRouteRequest,
    plan: &PublicWorkflowRoutePlan,
) -> &'static str {
    if matches!(
        plan.route_id,
        "native_vortex_relational_collect" | "native_vortex_relational_write"
    ) {
        return "bound_native_relational_plan,all_source_generations,complete_typed_result,resource_admission,explicit_sink_boundary,no_fallback";
    }
    if let Some(family) = native_vortex_family_from_plan_blocker(plan) {
        return family.required_evidence();
    }
    if plan.route_id == "native_vortex_user_profile" || request.requested_output == "profile" {
        return NativeVortexOperationFamily::Profile.required_evidence();
    }
    if let Ok(Some(family)) = normalized_native_vortex_operation_family(request) {
        return family.required_evidence();
    }
    if let Some(primitive) = normalized_vortex_primitive(request) {
        return NativeVortexOperationFamily::from_primitive(primitive).required_evidence();
    }
    if !is_native_vortex_route(request) {
        return "not_applicable";
    }
    NativeVortexOperationFamily::GeneralQuery.required_evidence()
}

fn native_vortex_next_action(
    request: &PublicWorkflowRouteRequest,
    plan: &PublicWorkflowRoutePlan,
) -> &'static str {
    if let Some(next_action) = admitted_native_vortex_next_action(request, plan) {
        return next_action;
    }
    if let Some(family) = native_vortex_family_from_plan_blocker(plan) {
        return family.next_action();
    }
    if plan.route_id == "native_vortex_user_profile" || request.requested_output == "profile" {
        return NativeVortexOperationFamily::Profile.next_action();
    }
    if let Ok(Some(family)) = normalized_native_vortex_operation_family(request) {
        return family.next_action();
    }
    if let Some(primitive) = normalized_vortex_primitive(request) {
        return NativeVortexOperationFamily::from_primitive(primitive).next_action();
    }
    if !is_native_vortex_route(request) {
        return "not_applicable";
    }
    NativeVortexOperationFamily::GeneralQuery.next_action()
}

fn admitted_native_vortex_next_action(
    request: &PublicWorkflowRouteRequest,
    plan: &PublicWorkflowRoutePlan,
) -> Option<&'static str> {
    if plan.status != CommandStatus::Success || plan.route_status != "admitted" {
        return None;
    }
    if matches!(
        plan.route_id,
        "native_vortex_relational_collect" | "native_vortex_relational_write"
    ) {
        return Some(
            "execute the native relational plan; preparation validates every source schema, type and resource requirement",
        );
    }
    if plan.route_id == "native_vortex_primitive_row_export" {
        return Some(
            "execute the admitted native Vortex primitive row-export route for JSONL/CSV or admitted fanout sinks",
        );
    }
    if plan.route_id == "native_vortex_user_profile" {
        return Some(
            "execute the admitted metadata-first native Vortex profile route; broader profile options remain blocked",
        );
    }
    let primitive = normalized_vortex_primitive(request)?;
    Some(match primitive {
        PublicVortexPrimitive::Count | PublicVortexPrimitive::CountWhere => {
            "execute the admitted native Vortex count primitive route"
        }
        PublicVortexPrimitive::Filter
        | PublicVortexPrimitive::Project
        | PublicVortexPrimitive::FilterProject => {
            "execute the admitted native Vortex filter/project primitive route"
        }
        PublicVortexPrimitive::Distinct => {
            "execute the admitted native Vortex row-level distinct primitive route with explicit materialization evidence"
        }
        PublicVortexPrimitive::DropDuplicates => {
            "execute the admitted native Vortex retained-row drop_duplicates primitive route with explicit row-key retention evidence"
        }
        PublicVortexPrimitive::DuplicateMask => {
            "execute the admitted native Vortex duplicate-mask primitive route with explicit row-key state evidence"
        }
        PublicVortexPrimitive::Tail => {
            "execute the admitted native Vortex bounded source-order tail primitive route"
        }
        PublicVortexPrimitive::Sample => {
            "execute the admitted native Vortex deterministic sample primitive route with a declared seed and row-count or fractional replacement policy"
        }
        PublicVortexPrimitive::ExpressionProject => {
            "execute the admitted native Vortex expression-project primitive route with explicit rewrite materialization evidence"
        }
        PublicVortexPrimitive::Melt => {
            "execute the admitted native Vortex melt primitive route with explicit row-expansion materialization evidence"
        }
        PublicVortexPrimitive::Explode => {
            "execute the admitted native Vortex explode primitive route with explicit list row-expansion materialization evidence"
        }
        PublicVortexPrimitive::Pivot => {
            "execute the admitted native Vortex pivot primitive route with explicit wide-reshape materialization evidence"
        }
        PublicVortexPrimitive::RollingWindow => {
            "execute the admitted native Vortex rolling-window primitive route with explicit source-order window-state materialization evidence"
        }
        PublicVortexPrimitive::Aggregate => {
            "execute the admitted native Vortex scalar aggregate primitive route with explicit aggregate-state materialization evidence"
        }
        PublicVortexPrimitive::SortRows => {
            "execute the admitted native Vortex bounded sort/top-N primitive route with explicit order-state materialization evidence"
        }
    })
}

fn typed_result_contract(
    _request: &PublicWorkflowRouteRequest,
    plan: &PublicWorkflowRoutePlan,
) -> &'static str {
    if plan.status != CommandStatus::Success {
        return "none_blocked_before_execution";
    }
    match plan.route_id {
        "native_vortex_count_all" | "native_vortex_count_where" => {
            "bounded_python_scalar_summary_with_native_vortex_evidence"
        }
        "native_vortex_filter"
        | "native_vortex_project"
        | "native_vortex_filter_project"
        | "native_vortex_distinct"
        | "native_vortex_drop_duplicates"
        | "native_vortex_duplicate_mask"
        | "native_vortex_tail"
        | "native_vortex_sample"
        | "native_vortex_expression_project"
        | "native_vortex_melt"
        | "native_vortex_explode"
        | "native_vortex_pivot"
        | "native_vortex_rolling_window"
        | "native_vortex_aggregate"
        | "native_vortex_relational_collect"
        | "native_vortex_sort_rows" => "bounded_python_rows_with_explicit_materialization_boundary",
        "native_vortex_relational_write" => {
            "native_relational_batches_with_explicit_sink_materialization"
        }
        "native_vortex_user_profile" => "metadata_first_native_vortex_profile_summary",
        "native_vortex_primitive_row_export" => {
            "native_vortex_primitive_row_stream_with_explicit_sink_materialization"
        }
        _ => "route_metadata_only",
    }
}

fn typed_sink_contract(
    request: &PublicWorkflowRouteRequest,
    plan: &PublicWorkflowRoutePlan,
) -> &'static str {
    if plan.status != CommandStatus::Success
        && matches!(
            plan.blocker_id,
            "py-vortex-route-unify-1.native_vortex_sink_contract_missing"
        )
    {
        return "blocked_until_native_vortex_typed_sink_contract";
    }
    if !is_write_request(request) {
        return "not_applicable_collect";
    }
    match plan.route_id {
        "native_vortex_relational_write" => match request.requested_output.as_str() {
            "write_vortex" => "native_vortex_array_stream_to_vortex_sink",
            "write_parquet" => "native_vortex_arrays_to_parquet_compatibility_sink",
            "write_arrow_ipc" => "native_vortex_arrays_to_arrow_ipc_compatibility_sink",
            "write_avro" => "native_vortex_arrays_to_avro_compatibility_sink",
            "write_orc" => "native_vortex_result_to_checked_orc_compatibility_sink",
            _ => "native_relational_batches_to_text_compatibility_sink",
        },
        "native_vortex_primitive_row_export" if request.requested_output == "write_orc" => {
            "native_vortex_result_to_checked_orc_compatibility_sink"
        }
        "native_vortex_primitive_row_export" if request.requested_output == "write_json" => {
            "native_vortex_row_stream_to_json_array_compatibility_sink"
        }
        "native_vortex_primitive_row_export"
            if native_vortex_row_export_requires_structured_payload(request) =>
        {
            if request.requested_output == "write_vortex" {
                "native_vortex_structured_row_stream_to_vortex_sink"
            } else {
                "native_vortex_structured_row_stream_to_parquet_arrow_avro_compatibility_sink"
            }
        }
        "native_vortex_primitive_row_export" if request.requested_output == "write_vortex" => {
            "native_vortex_array_stream_to_vortex_sink"
        }
        "native_vortex_primitive_row_export" if request.requested_output == "write_parquet" => {
            "native_vortex_arrays_to_parquet_compatibility_sink"
        }
        "native_vortex_primitive_row_export" if request.requested_output == "write_arrow_ipc" => {
            "native_vortex_arrays_to_arrow_ipc_compatibility_sink"
        }
        "native_vortex_primitive_row_export" if request.requested_output == "write_avro" => {
            "native_vortex_arrays_to_avro_compatibility_sink"
        }
        "native_vortex_primitive_row_export" => {
            "native_vortex_primitive_row_stream_to_jsonl_csv_compatibility_sink"
        }
        _ => "not_admitted",
    }
}

fn decode_materialization_boundary(
    request: &PublicWorkflowRouteRequest,
    plan: &PublicWorkflowRoutePlan,
) -> &'static str {
    if plan.status != CommandStatus::Success {
        return "not_executed";
    }
    if plan.route_id == "native_vortex_user_profile" {
        return "metadata_only_no_decode_materialization";
    }
    if matches!(
        plan.route_id,
        "native_vortex_relational_collect" | "native_vortex_relational_write"
    ) {
        return "native_relational_key_execution_and_bounded_result_materialization;provider_decode_bytes_unobserved";
    }
    if plan.route_id == "native_vortex_primitive_row_export" {
        return "native_vortex_scan_pushdown_then_selected_column_decode_at_compatibility_sink";
    }
    if is_native_vortex_route(request) {
        return "native_vortex_zero_decode_runtime_with_bounded_python_materialization_boundary";
    }
    match plan.route_id {
        "local_file_prepare_once"
        | "local_file_prepare_once_first_query"
        | "native_vortex_artifact_prepare" => "prepared_vortex_state_boundary",
        _ => "not_applicable",
    }
}

fn add_route_execution_fields(fields: &mut Vec<(String, String)>, plan: &PublicWorkflowRoutePlan) {
    push_field(fields, "start_state", plan.start_state);
    push_field(
        fields,
        "vortex_normalization_point",
        plan.vortex_normalization_point,
    );
    push_field(fields, "vortex_middle_status", vortex_middle_status(plan));
    push_field(
        fields,
        "underlying_runtime_command",
        underlying_runtime_command(plan),
    );
    push_field(
        fields,
        "local_workflow_runtime_profile",
        local_workflow_runtime_profile(plan),
    );
    push_field(fields, "execution_mode", plan.execution_mode);
    push_field(
        fields,
        "preparation_included",
        plan.preparation_included.to_string(),
    );
    push_field(
        fields,
        "query_timing_starts_after_preparation",
        plan.query_timing_starts_after_preparation.to_string(),
    );
    for (key, value) in [
        ("route_runtime_status", route_runtime_status(plan)),
        ("fallback_attempted", "false"),
        ("external_engine_invoked", "false"),
        ("runtime_execution", "false"),
        ("route_side_effect_free", "true"),
        ("side_effect_free", "true"),
        ("source_io_performed", "false"),
        ("output_io_performed", "false"),
        ("execution", "not_performed"),
        ("plan_only", "true"),
    ] {
        push_field(fields, key, value);
    }
}

fn effective_evidence_level<'a>(
    request: &'a PublicWorkflowRouteRequest,
    _plan: &PublicWorkflowRoutePlan,
) -> &'a str {
    request.evidence_level.as_str()
}

fn add_route_boundary_fields(fields: &mut Vec<(String, String)>, plan: &PublicWorkflowRoutePlan) {
    push_field(fields, "blocker_id", plan.blocker_id);
    push_field(fields, "blocker_reason", plan.blocker_reason);
    push_field(fields, "claim_boundary", CLAIM_BOUNDARY);
    push_field(fields, "fallback_boundary", FALLBACK_BOUNDARY);
    push_field(fields, "claim_gate_status", "route_inspection_only");
}

fn route_support_status(plan: &PublicWorkflowRoutePlan) -> &'static str {
    match plan.route_id {
        "native_vortex_distinct"
        | "native_vortex_drop_duplicates"
        | "native_vortex_tail"
        | "native_vortex_sample"
        | "native_vortex_expression_project"
        | "native_vortex_melt"
        | "native_vortex_explode"
        | "native_vortex_pivot"
        | "native_vortex_rolling_window"
        | "native_vortex_aggregate"
        | "native_vortex_sort_rows"
        | "native_vortex_user_profile"
        | "native_vortex_duplicate_mask"
        | "native_vortex_relational_collect"
        | "native_vortex_relational_write"
        | "native_vortex_primitive_row_export" => "production_admitted_local_workflow",
        "local_file_prepare_once"
        | "local_file_prepare_once_first_query"
        | "native_vortex_artifact_prepare"
        | "native_vortex_count_all"
        | "native_vortex_count_where"
        | "native_vortex_filter"
        | "native_vortex_project"
        | "native_vortex_filter_project" => "global_runtime_supported",
        _ => "unsupported_boundary",
    }
}

fn route_runtime_status(plan: &PublicWorkflowRoutePlan) -> &'static str {
    if plan.status == CommandStatus::Success {
        route_support_status(plan)
    } else {
        "blocked_before_execution"
    }
}

fn vortex_middle_status(plan: &PublicWorkflowRoutePlan) -> &'static str {
    match plan.route_id {
        "native_vortex_relational_collect" | "native_vortex_relational_write" => {
            "native_vortex_relational_plan"
        }
        "local_file_prepare_once"
        | "local_file_prepare_once_first_query"
        | "native_vortex_artifact_prepare" => "prepared_vortex_state",
        "native_vortex_count_all"
        | "native_vortex_count_where"
        | "native_vortex_filter"
        | "native_vortex_project"
        | "native_vortex_filter_project"
        | "native_vortex_distinct"
        | "native_vortex_duplicate_mask"
        | "native_vortex_tail"
        | "native_vortex_sample"
        | "native_vortex_expression_project"
        | "native_vortex_melt"
        | "native_vortex_explode"
        | "native_vortex_pivot"
        | "native_vortex_rolling_window"
        | "native_vortex_aggregate"
        | "native_vortex_sort_rows"
        | "native_vortex_primitive_row_export" => "native_vortex_primitive",
        "native_vortex_user_profile" => "native_vortex_metadata_profile",
        _ => "blocked_or_unsupported",
    }
}

fn underlying_runtime_command(plan: &PublicWorkflowRoutePlan) -> &'static str {
    plan.resolved_internal_command
}

fn local_workflow_runtime_profile(plan: &PublicWorkflowRoutePlan) -> &'static str {
    match plan.route_id {
        "local_file_prepare_once"
        | "local_file_prepare_once_first_query"
        | "native_vortex_artifact_prepare" => "product_local_workflow",
        _ => "not_applicable",
    }
}

fn push_field(
    fields: &mut Vec<(String, String)>,
    key: impl Into<String>,
    value: impl Into<String>,
) {
    fields.push((key.into(), value.into()));
}

fn push_bool_field(fields: &mut Vec<(String, String)>, key: impl Into<String>, value: bool) {
    push_field(fields, key, value.to_string());
}

fn optional_or_none(value: Option<&String>) -> String {
    value.cloned().unwrap_or_else(|| "none".to_string())
}

fn expression_projection_changed_columns(value: Option<&String>) -> String {
    let Some(value) = value else {
        return "none".to_string();
    };
    let Ok(payload) = serde_json::from_str::<serde_json::Value>(value) else {
        return "invalid".to_string();
    };
    let Some(rewrites) = payload
        .get("rewrites")
        .and_then(serde_json::Value::as_array)
    else {
        return "none".to_string();
    };
    let mut columns = std::collections::BTreeSet::new();
    for rewrite in rewrites {
        let Some(object) = rewrite.as_object() else {
            continue;
        };
        let column = object
            .get("target_column")
            .or_else(|| object.get("target"))
            .and_then(serde_json::Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty());
        if let Some(column) = column {
            columns.insert(column.to_string());
        }
    }
    if columns.is_empty() {
        "none".to_string()
    } else {
        columns.into_iter().collect::<Vec<_>>().join(",")
    }
}

fn sort_rows_tie_policy(value: Option<&String>) -> String {
    let Some(value) = value else {
        return "none".to_string();
    };
    let Ok(payload) = serde_json::from_str::<serde_json::Value>(value) else {
        return "invalid".to_string();
    };
    payload
        .get("tie_policy")
        .or_else(|| payload.get("keep"))
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("first")
        .to_string()
}

fn fanout_outputs_field(request: &PublicWorkflowRouteRequest) -> String {
    if request.fanout_outputs.is_empty() {
        "none".to_string()
    } else {
        request.fanout_outputs.join(";")
    }
}

fn emit_blocked_facade(
    command: &'static str,
    format: OutputFormat,
    request: &PublicWorkflowRouteRequest,
    plan: &PublicWorkflowRoutePlan,
) -> ExitCode {
    emit(
        command,
        format,
        CommandStatus::Unsupported,
        format!("public workflow {command} blocked before execution"),
        String::new(),
        plan.diagnostics.clone(),
        blocked_facade_fields(command, request, plan),
    );
    ExitCode::from(1)
}

fn blocked_facade_fields(
    command: &str,
    request: &PublicWorkflowRouteRequest,
    plan: &PublicWorkflowRoutePlan,
) -> Vec<(String, String)> {
    let mut fields = execution_attachment_fields(command, request, plan);
    fields.extend([
        ("runtime_execution".to_string(), "false".to_string()),
        ("source_io_performed".to_string(), "false".to_string()),
        ("output_io_performed".to_string(), "false".to_string()),
        (
            "execution".to_string(),
            "blocked_before_execution".to_string(),
        ),
        ("fallback_attempted".to_string(), "false".to_string()),
        ("external_engine_invoked".to_string(), "false".to_string()),
        (
            "claim_gate_status".to_string(),
            "not_claim_grade".to_string(),
        ),
    ]);
    fields
}

#[allow(clippy::too_many_lines)]
fn execution_attachment_fields(
    command: &str,
    request: &PublicWorkflowRouteRequest,
    plan: &PublicWorkflowRoutePlan,
) -> Vec<(String, String)> {
    let effective_request = effective_public_workflow_request(request);
    let mut fields = vec![
        (
            "public_workflow_facade_schema_version".to_string(),
            FACADE_SCHEMA_VERSION.to_string(),
        ),
        (
            "public_workflow_route_schema_version".to_string(),
            ROUTE_SCHEMA_VERSION.to_string(),
        ),
        (
            "public_workflow_route_report_id".to_string(),
            ROUTE_REPORT_ID.to_string(),
        ),
        (
            "public_workflow_route_docs_ref".to_string(),
            ROUTE_DOCS_REF.to_string(),
        ),
        (
            "public_workflow_facade_command".to_string(),
            command.to_string(),
        ),
        (
            "public_workflow_route_attached".to_string(),
            "true".to_string(),
        ),
        (
            "public_workflow_surface".to_string(),
            effective_request.surface.clone(),
        ),
        (
            "public_workflow_route_id".to_string(),
            plan.route_id.to_string(),
        ),
        (
            "public_workflow_route_status".to_string(),
            plan.route_status.to_string(),
        ),
        (
            "public_workflow_route_support_status".to_string(),
            route_support_status(plan).to_string(),
        ),
        (
            "public_workflow_route_runtime_status".to_string(),
            route_runtime_status(plan).to_string(),
        ),
        (
            "public_workflow_resolved_internal_command".to_string(),
            plan.resolved_internal_command.to_string(),
        ),
        (
            "public_workflow_source_format".to_string(),
            effective_request
                .input_format
                .clone()
                .unwrap_or_else(|| "not_declared".to_string()),
        ),
        (
            "public_workflow_start_state".to_string(),
            plan.start_state.to_string(),
        ),
        (
            "public_workflow_vortex_normalization_point".to_string(),
            plan.vortex_normalization_point.to_string(),
        ),
        (
            "public_workflow_vortex_middle_status".to_string(),
            vortex_middle_status(plan).to_string(),
        ),
        (
            "public_workflow_underlying_runtime_command".to_string(),
            underlying_runtime_command(plan).to_string(),
        ),
        (
            "public_workflow_local_workflow_runtime_profile".to_string(),
            local_workflow_runtime_profile(plan).to_string(),
        ),
        (
            "public_workflow_execution_mode".to_string(),
            plan.execution_mode.to_string(),
        ),
        (
            "public_workflow_preparation_included".to_string(),
            plan.preparation_included.to_string(),
        ),
        (
            "public_workflow_query_timing_starts_after_preparation".to_string(),
            plan.query_timing_starts_after_preparation.to_string(),
        ),
        (
            "public_workflow_requested_output".to_string(),
            effective_request.requested_output.clone(),
        ),
        (
            "public_workflow_output_ref".to_string(),
            optional_or_none(effective_request.output_ref.as_ref()),
        ),
        (
            "public_workflow_fanout_output_count".to_string(),
            effective_request.fanout_outputs.len().to_string(),
        ),
        (
            "public_workflow_fanout_outputs".to_string(),
            fanout_outputs_field(&effective_request),
        ),
        (
            "public_workflow_evidence_level".to_string(),
            effective_evidence_level(&effective_request, plan).to_string(),
        ),
        (
            "public_workflow_timing_surface".to_string(),
            public_workflow_timing_surface(command, plan).to_string(),
        ),
        (
            "public_workflow_actual_evidence_tier".to_string(),
            public_workflow_actual_evidence_tier(command, plan).to_string(),
        ),
        (
            "public_workflow_route_total_timing_reported".to_string(),
            "false".to_string(),
        ),
        (
            "public_workflow_result_sink_timing_included_in_route_total".to_string(),
            "false".to_string(),
        ),
        (
            "public_workflow_evidence_render_timing_included_in_route_total".to_string(),
            "false".to_string(),
        ),
        (
            "public_workflow_timing_claim_boundary".to_string(),
            public_workflow_timing_claim_boundary(command, plan).to_string(),
        ),
        (
            "public_workflow_bounded_request".to_string(),
            effective_request.bounded.to_string(),
        ),
        (
            "public_workflow_allow_overwrite".to_string(),
            effective_request.allow_overwrite.to_string(),
        ),
        (
            "public_workflow_source_fingerprint_policy".to_string(),
            effective_request
                .source_fingerprint_policy
                .clone()
                .unwrap_or_else(|| "metadata_only".to_string()),
        ),
        (
            "public_workflow_vortex_primitive".to_string(),
            normalized_vortex_primitive(&effective_request)
                .map_or("none", PublicVortexPrimitive::as_str)
                .to_string(),
        ),
        (
            "public_workflow_vortex_predicate".to_string(),
            optional_or_none(effective_request.vortex_predicate.as_ref()),
        ),
        (
            "public_workflow_vortex_columns".to_string(),
            optional_or_none(effective_request.vortex_columns.as_ref()),
        ),
        (
            "public_workflow_vortex_source_order_limit".to_string(),
            optional_or_none(effective_request.vortex_source_order_limit.as_ref()),
        ),
        (
            "public_workflow_vortex_sample_seed".to_string(),
            optional_or_none(effective_request.vortex_sample_seed.as_ref()),
        ),
        (
            "public_workflow_vortex_sample_fraction".to_string(),
            optional_or_none(effective_request.vortex_sample_fraction.as_ref()),
        ),
        (
            "public_workflow_vortex_sample_replacement".to_string(),
            effective_request.vortex_sample_replacement.to_string(),
        ),
        (
            "public_workflow_vortex_sample_weight_column".to_string(),
            optional_or_none(effective_request.vortex_sample_weight_column.as_ref()),
        ),
        (
            "public_workflow_vortex_duplicate_keep".to_string(),
            effective_request
                .vortex_duplicate_keep
                .clone()
                .unwrap_or_else(|| "first".to_string()),
        ),
        (
            "public_workflow_vortex_deduplicate_key_columns".to_string(),
            optional_or_none(effective_request.vortex_deduplicate_key_columns.as_ref()),
        ),
        (
            "public_workflow_vortex_expression_projection_present".to_string(),
            effective_request
                .vortex_expression_projection
                .is_some()
                .to_string(),
        ),
        (
            "public_workflow_vortex_expression_projection_changed_columns".to_string(),
            expression_projection_changed_columns(
                effective_request.vortex_expression_projection.as_ref(),
            ),
        ),
        (
            "public_workflow_vortex_melt_projection_present".to_string(),
            effective_request
                .vortex_melt_projection
                .is_some()
                .to_string(),
        ),
        (
            "public_workflow_vortex_explode_projection_present".to_string(),
            effective_request
                .vortex_explode_projection
                .is_some()
                .to_string(),
        ),
        (
            "public_workflow_vortex_pivot_projection_present".to_string(),
            effective_request
                .vortex_pivot_projection
                .is_some()
                .to_string(),
        ),
        (
            "public_workflow_vortex_rolling_window_present".to_string(),
            effective_request
                .vortex_rolling_window
                .is_some()
                .to_string(),
        ),
        (
            "public_workflow_vortex_aggregate_present".to_string(),
            effective_request.vortex_aggregate.is_some().to_string(),
        ),
        (
            "public_workflow_vortex_sort_rows_present".to_string(),
            effective_request.vortex_sort_rows.is_some().to_string(),
        ),
        (
            "public_workflow_vortex_sort_tie_policy".to_string(),
            sort_rows_tie_policy(effective_request.vortex_sort_rows.as_ref()),
        ),
        (
            "public_workflow_memory_gb".to_string(),
            public_workflow_effective_memory_gb_label(&effective_request),
        ),
        (
            "public_workflow_max_parallelism".to_string(),
            public_workflow_effective_max_parallelism_label(&effective_request),
        ),
        (
            "public_workflow_requested_max_parallelism".to_string(),
            public_workflow_requested_max_parallelism(&effective_request),
        ),
        (
            "public_workflow_dynamic_parallelism_floor_applied".to_string(),
            public_workflow_dynamic_parallelism_floor_applied(&effective_request).to_string(),
        ),
        (
            "public_workflow_blocker_id".to_string(),
            plan.blocker_id.to_string(),
        ),
        (
            "public_workflow_blocker_reason".to_string(),
            plan.blocker_reason.to_string(),
        ),
        (
            "public_workflow_fallback_attempted".to_string(),
            "false".to_string(),
        ),
        (
            "public_workflow_external_engine_invoked".to_string(),
            "false".to_string(),
        ),
        (
            "public_workflow_claim_boundary".to_string(),
            CLAIM_BOUNDARY.to_string(),
        ),
        (
            "public_workflow_fallback_boundary".to_string(),
            FALLBACK_BOUNDARY.to_string(),
        ),
    ];
    push_native_vortex_contract_fields(&mut fields, "public_workflow_", &effective_request, plan);
    spill::append_request_fields(&mut fields, &effective_request);
    if let Ok(Some(resources)) = effective_request.resources.optional() {
        crate::execution_resources::append_declaration_fields(&mut fields, resources);
    }
    fields
}

fn public_workflow_timing_surface(command: &str, plan: &PublicWorkflowRoutePlan) -> &'static str {
    if plan.status != CommandStatus::Success {
        return "route_inspection_no_timing";
    }
    match command {
        "run" => "hot_runtime",
        "prepare" => "prepare_runtime",
        _ => "route_inspection_no_timing",
    }
}

fn public_workflow_actual_evidence_tier(
    command: &str,
    plan: &PublicWorkflowRoutePlan,
) -> &'static str {
    if plan.status != CommandStatus::Success || command == "route" {
        "diagnostic_only"
    } else {
        "metadata_sink"
    }
}

fn public_workflow_timing_claim_boundary(
    command: &str,
    plan: &PublicWorkflowRoutePlan,
) -> &'static str {
    if plan.status != CommandStatus::Success || command == "route" {
        "route_readiness_only_no_benchmark_runtime_claim"
    } else {
        "runtime_route_evidence_only_no_benchmark_or_publication_claim"
    }
}

fn append_hot_runtime_timing_surface_fields(fields: &mut Vec<(String, String)>) {
    push_field(fields, "timing_surface", "hot_runtime");
    push_field(fields, "actual_evidence_tier", "metadata_sink");
    push_bool_field(fields, "route_total_timing_reported", false);
    push_bool_field(fields, "result_sink_timing_included_in_route_total", false);
    push_bool_field(
        fields,
        "evidence_render_timing_included_in_route_total",
        false,
    );
    push_field(
        fields,
        "timing_claim_boundary",
        "runtime_route_evidence_only_no_benchmark_or_publication_claim",
    );
}

#[cfg(test)]
fn append_declared_local_input_format_args(
    args: &mut Vec<String>,
    request: &PublicWorkflowRouteRequest,
) {
    if let Some(input_format) = request
        .input_format
        .as_deref()
        .filter(|format| is_local_file_format(format))
    {
        args.extend(["--input-format".to_string(), input_format.to_string()]);
    }
}

fn native_vortex_primitive_runtime_args(
    request: &PublicWorkflowRouteRequest,
    primitive: PublicVortexPrimitive,
) -> Result<Vec<String>, ShardLoomError> {
    let input_uri = request.input_uri.clone().ok_or_else(|| {
        ShardLoomError::InvalidOperation(
            "public native Vortex run requires --input with a Vortex dataset".to_string(),
        )
    })?;
    let resources = request.resources.resolve()?;
    let mut args = match primitive {
        PublicVortexPrimitive::Count => vec![input_uri, "count".to_string()],
        PublicVortexPrimitive::CountWhere => vec![
            input_uri,
            required_native_vortex_payload(request.vortex_predicate.as_ref(), "vortex predicate")?,
            "--execute-local-primitive".to_string(),
        ],
        PublicVortexPrimitive::Filter => vec![
            input_uri,
            required_native_vortex_payload(request.vortex_predicate.as_ref(), "vortex predicate")?,
        ],
        PublicVortexPrimitive::Project => vec![
            input_uri,
            required_native_vortex_payload(request.vortex_columns.as_ref(), "vortex columns")?,
        ],
        PublicVortexPrimitive::FilterProject => vec![
            input_uri,
            required_native_vortex_payload(request.vortex_predicate.as_ref(), "vortex predicate")?,
            required_native_vortex_payload(request.vortex_columns.as_ref(), "vortex columns")?,
        ],
        PublicVortexPrimitive::Distinct
        | PublicVortexPrimitive::DropDuplicates
        | PublicVortexPrimitive::DuplicateMask
        | PublicVortexPrimitive::Tail
        | PublicVortexPrimitive::Sample
        | PublicVortexPrimitive::ExpressionProject
        | PublicVortexPrimitive::Melt
        | PublicVortexPrimitive::Explode
        | PublicVortexPrimitive::Pivot
        | PublicVortexPrimitive::RollingWindow
        | PublicVortexPrimitive::Aggregate
        | PublicVortexPrimitive::SortRows => {
            return Err(ShardLoomError::InvalidOperation(
                "public native Vortex materializing primitives use direct local primitive execution; fallback execution was not attempted".to_string(),
            ));
        }
    };
    if primitive.allows_source_order_limit()
        && let Some(limit) = request.vortex_source_order_limit.as_ref()
    {
        args.extend([
            "--limit".to_string(),
            positive_usize_arg("source-order limit", limit)?.to_string(),
        ]);
    }
    if !matches!(
        primitive,
        PublicVortexPrimitive::Count | PublicVortexPrimitive::CountWhere
    ) {
        args.push("--execute-local-primitive".to_string());
    }
    args.extend(crate::execution_resources::command_args(resources));
    Ok(args)
}

fn required_native_vortex_payload(
    value: Option<&String>,
    label: &str,
) -> Result<String, ShardLoomError> {
    value
        .filter(|value| !value.trim().is_empty())
        .cloned()
        .ok_or_else(|| {
            ShardLoomError::InvalidOperation(format!(
                "public native Vortex run requires {label}; fallback execution was not attempted"
            ))
        })
}

fn non_negative_u64_arg(label: &str, value: &str) -> Result<u64, ShardLoomError> {
    value.parse::<u64>().map_err(|_| {
        ShardLoomError::InvalidOperation(format!("{label} must be a non-negative integer"))
    })
}

fn sample_fraction_arg(label: &str, value: &str) -> Result<f64, ShardLoomError> {
    let parsed = value.parse::<f64>().map_err(|_| {
        ShardLoomError::InvalidOperation(format!("{label} must be a finite decimal"))
    })?;
    if !parsed.is_finite() || parsed <= 0.0 || parsed > 1.0 {
        return Err(ShardLoomError::InvalidOperation(format!(
            "{label} must be finite and in the range (0, 1]"
        )));
    }
    Ok(parsed)
}

fn normalize_duplicate_keep(value: &str) -> Result<String, ShardLoomError> {
    let normalized = value.trim().to_ascii_lowercase().replace('_', "-");
    match normalized.as_str() {
        "first" => Ok("first".to_string()),
        "last" => Ok("last".to_string()),
        "false" | "all" | "none" => Ok("false".to_string()),
        _ => Err(ShardLoomError::InvalidOperation(format!(
            "duplicate keep must be first, last, or false: {value}"
        ))),
    }
}

fn duplicate_keep_policy_arg(
    value: Option<&str>,
) -> Result<shardloom_vortex::VortexDuplicateKeepPolicy, ShardLoomError> {
    match value {
        None | Some("first") => Ok(shardloom_vortex::VortexDuplicateKeepPolicy::First),
        Some("last") => Ok(shardloom_vortex::VortexDuplicateKeepPolicy::Last),
        Some("false") => Ok(shardloom_vortex::VortexDuplicateKeepPolicy::AllDuplicates),
        Some(other) => Err(ShardLoomError::InvalidOperation(format!(
            "duplicate keep must be first, last, or false: {other}"
        ))),
    }
}

fn positive_usize_arg(label: &str, value: &str) -> Result<usize, ShardLoomError> {
    let parsed = value.parse::<usize>().map_err(|_| {
        ShardLoomError::InvalidOperation(format!("{label} must be an unsigned integer"))
    })?;
    if parsed == 0 {
        return Err(ShardLoomError::InvalidOperation(format!(
            "{label} must be >= 1"
        )));
    }
    Ok(parsed)
}

fn output_required_route(
    command: &'static str,
    target_name: &'static str,
) -> PublicWorkflowRoutePlan {
    blocked_route(
        "cg21.facade.output_required",
        "public workflow facade requires an explicit output target for this operation",
        Diagnostic::new(
            DiagnosticCode::InvalidInput,
            DiagnosticSeverity::Error,
            DiagnosticCategory::InvalidInput,
            format!("public workflow {command} requires an explicit {target_name}"),
            Some(format!("public_workflow_{command}.output")),
            Some("no output target was provided".to_string()),
            Some("pass --output with a caller-owned local target".to_string()),
            FallbackStatus::disabled_by_policy(),
        ),
    )
}

fn run_route_not_executable_yet(plan: &PublicWorkflowRoutePlan) -> PublicWorkflowRoutePlan {
    blocked_route(
        "cg21.run.route_not_executable_yet",
        "this admitted route still requires a dedicated execution wrapper before public run can execute it",
        Diagnostic::new(
            DiagnosticCode::NotImplemented,
            DiagnosticSeverity::Error,
            DiagnosticCategory::UnsupportedFeature,
            "public workflow run route is not executable by this facade slice".to_string(),
            Some("public_workflow_run.route_id".to_string()),
            Some(format!(
                "route_id={} resolved_internal_command={}",
                plan.route_id, plan.resolved_internal_command
            )),
            Some(
                "use the lower-level explicit runtime command until this wrapper is promoted"
                    .to_string(),
            ),
            FallbackStatus::disabled_by_policy(),
        ),
    )
}

fn required_value(
    args: &mut std::iter::Peekable<impl Iterator<Item = String>>,
    flag: &str,
) -> Result<String, ShardLoomError> {
    let Some(value) = args.next() else {
        return Err(ShardLoomError::InvalidOperation(format!(
            "{flag} requires a value"
        )));
    };
    if value.starts_with("--") {
        return Err(ShardLoomError::InvalidOperation(format!(
            "{flag} requires a value"
        )));
    }
    Ok(value)
}

fn normalize_surface(value: &str) -> Result<String, ShardLoomError> {
    let normalized = value.trim().to_ascii_lowercase().replace('_', "-");
    match normalized.as_str() {
        "sql" | "python" | "dataframe" | "cli" => Ok(normalized),
        _ => Err(ShardLoomError::InvalidOperation(format!(
            "unsupported public workflow surface: {value}"
        ))),
    }
}

fn normalize_input_format(value: &str) -> Result<String, ShardLoomError> {
    let normalized = value.trim().to_ascii_lowercase().replace('_', "-");
    match normalized.as_str() {
        "csv" | "tsv" | "json" | "jsonl" | "ndjson" | "parquet" | "arrow-ipc" | "avro" | "orc"
        | "vortex" | "memory" | "iceberg" | "delta" | "hudi" => Ok(normalized),
        _ => Err(ShardLoomError::InvalidOperation(format!(
            "unsupported route input format: {value}"
        ))),
    }
}

fn normalize_requested_output(value: &str) -> Result<String, ShardLoomError> {
    let normalized = value.trim().to_ascii_lowercase().replace('-', "_");
    match normalized.as_str() {
        "collect" | "prepare" | "write_vortex" | "write_parquet" | "write_arrow_ipc"
        | "write_avro" | "write_orc" | "write_csv" | "write_json" | "write_jsonl" | "explain"
        | "route" | "evidence" | "profile" => Ok(normalized),
        _ => Err(ShardLoomError::InvalidOperation(format!(
            "unsupported route requested output: {value}"
        ))),
    }
}

fn normalize_execution_policy(value: &str) -> Result<String, ShardLoomError> {
    let normalized = value.trim().to_ascii_lowercase().replace('-', "_");
    match normalized.as_str() {
        "auto" | "vortex_middle" | "vortex_prepared" | "prepare_or_native" => {
            Ok("vortex_middle".to_string())
        }
        "direct" | "native_vortex" | "prepare_once" => Ok(normalized),
        _ => Err(ShardLoomError::InvalidOperation(format!(
            "unsupported route execution policy: {value}"
        ))),
    }
}

fn normalize_materialization_policy(value: &str) -> Result<String, ShardLoomError> {
    let normalized = value.trim().to_ascii_lowercase().replace('-', "_");
    match normalized.as_str() {
        "bounded" | "materialized" | "zero_decode" | "explicit" => Ok(normalized),
        _ => Err(ShardLoomError::InvalidOperation(format!(
            "unsupported route materialization/decode policy: {value}"
        ))),
    }
}

fn normalize_evidence_level(value: &str) -> Result<String, ShardLoomError> {
    let normalized = value.trim().to_ascii_lowercase().replace('-', "_");
    match normalized.as_str() {
        "report_only" | "runtime_smoke" | "production_admitted_local_workflow" | "claim_grade" => {
            Ok(normalized)
        }
        _ => Err(ShardLoomError::InvalidOperation(format!(
            "unsupported route evidence level: {value}"
        ))),
    }
}

fn normalize_source_fingerprint_policy(value: &str) -> Result<String, ShardLoomError> {
    let normalized = value.trim().to_ascii_lowercase().replace('-', "_");
    match normalized.as_str() {
        "metadata" | "metadata_only" | "metadata_first" => Ok("metadata_only".to_string()),
        "content" | "content_digest" | "full_content_digest" => Ok("content_digest".to_string()),
        _ => Err(ShardLoomError::InvalidOperation(format!(
            "unsupported route source fingerprint policy: {value}; expected metadata_only or content_digest"
        ))),
    }
}

fn parse_bool_flag(flag: &str, value: &str) -> Result<bool, ShardLoomError> {
    match value.trim().to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" => Ok(true),
        "false" | "0" | "no" => Ok(false),
        _ => Err(ShardLoomError::InvalidOperation(format!(
            "{flag} expects true or false"
        ))),
    }
}

fn infer_input_format_from_ref(value: &str) -> Option<&'static str> {
    let trimmed = value.trim();
    let source_ref = if trimmed.contains("://") {
        trimmed.split(['?', '#']).next().unwrap_or(trimmed)
    } else {
        trimmed
    };
    let source_path = std::path::Path::new(source_ref);
    let unwrapped;
    let compressed = source_path
        .extension()
        .and_then(|value| value.to_str())
        .is_some_and(|value| matches!(value.to_ascii_lowercase().as_str(), "gz" | "zst" | "zstd"));
    let source_path = if compressed {
        unwrapped = source_path.with_extension("");
        unwrapped.as_path()
    } else {
        source_path
    };
    let extension = source_path
        .extension()
        .and_then(|extension| extension.to_str())?;
    if compressed
        && !matches!(
            extension.to_ascii_lowercase().as_str(),
            "csv" | "tsv" | "json" | "jsonl" | "ndjson"
        )
    {
        return None;
    }
    match extension.to_ascii_lowercase().as_str() {
        "csv" => Some("csv"),
        "tsv" => Some("tsv"),
        "jsonl" | "ndjson" => Some("jsonl"),
        "json" => Some("json"),
        "parquet" => Some("parquet"),
        "arrow" | "arrow-ipc" | "ipc" | "feather" => Some("arrow-ipc"),
        "avro" => Some("avro"),
        "orc" => Some("orc"),
        "vortex" | "vtx" | "vortex-manifest" => Some("vortex"),
        _ => None,
    }
}

fn auto_prepared_vortex_target_path(source_uri: &str, source_format: &str) -> PathBuf {
    auto_prepared_vortex_target_path_with_schema(source_uri, source_format, None)
}

fn auto_prepared_vortex_target_path_with_schema(
    source_uri: &str,
    source_format: &str,
    source_schema: Option<&str>,
) -> PathBuf {
    let source_path = Path::new(source_uri);
    let source_name = source_path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("source");
    let stem = source_path
        .file_stem()
        .and_then(|stem| stem.to_str())
        .map(safe_generated_vortex_stem)
        .filter(|stem| !stem.is_empty())
        .unwrap_or_else(|| safe_generated_vortex_stem(source_name));
    let identity = source_schema.map_or_else(
        || format!("{source_uri}|{source_format}"),
        |schema| format!("{source_uri}|{source_format}|{schema}"),
    );
    let digest = fnv64_digest_hex(&identity);
    let parent = source_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    parent
        .join(".shardloom")
        .join("prepared")
        .join(format!("{stem}-{digest}.vortex"))
}

fn safe_generated_vortex_stem(value: &str) -> String {
    let mut normalized = String::with_capacity(value.len());
    let mut previous_dash = false;
    for ch in value.trim().chars() {
        if ch.is_ascii_alphanumeric() {
            normalized.push(ch.to_ascii_lowercase());
            previous_dash = false;
        } else if !previous_dash {
            normalized.push('-');
            previous_dash = true;
        }
    }
    let normalized = normalized.trim_matches('-').to_string();
    if normalized.is_empty() {
        "source".to_string()
    } else {
        normalized
    }
}

fn fnv64_digest_hex(value: &str) -> String {
    const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut hash = FNV_OFFSET;
    for byte in value.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    format!("{hash:016x}")
}

fn extract_first_quoted_source_ref(statement: &str) -> Option<String> {
    extract_first_sql_source_ref(statement, true)
}

fn extract_first_declared_sql_source_ref(statement: &str) -> Option<String> {
    extract_first_sql_source_ref(statement, false)
}

fn extract_first_sql_source_ref(statement: &str, require_known_format: bool) -> Option<String> {
    sql_source_ref_ranges(statement)
        .into_iter()
        .find(|source| {
            !require_known_format || infer_input_format_from_ref(&source.value).is_some()
        })
        .map(|source| source.value)
}

fn sql_statement_has_limit(statement: &str) -> bool {
    find_sql_keyword_outside_quotes_and_parens(statement, "LIMIT").is_some()
}

fn plan_summary_has_limit(summary: &str) -> bool {
    summary.to_ascii_lowercase().contains("limit(")
}

fn sql_keyword_prefix(raw: &str, keyword: &str) -> bool {
    let trimmed = trim_sql_leading_comments_and_whitespace(raw);
    let Some(prefix) = trimmed.get(..keyword.len()) else {
        return false;
    };
    prefix.eq_ignore_ascii_case(keyword)
        && trimmed
            .as_bytes()
            .get(keyword.len())
            .is_none_or(|byte| !byte.is_ascii_alphanumeric() && *byte != b'_')
}

fn find_sql_keyword_outside_quotes_and_parens(raw: &str, keyword: &str) -> Option<usize> {
    let bytes = raw.as_bytes();
    let keyword_bytes = keyword.as_bytes();
    let mut quote_char: Option<u8> = None;
    let mut paren_depth = 0usize;
    let mut index = 0usize;
    while index < bytes.len() {
        if quote_char.is_none() {
            if bytes[index] == b'-' && bytes.get(index + 1) == Some(&b'-') {
                index += 2;
                while index < bytes.len() && bytes[index] != b'\n' {
                    index += 1;
                }
                continue;
            }
            if bytes[index] == b'/' && bytes.get(index + 1) == Some(&b'*') {
                index += 2;
                while index + 1 < bytes.len() && !(bytes[index] == b'*' && bytes[index + 1] == b'/')
                {
                    index += 1;
                }
                index = (index + 2).min(bytes.len());
                continue;
            }
        }
        match bytes[index] {
            b'\'' | b'"' => {
                let byte = bytes[index];
                if quote_char == Some(byte) {
                    if byte == b'\'' && index + 1 < bytes.len() && bytes[index + 1] == b'\'' {
                        index += 2;
                        continue;
                    }
                    quote_char = None;
                } else if quote_char.is_none() {
                    quote_char = Some(byte);
                }
            }
            b'(' if quote_char.is_none() => paren_depth += 1,
            b')' if quote_char.is_none() && paren_depth > 0 => paren_depth -= 1,
            _ if quote_char.is_none()
                && paren_depth == 0
                && index + keyword_bytes.len() <= bytes.len()
                && bytes[index..index + keyword_bytes.len()]
                    .eq_ignore_ascii_case(keyword_bytes)
                && index
                    .checked_sub(1)
                    .and_then(|before| bytes.get(before))
                    .is_none_or(|byte| !byte.is_ascii_alphanumeric() && *byte != b'_')
                && bytes
                    .get(index + keyword_bytes.len())
                    .is_none_or(|byte| !byte.is_ascii_alphanumeric() && *byte != b'_') =>
            {
                return Some(index);
            }
            _ => {}
        }
        index += 1;
    }
    None
}

fn trim_sql_leading_comments_and_whitespace(mut raw: &str) -> &str {
    loop {
        let trimmed = raw.trim_start();
        if let Some(rest) = trimmed.strip_prefix("--") {
            let next_line = rest.find('\n').map_or("", |index| &rest[index + 1..]);
            raw = next_line;
            continue;
        }
        if let Some(rest) = trimmed.strip_prefix("/*") {
            let after_comment = rest.find("*/").map_or("", |index| &rest[index + 2..]);
            raw = after_comment;
            continue;
        }
        return trimmed;
    }
}

fn leading_quoted_sql_literal_with_consumed(raw: &str) -> Option<(String, usize)> {
    let raw = trim_sql_leading_comments_and_whitespace(raw);
    let mut chars = raw.char_indices();
    let (_, quote_char) = chars.next()?;
    if quote_char != '\'' && quote_char != '"' {
        return None;
    }
    let mut literal = String::new();
    let mut last_index = quote_char.len_utf8();
    while last_index < raw.len() {
        let mut iter = raw[last_index..].char_indices();
        let (relative_index, char) = iter.next()?;
        let index = last_index + relative_index;
        if char == quote_char {
            let next_index = index + char.len_utf8();
            if quote_char == '\'' && raw[next_index..].starts_with('\'') {
                literal.push('\'');
                last_index = next_index + 1;
                continue;
            }
            return Some((literal, next_index));
        }
        literal.push(char);
        last_index = index + char.len_utf8();
    }
    None
}

#[cfg(test)]
#[path = "routing/public_workflow_route_tests.rs"]
mod tests;
