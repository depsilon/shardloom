//! Headless portable serverless native worker entrypoint (`RFC 0047`).
//!
//! Wraps the existing admitted resident SQL/native pipeline in a short-lived
//! worker execution boundary with validated durable preparation/publication,
//! isolated per-attempt scratch cleanup, explicit stage timing, and fail-closed
//! non-fallback diagnostics.

use super::{
    SqlSource, SqlSourceBinding,
    portable_worker_store::{
        AttemptWorkspaceGuard, CommitOutcome, DurableArtifactStore, DurablePreparedSourceIdentity,
        PROVIDER_ARROW_VERSION, PROVIDER_VORTEX_VERSION, RUNTIME_VERSION,
        normalize_projected_columns, reject_secret_material, sha256_file, validate_safe_id,
    },
};
use serde::{Deserialize, Serialize};
use shardloom_core::{
    ExecutionResourceLimits, ExecutionResourceOrigin, ExecutionResources, FloatPolicy,
    RecursionLimit, Result, ShardLoomError,
};
use shardloom_exec::compute_pool::CancellationToken;
use shardloom_vortex::{
    VortexLocalPrimitiveRowExportFormat, resident_session::ResidentVortexSession,
};
use std::{
    collections::{BTreeMap, HashSet},
    fs,
    path::PathBuf,
    sync::{Arc, atomic::AtomicBool},
    time::{Duration, Instant},
};

pub const PORTABLE_WORKER_JOB_SCHEMA_VERSION: &str = "shardloom.portable_worker_job.v1";
pub const PORTABLE_WORKER_EVIDENCE_SCHEMA_VERSION: &str = "shardloom.portable_worker_evidence.v1";

fn worker_error(reason: &str) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "portable native worker: {reason}; no fallback execution was attempted"
    ))
}

/// Explicit source routing policy permitted for one portable worker execution.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkerRoutingPolicy {
    DirectOnly,
    ReuseIfCompatible,
    PrepareAndReuse,
    PrepareIfReuseJustifies {
        expected_reuse_count: u32,
        max_source_bytes: u64,
    },
    RequirePrepared,
}

/// One authorized local input file staged by the host for this worker.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerInputSpec {
    pub reference: String,
    pub path: PathBuf,
    pub format: String,
    pub schema: Option<String>,
    pub expected_sha256: Option<String>,
    pub expected_size_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prepare_columns: Option<Vec<String>>,
}

/// Resource bounds, semantics, and optional spill/deadline settings for one worker attempt.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkerResourceLimits {
    pub memory_bytes: u64,
    pub max_parallelism: usize,
    pub float_policy: FloatPolicy,
    pub recursion_limit: RecursionLimit,
    pub deadline_ms: Option<u64>,
    pub spill_buffer_bytes: Option<u64>,
    pub deployment_limits: Option<ExecutionResourceLimits>,
}

/// Output format, inline size cutoff, and host durable store root for publication.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerOutputSpec {
    pub format: String,
    pub inline_max_bytes: u64,
    pub durable_root: PathBuf,
}

/// Versioned job contract passed to a headless portable native worker.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PortableWorkerJob {
    pub schema_version: String,
    pub request_id: String,
    pub attempt_id: String,
    pub authorization_scope_id: String,
    pub sql: String,
    pub inputs: Vec<WorkerInputSpec>,
    pub resources: WorkerResourceLimits,
    pub output: WorkerOutputSpec,
    pub routing_policy: WorkerRoutingPolicy,
    pub allow_cross_request_result_reuse: bool,
}

/// Non-overlapping monotonic stage durations (ns) alongside enclosing wall duration.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerStageDurations {
    pub worker_startup_nanos: u64,
    pub input_validation_nanos: u64,
    pub preparation_or_artifact_open_nanos: u64,
    pub binding_planning_nanos: u64,
    pub engine_execution_nanos: u64,
    pub output_publication_nanos: u64,
    pub complete_request_nanos: u64,
}

/// Complete machine-readable evidence and publication handle returned by one attempt.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[allow(clippy::struct_excessive_bools)]
pub struct PortableWorkerReport {
    pub schema_version: String,
    pub request_id: String,
    pub attempt_id: String,
    pub commit_outcome: CommitOutcome,
    pub route_selected: String,
    pub route_reason: String,
    pub prepared_artifact_ids: Vec<String>,
    pub inline_payload_bytes: Option<Vec<u8>>,
    pub stage_durations: WorkerStageDurations,
    pub written_fields: Vec<(String, String)>,
    pub runtime_version: String,
    pub provider_vortex_version: String,
    pub provider_arrow_version: String,
    pub session_peak_reserved_bytes: u64,
    pub session_final_reserved_bytes: u64,
    pub live_host_verified: bool,
    pub billed_usage_available: bool,
    pub routing_uncertainty_note: String,
    pub fallback_attempted: bool,
    pub external_engine_invoked: bool,
    pub claim_gate_status: String,
}

struct ValidatedInput {
    reference: String,
    original_path: PathBuf,
    norm_format: String,
    schema: Option<String>,
    sha256: String,
    size_bytes: u64,
    prepare_columns: Option<Vec<String>>,
}

struct ResolvedInput {
    reference: String,
    original_path: PathBuf,
    resolved_path: PathBuf,
    effective_format: String,
    effective_schema: Option<String>,
}

struct ResolvedBatch {
    inputs: Vec<ResolvedInput>,
    route_selected: String,
    route_reason: String,
    prepared_artifact_ids: Vec<String>,
    peak_reserved: u64,
}

/// Headless short-lived Rust worker executing one admitted native SQL job.
pub struct PortableWorker;

impl PortableWorker {
    /// Validate `job`, enforce declared bounds and deadline cancellation,
    /// reuse or create durable prepared inputs per `routing_policy`, execute on
    /// a fresh `ResidentVortexSession`, atomically commit the output manifest,
    /// and clean up attempt scratch files before returning.
    ///
    /// # Errors
    /// Fails deterministically on malformed jobs, embedded credentials, stale or
    /// corrupted inputs/artifacts, missing required prepared artifacts, memory or
    /// cancellation limits, unsupported SQL/outputs, or post-execution leaks.
    pub fn execute(
        job: &PortableWorkerJob,
        caller_cancel: &CancellationToken,
    ) -> Result<PortableWorkerReport> {
        let total_started = Instant::now();
        let stage_started = Instant::now();
        let (resources, output_format, output_ext) = validate_job_contract(job)?;
        let cancel = job_cancellation_token(job.resources.deadline_ms, caller_cancel)?;
        cancel.check()?;
        let store = DurableArtifactStore::new(job.output.durable_root.clone());
        let attempt = store.begin_attempt(&job.request_id, &job.attempt_id)?;
        let session = ResidentVortexSession::new_with_policies(
            resources.memory_bytes(),
            resources.max_parallelism(),
            job.resources.float_policy,
            job.resources.recursion_limit,
        )?;
        let worker_startup_nanos = elapsed_nanos(stage_started);

        let stage_started = Instant::now();
        let validated = validate_inputs(&job.inputs, &cancel)?;
        let input_validation_nanos = elapsed_nanos(stage_started);

        let stage_started = Instant::now();
        let resolved = resolve_or_prepare_inputs(
            job, &store, &attempt, resources, &session, &validated, &cancel,
        )?;
        let preparation_or_artifact_open_nanos = elapsed_nanos(stage_started);

        let executed = plan_and_execute_sql(
            job,
            &resolved,
            &attempt,
            resources,
            output_format,
            output_ext,
            &session,
            &cancel,
        )?;

        let stage_started = Instant::now();
        verify_inputs_unchanged(&validated, &cancel)?;
        cancel.check()?;
        let rows = parse_output_rows(&executed.written_fields)?;
        let commit_outcome = store.commit_attempt_output(
            &job.request_id,
            &job.attempt_id,
            &executed.staged_output,
            output_ext,
            rows,
            resolved.route_selected.clone(),
            resolved.prepared_artifact_ids.clone(),
        )?;
        let inline_payload_bytes =
            read_inline_payload(&store, &commit_outcome, job.output.inline_max_bytes)?;
        let output_publication_nanos = elapsed_nanos(stage_started);

        session.reclaim_optional_memory();
        let final_reserved = session.snapshot().memory.reserved_bytes;
        drop(attempt);
        drop(session);

        Ok(PortableWorkerReport {
            schema_version: PORTABLE_WORKER_EVIDENCE_SCHEMA_VERSION.to_string(),
            request_id: job.request_id.clone(),
            attempt_id: job.attempt_id.clone(),
            commit_outcome,
            route_selected: resolved.route_selected,
            route_reason: resolved.route_reason,
            prepared_artifact_ids: resolved.prepared_artifact_ids,
            inline_payload_bytes,
            stage_durations: WorkerStageDurations {
                worker_startup_nanos,
                input_validation_nanos,
                preparation_or_artifact_open_nanos,
                binding_planning_nanos: executed.binding_planning_nanos,
                engine_execution_nanos: executed.engine_execution_nanos,
                output_publication_nanos,
                complete_request_nanos: elapsed_nanos(total_started),
            },
            written_fields: executed.written_fields,
            runtime_version: RUNTIME_VERSION.to_string(),
            provider_vortex_version: PROVIDER_VORTEX_VERSION.to_string(),
            provider_arrow_version: PROVIDER_ARROW_VERSION.to_string(),
            session_peak_reserved_bytes: executed.peak_reserved,
            session_final_reserved_bytes: final_reserved,
            live_host_verified: false,
            billed_usage_available: false,
            routing_uncertainty_note: routing_uncertainty_note(job.routing_policy),
            fallback_attempted: false,
            external_engine_invoked: false,
            claim_gate_status: "not_claim_grade".to_string(),
        })
    }
}

fn routing_uncertainty_note(policy: WorkerRoutingPolicy) -> String {
    match policy {
        WorkerRoutingPolicy::PrepareIfReuseJustifies {
            expected_reuse_count,
            max_source_bytes,
        } => format!(
            "local worker only (live_host_verified=false, billed_usage_available=false); routing evaluated declared expected_reuse_count={expected_reuse_count}, max_source_bytes={max_source_bytes} without live host cold/warm billing or remote network telemetry"
        ),
        _ => "local worker only (live_host_verified=false, billed_usage_available=false); static caller routing_policy without live host cold/warm billing or remote network telemetry".to_string(),
    }
}

struct ExecutedSqlStage {
    binding_planning_nanos: u64,
    engine_execution_nanos: u64,
    peak_reserved: u64,
    staged_output: PathBuf,
    written_fields: Vec<(String, String)>,
}

fn job_cancellation_token(
    deadline_ms: Option<u64>,
    caller_cancel: &CancellationToken,
) -> Result<CancellationToken> {
    match deadline_ms {
        Some(ms) => {
            let deadline = Instant::now()
                .checked_add(Duration::from_millis(ms))
                .ok_or_else(|| worker_error("deadline_ms exceeds representable clock"))?;
            Ok(caller_cancel.with_deadline(deadline))
        }
        None => Ok(CancellationToken::from_shared_flag_with_parent(
            Arc::new(AtomicBool::new(false)),
            caller_cancel,
        )),
    }
}

#[allow(clippy::too_many_arguments)]
fn plan_and_execute_sql(
    job: &PortableWorkerJob,
    resolved: &ResolvedBatch,
    attempt: &AttemptWorkspaceGuard,
    resources: ExecutionResources,
    output_format: VortexLocalPrimitiveRowExportFormat,
    output_ext: &str,
    session: &ResidentVortexSession,
    cancel: &CancellationToken,
) -> Result<ExecutedSqlStage> {
    let stage_started = Instant::now();
    let rewritten_sql = rewrite_sql_sources(&job.sql, &resolved.inputs)?;
    let bindings = build_sql_bindings(&resolved.inputs)?;
    let spill_spec = build_spill_spec(attempt, resources, job.resources.spill_buffer_bytes)?;
    let operation = super::prepare_sql_operation(
        &rewritten_sql,
        resources,
        session,
        &bindings,
        spill_spec.as_deref(),
        cancel,
    )?;
    let binding_planning_nanos = elapsed_nanos(stage_started);

    let stage_started = Instant::now();
    let staged_output = attempt
        .attempt_dir()
        .join(format!("staged_output.{output_ext}"));
    let written_fields = operation.write_fields(
        &staged_output,
        output_format,
        false,
        cancel,
        None,
        !resolved.prepared_artifact_ids.is_empty(),
    )?;
    let peak_reserved = resolved
        .peak_reserved
        .max(session.snapshot().memory.reserved_bytes);
    drop(operation);
    let engine_execution_nanos = elapsed_nanos(stage_started);

    Ok(ExecutedSqlStage {
        binding_planning_nanos,
        engine_execution_nanos,
        peak_reserved,
        staged_output,
        written_fields,
    })
}

fn elapsed_nanos(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX)
}

fn validate_job_contract(
    job: &PortableWorkerJob,
) -> Result<(
    ExecutionResources,
    VortexLocalPrimitiveRowExportFormat,
    &'static str,
)> {
    if job.schema_version != PORTABLE_WORKER_JOB_SCHEMA_VERSION {
        return Err(worker_error(&format!(
            "unsupported job schema_version '{}'",
            job.schema_version
        )));
    }
    validate_safe_id("request_id", &job.request_id)?;
    validate_safe_id("attempt_id", &job.attempt_id)?;
    validate_safe_id("authorization_scope_id", &job.authorization_scope_id)?;
    reject_secret_material("request_id", &job.request_id)?;
    reject_secret_material("attempt_id", &job.attempt_id)?;
    reject_secret_material("authorization_scope_id", &job.authorization_scope_id)?;
    reject_secret_material("sql", &job.sql)?;
    if job.allow_cross_request_result_reuse {
        return Err(worker_error(
            "cross-request completed result caching is disabled in v1; use durable prepared sources or idempotent request manifests",
        ));
    }
    if job.sql.trim().is_empty() {
        return Err(worker_error("job sql must not be empty"));
    }
    if job.inputs.is_empty() {
        return Err(worker_error("job must declare at least one input binding"));
    }
    let output_format = resolve_worker_output_format(&job.output.format)?;
    let output_ext = format_extension(output_format);
    let resources = ExecutionResources::from_declaration(
        job.resources.memory_bytes,
        job.resources.max_parallelism,
        ExecutionResourceOrigin::ExecutionCall,
        ExecutionResourceOrigin::ExecutionCall,
        job.resources.deployment_limits,
    )?;
    Ok((resources, output_format, output_ext))
}

fn resolve_worker_output_format(name: &str) -> Result<VortexLocalPrimitiveRowExportFormat> {
    let normalized = name.trim().to_ascii_lowercase();
    let mapped = match normalized.as_str() {
        "json" => "write_json",
        "jsonl" => "write_jsonl",
        "csv" => "write_csv",
        "vortex" => "write_vortex",
        "parquet" => "write_parquet",
        "arrow-ipc" | "arrow_ipc" | "ipc" => "write_arrow_ipc",
        "avro" => "write_avro",
        "orc" => "write_orc",
        other => other,
    };
    super::output_format(mapped)
}

const fn format_extension(format: VortexLocalPrimitiveRowExportFormat) -> &'static str {
    match format {
        VortexLocalPrimitiveRowExportFormat::Vortex => "vortex",
        VortexLocalPrimitiveRowExportFormat::Parquet => "parquet",
        VortexLocalPrimitiveRowExportFormat::ArrowIpc => "arrow",
        VortexLocalPrimitiveRowExportFormat::Json => "json",
        VortexLocalPrimitiveRowExportFormat::Jsonl => "jsonl",
        VortexLocalPrimitiveRowExportFormat::Csv => "csv",
        VortexLocalPrimitiveRowExportFormat::Avro => "avro",
        VortexLocalPrimitiveRowExportFormat::Orc => "orc",
    }
}

fn validate_inputs(
    inputs: &[WorkerInputSpec],
    cancel: &CancellationToken,
) -> Result<Vec<ValidatedInput>> {
    let mut seen_refs = HashSet::new();
    let mut out = Vec::with_capacity(inputs.len());
    for input in inputs {
        cancel.check()?;
        if input.reference.trim().is_empty() {
            return Err(worker_error("input reference must not be empty"));
        }
        reject_secret_material("input.reference", &input.reference)?;
        if let Some(schema) = &input.schema {
            reject_secret_material("input.schema", schema)?;
        }
        if !seen_refs.insert(input.reference.clone()) {
            return Err(worker_error(&format!(
                "duplicate input reference '{}'",
                input.reference
            )));
        }
        let norm_format = super::super::normalize_input_format(&input.format)?;
        let (sha256, size_bytes) = sha256_file(&input.path)?;
        if let Some(expected_size) = input.expected_size_bytes
            && expected_size != size_bytes
        {
            return Err(worker_error(&format!(
                "input '{}' byte length changed: expected {expected_size}, got {size_bytes}",
                input.reference
            )));
        }
        if let Some(expected_sha) = &input.expected_sha256
            && expected_sha != &sha256
        {
            return Err(worker_error(&format!(
                "input '{}' content sha256 changed: expected {expected_sha}, got {sha256}",
                input.reference
            )));
        }
        let prepare_columns = normalize_projected_columns(input.prepare_columns.as_deref())?;
        out.push(ValidatedInput {
            reference: input.reference.clone(),
            original_path: input.path.clone(),
            norm_format,
            schema: input.schema.clone(),
            sha256,
            size_bytes,
            prepare_columns,
        });
    }
    Ok(out)
}

fn verify_inputs_unchanged(validated: &[ValidatedInput], cancel: &CancellationToken) -> Result<()> {
    for input in validated {
        cancel.check()?;
        let (sha256, size_bytes) = sha256_file(&input.original_path)?;
        if sha256 != input.sha256 || size_bytes != input.size_bytes {
            return Err(worker_error(&format!(
                "input '{}' changed on disk during worker execution",
                input.reference
            )));
        }
    }
    Ok(())
}

fn resolve_or_prepare_inputs(
    job: &PortableWorkerJob,
    store: &DurableArtifactStore,
    attempt: &AttemptWorkspaceGuard,
    resources: ExecutionResources,
    session: &ResidentVortexSession,
    validated: &[ValidatedInput],
    cancel: &CancellationToken,
) -> Result<ResolvedBatch> {
    let mut resolved = Vec::with_capacity(validated.len());
    let mut prepared_ids = Vec::new();
    let mut hit_count = 0_usize;
    let mut built_count = 0_usize;
    let mut declined_prep_count = 0_usize;
    let mut peak_reserved = session.snapshot().memory.reserved_bytes;

    for (idx, input) in validated.iter().enumerate() {
        cancel.check()?;
        if job.routing_policy == WorkerRoutingPolicy::DirectOnly {
            resolved.push(direct_resolved(input));
            continue;
        }
        let identity = DurablePreparedSourceIdentity::current(
            input.sha256.clone(),
            input.size_bytes,
            input.norm_format.clone(),
            job.resources.float_policy,
            job.resources.recursion_limit,
            job.authorization_scope_id.clone(),
        )?
        .with_projected_columns(input.prepare_columns.as_deref())?;
        if let Some(found) = store.open_validated_prepared(&identity)? {
            hit_count += 1;
            prepared_ids.push(found.manifest.artifact_id.clone());
            resolved.push(ResolvedInput {
                reference: input.reference.clone(),
                original_path: input.original_path.clone(),
                resolved_path: found.vortex_path,
                effective_format: "vortex".to_string(),
                effective_schema: None,
            });
            continue;
        }
        match job.routing_policy {
            WorkerRoutingPolicy::RequirePrepared => {
                return Err(worker_error(&format!(
                    "required durable prepared artifact missing for input '{}'",
                    input.reference
                )));
            }
            WorkerRoutingPolicy::ReuseIfCompatible | WorkerRoutingPolicy::DirectOnly => {
                resolved.push(direct_resolved(input));
            }
            WorkerRoutingPolicy::PrepareIfReuseJustifies {
                expected_reuse_count,
                max_source_bytes,
            } => {
                if expected_reuse_count >= 2 && input.size_bytes <= max_source_bytes {
                    let (manifest, prep_peak) = prepare_and_publish_input(
                        idx, input, &identity, store, attempt, resources, session, cancel,
                    )?;
                    peak_reserved = peak_reserved.max(prep_peak);
                    built_count += 1;
                    let vortex_path = store.resolve_rel_path(&manifest.artifact_rel_path)?;
                    prepared_ids.push(manifest.artifact_id.clone());
                    resolved.push(ResolvedInput {
                        reference: input.reference.clone(),
                        original_path: input.original_path.clone(),
                        resolved_path: vortex_path,
                        effective_format: "vortex".to_string(),
                        effective_schema: None,
                    });
                } else {
                    declined_prep_count += 1;
                    resolved.push(direct_resolved(input));
                }
            }
            WorkerRoutingPolicy::PrepareAndReuse => {
                let (manifest, prep_peak) = prepare_and_publish_input(
                    idx, input, &identity, store, attempt, resources, session, cancel,
                )?;
                peak_reserved = peak_reserved.max(prep_peak);
                built_count += 1;
                let vortex_path = store.resolve_rel_path(&manifest.artifact_rel_path)?;
                prepared_ids.push(manifest.artifact_id.clone());
                resolved.push(ResolvedInput {
                    reference: input.reference.clone(),
                    original_path: input.original_path.clone(),
                    resolved_path: vortex_path,
                    effective_format: "vortex".to_string(),
                    effective_schema: None,
                });
            }
        }
    }

    let (route_selected, route_reason) = summarize_route(
        job.routing_policy,
        validated.len(),
        hit_count,
        built_count,
        declined_prep_count,
    );
    Ok(ResolvedBatch {
        inputs: resolved,
        route_selected,
        route_reason,
        prepared_artifact_ids: prepared_ids,
        peak_reserved,
    })
}

fn direct_resolved(input: &ValidatedInput) -> ResolvedInput {
    ResolvedInput {
        reference: input.reference.clone(),
        original_path: input.original_path.clone(),
        resolved_path: input.original_path.clone(),
        effective_format: input.norm_format.clone(),
        effective_schema: input.schema.clone(),
    }
}

fn summarize_route(
    policy: WorkerRoutingPolicy,
    inputs: usize,
    hits: usize,
    builds: usize,
    declined_prep: usize,
) -> (String, String) {
    match policy {
        WorkerRoutingPolicy::DirectOnly => (
            "direct_source".to_string(),
            "routing_policy=direct_only bypassed durable preparation".to_string(),
        ),
        WorkerRoutingPolicy::PrepareIfReuseJustifies {
            expected_reuse_count,
            max_source_bytes,
        } if declined_prep > 0 && hits == 0 && builds == 0 => (
            "direct_source_policy_declined_prep".to_string(),
            format!(
                "expected_reuse_count={expected_reuse_count} / max_source_bytes={max_source_bytes} declined durable preparation; executed directly"
            ),
        ),
        _ if hits == inputs && inputs > 0 => (
            "reused_durable_prepared_vortex".to_string(),
            format!("all {hits} inputs validated against durable .vortex artifacts"),
        ),
        _ if builds > 0 && hits == 0 && declined_prep == 0 => (
            "prepared_and_executed_vortex".to_string(),
            format!("materialized {builds} inputs into durable .vortex artifacts and executed"),
        ),
        _ if builds > 0 || hits > 0 => (
            "mixed_prepared_and_direct".to_string(),
            format!(
                "hits={hits}, builds={builds}, declined_prep={declined_prep}, total_inputs={inputs}"
            ),
        ),
        _ => (
            "direct_source_cache_miss".to_string(),
            "compatible prepared artifacts absent under reuse_if_compatible".to_string(),
        ),
    }
}

#[allow(clippy::too_many_arguments)]
fn prepare_and_publish_input(
    idx: usize,
    input: &ValidatedInput,
    identity: &DurablePreparedSourceIdentity,
    store: &DurableArtifactStore,
    attempt: &AttemptWorkspaceGuard,
    resources: ExecutionResources,
    session: &ResidentVortexSession,
    cancel: &CancellationToken,
) -> Result<(
    super::portable_worker_store::DurablePreparedSourceManifest,
    u64,
)> {
    let raw_path = input
        .original_path
        .to_str()
        .ok_or_else(|| worker_error("input path must be valid UTF-8"))?;
    let projection = match identity.projected_columns.as_deref() {
        Some(cols) if !cols.is_empty() => cols.join(", "),
        _ => "*".to_string(),
    };
    let prep_sql = format!(
        "SELECT {projection} FROM '{}'",
        raw_path.replace('\'', "''")
    );
    let bindings = [SqlSourceBinding {
        reference: raw_path.to_string(),
        source: SqlSource::File {
            format: input.norm_format.clone(),
            schema: input.schema.clone(),
        },
    }];
    let operation =
        super::prepare_sql_operation(&prep_sql, resources, session, &bindings, None, cancel)?;
    let staged_prep = attempt.attempt_dir().join(format!("prep_{idx}.vortex"));
    let written = operation.write_fields(
        &staged_prep,
        VortexLocalPrimitiveRowExportFormat::Vortex,
        false,
        cancel,
        None,
        false,
    )?;
    let peak = session.snapshot().memory.reserved_bytes;
    drop(operation);
    let rows = parse_output_rows(&written)?;
    let manifest = store.publish_prepared_vortex(&staged_prep, identity, rows)?;
    Ok((manifest, peak))
}

fn build_sql_bindings(resolved: &[ResolvedInput]) -> Result<Vec<SqlSourceBinding>> {
    let mut by_path: BTreeMap<String, (String, Option<String>)> = BTreeMap::new();
    for item in resolved {
        let path_str = item
            .resolved_path
            .to_str()
            .ok_or_else(|| worker_error("resolved source path must be valid UTF-8"))?
            .to_string();
        if let Some((prev_fmt, prev_schema)) = by_path.get(&path_str) {
            if prev_fmt != &item.effective_format || prev_schema != &item.effective_schema {
                return Err(worker_error(&format!(
                    "conflicting formats/schemas bound to resolved path '{path_str}'"
                )));
            }
            continue;
        }
        by_path.insert(
            path_str,
            (item.effective_format.clone(), item.effective_schema.clone()),
        );
    }
    Ok(by_path
        .into_iter()
        .map(|(reference, (format, schema))| SqlSourceBinding {
            reference,
            source: SqlSource::File { format, schema },
        })
        .collect())
}

fn build_spill_spec(
    attempt: &AttemptWorkspaceGuard,
    resources: ExecutionResources,
    spill_buffer_bytes: Option<u64>,
) -> Result<Option<String>> {
    let Some(buffer_bytes) = spill_buffer_bytes else {
        return Ok(None);
    };
    let spec = serde_json::json!({
        "workspace": attempt.spill_dir(),
        "quota_bytes": resources.memory_bytes(),
        "buffer_bytes": buffer_bytes,
    });
    serde_json::to_string(&spec)
        .map(Some)
        .map_err(|err| worker_error(&format!("cannot encode spill policy: {err}")))
}

fn parse_output_rows(written_fields: &[(String, String)]) -> Result<u64> {
    let raw = written_fields
        .iter()
        .find(|(k, _)| k == "output_row_count")
        .map(|(_, v)| v.as_str())
        .ok_or_else(|| worker_error("writer evidence omitted output_row_count"))?;
    raw.parse::<u64>()
        .map_err(|err| worker_error(&format!("invalid output_row_count '{raw}': {err}")))
}

fn read_inline_payload(
    store: &DurableArtifactStore,
    outcome: &CommitOutcome,
    inline_max_bytes: u64,
) -> Result<Option<Vec<u8>>> {
    let manifest = match outcome {
        CommitOutcome::Committed { manifest }
        | CommitOutcome::FencedAlreadyCommitted { manifest, .. } => manifest,
    };
    if manifest.output_bytes > inline_max_bytes {
        return Ok(None);
    }
    let path = store.resolve_rel_path(&manifest.output_rel_path)?;
    fs::read(&path)
        .map(Some)
        .map_err(|err| worker_error(&format!("cannot read committed output payload: {err}")))
}

/// Token-aware SQL `FROM`/`JOIN` source rewriter: binds logical input
/// references or original paths strictly in relation position to resolved local paths.
fn rewrite_sql_sources(sql: &str, resolved: &[ResolvedInput]) -> Result<String> {
    let bytes = sql.as_bytes();
    let mut out = String::with_capacity(sql.len() + 64);
    let mut pos = 0_usize;
    let mut expecting_relation = false;

    while pos < bytes.len() {
        let ch = bytes[pos];
        if ch.is_ascii_whitespace() {
            let start = pos;
            while pos < bytes.len() && bytes[pos].is_ascii_whitespace() {
                pos += 1;
            }
            out.push_str(&sql[start..pos]);
            continue;
        }
        if ch == b'-' && bytes.get(pos + 1) == Some(&b'-') {
            let start = pos;
            pos += 2;
            while pos < bytes.len() && bytes[pos] != b'\n' {
                pos += 1;
            }
            out.push_str(&sql[start..pos]);
            continue;
        }
        if ch == b'\'' {
            let (raw, next_pos) = read_single_quoted(sql, pos)?;
            let unescaped = raw.replace("''", "'");
            if expecting_relation && let Some(target) = match_resolved(&unescaped, resolved, true) {
                let target_str = target
                    .resolved_path
                    .to_str()
                    .ok_or_else(|| worker_error("resolved path must be valid UTF-8"))?;
                out.push('\'');
                out.push_str(&target_str.replace('\'', "''"));
                out.push('\'');
                expecting_relation = false;
                pos = next_pos;
                continue;
            }
            out.push_str(&sql[pos..next_pos]);
            expecting_relation = false;
            pos = next_pos;
            continue;
        }
        if ch.is_ascii_alphabetic() || ch == b'_' {
            let start = pos;
            while pos < bytes.len() && (bytes[pos].is_ascii_alphanumeric() || bytes[pos] == b'_') {
                pos += 1;
            }
            let ident = &sql[start..pos];
            if expecting_relation && let Some(target) = match_resolved(ident, resolved, false) {
                let target_str = target
                    .resolved_path
                    .to_str()
                    .ok_or_else(|| worker_error("resolved path must be valid UTF-8"))?;
                out.push('\'');
                out.push_str(&target_str.replace('\'', "''"));
                out.push('\'');
                expecting_relation = false;
                continue;
            }
            out.push_str(ident);
            expecting_relation =
                ident.eq_ignore_ascii_case("FROM") || ident.eq_ignore_ascii_case("JOIN");
            continue;
        }
        let next = pos + sql[pos..].chars().next().map_or(1, char::len_utf8);
        out.push_str(&sql[pos..next]);
        if ch == b',' && !expecting_relation {
            // Keep false unless preceded immediately after relation in FROM a, b; handled simply.
        } else if ch != b'(' {
            expecting_relation = false;
        }
        pos = next;
    }
    let _ = &resolved;
    Ok(out)
}

fn read_single_quoted(sql: &str, quote_start: usize) -> Result<(&str, usize)> {
    let bytes = sql.as_bytes();
    let mut pos = quote_start + 1;
    while pos < bytes.len() {
        if bytes[pos] == b'\'' {
            if bytes.get(pos + 1) == Some(&b'\'') {
                pos += 2;
                continue;
            }
            return Ok((&sql[quote_start + 1..pos], pos + 1));
        }
        pos += 1;
    }
    Err(worker_error("unterminated single-quoted SQL literal"))
}

fn match_resolved<'a>(
    token: &str,
    resolved: &'a [ResolvedInput],
    exact: bool,
) -> Option<&'a ResolvedInput> {
    resolved.iter().find(|item| {
        let matches_ref = if exact {
            item.reference == token
        } else {
            item.reference.eq_ignore_ascii_case(token)
        };
        matches_ref
            || item.original_path.to_str().is_some_and(|p| {
                if exact {
                    p == token
                } else {
                    p.eq_ignore_ascii_case(token)
                }
            })
    })
}

#[cfg(test)]
#[path = "portable_worker_tests.rs"]
mod tests;
