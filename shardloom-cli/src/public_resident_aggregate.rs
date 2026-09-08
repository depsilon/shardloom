//! Retain only an admitted aggregate source/lowering. Every call executes fresh
//! native state and emits that execution's report and native I/O certificate.

use super::{
    CommandStatus, ExitCode, NativeVortexInputBinding, OutputFormat, PublicExecutionSession,
    PublicVortexPrimitive, PublicWorkflowRoutePlan, PublicWorkflowRouteRequest, ShardLoomError,
    append_native_vortex_materializing_primitive_fields, emit,
    execute_native_vortex_materializing_primitive_run_with_extra, execution_attachment_fields,
    native_vortex_bound_request_and_arg, native_vortex_input_binding_for_request,
    native_vortex_materializing_error, native_vortex_materializing_policy,
};
use shardloom_vortex::{
    VortexLocalPrimitiveExecutionPolicy, VortexQueryPrimitiveRequest,
    local_primitives::prepared_aggregate::{
        ExecutedVortexAggregate, PreparedAggregateDisposition, PreparedVortexAggregate,
        prepare_aggregate_for_optional_reuse,
    },
};

pub(super) struct PreparedPublicAggregate {
    pub(super) request: PublicWorkflowRouteRequest,
    primitive: VortexQueryPrimitiveRequest,
    policy: VortexLocalPrimitiveExecutionPolicy,
    operation: PreparedVortexAggregate,
}

struct Executed {
    value: ExecutedVortexAggregate,
    primitive_arg: String,
    opened: bool,
    retained: bool,
}

pub(super) fn run(
    request: &PublicWorkflowRouteRequest,
    plan: &PublicWorkflowRoutePlan,
    format: OutputFormat,
    extra_fields: Vec<(String, String)>,
    execution_session: &mut PublicExecutionSession,
) -> ExitCode {
    // Keep the ordinary materialization-policy rejection before any source open.
    if request.materialization_policy == "zero_decode" {
        execution_session.clear();
        return ordinary(request, plan, format, extra_fields);
    }
    let binding = match native_vortex_input_binding_for_request(request) {
        Ok(binding) => binding,
        Err(error) => {
            execution_session.clear();
            return native_vortex_materializing_error(
                format,
                PublicVortexPrimitive::Aggregate,
                &error,
            );
        }
    };
    if binding.mode != "single_file" {
        execution_session.clear();
        return ordinary(request, plan, format, extra_fields);
    }
    match execute(request, &binding, execution_session) {
        Ok(Some(executed)) => render(request, plan, format, extra_fields, &binding, &executed),
        Ok(None) => {
            execution_session.clear();
            ordinary(request, plan, format, extra_fields)
        }
        Err(error) => {
            // Invalidation is terminal for this call. A later explicit request
            // may prepare again; never reopen and retry the failed operation.
            execution_session.clear();
            native_vortex_materializing_error(format, PublicVortexPrimitive::Aggregate, &error)
        }
    }
}

fn ordinary(
    request: &PublicWorkflowRouteRequest,
    plan: &PublicWorkflowRoutePlan,
    format: OutputFormat,
    extra_fields: Vec<(String, String)>,
) -> ExitCode {
    execute_native_vortex_materializing_primitive_run_with_extra(
        request,
        plan,
        format,
        extra_fields,
        PublicVortexPrimitive::Aggregate,
    )
}

fn execute(
    request: &PublicWorkflowRouteRequest,
    binding: &NativeVortexInputBinding,
    execution_session: &mut PublicExecutionSession,
) -> Result<Option<Executed>, ShardLoomError> {
    let (primitive, primitive_arg, _) =
        native_vortex_bound_request_and_arg(request, PublicVortexPrimitive::Aggregate, binding)?;
    let policy = native_vortex_materializing_policy(request)?;
    let matches = execution_session.aggregate.as_ref().is_some_and(|entry| {
        entry.request == *request && entry.primitive == primitive && entry.policy == policy
    });
    if !matches {
        execution_session.clear();
        match prepare_aggregate_for_optional_reuse(&primitive, policy)? {
            None => return Ok(None),
            Some(PreparedAggregateDisposition::Unretained(operation)) => {
                return Ok(Some(Executed {
                    value: operation.execute()?,
                    primitive_arg,
                    opened: true,
                    retained: false,
                }));
            }
            Some(PreparedAggregateDisposition::Reusable(operation)) => {
                execution_session.aggregate = Some(PreparedPublicAggregate {
                    request: request.clone(),
                    primitive,
                    policy,
                    operation,
                });
            }
        }
    }
    let prepared = execution_session.aggregate.as_ref().ok_or_else(|| {
        ShardLoomError::InvalidOperation(
            "prepared aggregate was not admitted; no fallback execution was attempted".into(),
        )
    })?;
    Ok(Some(Executed {
        value: prepared.operation.execute()?,
        primitive_arg,
        opened: !matches,
        retained: true,
    }))
}

fn render(
    request: &PublicWorkflowRouteRequest,
    plan: &PublicWorkflowRoutePlan,
    format: OutputFormat,
    mut extra_fields: Vec<(String, String)>,
    binding: &NativeVortexInputBinding,
    executed: &Executed,
) -> ExitCode {
    let mut fields = execution_attachment_fields("run", request, plan);
    fields.append(&mut extra_fields);
    fields.extend(binding.evidence_fields());
    let result = &executed.value;
    append_native_vortex_materializing_primitive_fields(
        &mut fields,
        &result.report,
        &executed.primitive_arg,
        Some(&result.native_io_certificate),
        None,
    );
    fields.extend([
        (
            "resident_source_opens".into(),
            result.runtime.prepared_source_opens.to_string(),
        ),
        (
            "resident_completed_executions".into(),
            result.runtime.completed_executions.to_string(),
        ),
        (
            "resident_provider_background_workers".into(),
            result.runtime.provider_background_workers.to_string(),
        ),
        (
            "resident_peak_reserved_buffer_bytes".into(),
            result.runtime.memory.peak_reserved_bytes.to_string(),
        ),
        (
            "resident_footer_open_performed_this_call".into(),
            executed.opened.to_string(),
        ),
        (
            "resident_aggregate_handle_retained".into(),
            executed.retained.to_string(),
        ),
        (
            "resident_aggregate_lowering_reused".into(),
            (executed.retained && !executed.opened).to_string(),
        ),
        (
            "resident_source_generation_validation".into(),
            "before_and_after_native_scan_including_metadata_pruned_result".into(),
        ),
        ("resident_provider_crate".into(), "vortex".into()),
        (
            "resident_provider_version".into(),
            shardloom_vortex::UPSTREAM_VORTEX_PROVIDER_VERSION.into(),
        ),
    ]);
    emit(
        "run",
        format,
        CommandStatus::Success,
        "native Vortex aggregate primitive".into(),
        result.report.to_human_text(),
        result.report.diagnostics.clone(),
        fields,
    );
    ExitCode::SUCCESS
}
