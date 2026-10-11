//! One retained source/lowering and fresh unary state for every public collect.

use super::{
    CommandStatus, ExitCode, NativeVortexInputBinding, OutputFormat, PublicExecutionSession,
    PublicVortexPrimitive, PublicWorkflowRoutePlan, PublicWorkflowRouteRequest, ShardLoomError,
    append_native_result_schema_fields, append_native_vortex_materializing_primitive_fields, emit,
    execute_native_vortex_materializing_primitive_run_with_extra, execution_attachment_fields,
    native_vortex_bound_request_and_arg, native_vortex_input_binding_for_request,
    native_vortex_materializing_error, native_vortex_materializing_policy,
};
use shardloom_exec::compute_pool::CancellationToken;
use shardloom_vortex::{
    VortexLocalPrimitiveExecutionPolicy, VortexQueryPrimitiveRequest,
    local_primitives::prepared_unary::{
        CollectedVortexUnary, PreparedVortexUnary, prepare_unary_for_optional_reuse,
    },
};

pub(super) struct PreparedPublicUnary {
    pub(super) request: PublicWorkflowRouteRequest,
    primitive: VortexQueryPrimitiveRequest,
    policy: VortexLocalPrimitiveExecutionPolicy,
    operation: PreparedVortexUnary,
}

struct Executed {
    value: CollectedVortexUnary,
    primitive_arg: String,
    opened: bool,
}

pub(super) fn run(
    request: &PublicWorkflowRouteRequest,
    plan: &PublicWorkflowRoutePlan,
    format: OutputFormat,
    extra_fields: Vec<(String, String)>,
    execution_session: &mut PublicExecutionSession,
    primitive: PublicVortexPrimitive,
) -> ExitCode {
    if request.materialization_policy == "zero_decode" {
        execution_session.clear();
        return execute_native_vortex_materializing_primitive_run_with_extra(
            request,
            plan,
            format,
            extra_fields,
            primitive,
        );
    }
    let binding = match native_vortex_input_binding_for_request(request) {
        Ok(binding) => binding,
        Err(error) => {
            execution_session.clear();
            return native_vortex_materializing_error(format, primitive, &error);
        }
    };
    if binding.mode == "single_file" {
        match execute(request, primitive, &binding, execution_session) {
            Ok(Some(executed)) => {
                return render(request, plan, format, extra_fields, &binding, executed);
            }
            Ok(None) => {}
            Err(error) => {
                // Source/resource/operator failure is terminal. Repreparation
                // requires a later explicit call, never an internal retry.
                execution_session.clear();
                return native_vortex_materializing_error(format, primitive, &error);
            }
        }
    }
    execution_session.clear();
    execute_native_vortex_materializing_primitive_run_with_extra(
        request,
        plan,
        format,
        extra_fields,
        primitive,
    )
}

fn execute(
    request: &PublicWorkflowRouteRequest,
    kind: PublicVortexPrimitive,
    binding: &NativeVortexInputBinding,
    execution_session: &mut PublicExecutionSession,
) -> Result<Option<Executed>, ShardLoomError> {
    let (primitive, primitive_arg, _) =
        native_vortex_bound_request_and_arg(request, kind, binding)?;
    let policy = native_vortex_materializing_policy(request)?;
    let matches = execution_session.unary.as_ref().is_some_and(|entry| {
        entry.request == *request && entry.primitive == primitive && entry.policy == policy
    });
    if !matches {
        execution_session.clear();
        let Some(operation) = prepare_unary_for_optional_reuse(&primitive, policy)? else {
            return Ok(None);
        };
        execution_session.unary = Some(PreparedPublicUnary {
            request: request.clone(),
            primitive,
            policy,
            operation,
        });
    }
    let entry = execution_session.unary.as_ref().ok_or_else(|| {
        ShardLoomError::InvalidOperation(
            "prepared unary was not admitted; no fallback execution was attempted".into(),
        )
    })?;
    Ok(Some(Executed {
        value: entry
            .operation
            .collect_jsonl(&CancellationToken::default())?,
        primitive_arg,
        opened: !matches,
    }))
}

fn render(
    request: &PublicWorkflowRouteRequest,
    plan: &PublicWorkflowRoutePlan,
    format: OutputFormat,
    mut extra_fields: Vec<(String, String)>,
    binding: &NativeVortexInputBinding,
    executed: Executed,
) -> ExitCode {
    let mut fields = execution_attachment_fields("run", request, plan);
    fields.append(&mut extra_fields);
    fields.extend(binding.evidence_fields());
    let result = executed.value.execution;
    append_native_vortex_materializing_primitive_fields(
        &mut fields,
        &result.report,
        &executed.primitive_arg,
        Some(&result.native_io_certificate),
        None,
    );
    crate::execution_resources::append_resident_snapshot_fields(&mut fields, &result.runtime);
    let (jsonl, _json_ownership) = executed.value.result_jsonl.into_parts();
    let (schema, _schema_ownership) = executed.value.result_schema_json.into_parts();
    append_native_result_schema_fields(&mut fields, schema);
    fields.extend([
        ("result_jsonl".into(), jsonl),
        ("result_payload_complete".into(), "true".into()),
        (
            "result_materialization_boundary".into(),
            "bounded_native_batches_to_jsonl".into(),
        ),
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
        ("resident_unary_handle_retained".into(), "true".into()),
        (
            "resident_unary_lowering_reused".into(),
            (!executed.opened).to_string(),
        ),
        (
            "resident_source_generation_validation".into(),
            "before_and_after_native_scan_and_result_serialization".into(),
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
        "native Vortex unary collection".into(),
        result.report.to_human_text(),
        result.report.diagnostics.clone(),
        fields,
    );
    ExitCode::SUCCESS
}
