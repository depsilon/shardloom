//! Callable SQL preparation over the CLI's shared parser, source normalization
//! and native executor. This module does not emit stdout or start a worker process.

use super::{PublicSourceBinding, PublicWorkflowRouteRequest, resident_relational};
use shardloom_core::{DatasetUri, ExecutionResources, Result, ShardLoomError};
use shardloom_exec::compute_pool::CancellationToken;
use shardloom_vortex::{
    local_primitives::prepared_relational::PreparedVortexRelational,
    resident_memory_source::ResidentMemorySource, resident_session::ResidentVortexSession,
};

#[path = "native_runtime_owner.rs"]
mod owner;
pub use owner::EmbeddedExecutionOwner;

#[cfg(test)]
#[path = "native_command_admission_tests.rs"]
mod command_admission_tests;

/// An explicitly declared source at the shared SQL boundary.
#[derive(Clone)]
pub struct SqlSourceBinding {
    /// The same URI/path or source identifier referenced by SQL.
    pub reference: String,
    pub source: SqlSource,
}

/// Native data stays owned; compatibility files use the existing preparation
/// boundary and generation evidence before they become Vortex inputs.
#[derive(Clone)]
pub enum SqlSource {
    File {
        format: String,
        schema: Option<String>,
    },
    Memory(ResidentMemorySource),
    #[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
    ColumnarStream(arrow_schema::SchemaRef),
}

/// Invoke the existing command interface without launching a CLI process.
/// Native data operations use retained plan/result APIs instead; this callable
/// boundary preserves diagnostic, discovery and explicit command envelopes.
/// # Errors
/// Refuses process-owning worker/server commands and malformed response output.
pub fn command_json(args: Vec<String>) -> Result<String> {
    command_json_in_context(args, &crate::command_context::CommandContext::default())
}

/// Invoke a command under the same owner as this caller's retained native data.
/// Resource-free discovery keeps an unconfigured owner inert. Data handlers
/// resolve their existing argument grammar and admit one complete operation.
/// # Errors
/// Rejects cancellation, process-owning commands and malformed response output.
pub fn command_json_in_owner(
    args: Vec<String>,
    owner: &EmbeddedExecutionOwner,
    cancellation: &CancellationToken,
) -> Result<String> {
    command_json_in_context(
        args,
        &crate::command_context::CommandContext::embedded(owner, cancellation),
    )
}

fn command_json_in_context(
    mut args: Vec<String>,
    context: &crate::command_context::CommandContext<'_>,
) -> Result<String> {
    if matches!(
        args.first().map(String::as_str),
        Some("python-worker" | "serve")
    ) {
        return Err(failed(
            "process-owning commands require the explicit subprocess interface",
        ));
    }
    args.extend(["--format".into(), "json".into()]);
    // Legacy command reports retain deeply nested planning evidence. Give this
    // synchronous, joined call the same stack used by CLI command validation;
    // an embedding thread (including a Python thread) may have a smaller stack.
    // Capture inside the thread because command output scopes are thread-local.
    let dependency = shardloom_vortex::resident_session::ResidentOperationDependency::capture();
    std::thread::scope(|scope| {
        std::thread::Builder::new()
            .name("shardloom-command".into())
            .stack_size(crate::CLI_STACK_SIZE)
            .spawn_scoped(scope, || {
                dependency.run(|| {
                    crate::cli_output::capture_json(|| {
                        crate::run_with_session_and_context(
                            args,
                            &mut super::PublicExecutionSession::default(),
                            context,
                        );
                    })
                })
            })
            .map_err(|error| failed(&format!("command thread could not start: {error}")))?
            .join()
            .map_err(|_| failed("command thread panicked"))?
    })
}

/// The same resource, provider, source, spill and execution evidence emitted by
/// the public CLI. This contains metadata only; native values stay in their
/// result owner until an explicit data conversion is requested.
#[must_use]
pub fn execution_fields(
    execution: &shardloom_vortex::local_primitives::prepared_relational::ExecutedVortexRelational,
    reused: bool,
) -> Vec<(String, String)> {
    let mut fields = Vec::new();
    resident_relational::append_execution(&mut fields, execution, reused);
    fields
}

/// Resolve the existing public local writer format vocabulary without executing
/// a query or inspecting a destination.
/// # Errors
/// Returns the same unsupported-format reason as the public workflow router.
#[cfg(feature = "vortex-write")]
pub fn output_format(name: &str) -> Result<shardloom_vortex::VortexLocalPrimitiveRowExportFormat> {
    let mut request = PublicWorkflowRouteRequest::new("python".into());
    request.requested_output = name.into();
    super::native_vortex_row_export_format_for_output_request(&request)
        .map_err(|blocked| failed(blocked.blocker_reason))
}

/// Shared execution and output-adapter evidence for a completed native writer.
#[must_use]
#[cfg(feature = "vortex-write")]
pub fn written_fields(
    written: &shardloom_vortex::local_primitives::prepared_relational::WrittenVortexRelational,
    reused: bool,
) -> Vec<(String, String)> {
    let mut fields = Vec::new();
    super::append_native_vortex_primitive_row_export_fields(&mut fields, &written.output);
    resident_relational::append_execution(&mut fields, &written.execution, reused);
    fields
}

/// Shared per-target commit, adapter and execution fields for a completed fanout.
/// # Errors
/// Rejects a mismatched or empty completion before constructing success evidence.
#[cfg(feature = "vortex-write")]
pub fn written_many_fields(
    targets: &[(
        std::path::PathBuf,
        shardloom_vortex::VortexLocalPrimitiveRowExportFormat,
    )],
    written: Vec<shardloom_vortex::local_primitives::prepared_relational::WrittenVortexRelational>,
    reused: bool,
) -> Result<Vec<(String, String)>> {
    if targets.is_empty() || targets.len() != written.len() {
        return Err(failed(
            "fanout completion does not match its declared targets",
        ));
    }
    let targets = targets
        .iter()
        .enumerate()
        .map(
            |(index, (path, format))| super::NativeVortexPrimitiveRowExportTarget {
                role: if index == 0 { "primary" } else { "fanout" },
                format: *format,
                path: path.clone(),
            },
        )
        .collect::<Vec<_>>();
    let mut fields = Vec::new();
    resident_relational::append_written(&mut fields, &targets, written, reused);
    Ok(fields)
}

/// Prepare a complete SQL operation in the caller's already configured session.
/// All admitted SQL operators share the CLI implementation, including dynamic
/// schemas. File preparation and retained memory inputs use the same grant.
/// Cancellation is checked before source inspection and after preparation.
/// Execution/export/writers take their own explicit cancellation owner.
/// # Errors
/// Rejects unsupported syntax/types, invalid or conflicting declarations, foreign
/// native owners, source changes and shared-resource admission failures.
pub fn prepare_sql(
    statement: &str,
    resources: ExecutionResources,
    session: &ResidentVortexSession,
    bindings: &[SqlSourceBinding],
    cancellation: &CancellationToken,
) -> Result<PreparedVortexRelational> {
    prepare_sql_with_spill(statement, resources, session, bindings, None, cancellation)
}

/// Validate the existing public spill declaration without opening its workspace.
/// Embedded batch callers use this before demanding any producer input.
/// # Errors
/// Rejects malformed options, unsupported builds and a buffer beyond the grant.
pub fn validate_sql_spill(spill: Option<&str>, resources: ExecutionResources) -> Result<()> {
    let Some(spill) = spill else {
        return Ok(());
    };
    if !cfg!(feature = "vortex-write") {
        return Err(failed("native spill requires vortex-write"));
    }
    let policy = super::spill::Options::parse(spill)?.relational()?;
    if policy.buffer_bytes > resources.memory_bytes() {
        return Err(failed(
            "spill buffer_bytes exceeds the declared query memory budget",
        ));
    }
    Ok(())
}

/// Prepare SQL with the same explicit native spill policy as the CLI.
/// Validation precedes source access; the resulting plan retains the caller's
/// session for state, spill buffers and output. No workspace is opened here.
/// # Errors
/// Returns the preparation errors of `prepare_sql` and invalid spill declarations.
pub fn prepare_sql_with_spill(
    statement: &str,
    resources: ExecutionResources,
    session: &ResidentVortexSession,
    bindings: &[SqlSourceBinding],
    spill: Option<&str>,
    cancellation: &CancellationToken,
) -> Result<PreparedVortexRelational> {
    cancellation.check()?;
    validate_sql_spill(spill, resources)?;
    shardloom_vortex::local_primitives::prepared_relational::validate_session_policy(
        shardloom_vortex::VortexLocalPrimitiveExecutionPolicy::from_resources(resources)?,
        session,
    )?;
    // Use the existing validated argument representation for compatibility
    // preparation, including exact bytes, per-field origin and administrator limits.
    let mut request = PublicWorkflowRouteRequest::new("python".into());
    let mut args = crate::execution_resources::command_args(resources).into_iter();
    while let Some(flag) = args.next() {
        if !request.resources.parse_flag(&flag, &mut args)? {
            return Err(failed("resource declaration contained an unknown option"));
        }
    }
    request.sql_statement = Some(statement.into());
    request.bounded = true;
    request.spill = spill.map(super::spill::Options::parse).transpose()?;
    for binding in bindings {
        let (format, schema) = match &binding.source {
            SqlSource::File { format, schema } => (format.clone(), schema.clone()),
            SqlSource::Memory(_) => ("memory".into(), None),
            #[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
            SqlSource::ColumnarStream(_) => ("memory".into(), None),
        };
        if request
            .source_bindings
            .insert(
                binding.reference.clone(),
                PublicSourceBinding {
                    input_format: format,
                    source_schema: schema,
                    memory_input: None,
                },
            )
            .is_some()
        {
            return Err(failed("source reference is declared more than once"));
        }
    }
    let operation = resident_relational::prepare_in_session(
        statement,
        &request,
        session,
        |schemas| {
            for binding in bindings {
                cancellation.check()?;
                if let SqlSource::Memory(source) = &binding.source {
                    schemas.register_memory_source(DatasetUri::new(&binding.reference)?, |_| {
                        Ok(source.clone())
                    })?;
                }
                #[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
                if let SqlSource::ColumnarStream(schema) = &binding.source {
                    schemas.register_arrow_batch_source(
                        DatasetUri::new(&binding.reference)?,
                        std::sync::Arc::clone(schema),
                        cancellation,
                    )?;
                }
            }
            Ok(())
        },
        cancellation,
    )?;
    cancellation.check()?;
    Ok(operation)
}

fn failed(message: &str) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "native SQL preparation: {message}; no fallback execution was attempted"
    ))
}

#[cfg(all(test, feature = "vortex-write", feature = "universal-format-io"))]
#[path = "native_runtime_tests.rs"]
mod tests;
