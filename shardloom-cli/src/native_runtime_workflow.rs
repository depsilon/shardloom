//! Public workflow declarations compile into the shared native operator plan.

use super::{CancellationToken, ExecutionResources, PreparedNativeOperation, Result, workflow};
use shardloom_vortex::{
    VortexLocalPrimitiveExecutionPolicy, local_primitives::prepared_relational,
    resident_session::ResidentVortexSession,
};

/// Inert native workflow declaration parsed by the ordinary public command
/// grammar. Constructing a declaration never opens sources or destinations.
pub struct NativeWorkflowDeclaration {
    request: workflow::PublicWorkflowRouteRequest,
    control_bytes: u64,
    sql_preserves_operation: bool,
}

impl NativeWorkflowDeclaration {
    /// Use the shared parser and route admission, including unused-option checks.
    /// Arguments begin with the surface (`python`, `dataframe`, `sql` or `cli`).
    /// # Errors
    /// Returns the same declaration/route errors as the command interface.
    pub fn new(arguments: Vec<String>) -> Result<Self> {
        let has_primitive_payload = arguments.iter().any(|argument| {
            argument.starts_with("--vortex-") || argument == "--native-vortex-right-input"
        });
        let bytes = arguments.iter().try_fold(0_u64, |sum, argument| {
            let bytes = u64::try_from(argument.capacity() + std::mem::size_of::<String>())
                .map_err(|_| failed("declaration size overflow"))?;
            sum.checked_add(bytes)
                .ok_or_else(|| failed("declaration size overflow"))
        })?;
        let control_bytes = bytes
            .checked_mul(128)
            .and_then(|bytes| bytes.checked_add(8192))
            .ok_or_else(|| failed("declaration metadata estimate overflow"))?;
        let request = workflow::PublicWorkflowRouteRequest::parse(arguments.into_iter())?;
        let request = workflow::effective_public_workflow_request(&request);
        if has_primitive_payload && workflow::normalized_vortex_primitive(&request).is_none() {
            return Err(failed(
                "native operation options require a matching complete primitive declaration",
            ));
        }
        validate_route(&request)?;
        Ok(Self {
            request,
            control_bytes,
            sql_preserves_operation: !has_primitive_payload,
        })
    }

    /// Bind SQL or an explicit primitive declaration into the same native plan.
    /// Source preparation is performed only when explicitly requested.
    /// # Errors
    /// Propagates invalid resources, source changes, schema and admission failures.
    pub fn prepare(
        &self,
        resources: ExecutionResources,
        session: &ResidentVortexSession,
        cancellation: &CancellationToken,
    ) -> Result<PreparedNativeOperation> {
        cancellation.check()?;
        let policy = VortexLocalPrimitiveExecutionPolicy::from_resources(resources)?;
        prepared_relational::validate_session_policy(policy, session)?;
        let _metadata = session.reserve_input_scratch(self.control_bytes)?;
        let mut request = self.request.clone();
        request.resources =
            crate::execution_resources::ResourceArguments::from_resources(resources);
        validate_route(&request)?;
        if session.float_policy() == shardloom_core::FloatPolicy::Extended
            && !self.sql_preserves_operation
        {
            return Err(failed(
                "extended float_policy requires a complete shared SQL or typed relational declaration; explicit legacy primitive options are not admitted",
            ));
        }
        let (operation, reusable) =
            if !self.sql_preserves_operation || request.sql_statement.is_none() {
                let (operation, reusable, _) =
                    workflow::resident_relational::prepare_primitive_in_session(
                        &request,
                        session,
                        cancellation,
                    )?;
                (operation, reusable)
            } else {
                let statement = request
                    .sql_statement
                    .as_deref()
                    .ok_or_else(|| failed("complete SQL declaration is absent"))?;
                workflow::resident_relational::prepare_in_session_with_reuse(
                    statement,
                    &request,
                    session,
                    |_| Ok(()),
                    cancellation,
                )?
            };
        cancellation.check()?;
        Ok(PreparedNativeOperation::relational(operation, resources).with_reuse(reusable))
    }
}

fn validate_route(request: &workflow::PublicWorkflowRouteRequest) -> Result<()> {
    if request.materialization_policy == "zero_decode"
        && workflow::normalized_vortex_primitive(request)
            .is_some_and(workflow::native_vortex_primitive_materializes)
    {
        return Err(failed(
            "native row computation requires admitted materialization; use --materialization-policy bounded",
        ));
    }
    if let Some(primitive) = workflow::normalized_vortex_primitive(request)
        && let Some(plan) = workflow::native_vortex_primitive_payload_blocker(request, primitive)
    {
        return Err(failed(plan.blocker_reason));
    }
    let plan = workflow::plan_public_workflow_route(request);
    if plan.status != workflow::CommandStatus::Success {
        let message = plan
            .diagnostics
            .first()
            .map_or(plan.blocker_reason, |diagnostic| {
                diagnostic.message.as_str()
            });
        return Err(failed(message));
    }
    Ok(())
}

fn failed(message: &str) -> shardloom_core::ShardLoomError {
    shardloom_core::ShardLoomError::InvalidOperation(format!(
        "native operation: {message}; no fallback execution was attempted"
    ))
}
