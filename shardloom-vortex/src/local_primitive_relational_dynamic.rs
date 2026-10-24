//! Retain inert frontend declarations; resolve dynamic schemas once per execution.

use super::{
    Metrics, NativeExecutionContext, Node, PreparedRoot, PreparedVortexRelational,
    ResidentVortexSession, Result, VortexLocalPrimitiveExecutionPolicy, VortexRelationalPlan,
    VortexRelationalPreparation, bind, failed, validate_session_policy,
};
use shardloom_core::DatasetUri;

/// Prepare every declared native source without executing query rows. The
/// lowerer runs once inside each admitted execution and may resolve a dynamic
/// prefix through [`VortexRelationalPreparation::resolve_output`].
///
/// `declaration_bytes` is the caller's conservative reservation for captured
/// frontend declarations and transient lowering allocations. The callback must
/// only build native plans; it must not run an external executor or add sources.
/// Native operator and schema-binding allocations have separate shared credits.
/// # Errors
/// Rejects invalid grants, absent sources, generation failures and denied metadata.
pub fn prepare_relational_with_dynamic_schema(
    sources: &[DatasetUri],
    policy: VortexLocalPrimitiveExecutionPolicy,
    declaration_bytes: usize,
    lower: impl Fn(&mut VortexRelationalPreparation<'_>) -> Result<VortexRelationalPlan>
    + Send
    + 'static,
) -> Result<PreparedVortexRelational> {
    prepare_relational_with_dynamic_inputs(sources, policy, declaration_bytes, |_| Ok(()), lower)
}

/// Register native memory inputs before retaining a dynamic declaration. The
/// same native schema binder and executor also admit ordinary file sources.
/// # Errors
/// Propagates input, resource, schema and source admission failures.
pub fn prepare_relational_with_dynamic_inputs(
    sources: &[DatasetUri],
    policy: VortexLocalPrimitiveExecutionPolicy,
    declaration_bytes: usize,
    inputs: impl FnOnce(&mut VortexRelationalPreparation<'_>) -> Result<()>,
    lower: impl Fn(&mut VortexRelationalPreparation<'_>) -> Result<VortexRelationalPlan>
    + Send
    + 'static,
) -> Result<PreparedVortexRelational> {
    if policy.max_parallelism == 0
        || policy.resource_envelope.max_parallelism != policy.max_parallelism
        || policy.resource_envelope.memory_budget_bytes == 0
        || declaration_bytes == 0
    {
        return Err(failed(
            "dynamic preparation requires consistent positive resource and metadata grants",
        ));
    }
    validate_sources(sources)?;
    let session = ResidentVortexSession::new(
        policy.resource_envelope.memory_budget_bytes,
        policy.max_parallelism,
    )?;
    prepare_relational_with_dynamic_inputs_in_session(
        sources,
        policy,
        &session,
        declaration_bytes,
        inputs,
        lower,
    )
}

/// Retain a dynamic frontend declaration inside an existing native allocation.
/// Previously retained inputs, results and every execution share its owners.
/// # Errors
/// Rejects inconsistent allocation, source/schema failures and metadata denial.
pub fn prepare_relational_with_dynamic_inputs_in_session(
    sources: &[DatasetUri],
    policy: VortexLocalPrimitiveExecutionPolicy,
    session: &ResidentVortexSession,
    declaration_bytes: usize,
    inputs: impl FnOnce(&mut VortexRelationalPreparation<'_>) -> Result<()>,
    lower: impl Fn(&mut VortexRelationalPreparation<'_>) -> Result<VortexRelationalPlan>
    + Send
    + 'static,
) -> Result<PreparedVortexRelational> {
    prepare_relational_with_dynamic_inputs_in_session_controlled(
        sources,
        policy,
        session,
        declaration_bytes,
        inputs,
        lower,
        &shardloom_exec::compute_pool::CancellationToken::default(),
    )
}

/// Retain a dynamic declaration with the caller's cancellation owner.
/// # Errors
/// Rejects cancellation and ordinary source, resource or schema failures.
#[allow(clippy::too_many_arguments)]
pub fn prepare_relational_with_dynamic_inputs_in_session_controlled(
    sources: &[DatasetUri],
    policy: VortexLocalPrimitiveExecutionPolicy,
    session: &ResidentVortexSession,
    declaration_bytes: usize,
    inputs: impl FnOnce(&mut VortexRelationalPreparation<'_>) -> Result<()>,
    lower: impl Fn(&mut VortexRelationalPreparation<'_>) -> Result<VortexRelationalPlan>
    + Send
    + 'static,
    cancellation: &shardloom_exec::compute_pool::CancellationToken,
) -> Result<PreparedVortexRelational> {
    cancellation.check()?;
    validate_session_policy(policy, session)?;
    if declaration_bytes == 0 {
        return Err(failed(
            "dynamic preparation requires a positive metadata grant",
        ));
    }
    validate_sources(sources)?;
    let mut preparation = VortexRelationalPreparation {
        binding: bind::Binder::new_with_cancellation(session, cancellation)?,
    };
    inputs(&mut preparation)?;
    preparation.binding.charge(declaration_bytes)?;
    for source in sources {
        preparation.binding.source_columns(source)?;
    }
    let bind::BoundSources {
        sources,
        source_paths,
        memory_sources,
        batch_sources,
        metadata,
    } = preparation.binding.finish()?;
    Ok(PreparedVortexRelational {
        session: session.clone(),
        sources,
        memory_sources,
        batch_sources,
        source_paths,
        root: PreparedRoot::Dynamic(Box::new(lower)),
        provider: None,
        provider_selection_reason: "deferred_schema_requires_relational_execution",
        policy,
        _metadata: metadata,
        #[cfg(feature = "vortex-write")]
        spill: None,
        #[cfg(feature = "vortex-write")]
        spill_metadata: None,
        #[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
        preparation_sources: Vec::new(),
        #[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
        _preparation_metadata: None,
    })
}

fn validate_sources(sources: &[DatasetUri]) -> Result<()> {
    if sources.is_empty() || sources.len() > 128 {
        return Err(failed(
            "dynamic preparation requires between 1 and 128 declared sources",
        ));
    }
    Ok(())
}

impl VortexRelationalPreparation<'_> {
    /// Declare a source needed by several dynamic subtrees before its first
    /// input demand. Static complete plans infer reuse automatically. Streaming
    /// inputs retain compact native replay under the shared memory/spill policy;
    /// file and resident sources already support repeated scans. No rows are read.
    /// # Errors
    /// Rejects unknown sources, cancellation and reuse declared after demand.
    pub fn declare_source_reuse(&mut self, uri: &DatasetUri) -> Result<()> {
        self.binding.declare_source_reuse(uri)
    }

    /// Retain a schema-dependent inner declaration. The existing correlated
    /// executor lowers it afresh for each native outer singleton, under the same
    /// execution grant. The returned reference must be consumed once as the
    /// immediate relation of a `CorrelatedSubquery` plan.
    /// # Errors
    /// Rejects metadata-only use, a zero declaration budget and denied credits.
    pub fn defer_subquery(
        &mut self,
        declaration_bytes: usize,
        lower: impl Fn(&mut VortexRelationalPreparation<'_>) -> Result<VortexRelationalPlan>
        + Send
        + 'static,
    ) -> Result<VortexRelationalPlan> {
        self.binding
            .defer_subquery(declaration_bytes, None, Box::new(lower))
    }

    /// Retain a schema-dependent inner declaration with all possible native
    /// inputs named before demand. The callback may use only these registered
    /// sources and `Outer`; nested declarations must preserve that restriction.
    /// Streaming inputs replay under the existing shared memory/spill policy.
    /// An empty list admits an inner relation that needs no registered source.
    /// The reference remains single-use and scoped to this execution.
    /// # Errors
    /// Rejects unknown sources, more than 128 entries, denied metadata, late
    /// replay requirements and attempts to escape a parent source declaration.
    pub fn defer_subquery_with_sources(
        &mut self,
        sources: &[DatasetUri],
        declaration_bytes: usize,
        lower: impl Fn(&mut VortexRelationalPreparation<'_>) -> Result<VortexRelationalPlan>
        + Send
        + 'static,
    ) -> Result<VortexRelationalPlan> {
        self.binding
            .defer_subquery(declaration_bytes, Some(sources), Box::new(lower))
    }

    /// Resolve a native prefix inside the current execution. Returns authoritative
    /// column names and a single-use plan reference to the same owned operation
    /// state. This never serializes or collects the prefix to obtain its schema.
    /// # Errors
    /// Rejects use during metadata-only preparation, invalid plans, source changes,
    /// cancellation, pressure and repeated/foreign ownership references.
    pub fn resolve_output(
        &mut self,
        plan: &VortexRelationalPlan,
    ) -> Result<(VortexRelationalPlan, Vec<String>)> {
        self.binding.resolve_output(plan)
    }
}

impl PreparedVortexRelational {
    pub(super) fn with_bound_root<T>(
        &self,
        context: &NativeExecutionContext<'_>,
        input: Option<super::batch_input::Provider<'_>>,
        consume: impl FnOnce(&Node, &Metrics<'_>) -> Result<T>,
    ) -> Result<T> {
        self.validate_batch_provider(input.is_some())?;
        context.check_cancelled()?;
        #[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
        self.validate_preparation_sources()?;
        let input = input
            .map(|input| super::batch_input::Execution::new(self, input))
            .transpose()?;
        let metrics = Metrics {
            input: input
                .as_ref()
                .map(|input| input as &dyn super::batch_input::Input),
            #[cfg(feature = "vortex-write")]
            spill: self
                .spill
                .as_ref()
                .map(|policy| super::super::native_relational_spill::State::new(policy, context))
                .transpose()?,
            ..Metrics::default()
        };
        match &self.root {
            PreparedRoot::Bound(root) => consume(root, &metrics),
            PreparedRoot::Dynamic(lower) => {
                let mut preparation = VortexRelationalPreparation {
                    binding: bind::Binder::for_execution(self, context, &metrics, None)?,
                };
                let plan = lower(&mut preparation)?;
                let root = preparation.binding.bind(&plan, 0)?;
                preparation.binding.ensure_consumed()?;
                let result = consume(&root, &metrics)?;
                // Binding and its reservations stay alive until all native
                // consumers have finished. They never escape into a later call.
                drop(root);
                drop(preparation);
                Ok(result)
            }
        }
    }
}
