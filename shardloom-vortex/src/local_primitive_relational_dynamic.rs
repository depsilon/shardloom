//! Retain inert frontend declarations; resolve dynamic schemas once per execution.

use super::{
    Metrics, NativeExecutionContext, Node, PreparedRoot, PreparedVortexRelational,
    ResidentVortexSession, Result, VortexLocalPrimitiveExecutionPolicy, VortexRelationalPlan,
    VortexRelationalPreparation, bind, failed,
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
    lower: impl Fn(&mut VortexRelationalPreparation<'_>) -> Result<VortexRelationalPlan> + 'static,
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
    lower: impl Fn(&mut VortexRelationalPreparation<'_>) -> Result<VortexRelationalPlan> + 'static,
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
    if sources.is_empty() || sources.len() > 128 {
        return Err(failed(
            "dynamic preparation requires between 1 and 128 declared sources",
        ));
    }
    let session = ResidentVortexSession::new(
        policy.resource_envelope.memory_budget_bytes,
        policy.max_parallelism,
    )?;
    let mut preparation = VortexRelationalPreparation {
        binding: bind::Binder::new(&session)?,
    };
    inputs(&mut preparation)?;
    preparation.binding.reject_dynamic_batch_input()?;
    preparation.binding.charge(declaration_bytes)?;
    for source in sources {
        preparation.binding.source_columns(source)?;
    }
    let bind::BoundSources {
        sources,
        source_paths,
        memory_sources,
        batch_source,
        metadata,
    } = preparation.binding.finish()?;
    Ok(PreparedVortexRelational {
        session,
        sources,
        memory_sources,
        batch_source,
        source_paths,
        root: PreparedRoot::Dynamic(Box::new(lower)),
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

impl VortexRelationalPreparation<'_> {
    /// Retain a schema-dependent inner declaration. The existing correlated
    /// executor lowers it afresh for each native outer singleton, under the same
    /// execution grant. The returned reference must be consumed once as the
    /// immediate relation of a `CorrelatedSubquery` plan.
    /// # Errors
    /// Rejects metadata-only use, a zero declaration budget and denied credits.
    pub fn defer_subquery(
        &mut self,
        declaration_bytes: usize,
        lower: impl Fn(&mut VortexRelationalPreparation<'_>) -> Result<VortexRelationalPlan> + 'static,
    ) -> Result<VortexRelationalPlan> {
        self.binding
            .defer_subquery(declaration_bytes, Box::new(lower))
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
        input: Option<&mut super::batch_input::Provider<'_>>,
        consume: impl FnOnce(&Node, &Metrics<'_>) -> Result<T>,
    ) -> Result<T> {
        self.validate_batch_provider(input.is_some())?;
        context.check_cancelled()?;
        #[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
        self.validate_preparation_sources()?;
        let input = input.map(|input| super::batch_input::Execution::new(self, input));
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
