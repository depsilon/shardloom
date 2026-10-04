//! Prepared native relational trees sharing one resource and source admission.

#[cfg(test)]
use super::result_batch;
use super::{
    LocalVortexScanPlan, MaterializedPredicateEvaluator, VortexLocalPrimitiveExecutionPolicy,
    VortexQueryPrimitiveKind, VortexQueryPrimitiveRequest,
    native_capacity::ReservedVec,
    native_relational_aggregate,
    native_relational_batch::{failed, index_array, take_column},
    native_relational_expression, native_relational_join,
    native_relational_set::RowSet,
    native_relational_sort, native_relational_subquery, native_relational_window,
    prepared_unary::{BoundUnary, CompletedPivot},
    vortex_error,
};
use crate::{
    relational_query::{VortexRelationalPlan, VortexRelationalSetKind as SetKind},
    resident_session::{
        NativeExecutionContext, OwnedVortexResultBatch, PreparedVortexSource,
        ResidentSessionSnapshot, ResidentVortexSession,
    },
};
use shardloom_core::{NativeIoCertificate, Result};
use shardloom_exec::{
    compute_pool::CancellationToken,
    live_memory::{Budgeted, MemoryLease},
};
use std::{
    cell::{Cell, RefCell},
    path::PathBuf,
};
#[cfg(test)]
use vortex::array::VortexSessionExecute as _;
use vortex::array::{
    ArrayRef,
    arrays::StructArray,
    dtype::{DType, FieldNames, Nullability},
    validity::Validity,
};

#[path = "local_primitive_relational_bind.rs"]
mod bind;
pub(super) use bind::{
    arithmetic_dtype as scalar_arithmetic_dtype, literal_dtype as scalar_literal_dtype,
};
#[path = "local_primitive_relational_dynamic.rs"]
mod dynamic;
pub use dynamic::prepare_relational_with_dynamic_schema;
#[path = "local_primitive_relational_correlated.rs"]
mod correlated;
#[path = "local_primitive_relational_report.rs"]
mod report;
#[path = "local_primitive_relational_scan.rs"]
mod scan;
#[path = "local_primitive_relational_transform.rs"]
mod transform;
#[cfg(feature = "vortex-write")]
#[path = "local_primitive_relational_writer.rs"]
mod writer;
#[cfg(feature = "vortex-write")]
pub use writer::WrittenVortexRelational;
#[cfg(all(test, feature = "vortex-write"))]
#[path = "local_primitive_relational_tests.rs"]
mod tests;

const BATCH_ROWS: usize = 2048;

/// The report and certificate describe the same execution as delivered batches.
pub struct ExecutedVortexRelational {
    pub output_rows: u64,
    pub output_batches: u64,
    pub max_output_batch_rows: usize,
    pub output_buffer_bytes: u64,
    pub scan_rows_delivered: u64,
    pub scan_batches: u64,
    pub prepared_sources: usize,
    /// Data-dependent declarations are lowered afresh inside this execution.
    pub schema_binding_deferred: bool,
    pub dynamic_schema_stages: u64,
    pub output_columns: Vec<String>,
    /// Physical decoder work inside upstream providers is not measured here.
    pub bytes_decoded: Option<u64>,
    pub native_io_certificate: NativeIoCertificate,
    pub runtime: ResidentSessionSnapshot,
    pub spill: Option<crate::relational_query::VortexRelationalSpillReport>,
    _metadata: MemoryLease,
}

/// Complete small collection; streaming delivery has an independent row bound.
pub struct CollectedVortexRelational {
    pub execution: ExecutedVortexRelational,
    pub result_jsonl: Budgeted<String>,
}

pub struct ExecutedOwnedVortexRelational {
    pub execution: ExecutedVortexRelational,
    pub result: OwnedVortexResultBatch,
}

/// Source identities and bound schemas are retained; operator state is fresh.
pub struct PreparedVortexRelational {
    session: ResidentVortexSession,
    sources: Vec<PreparedVortexSource>,
    #[cfg_attr(not(feature = "vortex-write"), allow(dead_code))]
    source_paths: Vec<PathBuf>,
    root: PreparedRoot,
    #[cfg_attr(not(feature = "vortex-write"), allow(dead_code))]
    policy: VortexLocalPrimitiveExecutionPolicy,
    _metadata: MemoryLease,
    #[cfg(feature = "vortex-write")]
    spill: Option<crate::relational_query::VortexRelationalSpillPolicy>,
    #[cfg(feature = "vortex-write")]
    spill_metadata: Option<MemoryLease>,
    #[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
    preparation_sources:
        Vec<std::sync::Arc<crate::prepared_source_binding::LocalPreparationIdentity>>,
    #[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
    _preparation_metadata: Option<MemoryLease>,
}

type DynamicLowerer = dyn Fn(&mut VortexRelationalPreparation<'_>) -> Result<VortexRelationalPlan>;

enum PreparedRoot {
    Bound(Box<Node>),
    Dynamic(Box<DynamicLowerer>),
}

enum SubqueryRelation {
    Bound(Box<Node>),
    Dynamic(Box<DynamicLowerer>),
}

struct Node {
    fields: Vec<(String, DType)>,
    kind: NodeKind,
}

impl Node {
    /// Propagate only conservative bounds from already held metadata. An
    /// overflowing or unknown bound stays unknown; no input is sampled/replayed.
    fn upper_rows(&self, sources: &[PreparedVortexSource]) -> Option<u64> {
        use crate::relational_query::VortexRelationalJoinKind as JoinKind;
        match &self.kind {
            NodeKind::CompletedPivot { rows, .. } => Some(*rows as u64),
            NodeKind::Outer => Some(1),
            NodeKind::Scan { source, .. } => Some(sources[*source].file().row_count()),
            NodeKind::Project { input, .. }
            | NodeKind::Filter { input, .. }
            | NodeKind::Sort { input, .. }
            | NodeKind::Window { input, .. }
            | NodeKind::Subquery { input, .. } => input.upper_rows(sources),
            NodeKind::Unary { input, operation } => {
                operation.upper_output_rows(input.upper_rows(sources))
            }
            NodeKind::Aggregate { input, spec } => {
                if spec.groups.is_empty() {
                    Some(1)
                } else {
                    input.upper_rows(sources)
                }
            }
            NodeKind::Limit {
                input,
                offset,
                count,
            } => Some(input.upper_rows(sources).map_or(*count as u64, |rows| {
                rows.saturating_sub(*offset as u64).min(*count as u64)
            })),
            NodeKind::Set {
                left, right, kind, ..
            } => match kind {
                SetKind::UnionAll | SetKind::UnionDistinct => left
                    .upper_rows(sources)?
                    .checked_add(right.upper_rows(sources)?),
                SetKind::Except => left.upper_rows(sources),
                SetKind::Intersect => match (left.upper_rows(sources), right.upper_rows(sources)) {
                    (Some(left), Some(right)) => Some(left.min(right)),
                    (left, right) => left.or(right),
                },
            },
            NodeKind::Join { left, right, spec } => {
                if matches!(spec.kind, JoinKind::LeftSemi | JoinKind::LeftAnti) {
                    return left.upper_rows(sources);
                }
                let left = left.upper_rows(sources)?;
                let right = right.upper_rows(sources)?;
                match spec.kind {
                    JoinKind::Inner | JoinKind::Cross => left.checked_mul(right),
                    JoinKind::Left => left.checked_mul(right.max(1)),
                    JoinKind::Right => left.max(1).checked_mul(right),
                    JoinKind::Full => left
                        .checked_mul(right)?
                        .checked_add(left)?
                        .checked_add(right),
                    JoinKind::LeftSemi | JoinKind::LeftAnti => Some(left),
                }
            }
        }
    }
}

enum NodeKind {
    CompletedPivot {
        operation: Box<BoundUnary>,
        result: RefCell<Option<CompletedPivot>>,
        rows: usize,
    },
    Outer,
    Unary {
        input: Box<Node>,
        operation: Box<BoundUnary>,
    },
    Aggregate {
        input: Box<Node>,
        spec: native_relational_aggregate::Spec,
    },
    Project {
        input: Box<Node>,
        expressions: Vec<native_relational_expression::Expression>,
    },
    Filter {
        input: Box<Node>,
        predicate: native_relational_expression::Expression,
    },
    Sort {
        input: Box<Node>,
        spec: native_relational_sort::Spec,
    },
    Limit {
        input: Box<Node>,
        offset: usize,
        count: usize,
    },
    Scan {
        source: usize,
        plan: LocalVortexScanPlan,
        columns: Vec<String>,
        residual: Option<MaterializedPredicateEvaluator>,
    },
    Join {
        left: Box<Node>,
        right: Box<Node>,
        spec: native_relational_join::Spec,
    },
    Set {
        left: Box<Node>,
        right: Box<Node>,
        kind: SetKind,
        names: Vec<String>,
    },
    Window {
        input: Box<Node>,
        spec: native_relational_window::Spec,
    },
    Subquery {
        input: Box<Node>,
        relation: SubqueryRelation,
        spec: native_relational_subquery::Spec,
        parameterized: bool,
    },
}

/// Bind a static-schema plan and source generations without reading payload rows.
/// Use [`prepare_relational_with_dynamic_schema`] when a frontend must discover
/// data-dependent fields before building its downstream plan.
/// # Errors
/// Rejects unsupported types, ambiguous names, invalid CPU/memory policies,
/// incompatible keys, excessive plan metadata and inaccessible native sources.
pub fn prepare_relational(
    plan: &VortexRelationalPlan,
    policy: VortexLocalPrimitiveExecutionPolicy,
) -> Result<PreparedVortexRelational> {
    if policy.max_parallelism == 0
        || policy.resource_envelope.max_parallelism != policy.max_parallelism
        || policy.resource_envelope.memory_budget_bytes == 0
    {
        return Err(failed("requires consistent positive CPU and memory grants"));
    }
    let session = ResidentVortexSession::new(
        policy.resource_envelope.memory_budget_bytes,
        policy.max_parallelism,
    )?;
    prepare_relational_in_session(plan, policy, &session)
}

/// Source schemas for frontend lowering, sharing the eventual prepared readers.
/// Asking for columns reads file metadata only; it never evaluates query rows.
pub struct VortexRelationalPreparation<'a> {
    binding: bind::Binder<'a>,
}

impl VortexRelationalPreparation<'_> {
    /// Resolve authoritative column names from the retained native source schema.
    /// # Errors
    /// Rejects unavailable sources, nonstruct schemas and denied metadata capacity.
    pub fn source_columns(&mut self, uri: &shardloom_core::DatasetUri) -> Result<Vec<String>> {
        self.binding.source_columns(uri)
    }
}

/// Lower a frontend against native file schemas without reopening those sources
/// when binding the resulting relational tree. No payload rows are read here.
/// # Errors
/// Propagates frontend, schema, source generation and resource admission failures.
pub fn prepare_relational_with_schema(
    policy: VortexLocalPrimitiveExecutionPolicy,
    lower: impl FnOnce(&mut VortexRelationalPreparation<'_>) -> Result<VortexRelationalPlan>,
) -> Result<PreparedVortexRelational> {
    if policy.max_parallelism == 0
        || policy.resource_envelope.max_parallelism != policy.max_parallelism
        || policy.resource_envelope.memory_budget_bytes == 0
    {
        return Err(failed("requires consistent positive CPU and memory grants"));
    }
    let session = ResidentVortexSession::new(
        policy.resource_envelope.memory_budget_bytes,
        policy.max_parallelism,
    )?;
    prepare_relational_with_owner(policy, session, None, lower)
}

/// Lower against an already admitted source and retain its generation and grant.
/// # Errors
/// Rejects source/request/resource mismatch and all ordinary relational bind errors.
pub fn prepare_relational_from_source(
    uri: shardloom_core::DatasetUri,
    source: PreparedVortexSource,
    policy: VortexLocalPrimitiveExecutionPolicy,
    lower: impl FnOnce(&mut VortexRelationalPreparation<'_>) -> Result<VortexRelationalPlan>,
) -> Result<PreparedVortexRelational> {
    let request =
        VortexQueryPrimitiveRequest::project(uri.clone(), shardloom_plan::ProjectionRequest::All);
    let session = super::prepared_dispatch::source_session(&source, &request, Some(policy))?;
    prepare_relational_with_owner(policy, session, Some((uri, source)), lower)
}

fn prepare_relational_with_owner(
    policy: VortexLocalPrimitiveExecutionPolicy,
    session: ResidentVortexSession,
    source: Option<(shardloom_core::DatasetUri, PreparedVortexSource)>,
    lower: impl FnOnce(&mut VortexRelationalPreparation<'_>) -> Result<VortexRelationalPlan>,
) -> Result<PreparedVortexRelational> {
    let mut preparation = VortexRelationalPreparation {
        binding: bind::Binder::new(&session)?,
    };
    if let Some((uri, source)) = source {
        preparation.binding.seed_source(&uri, source)?;
    }
    let plan = lower(&mut preparation)?;
    let root = preparation.binding.bind(&plan, 0)?;
    let (sources, source_paths, metadata) = preparation.binding.finish()?;
    Ok(PreparedVortexRelational {
        session,
        sources,
        source_paths,
        root: PreparedRoot::Bound(Box::new(root)),
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

/// Reuse a resident session, including exact same-path source references.
/// # Errors
/// In addition to plan admission, rejects a session wider than the requested grant.
pub fn prepare_relational_in_session(
    plan: &VortexRelationalPlan,
    policy: VortexLocalPrimitiveExecutionPolicy,
    session: &ResidentVortexSession,
) -> Result<PreparedVortexRelational> {
    if policy.max_parallelism == 0
        || policy.max_parallelism != policy.resource_envelope.max_parallelism
        || session.parallelism() > policy.max_parallelism
        || session.memory().snapshot().limit_bytes > policy.resource_envelope.memory_budget_bytes
    {
        return Err(failed("session exceeds the requested resource grant"));
    }
    let mut binding = bind::Binder::new(session)?;
    let root = binding.bind(plan, 0)?;
    let (sources, source_paths, metadata) = binding.finish()?;
    Ok(PreparedVortexRelational {
        session: session.clone(),
        sources,
        source_paths,
        root: PreparedRoot::Bound(Box::new(root)),
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

#[derive(Default)]
struct Metrics {
    #[cfg(feature = "vortex-write")]
    spill: Option<super::native_relational_spill::State>,
    scan_rows: Cell<u64>,
    scan_batches: Cell<u64>,
    scans_started: Cell<u64>,
    scans_pruned: Cell<u64>,
    data_scans: Cell<u64>,
    residual_batches: Cell<u64>,
    unary_stages: Cell<u64>,
    unary_state_items: Cell<u64>,
    unary_population_retention: Cell<u64>,
    schema_discovery_stages: Cell<u64>,
}

impl Metrics {
    fn record_unary(&self, state_items: usize, all_input_retained: bool) -> Result<()> {
        add(&self.unary_stages, 1)?;
        add(&self.unary_state_items, state_items as u64)?;
        add(
            &self.unary_population_retention,
            u64::from(all_input_retained),
        )
    }
}

fn add(counter: &Cell<u64>, value: u64) -> Result<()> {
    counter.set(
        counter
            .get()
            .checked_add(value)
            .ok_or_else(|| failed("execution counter overflow"))?,
    );
    Ok(())
}

impl PreparedVortexRelational {
    /// Permit relational ordering runs inside this plan's existing resource grant.
    /// This validates configuration only; execution validates the workspace.
    /// # Errors
    /// Rejects invalid configuration and a buffer threshold beyond the query grant.
    #[cfg(feature = "vortex-write")]
    pub fn with_spill(
        mut self,
        policy: crate::relational_query::VortexRelationalSpillPolicy,
    ) -> Result<Self> {
        let policy = crate::relational_query::VortexRelationalSpillPolicy::new(
            policy.workspace,
            policy.quota_bytes,
            policy.buffer_bytes,
        )?;
        if policy.buffer_bytes > self.policy.resource_envelope.memory_budget_bytes {
            return Err(failed(
                "spill buffer threshold exceeds the query memory grant",
            ));
        }
        self.spill_metadata = Some(
            self.session.memory().reserve(
                (policy.workspace.capacity() as u64)
                    .checked_add(1024)
                    .ok_or_else(|| failed("spill configuration capacity overflow"))?,
            )?,
        );
        self.spill = Some(policy);
        Ok(self)
    }

    /// Attach the original compatibility generations to this normalized native plan.
    /// Checks cover collection, every writer's final commit and output aliases.
    /// # Errors
    /// Rejects changed preparation identities or metadata beyond the shared grant.
    #[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
    pub fn with_preparation_sources(
        self,
        sources: Vec<std::sync::Arc<crate::prepared_source_binding::LocalPreparationIdentity>>,
    ) -> Result<Self> {
        if sources.len() > 128 {
            return Err(failed(
                "relational preparation exceeds 128 compatibility sources",
            ));
        }
        // Each identity has a <=64-KiB binding, two digests and a held descriptor.
        let metadata = self
            .session
            .memory()
            .reserve(sources.len() as u64 * 131_072)?;
        for source in &sources {
            source.validate_generation()?;
        }
        Ok(Self {
            preparation_sources: sources,
            _preparation_metadata: Some(metadata),
            ..self
        })
    }

    #[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
    fn validate_preparation_sources(&self) -> Result<()> {
        for source in &self.preparation_sources {
            source.validate_generation()?;
        }
        Ok(())
    }

    #[must_use]
    pub fn snapshot(&self) -> ResidentSessionSnapshot {
        self.session.snapshot()
    }

    /// Metadata-only schema query. A dynamic declaration returns `None`; its
    /// authoritative schema accompanies the actual execution's native batches.
    #[must_use]
    pub fn output_dtype(&self) -> Option<DType> {
        match &self.root {
            PreparedRoot::Bound(root) => Some(DType::struct_(
                root.fields.clone(),
                Nullability::NonNullable,
            )),
            PreparedRoot::Dynamic(_) => None,
        }
    }

    /// Synchronously consume bounded native batches. Even an empty result has a
    /// typed batch. Batches remain provisional until final source validation.
    /// # Errors
    /// Propagates consumer errors, cancellation, source changes and resource denial.
    pub fn for_each_batch(
        &self,
        cancellation: &CancellationToken,
        mut consume: impl FnMut(ArrayRef, &NativeExecutionContext<'_>) -> Result<()>,
    ) -> Result<ExecutedVortexRelational> {
        let mut execution =
            self.session
                .with_sources_execution(&self.sources, cancellation, |context| {
                    self.consume_in_context(context, BATCH_ROWS, &mut |array| {
                        consume(array, context)
                    })
                })?;
        execution.runtime = self.snapshot();
        Ok(execution)
    }

    fn consume_in_context(
        &self,
        context: &NativeExecutionContext<'_>,
        batch_rows: usize,
        consume: &mut dyn FnMut(ArrayRef) -> Result<()>,
    ) -> Result<ExecutedVortexRelational> {
        self.with_bound_root(context, |root, metrics| {
            self.consume_bound(root, context, metrics, batch_rows, consume)
        })
    }

    fn consume_bound(
        &self,
        root: &Node,
        context: &NativeExecutionContext<'_>,
        metrics: &Metrics,
        batch_rows: usize,
        consume: &mut dyn FnMut(ArrayRef) -> Result<()>,
    ) -> Result<ExecutedVortexRelational> {
        #[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
        self.validate_preparation_sources()?;
        let metadata_bytes =
            65_536 + self.sources.len() as u64 * 4096 + root.fields.len() as u64 * 1024;
        #[cfg(feature = "vortex-write")]
        let metadata_bytes = metadata_bytes
            .checked_add(
                self.spill
                    .as_ref()
                    .map_or(0, |policy| policy.workspace.as_os_str().len() as u64),
            )
            .ok_or_else(|| failed("spill report metadata capacity overflow"))?;
        let metadata = context.memory().reserve(metadata_bytes)?;
        let mut rows = 0u64;
        let mut batches = 0u64;
        let mut bytes = 0u64;
        let mut max_rows = 0usize;
        let dtype = DType::struct_(root.fields.clone(), Nullability::NonNullable);
        let nested = root
            .fields
            .iter()
            .any(|(_, dtype)| super::native_payload::is_nested(dtype));
        let emitted = Cell::new(false);
        let mut emit = |array: ArrayRef| {
            context.check_cancelled()?;
            if array.dtype() != &dtype || array.len() > batch_rows {
                return Err(failed("producer changed its bound schema or batch size"));
            }
            let array = if nested {
                let indices = index_array(array.len(), false, context, |row| Ok(Some(row)))?;
                super::native_payload::take(&array, &indices, &dtype, context)?
            } else {
                array
            };
            rows = rows
                .checked_add(array.len() as u64)
                .ok_or_else(|| failed("output row count overflow"))?;
            batches = batches
                .checked_add(1)
                .ok_or_else(|| failed("output batch count overflow"))?;
            bytes = bytes
                .checked_add(array.nbytes())
                .ok_or_else(|| failed("output byte count overflow"))?;
            max_rows = max_rows.max(array.len());
            emitted.set(true);
            consume(array)?;
            context.check_cancelled()
        };
        self.run(root, context, metrics, batch_rows, None, &mut emit)?;
        // No input schema sampling, and no missing-schema sentinel for empty output.
        if !emitted.get() {
            let array = super::native_payload::defaults(&dtype, 0, context)?;
            emit(array)?;
        }
        #[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
        self.validate_preparation_sources()?;
        #[cfg(feature = "vortex-write")]
        let spill = metrics
            .spill
            .as_ref()
            .map(super::native_relational_spill::State::finish)
            .transpose()?;
        #[cfg(not(feature = "vortex-write"))]
        let spill: Option<crate::relational_query::VortexRelationalSpillReport> = None;
        let certificate = report::certificate(
            metrics,
            rows,
            batch_rows,
            self.sources.len(),
            spill.as_ref(),
        )?;
        Ok(ExecutedVortexRelational {
            output_rows: rows,
            output_batches: batches,
            max_output_batch_rows: max_rows,
            output_buffer_bytes: bytes,
            scan_rows_delivered: metrics.scan_rows.get(),
            scan_batches: metrics.scan_batches.get(),
            prepared_sources: self.sources.len(),
            schema_binding_deferred: matches!(self.root, PreparedRoot::Dynamic(_)),
            dynamic_schema_stages: metrics.schema_discovery_stages.get(),
            output_columns: root.fields.iter().map(|(name, _)| name.clone()).collect(),
            bytes_decoded: None,
            native_io_certificate: certificate,
            runtime: self.snapshot(),
            spill,
            _metadata: metadata,
        })
    }

    #[allow(clippy::too_many_lines)] // Keep the exhaustive dispatch table together; native kernels own the algorithms.
    fn run(
        &self,
        node: &Node,
        context: &NativeExecutionContext<'_>,
        metrics: &Metrics,
        batch_rows: usize,
        parameter: Option<&ArrayRef>,
        consume: &mut dyn FnMut(ArrayRef) -> Result<()>,
    ) -> Result<()> {
        context.check_cancelled()?;
        match &node.kind {
            NodeKind::CompletedPivot {
                operation, result, ..
            } => {
                let result = result
                    .borrow_mut()
                    .take()
                    .ok_or_else(|| failed("dynamic pivot result was consumed twice"))?;
                result.emit(operation, context, batch_rows, consume)
            }
            NodeKind::Unary { input, operation } => {
                let usage = operation.consume_relation(
                    context,
                    input.upper_rows(&self.sources),
                    batch_rows,
                    |accept| self.run(input, context, metrics, batch_rows, parameter, accept),
                    consume,
                )?;
                metrics.record_unary(usage.items, usage.all_input_retained)
            }
            NodeKind::Outer => {
                let array = parameter.ok_or_else(|| failed("outer row binding is absent"))?;
                if array.len() != 1
                    || array.dtype()
                        != &DType::struct_(node.fields.clone(), Nullability::NonNullable)
                {
                    return Err(failed("outer row binding changed its prepared schema"));
                }
                consume(array.clone())
            }
            NodeKind::Aggregate { input, spec } => {
                let mut aggregate =
                    native_relational_aggregate::Aggregate::new(spec, context.memory())?;
                self.run(
                    input,
                    context,
                    metrics,
                    batch_rows,
                    parameter,
                    &mut |array| aggregate.consume(&array, context, batch_rows),
                )?;
                aggregate.finish(context, batch_rows, consume)
            }
            NodeKind::Project { .. }
            | NodeKind::Filter { .. }
            | NodeKind::Sort { .. }
            | NodeKind::Limit { .. } => {
                self.run_transform(node, context, metrics, batch_rows, parameter, consume)
            }
            NodeKind::Scan {
                source,
                plan,
                columns,
                residual,
            } => scan::run(
                &self.sources[*source],
                plan,
                columns,
                residual.as_ref(),
                &node.fields,
                context,
                metrics,
                batch_rows,
                consume,
            ),
            NodeKind::Subquery { .. } => {
                self.run_subquery(node, context, metrics, batch_rows, parameter, consume)
            }
            NodeKind::Window { input, spec } => {
                let mut window = native_relational_window::Window::new(spec, context.memory())?;
                self.run(
                    input,
                    context,
                    metrics,
                    batch_rows,
                    parameter,
                    &mut |array| window.build(array, context),
                )?;
                window.finish(context, batch_rows, consume)
            }
            NodeKind::Join { left, right, spec } => {
                let mut join = native_relational_join::Join::new(spec, context.memory())?;
                self.run(
                    right,
                    context,
                    metrics,
                    batch_rows,
                    parameter,
                    &mut |array| join.build(array, context),
                )?;
                self.run(
                    left,
                    context,
                    metrics,
                    batch_rows,
                    parameter,
                    &mut |array| {
                        join.consume(array, context, batch_rows, consume)
                            .map(|_| ())
                    },
                )?;
                join.finish(context, batch_rows, consume).map(|_| ())
            }
            NodeKind::Set { .. } => {
                self.run_set(node, context, metrics, batch_rows, parameter, consume)
            }
        }
    }

    fn run_set(
        &self,
        node: &Node,
        context: &NativeExecutionContext<'_>,
        metrics: &Metrics,
        batch_rows: usize,
        parameter: Option<&ArrayRef>,
        consume: &mut dyn FnMut(ArrayRef) -> Result<()>,
    ) -> Result<()> {
        let NodeKind::Set {
            left,
            right,
            kind,
            names,
        } = &node.kind
        else {
            return Err(failed("set execution requires a bound set node"));
        };
        if *kind == SetKind::UnionAll {
            for input in [left, right] {
                self.run(
                    input,
                    context,
                    metrics,
                    batch_rows,
                    parameter,
                    &mut |array| consume(cast_batch(&array, &input.fields, &node.fields, context)?),
                )?;
            }
            return Ok(());
        }
        let mut output = RowSet::new(&node.fields, names, context.memory())?;
        if *kind == SetKind::UnionDistinct {
            for input in [left, right] {
                self.run(
                    input,
                    context,
                    metrics,
                    batch_rows,
                    parameter,
                    &mut |array| {
                        let array = cast_batch(&array, &input.fields, &node.fields, context)?;
                        output
                            .insert_batch(array, None, context, batch_rows, Some(consume))
                            .map(|_| ())
                    },
                )?;
            }
        } else {
            let mut membership = RowSet::new(&node.fields, names, context.memory())?;
            self.run(
                right,
                context,
                metrics,
                batch_rows,
                parameter,
                &mut |array| {
                    let array = cast_batch(&array, &right.fields, &node.fields, context)?;
                    membership
                        .insert_batch(array, None, context, batch_rows, None)
                        .map(|_| ())
                },
            )?;
            self.run(
                left,
                context,
                metrics,
                batch_rows,
                parameter,
                &mut |array| {
                    let array = cast_batch(&array, &left.fields, &node.fields, context)?;
                    output
                        .insert_batch(
                            array,
                            Some((&membership, *kind == SetKind::Intersect)),
                            context,
                            batch_rows,
                            Some(consume),
                        )
                        .map(|_| ())
                },
            )?;
        }
        Ok(())
    }

    /// Render complete small results directly from native batches in this call.
    /// # Errors
    /// Denies results above 65,536 rows or 8 MiB, and propagates execution failures.
    pub fn collect_jsonl(
        &self,
        cancellation: &CancellationToken,
    ) -> Result<CollectedVortexRelational> {
        let mut sink = super::collect::JsonRows::new(self.session.memory(), 8 * 1024 * 1024, true)?;
        let mut execution = self.for_each_batch(cancellation, |array, context| {
            sink.append_native(&array, context)
        })?;
        let result_jsonl = sink.finish_certified(&mut execution.native_io_certificate)?;
        Ok(CollectedVortexRelational {
            execution,
            result_jsonl,
        })
    }

    /// Retain complete small native results; each final buffer owns its credits.
    /// # Errors
    /// Denies results above the small collection boundary; use streaming or a writer.
    pub fn execute_owned(&self) -> Result<ExecutedOwnedVortexRelational> {
        let mut arrays = ReservedVec::new(self.session.memory())?;
        let mut rows = 0usize;
        let mut bytes = 0u64;
        let execution = self.for_each_batch(&CancellationToken::default(), |array, _| {
            rows = rows
                .checked_add(array.len())
                .ok_or_else(|| failed("collection row overflow"))?;
            bytes = bytes
                .checked_add(array.nbytes())
                .ok_or_else(|| failed("collection byte overflow"))?;
            if rows > 65_536 || bytes > 8 * 1024 * 1024 {
                return Err(failed(
                    "small collection exceeds 65536 rows or 8 MiB; use streaming output",
                ));
            }
            arrays.push(array)
        })?;
        let (arrays, ownership) = arrays.into_parts();
        let result = self.session.own_completed_arrays(arrays, ownership)?;
        Ok(ExecutedOwnedVortexRelational { execution, result })
    }
}

fn select_batch(
    array: &ArrayRef,
    fields: &[(String, DType)],
    rows: &[usize],
    context: &NativeExecutionContext<'_>,
) -> Result<ArrayRef> {
    let indices = index_array(rows.len(), false, context, |row| Ok(Some(rows[row])))?;
    let selected = array.take(indices).map_err(vortex_error)?;
    let mut columns = ReservedVec::new(context.memory())?;
    columns.reserve(fields.len())?;
    for (name, _) in fields {
        columns
            .values
            .push(super::logical_field_from_native_array(&selected, name)?);
    }
    let (columns, _ownership) = columns.into_parts();
    StructArray::try_new(
        fields
            .iter()
            .map(|(name, _)| name.as_str())
            .collect::<FieldNames>(),
        columns,
        rows.len(),
        Validity::NonNullable,
    )
    .map_err(vortex_error)
    .map(vortex::array::IntoArray::into_array)
}

fn cast_batch(
    array: &ArrayRef,
    source: &[(String, DType)],
    fields: &[(String, DType)],
    context: &NativeExecutionContext<'_>,
) -> Result<ArrayRef> {
    let indices = index_array(array.len(), false, context, |row| Ok(Some(row)))?;
    let mut columns = ReservedVec::new(context.memory())?;
    columns.reserve(fields.len())?;
    for ((name, _), (_, dtype)) in source.iter().zip(fields) {
        columns.values.push(take_column(
            &super::logical_field_from_native_array(array, name)?,
            &indices,
            dtype,
            context,
        )?);
    }
    let (columns, _ownership) = columns.into_parts();
    StructArray::try_new(
        fields
            .iter()
            .map(|(name, _)| name.as_str())
            .collect::<FieldNames>(),
        columns,
        array.len(),
        Validity::NonNullable,
    )
    .map_err(vortex_error)
    .map(vortex::array::IntoArray::into_array)
}
