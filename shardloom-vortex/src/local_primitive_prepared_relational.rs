//! Prepared native relational trees sharing one resource and source admission.

#[cfg(all(test, feature = "vortex-write"))]
use super::result_batch;
use super::{
    LocalVortexScanPlan, MaterializedPredicateEvaluator, VortexLocalPrimitiveExecutionPolicy,
    VortexQueryPrimitiveKind, VortexQueryPrimitiveRequest,
    native_capacity::ReservedVec,
    native_payload::detach as detach_batch,
    native_relational_aggregate,
    native_relational_batch::{failed, index_array, take_column},
    native_relational_expression, native_relational_join,
    native_relational_set::RowSet,
    native_relational_sort, native_relational_subquery, native_relational_window,
    prepared_unary::{BoundUnary, CompletedPivot, PivotSpillReport},
    vortex_error,
};
use crate::{
    relational_query::{VortexRelationalPlan, VortexRelationalSetKind as SetKind},
    resident_memory_source::{PreparedMemoryProjection, ResidentMemorySource},
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
#[cfg(all(test, feature = "vortex-write"))]
use vortex::array::VortexSessionExecute as _;
use vortex::array::{
    ArrayRef,
    arrays::StructArray,
    dtype::{DType, FieldNames, Nullability},
    validity::Validity,
};

#[path = "local_primitive_relational_batch_input.rs"]
mod batch_input;
#[path = "local_primitive_relational_bind.rs"]
mod bind;
pub use batch_input::{ExecutedVortexBatchInput, VortexRelationalBatchInput};
pub(super) use bind::{
    arithmetic_dtype as scalar_arithmetic_dtype, literal_dtype as scalar_literal_dtype,
};
#[path = "local_primitive_relational_dynamic.rs"]
mod dynamic;
pub use dynamic::{prepare_relational_with_dynamic_inputs, prepare_relational_with_dynamic_schema};
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
    /// Sparse pivots executed through complete-state native replacement runs.
    pub spilled_pivot_stages: u64,
    pub spilled_pivot_input_rows: u64,
    /// Distinct indices and cells before the output limit, summed across stages.
    pub spilled_pivot_index_rows: u64,
    pub spilled_pivot_domains: u64,
    pub spilled_pivot_cells: u64,
    /// Native blocks loaded by online, margin and output lookups; excludes merge scans.
    pub spilled_pivot_lookup_blocks: u64,
    /// Actual held-reader opens, including completion and output reopenings.
    pub spilled_pivot_reader_opens: u64,
    /// General aggregate nodes executed through the explicit native spill strategy.
    pub ordered_aggregate_stages: u64,
    /// Rows consumed by those aggregate nodes, summed across composed stages.
    pub ordered_aggregate_input_rows: u64,
    /// Additional nonnull distinct records, with duplicate measure aliases shared.
    pub ordered_aggregate_distinct_rows: u64,
    /// Join nodes executed through the explicit ordered native spill strategy.
    pub ordered_join_stages: u64,
    pub ordered_join_build_rows: u64,
    pub ordered_join_probe_rows: u64,
    /// Exact-key candidates before ON, including evaluated semi/anti batches.
    pub ordered_join_candidate_rows: u64,
    /// Right/Full matched positions recorded before adjacent deduplication.
    pub ordered_join_match_records: u64,
    /// Bounded lookup blocks loaded, including resident and native-run blocks.
    pub ordered_join_lookup_blocks: u64,
    /// Analytic window nodes using the explicit bounded native spill strategy.
    pub ordered_window_stages: u64,
    pub ordered_window_input_rows: u64,
    pub ordered_window_groups: u64,
    pub ordered_window_partitions: u64,
    /// Peer starts and partition-end sentinels written to native ordinal records.
    pub ordered_window_peer_records: u64,
    /// Frame bounds recorded for exact DISTINCT and extrema evaluation.
    pub ordered_window_bounds_rows: u64,
    pub ordered_window_distinct_intervals: u64,
    pub ordered_window_distinct_events: u64,
    pub ordered_window_extrema_summary_rows: u64,
    /// Bounded key, summary and result blocks loaded by native window lookups.
    pub ordered_window_lookup_blocks: u64,
    pub output_columns: Vec<String>,
    /// Physical decoder work inside upstream providers is not measured here.
    pub bytes_decoded: Option<u64>,
    pub native_io_certificate: NativeIoCertificate,
    pub runtime: ResidentSessionSnapshot,
    pub spill: Option<crate::relational_query::VortexRelationalSpillReport>,
    /// Present only after a streaming source's explicit end-of-input was observed.
    pub input: Option<ExecutedVortexBatchInput>,
    _metadata: MemoryLease,
}

/// Complete small collection; streaming delivery has an independent row bound.
pub struct CollectedVortexRelational {
    pub execution: ExecutedVortexRelational,
    pub result_jsonl: Budgeted<String>,
    pub result_schema_json: Budgeted<String>,
}

pub struct ExecutedOwnedVortexRelational {
    pub execution: ExecutedVortexRelational,
    pub result: OwnedVortexResultBatch,
}

/// Source identities and bound schemas are retained; operator state is fresh.
pub struct PreparedVortexRelational {
    session: ResidentVortexSession,
    sources: Vec<PreparedVortexSource>,
    memory_sources: Vec<(shardloom_core::DatasetUri, ResidentMemorySource)>,
    batch_source: Option<(shardloom_core::DatasetUri, ResidentMemorySource)>,
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
            NodeKind::Scan { source, .. } => Some(match source {
                ScanSource::File(index) => sources[*index].file().row_count(),
                ScanSource::Memory(projection) => projection.source_rows() as u64,
                ScanSource::Batch(_) => {
                    (crate::resident_memory_source::MAX_BATCHES
                        * crate::resident_memory_source::MAX_BATCH_ROWS) as u64
                }
            }),
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

enum ScanSource {
    File(usize),
    Memory(Box<PreparedMemoryProjection>),
    Batch(Box<PreparedMemoryProjection>),
}

enum NodeKind {
    CompletedPivot {
        operation: Box<BoundUnary>,
        result: Box<RefCell<Option<CompletedPivot>>>,
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
        source: ScanSource,
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
    /// Normalize a declared input into native memory owned by this preparation.
    /// The adapter constructs arrays only; the returned URI is consumed by the
    /// ordinary relational scan, operators, resource controls and writers.
    /// # Errors
    /// Rejects duplicate/non-memory URIs, foreign ownership and adapter failures.
    pub fn register_memory_source(
        &mut self,
        uri: shardloom_core::DatasetUri,
        build: impl FnOnce(&ResidentVortexSession) -> Result<ResidentMemorySource>,
    ) -> Result<()> {
        self.binding.register_memory_source(uri, build)
    }

    /// Declare a single streaming source without opening its producer. Build a
    /// typed empty owner with `ResidentMemorySource::from_batch_columns`.
    /// The complete lowered plan admits Scan/Filter/Project/Sort/Aggregate and
    /// draining Limit/Offset. Stateful operators share the query grant; native
    /// ordering and general aggregation support an explicit spill policy.
    /// # Errors
    /// Rejects duplicate/extra sources, nonempty schemas and foreign ownership.
    pub fn register_batch_source(
        &mut self,
        uri: shardloom_core::DatasetUri,
        build_schema: impl FnOnce(&ResidentVortexSession) -> Result<ResidentMemorySource>,
    ) -> Result<()> {
        self.binding.register_batch_source(uri, build_schema)
    }

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
    preparation.binding.validate_batch_plan(&plan)?;
    let root = preparation.binding.bind(&plan, 0)?;
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
    let bind::BoundSources {
        sources,
        source_paths,
        memory_sources,
        batch_source,
        metadata,
    } = binding.finish()?;
    Ok(PreparedVortexRelational {
        session: session.clone(),
        sources,
        memory_sources,
        batch_source,
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
struct Metrics<'a> {
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
    spilled_pivot_stages: Cell<u64>,
    spilled_pivot_input_rows: Cell<u64>,
    spilled_pivot_index_rows: Cell<u64>,
    spilled_pivot_domains: Cell<u64>,
    spilled_pivot_cells: Cell<u64>,
    spilled_pivot_lookup_blocks: Cell<u64>,
    spilled_pivot_reader_opens: Cell<u64>,
    ordered_aggregate_stages: Cell<u64>,
    ordered_aggregate_input_rows: Cell<u64>,
    ordered_aggregate_distinct_rows: Cell<u64>,
    ordered_join_stages: Cell<u64>,
    ordered_join_build_rows: Cell<u64>,
    ordered_join_probe_rows: Cell<u64>,
    ordered_join_candidate_rows: Cell<u64>,
    ordered_join_match_records: Cell<u64>,
    ordered_join_lookup_blocks: Cell<u64>,
    ordered_window_stages: Cell<u64>,
    ordered_window_input_rows: Cell<u64>,
    ordered_window_groups: Cell<u64>,
    ordered_window_partitions: Cell<u64>,
    ordered_window_peer_records: Cell<u64>,
    ordered_window_bounds_rows: Cell<u64>,
    ordered_window_distinct_intervals: Cell<u64>,
    ordered_window_distinct_events: Cell<u64>,
    ordered_window_extrema_summary_rows: Cell<u64>,
    ordered_window_lookup_blocks: Cell<u64>,
    input: Option<&'a dyn batch_input::Input>,
    ordering_batches_detached: Cell<u64>,
    ordering_rows_detached: Cell<u64>,
    join_build_batches_detached: Cell<u64>,
    join_build_rows_detached: Cell<u64>,
    window_batches_detached: Cell<u64>,
    window_rows_detached: Cell<u64>,
}

impl Metrics<'_> {
    fn record_pivot_spill(&self, report: PivotSpillReport) -> Result<()> {
        add(&self.spilled_pivot_stages, report.stages)?;
        add(&self.spilled_pivot_input_rows, report.input_rows)?;
        add(&self.spilled_pivot_index_rows, report.index_rows)?;
        add(&self.spilled_pivot_domains, report.domains)?;
        add(&self.spilled_pivot_cells, report.cells)?;
        add(&self.spilled_pivot_lookup_blocks, report.lookup_blocks)?;
        add(&self.spilled_pivot_reader_opens, report.reader_opens)
    }

    fn detach_window_input(
        &self,
        array: ArrayRef,
        context: &NativeExecutionContext<'_>,
    ) -> Result<ArrayRef> {
        if self.input.is_none() || array.is_empty() {
            return Ok(array);
        }
        let detached = super::native_payload::detach_with_policy(
            &array,
            super::native_payload::CopyPolicy::PreserveUnobserved,
            context,
        )?;
        add(&self.window_batches_detached, 1)?;
        add(&self.window_rows_detached, array.len() as u64)?;
        Ok(detached)
    }

    fn record_unary(&self, state_items: usize, all_input_retained: bool) -> Result<()> {
        add(&self.unary_stages, 1)?;
        add(&self.unary_state_items, state_items as u64)?;
        add(
            &self.unary_population_retention,
            u64::from(all_input_retained),
        )
    }

    fn detach_ordering_input(
        &self,
        array: ArrayRef,
        context: &NativeExecutionContext<'_>,
    ) -> Result<ArrayRef> {
        if self.input.is_none() {
            return Ok(array);
        }
        let detached = detach_batch(&array, context)?;
        add(&self.ordering_batches_detached, 1)?;
        add(&self.ordering_rows_detached, array.len() as u64)?;
        Ok(detached)
    }

    fn detach_join_build(
        &self,
        array: ArrayRef,
        context: &NativeExecutionContext<'_>,
    ) -> Result<ArrayRef> {
        if self.input.is_none() {
            return Ok(array);
        }
        let detached = detach_batch(&array, context)?;
        add(&self.join_build_batches_detached, 1)?;
        add(&self.join_build_rows_detached, array.len() as u64)?;
        Ok(detached)
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
    /// Permit native ordering, aggregate, join and analytic window runs in the existing grant.
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
        consume: impl FnMut(ArrayRef, &NativeExecutionContext<'_>) -> Result<()>,
    ) -> Result<ExecutedVortexRelational> {
        self.for_each_batch_with_input(cancellation, BATCH_ROWS, None, consume)
    }

    fn for_each_batch_with_input(
        &self,
        cancellation: &CancellationToken,
        batch_rows: usize,
        input: Option<&mut batch_input::Provider<'_>>,
        mut consume: impl FnMut(ArrayRef, &NativeExecutionContext<'_>) -> Result<()>,
    ) -> Result<ExecutedVortexRelational> {
        self.validate_batch_provider(input.is_some())?;
        let mut execution =
            self.session
                .with_sources_execution(&self.sources, cancellation, |context| {
                    self.consume_in_context(context, batch_rows, input, &mut |array| {
                        consume(array, context)
                    })
                })?;
        execution.runtime = self.snapshot();
        Ok(execution)
    }

    /// Materialize one bounded JSON batch at a time through the existing native
    /// consumer. No query replay or complete-result collection occurs. Delivered
    /// batches are provisional until this call's final source validation succeeds.
    /// # Errors
    /// Rejects invalid batch bounds, a batch above its byte bound, cancellation,
    /// consumer failures and source changes. Retained batches keep their credits.
    pub fn for_each_json_batch(
        &self,
        cancellation: &CancellationToken,
        batch_rows: usize,
        max_batch_bytes: usize,
        consume: impl FnMut(super::collect::SerializedVortexResultBatch) -> Result<()>,
    ) -> Result<ExecutedVortexRelational> {
        self.for_each_json_batch_with_input(
            cancellation,
            batch_rows,
            max_batch_bytes,
            None,
            consume,
        )
    }

    fn for_each_json_batch_with_input(
        &self,
        cancellation: &CancellationToken,
        batch_rows: usize,
        max_batch_bytes: usize,
        input: Option<&mut batch_input::Provider<'_>>,
        mut consume: impl FnMut(super::collect::SerializedVortexResultBatch) -> Result<()>,
    ) -> Result<ExecutedVortexRelational> {
        if batch_rows == 0
            || batch_rows > BATCH_ROWS
            || max_batch_bytes == 0
            || max_batch_bytes > 8 * 1024 * 1024
        {
            return Err(failed("JSON batches require 1..=2,048 rows and 1..=8 MiB"));
        }
        let mut execution =
            self.for_each_batch_with_input(cancellation, batch_rows, input, |array, context| {
                let mut sink =
                    super::collect::JsonRows::new(context.memory(), max_batch_bytes, false)?;
                sink.append_native(&array, context)?;
                let result_schema_json =
                    super::collect::serialize_result_schema(array.dtype(), context.memory())?;
                consume(super::collect::SerializedVortexResultBatch {
                    rows: array.len(),
                    values_json: sink.finish()?,
                    result_schema_json,
                })
            })?;
        super::collect::certify_json_delivery(
            &mut execution.native_io_certificate,
            execution.output_rows,
            true,
        )?;
        execution
            .native_io_certificate
            .sink_requirement_report
            .max_chunk_size = Some(batch_rows as u64);
        execution.runtime = self.snapshot();
        Ok(execution)
    }

    fn consume_in_context(
        &self,
        context: &NativeExecutionContext<'_>,
        batch_rows: usize,
        input: Option<&mut batch_input::Provider<'_>>,
        consume: &mut dyn FnMut(ArrayRef) -> Result<()>,
    ) -> Result<ExecutedVortexRelational> {
        self.with_bound_root(context, input, |root, metrics| {
            self.consume_bound(root, context, metrics, batch_rows, consume)
        })
    }

    #[allow(clippy::too_many_lines)] // Execution, final validation, cleanup and certificate share one source lifetime.
    fn consume_bound(
        &self,
        root: &Node,
        context: &NativeExecutionContext<'_>,
        metrics: &Metrics<'_>,
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
            let array = if nested || self.batch_source.is_some() {
                detach_batch(&array, context)?
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
        let mut input = metrics
            .input
            .map(batch_input::Input::completed)
            .transpose()?;
        if let Some(input) = &mut input {
            input.ordering_batches_detached = metrics.ordering_batches_detached.get();
            input.ordering_rows_detached = metrics.ordering_rows_detached.get();
            input.join_build_batches_detached = metrics.join_build_batches_detached.get();
            input.join_build_rows_detached = metrics.join_build_rows_detached.get();
            input.window_batches_detached = metrics.window_batches_detached.get();
            input.window_rows_detached = metrics.window_rows_detached.get();
        }
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
            self.memory_sources.len(),
            input.as_ref(),
            spill.as_ref(),
        )?;
        Ok(ExecutedVortexRelational {
            output_rows: rows,
            output_batches: batches,
            max_output_batch_rows: max_rows,
            output_buffer_bytes: bytes,
            scan_rows_delivered: metrics.scan_rows.get(),
            scan_batches: metrics.scan_batches.get(),
            prepared_sources: self.sources.len()
                + self.memory_sources.len()
                + usize::from(input.is_some()),
            schema_binding_deferred: matches!(self.root, PreparedRoot::Dynamic(_)),
            dynamic_schema_stages: metrics.schema_discovery_stages.get(),
            spilled_pivot_stages: metrics.spilled_pivot_stages.get(),
            spilled_pivot_input_rows: metrics.spilled_pivot_input_rows.get(),
            spilled_pivot_index_rows: metrics.spilled_pivot_index_rows.get(),
            spilled_pivot_domains: metrics.spilled_pivot_domains.get(),
            spilled_pivot_cells: metrics.spilled_pivot_cells.get(),
            spilled_pivot_lookup_blocks: metrics.spilled_pivot_lookup_blocks.get(),
            spilled_pivot_reader_opens: metrics.spilled_pivot_reader_opens.get(),
            ordered_aggregate_stages: metrics.ordered_aggregate_stages.get(),
            ordered_aggregate_input_rows: metrics.ordered_aggregate_input_rows.get(),
            ordered_aggregate_distinct_rows: metrics.ordered_aggregate_distinct_rows.get(),
            ordered_join_stages: metrics.ordered_join_stages.get(),
            ordered_join_build_rows: metrics.ordered_join_build_rows.get(),
            ordered_join_probe_rows: metrics.ordered_join_probe_rows.get(),
            ordered_join_candidate_rows: metrics.ordered_join_candidate_rows.get(),
            ordered_join_match_records: metrics.ordered_join_match_records.get(),
            ordered_join_lookup_blocks: metrics.ordered_join_lookup_blocks.get(),
            ordered_window_stages: metrics.ordered_window_stages.get(),
            ordered_window_input_rows: metrics.ordered_window_input_rows.get(),
            ordered_window_groups: metrics.ordered_window_groups.get(),
            ordered_window_partitions: metrics.ordered_window_partitions.get(),
            ordered_window_peer_records: metrics.ordered_window_peer_records.get(),
            ordered_window_bounds_rows: metrics.ordered_window_bounds_rows.get(),
            ordered_window_distinct_intervals: metrics.ordered_window_distinct_intervals.get(),
            ordered_window_distinct_events: metrics.ordered_window_distinct_events.get(),
            ordered_window_extrema_summary_rows: metrics.ordered_window_extrema_summary_rows.get(),
            ordered_window_lookup_blocks: metrics.ordered_window_lookup_blocks.get(),
            output_columns: root.fields.iter().map(|(name, _)| name.clone()).collect(),
            bytes_decoded: None,
            native_io_certificate: certificate,
            runtime: self.snapshot(),
            spill,
            input,
            _metadata: metadata,
        })
    }

    #[allow(clippy::too_many_lines)] // Keep the exhaustive dispatch table together; native kernels own the algorithms.
    fn run(
        &self,
        node: &Node,
        context: &NativeExecutionContext<'_>,
        metrics: &Metrics<'_>,
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
                let report = result.emit_report(
                    operation,
                    context,
                    batch_rows,
                    #[cfg(all(feature = "vortex-write", unix))]
                    metrics.spill.as_ref(),
                    consume,
                )?;
                metrics.record_pivot_spill(report)
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
                #[cfg(feature = "vortex-write")]
                if let Some(spill) = &metrics.spill {
                    let report = super::native_relational_aggregate_spill::run(
                        spec,
                        &input.fields,
                        spill,
                        context,
                        batch_rows,
                        |accept| self.run(input, context, metrics, batch_rows, parameter, accept),
                        consume,
                    )?;
                    add(&metrics.ordered_aggregate_stages, 1)?;
                    add(&metrics.ordered_aggregate_input_rows, report.input_rows)?;
                    add(
                        &metrics.ordered_aggregate_distinct_rows,
                        report.distinct_rows,
                    )?;
                    return Ok(());
                }
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
                source,
                &self.sources,
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
                #[cfg(feature = "vortex-write")]
                if let Some(spill) = &metrics.spill {
                    let report = native_relational_window::spill::run(
                        spec,
                        &input.fields,
                        spill,
                        context,
                        batch_rows,
                        |accept| self.run(input, context, metrics, batch_rows, parameter, accept),
                        consume,
                    )?;
                    add(&metrics.ordered_window_stages, 1)?;
                    add(&metrics.ordered_window_input_rows, report.input_rows)?;
                    add(&metrics.ordered_window_groups, report.groups)?;
                    add(&metrics.ordered_window_partitions, report.partitions)?;
                    add(&metrics.ordered_window_peer_records, report.peer_records)?;
                    add(&metrics.ordered_window_bounds_rows, report.bounds_rows)?;
                    add(
                        &metrics.ordered_window_distinct_intervals,
                        report.distinct_intervals,
                    )?;
                    add(
                        &metrics.ordered_window_distinct_events,
                        report.distinct_events,
                    )?;
                    add(
                        &metrics.ordered_window_extrema_summary_rows,
                        report.extrema_summary_rows,
                    )?;
                    add(&metrics.ordered_window_lookup_blocks, report.lookup_blocks)?;
                    if metrics.input.is_some() {
                        add(
                            &metrics.window_batches_detached,
                            report.input_batches_detached,
                        )?;
                        add(&metrics.window_rows_detached, report.input_rows)?;
                    }
                    return Ok(());
                }
                let mut window = native_relational_window::Window::new(spec, context.memory())?;
                self.run(
                    input,
                    context,
                    metrics,
                    batch_rows,
                    parameter,
                    &mut |array| {
                        window.build(metrics.detach_window_input(array, context)?, context)
                    },
                )?;
                window.finish(context, batch_rows, consume)
            }
            NodeKind::Join { left, right, spec } => {
                #[cfg(feature = "vortex-write")]
                if let Some(spill) = &metrics.spill {
                    let report = native_relational_join::spill::run(
                        spec,
                        &right.fields,
                        spill,
                        context,
                        batch_rows,
                        |accept| self.run(right, context, metrics, batch_rows, parameter, accept),
                        |accept| self.run(left, context, metrics, batch_rows, parameter, accept),
                        consume,
                    )?;
                    add(&metrics.ordered_join_stages, 1)?;
                    add(&metrics.ordered_join_build_rows, report.build_rows)?;
                    add(&metrics.ordered_join_probe_rows, report.probe_rows)?;
                    add(&metrics.ordered_join_candidate_rows, report.candidate_rows)?;
                    add(&metrics.ordered_join_match_records, report.match_records)?;
                    add(&metrics.ordered_join_lookup_blocks, report.lookup_blocks)?;
                    if metrics.input.is_some() {
                        add(
                            &metrics.join_build_batches_detached,
                            report.build_batches_detached,
                        )?;
                        add(&metrics.join_build_rows_detached, report.build_rows)?;
                    }
                    return Ok(());
                }
                let mut join = native_relational_join::Join::new(spec, context.memory())?;
                self.run(
                    right,
                    context,
                    metrics,
                    batch_rows,
                    parameter,
                    &mut |array| join.build(metrics.detach_join_build(array, context)?, context),
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
        metrics: &Metrics<'_>,
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
        self.collect_jsonl_with_input(cancellation, None)
    }

    fn collect_jsonl_with_input(
        &self,
        cancellation: &CancellationToken,
        input: Option<&mut batch_input::Provider<'_>>,
    ) -> Result<CollectedVortexRelational> {
        let mut sink = super::collect::JsonRows::new(self.session.memory(), 8 * 1024 * 1024, true)?;
        let mut execution =
            self.for_each_batch_with_input(cancellation, BATCH_ROWS, input, |array, context| {
                sink.append_native(&array, context)
            })?;
        let (result_jsonl, result_schema_json) =
            sink.finish_certified(&mut execution.native_io_certificate)?;
        execution.runtime = self.snapshot();
        Ok(CollectedVortexRelational {
            execution,
            result_jsonl,
            result_schema_json,
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
