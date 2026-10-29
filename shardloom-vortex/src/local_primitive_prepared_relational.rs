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
#[path = "local_primitive_relational_grouping.rs"]
mod grouping;
#[path = "local_primitive_relational_batch_replay.rs"]
mod replay;
pub use batch_input::{ExecutedVortexBatchInput, VortexRelationalBatchInput};
pub(super) use bind::{
    arithmetic_dtype as scalar_arithmetic_dtype, literal_dtype as scalar_literal_dtype,
};
#[path = "local_primitive_relational_dynamic.rs"]
mod dynamic;
pub use dynamic::{
    prepare_relational_with_dynamic_inputs, prepare_relational_with_dynamic_inputs_in_session,
    prepare_relational_with_dynamic_inputs_in_session_controlled,
    prepare_relational_with_dynamic_schema,
};
#[path = "local_primitive_relational_correlated.rs"]
mod correlated;
#[path = "local_primitive_relational_cyclic_exists.rs"]
mod cyclic_exists;
#[path = "local_primitive_relational_provider.rs"]
mod provider;
#[path = "local_primitive_relational_report.rs"]
mod report;
#[path = "local_primitive_relational_sample_preparation.rs"]
mod sample_preparation;
#[path = "local_primitive_relational_scan.rs"]
mod scan;
#[path = "local_primitive_relational_selection.rs"]
mod selection;
use selection::LocalSelection;
#[path = "local_primitive_relational_observation.rs"]
mod observation;
#[path = "local_primitive_relational_transform.rs"]
mod transform;
#[cfg(feature = "vortex-write")]
#[path = "local_primitive_relational_writer.rs"]
mod writer;
#[cfg(feature = "vortex-write")]
pub use writer::{
    VortexRelationalOutputReplay, WrittenVortexRelational, WrittenVortexRelationalFanout,
};
#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
#[path = "local_primitive_relational_arrow.rs"]
mod arrow;
#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
pub use arrow::ExecutedVortexArrowRelational;
#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
#[path = "local_primitive_relational_credit_window.rs"]
mod credit_window;
#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
pub use credit_window::OutputCreditWindow;
#[cfg(all(test, feature = "vortex-write"))]
#[path = "local_primitives/native_local_selection_observation.rs"]
mod selection_observation;
#[cfg(all(test, feature = "vortex-write"))]
#[path = "local_primitive_relational_tests.rs"]
mod tests;

const BATCH_ROWS: usize = 2048;

/// The report and certificate describe the same execution as delivered batches.
pub struct ExecutedVortexRelational {
    /// Complete metrics from this execution. A writer exposes them only after
    /// successful publication; failed actions never return a completed report.
    pub observations: Vec<CompletedVortexRelationalObservation>,
    _observation_metadata: Option<MemoryLease>,
    /// Actual primitive report, selected after complete typed validation.
    /// Projection-only file scans, general relational, memory and streaming
    /// providers use the relational certificate without a primitive report.
    pub primitive_execution: Option<super::VortexLocalPrimitiveExecutionReport>,
    /// Deterministic native selection outcome from the bound plan. This is an
    /// admission/semantics reason, not a measured explanation of elapsed time.
    pub provider_selection_reason: &'static str,
    pub resources: shardloom_core::ExecutionResources,
    pub output_rows: u64,
    pub output_batches: u64,
    pub max_output_batch_rows: usize,
    pub output_buffer_bytes: u64,
    pub scan_rows_delivered: u64,
    pub scan_batches: u64,
    pub prepared_sources: usize,
    /// Held file sources for this plan, excluding memory and batch inputs.
    /// Session-lifetime open counters may include unrelated earlier plans.
    pub prepared_file_sources: usize,
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
    /// Nonnull native sorted-value records serving exact quantiles; shared levels reuse records.
    /// Records shared with DISTINCT are counted by both semantic counters; counters are not
    /// disjoint physical row or byte totals.
    pub ordered_aggregate_quantile_rows: u64,
    /// Distinct set nodes using the bounded native ordering strategy.
    pub ordered_set_stages: u64,
    pub ordered_set_input_rows: u64,
    /// Complete null-equal key groups across both source sides.
    pub ordered_set_groups: u64,
    pub ordered_set_output_rows: u64,
    /// Unary membership nodes using a pressure-capable native strategy. Actual
    /// file activity remains in `spill`; small instances can stay resident.
    pub spillable_unary_membership_stages: u64,
    /// Rows delivered to these nodes, including an unevaluated drained suffix.
    pub spillable_unary_input_rows: u64,
    /// Passing keys actually observed, excluding the drained prefix-limit suffix.
    pub spillable_unary_observed_rows: u64,
    /// Exact distinct keys among the observed rows, summed across these nodes.
    pub spillable_unary_groups: u64,
    /// Requested prefix identities retained for future-dependent duplicate masks.
    pub spillable_unary_identity_rows: u64,
    /// Rows emitted by these nodes before downstream consumption.
    pub spillable_unary_output_rows: u64,
    /// Replacement-state blocks loaded for lookup and final state traversal.
    pub spillable_unary_lookup_blocks: u64,
    /// Replacement-state readers opened; excludes identity/output ordering I/O.
    pub spillable_unary_reader_opens: u64,
    /// Sampling nodes admitted to the finite native population strategy.
    /// Actual file activity remains in `spill`; small populations stay resident.
    pub spillable_sampling_stages: u64,
    pub spillable_sampling_input_rows: u64,
    /// Passing rows retained logically across resident buffers and native runs.
    pub spillable_sampling_population_rows: u64,
    pub spillable_sampling_output_rows: u64,
    /// Replacement draws, including repeated selected rows.
    pub spillable_sampling_replacement_draws: u64,
    /// Positional blocks loaded for replacement; excludes ordering merge scans.
    pub spillable_sampling_lookup_blocks: u64,
    /// Completed predicate inner builds using the native spill-capable strategy.
    /// Each selected parameter can build one instance; small key sets stay resident.
    pub spillable_subquery_stages: u64,
    /// Completed predicate inner populations retained in an actual native run.
    /// Excludes source replay, inner operators and output replay spill.
    pub spilled_subquery_stages: u64,
    pub spillable_subquery_input_rows: u64,
    /// Rows retained after complete key validation and null-correlation exclusion.
    pub spillable_subquery_indexed_rows: u64,
    /// Selected outer rows evaluated against those completed inner builds.
    pub spillable_subquery_probe_rows: u64,
    /// Exact-correlation candidate visits, including complete-key hash collisions.
    pub spillable_subquery_candidate_rows: u64,
    /// Positional blocks loaded; excludes source replay and ordering merge scans.
    pub spillable_subquery_lookup_blocks: u64,
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
    /// Cumulative observations of the shared session, not per-plan counters.
    pub runtime: ResidentSessionSnapshot,
    pub spill: Option<crate::relational_query::VortexRelationalSpillReport>,
    /// Present only after a streaming source's explicit end-of-input was observed.
    pub input: Option<ExecutedVortexBatchInput>,
    _metadata: MemoryLease,
}

/// One explicitly named plan-position observation with the exact native schema.
pub struct CompletedVortexRelationalObservation {
    pub name: String,
    pub result_jsonl: Budgeted<String>,
    pub result_schema_json: Budgeted<String>,
    _metadata: MemoryLease,
}

/// Complete small collection; streaming delivery has an independent row bound.
pub struct CollectedVortexRelational {
    pub timing: super::query_timing::QueryTiming,
    pub execution: ExecutedVortexRelational,
    pub result_jsonl: Budgeted<String>,
    pub result_schema_json: Budgeted<String>,
}

pub struct ExecutedOwnedVortexRelational {
    pub execution: ExecutedVortexRelational,
    pub result: OwnedVortexResultBatch,
}

/// Source identities and schemas are retained; operator populations are fresh.
/// An admitted immutable sampling root may retain credited scalar calibration.
pub struct PreparedVortexRelational {
    session: ResidentVortexSession,
    sources: Vec<PreparedVortexSource>,
    memory_sources: Vec<(shardloom_core::DatasetUri, ResidentMemorySource)>,
    batch_sources: Vec<batch_input::Source>,
    #[cfg_attr(not(feature = "vortex-write"), allow(dead_code))]
    source_paths: Vec<PathBuf>,
    root: PreparedRoot,
    provider: Option<provider::Prepared>,
    provider_selection_reason: &'static str,
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

type DynamicLowerer =
    dyn Fn(&mut VortexRelationalPreparation<'_>) -> Result<VortexRelationalPlan> + Send;

enum PreparedRoot {
    Bound(Box<Node>),
    Dynamic(Box<DynamicLowerer>),
}

enum SubqueryRelation {
    Bound(Box<Node>),
    Dynamic(DeferredRelation),
}

struct DeferredRelation {
    lower: Box<DynamicLowerer>,
    source_scope: Option<std::sync::Arc<[bind::SourceIndex]>>,
    source_metadata: Option<std::sync::Arc<MemoryLease>>,
}

struct Node {
    fields: Vec<(String, DType)>,
    kind: NodeKind,
}

impl Node {
    fn has_observations(&self) -> bool {
        match &self.kind {
            NodeKind::Pipeline { input, stages } => {
                stages
                    .iter()
                    .any(|stage| matches!(stage.kind, PipelineKind::Observe(_)))
                    || input.has_observations()
            }
            NodeKind::Project { input, .. }
            | NodeKind::Filter { input, .. }
            | NodeKind::Sort { input, .. }
            | NodeKind::Limit { input, .. }
            | NodeKind::Aggregate { input, .. }
            | NodeKind::Window { input, .. }
            | NodeKind::GapFill { input, .. }
            | NodeKind::Unary { input, .. } => input.has_observations(),
            NodeKind::Join { left, right, .. } | NodeKind::Set { left, right, .. } => {
                left.has_observations() || right.has_observations()
            }
            NodeKind::Subquery {
                input, relation, ..
            } => {
                input.has_observations()
                    || matches!(relation, SubqueryRelation::Bound(relation) if relation.has_observations())
            }
            NodeKind::Scan { .. } | NodeKind::Outer | NodeKind::CompletedPivot { .. } => false,
        }
    }

    /// Propagate only conservative bounds from already held metadata. An
    /// overflowing or unknown bound stays unknown; no input is sampled/replayed.
    fn upper_rows(&self, sources: &[PreparedVortexSource]) -> Option<u64> {
        use crate::relational_query::VortexRelationalJoinKind as JoinKind;
        match &self.kind {
            NodeKind::CompletedPivot { rows, .. } => Some(*rows as u64),
            // Row expansion is not bounded by source cardinality.
            NodeKind::GapFill { .. } => None,
            NodeKind::Outer => Some(1),
            NodeKind::Scan { source, .. } => match source {
                ScanSource::File(index) => Some(sources[*index].file().row_count()),
                ScanSource::Memory(projection) => Some(projection.source_rows() as u64),
                ScanSource::Batch { .. } => None,
            },
            NodeKind::Project { input, .. }
            | NodeKind::Filter { input, .. }
            | NodeKind::Sort { input, .. }
            | NodeKind::Window { input, .. }
            | NodeKind::Subquery { input, .. } => input.upper_rows(sources),
            NodeKind::Unary { input, operation } => {
                operation.upper_output_rows(input.upper_rows(sources))
            }
            NodeKind::Aggregate {
                input,
                spec,
                expansion,
            } => {
                if let Some(expansion) = expansion {
                    expansion.upper_rows(input.upper_rows(sources))
                } else if spec.groups.is_empty() {
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
            NodeKind::Pipeline { input, stages } => {
                let mut rows = input.upper_rows(sources);
                for stage in stages {
                    if let PipelineKind::Limit { offset, count } = stage.kind {
                        rows = Some(rows.map_or(count as u64, |rows| {
                            rows.saturating_sub(offset as u64).min(count as u64)
                        }));
                    }
                }
                rows
            }
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
                if spec.asof.is_some()
                    || matches!(spec.kind, JoinKind::LeftSemi | JoinKind::LeftAnti)
                {
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
    Batch {
        index: usize,
        projection: Box<PreparedMemoryProjection>,
    },
}

enum NodeKind {
    Pipeline {
        input: Box<Node>,
        stages: Vec<PipelineStage>,
    },
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
        expansion: Option<Box<grouping::Spec>>,
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
        output_prefix: Option<usize>,
    },
    Limit {
        input: Box<Node>,
        offset: usize,
        count: usize,
    },
    Scan {
        source: ScanSource,
        source_uri: shardloom_core::DatasetUri,
        // Validated predicate semantics survive scan pushdown for native
        // provider selection; the executable Vortex filter stays in `plan`.
        predicate: Option<shardloom_core::PredicateExpr>,
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
    GapFill {
        input: Box<Node>,
        spec: Box<super::native_relational_gap_fill::Spec>,
    },
    Subquery {
        input: Box<Node>,
        relation: SubqueryRelation,
        spec: native_relational_subquery::Spec,
        parameterized: bool,
    },
}

struct PipelineStage {
    fields: Vec<(String, DType)>,
    kind: PipelineKind,
}

enum PipelineKind {
    Project(Vec<native_relational_expression::Expression>),
    Filter(native_relational_expression::Expression),
    Limit { offset: usize, count: usize },
    Observe(Box<BoundObservation>),
}

type BoundProjection = (
    Vec<(String, DType)>,
    Vec<native_relational_expression::Expression>,
);

struct BoundObservation {
    name: String,
    projection: Option<BoundProjection>,
    spec: native_relational_aggregate::Spec,
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
    /// The current action's cancellation owner. Dynamic lowering must use this
    /// token instead of retaining a token from an earlier preparation/action.
    #[must_use]
    pub fn cancellation_token(&self) -> shardloom_exec::compute_pool::CancellationToken {
        self.binding.cancellation_token()
    }

    /// Register an already admitted native file without reopening its provider.
    /// # Errors
    /// Rejects a changed source, mismatched URI or a different resource owner.
    pub fn register_prepared_source(
        &mut self,
        uri: &shardloom_core::DatasetUri,
        source: PreparedVortexSource,
    ) -> Result<()> {
        self.binding.seed_source(uri, source)
    }

    /// Retain this preparation's existing reservation owner for an input adapter.
    /// Cloning the handle creates no additional grant. Imported buffers, retained
    /// sources and adapter scratch then compete for the same live capacity.
    #[must_use]
    pub fn shared_memory_pool(&self) -> shardloom_exec::live_memory::LiveMemoryPool {
        self.binding.shared_memory_pool()
    }

    /// Admit source-adapter scratch under the same grant as the eventual plan.
    /// This does not open a source or evaluate rows. A control transport can use
    /// a zero-byte lease as an owner before starting its credited reader.
    /// # Errors
    /// Rejects growth beyond the preparation's shared memory grant.
    pub fn reserve_input_scratch(&self, bytes: u64) -> Result<MemoryLease> {
        self.binding.reserve_input_scratch(bytes)
    }

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

    /// Declare a streaming source without opening its producer. Build a
    /// typed empty owner with `ResidentMemorySource::from_batch_columns`.
    /// The complete lowered plan admits Scan/Filter/Project/Sort/Aggregate and
    /// draining Limit/Offset. Stateful operators share the query grant; native
    /// ordering and general aggregation support an explicit spill policy.
    /// # Errors
    /// Rejects duplicate URIs, excess declarations, nonempty schemas and foreign ownership.
    pub fn register_batch_source(
        &mut self,
        uri: shardloom_core::DatasetUri,
        build_schema: impl FnOnce(&ResidentVortexSession) -> Result<ResidentMemorySource>,
    ) -> Result<()> {
        self.binding.register_batch_source(uri, build_schema)
    }

    /// Register an already-admitted local columnar source without decoding data.
    /// The bound native plan determines its physical demand before execution.
    /// # Errors
    /// Rejects duplicate sources, unsupported schema, cancellation and pressure.
    #[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
    pub fn register_local_source(
        &mut self,
        uri: shardloom_core::DatasetUri,
        source: std::sync::Arc<crate::universal_format_io::demanded::LocalColumnarSource>,
    ) -> Result<()> {
        self.binding.register_local_source(uri, source)
    }

    /// Resolve public column names from the retained native source schema,
    /// excluding private derived metadata just as an all-column scan does.
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
    prepare_relational_with_owner(policy, session, None, lower, &CancellationToken::default())
}

/// Lower a frontend inside an existing session's allocation. Input conversion,
/// previously retained results and this plan compete for the same memory credits.
/// No additional runtime or resource grant is created.
/// # Errors
/// Rejects inconsistent resource policy, a session exceeding the requested grant,
/// frontend errors, source changes and native preparation failures.
pub fn prepare_relational_with_schema_in_session(
    policy: VortexLocalPrimitiveExecutionPolicy,
    session: &ResidentVortexSession,
    lower: impl FnOnce(&mut VortexRelationalPreparation<'_>) -> Result<VortexRelationalPlan>,
) -> Result<PreparedVortexRelational> {
    prepare_relational_with_schema_in_session_controlled(
        policy,
        session,
        lower,
        &CancellationToken::default(),
    )
}

/// Lower a frontend using the caller's allocation and cancellation owner.
/// # Errors
/// Rejects cancellation, resource denial and all ordinary native binding failures.
pub fn prepare_relational_with_schema_in_session_controlled(
    policy: VortexLocalPrimitiveExecutionPolicy,
    session: &ResidentVortexSession,
    lower: impl FnOnce(&mut VortexRelationalPreparation<'_>) -> Result<VortexRelationalPlan>,
    cancellation: &CancellationToken,
) -> Result<PreparedVortexRelational> {
    prepare_relational_with_owner(policy, session.clone(), None, lower, cancellation)
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
    prepare_relational_from_source_controlled(
        uri,
        source,
        policy,
        lower,
        &CancellationToken::default(),
    )
}

/// Bind a retained source with the caller's cancellation and deadline owner.
/// # Errors
/// Rejects cancellation, source/request/resource mismatch and native bind errors.
pub fn prepare_relational_from_source_controlled(
    uri: shardloom_core::DatasetUri,
    source: PreparedVortexSource,
    policy: VortexLocalPrimitiveExecutionPolicy,
    lower: impl FnOnce(&mut VortexRelationalPreparation<'_>) -> Result<VortexRelationalPlan>,
    cancellation: &CancellationToken,
) -> Result<PreparedVortexRelational> {
    cancellation.check()?;
    let request =
        VortexQueryPrimitiveRequest::project(uri.clone(), shardloom_plan::ProjectionRequest::All);
    let session = super::prepared_dispatch::source_session(&source, &request, Some(policy))?;
    prepare_relational_with_owner(policy, session, Some((uri, source)), lower, cancellation)
}

fn prepare_relational_with_owner(
    policy: VortexLocalPrimitiveExecutionPolicy,
    session: ResidentVortexSession,
    source: Option<(shardloom_core::DatasetUri, PreparedVortexSource)>,
    lower: impl FnOnce(&mut VortexRelationalPreparation<'_>) -> Result<VortexRelationalPlan>,
    cancellation: &CancellationToken,
) -> Result<PreparedVortexRelational> {
    cancellation.check()?;
    validate_session_policy(policy, &session)?;
    let mut preparation = VortexRelationalPreparation {
        binding: bind::Binder::new_with_cancellation(&session, cancellation)?,
    };
    if let Some((uri, source)) = source {
        preparation.binding.seed_source(&uri, source)?;
    }
    let plan = lower(&mut preparation)?;
    cancellation.check()?;
    preparation.binding.validate_batch_plan(&plan)?;
    let mut root = preparation.binding.bind(&plan, 0)?;
    #[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
    preparation.binding.apply_source_demand(&mut root)?;
    let bind::BoundSources {
        sources,
        source_paths,
        memory_sources,
        batch_sources,
        metadata,
    } = preparation.binding.finish()?;
    if sources.is_empty() && batch_sources.is_empty() && memory_sources.len() == 1 {
        root.admit_sampling_preparation(session.memory())?;
    }
    let provider = if sources.len() == 1 && memory_sources.is_empty() && batch_sources.is_empty() {
        provider::prepare(&root, &sources[0], policy, session.memory())?
    } else {
        provider::Selection::declined("multiple_or_nonfile_sources_require_relational_execution")
    };
    Ok(PreparedVortexRelational {
        session,
        sources,
        memory_sources,
        batch_sources,
        source_paths,
        root: PreparedRoot::Bound(Box::new(root)),
        provider: provider.provider,
        provider_selection_reason: provider.reason,
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
    validate_session_policy(policy, session)?;
    let mut binding = bind::Binder::new(session)?;
    let mut root = binding.bind(plan, 0)?;
    #[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
    binding.apply_source_demand(&mut root)?;
    let bind::BoundSources {
        sources,
        source_paths,
        memory_sources,
        batch_sources,
        metadata,
    } = binding.finish()?;
    if sources.is_empty() && batch_sources.is_empty() && memory_sources.len() == 1 {
        root.admit_sampling_preparation(session.memory())?;
    }
    let provider = if sources.len() == 1 && memory_sources.is_empty() && batch_sources.is_empty() {
        provider::prepare(&root, &sources[0], policy, session.memory())?
    } else {
        provider::Selection::declined("multiple_or_nonfile_sources_require_relational_execution")
    };
    Ok(PreparedVortexRelational {
        session: session.clone(),
        sources,
        memory_sources,
        batch_sources,
        source_paths,
        root: PreparedRoot::Bound(Box::new(root)),
        provider: provider.provider,
        provider_selection_reason: provider.reason,
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

/// Check a caller-owned session before an adapter inspects or prepares inputs.
/// This does not access sources, allocate another grant or execute a plan.
/// # Errors
/// Rejects inconsistent policy or a session exceeding the requested allocation.
pub fn validate_session_policy(
    policy: VortexLocalPrimitiveExecutionPolicy,
    session: &ResidentVortexSession,
) -> Result<()> {
    if policy.max_parallelism == 0
        || policy.max_parallelism != policy.resource_envelope.max_parallelism
        || session.parallelism() > policy.max_parallelism
        || session.memory().snapshot().limit_bytes > policy.resource_envelope.memory_budget_bytes
    {
        return Err(failed("session exceeds the requested resource grant"));
    }
    Ok(())
}

#[derive(Clone, Copy, Default)]
enum FileScanBatching {
    /// Preserve shared encoded buffers across native result batches.
    #[default]
    Native,
    /// Bound file reads before a consumer materializes each output batch.
    Output,
}

#[derive(Default)]
struct Metrics<'a> {
    observations: RefCell<Option<ReservedVec<CompletedVortexRelationalObservation>>>,
    #[cfg(feature = "vortex-write")]
    spill: Option<super::native_relational_spill::State>,
    scan_rows: Cell<u64>,
    scan_batches: Cell<u64>,
    scans_started: Cell<u64>,
    scans_pruned: Cell<u64>,
    data_scans: Cell<u64>,
    file_scan_batching: Cell<FileScanBatching>,
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
    ordered_aggregate_quantile_rows: Cell<u64>,
    ordered_set_stages: Cell<u64>,
    ordered_set_input_rows: Cell<u64>,
    ordered_set_groups: Cell<u64>,
    ordered_set_output_rows: Cell<u64>,
    spillable_unary_membership_stages: Cell<u64>,
    spillable_unary_input_rows: Cell<u64>,
    spillable_unary_observed_rows: Cell<u64>,
    spillable_unary_groups: Cell<u64>,
    spillable_unary_identity_rows: Cell<u64>,
    spillable_unary_output_rows: Cell<u64>,
    spillable_unary_lookup_blocks: Cell<u64>,
    spillable_unary_reader_opens: Cell<u64>,
    spillable_sampling_stages: Cell<u64>,
    spillable_sampling_input_rows: Cell<u64>,
    spillable_sampling_population_rows: Cell<u64>,
    spillable_sampling_output_rows: Cell<u64>,
    spillable_sampling_replacement_draws: Cell<u64>,
    spillable_sampling_lookup_blocks: Cell<u64>,
    spillable_subquery_stages: Cell<u64>,
    spilled_subquery_stages: Cell<u64>,
    spillable_subquery_input_rows: Cell<u64>,
    spillable_subquery_indexed_rows: Cell<u64>,
    spillable_subquery_probe_rows: Cell<u64>,
    spillable_subquery_candidate_rows: Cell<u64>,
    spillable_subquery_lookup_blocks: Cell<u64>,
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
    ordering_payload_buffer_bytes_copied: Cell<u64>,
    ordering_prefix_batches: Cell<u64>,
    ordering_prefix_rows_validated: Cell<u64>,
    ordering_prefix_peak_retained_buffer_bytes: Cell<u64>,
    join_build_batches_detached: Cell<u64>,
    join_build_rows_detached: Cell<u64>,
    window_batches_detached: Cell<u64>,
    window_rows_detached: Cell<u64>,
    subquery_build_batches_detached: Cell<u64>,
    subquery_build_rows_detached: Cell<u64>,
}

impl Metrics<'_> {
    fn record_subquery(&self, report: &super::native_relational_subquery::Report) -> Result<()> {
        add(&self.spillable_subquery_stages, report.stages)?;
        add(&self.spilled_subquery_stages, report.spilled_stages)?;
        add(&self.spillable_subquery_input_rows, report.input_rows)?;
        add(&self.spillable_subquery_indexed_rows, report.indexed_rows)?;
        add(&self.spillable_subquery_probe_rows, report.probe_rows)?;
        add(
            &self.spillable_subquery_candidate_rows,
            report.candidate_rows,
        )?;
        add(&self.spillable_subquery_lookup_blocks, report.lookup_blocks)
    }

    fn detach_subquery_build(
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
        self.record_detached_subquery_build(array.len())?;
        Ok(detached)
    }

    fn record_detached_subquery_build(&self, rows: usize) -> Result<()> {
        if self.input.is_some() && rows != 0 {
            add(&self.subquery_build_batches_detached, 1)?;
            add(&self.subquery_build_rows_detached, rows as u64)?;
        }
        Ok(())
    }

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
        self.record_ordering_copy(array.len(), detached.nbytes())?;
        Ok(detached)
    }

    fn record_ordering_copy(&self, rows: usize, buffer_bytes: u64) -> Result<()> {
        add(&self.ordering_batches_detached, 1)?;
        add(&self.ordering_rows_detached, rows as u64)?;
        add(&self.ordering_payload_buffer_bytes_copied, buffer_bytes)
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
        // Explicit relational spill keeps its established operator strategy.
        // Never select a non-spill provider then retry after resource failure.
        self.provider = None;
        self.provider_selection_reason = "explicit_relational_spill_policy";
        self.disable_sampling_preparation();
        Ok(self)
    }

    /// Attach the original compatibility generations to this normalized native plan.
    /// Checks cover collection, every writer's final commit and output aliases.
    /// # Errors
    /// Rejects changed preparation identities or metadata beyond the shared grant.
    #[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
    pub fn with_preparation_sources(
        mut self,
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
        if !sources.is_empty() {
            self.disable_sampling_preparation();
        }
        Ok(Self {
            preparation_sources: sources,
            _preparation_metadata: Some(metadata),
            ..self
        })
    }

    #[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
    fn validate_preparation_sources(&self, cancellation: &CancellationToken) -> Result<()> {
        for source in &self.preparation_sources {
            source.validate_generation_controlled(cancellation)?;
        }
        for source in &self.batch_sources {
            if let Some(source) = &source.local {
                source.source.validate_generation_controlled(cancellation)?;
            }
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

    /// Serialize admitted output metadata without starting a consumer or reading
    /// rows. Data-dependent schemas stay unavailable until execution completes.
    /// # Errors
    /// Returns schema serialization or shared memory admission errors.
    pub fn output_schema_json(
        &self,
    ) -> Result<Option<shardloom_exec::live_memory::Budgeted<String>>> {
        self.output_dtype()
            .map(|dtype| {
                super::collect::serialize_result_schema(&dtype, &self.session.shared_memory_pool())
            })
            .transpose()
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
        self.for_each_batch_with_input(
            cancellation,
            BATCH_ROWS,
            FileScanBatching::Native,
            None,
            consume,
        )
    }

    fn for_each_batch_with_input(
        &self,
        cancellation: &CancellationToken,
        batch_rows: usize,
        file_scan_batching: FileScanBatching,
        input: Option<batch_input::Provider<'_>>,
        consume: impl FnMut(ArrayRef, &NativeExecutionContext<'_>) -> Result<()>,
    ) -> Result<ExecutedVortexRelational> {
        self.for_each_batch_with_input_and_initialize(
            cancellation,
            batch_rows,
            file_scan_batching,
            input,
            || Ok(()),
            consume,
        )
    }

    /// Policy initialization belongs to the complete native call, before any
    /// source or output demand. Ordinary callers have no additional policy.
    fn for_each_batch_with_input_and_initialize(
        &self,
        cancellation: &CancellationToken,
        batch_rows: usize,
        file_scan_batching: FileScanBatching,
        input: Option<batch_input::Provider<'_>>,
        initialize: impl FnOnce() -> Result<()>,
        mut consume: impl FnMut(ArrayRef, &NativeExecutionContext<'_>) -> Result<()>,
    ) -> Result<ExecutedVortexRelational> {
        let sampling = self.begin_sampling_call()?;
        #[cfg(feature = "output-window-observation")]
        let mut observed_engine = crate::output_window_observation::engine_scope();
        cancellation.check()?;
        self.validate_batch_provider(input.is_some())?;
        initialize()?;
        let mut execution =
            self.session
                .with_sources_execution(&self.sources, cancellation, |context| {
                    self.consume_in_context(
                        context,
                        batch_rows,
                        file_scan_batching,
                        input,
                        &mut |array| {
                            #[cfg(feature = "output-window-observation")]
                            let mut observed_sink =
                                crate::output_window_observation::sink_scope(array.len());
                            let result = consume(array, context);
                            #[cfg(feature = "output-window-observation")]
                            if result.is_ok() {
                                observed_sink.complete();
                            }
                            result
                        },
                    )
                })?;
        execution.runtime = self.snapshot();
        #[cfg(feature = "output-window-observation")]
        observed_engine.complete();
        sampling.complete();
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
        input: Option<batch_input::Provider<'_>>,
        mut consume: impl FnMut(super::collect::SerializedVortexResultBatch) -> Result<()>,
    ) -> Result<ExecutedVortexRelational> {
        let sampling = self.sampling_failure_guard();
        if batch_rows == 0
            || batch_rows > BATCH_ROWS
            || max_batch_bytes == 0
            || max_batch_bytes > 8 * 1024 * 1024
        {
            return Err(failed("JSON batches require 1..=2,048 rows and 1..=8 MiB"));
        }
        let mut execution = self.for_each_batch_with_input(
            cancellation,
            batch_rows,
            FileScanBatching::Output,
            input,
            |array, context| {
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
            },
        )?;
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
        sampling.complete();
        Ok(execution)
    }

    fn consume_in_context(
        &self,
        context: &NativeExecutionContext<'_>,
        batch_rows: usize,
        file_scan_batching: FileScanBatching,
        input: Option<batch_input::Provider<'_>>,
        consume: &mut dyn FnMut(ArrayRef) -> Result<()>,
    ) -> Result<ExecutedVortexRelational> {
        self.with_bound_root(context, input, |root, metrics| {
            // Dynamic schema discovery has its own internal native work units.
            // This policy applies to the now-bound terminal consumption.
            metrics.file_scan_batching.set(file_scan_batching);
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
        self.consume_bound_with_completion(root, context, metrics, batch_rows, consume, || Ok(()))
            .map(|(execution, ())| execution)
    }

    // Finish provisional consumers while the query's single native spill owner
    // remains live. Only successful completion and cleanup can be certified.
    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    fn consume_bound_with_completion<T>(
        &self,
        root: &Node,
        context: &NativeExecutionContext<'_>,
        metrics: &Metrics<'_>,
        batch_rows: usize,
        consume: &mut dyn FnMut(ArrayRef) -> Result<()>,
        complete: impl FnOnce() -> Result<T>,
    ) -> Result<(ExecutedVortexRelational, T)> {
        #[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
        self.validate_preparation_sources(context.cancellation())?;
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
            let array = if nested || !self.batch_sources.is_empty() {
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
        let provider = if let Some(provider) = &self.provider {
            Some(provider.consume(context, metrics, batch_rows, &mut emit)?)
        } else {
            self.run(root, context, metrics, batch_rows, None, &mut emit)?;
            None
        };
        let mut input = metrics
            .input
            .map(|input| input.completed(context, metrics))
            .transpose()?;
        if let Some(input) = &mut input {
            input.ordering_batches_detached = metrics.ordering_batches_detached.get();
            input.ordering_rows_detached = metrics.ordering_rows_detached.get();
            input.ordering_payload_buffer_bytes_copied =
                metrics.ordering_payload_buffer_bytes_copied.get();
            input.ordering_prefix_batches = metrics.ordering_prefix_batches.get();
            input.ordering_prefix_rows_validated = metrics.ordering_prefix_rows_validated.get();
            input.ordering_prefix_peak_retained_buffer_bytes =
                metrics.ordering_prefix_peak_retained_buffer_bytes.get();
            input.join_build_batches_detached = metrics.join_build_batches_detached.get();
            input.join_build_rows_detached = metrics.join_build_rows_detached.get();
            input.window_batches_detached = metrics.window_batches_detached.get();
            input.window_rows_detached = metrics.window_rows_detached.get();
            input.subquery_build_batches_detached = metrics.subquery_build_batches_detached.get();
            input.subquery_build_rows_detached = metrics.subquery_build_rows_detached.get();
        }
        // No input schema sampling, and no missing-schema sentinel for empty output.
        if !emitted.get() {
            let array = super::native_payload::empty_record(&dtype, context)?;
            emit(array)?;
        }
        context.check_cancelled()?;
        let completed = complete()?;
        context.check_cancelled()?;
        #[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
        self.validate_preparation_sources(context.cancellation())?;
        #[cfg(feature = "vortex-write")]
        let spill = metrics
            .spill
            .as_ref()
            .map(super::native_relational_spill::State::finish)
            .transpose()?;
        #[cfg(not(feature = "vortex-write"))]
        let spill: Option<crate::relational_query::VortexRelationalSpillReport> = None;
        let (primitive_execution, certificate) = provider.map_or((None, None), |provider| {
            (provider.report, provider.certificate)
        });
        let certificate = match certificate {
            Some(certificate) => certificate,
            None => report::certificate(
                metrics,
                rows,
                batch_rows,
                self.sources.len(),
                self.memory_sources.len(),
                input.as_ref(),
                spill.as_ref(),
            )?,
        };
        let (observations, observation_metadata) =
            metrics.observations.borrow_mut().take().map_or_else(
                || (vec![], None),
                |observations| {
                    let (values, lease) = observations.into_parts();
                    (values, Some(lease))
                },
            );
        Ok((
            ExecutedVortexRelational {
                observations,
                _observation_metadata: observation_metadata,
                primitive_execution,
                provider_selection_reason: self.provider_selection_reason,
                resources: self.policy.resource_envelope.declared_resources,
                output_rows: rows,
                output_batches: batches,
                max_output_batch_rows: max_rows,
                output_buffer_bytes: bytes,
                scan_rows_delivered: metrics.scan_rows.get(),
                scan_batches: metrics.scan_batches.get(),
                prepared_sources: self.sources.len()
                    + self.memory_sources.len()
                    + self.batch_sources.len(),
                prepared_file_sources: self.sources.len(),
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
                ordered_aggregate_quantile_rows: metrics.ordered_aggregate_quantile_rows.get(),
                ordered_set_stages: metrics.ordered_set_stages.get(),
                ordered_set_input_rows: metrics.ordered_set_input_rows.get(),
                ordered_set_groups: metrics.ordered_set_groups.get(),
                ordered_set_output_rows: metrics.ordered_set_output_rows.get(),
                spillable_unary_membership_stages: metrics.spillable_unary_membership_stages.get(),
                spillable_unary_input_rows: metrics.spillable_unary_input_rows.get(),
                spillable_unary_observed_rows: metrics.spillable_unary_observed_rows.get(),
                spillable_unary_groups: metrics.spillable_unary_groups.get(),
                spillable_unary_identity_rows: metrics.spillable_unary_identity_rows.get(),
                spillable_unary_output_rows: metrics.spillable_unary_output_rows.get(),
                spillable_unary_lookup_blocks: metrics.spillable_unary_lookup_blocks.get(),
                spillable_unary_reader_opens: metrics.spillable_unary_reader_opens.get(),
                spillable_sampling_stages: metrics.spillable_sampling_stages.get(),
                spillable_sampling_input_rows: metrics.spillable_sampling_input_rows.get(),
                spillable_sampling_population_rows: metrics
                    .spillable_sampling_population_rows
                    .get(),
                spillable_sampling_output_rows: metrics.spillable_sampling_output_rows.get(),
                spillable_sampling_replacement_draws: metrics
                    .spillable_sampling_replacement_draws
                    .get(),
                spillable_sampling_lookup_blocks: metrics.spillable_sampling_lookup_blocks.get(),
                spillable_subquery_stages: metrics.spillable_subquery_stages.get(),
                spilled_subquery_stages: metrics.spilled_subquery_stages.get(),
                spillable_subquery_input_rows: metrics.spillable_subquery_input_rows.get(),
                spillable_subquery_indexed_rows: metrics.spillable_subquery_indexed_rows.get(),
                spillable_subquery_probe_rows: metrics.spillable_subquery_probe_rows.get(),
                spillable_subquery_candidate_rows: metrics.spillable_subquery_candidate_rows.get(),
                spillable_subquery_lookup_blocks: metrics.spillable_subquery_lookup_blocks.get(),
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
                ordered_window_extrema_summary_rows: metrics
                    .ordered_window_extrema_summary_rows
                    .get(),
                ordered_window_lookup_blocks: metrics.ordered_window_lookup_blocks.get(),
                output_columns: root.fields.iter().map(|(name, _)| name.clone()).collect(),
                bytes_decoded: None,
                native_io_certificate: certificate,
                runtime: self.snapshot(),
                spill,
                input,
                _metadata: metadata,
            },
            completed,
        ))
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
                #[cfg(all(feature = "vortex-write", unix))]
                if let Some(spill) = &metrics.spill
                    && operation.sampling_spill_supported(input.upper_rows(&self.sources))
                {
                    let (usage, report) = operation.consume_relation_sampling(
                        context,
                        spill,
                        batch_rows,
                        |accept| self.run(input, context, metrics, batch_rows, parameter, accept),
                        consume,
                    )?;
                    metrics.record_unary(usage.items, usage.all_input_retained)?;
                    add(&metrics.spillable_sampling_stages, 1)?;
                    add(&metrics.spillable_sampling_input_rows, report.input_rows)?;
                    add(
                        &metrics.spillable_sampling_population_rows,
                        report.population_rows,
                    )?;
                    add(&metrics.spillable_sampling_output_rows, report.output_rows)?;
                    add(
                        &metrics.spillable_sampling_replacement_draws,
                        report.replacement_draws,
                    )?;
                    return add(
                        &metrics.spillable_sampling_lookup_blocks,
                        report.lookup_blocks,
                    );
                }
                #[cfg(all(feature = "vortex-write", unix))]
                if let Some(spill) = &metrics.spill
                    && operation.membership_spill_supported()
                {
                    let (usage, report) = operation.consume_relation_membership(
                        context,
                        spill,
                        batch_rows,
                        |accept| self.run(input, context, metrics, batch_rows, parameter, accept),
                        consume,
                    )?;
                    metrics.record_unary(usage.items, usage.all_input_retained)?;
                    add(&metrics.spillable_unary_membership_stages, 1)?;
                    add(&metrics.spillable_unary_input_rows, report.input_rows)?;
                    add(&metrics.spillable_unary_observed_rows, report.observed_rows)?;
                    add(&metrics.spillable_unary_groups, report.groups)?;
                    add(&metrics.spillable_unary_identity_rows, report.identity_rows)?;
                    add(&metrics.spillable_unary_output_rows, report.output_rows)?;
                    add(&metrics.spillable_unary_lookup_blocks, report.lookup_blocks)?;
                    return add(&metrics.spillable_unary_reader_opens, report.reader_opens);
                }
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
            NodeKind::Aggregate {
                input,
                spec,
                expansion,
            } => {
                let produce = |accept: &mut dyn FnMut(ArrayRef) -> Result<()>| {
                    grouping::produce(
                        expansion.as_deref(),
                        context,
                        |accept| self.run(input, context, metrics, batch_rows, parameter, accept),
                        accept,
                    )
                };
                #[cfg(feature = "vortex-write")]
                if let Some(spill) = &metrics.spill {
                    let report = super::native_relational_aggregate_spill::run(
                        spec,
                        expansion
                            .as_ref()
                            .map_or(&input.fields, |spec| &spec.fields),
                        spill,
                        context,
                        batch_rows,
                        produce,
                        consume,
                    )?;
                    add(&metrics.ordered_aggregate_stages, 1)?;
                    add(&metrics.ordered_aggregate_input_rows, report.input_rows)?;
                    add(
                        &metrics.ordered_aggregate_distinct_rows,
                        report.distinct_rows,
                    )?;
                    add(
                        &metrics.ordered_aggregate_quantile_rows,
                        report.quantile_rows,
                    )?;
                    return Ok(());
                }
                let mut aggregate =
                    native_relational_aggregate::Aggregate::new(spec, context.memory())?;
                produce(&mut |array| aggregate.consume(&array, context, batch_rows))?;
                aggregate.finish(context, batch_rows, consume)
            }
            NodeKind::Project { .. }
            | NodeKind::Filter { .. }
            | NodeKind::Sort { .. }
            | NodeKind::Limit { .. } => {
                self.run_transform(node, context, metrics, batch_rows, parameter, consume)
            }
            NodeKind::Pipeline { input, stages } => self.run_pipeline(
                input, stages, context, metrics, batch_rows, parameter, consume,
            ),
            NodeKind::Scan {
                source,
                plan,
                columns,
                residual,
                ..
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
            NodeKind::GapFill { input, spec } => {
                let mut fill = super::native_relational_gap_fill::GapFill::new(spec);
                let mut output = super::native_relational_output_buffer::OutputBuffer::new(
                    spec.dtype.clone(),
                    batch_rows,
                    super::native_payload::CopyPolicy::ValidateValues,
                    context,
                )?;
                let mut emit = |array| output.push(array, context, consume);
                self.run(
                    input,
                    context,
                    metrics,
                    batch_rows,
                    parameter,
                    &mut |array| fill.build(array, context, batch_rows, &mut emit),
                )?;
                fill.finish(context, batch_rows, &mut emit)?;
                output.flush(context, consume)
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
                    let report = if spec.asof.is_some() {
                        native_relational_join::asof::spill::run(
                            spec,
                            &right.fields,
                            spill,
                            context,
                            batch_rows,
                            |accept| {
                                self.run(right, context, metrics, batch_rows, parameter, accept)
                            },
                            |accept| {
                                self.run(left, context, metrics, batch_rows, parameter, accept)
                            },
                            consume,
                        )?
                    } else {
                        native_relational_join::spill::run(
                            spec,
                            &right.fields,
                            spill,
                            context,
                            batch_rows,
                            |accept| {
                                self.run(right, context, metrics, batch_rows, parameter, accept)
                            },
                            |accept| {
                                self.run(left, context, metrics, batch_rows, parameter, accept)
                            },
                            consume,
                        )?
                    };
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
                if spec.asof.is_some() {
                    let mut join = native_relational_join::asof::Join::new(spec, context.memory())?;
                    self.run(
                        right,
                        context,
                        metrics,
                        batch_rows,
                        parameter,
                        &mut |array| {
                            join.build(metrics.detach_join_build(array, context)?, context)
                        },
                    )?;
                    join.seal(context)?;
                    return self.run(
                        left,
                        context,
                        metrics,
                        batch_rows,
                        parameter,
                        &mut |array| join.consume(array, context, batch_rows, consume),
                    );
                }
                if let Some(cyclic) = cyclic_exists::Plan::admit(left, right, spec) {
                    return cyclic.run(self, context, metrics, batch_rows, parameter, consume);
                }
                let mut join = native_relational_join::Join::new(spec, context.memory())?;
                self.run(
                    right,
                    context,
                    metrics,
                    batch_rows,
                    parameter,
                    &mut |array| {
                        join.build(
                            metrics.detach_join_build(array, context)?,
                            context,
                            batch_rows,
                        )
                    },
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

    #[allow(clippy::too_many_lines)] // Keep streaming, authorized spill and resident membership under the same alignment boundary.
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
        #[cfg(feature = "vortex-write")]
        if let Some(spill) = &metrics.spill {
            let report = super::native_relational_set_spill::run(
                *kind,
                &node.fields,
                spill,
                context,
                batch_rows,
                |is_left, accept| {
                    let input = if is_left { left } else { right };
                    self.run(
                        input,
                        context,
                        metrics,
                        batch_rows,
                        parameter,
                        &mut |array| {
                            accept(cast_batch(&array, &input.fields, &node.fields, context)?)
                        },
                    )
                },
                consume,
            )?;
            add(&metrics.ordered_set_stages, 1)?;
            add(&metrics.ordered_set_input_rows, report.input_rows)?;
            add(&metrics.ordered_set_groups, report.groups)?;
            add(&metrics.ordered_set_output_rows, report.output_rows)?;
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
        input: Option<batch_input::Provider<'_>>,
    ) -> Result<CollectedVortexRelational> {
        let started = std::time::Instant::now();
        let sampling = self.sampling_failure_guard();
        let mut sink = super::collect::JsonRows::new(self.session.memory(), 8 * 1024 * 1024, true)?;
        let mut execution = self.for_each_batch_with_input(
            cancellation,
            BATCH_ROWS,
            FileScanBatching::Output,
            input,
            |array, context| sink.append_native(&array, context),
        )?;
        #[cfg(test)]
        sample_preparation::BEFORE_JSON_COMPLETION.with_borrow_mut(|slot| {
            if let Some(before) = slot.take() {
                before();
            }
        });
        let (result_jsonl, result_schema_json, timing) =
            sink.finish_with_timing(&mut execution.native_io_certificate, started)?;
        execution.runtime = self.snapshot();
        sampling.complete();
        Ok(CollectedVortexRelational {
            timing,
            execution,
            result_jsonl,
            result_schema_json,
        })
    }

    /// Retain complete small native results; each final buffer owns its credits.
    /// # Errors
    /// Denies results above the small collection boundary; use streaming or a writer.
    pub fn execute_owned(&self) -> Result<ExecutedOwnedVortexRelational> {
        self.retain_complete_result(&CancellationToken::default(), true, None)
    }

    /// Retain complete native batches under the existing shared allocation.
    /// This explicit materialization has no synthetic row or logical-byte limit;
    /// live input, operator state and retained output must fit the admitted grant.
    /// Use `for_each_batch` when the consumer can release batches incrementally.
    /// No Python rows, JSON, intermediate file or query replay is involved.
    /// # Errors
    /// Propagates cancellation, source changes, arithmetic overflow and shared
    /// resource denial. Failed executions publish no partially completed result.
    pub fn execute_native(
        &self,
        cancellation: &CancellationToken,
    ) -> Result<ExecutedOwnedVortexRelational> {
        self.retain_complete_result(cancellation, false, None)
    }

    fn retain_complete_result(
        &self,
        cancellation: &CancellationToken,
        small_collection: bool,
        input: Option<batch_input::Provider<'_>>,
    ) -> Result<ExecutedOwnedVortexRelational> {
        let sampling = self.sampling_failure_guard();
        let mut arrays = ReservedVec::new(self.session.memory())?;
        let mut rows = 0usize;
        let mut bytes = 0u64;
        let mut execution = self.for_each_batch_with_input(
            cancellation,
            BATCH_ROWS,
            FileScanBatching::Native,
            input,
            |array, _| {
                rows = rows
                    .checked_add(array.len())
                    .ok_or_else(|| failed("collection row overflow"))?;
                bytes = bytes
                    .checked_add(array.nbytes())
                    .ok_or_else(|| failed("collection byte overflow"))?;
                if small_collection && (rows > 65_536 || bytes > 8 * 1024 * 1024) {
                    return Err(failed(
                        "small collection exceeds 65536 rows or 8 MiB; use streaming output",
                    ));
                }
                arrays.push(array)
            },
        )?;
        let (mut arrays, ownership) = arrays.into_parts();
        #[cfg(test)]
        sample_preparation::BEFORE_RESULT_OWNERSHIP.with_borrow_mut(|slot| {
            if let Some(before) = slot.take() {
                before(&mut arrays);
            }
        });
        #[cfg(not(test))]
        let _ = &mut arrays;
        let result = self.session.own_completed_arrays(arrays, ownership)?;
        execution.runtime = self.snapshot();
        sampling.complete();
        Ok(ExecutedOwnedVortexRelational { execution, result })
    }
}

fn select_batch(
    array: &ArrayRef,
    fields: &[(String, DType)],
    rows: &[usize],
    context: &NativeExecutionContext<'_>,
) -> Result<ArrayRef> {
    context.check_cancelled()?;
    let start = rows.first().copied().unwrap_or(0);
    let end = start.checked_add(rows.len());
    let mut contiguous = end.is_some();
    if contiguous {
        for (offset, row) in rows.iter().enumerate() {
            if offset.is_multiple_of(1024) {
                context.check_cancelled()?;
            }
            if *row != start + offset {
                contiguous = false;
                break;
            }
        }
    }
    let selected = if let Some(end) = end.filter(|_| contiguous) {
        // Ordered scan batches and consecutive filter matches need only a
        // native view. A gather would retain one u64 index per selected row.
        array.slice(start..end).map_err(vortex_error)?
    } else {
        let indices = index_array(rows.len(), false, context, |row| Ok(Some(rows[row])))?;
        array.take(indices).map_err(vortex_error)?
    };
    project_selected_batch(&selected, fields, context)
}

fn project_selected_batch(
    selected: &ArrayRef,
    fields: &[(String, DType)],
    context: &NativeExecutionContext<'_>,
) -> Result<ArrayRef> {
    let mut columns = ReservedVec::new(context.memory())?;
    columns.reserve(fields.len())?;
    for (name, _) in fields {
        columns
            .values
            .push(super::logical_field_from_native_array(selected, name)?);
    }
    let (columns, _ownership) = columns.into_parts();
    StructArray::try_new(
        fields
            .iter()
            .map(|(name, _)| name.as_str())
            .collect::<FieldNames>(),
        columns,
        selected.len(),
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
