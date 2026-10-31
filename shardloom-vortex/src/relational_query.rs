//! Typed relational requests shared by native Rust and public front ends.
//! Static schemas bind at preparation; data-dependent schemas bind inside the
//! admitted execution. Requests never store result rows.

use shardloom_core::{ColumnRef, ComparisonOp, DatasetUri, Expression, PredicateExpr};
use shardloom_plan::ProjectionRequest;

/// The existing recursive native binder limit, independent of total plan or
/// expression count. Frontends use it before copying a recursively owned tree.
pub const NATIVE_RECURSION_LIMIT: usize = 24;

#[path = "relational_window_frame.rs"]
mod window_frame;
pub use window_frame::{
    VortexRelationalFrameBound, VortexRelationalFrameExclusion, VortexRelationalFrameFunction,
    VortexRelationalFrameOffset, VortexRelationalFrameUnit, VortexRelationalWindowFrame,
};

/// Explicit permission for native ordering, aggregate, join, window and pivot runs. The buffer
/// threshold controls flushing; the resident query pool remains the memory grant.
/// Construction validates configuration without inspecting the filesystem.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VortexRelationalSpillPolicy {
    pub workspace: std::path::PathBuf,
    pub quota_bytes: u64,
    pub buffer_bytes: u64,
}

impl VortexRelationalSpillPolicy {
    /// # Errors
    /// Rejects relative paths, less than 32 KiB disk quota, and a retained-input
    /// threshold below 1 MiB. Execution requires an existing real directory.
    pub fn new(
        workspace: impl Into<std::path::PathBuf>,
        quota_bytes: u64,
        buffer_bytes: u64,
    ) -> shardloom_core::Result<Self> {
        let workspace = workspace.into();
        if !workspace.is_absolute() || quota_bytes < 32 * 1024 || buffer_bytes < 1024 * 1024 {
            return Err(shardloom_core::ShardLoomError::InvalidOperation(
                "native relational spill requires an absolute workspace, at least 32 KiB disk quota and a buffer threshold of at least 1 MiB; no fallback execution was attempted".into(),
            ));
        }
        Ok(Self {
            workspace,
            quota_bytes,
            buffer_bytes,
        })
    }

    /// Remove only a verified abandoned relational-order run directory.
    /// Unknown files, symlinks and replaced identities are preserved.
    /// # Errors
    /// Rejects invalid configuration or a directory that cannot prove ownership.
    #[cfg(all(feature = "vortex-local-primitives", feature = "vortex-write", unix))]
    pub fn cleanup_abandoned(&self, directory: &std::path::Path) -> shardloom_core::Result<()> {
        self.cleanup_abandoned_controlled(
            directory,
            &shardloom_exec::compute_pool::CancellationToken::default(),
        )
    }

    /// Remove a verified abandoned run directory under the caller's cancellation owner.
    /// Cancellation is checked before filesystem access, between run deletions and
    /// before marker removal. The marker and remaining runs survive cancellation
    /// so cleanup can be explicitly retried with a fresh token.
    /// # Errors
    /// Rejects invalid configuration, cancellation or unverifiable ownership.
    #[cfg(all(feature = "vortex-local-primitives", feature = "vortex-write", unix))]
    pub fn cleanup_abandoned_controlled(
        &self,
        directory: &std::path::Path,
        cancellation: &shardloom_exec::compute_pool::CancellationToken,
    ) -> shardloom_core::Result<()> {
        crate::local_primitives::native_relational_spill::recover(self, directory, cancellation)
    }
}

/// Actual query-local native relational run spill and verified cleanup evidence.
/// The buffer threshold and shared reservation peak are not process RSS bounds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VortexRelationalSpillReport {
    pub workspace: std::path::PathBuf,
    pub quota_bytes: u64,
    pub buffer_bytes: u64,
    pub peak_disk_bytes: u64,
    pub runs_written: u64,
    pub runs_validated: u64,
    pub merge_passes: u64,
    pub max_open_runs: usize,
    pub run_block_rows: usize,
    pub owned_cleanup_completed: bool,
}

/// A native relational tree. Both inputs of an operator share one session grant.
#[derive(Debug, Clone, PartialEq)]
pub enum VortexRelationalPlan {
    /// An input slot in an inert frontend operator template. The declaration
    /// binder must resolve every slot before native preparation or execution.
    /// It carries neither a source URI nor execution state.
    Input(usize),
    Scan(VortexRelationalScan),
    Join(Box<VortexRelationalJoin>),
    AsOfJoin(Box<VortexRelationalAsOfJoin>),
    Set(Box<VortexRelationalSet>),
    Window(Box<VortexRelationalWindow>),
    GapFill(Box<VortexRelationalGapFill>),
    Subquery(Box<VortexRelationalSubquery>),
    /// Execute the inner tree with each outer row bound as a native singleton.
    /// Grouping, sorting and limits in that tree apply separately to each row.
    CorrelatedSubquery(Box<VortexRelationalSubquery>),
    /// The nearest correlated subquery's current outer row, with its input schema.
    /// This source is invalid outside a `CorrelatedSubquery` inner tree.
    Outer,
    Project(Box<VortexRelationalProject>),
    Filter(Box<VortexRelationalFilter>),
    Sort(Box<VortexRelationalSort>),
    Limit(Box<VortexRelationalLimit>),
    /// A long ordered chain of native row-preserving/selection stages. The
    /// flat representation bounds ownership, binding and execution call depth.
    Pipeline(Box<VortexRelationalPipeline>),
    Aggregate(Box<VortexRelationalAggregate>),
    Unary(Box<VortexRelationalUnary>),
    /// Retain this immutable definition once per action, on first demand.
    /// Cloning the Arc shares the definition and its action-local result.
    /// A different Arc is a different declaration, even for equal input plans.
    Materialized(std::sync::Arc<VortexRelationalPlan>),
    /// Complete an anchor/frontier iteration with native operators and storage.
    /// Repeated references to the same Arc share one result within the action.
    Recursive(std::sync::Arc<VortexRelationalRecursive>),
    /// The immediately preceding frontier of its enclosing recursive definition.
    /// Names are declared for frontend lowering; native binding verifies them.
    RecursiveInput(VortexRelationalRecursiveInput),
    /// A single-use native relation owned by the current schema-binding execution.
    /// Only the native preparation API can create this reference; it is invalid
    /// outside the execution that resolved its schema.
    ExecutionResult(VortexRelationalExecutionRef),
    /// An execution-scoped declaration lowered separately for each outer row.
    /// Valid only as the immediate relation of a correlated subquery.
    DeferredSubquery(VortexRelationalDeferredRef),
}

/// Identity of one lexical recursive input. It never carries data or resources.
#[derive(Debug, Clone, Default)]
pub struct VortexRelationalRecursionId(std::sync::Arc<()>);

impl PartialEq for VortexRelationalRecursionId {
    fn eq(&self, other: &Self) -> bool {
        std::sync::Arc::ptr_eq(&self.0, &other.0)
    }
}

impl Eq for VortexRelationalRecursionId {}

/// A schema-stable recursive relation. The step must reference its own input
/// exactly once. `distinct` applies complete-row UNION identity across all steps.
#[derive(Debug, Clone, PartialEq)]
pub struct VortexRelationalRecursive {
    pub identity: VortexRelationalRecursionId,
    pub anchor: VortexRelationalPlan,
    pub step: VortexRelationalPlan,
    pub distinct: bool,
}

/// A lexical frontier reference, invalid outside its own recursive step.
#[derive(Debug, Clone, PartialEq)]
pub struct VortexRelationalRecursiveInput {
    pub identity: VortexRelationalRecursionId,
    pub columns: Vec<String>,
}

/// Completed action-local retention and recursive work, with no inferred timing.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct VortexRelationalCompositionReport {
    pub materialized_relations: u64,
    pub materialized_reuses: u64,
    pub materialized_rows: u64,
    pub recursive_relations: u64,
    pub recursive_reuses: u64,
    pub recursive_iterations: u64,
    pub recursive_candidate_rows: u64,
    pub recursive_output_rows: u64,
    pub recursive_peak_frontier_rows: u64,
    pub retained_batches: u64,
    pub retained_payload_bytes_copied: u64,
    pub spilled_relations: u64,
}

impl VortexRelationalCompositionReport {
    /// Stable evidence names shared by certificates and public execution reports.
    #[must_use]
    pub const fn counters(&self) -> [(&'static str, u64); 12] {
        [
            (
                "relational_materialized_relations",
                self.materialized_relations,
            ),
            ("relational_materialized_reuses", self.materialized_reuses),
            ("relational_materialized_rows", self.materialized_rows),
            ("relational_recursive_relations", self.recursive_relations),
            ("relational_recursive_reuses", self.recursive_reuses),
            ("relational_recursive_iterations", self.recursive_iterations),
            (
                "relational_recursive_candidate_rows",
                self.recursive_candidate_rows,
            ),
            (
                "relational_recursive_output_rows",
                self.recursive_output_rows,
            ),
            (
                "relational_recursive_peak_frontier_rows",
                self.recursive_peak_frontier_rows,
            ),
            ("relational_retained_batches", self.retained_batches),
            (
                "relational_retained_payload_bytes_copied",
                self.retained_payload_bytes_copied,
            ),
            ("relational_spilled_relations", self.spilled_relations),
        ]
    }
}

/// Opaque, single-use declaration reference for per-parameter schema binding.
#[derive(Debug, Clone)]
pub struct VortexRelationalDeferredRef {
    pub(crate) scope: std::sync::Arc<()>,
    pub(crate) slot: usize,
    #[cfg(all(feature = "vortex-local-primitives", unix))]
    pub(crate) sources: Option<std::sync::Arc<[DatasetUri]>>,
    pub(crate) _source_metadata: Option<std::sync::Arc<shardloom_exec::live_memory::MemoryLease>>,
}

impl PartialEq for VortexRelationalDeferredRef {
    fn eq(&self, other: &Self) -> bool {
        self.slot == other.slot && std::sync::Arc::ptr_eq(&self.scope, &other.scope)
    }
}

/// Opaque ownership reference returned by execution-time schema resolution.
/// Cloning a reference does not clone its result or authorize a second consumer.
#[derive(Debug, Clone)]
pub struct VortexRelationalExecutionRef {
    pub(crate) scope: std::sync::Arc<()>,
    pub(crate) slot: usize,
}

impl PartialEq for VortexRelationalExecutionRef {
    fn eq(&self, other: &Self) -> bool {
        self.slot == other.slot && std::sync::Arc::ptr_eq(&self.scope, &other.scope)
    }
}

/// An existing native unary operation applied at this position in the tree.
/// The request has no source URI: its schema and rows come from `input`.
#[derive(Debug, Clone, PartialEq)]
pub struct VortexRelationalUnary {
    pub input: VortexRelationalPlan,
    pub request: crate::query_primitive::VortexQueryPrimitiveRequest,
}

/// Fill an aligned temporal grid through native ordering and bounded expansion.
/// Bounds are inclusive source-domain ticks; omitted bounds use observed edges.
/// NULL, duplicate or unaligned times fail explicitly within each partition.
#[derive(Debug, Clone, PartialEq)]
pub struct VortexRelationalGapFill {
    pub input: VortexRelationalPlan,
    pub time: ColumnRef,
    pub partition_by: Vec<ColumnRef>,
    pub step: shardloom_core::temporal::TemporalGridStep,
    pub origin: Option<i64>,
    pub start: Option<i64>,
    pub end: Option<i64>,
}

/// Grouped or scalar aggregation consumes native input batches. Computed keys
/// and arguments are explicit Project nodes before this operator; HAVING and
/// result ordering are explicit Filter and Sort nodes after it.
#[derive(Debug, Clone, PartialEq)]
pub struct VortexRelationalAggregate {
    pub input: VortexRelationalPlan,
    pub group_by: Vec<ColumnRef>,
    /// Optional grouping sets over `group_by`. The input is consumed once.
    pub grouping_sets: Option<VortexRelationalGroupingSets>,
    pub measures: Vec<crate::query_primitive::VortexSimpleAggregateMeasure>,
}

/// Normalized grouping sets. Duplicate sets retain duplicate groups; an empty
/// set produces one aggregate result even when its input is empty. Each set
/// names a subset of the aggregate's distinct `group_by` columns.
#[derive(Debug, Clone, PartialEq)]
pub struct VortexRelationalGroupingSets {
    pub sets: Vec<Vec<ColumnRef>>,
    pub indicators: Vec<VortexRelationalGroupingIndicator>,
}

/// Unsigned omission mask: the rightmost argument occupies the low bit.
/// Arguments must be grouping keys and there must be between one and 64.
#[derive(Debug, Clone, PartialEq)]
pub struct VortexRelationalGroupingIndicator {
    pub columns: Vec<ColumnRef>,
    pub alias: String,
}

impl VortexRelationalAggregate {
    /// Declared output names in native order; binding validates uniqueness.
    pub fn output_columns(&self) -> impl Iterator<Item = &str> {
        self.group_by
            .iter()
            .map(ColumnRef::as_str)
            .chain(self.grouping_sets.iter().flat_map(|sets| {
                sets.indicators
                    .iter()
                    .map(|indicator| indicator.alias.as_str())
            }))
            .chain(self.measures.iter().map(|measure| measure.alias.as_str()))
    }
}

/// Named scalar expressions use the shared `ShardLoom` expression IR. Every
/// expression reads the input schema; aliases do not mutate another expression.
#[derive(Debug, Clone, PartialEq)]
pub struct VortexRelationalProject {
    pub input: VortexRelationalPlan,
    pub expressions: Vec<(String, Expression)>,
}

/// SQL filtering retains only true; false and unknown are both excluded.
#[derive(Debug, Clone, PartialEq)]
pub struct VortexRelationalFilter {
    pub input: VortexRelationalPlan,
    pub predicate: Expression,
}

/// Stable lexicographic ordering, with native payload gathered in bounded batches.
#[derive(Debug, Clone, PartialEq)]
pub struct VortexRelationalSort {
    pub input: VortexRelationalPlan,
    pub keys: Vec<VortexRelationalOrderKey>,
}

/// An exact output range. Counts are never substituted for a memory grant.
#[derive(Debug, Clone, PartialEq)]
pub struct VortexRelationalLimit {
    pub input: VortexRelationalPlan,
    pub offset: usize,
    pub count: usize,
}

/// Stages run in source-to-consumer order without changing their expression,
/// error, filtering or range domain. They are not fused or reordered.
#[derive(Debug, Clone, PartialEq)]
pub struct VortexRelationalPipeline {
    pub input: VortexRelationalPlan,
    pub stages: Vec<VortexRelationalStage>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum VortexRelationalStage {
    Project(Vec<(String, Expression)>),
    Filter(Expression),
    Limit {
        offset: usize,
        count: usize,
    },
    /// Accumulate selected global aggregates at this exact stage while passing
    /// the same native rows downstream. Private projections feed only metrics.
    Observe(VortexRelationalObservation),
    /// Explicit trusted row-preserving application transformation. Its position,
    /// callback identity and complete input/output schemas remain observable.
    ApplicationBatch(crate::application_batch::ApplicationBatchFunction),
}

impl VortexRelationalPlan {
    /// Append a trusted application batch transformation at this exact plan
    /// position. The handle is inert until an action reaches a nonempty batch.
    #[must_use]
    pub fn map_batches(self, function: crate::application_batch::ApplicationBatchFunction) -> Self {
        Self::Pipeline(Box::new(VortexRelationalPipeline {
            input: self,
            stages: vec![VortexRelationalStage::ApplicationBatch(function)],
        }))
        .compact_streaming()
    }
}

/// An observation consumes its action's batches; it never owns a second scan.
#[derive(Debug, Clone, PartialEq)]
pub struct VortexRelationalObservation {
    pub name: String,
    pub projection: Vec<(String, Expression)>,
    pub measures: Vec<crate::query_primitive::VortexSimpleAggregateMeasure>,
}

#[path = "relational_plan_pipeline.rs"]
mod pipeline;

/// Projection and safe predicates are lowered into the immutable native scan.
#[derive(Debug, Clone, PartialEq)]
pub struct VortexRelationalScan {
    pub source_uri: DatasetUri,
    pub projection: ProjectionRequest,
    pub predicate: Option<PredicateExpr>,
}

/// Equijoins use SQL null semantics and retain every matching pair.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexRelationalJoinKind {
    Inner,
    Left,
    Right,
    Full,
    LeftSemi,
    LeftAnti,
    Cross,
}

/// Input side of a join projection. Output names are explicit and unique.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexRelationalSide {
    Left,
    Right,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VortexRelationalJoinKey {
    pub left: ColumnRef,
    pub right: ColumnRef,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VortexRelationalJoinColumn {
    pub side: VortexRelationalSide,
    pub column: ColumnRef,
    pub output_column: String,
}

/// Explicit keys and output columns avoid ambiguous same-name source fields.
#[derive(Debug, Clone, PartialEq)]
pub struct VortexRelationalJoin {
    pub left: VortexRelationalPlan,
    pub right: VortexRelationalPlan,
    pub kind: VortexRelationalJoinKind,
    pub keys: Vec<VortexRelationalJoinKey>,
    /// Additional ON predicate. Names use `left.<column>` and `right.<column>`;
    /// it runs before outer null extension. With no keys it is a non-equi join.
    pub condition: Option<Expression>,
    pub columns: Vec<VortexRelationalJoinColumn>,
}

/// Direction of the closest eligible right ordering key in an as-of join.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexRelationalAsOfDirection {
    Backward,
    Forward,
    /// Equal-distance ties prefer backward; exact matches use its last-row rule.
    Nearest,
}

/// Exact as-of selection policy. Tolerance is inclusive and measured in the
/// ordering key's ticks: integer units, date32 days, date64 milliseconds, or
/// the temporal type's declared unit. No implicit timezone/unit conversion occurs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VortexRelationalAsOfOptions {
    pub direction: VortexRelationalAsOfDirection,
    pub tolerance: Option<u64>,
    pub allow_exact_matches: bool,
}

impl Default for VortexRelationalAsOfOptions {
    fn default() -> Self {
        Self {
            direction: VortexRelationalAsOfDirection::Backward,
            tolerance: None,
            allow_exact_matches: true,
        }
    }
}

/// Left-preserving as-of join over unordered inputs. Equality keys partition
/// matching before `on` selects one right row. NULL keys never match. Backward
/// chooses the last source occurrence at a tied key, forward the first; output
/// keeps left input order and unmatched right fields are nullable.
#[derive(Debug, Clone, PartialEq)]
pub struct VortexRelationalAsOfJoin {
    pub left: VortexRelationalPlan,
    pub right: VortexRelationalPlan,
    pub by: Vec<VortexRelationalJoinKey>,
    pub on: VortexRelationalJoinKey,
    pub options: VortexRelationalAsOfOptions,
    pub columns: Vec<VortexRelationalJoinColumn>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexRelationalSetKind {
    UnionAll,
    UnionDistinct,
    Intersect,
    Except,
}

/// Columns align by position, with a lossless common schema bound before work.
#[derive(Debug, Clone, PartialEq)]
pub struct VortexRelationalSet {
    pub left: VortexRelationalPlan,
    pub right: VortexRelationalPlan,
    pub kind: VortexRelationalSetKind,
}

/// Explicit null placement is independent of ascending/descending value order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexRelationalNullOrder {
    First,
    Last,
}

impl VortexRelationalNullOrder {
    /// Compare null placement separately from the direction of nonnull values.
    #[cfg(feature = "vortex-local-primitives")]
    pub(crate) fn compare(
        self,
        descending: bool,
        left_null: bool,
        right_null: bool,
        values: impl FnOnce() -> std::cmp::Ordering,
    ) -> std::cmp::Ordering {
        use std::cmp::Ordering;
        match (left_null, right_null) {
            (true, true) => Ordering::Equal,
            (true, false) => match self {
                Self::First => Ordering::Less,
                Self::Last => Ordering::Greater,
            },
            (false, true) => match self {
                Self::First => Ordering::Greater,
                Self::Last => Ordering::Less,
            },
            (false, false) => {
                let ordering = values();
                if descending {
                    ordering.reverse()
                } else {
                    ordering
                }
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VortexRelationalOrderKey {
    pub column: ColumnRef,
    pub descending: bool,
    /// Omission admits only nonnull values, matching the existing SQL contract.
    pub nulls: Option<VortexRelationalNullOrder>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum VortexRelationalWindowFunction {
    RowNumber,
    Rank,
    DenseRank,
    Lag {
        column: ColumnRef,
        offset: usize,
    },
    Lead {
        column: ColumnRef,
        offset: usize,
    },
    Ntile {
        buckets: usize,
    },
    PercentRank,
    CumeDist,
    /// Fill NULLs within one complete, distinctly ordered temporal partition.
    /// Explicit frames are refused; linear interpolation retains NULL edges.
    Fill {
        column: ColumnRef,
        method: VortexRelationalFillMethod,
    },
    /// Native framed reductions and positional values share partition ordering.
    Framed(VortexRelationalFrameFunction),
}

/// Direction follows ORDER BY; linear interpolation uses elapsed source ticks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexRelationalFillMethod {
    Forward,
    Backward,
    Linear,
}

#[derive(Debug, Clone, PartialEq)]
pub struct VortexRelationalWindowExpression {
    pub output_column: String,
    pub function: VortexRelationalWindowFunction,
    pub partition_by: Vec<ColumnRef>,
    pub order_by: Vec<VortexRelationalOrderKey>,
    /// Omission uses the default SQL frame for framed functions. Ranking and
    /// navigation validate explicit frames but keep their partition semantics.
    pub frame: Option<VortexRelationalWindowFrame>,
}

/// Window columns follow the retained source columns. Delivery keeps input order.
/// Ranking and navigation use complete partitions; framed functions carry their
/// own explicit or default SQL bounds.
#[derive(Debug, Clone, PartialEq)]
pub struct VortexRelationalWindow {
    pub input: VortexRelationalPlan,
    pub columns: Vec<ColumnRef>,
    pub expressions: Vec<VortexRelationalWindowExpression>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexRelationalQuantifier {
    Any,
    All,
}

/// Subquery comparisons produce nullable SQL booleans, without filtering away
/// unknown values prematurely. EXISTS is always nonnullable. Scalar values
/// preserve one bound column's dtype and widen nullability for an empty result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VortexRelationalSubqueryKind {
    In {
        columns: Vec<VortexRelationalJoinKey>,
    },
    Quantified {
        columns: VortexRelationalJoinKey,
        comparison: ComparisonOp,
        quantifier: VortexRelationalQuantifier,
    },
    Exists,
    Scalar,
}

/// Append one subquery result to every input row. Correlation keys use ordinary
/// SQL equality; null correlation keys select an empty inner relation. More
/// complex correlation requires an explicit native parameterized plan.
#[derive(Debug, Clone, PartialEq)]
pub struct VortexRelationalSubquery {
    pub input: VortexRelationalPlan,
    pub relation: VortexRelationalPlan,
    pub kind: VortexRelationalSubqueryKind,
    /// Evaluate only for input rows where this bound Boolean expression is true.
    /// Inactive rows carry an unobserved NULL (false for nonnullable EXISTS).
    pub evaluation_guard: Option<Expression>,
    pub correlation: Vec<VortexRelationalJoinKey>,
    pub output_column: String,
    pub negated: bool,
}
