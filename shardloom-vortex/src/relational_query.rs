//! Typed relational requests shared by native Rust and public front ends.
//! Static schemas bind at preparation; data-dependent schemas bind inside the
//! admitted execution. Requests never store result rows.

use shardloom_core::{ColumnRef, ComparisonOp, DatasetUri, Expression, PredicateExpr};
use shardloom_plan::ProjectionRequest;

#[path = "relational_window_frame.rs"]
mod window_frame;
pub use window_frame::{
    VortexRelationalFrameBound, VortexRelationalFrameExclusion, VortexRelationalFrameFunction,
    VortexRelationalFrameOffset, VortexRelationalFrameUnit, VortexRelationalWindowFrame,
};

/// Explicit permission for query-local relational ordering runs. The buffer
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
        crate::local_primitives::native_relational_spill::recover(self, directory)
    }
}

/// Actual query-local relational ordering spill and verified cleanup evidence.
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
    Scan(VortexRelationalScan),
    Join(Box<VortexRelationalJoin>),
    Set(Box<VortexRelationalSet>),
    Window(Box<VortexRelationalWindow>),
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
    Aggregate(Box<VortexRelationalAggregate>),
    Unary(Box<VortexRelationalUnary>),
    /// A single-use native relation owned by the current schema-binding execution.
    /// Only the native preparation API can create this reference; it is invalid
    /// outside the execution that resolved its schema.
    ExecutionResult(VortexRelationalExecutionRef),
    /// An execution-scoped declaration lowered separately for each outer row.
    /// Valid only as the immediate relation of a correlated subquery.
    DeferredSubquery(VortexRelationalDeferredRef),
}

/// Opaque, single-use declaration reference for per-parameter schema binding.
#[derive(Debug, Clone)]
pub struct VortexRelationalDeferredRef {
    pub(crate) scope: std::sync::Arc<()>,
    pub(crate) slot: usize,
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

/// Grouped or scalar aggregation consumes native input batches. Computed keys
/// and arguments are explicit Project nodes before this operator; HAVING and
/// result ordering are explicit Filter and Sort nodes after it.
#[derive(Debug, Clone, PartialEq)]
pub struct VortexRelationalAggregate {
    pub input: VortexRelationalPlan,
    pub group_by: Vec<ColumnRef>,
    pub measures: Vec<crate::query_primitive::VortexSimpleAggregateMeasure>,
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
    /// Native framed reductions and positional values share partition ordering.
    Framed(VortexRelationalFrameFunction),
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
