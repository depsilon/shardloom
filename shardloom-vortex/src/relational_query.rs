//! Typed relational requests shared by native Rust and public front ends.
//! Names and schemas are bound before execution; no request stores result rows.

use shardloom_core::{ColumnRef, ComparisonOp, DatasetUri, Expression, PredicateExpr};
use shardloom_plan::ProjectionRequest;

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VortexRelationalOrderKey {
    pub column: ColumnRef,
    pub descending: bool,
    /// Omission admits only nonnull values, matching the existing SQL contract.
    pub nulls: Option<VortexRelationalNullOrder>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VortexRelationalWindowFunction {
    RowNumber,
    Rank,
    DenseRank,
    Lag { column: ColumnRef, offset: usize },
    Lead { column: ColumnRef, offset: usize },
    Ntile { buckets: usize },
    PercentRank,
    CumeDist,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VortexRelationalWindowExpression {
    pub output_column: String,
    pub function: VortexRelationalWindowFunction,
    pub partition_by: Vec<ColumnRef>,
    pub order_by: Vec<VortexRelationalOrderKey>,
}

/// Window columns follow the retained source columns. Delivery keeps input order.
/// The admitted functions use complete partitions; no frame clause is implied.
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
/// unknown values prematurely. EXISTS is always nonnullable.
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
}

/// Append one predicate result to every input row. Correlation keys use ordinary
/// SQL equality; null correlation keys select an empty inner relation. More
/// complex correlation requires an explicit native parameterized plan.
#[derive(Debug, Clone, PartialEq)]
pub struct VortexRelationalSubquery {
    pub input: VortexRelationalPlan,
    pub relation: VortexRelationalPlan,
    pub kind: VortexRelationalSubqueryKind,
    pub correlation: Vec<VortexRelationalJoinKey>,
    pub output_column: String,
    pub negated: bool,
}
