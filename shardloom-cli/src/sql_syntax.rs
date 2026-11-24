//! SQL declarations lowered into the shared native execution engine.

use super::*;
use regex::Regex;
use shardloom_core::{
    BinaryOp, ColumnRef, ComparisonOp, ExprId, Expression, ExpressionKind, UnaryOp,
    evaluate_expression, parse_iso_date32, parse_iso_timestamp_micros,
};
use shardloom_vortex::{
    query_primitive::VortexQuantileLevel,
    relational_query::{VortexRelationalFillMethod, VortexRelationalWindowFrame},
};

#[path = "sql_grouping_syntax.rs"]
mod grouping_syntax;
#[path = "sql_window_frames.rs"]
pub(super) mod window_frames;

#[cfg(all(feature = "vortex-local-primitives", unix))]
#[path = "sql_projection_lowering.rs"]
mod projection_lowering;
#[cfg(all(feature = "vortex-local-primitives", unix))]
use projection_lowering::{append_ordered_projection_expression, find_projection_by_alias};

#[path = "sql_memory_inputs.rs"]
mod memory_inputs;
#[path = "sql_relation_sources.rs"]
mod relation_sources;
#[cfg(all(feature = "vortex-local-primitives", unix))]
use relation_sources::ParsedRelationQuery;
#[cfg(all(feature = "vortex-local-primitives", unix))]
pub(crate) use relation_sources::native_unary_request;
use relation_sources::{ParsedRelationLeaf, ParsedRelationSource};

#[cfg(all(feature = "vortex-local-primitives", unix))]
#[path = "sql_native_relational.rs"]
pub(crate) mod native_relational;

#[allow(dead_code)] // Canonical SQL AST entrypoint exposed for phased binder adoption.
pub(crate) fn bind_canonical_plan(
    raw: &str,
) -> Result<shardloom_plan::sql_binder::BoundSqlPlan, ShardLoomError> {
    shardloom_plan::sql_binder::bind_sql(raw)
}

fn find_top_level_numeric_operator(
    raw: &str,
    operators: &[char],
) -> Result<Option<(usize, char)>, ShardLoomError> {
    shardloom_plan::sql_binder::find_top_level_binary_operator(raw, operators)
}
const MAX_IN_LIST_VALUES: usize = 32;
const OUTER_CORRELATION_ALIAS: &str = "outer";
const MAX_DATE_ARITHMETIC_DAYS: i32 = 366_000;
const MAX_TIMESTAMP_ARITHMETIC_SECONDS: i64 = (MAX_DATE_ARITHMETIC_DAYS as i64) * 86_400;

#[derive(Debug, Clone, PartialEq)]
struct ParsedSqlLocalSource {
    distinct_projection: bool,
    replace_or_add_projection: bool,
    projection_order: Vec<ParsedProjectionOutput>,
    projections: Vec<String>,
    literal_projections: Vec<ParsedLiteralProjection>,
    complex_projections: Vec<ParsedComplexProjection>,
    cast_projections: Vec<ParsedCastProjection>,
    null_coalesce_projections: Vec<ParsedNullCoalesceProjection>,
    nullif_projections: Vec<ParsedNullIfProjection>,
    conditional_projections: Vec<ParsedConditionalProjection>,
    predicate_projections: Vec<ParsedPredicateProjection>,
    numeric_arithmetic_projections: Vec<ParsedNumericArithmeticProjection>,
    numeric_abs_projections: Vec<ParsedNumericAbsProjection>,
    numeric_rounding_projections: Vec<ParsedNumericRoundingProjection>,
    generic_expression_projections: Vec<ParsedGenericExpressionProjection>,
    date_arithmetic_projections: Vec<ParsedDateArithmeticProjection>,
    timestamp_arithmetic_projections: Vec<ParsedTimestampArithmeticProjection>,
    string_length_projections: Vec<ParsedStringLengthProjection>,
    string_transform_projections: Vec<ParsedStringTransformProjection>,
    string_function_projections: Vec<ParsedStringFunctionProjection>,
    binary_helper_projections: Vec<ParsedBinaryHelperProjection>,
    binary_byte_length_projections: Vec<ParsedBinaryByteLengthProjection>,
    date_extract_projections: Vec<ParsedDateExtractProjection>,
    timestamp_extract_projections: Vec<ParsedTimestampExtractProjection>,
    window_projections: Vec<ParsedWindowProjection>,
    aggregates: Vec<ParsedAggregate>,
    having_aggregates: Vec<ParsedAggregate>,
    group_by: Vec<String>,
    grouping_sets: Option<grouping_syntax::Grouping>,
    order_by: Option<ParsedOrderBy>,
    source: ParsedRelationSource,
    source_alias: Option<String>,
    join: Option<ParsedJoin>,
    predicate: ParsedPredicate,
    having: ParsedPredicate,
    limit: usize,
    /// Native relation range; decoded-reference parsing leaves this at zero.
    offset: usize,
    /// Parser-only bounds for the decoded reference must not truncate native SQL.
    limit_is_synthetic: bool,
    normalized_statement: String,
}

#[derive(Debug, Clone, PartialEq)]
struct ParsedAggregate {
    function: AggregateFunction,
    argument: ParsedAggregateArgument,
    secondary_argument: Option<ParsedAggregateArgument>,
    filter: Option<ParsedAggregateArgument>,
    quantile_level: Option<VortexQuantileLevel>,
    decimal_output: Option<shardloom_core::decimal_output::DecimalOutput>,
    order_by: Option<ParsedOrderBy>,
    alias: Option<String>,
    distinct: bool,
    separator: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
enum ParsedAggregateArgument {
    All,
    Column(String),
    Computed {
        raw: String,
        expression: Box<scalar_expression::ParsedScalarExpression>,
        source_columns: Vec<String>,
    },
}

#[derive(Debug, Clone, PartialEq)]
struct ParsedLiteralProjection {
    alias: String,
    value: ScalarValue,
}

#[derive(Debug, Clone, PartialEq)]
struct ParsedComplexProjection {
    alias: String,
    kind: ParsedComplexProjectionKind,
}

#[derive(Debug, Clone, PartialEq)]
enum ParsedComplexProjectionKind {
    ArrayLiteral(Vec<ScalarValue>),
    StructColumns(Vec<String>),
}

#[derive(Debug, Clone, PartialEq)]
struct ParsedCastProjection {
    alias: String,
    column: String,
    expression: Expression,
    source_columns: Vec<String>,
    target_dtype: LogicalDType,
    mode: CastMode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CastMode {
    Strict,
    Try,
}

impl CastMode {
    const fn function_label(self) -> &'static str {
        match self {
            Self::Strict => "CAST",
            Self::Try => "TRY_CAST",
        }
    }

    fn build_expression(
        self,
        id: ExprId,
        expr: Expression,
        target_dtype: LogicalDType,
    ) -> Expression {
        match self {
            Self::Strict => Expression::cast(id, expr, target_dtype),
            Self::Try => Expression::try_cast(id, expr, target_dtype),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
struct ParsedNullCoalesceProjection {
    alias: String,
    column: String,
    source_cast_dtype: Option<LogicalDType>,
    fallback: ScalarValue,
}

#[derive(Debug, Clone, PartialEq)]
struct ParsedNullIfProjection {
    alias: String,
    column: String,
    source_cast_dtype: Option<LogicalDType>,
    sentinel: ScalarValue,
}

#[derive(Debug, Clone, PartialEq)]
struct ParsedConditionalProjection {
    alias: String,
    predicate: ParsedPredicate,
    then_branch: ParsedConditionalBranch,
    else_branch: ParsedConditionalBranch,
    then_dtype: Option<LogicalDType>,
    else_dtype: Option<LogicalDType>,
}

#[derive(Debug, Clone, PartialEq)]
enum ParsedConditionalBranch {
    Literal(ScalarValue),
    Column(String),
}

impl ParsedConditionalBranch {
    fn literal_dtype(&self) -> Option<LogicalDType> {
        match self {
            Self::Literal(value) => Some(value.dtype()),
            Self::Column(_) => None,
        }
    }

    #[cfg(all(feature = "vortex-local-primitives", unix))]
    fn to_expression(&self, expr_id: ExprId) -> Result<Expression, ShardLoomError> {
        match self {
            Self::Literal(value) => Ok(Expression::literal(expr_id, value.clone())),
            Self::Column(column) => {
                Ok(Expression::column(expr_id, ColumnRef::new(column.clone())?))
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
struct ParsedPredicateProjection {
    alias: String,
    predicate: ParsedPredicate,
}

#[derive(Debug, Clone, PartialEq)]
struct ParsedNumericArithmeticProjection {
    alias: String,
    column: String,
    op: NumericArithmeticOp,
    rhs: ScalarValue,
}

#[derive(Debug, Clone, PartialEq)]
struct ParsedNumericAbsProjection {
    alias: String,
    column: String,
}

#[derive(Debug, Clone, PartialEq)]
struct ParsedNumericRoundingProjection {
    alias: String,
    column: String,
    op: NumericRoundingOp,
}

#[derive(Debug, Clone, PartialEq)]
struct ParsedGenericExpressionProjection {
    alias: String,
    expression: scalar_expression::ParsedScalarExpression,
    source_columns: Vec<String>,
    operator_families: Vec<String>,
    binary_operator_count: usize,
}

#[derive(Debug, Clone, PartialEq)]
struct ParsedDateArithmeticProjection {
    alias: String,
    column: String,
    op: DateArithmeticOp,
    day_count: i32,
}

#[derive(Debug, Clone, PartialEq)]
struct ParsedTimestampArithmeticProjection {
    alias: String,
    column: String,
    op: TimestampArithmeticOp,
    second_count: i64,
}

#[derive(Debug, Clone, PartialEq)]
struct ParsedStringLengthProjection {
    alias: String,
    expression: Expression,
    source_columns: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
struct ParsedStringTransformProjection {
    alias: String,
    expression: Expression,
    op: StringTransformOp,
    source_columns: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
struct ParsedStringFunctionProjection {
    alias: String,
    expression: Expression,
    op: StringFunctionOp,
    source_columns: Vec<String>,
    literal_count: usize,
}

#[derive(Debug, Clone, PartialEq)]
struct ParsedBinaryHelperProjection {
    alias: String,
    expression: Expression,
    source_columns: Vec<String>,
    op: BinaryHelperOp,
}

#[derive(Debug, Clone, PartialEq)]
struct ParsedBinaryHelperPredicate {
    expression: Expression,
    source_columns: Vec<String>,
    op: BinaryHelperOp,
    comparison: ComparisonOp,
    value: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq)]
struct ParsedBinaryByteLengthProjection {
    alias: String,
    expression: Expression,
    source_columns: Vec<String>,
    argument_family: BinaryByteLengthArgumentFamily,
}

#[derive(Debug, Clone, PartialEq)]
struct ParsedBinaryByteLengthPredicate {
    expression: Expression,
    source_columns: Vec<String>,
    argument_family: BinaryByteLengthArgumentFamily,
    comparison: ComparisonOp,
    value: ScalarValue,
}

#[derive(Debug, Clone, PartialEq)]
struct ParsedStringFunctionCall {
    expression: Expression,
    op: StringFunctionOp,
    source_columns: Vec<String>,
    literal_count: usize,
}

#[derive(Debug, Clone, PartialEq)]
struct ParsedDateExtractProjection {
    alias: String,
    column: String,
    op: DateExtractOp,
}

#[derive(Debug, Clone, PartialEq)]
struct ParsedTimestampExtractProjection {
    alias: String,
    column: String,
    op: TimestampExtractOp,
}

#[derive(Debug, Clone, PartialEq)]
struct ParsedWindowProjection {
    alias: String,
    function: WindowFunction,
    partition_by: Vec<String>,
    order_by: ParsedOrderBy,
    frame: Option<VortexRelationalWindowFrame>,
}

#[derive(Debug, Clone, PartialEq)]
enum WindowFunction {
    RowNumber,
    Rank,
    DenseRank,
    Lag {
        column: String,
        offset: usize,
    },
    Lead {
        column: String,
        offset: usize,
    },
    Ntile {
        bucket_count: usize,
    },
    PercentRank,
    CumeDist,
    Aggregate(ParsedAggregate),
    FirstValue(scalar_expression::ParsedScalarExpression),
    LastValue(scalar_expression::ParsedScalarExpression),
    NthValue {
        expression: scalar_expression::ParsedScalarExpression,
        index: u64,
    },
    Fill {
        expression: scalar_expression::ParsedScalarExpression,
        method: VortexRelationalFillMethod,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ParsedOrderBy {
    keys: Vec<ParsedOrderKey>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ParsedOrderKey {
    column: String,
    direction: SortDirection,
    null_ordering: Option<SortNullOrdering>,
}

#[derive(Debug, Clone, PartialEq)]
struct ParsedJoin {
    join_type: ParsedJoinType,
    right_source: ParsedRelationSource,
    right_alias: String,
    key_pairs: Vec<ParsedJoinKeyPair>,
    on_predicate: Option<ParsedPredicate>,
    on_predicate_family: ParsedJoinOnPredicateFamily,
    asof: Option<ParsedAsOfJoin>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ParsedAsOfJoin {
    on: ParsedJoinKeyPair,
    options: shardloom_vortex::relational_query::VortexRelationalAsOfOptions,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ParsedJoinType {
    InnerEqui,
    LeftOuterEqui,
    RightOuterEqui,
    FullOuterEqui,
    LeftSemiEqui,
    LeftAntiEqui,
    Cross,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ParsedJoinOnPredicateFamily {
    NotApplicable,
    EquiKeys,
    ColumnCompare,
    GenericExpression,
    Logical,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ParsedJoinKeyPair {
    left: QualifiedColumn,
    right: QualifiedColumn,
}

#[derive(Debug, Clone, PartialEq)]
struct ParsedJoinOn {
    key_pairs: Vec<ParsedJoinKeyPair>,
    predicate: Option<ParsedPredicate>,
    predicate_family: ParsedJoinOnPredicateFamily,
}

#[derive(Debug, Clone, PartialEq)]
struct ParsedSourceClause {
    source: ParsedRelationSource,
    source_alias: Option<String>,
    join: Option<ParsedJoin>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct QualifiedColumn {
    alias: String,
    column: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AggregateFunction {
    Count,
    Sum,
    Avg,
    Min,
    Max,
    VarPop,
    VarSamp,
    StddevPop,
    StddevSamp,
    CovarPop,
    CovarSamp,
    Corr,
    Median,
    QuantileCont,
    QuantileDisc,
    First,
    Last,
    ArgMin,
    ArgMax,
    List,
    StringAgg,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SortDirection {
    Asc,
    Desc,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SortNullOrdering {
    First,
    Last,
}

#[derive(Debug, Clone, PartialEq)]
struct ParsedProjectionList {
    replace_or_add: bool,
    projection_order: Vec<ParsedProjectionOutput>,
    projections: Vec<String>,
    literal_projections: Vec<ParsedLiteralProjection>,
    complex_projections: Vec<ParsedComplexProjection>,
    cast_projections: Vec<ParsedCastProjection>,
    null_coalesce_projections: Vec<ParsedNullCoalesceProjection>,
    nullif_projections: Vec<ParsedNullIfProjection>,
    conditional_projections: Vec<ParsedConditionalProjection>,
    predicate_projections: Vec<ParsedPredicateProjection>,
    numeric_arithmetic_projections: Vec<ParsedNumericArithmeticProjection>,
    numeric_abs_projections: Vec<ParsedNumericAbsProjection>,
    numeric_rounding_projections: Vec<ParsedNumericRoundingProjection>,
    generic_expression_projections: Vec<ParsedGenericExpressionProjection>,
    date_arithmetic_projections: Vec<ParsedDateArithmeticProjection>,
    timestamp_arithmetic_projections: Vec<ParsedTimestampArithmeticProjection>,
    string_length_projections: Vec<ParsedStringLengthProjection>,
    string_transform_projections: Vec<ParsedStringTransformProjection>,
    string_function_projections: Vec<ParsedStringFunctionProjection>,
    binary_helper_projections: Vec<ParsedBinaryHelperProjection>,
    binary_byte_length_projections: Vec<ParsedBinaryByteLengthProjection>,
    date_extract_projections: Vec<ParsedDateExtractProjection>,
    timestamp_extract_projections: Vec<ParsedTimestampExtractProjection>,
    window_projections: Vec<ParsedWindowProjection>,
    aggregates: Vec<ParsedAggregate>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ParsedProjectionOutput {
    Raw(String),
    Aggregate(String),
    Literal(String),
    Complex(String),
    Cast(String),
    NullCoalesce(String),
    NullIf(String),
    Conditional(String),
    Predicate(String),
    NumericArithmetic(String),
    NumericAbs(String),
    NumericRounding(String),
    GenericExpression(String),
    DateArithmetic(String),
    TimestampArithmetic(String),
    StringLength(String),
    StringTransform(String),
    StringFunction(String),
    BinaryHelper(String),
    BinaryByteLength(String),
    DateExtract(String),
    TimestampExtract(String),
    Window(String),
}

impl ParsedProjectionOutput {
    fn computed_alias(&self) -> Option<&str> {
        match self {
            Self::Raw(_) => None,
            Self::Aggregate(alias)
            | Self::Literal(alias)
            | Self::Complex(alias)
            | Self::Cast(alias)
            | Self::NullCoalesce(alias)
            | Self::NullIf(alias)
            | Self::Conditional(alias)
            | Self::Predicate(alias)
            | Self::NumericArithmetic(alias)
            | Self::NumericAbs(alias)
            | Self::NumericRounding(alias)
            | Self::GenericExpression(alias)
            | Self::DateArithmetic(alias)
            | Self::TimestampArithmetic(alias)
            | Self::StringLength(alias)
            | Self::StringTransform(alias)
            | Self::StringFunction(alias)
            | Self::BinaryHelper(alias)
            | Self::BinaryByteLength(alias)
            | Self::DateExtract(alias)
            | Self::TimestampExtract(alias)
            | Self::Window(alias) => Some(alias),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
struct ParsedInSubquery {
    source_column: String,
    source: ParsedRelationSource,
    source_qualifier: Option<String>,
    predicate: Box<ParsedPredicate>,
    order_by: Option<ParsedOrderBy>,
    limit: Option<usize>,
    projected_plan: Option<Box<ParsedSqlLocalSource>>,
    source_format: Option<LocalSourceFormat>,
    source_digest: Option<String>,
    input_row_count: usize,
    filtered_row_count: usize,
    values: Vec<ScalarValue>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ParsedLocalSubquerySourceRef {
    leaf: ParsedRelationLeaf,
    qualifier: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ParsedQuantifiedSubqueryQuantifier {
    Any,
    All,
}

impl ParsedQuantifiedSubqueryQuantifier {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Any => "any",
            Self::All => "all",
        }
    }

    const fn binary_op(self) -> BinaryOp {
        match self {
            Self::Any => BinaryOp::Or,
            Self::All => BinaryOp::And,
        }
    }

    const fn empty_result(self) -> bool {
        match self {
            Self::Any => false,
            Self::All => true,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
struct ParsedRowValueInSubquery {
    source_columns: Vec<String>,
    source: ParsedRelationSource,
    source_qualifier: Option<String>,
    predicate: Box<ParsedPredicate>,
    order_by: Option<ParsedOrderBy>,
    limit: Option<usize>,
    projected_plan: Option<Box<ParsedSqlLocalSource>>,
    source_format: Option<LocalSourceFormat>,
    source_digest: Option<String>,
    input_row_count: usize,
    filtered_row_count: usize,
    tuples: Vec<Vec<ScalarValue>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ParsedExistsSubqueryProjectionKind {
    Wildcard,
    Literal,
    ColumnList,
}

#[derive(Debug, Clone, PartialEq)]
struct ParsedExistsSubquery {
    projection_kind: ParsedExistsSubqueryProjectionKind,
    selected_columns: Vec<String>,
    source: ParsedRelationSource,
    source_qualifier: Option<String>,
    predicate: Box<ParsedPredicate>,
    order_by: Option<ParsedOrderBy>,
    limit: Option<usize>,
    projected_plan: Option<Box<ParsedSqlLocalSource>>,
    source_format: Option<LocalSourceFormat>,
    source_digest: Option<String>,
    input_row_count: usize,
    filtered_row_count: usize,
    bounded_row_count: usize,
    exists: bool,
}

impl ParsedInSubquery {
    fn uses_outer_correlation(&self) -> bool {
        self.predicate.uses_outer_correlation()
            || self
                .projected_plan
                .as_deref()
                .is_some_and(ParsedSqlLocalSource::uses_outer_correlation)
    }
}

impl ParsedRowValueInSubquery {
    fn uses_outer_correlation(&self) -> bool {
        self.predicate.uses_outer_correlation()
            || self
                .projected_plan
                .as_deref()
                .is_some_and(ParsedSqlLocalSource::uses_outer_correlation)
    }
}

impl ParsedExistsSubquery {
    fn uses_outer_correlation(&self) -> bool {
        self.predicate.uses_outer_correlation()
            || self
                .projected_plan
                .as_deref()
                .is_some_and(ParsedSqlLocalSource::uses_outer_correlation)
    }
}

#[derive(Debug, Clone, PartialEq)]
enum ParsedPredicate {
    All,
    Compare {
        column: String,
        op: ComparisonOp,
        value: ScalarValue,
    },
    ColumnCompare {
        left_column: String,
        op: ComparisonOp,
        right_column: String,
    },
    CastCompare {
        column: String,
        expression: Box<Expression>,
        source_columns: Vec<String>,
        target_dtype: LogicalDType,
        mode: CastMode,
        op: ComparisonOp,
        value: ScalarValue,
    },
    NumericArithmeticCompare {
        column: String,
        op: NumericArithmeticOp,
        rhs: ScalarValue,
        comparison: ComparisonOp,
        value: ScalarValue,
    },
    NumericAbsCompare {
        column: String,
        comparison: ComparisonOp,
        value: ScalarValue,
    },
    NumericRoundingCompare {
        column: String,
        op: NumericRoundingOp,
        comparison: ComparisonOp,
        value: ScalarValue,
    },
    GenericExpressionCompare {
        left: Box<scalar_expression::ParsedScalarExpression>,
        comparison: ComparisonOp,
        right: Box<scalar_expression::ParsedScalarExpression>,
        source_columns: Vec<String>,
        operator_families: Vec<String>,
        binary_operator_count: usize,
    },
    DateArithmeticCompare {
        column: String,
        op: DateArithmeticOp,
        day_count: i32,
        comparison: ComparisonOp,
        value: ScalarValue,
    },
    TimestampArithmeticCompare {
        column: String,
        op: TimestampArithmeticOp,
        second_count: i64,
        comparison: ComparisonOp,
        value: ScalarValue,
    },
    DateExtractCompare {
        column: String,
        op: DateExtractOp,
        comparison: ComparisonOp,
        value: ScalarValue,
    },
    StringLengthCompare {
        expression: Box<Expression>,
        comparison: ComparisonOp,
        value: ScalarValue,
        source_columns: Vec<String>,
    },
    TimestampExtractCompare {
        column: String,
        op: TimestampExtractOp,
        comparison: ComparisonOp,
        value: ScalarValue,
    },
    BooleanPredicate {
        column: String,
        expected: bool,
        null_is_false: bool,
        negated: bool,
    },
    IsNull {
        column: String,
    },
    IsNotNull {
        column: String,
    },
    InList {
        column: String,
        values: Vec<ScalarValue>,
    },
    RowValueInList {
        columns: Vec<String>,
        tuples: Vec<Vec<ScalarValue>>,
    },
    RowValueInSubquery {
        columns: Vec<String>,
        subquery: Box<ParsedRowValueInSubquery>,
    },
    InSubquery {
        column: String,
        subquery: Box<ParsedInSubquery>,
    },
    QuantifiedSubquery {
        column: String,
        comparison: ComparisonOp,
        quantifier: ParsedQuantifiedSubqueryQuantifier,
        subquery: Box<ParsedInSubquery>,
    },
    ExistsSubquery {
        subquery: Box<ParsedExistsSubquery>,
    },
    StringMatch {
        column: String,
        op: StringPredicateOp,
        value: String,
        like_escape: Option<char>,
    },
    StringTransformCompare {
        expression: Box<Expression>,
        op: StringTransformOp,
        comparison: ComparisonOp,
        value: ScalarValue,
        source_columns: Vec<String>,
    },
    StringFunctionCompare {
        expression: Box<Expression>,
        op: StringFunctionOp,
        comparison: ComparisonOp,
        value: ScalarValue,
        source_columns: Vec<String>,
        literal_count: usize,
    },
    BinaryHelperCompare(ParsedBinaryHelperPredicate),
    BinaryByteLengthCompare(ParsedBinaryByteLengthPredicate),
    Logical {
        op: LogicalPredicateOp,
        left: Box<ParsedPredicate>,
        right: Box<ParsedPredicate>,
    },
    Not {
        inner: Box<ParsedPredicate>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LogicalPredicateOp {
    And,
    Or,
}

impl LogicalPredicateOp {
    const fn as_str(self) -> &'static str {
        match self {
            Self::And => "and",
            Self::Or => "or",
        }
    }

    const fn binary_op(self) -> BinaryOp {
        match self {
            Self::And => BinaryOp::And,
            Self::Or => BinaryOp::Or,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StringPredicateOp {
    StartsWith,
    Contains,
    EndsWith,
    LikePattern,
    RegexMatch,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StringTransformOp {
    Lower,
    Upper,
    Trim,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StringFunctionOp {
    Concat,
    Substr,
    Left,
    Right,
    Replace,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BinaryHelperOp {
    Unhex,
    FromBase64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BinaryByteLengthArgumentFamily {
    Helper(BinaryHelperOp),
    Cast(CastMode),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NumericArithmeticOp {
    Add,
    Subtract,
    Multiply,
    Divide,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NumericRoundingOp {
    Floor,
    Ceil,
    Round,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DateArithmeticOp {
    AddDays,
    SubDays,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TimestampArithmeticOp {
    AddSeconds,
    SubSeconds,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SqlIntervalUnit {
    Day,
    Hour,
    Minute,
    Second,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DateExtractOp {
    Year,
    Month,
    Day,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TimestampExtractOp {
    Year,
    Month,
    Day,
    Hour,
    Minute,
    Second,
}

impl DateArithmeticOp {
    const fn function_name(self) -> &'static str {
        match self {
            Self::AddDays => "date_add_days",
            Self::SubDays => "date_sub_days",
        }
    }
}

impl TimestampArithmeticOp {
    const fn function_name(self) -> &'static str {
        match self {
            Self::AddSeconds => "timestamp_add_seconds",
            Self::SubSeconds => "timestamp_sub_seconds",
        }
    }
}

impl SqlIntervalUnit {
    fn parse(raw: &str) -> Result<Self, ShardLoomError> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "day" | "days" => Ok(Self::Day),
            "hour" | "hours" => Ok(Self::Hour),
            "minute" | "minutes" => Ok(Self::Minute),
            "second" | "seconds" => Ok(Self::Second),
            _ => Err(unsupported_sql_error(
                "ANSI INTERVAL literals in scoped temporal arithmetic admit DAY, HOUR, MINUTE, or SECOND units only",
            )),
        }
    }

    const fn seconds_multiplier(self) -> i64 {
        match self {
            Self::Day => 86_400,
            Self::Hour => 3_600,
            Self::Minute => 60,
            Self::Second => 1,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SqlIntervalLiteral {
    value: i64,
    unit: SqlIntervalUnit,
}

impl DateExtractOp {
    const fn function_name(self) -> &'static str {
        match self {
            Self::Year => "date_year",
            Self::Month => "date_month",
            Self::Day => "date_day",
        }
    }
}

impl TimestampExtractOp {
    const fn function_name(self) -> &'static str {
        match self {
            Self::Year => "timestamp_year",
            Self::Month => "timestamp_month",
            Self::Day => "timestamp_day",
            Self::Hour => "timestamp_hour",
            Self::Minute => "timestamp_minute",
            Self::Second => "timestamp_second",
        }
    }
}

impl StringPredicateOp {
    const fn function_name(self) -> &'static str {
        match self {
            Self::StartsWith => "utf8_starts_with",
            Self::Contains => "utf8_contains",
            Self::EndsWith => "utf8_ends_with",
            Self::LikePattern | Self::RegexMatch => "utf8_regex_match",
        }
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::StartsWith => "starts_with",
            Self::Contains => "contains",
            Self::EndsWith => "ends_with",
            Self::LikePattern => "like_pattern",
            Self::RegexMatch => "regex_match",
        }
    }
}

impl StringTransformOp {
    const fn function_name(self) -> &'static str {
        match self {
            Self::Lower => "utf8_lower",
            Self::Upper => "utf8_upper",
            Self::Trim => "utf8_trim",
        }
    }
}

impl StringFunctionOp {
    const fn function_name(self) -> &'static str {
        match self {
            Self::Concat => "concat",
            Self::Substr => "substr",
            Self::Left => "left",
            Self::Right => "right",
            Self::Replace => "replace",
        }
    }
}

impl BinaryHelperOp {
    const fn function_name(self) -> &'static str {
        match self {
            Self::Unhex => "unhex",
            Self::FromBase64 => "from_base64",
        }
    }
}

impl NumericArithmeticOp {
    const fn binary_op(self) -> BinaryOp {
        match self {
            Self::Add => BinaryOp::Add,
            Self::Subtract => BinaryOp::Subtract,
            Self::Multiply => BinaryOp::Multiply,
            Self::Divide => BinaryOp::Divide,
        }
    }
}

impl NumericRoundingOp {
    const fn function_name(self) -> &'static str {
        match self {
            Self::Floor => "floor",
            Self::Ceil => "ceil",
            Self::Round => "round",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SqlLocalSourceClauseIndexes {
    from: usize,
    limit: usize,
    filter: Option<usize>,
    group_by: Option<usize>,
    having: Option<usize>,
    order_by: Option<usize>,
}

struct ParsedSqlLocalSourceParts {
    statement: String,
    projection_list: ParsedProjectionList,
    distinct_projection: bool,
    having_aggregates: Vec<ParsedAggregate>,
    group_by: Vec<String>,
    grouping_sets: Option<grouping_syntax::Grouping>,
    order_by: Option<ParsedOrderBy>,
    source_clause: ParsedSourceClause,
    predicate: ParsedPredicate,
    having: ParsedPredicate,
    limit: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SqlUnionMode {
    Distinct,
    All,
    IntersectDistinct,
    ExceptDistinct,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SqlUnionOperator {
    index: usize,
    len: usize,
    mode: SqlUnionMode,
}

#[derive(Debug, Clone, PartialEq)]
struct ParsedSqlLocalSourceUnion {
    normalized_statement: String,
    mode: SqlUnionMode,
    branch_statements: Vec<String>,
    branches: Vec<ParsedSqlLocalSource>,
    order_by: Option<ParsedOrderBy>,
    limit: usize,
    offset: usize,
}

#[cfg(all(test, feature = "vortex-write", feature = "universal-format-io"))]
fn run_vortex_prepare(request: VortexIngestRequest) -> Result<VortexIngestOutcome, ShardLoomError> {
    run_vortex_prepare_with_schema(request, &[])
}

fn is_outer_correlation_ref(column: &str) -> bool {
    outer_correlation_ref_column(column).is_some()
}

fn outer_correlation_ref_column(column: &str) -> Option<String> {
    let qualified = parse_qualified_column_ref(column).ok()?;
    (qualified.alias == OUTER_CORRELATION_ALIAS).then_some(qualified.column)
}

fn bytes_to_hex(value: &[u8]) -> String {
    let mut out = String::with_capacity(value.len().saturating_mul(2));
    for byte in value {
        let _ = write!(out, "{byte:02x}");
    }
    out
}

impl ParsedSqlLocalSource {
    fn is_aggregate(&self) -> bool {
        !self.aggregates.is_empty()
            || !self.having_aggregates.is_empty()
            || self.grouping_sets.is_some()
    }

    fn is_grouped_aggregate(&self) -> bool {
        (!self.group_by.is_empty() || self.grouping_sets.is_some()) && self.is_aggregate()
    }

    fn uses_outer_correlation(&self) -> bool {
        self.any_predicate_surface(ParsedPredicate::uses_outer_correlation)
    }

    fn predicate_surfaces(&self) -> Vec<&ParsedPredicate> {
        let mut predicates = Vec::with_capacity(
            2 + self.conditional_projections.len() + self.predicate_projections.len(),
        );
        predicates.push(&self.predicate);
        predicates.push(&self.having);
        predicates.extend(
            self.conditional_projections
                .iter()
                .map(|projection| &projection.predicate),
        );
        predicates.extend(
            self.predicate_projections
                .iter()
                .map(|projection| &projection.predicate),
        );
        predicates
    }

    fn any_predicate_surface<F>(&self, test: F) -> bool
    where
        F: FnMut(&ParsedPredicate) -> bool,
    {
        self.predicate_surfaces().into_iter().any(test)
    }

    fn has_complex_projection(&self) -> bool {
        !self.complex_projections.is_empty()
    }
}

impl ParsedAggregate {
    /// Aliases do not change a reduction's value within the same query scope.
    /// Compare parsed computed arguments rather than their SQL whitespace.
    fn same_value(&self, other: &Self) -> bool {
        self.function == other.function
            && self.distinct == other.distinct
            && self.quantile_level == other.quantile_level
            && self.decimal_output == other.decimal_output
            && self.order_by == other.order_by
            && self.separator == other.separator
            && match (&self.filter, &other.filter) {
                (Some(left), Some(right)) => Self::same_argument(left, right),
                (None, None) => true,
                _ => false,
            }
            && self.secondary_argument.is_some() == other.secondary_argument.is_some()
            && self
                .arguments()
                .zip(other.arguments())
                .all(|(left, right)| Self::same_argument(left, right))
    }

    fn same_argument(left: &ParsedAggregateArgument, right: &ParsedAggregateArgument) -> bool {
        match (left, right) {
            (
                ParsedAggregateArgument::Computed {
                    expression: left, ..
                },
                ParsedAggregateArgument::Computed {
                    expression: right, ..
                },
            ) => left == right,
            (left, right) => left == right,
        }
    }

    fn arguments(&self) -> impl Iterator<Item = &ParsedAggregateArgument> {
        std::iter::once(&self.argument).chain(self.secondary_argument.iter())
    }

    fn arguments_mut(&mut self) -> impl Iterator<Item = &mut ParsedAggregateArgument> {
        std::iter::once(&mut self.argument).chain(self.secondary_argument.iter_mut())
    }

    #[cfg(all(feature = "vortex-local-primitives", unix))]
    fn input_arguments(&self) -> impl Iterator<Item = &ParsedAggregateArgument> {
        self.arguments().chain(self.filter.iter())
    }

    fn columns(&self) -> impl Iterator<Item = &str> {
        self.column()
            .into_iter()
            .chain(
                self.secondary_argument
                    .iter()
                    .filter_map(|argument| match argument {
                        ParsedAggregateArgument::Column(column) => Some(column.as_str()),
                        _ => None,
                    }),
            )
            .chain(
                self.filter
                    .iter()
                    .flat_map(|filter| match filter {
                        ParsedAggregateArgument::Column(column) => {
                            std::slice::from_ref(column).iter()
                        }
                        ParsedAggregateArgument::Computed { source_columns, .. } => {
                            source_columns.iter()
                        }
                        ParsedAggregateArgument::All => [].iter(),
                    })
                    .map(String::as_str),
            )
            .chain(
                self.order_by
                    .iter()
                    .flat_map(|order| order.keys.iter().map(|key| key.column.as_str())),
            )
    }

    fn column(&self) -> Option<&str> {
        match &self.argument {
            ParsedAggregateArgument::Column(column) => Some(column),
            _ => None,
        }
    }

    fn output_name(&self) -> String {
        if let Some(alias) = self.alias.as_ref() {
            return alias.clone();
        }
        if self.secondary_argument.is_some()
            || self.quantile_level.is_some()
            || self.decimal_output.is_some()
            || self.order_by.is_some()
            || self.separator.is_some()
            || self.filter.is_some()
        {
            use sha2::{Digest as _, Sha256};
            let mut hash = Sha256::new();
            if let Some(filter) = &self.filter {
                hash.update(b"aggregate_filter");
                hash.update([u8::from(self.distinct)]);
                let raw = match filter {
                    ParsedAggregateArgument::All => "*",
                    ParsedAggregateArgument::Column(name) => name,
                    ParsedAggregateArgument::Computed { raw, .. } => raw,
                };
                hash.update(
                    u64::try_from(raw.len())
                        .expect("SQL filter fits memory")
                        .to_le_bytes(),
                );
                hash.update(raw.as_bytes());
            }
            for argument in self.arguments() {
                let raw = match argument {
                    ParsedAggregateArgument::All => "*",
                    ParsedAggregateArgument::Column(name) => name,
                    ParsedAggregateArgument::Computed { raw, .. } => raw,
                };
                hash.update(
                    u64::try_from(raw.len())
                        .expect("SQL names fit addressable memory")
                        .to_le_bytes(),
                );
                hash.update(raw.as_bytes());
            }
            hash.update([u8::from(self.separator.is_some())]);
            if let Some(separator) = &self.separator {
                hash.update(
                    u64::try_from(separator.len())
                        .expect("separator fits memory")
                        .to_le_bytes(),
                );
                hash.update(separator.as_bytes());
            }
            if let Some(level) = self.quantile_level {
                hash.update(b"quantile_level");
                hash.update(level.value().to_bits().to_le_bytes());
            }
            if let Some(output) = self.decimal_output {
                hash.update(b"decimal_output");
                hash.update([output.precision(), output.scale(), u8::from(self.distinct)]);
                hash.update(output.rounding().as_str().as_bytes());
            }
            if let Some(order) = &self.order_by {
                hash.update(b"aggregate_order");
                for key in &order.keys {
                    hash.update(
                        u64::try_from(key.column.len())
                            .expect("SQL names fit addressable memory")
                            .to_le_bytes(),
                    );
                    hash.update(key.column.as_bytes());
                    hash.update([
                        u8::from(key.direction == SortDirection::Desc),
                        match key.null_ordering {
                            None => 0,
                            Some(SortNullOrdering::First) => 1,
                            Some(SortNullOrdering::Last) => 2,
                        },
                    ]);
                }
            }
            return format!(
                "{}_expr_{}",
                self.function.as_str(),
                bytes_to_hex(&hash.finalize())
            );
        }
        if let ParsedAggregateArgument::Computed { raw, .. } = &self.argument {
            use sha2::{Digest as _, Sha256};
            // Bound the schema name independently of expression length. Existing
            // output-name validation rejects duplicate or colliding names.
            return format!(
                "{}{}_expr_{}",
                self.function.as_str(),
                if self.distinct { "_distinct" } else { "" },
                bytes_to_hex(&Sha256::digest(raw.as_bytes()))
            );
        }
        match (self.function, self.column(), self.distinct) {
            (AggregateFunction::Count, None, _) => "count_all".to_string(),
            (function, Some(column), true) => {
                format!("{}_distinct_{}", function.as_str(), column)
            }
            (function, Some(column), false) => format!("{}_{}", function.as_str(), column),
            (function, None, _) => function.as_str().to_string(),
        }
    }
}

impl AggregateFunction {
    fn native_name(self, distinct: bool) -> Result<&'static str, ShardLoomError> {
        if !distinct {
            return Ok(self.as_str());
        }
        match self {
            Self::Count => Ok("count_distinct"),
            Self::Sum => Ok("sum_distinct"),
            Self::Avg => Ok("avg_distinct"),
            _ => Err(unsupported_sql_error(
                "native DISTINCT aggregates admit COUNT, SUM and AVG",
            )),
        }
    }

    fn parse(value: &str) -> Option<Self> {
        Some(match value.trim().to_ascii_lowercase().as_str() {
            "count" => Self::Count,
            "sum" => Self::Sum,
            "avg" | "mean" => Self::Avg,
            "min" => Self::Min,
            "max" => Self::Max,
            "var_pop" => Self::VarPop,
            "var_samp" | "variance" | "var" => Self::VarSamp,
            "stddev_pop" => Self::StddevPop,
            "stddev_samp" | "stddev" | "std" => Self::StddevSamp,
            "covar_pop" => Self::CovarPop,
            "covar_samp" => Self::CovarSamp,
            "corr" => Self::Corr,
            "median" => Self::Median,
            "quantile_cont" => Self::QuantileCont,
            "quantile_disc" => Self::QuantileDisc,
            "first" => Self::First,
            "last" => Self::Last,
            "arg_min" => Self::ArgMin,
            "arg_max" => Self::ArgMax,
            "list" | "array_agg" => Self::List,
            "string_agg" => Self::StringAgg,
            _ => return None,
        })
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::Count => "count",
            Self::Sum => "sum",
            Self::Avg => "avg",
            Self::Min => "min",
            Self::Max => "max",
            Self::VarPop => "var_pop",
            Self::VarSamp => "var_samp",
            Self::StddevPop => "stddev_pop",
            Self::StddevSamp => "stddev_samp",
            Self::CovarPop => "covar_pop",
            Self::CovarSamp => "covar_samp",
            Self::Corr => "corr",
            Self::Median => "median",
            Self::QuantileCont => "quantile_cont",
            Self::QuantileDisc => "quantile_disc",
            Self::First => "first",
            Self::Last => "last",
            Self::ArgMin => "arg_min",
            Self::ArgMax => "arg_max",
            Self::List => "list",
            Self::StringAgg => "string_agg",
        }
    }

    pub(crate) const fn requires_secondary(self) -> bool {
        matches!(
            self,
            Self::CovarPop | Self::CovarSamp | Self::Corr | Self::ArgMin | Self::ArgMax
        )
    }

    const fn is_selector(self) -> bool {
        matches!(self, Self::First | Self::Last | Self::ArgMin | Self::ArgMax)
    }

    pub(crate) const fn requires_separator(self) -> bool {
        matches!(self, Self::StringAgg)
    }

    pub(crate) const fn requires_quantile_level(self) -> bool {
        matches!(self, Self::QuantileCont | Self::QuantileDisc)
    }
}

impl ParsedJoinType {
    const fn requires_equi_on(self) -> bool {
        !matches!(self, Self::Cross)
    }
}

impl ParsedPredicate {
    const fn is_all(&self) -> bool {
        matches!(self, Self::All)
    }

    fn columns(&self) -> Vec<&str> {
        let mut columns = Vec::new();
        self.push_columns(&mut columns);
        columns
    }

    fn uses_outer_correlation(&self) -> bool {
        match self {
            Self::Compare { column, .. }
            | Self::NumericArithmeticCompare { column, .. }
            | Self::NumericAbsCompare { column, .. }
            | Self::NumericRoundingCompare { column, .. }
            | Self::DateArithmeticCompare { column, .. }
            | Self::TimestampArithmeticCompare { column, .. }
            | Self::DateExtractCompare { column, .. }
            | Self::TimestampExtractCompare { column, .. }
            | Self::BooleanPredicate { column, .. }
            | Self::IsNull { column }
            | Self::IsNotNull { column }
            | Self::InList { column, .. }
            | Self::StringMatch { column, .. } => is_outer_correlation_ref(column),
            Self::BinaryHelperCompare(predicate) => predicate
                .source_columns
                .iter()
                .any(|column| is_outer_correlation_ref(column)),
            Self::BinaryByteLengthCompare(predicate) => predicate
                .source_columns
                .iter()
                .any(|column| is_outer_correlation_ref(column)),
            Self::ColumnCompare {
                left_column,
                right_column,
                ..
            } => is_outer_correlation_ref(left_column) || is_outer_correlation_ref(right_column),
            Self::StringLengthCompare { source_columns, .. }
            | Self::StringTransformCompare { source_columns, .. }
            | Self::StringFunctionCompare { source_columns, .. }
            | Self::GenericExpressionCompare { source_columns, .. } => source_columns
                .iter()
                .any(|column| is_outer_correlation_ref(column)),
            Self::CastCompare { source_columns, .. } => source_columns
                .iter()
                .any(|column| is_outer_correlation_ref(column)),
            Self::RowValueInList { columns, .. } => columns
                .iter()
                .any(|column| is_outer_correlation_ref(column)),
            Self::RowValueInSubquery { columns, subquery } => {
                columns
                    .iter()
                    .any(|column| is_outer_correlation_ref(column))
                    || subquery.uses_outer_correlation()
            }
            Self::Logical { left, right, .. } => {
                left.uses_outer_correlation() || right.uses_outer_correlation()
            }
            Self::Not { inner } => inner.uses_outer_correlation(),
            Self::ExistsSubquery { subquery } => subquery.uses_outer_correlation(),
            Self::InSubquery {
                column, subquery, ..
            }
            | Self::QuantifiedSubquery {
                column, subquery, ..
            } => is_outer_correlation_ref(column) || subquery.uses_outer_correlation(),
            Self::All => false,
        }
    }

    fn push_columns<'a>(&'a self, columns: &mut Vec<&'a str>) {
        match self {
            Self::All | Self::ExistsSubquery { .. } => {}
            Self::Compare { column, .. }
            | Self::NumericArithmeticCompare { column, .. }
            | Self::NumericAbsCompare { column, .. }
            | Self::NumericRoundingCompare { column, .. }
            | Self::DateArithmeticCompare { column, .. }
            | Self::TimestampArithmeticCompare { column, .. }
            | Self::DateExtractCompare { column, .. }
            | Self::TimestampExtractCompare { column, .. }
            | Self::BooleanPredicate { column, .. }
            | Self::IsNull { column }
            | Self::IsNotNull { column }
            | Self::InList { column, .. }
            | Self::InSubquery { column, .. }
            | Self::QuantifiedSubquery { column, .. }
            | Self::StringMatch { column, .. } => columns.push(column),
            Self::BinaryHelperCompare(predicate) => {
                columns.extend(predicate.source_columns.iter().map(String::as_str));
            }
            Self::BinaryByteLengthCompare(predicate) => {
                columns.extend(predicate.source_columns.iter().map(String::as_str));
            }
            Self::RowValueInList {
                columns: row_columns,
                ..
            }
            | Self::RowValueInSubquery {
                columns: row_columns,
                ..
            } => columns.extend(row_columns.iter().map(String::as_str)),
            Self::ColumnCompare {
                left_column,
                right_column,
                ..
            } => {
                columns.push(left_column);
                columns.push(right_column);
            }
            Self::StringLengthCompare { source_columns, .. }
            | Self::StringTransformCompare { source_columns, .. }
            | Self::StringFunctionCompare { source_columns, .. }
            | Self::GenericExpressionCompare { source_columns, .. }
            | Self::CastCompare { source_columns, .. } => {
                columns.extend(source_columns.iter().map(String::as_str));
            }
            Self::Logical { left, right, .. } => {
                left.push_columns(columns);
                right.push_columns(columns);
            }
            Self::Not { inner } => inner.push_columns(columns),
        }
    }

    fn to_expression(&self) -> Result<Expression, ShardLoomError> {
        match self {
            Self::All => Err(ShardLoomError::InvalidOperation(
                "internal error: all-rows predicate should not be lowered to an expression"
                    .to_string(),
            )),
            Self::Compare { column, op, value } => compare_expression(column, *op, value),
            Self::ColumnCompare {
                left_column,
                op,
                right_column,
            } => column_compare_expression(left_column, *op, right_column),
            Self::CastCompare { .. } => self.cast_compare_expression(),
            Self::NumericArithmeticCompare {
                column,
                op,
                rhs,
                comparison,
                value,
            } => numeric_arithmetic_compare_expression(column, *op, rhs, *comparison, value),
            Self::NumericAbsCompare {
                column,
                comparison,
                value,
            } => numeric_abs_compare_expression(column, *comparison, value),
            Self::NumericRoundingCompare {
                column,
                op,
                comparison,
                value,
            } => numeric_rounding_compare_expression(column, *op, *comparison, value),
            Self::GenericExpressionCompare {
                left,
                comparison,
                right,
                ..
            } => {
                generic_expression_compare_expression(&left.plain()?, *comparison, &right.plain()?)
            }
            Self::DateArithmeticCompare {
                column,
                op,
                day_count,
                comparison,
                value,
            } => date_arithmetic_compare_expression(column, *op, *day_count, *comparison, value),
            Self::TimestampArithmeticCompare { .. } => self.timestamp_arithmetic_expression(),
            Self::DateExtractCompare {
                column,
                op,
                comparison,
                value,
            } => date_extract_compare_expression(column, *op, *comparison, value),
            Self::StringLengthCompare { .. }
            | Self::StringTransformCompare { .. }
            | Self::StringFunctionCompare { .. } => self.string_compare_expression(),
            Self::BinaryHelperCompare(predicate) => binary_helper_compare_expression(predicate),
            Self::BinaryByteLengthCompare(predicate) => {
                binary_byte_length_compare_expression(predicate)
            }
            _ => self.secondary_to_expression(),
        }
    }

    fn secondary_to_expression(&self) -> Result<Expression, ShardLoomError> {
        match self {
            Self::TimestampExtractCompare {
                column,
                op,
                comparison,
                value,
            } => timestamp_extract_compare_expression(column, *op, *comparison, value),
            Self::BooleanPredicate {
                column,
                expected,
                null_is_false,
                negated,
            } => boolean_predicate_expression(column, *expected, *null_is_false, *negated),
            Self::IsNull { column } => null_predicate_expression(column, true),
            Self::IsNotNull { column } => null_predicate_expression(column, false),
            Self::InList { column, values } => in_list_expression(column, values),
            Self::RowValueInList { columns, tuples } => {
                row_value_in_list_expression(columns, tuples)
            }
            Self::RowValueInSubquery { columns, subquery } => {
                row_value_in_subquery_expression(columns, subquery)
            }
            Self::InSubquery { column, subquery } => in_subquery_expression(column, subquery),
            Self::QuantifiedSubquery {
                column,
                comparison,
                quantifier,
                subquery,
            } => quantified_subquery_expression(column, *comparison, *quantifier, subquery),
            Self::ExistsSubquery { subquery } => exists_subquery_expression(subquery),
            Self::StringMatch { .. } => self.string_match_expression(),
            Self::Logical { op, left, right } => Ok(Expression::new(
                ExprId::new(format!("where.logical.{}", op.as_str()))?,
                ExpressionKind::Binary {
                    left: Box::new(left.to_expression()?),
                    op: op.binary_op(),
                    right: Box::new(right.to_expression()?),
                },
            )),
            Self::Not { inner } => Ok(Expression::new(
                ExprId::new("where.logical.not")?,
                ExpressionKind::Unary {
                    op: UnaryOp::Not,
                    expr: Box::new(inner.to_expression()?),
                },
            )),
            _ => Err(ShardLoomError::InvalidOperation(
                "internal error: predicate variant should be lowered by primary expression dispatcher"
                    .to_string(),
            )),
        }
    }

    fn string_match_expression(&self) -> Result<Expression, ShardLoomError> {
        let Self::StringMatch {
            column, op, value, ..
        } = self
        else {
            unreachable!("string_match_expression called for non-string-match predicate")
        };
        string_match_expression(column, *op, value)
    }

    fn string_compare_expression(&self) -> Result<Expression, ShardLoomError> {
        match self {
            Self::StringLengthCompare {
                expression,
                comparison,
                value,
                ..
            } => string_length_compare_expression(expression, *comparison, value),
            Self::StringTransformCompare {
                expression,
                comparison,
                value,
                ..
            } => string_transform_compare_expression(expression, *comparison, value),
            Self::StringFunctionCompare {
                expression,
                comparison,
                value,
                ..
            } => string_function_compare_expression(expression, *comparison, value),
            Self::BinaryHelperCompare(_) => Err(ShardLoomError::InvalidOperation(
                "internal error: binary helper predicate cannot lower through string expression"
                    .to_string(),
            )),
            Self::BinaryByteLengthCompare(_) => Err(ShardLoomError::InvalidOperation(
                "internal error: binary byte length predicate cannot lower through string expression"
                    .to_string(),
            )),
            _ => Err(ShardLoomError::InvalidOperation(
                "internal error: non-string predicate cannot lower through string expression"
                    .to_string(),
            )),
        }
    }

    fn cast_compare_expression(&self) -> Result<Expression, ShardLoomError> {
        let Self::CastCompare {
            column,
            expression,
            target_dtype,
            mode,
            op,
            value,
            ..
        } = self
        else {
            return Err(ShardLoomError::InvalidOperation(
                "internal error: non-cast predicate cannot lower through cast expression"
                    .to_string(),
            ));
        };
        cast_compare_expression(column, expression, target_dtype, *mode, *op, value)
    }

    fn timestamp_arithmetic_expression(&self) -> Result<Expression, ShardLoomError> {
        let Self::TimestampArithmeticCompare {
            column,
            op,
            second_count,
            comparison,
            value,
        } = self
        else {
            return Err(ShardLoomError::InvalidOperation(
                "internal error: non-timestamp-arithmetic predicate lowered through timestamp arithmetic path"
                    .to_string(),
            ));
        };
        timestamp_arithmetic_compare_expression(column, *op, *second_count, *comparison, value)
    }

    fn uses_generic_expression(&self) -> bool {
        match self {
            Self::GenericExpressionCompare { .. } => true,
            Self::Logical { left, right, .. } => {
                left.uses_generic_expression() || right.uses_generic_expression()
            }
            Self::Not { inner } => inner.uses_generic_expression(),
            Self::All
            | Self::Compare { .. }
            | Self::ColumnCompare { .. }
            | Self::CastCompare { .. }
            | Self::NumericArithmeticCompare { .. }
            | Self::NumericAbsCompare { .. }
            | Self::NumericRoundingCompare { .. }
            | Self::DateArithmeticCompare { .. }
            | Self::TimestampArithmeticCompare { .. }
            | Self::DateExtractCompare { .. }
            | Self::StringLengthCompare { .. }
            | Self::TimestampExtractCompare { .. }
            | Self::StringTransformCompare { .. }
            | Self::StringFunctionCompare { .. }
            | Self::BooleanPredicate { .. }
            | Self::IsNull { .. }
            | Self::IsNotNull { .. }
            | Self::InList { .. }
            | Self::RowValueInList { .. }
            | Self::RowValueInSubquery { .. }
            | Self::InSubquery { .. }
            | Self::QuantifiedSubquery { .. }
            | Self::ExistsSubquery { .. }
            | Self::StringMatch { .. }
            | Self::BinaryHelperCompare { .. }
            | Self::BinaryByteLengthCompare { .. } => false,
        }
    }

    fn contains_logical_or(&self) -> bool {
        match self {
            Self::Logical { op, left, right } => {
                *op == LogicalPredicateOp::Or
                    || left.contains_logical_or()
                    || right.contains_logical_or()
            }
            Self::Not { inner } => inner.contains_logical_or(),
            Self::All
            | Self::Compare { .. }
            | Self::ColumnCompare { .. }
            | Self::CastCompare { .. }
            | Self::NumericArithmeticCompare { .. }
            | Self::NumericAbsCompare { .. }
            | Self::NumericRoundingCompare { .. }
            | Self::GenericExpressionCompare { .. }
            | Self::DateArithmeticCompare { .. }
            | Self::TimestampArithmeticCompare { .. }
            | Self::DateExtractCompare { .. }
            | Self::StringLengthCompare { .. }
            | Self::TimestampExtractCompare { .. }
            | Self::StringTransformCompare { .. }
            | Self::StringFunctionCompare { .. }
            | Self::BooleanPredicate { .. }
            | Self::IsNull { .. }
            | Self::IsNotNull { .. }
            | Self::InList { .. }
            | Self::RowValueInList { .. }
            | Self::RowValueInSubquery { .. }
            | Self::InSubquery { .. }
            | Self::QuantifiedSubquery { .. }
            | Self::ExistsSubquery { .. }
            | Self::StringMatch { .. }
            | Self::BinaryHelperCompare { .. }
            | Self::BinaryByteLengthCompare { .. } => false,
        }
    }
}

fn compare_expression(
    column: &str,
    op: ComparisonOp,
    value: &ScalarValue,
) -> Result<Expression, ShardLoomError> {
    Ok(Expression::new(
        ExprId::new("where.compare")?,
        ExpressionKind::Compare {
            left: Box::new(Expression::column(
                ExprId::new(format!("where.{column}"))?,
                ColumnRef::new(column.to_string())?,
            )),
            op,
            right: Box::new(Expression::literal(
                ExprId::new("where.literal")?,
                value.clone(),
            )),
        },
    ))
}

fn column_compare_expression(
    left_column: &str,
    op: ComparisonOp,
    right_column: &str,
) -> Result<Expression, ShardLoomError> {
    Ok(Expression::new(
        ExprId::new("where.column_compare")?,
        ExpressionKind::Compare {
            left: Box::new(Expression::column(
                ExprId::new(format!("where.{left_column}"))?,
                ColumnRef::new(left_column.to_string())?,
            )),
            op,
            right: Box::new(Expression::column(
                ExprId::new(format!("where.{right_column}"))?,
                ColumnRef::new(right_column.to_string())?,
            )),
        },
    ))
}

fn cast_compare_expression(
    source_label: &str,
    source_expression: &Expression,
    target_dtype: &LogicalDType,
    mode: CastMode,
    op: ComparisonOp,
    value: &ScalarValue,
) -> Result<Expression, ShardLoomError> {
    Ok(Expression::new(
        ExprId::new("where.cast_compare")?,
        ExpressionKind::Compare {
            left: Box::new(mode.build_expression(
                ExprId::new(format!("where.cast.{source_label}"))?,
                source_expression.clone(),
                target_dtype.clone(),
            )),
            op,
            right: Box::new(Expression::literal(
                ExprId::new("where.cast.literal")?,
                value.clone(),
            )),
        },
    ))
}

fn numeric_arithmetic_compare_expression(
    column: &str,
    op: NumericArithmeticOp,
    rhs: &ScalarValue,
    comparison: ComparisonOp,
    value: &ScalarValue,
) -> Result<Expression, ShardLoomError> {
    Ok(Expression::new(
        ExprId::new("where.numeric_arithmetic_compare")?,
        ExpressionKind::Compare {
            left: Box::new(Expression::new(
                ExprId::new(format!("where.numeric_arithmetic.{column}"))?,
                ExpressionKind::Binary {
                    left: Box::new(Expression::column(
                        ExprId::new(format!("where.{column}"))?,
                        ColumnRef::new(column.to_string())?,
                    )),
                    op: op.binary_op(),
                    right: Box::new(Expression::literal(
                        ExprId::new("where.numeric_arithmetic.literal")?,
                        rhs.clone(),
                    )),
                },
            )),
            op: comparison,
            right: Box::new(Expression::literal(
                ExprId::new("where.numeric_arithmetic.compare_literal")?,
                value.clone(),
            )),
        },
    ))
}

fn numeric_abs_compare_expression(
    column: &str,
    comparison: ComparisonOp,
    value: &ScalarValue,
) -> Result<Expression, ShardLoomError> {
    Ok(Expression::new(
        ExprId::new("where.numeric_abs_compare")?,
        ExpressionKind::Compare {
            left: Box::new(Expression::new(
                ExprId::new(format!("where.numeric_abs.{column}"))?,
                ExpressionKind::FunctionCall {
                    name: "abs".to_string(),
                    args: vec![Expression::column(
                        ExprId::new(format!("where.{column}"))?,
                        ColumnRef::new(column.to_string())?,
                    )],
                },
            )),
            op: comparison,
            right: Box::new(Expression::literal(
                ExprId::new("where.numeric_abs.literal")?,
                value.clone(),
            )),
        },
    ))
}

fn numeric_rounding_compare_expression(
    column: &str,
    op: NumericRoundingOp,
    comparison: ComparisonOp,
    value: &ScalarValue,
) -> Result<Expression, ShardLoomError> {
    Ok(Expression::new(
        ExprId::new("where.numeric_rounding_compare")?,
        ExpressionKind::Compare {
            left: Box::new(Expression::new(
                ExprId::new(format!("where.numeric_rounding.{column}"))?,
                ExpressionKind::FunctionCall {
                    name: op.function_name().to_string(),
                    args: vec![Expression::column(
                        ExprId::new(format!("where.{column}"))?,
                        ColumnRef::new(column.to_string())?,
                    )],
                },
            )),
            op: comparison,
            right: Box::new(Expression::literal(
                ExprId::new("where.numeric_rounding.literal")?,
                value.clone(),
            )),
        },
    ))
}

fn generic_expression_compare_expression(
    left: &Expression,
    comparison: ComparisonOp,
    right: &Expression,
) -> Result<Expression, ShardLoomError> {
    Ok(Expression::new(
        ExprId::new("where.generic_expression_compare")?,
        ExpressionKind::Compare {
            left: Box::new(left.clone()),
            op: comparison,
            right: Box::new(right.clone()),
        },
    ))
}

fn string_match_expression(
    column: &str,
    op: StringPredicateOp,
    value: &str,
) -> Result<Expression, ShardLoomError> {
    Ok(Expression::new(
        ExprId::new(format!("where.string.{}", op.as_str()))?,
        ExpressionKind::FunctionCall {
            name: op.function_name().to_string(),
            args: vec![
                Expression::column(
                    ExprId::new(format!("where.{column}"))?,
                    ColumnRef::new(column.to_string())?,
                ),
                Expression::literal(
                    ExprId::new("where.string.literal")?,
                    ScalarValue::Utf8(value.to_string()),
                ),
            ],
        },
    ))
}

fn string_transform_compare_expression(
    expression: &Expression,
    comparison: ComparisonOp,
    value: &ScalarValue,
) -> Result<Expression, ShardLoomError> {
    Ok(Expression::new(
        ExprId::new("where.string_transform_compare")?,
        ExpressionKind::Compare {
            left: Box::new(expression.clone()),
            op: comparison,
            right: Box::new(Expression::literal(
                ExprId::new("where.string_transform.literal")?,
                value.clone(),
            )),
        },
    ))
}

fn string_length_compare_expression(
    expression: &Expression,
    comparison: ComparisonOp,
    value: &ScalarValue,
) -> Result<Expression, ShardLoomError> {
    Ok(Expression::new(
        ExprId::new("where.string_length_compare")?,
        ExpressionKind::Compare {
            left: Box::new(expression.clone()),
            op: comparison,
            right: Box::new(Expression::literal(
                ExprId::new("where.string_length.literal")?,
                value.clone(),
            )),
        },
    ))
}

fn string_function_compare_expression(
    expression: &Expression,
    comparison: ComparisonOp,
    value: &ScalarValue,
) -> Result<Expression, ShardLoomError> {
    Ok(Expression::new(
        ExprId::new("where.string_function_compare")?,
        ExpressionKind::Compare {
            left: Box::new(expression.clone()),
            op: comparison,
            right: Box::new(Expression::literal(
                ExprId::new("where.string_function.literal")?,
                value.clone(),
            )),
        },
    ))
}

fn binary_helper_compare_expression(
    predicate: &ParsedBinaryHelperPredicate,
) -> Result<Expression, ShardLoomError> {
    Ok(Expression::new(
        ExprId::new("where.binary_helper_compare")?,
        ExpressionKind::Compare {
            left: Box::new(Expression::new(
                ExprId::new("where.binary_helper")?,
                ExpressionKind::FunctionCall {
                    name: predicate.op.function_name().to_string(),
                    args: vec![predicate.expression.clone()],
                },
            )),
            op: predicate.comparison,
            right: Box::new(Expression::literal(
                ExprId::new("where.binary_helper.literal")?,
                ScalarValue::Binary(predicate.value.clone()),
            )),
        },
    ))
}

fn binary_byte_length_compare_expression(
    predicate: &ParsedBinaryByteLengthPredicate,
) -> Result<Expression, ShardLoomError> {
    Ok(Expression::new(
        ExprId::new("where.binary_byte_length_compare")?,
        ExpressionKind::Compare {
            left: Box::new(predicate.expression.clone()),
            op: predicate.comparison,
            right: Box::new(Expression::literal(
                ExprId::new("where.binary_byte_length.literal")?,
                predicate.value.clone(),
            )),
        },
    ))
}

fn boolean_predicate_expression(
    column: &str,
    expected: bool,
    null_is_false: bool,
    negated: bool,
) -> Result<Expression, ShardLoomError> {
    let column_expression = Expression::column(
        ExprId::new(format!("where.boolean.{column}"))?,
        ColumnRef::new(column.to_string())?,
    );
    let value_expression = if expected {
        column_expression
    } else {
        Expression::new(
            ExprId::new("where.boolean.is_false")?,
            ExpressionKind::Unary {
                op: UnaryOp::Not,
                expr: Box::new(column_expression),
            },
        )
    };
    let value_expression = if null_is_false {
        Expression::new(
            ExprId::new("where.boolean.null_is_false")?,
            ExpressionKind::Binary {
                left: Box::new(value_expression),
                op: BinaryOp::And,
                right: Box::new(null_predicate_expression(column, false)?),
            },
        )
    } else {
        value_expression
    };
    if negated {
        Ok(Expression::new(
            ExprId::new("where.boolean.is_not_truth")?,
            ExpressionKind::Unary {
                op: UnaryOp::Not,
                expr: Box::new(value_expression),
            },
        ))
    } else {
        Ok(value_expression)
    }
}

fn null_predicate_expression(column: &str, is_null: bool) -> Result<Expression, ShardLoomError> {
    Ok(Expression::new(
        ExprId::new(if is_null {
            "where.is_null"
        } else {
            "where.is_not_null"
        })?,
        ExpressionKind::Unary {
            op: if is_null {
                UnaryOp::IsNull
            } else {
                UnaryOp::IsNotNull
            },
            expr: Box::new(Expression::column(
                ExprId::new(format!("where.{column}"))?,
                ColumnRef::new(column.to_string())?,
            )),
        },
    ))
}

fn date_arithmetic_compare_expression(
    column: &str,
    op: DateArithmeticOp,
    day_count: i32,
    comparison: ComparisonOp,
    value: &ScalarValue,
) -> Result<Expression, ShardLoomError> {
    Ok(Expression::new(
        ExprId::new("where.date_arithmetic_compare")?,
        ExpressionKind::Compare {
            left: Box::new(Expression::new(
                ExprId::new(format!("where.date_arithmetic.{column}"))?,
                ExpressionKind::FunctionCall {
                    name: op.function_name().to_string(),
                    args: vec![
                        Expression::column(
                            ExprId::new(format!("where.{column}"))?,
                            ColumnRef::new(column.to_string())?,
                        ),
                        Expression::literal(
                            ExprId::new("where.date_arithmetic.days")?,
                            ScalarValue::Int64(i64::from(day_count)),
                        ),
                    ],
                },
            )),
            op: comparison,
            right: Box::new(Expression::literal(
                ExprId::new("where.date_arithmetic.literal")?,
                value.clone(),
            )),
        },
    ))
}

fn timestamp_arithmetic_compare_expression(
    column: &str,
    op: TimestampArithmeticOp,
    second_count: i64,
    comparison: ComparisonOp,
    value: &ScalarValue,
) -> Result<Expression, ShardLoomError> {
    Ok(Expression::new(
        ExprId::new("where.timestamp_arithmetic_compare")?,
        ExpressionKind::Compare {
            left: Box::new(Expression::new(
                ExprId::new(format!("where.timestamp_arithmetic.{column}"))?,
                ExpressionKind::FunctionCall {
                    name: op.function_name().to_string(),
                    args: vec![
                        Expression::column(
                            ExprId::new(format!("where.{column}"))?,
                            ColumnRef::new(column.to_string())?,
                        ),
                        Expression::literal(
                            ExprId::new("where.timestamp_arithmetic.seconds")?,
                            ScalarValue::Int64(second_count),
                        ),
                    ],
                },
            )),
            op: comparison,
            right: Box::new(Expression::literal(
                ExprId::new("where.timestamp_arithmetic.literal")?,
                value.clone(),
            )),
        },
    ))
}

fn date_extract_compare_expression(
    column: &str,
    op: DateExtractOp,
    comparison: ComparisonOp,
    value: &ScalarValue,
) -> Result<Expression, ShardLoomError> {
    Ok(Expression::new(
        ExprId::new("where.date_extract_compare")?,
        ExpressionKind::Compare {
            left: Box::new(Expression::new(
                ExprId::new(format!("where.date_extract.{column}"))?,
                ExpressionKind::FunctionCall {
                    name: op.function_name().to_string(),
                    args: vec![Expression::column(
                        ExprId::new(format!("where.{column}"))?,
                        ColumnRef::new(column.to_string())?,
                    )],
                },
            )),
            op: comparison,
            right: Box::new(Expression::literal(
                ExprId::new("where.date_extract.literal")?,
                value.clone(),
            )),
        },
    ))
}

fn timestamp_extract_compare_expression(
    column: &str,
    op: TimestampExtractOp,
    comparison: ComparisonOp,
    value: &ScalarValue,
) -> Result<Expression, ShardLoomError> {
    Ok(Expression::new(
        ExprId::new("where.timestamp_extract_compare")?,
        ExpressionKind::Compare {
            left: Box::new(Expression::new(
                ExprId::new(format!("where.timestamp_extract.{column}"))?,
                ExpressionKind::FunctionCall {
                    name: op.function_name().to_string(),
                    args: vec![Expression::column(
                        ExprId::new(format!("where.{column}"))?,
                        ColumnRef::new(column.to_string())?,
                    )],
                },
            )),
            op: comparison,
            right: Box::new(Expression::literal(
                ExprId::new("where.timestamp_extract.literal")?,
                value.clone(),
            )),
        },
    ))
}

fn in_list_expression(column: &str, values: &[ScalarValue]) -> Result<Expression, ShardLoomError> {
    let mut values = values.iter().enumerate();
    let Some((first_index, first_value)) = values.next() else {
        return Err(unsupported_sql_error(
            "IN predicates require at least one literal value",
        ));
    };
    let mut expression = in_list_equality_expression(column, first_value, first_index)?;
    for (index, value) in values {
        expression = Expression::new(
            ExprId::new(format!("where.in.or.{index}"))?,
            ExpressionKind::Binary {
                left: Box::new(expression),
                op: BinaryOp::Or,
                right: Box::new(in_list_equality_expression(column, value, index)?),
            },
        );
    }
    Ok(expression)
}

fn row_value_in_list_expression(
    columns: &[String],
    tuples: &[Vec<ScalarValue>],
) -> Result<Expression, ShardLoomError> {
    if columns.len() < 2 {
        return Err(unsupported_sql_error(
            "row-value IN predicates require at least two source columns",
        ));
    }
    let mut tuples = tuples.iter().enumerate();
    let Some((first_index, first_tuple)) = tuples.next() else {
        return Err(unsupported_sql_error(
            "row-value IN predicates require at least one literal tuple",
        ));
    };
    let mut expression = row_value_tuple_equality_expression(columns, first_tuple, first_index)?;
    for (tuple_index, tuple) in tuples {
        expression = Expression::new(
            ExprId::new(format!("where.row_value_in.or.{tuple_index}"))?,
            ExpressionKind::Binary {
                left: Box::new(expression),
                op: BinaryOp::Or,
                right: Box::new(row_value_tuple_equality_expression(
                    columns,
                    tuple,
                    tuple_index,
                )?),
            },
        );
    }
    Ok(expression)
}

fn row_value_tuple_equality_expression(
    columns: &[String],
    tuple: &[ScalarValue],
    tuple_index: usize,
) -> Result<Expression, ShardLoomError> {
    if tuple.len() != columns.len() {
        return Err(unsupported_sql_error(
            "row-value IN literal tuple arity must match the source column count",
        ));
    }
    let mut comparisons = columns.iter().zip(tuple).enumerate();
    let Some((first_column_index, (first_column, first_value))) = comparisons.next() else {
        return Err(unsupported_sql_error(
            "row-value IN predicates require at least two source columns",
        ));
    };
    let mut expression = row_value_component_equality_expression(
        first_column,
        first_value,
        tuple_index,
        first_column_index,
    )?;
    for (column_index, (column, value)) in comparisons {
        expression = Expression::new(
            ExprId::new(format!(
                "where.row_value_in.and.{tuple_index}.{column_index}"
            ))?,
            ExpressionKind::Binary {
                left: Box::new(expression),
                op: BinaryOp::And,
                right: Box::new(row_value_component_equality_expression(
                    column,
                    value,
                    tuple_index,
                    column_index,
                )?),
            },
        );
    }
    Ok(expression)
}

fn row_value_component_equality_expression(
    column: &str,
    value: &ScalarValue,
    tuple_index: usize,
    column_index: usize,
) -> Result<Expression, ShardLoomError> {
    Ok(Expression::new(
        ExprId::new(format!(
            "where.row_value_in.compare.{tuple_index}.{column_index}"
        ))?,
        ExpressionKind::Compare {
            left: Box::new(Expression::column(
                ExprId::new(format!(
                    "where.row_value_in.{column}.{tuple_index}.{column_index}"
                ))?,
                ColumnRef::new(column.to_string())?,
            )),
            op: ComparisonOp::Eq,
            right: Box::new(Expression::literal(
                ExprId::new(format!(
                    "where.row_value_in.literal.{tuple_index}.{column_index}"
                ))?,
                value.clone(),
            )),
        },
    ))
}

fn in_subquery_expression(
    column: &str,
    subquery: &ParsedInSubquery,
) -> Result<Expression, ShardLoomError> {
    if subquery.values.is_empty() {
        return Ok(Expression::literal(
            ExprId::new("where.in_subquery.empty")?,
            ScalarValue::Boolean(false),
        ));
    }
    in_list_expression(column, &subquery.values)
}

fn quantified_subquery_expression(
    column: &str,
    comparison: ComparisonOp,
    quantifier: ParsedQuantifiedSubqueryQuantifier,
    subquery: &ParsedInSubquery,
) -> Result<Expression, ShardLoomError> {
    let mut values = subquery.values.iter().enumerate();
    let Some((first_index, first_value)) = values.next() else {
        return Ok(Expression::literal(
            ExprId::new("where.quantified_subquery.empty")?,
            ScalarValue::Boolean(quantifier.empty_result()),
        ));
    };
    let mut expression =
        quantified_subquery_compare_expression(column, comparison, first_value, first_index)?;
    for (index, value) in values {
        expression = Expression::new(
            ExprId::new(format!(
                "where.quantified_subquery.{}.{}",
                quantifier.as_str(),
                index
            ))?,
            ExpressionKind::Binary {
                left: Box::new(expression),
                op: quantifier.binary_op(),
                right: Box::new(quantified_subquery_compare_expression(
                    column, comparison, value, index,
                )?),
            },
        );
    }
    Ok(expression)
}

fn quantified_subquery_compare_expression(
    column: &str,
    comparison: ComparisonOp,
    value: &ScalarValue,
    index: usize,
) -> Result<Expression, ShardLoomError> {
    Ok(Expression::new(
        ExprId::new(format!("where.quantified_subquery.compare.{index}"))?,
        ExpressionKind::Compare {
            left: Box::new(Expression::column(
                ExprId::new(format!("where.quantified_subquery.{column}.{index}"))?,
                ColumnRef::new(column.to_string())?,
            )),
            op: comparison,
            right: Box::new(Expression::literal(
                ExprId::new(format!("where.quantified_subquery.literal.{index}"))?,
                value.clone(),
            )),
        },
    ))
}

fn row_value_in_subquery_expression(
    columns: &[String],
    subquery: &ParsedRowValueInSubquery,
) -> Result<Expression, ShardLoomError> {
    if subquery.tuples.is_empty() {
        return Ok(Expression::literal(
            ExprId::new("where.row_value_in_subquery.empty")?,
            ScalarValue::Boolean(false),
        ));
    }
    row_value_in_list_expression(columns, &subquery.tuples)
}

fn exists_subquery_expression(
    subquery: &ParsedExistsSubquery,
) -> Result<Expression, ShardLoomError> {
    Ok(Expression::literal(
        ExprId::new("where.exists_subquery.result")?,
        ScalarValue::Boolean(subquery.exists),
    ))
}

fn in_list_equality_expression(
    column: &str,
    value: &ScalarValue,
    index: usize,
) -> Result<Expression, ShardLoomError> {
    Ok(Expression::new(
        ExprId::new(format!("where.in.compare.{index}"))?,
        ExpressionKind::Compare {
            left: Box::new(Expression::column(
                ExprId::new(format!("where.in.{column}.{index}"))?,
                ColumnRef::new(column.to_string())?,
            )),
            op: ComparisonOp::Eq,
            right: Box::new(Expression::literal(
                ExprId::new(format!("where.in.literal.{index}"))?,
                value.clone(),
            )),
        },
    ))
}

fn earliest_clause_index_after(start: usize, indexes: &[Option<usize>]) -> usize {
    indexes
        .iter()
        .flatten()
        .copied()
        .filter(|index| *index > start)
        .min()
        .expect("at least one later SQL clause exists")
}

fn sql_local_source_clause_indexes(
    statement: &str,
) -> Result<SqlLocalSourceClauseIndexes, ShardLoomError> {
    let from_clause = find_sql_source_from_clause(statement)?.ok_or_else(|| {
        unsupported_sql_error("SQL local-source runtime requires a FROM <local.csv> clause")
    })?;
    let limit_clause = find_keyword_outside_quotes_and_parentheses(statement, "limit")?
        .ok_or_else(|| {
            unsupported_sql_error("SQL local-source runtime requires a LIMIT <n> clause")
        })?;
    let indexes = SqlLocalSourceClauseIndexes {
        from: from_clause,
        limit: limit_clause,
        filter: find_keyword_outside_quotes_and_parentheses(statement, "where")?,
        group_by: find_keyword_outside_quotes_and_parentheses(statement, "group by")?,
        having: find_keyword_outside_quotes_and_parentheses(statement, "having")?,
        order_by: find_keyword_outside_quotes_and_parentheses(statement, "order by")?,
    };
    validate_sql_local_source_clause_order(indexes)?;
    Ok(indexes)
}

fn find_sql_source_from_clause(raw: &str) -> Result<Option<usize>, ShardLoomError> {
    shardloom_plan::sql_binder::find_source_from_clause(raw)
}

fn validate_sql_local_source_clause_order(
    indexes: SqlLocalSourceClauseIndexes,
) -> Result<(), ShardLoomError> {
    if !(indexes.from > 6 && indexes.limit > indexes.from)
        || indexes
            .filter
            .is_some_and(|index| !(index > indexes.from && index < indexes.limit))
        || indexes
            .group_by
            .is_some_and(|index| !(index > indexes.from && index < indexes.limit))
        || indexes
            .having
            .is_some_and(|index| !(index > indexes.from && index < indexes.limit))
        || indexes
            .order_by
            .is_some_and(|index| !(index > indexes.from && index < indexes.limit))
        || indexes
            .filter
            .zip(indexes.group_by)
            .is_some_and(|(filter, group_by)| filter > group_by)
        || indexes
            .filter
            .zip(indexes.having)
            .is_some_and(|(filter, having)| filter > having)
        || indexes
            .filter
            .zip(indexes.order_by)
            .is_some_and(|(filter, order_by)| filter > order_by)
        || indexes
            .group_by
            .zip(indexes.having)
            .is_some_and(|(group_by, having)| group_by > having)
        || indexes
            .group_by
            .zip(indexes.order_by)
            .is_some_and(|(group_by, order_by)| group_by > order_by)
        || indexes
            .having
            .zip(indexes.order_by)
            .is_some_and(|(having, order_by)| having > order_by)
    {
        return Err(unsupported_sql_error(
            "SQL local-source runtime requires SELECT ... FROM ... [WHERE ...] [GROUP BY ...] [HAVING ...] [ORDER BY ...] LIMIT ... order",
        ));
    }
    Ok(())
}

fn parse_sql_local_source_union_statement(
    raw: &str,
) -> Result<ParsedSqlLocalSourceUnion, ShardLoomError> {
    let statement = normalize_sql_statement(raw)?;
    validate_sql_cte_policy_boundary(&statement)?;
    validate_advanced_scalar_policy_boundaries(&statement)?;
    validate_complex_dtype_policy_boundaries_with_sql_union(&statement, true)?;

    let operators = top_level_sql_union_operators(&statement)?;
    if operators.is_empty() {
        return Err(unsupported_sql_error(
            "SQL set-operation runtime requires at least two SELECT branches",
        ));
    }
    let mode = operators[0].mode;
    if operators.iter().any(|operator| operator.mode != mode) {
        return Err(unsupported_sql_error(
            "Mixed SQL set-operation chains are not admitted in this scoped local-source runtime; use one set operator and mode per statement",
        ));
    }
    let last_union_index = operators.last().expect("operators checked non-empty").index;
    let limit_indexes = top_level_keyword_indexes(&statement, "limit")?;
    let Some(&limit_index) = limit_indexes.last() else {
        return Err(unsupported_sql_error(
            "SQL set-operation local-source runtime requires one global LIMIT <n> clause",
        ));
    };
    if limit_indexes.iter().any(|index| *index < last_union_index) || limit_indexes.len() > 1 {
        return Err(unsupported_sql_error(
            "SQL set-operation branch-local LIMIT clauses are not admitted; apply one global LIMIT after the set-operation chain",
        ));
    }
    let order_by_indexes = top_level_keyword_indexes(&statement, "order by")?;
    if order_by_indexes
        .iter()
        .any(|index| *index < last_union_index)
        || order_by_indexes.len() > 1
    {
        return Err(unsupported_sql_error(
            "SQL set-operation branch-local ORDER BY clauses are not admitted; apply one global ORDER BY after the set-operation chain",
        ));
    }
    let order_by_index = order_by_indexes.first().copied();
    if order_by_index.is_some_and(|index| index > limit_index) {
        return Err(unsupported_sql_error(
            "SQL set-operation global ORDER BY must appear before the global LIMIT clause",
        ));
    }
    if limit_index < last_union_index {
        return Err(unsupported_sql_error(
            "SQL set-operation global LIMIT must appear after the SELECT branches",
        ));
    }

    let limit_raw = statement[limit_index + "limit".len()..].trim();
    if limit_raw.is_empty() || limit_clause_contains_sql_clause_keyword(limit_raw) {
        return Err(unsupported_sql_error(
            "SQL set-operation global LIMIT admits one non-negative integer literal only",
        ));
    }
    let limit = parse_limit(limit_raw)?;
    let union_body_end = order_by_index.unwrap_or(limit_index);
    let order_by = order_by_index
        .map(|index| {
            let raw = statement[index + "order by".len()..limit_index].trim();
            parse_order_by(Some(raw))
        })
        .transpose()?
        .flatten();
    let branch_statements = sql_union_branch_statements(&statement, &operators, union_body_end)?;
    let branches = branch_statements
        .iter()
        .map(|branch| parse_sql_local_source_statement(branch))
        .collect::<Result<Vec<_>, _>>()?;
    if branches.len() < 2 {
        return Err(unsupported_sql_error(
            "SQL set operation requires at least two SELECT branches",
        ));
    }

    Ok(ParsedSqlLocalSourceUnion {
        normalized_statement: statement,
        mode,
        branch_statements,
        branches,
        order_by,
        limit,
        offset: 0,
    })
}

fn validate_sql_cte_policy_boundary(statement: &str) -> Result<(), ShardLoomError> {
    if starts_with_keyword(statement.trim_start(), "with") {
        return Err(unsupported_sql_error(
            "SQL common table expressions (WITH/RECURSIVE) are not admitted in this scoped local-source runtime; cte_plan_nodes, catalog scope, recursive policy, execution certificate, and no-fallback evidence are required before execution",
        ));
    }
    Ok(())
}

fn sql_union_branch_statements(
    statement: &str,
    operators: &[SqlUnionOperator],
    union_body_end: usize,
) -> Result<Vec<String>, ShardLoomError> {
    let mut branches = Vec::with_capacity(operators.len() + 1);
    let mut branch_start = 0;
    for operator in operators {
        if operator.index >= union_body_end {
            return Err(unsupported_sql_error(
                "SQL set operator must appear before the global ORDER BY/LIMIT tail",
            ));
        }
        let branch = statement[branch_start..operator.index].trim();
        branches.push(sql_union_bounded_branch_statement(branch)?);
        branch_start = operator.index + operator.len;
    }
    let branch = statement[branch_start..union_body_end].trim();
    branches.push(sql_union_bounded_branch_statement(branch)?);
    Ok(branches)
}

fn sql_union_bounded_branch_statement(branch: &str) -> Result<String, ShardLoomError> {
    if branch.is_empty() {
        return Err(unsupported_sql_error(
            "SQL set-operation branches must not be empty",
        ));
    }
    if !branch
        .get(..6)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("select"))
    {
        return Err(unsupported_sql_error(
            "SQL set-operation branches must be SELECT local-source statements",
        ));
    }
    if top_level_keyword_indexes(branch, "limit")?.is_empty() {
        Ok(format!("{branch} LIMIT {}", usize::MAX))
    } else {
        Ok(branch.to_string())
    }
}

fn parse_sql_local_source_statement(raw: &str) -> Result<ParsedSqlLocalSource, ShardLoomError> {
    let statement = normalize_and_validate_sql_statement(raw)?;
    let (statement, memory_source) = memory_inputs::normalize_statement(statement)?;
    validate_sql_cte_policy_boundary(&statement)?;
    if !statement
        .get(..6)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("select"))
    {
        return Err(unsupported_sql_error(
            "SQL local-source runtime admits SELECT statements only",
        ));
    }
    let indexes = sql_local_source_clause_indexes(&statement)?;

    let select_list = statement[6..indexes.from].trim();
    let (distinct_projection, select_list) = parse_select_distinct_marker(select_list)?;
    let source_end = earliest_clause_index_after(
        indexes.from,
        &[
            indexes.filter,
            indexes.group_by,
            indexes.having,
            indexes.order_by,
            Some(indexes.limit),
        ],
    );
    let source_raw = statement[indexes.from + 4..source_end].trim();
    let predicate_raw = indexes.filter.map(|index| {
        let end = earliest_clause_index_after(
            index,
            &[
                indexes.group_by,
                indexes.having,
                indexes.order_by,
                Some(indexes.limit),
            ],
        );
        statement[index + 5..end].trim()
    });
    let group_by_raw = indexes.group_by.map(|index| {
        let end = earliest_clause_index_after(
            index,
            &[indexes.having, indexes.order_by, Some(indexes.limit)],
        );
        statement[index + "group by".len()..end].trim()
    });
    let having_raw = indexes.having.map(|index| {
        let end = indexes.order_by.unwrap_or(indexes.limit);
        statement[index + "having".len()..end].trim()
    });
    let order_by_raw = indexes
        .order_by
        .map(|index| statement[index + "order by".len()..indexes.limit].trim());
    let limit_raw = statement[indexes.limit + 5..].trim();
    if select_list.is_empty()
        || source_raw.is_empty()
        || predicate_raw.is_some_and(str::is_empty)
        || having_raw.is_some_and(str::is_empty)
        || limit_raw.is_empty()
    {
        return Err(unsupported_sql_error(
            "SQL local-source SELECT list, source, optional predicate, and limit must not be empty",
        ));
    }
    if limit_clause_contains_sql_clause_keyword(limit_raw) {
        return Err(unsupported_sql_error(
            "SQL local-source runtime admits one flat SELECT without subqueries",
        ));
    }

    let ParsedAggregateClauses {
        projection_list,
        having_aggregates,
        group_by,
        grouping_sets,
        order_by,
        having,
    } = parse_aggregate_clauses(
        &statement,
        select_list,
        group_by_raw,
        having_raw,
        order_by_raw,
    )?;
    let source_clause = parse_normalized_source_clause(source_raw, memory_source)?;
    let predicate = predicate_raw.map_or(Ok(ParsedPredicate::All), parse_predicate)?;
    let limit = parse_limit(limit_raw)?;

    Ok(parsed_sql_local_source_from_parts(
        ParsedSqlLocalSourceParts {
            statement,
            projection_list,
            distinct_projection,
            having_aggregates,
            group_by,
            grouping_sets,
            order_by,
            source_clause,
            predicate,
            having,
            limit,
        },
    ))
}

struct ParsedAggregateClauses {
    projection_list: ParsedProjectionList,
    having_aggregates: Vec<ParsedAggregate>,
    group_by: Vec<String>,
    grouping_sets: Option<grouping_syntax::Grouping>,
    order_by: Option<ParsedOrderBy>,
    having: ParsedPredicate,
}

fn parse_aggregate_clauses(
    statement: &str,
    select_list: &str,
    group_by_raw: Option<&str>,
    having_raw: Option<&str>,
    order_by_raw: Option<&str>,
) -> Result<ParsedAggregateClauses, ShardLoomError> {
    let mut grouping_rewrite = grouping_syntax::Rewrite::new(statement);
    let select_list = grouping_rewrite.select(select_list)?;
    let having_raw = having_raw
        .map(|raw| grouping_rewrite.expression(raw))
        .transpose()?;
    let order_by_raw = order_by_raw
        .map(|raw| grouping_rewrite.expression(raw))
        .transpose()?;
    let projection_list = parse_projection_list(&select_list)?;
    let (having_raw, mut having_aggregates) = if let Some(having_raw) = having_raw.as_deref() {
        let (rewritten, aggregates) = rewrite_having_aggregate_predicate(
            having_raw,
            &projection_list.aggregates,
            reserved_having_aggregate_aliases(&projection_list),
        )?;
        (Some(rewritten), aggregates)
    } else {
        (None, Vec::new())
    };
    let (group_by, grouping_sets) =
        grouping_syntax::parse(group_by_raw, &projection_list, grouping_rewrite.indicators)?;
    let order_by = parse_selected_order_by(
        order_by_raw.as_deref(),
        &projection_list,
        &mut having_aggregates,
    )?;
    let having = having_raw
        .as_deref()
        .map_or(Ok(ParsedPredicate::All), parse_predicate)?;
    Ok(ParsedAggregateClauses {
        projection_list,
        having_aggregates,
        group_by,
        grouping_sets,
        order_by,
        having,
    })
}

fn parse_normalized_source_clause(
    raw: &str,
    memory_source: Option<crate::native_memory_input::MemoryInput>,
) -> Result<ParsedSourceClause, ShardLoomError> {
    if let Some(input) = memory_source {
        Ok(ParsedSourceClause {
            source: ParsedRelationSource::Local(ParsedRelationLeaf::memory(input)?),
            source_alias: None,
            join: None,
        })
    } else {
        parse_source_clause(raw)
    }
}

fn parsed_sql_local_source_from_parts(parts: ParsedSqlLocalSourceParts) -> ParsedSqlLocalSource {
    let ParsedSqlLocalSourceParts {
        statement,
        projection_list,
        distinct_projection,
        having_aggregates,
        group_by,
        grouping_sets,
        order_by,
        source_clause,
        predicate,
        having,
        limit,
    } = parts;
    ParsedSqlLocalSource {
        distinct_projection,
        replace_or_add_projection: projection_list.replace_or_add,
        projection_order: projection_list.projection_order,
        projections: projection_list.projections,
        literal_projections: projection_list.literal_projections,
        complex_projections: projection_list.complex_projections,
        cast_projections: projection_list.cast_projections,
        null_coalesce_projections: projection_list.null_coalesce_projections,
        nullif_projections: projection_list.nullif_projections,
        conditional_projections: projection_list.conditional_projections,
        predicate_projections: projection_list.predicate_projections,
        numeric_arithmetic_projections: projection_list.numeric_arithmetic_projections,
        numeric_abs_projections: projection_list.numeric_abs_projections,
        numeric_rounding_projections: projection_list.numeric_rounding_projections,
        generic_expression_projections: projection_list.generic_expression_projections,
        date_arithmetic_projections: projection_list.date_arithmetic_projections,
        timestamp_arithmetic_projections: projection_list.timestamp_arithmetic_projections,
        string_length_projections: projection_list.string_length_projections,
        string_transform_projections: projection_list.string_transform_projections,
        string_function_projections: projection_list.string_function_projections,
        binary_helper_projections: projection_list.binary_helper_projections,
        binary_byte_length_projections: projection_list.binary_byte_length_projections,
        date_extract_projections: projection_list.date_extract_projections,
        timestamp_extract_projections: projection_list.timestamp_extract_projections,
        window_projections: projection_list.window_projections,
        aggregates: projection_list.aggregates,
        having_aggregates,
        group_by,
        grouping_sets,
        order_by,
        source: source_clause.source,
        source_alias: source_clause.source_alias,
        join: source_clause.join,
        predicate,
        having,
        limit,
        offset: 0,
        limit_is_synthetic: false,
        normalized_statement: statement,
    }
}

fn limit_clause_contains_sql_clause_keyword(limit_raw: &str) -> bool {
    contains_keyword_outside_quotes(limit_raw, "where")
        || contains_keyword_outside_quotes(limit_raw, "from")
        || contains_keyword_outside_quotes(limit_raw, "select")
        || contains_keyword_outside_quotes(limit_raw, "having")
        || contains_keyword_outside_quotes(limit_raw, "order by")
}

fn parse_select_distinct_marker(select_list: &str) -> Result<(bool, &str), ShardLoomError> {
    let trimmed = select_list.trim_start();
    let keyword = "distinct";
    if !trimmed
        .get(..keyword.len())
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case(keyword))
    {
        return Ok((false, select_list));
    }
    if trimmed
        .as_bytes()
        .get(keyword.len())
        .is_some_and(|byte| byte.is_ascii_alphanumeric() || *byte == b'_')
    {
        return Ok((false, select_list));
    }
    let rest = trimmed[keyword.len()..].trim_start();
    if rest.is_empty() {
        return Err(unsupported_sql_error(
            "SELECT DISTINCT requires at least one projection expression",
        ));
    }
    Ok((true, rest))
}

fn normalize_and_validate_sql_statement(raw: &str) -> Result<String, ShardLoomError> {
    let statement = normalize_sql_statement(raw)?;
    validate_advanced_scalar_policy_boundaries(&statement)?;
    validate_complex_dtype_policy_boundaries_with_sql_union(&statement, false)?;
    Ok(statement)
}

fn normalize_sql_statement(raw: &str) -> Result<String, ShardLoomError> {
    relation_sources::validate_query_structure(raw)?;
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(unsupported_sql_error("SQL statement must not be empty"));
    }
    if trimmed.matches(';').count() > 1 {
        return Err(unsupported_sql_error(
            "SQL local-source runtime admits a single statement only",
        ));
    }
    let statement = trimmed.strip_suffix(';').unwrap_or(trimmed).trim();
    if statement.is_empty() {
        return Err(unsupported_sql_error("SQL statement must not be empty"));
    }
    Ok(statement.to_string())
}

const TIMEZONE_DATABASE_POLICY_MESSAGE: &str = "timezone database semantics are not admitted; scoped timestamp_micros literals admit UTC Z or fixed numeric offsets only";
const LOCALE_COLLATION_POLICY_MESSAGE: &str = "SQL COLLATE, ILIKE, and locale-aware collation/case-folding semantics are not admitted; UTF-8 comparisons remain case-sensitive codepoint comparisons in this slice";
const ARBITRARY_INTERVAL_ARITHMETIC_POLICY_MESSAGE: &str = "arbitrary ANSI INTERVAL arithmetic is not admitted; use DATE_ADD_DAYS/DATE_SUB_DAYS or TIMESTAMP_ADD_SECONDS/TIMESTAMP_SUB_SECONDS with scoped INTERVAL literals";

fn validate_advanced_scalar_policy_boundaries(statement: &str) -> Result<(), ShardLoomError> {
    if contains_timezone_database_policy_construct(statement)? {
        return Err(unsupported_sql_error(TIMEZONE_DATABASE_POLICY_MESSAGE));
    }
    if contains_locale_collation_policy_construct(statement) {
        return Err(unsupported_sql_error(LOCALE_COLLATION_POLICY_MESSAGE));
    }
    if contains_arbitrary_interval_literal_outside_scoped_temporal_helpers(statement)? {
        return Err(unsupported_sql_error(
            ARBITRARY_INTERVAL_ARITHMETIC_POLICY_MESSAGE,
        ));
    }
    Ok(())
}

fn contains_arbitrary_interval_literal_outside_scoped_temporal_helpers(
    statement: &str,
) -> Result<bool, ShardLoomError> {
    let helper_ranges = scoped_temporal_helper_argument_ranges(statement)?;
    let mut search_start = 0;
    while search_start < statement.len() {
        let Some(relative_index) =
            find_keyword_outside_quotes(&statement[search_start..], "interval")
        else {
            return Ok(false);
        };
        let interval_index = search_start + relative_index;
        let after_keyword = &statement[interval_index + "interval".len()..];
        if after_keyword.trim_start().starts_with('\'')
            && !helper_ranges
                .iter()
                .any(|(start, end)| interval_index >= *start && interval_index < *end)
        {
            return Ok(true);
        }
        search_start = interval_index + "interval".len();
    }
    Ok(false)
}

fn scoped_temporal_helper_argument_ranges(
    statement: &str,
) -> Result<Vec<(usize, usize)>, ShardLoomError> {
    let mut ranges = Vec::new();
    for helper in [
        "date_add_days",
        "date_sub_days",
        "timestamp_add_seconds",
        "timestamp_sub_seconds",
        // INTERVAL is admitted here only as a typed frame bound by the window
        // parser; arbitrary arithmetic inside this clause still fails parsing.
        "over",
    ] {
        let mut search_start = 0;
        while search_start < statement.len() {
            let Some(relative_index) =
                find_keyword_outside_quotes(&statement[search_start..], helper)
            else {
                break;
            };
            let helper_index = search_start + relative_index;
            let after_helper = &statement[helper_index + helper.len()..];
            let leading_whitespace = after_helper.len() - after_helper.trim_start().len();
            let open_index = helper_index + helper.len() + leading_whitespace;
            if statement.as_bytes().get(open_index) != Some(&b'(') {
                search_start = helper_index + helper.len();
                continue;
            }
            if let Some(close_index) = matching_closing_parenthesis(statement, open_index)? {
                ranges.push((open_index + 1, close_index));
                search_start = close_index + 1;
            } else {
                search_start = helper_index + helper.len();
            }
        }
    }
    Ok(ranges)
}

fn contains_timezone_database_policy_construct(statement: &str) -> Result<bool, ShardLoomError> {
    Ok(contains_keyword_outside_quotes(statement, "at time zone")
        || contains_keyword_outside_quotes(statement, "with time zone")
        || contains_type_literal_outside_quotes(statement, "timestamp with local time zone")
        || contains_type_literal_outside_quotes(statement, "timestamptz")
        || contains_type_literal_outside_quotes(statement, "timestamp_tz")
        || contains_cast_target_matching(statement, "cast", target_is_timezone_database_dtype)?
        || contains_cast_target_matching(statement, "try_cast", target_is_timezone_database_dtype)?
        || contains_function_call_outside_quotes(statement, "timezone")
        || contains_function_call_outside_quotes(statement, "convert_timezone"))
}

fn contains_locale_collation_policy_construct(statement: &str) -> bool {
    contains_keyword_outside_quotes(statement, "collate")
        || contains_keyword_outside_quotes(statement, "ilike")
}

fn contains_type_literal_outside_quotes(raw: &str, type_name: &str) -> bool {
    let mut search_start = 0;
    while search_start < raw.len() {
        let Some(relative_index) = find_keyword_outside_quotes(&raw[search_start..], type_name)
        else {
            return false;
        };
        let type_index = search_start + relative_index;
        let after_type = &raw[type_index + type_name.len()..];
        if after_type.trim_start().starts_with('\'') {
            return true;
        }
        search_start = type_index + type_name.len();
    }
    false
}

fn target_is_timezone_database_dtype(target_dtype: &str) -> bool {
    target_dtype_matches_any(
        target_dtype,
        &[
            "timestamptz",
            "timestamp_tz",
            "timestamp with local time zone",
        ],
    )
}

fn contains_cast_target_matching(
    statement: &str,
    function_name: &str,
    target_matches: fn(&str) -> bool,
) -> Result<bool, ShardLoomError> {
    let mut search_start = 0;
    while search_start < statement.len() {
        let Some(relative_index) =
            find_keyword_outside_quotes(&statement[search_start..], function_name)
        else {
            break;
        };
        let name_index = search_start + relative_index;
        let after_name = &statement[name_index + function_name.len()..];
        let leading_whitespace = after_name.len() - after_name.trim_start().len();
        let open_index = name_index + function_name.len() + leading_whitespace;
        if statement.as_bytes().get(open_index) != Some(&b'(') {
            search_start = name_index + function_name.len();
            continue;
        }
        let close_index =
            matching_closing_parenthesis(statement, open_index)?.ok_or_else(|| {
                unsupported_sql_error(
                    "CAST/TRY_CAST expressions must be written as CAST(<column> AS <dtype>)",
                )
            })?;
        let inner = statement[open_index + 1..close_index].trim();
        if let Some(as_index) = find_keyword_outside_quotes_and_parentheses(inner, "as")? {
            let target_dtype = inner[as_index + "as".len()..].trim();
            if target_matches(target_dtype) {
                return Ok(true);
            }
        }
        search_start = close_index + 1;
    }
    Ok(false)
}

fn target_dtype_matches_any(target_dtype: &str, dtypes: &[&str]) -> bool {
    dtypes.iter().any(|dtype| {
        target_dtype
            .get(..dtype.len())
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case(dtype))
            && keyword_boundary(target_dtype, 0, dtype.len())
    })
}

fn validate_complex_dtype_policy_boundaries_with_sql_union(
    statement: &str,
    allow_sql_union: bool,
) -> Result<(), ShardLoomError> {
    if contains_function_call_outside_quotes(statement, "array")
        || contains_function_call_outside_quotes(statement, "list_value")
        || contains_function_call_outside_quotes(statement, "list_extract")
        || contains_cast_target_matching(statement, "cast", target_is_list_dtype)?
        || contains_cast_target_matching(statement, "try_cast", target_is_list_dtype)?
    {
        return Err(unsupported_sql_error(
            "list and array accessors, function constructors, casts, and equality semantics are not admitted by the current complex dtype profile; scoped ARRAY[...] projection literals are admitted through the JSONL result boundary and CSV JSON-text output boundary",
        ));
    }
    if contains_function_call_outside_quotes(statement, "row")
        || contains_cast_target_matching(statement, "cast", target_is_struct_dtype)?
        || contains_cast_target_matching(statement, "try_cast", target_is_struct_dtype)?
    {
        return Err(unsupported_sql_error(
            "row constructors plus struct casts, equality, and access semantics are not admitted by the current complex dtype profile; scoped STRUCT(<source column>, ...) projection construction is admitted through the JSONL result boundary and CSV JSON-text output boundary",
        ));
    }
    if contains_function_call_outside_quotes(statement, "variant")
        || contains_cast_target_matching(statement, "cast", target_is_variant_dtype)?
        || contains_cast_target_matching(statement, "try_cast", target_is_variant_dtype)?
    {
        return Err(unsupported_sql_error(
            "Variant constructors and casts are not admitted by the current complex dtype profile; native Variant field functions have separate binder admission",
        ));
    }
    if contains_cast_target_matching(statement, "cast", target_is_union_dtype)?
        || contains_cast_target_matching(statement, "try_cast", target_is_union_dtype)?
    {
        return Err(unsupported_sql_error(
            "union dtype casts are not admitted by the current complex dtype profile",
        ));
    }
    if !allow_sql_union && !top_level_sql_union_operators(statement)?.is_empty() {
        return Err(unsupported_sql_error(
            "SQL set operations are not admitted in single SELECT branch parsing; use the scoped top-level set-operation runtime path",
        ));
    }
    Ok(())
}

fn target_is_list_dtype(target_dtype: &str) -> bool {
    target_dtype_matches_any(target_dtype, &["list", "array"])
}

fn target_is_struct_dtype(target_dtype: &str) -> bool {
    target_dtype_matches_any(target_dtype, &["struct", "row"])
}

fn target_is_variant_dtype(target_dtype: &str) -> bool {
    target_dtype_matches_any(target_dtype, &["variant"])
}

fn target_is_union_dtype(target_dtype: &str) -> bool {
    target_dtype_matches_any(target_dtype, &["union"])
}

fn contains_function_call_outside_quotes(raw: &str, function_name: &str) -> bool {
    let mut search_start = 0;
    while search_start < raw.len() {
        let Some(relative_index) = find_keyword_outside_quotes(&raw[search_start..], function_name)
        else {
            return false;
        };
        let name_index = search_start + relative_index;
        let after_name = &raw[name_index + function_name.len()..];
        let leading_whitespace = after_name.len() - after_name.trim_start().len();
        let open_index = name_index + function_name.len() + leading_whitespace;
        if raw.as_bytes().get(open_index) == Some(&b'(') {
            return true;
        }
        search_start = name_index + function_name.len();
    }
    false
}

fn contains_array_literal_outside_quotes(raw: &str) -> bool {
    let mut search_start = 0;
    while search_start < raw.len() {
        let Some(relative_index) = find_keyword_outside_quotes(&raw[search_start..], "array")
        else {
            return false;
        };
        let name_index = search_start + relative_index;
        let after_name = &raw[name_index + "array".len()..];
        let leading_whitespace = after_name.len() - after_name.trim_start().len();
        let bracket_index = name_index + "array".len() + leading_whitespace;
        if raw.as_bytes().get(bracket_index) == Some(&b'[') {
            return true;
        }
        search_start = name_index + "array".len();
    }
    false
}

fn rewrite_having_aggregate_predicate(
    raw: &str,
    selected: &[ParsedAggregate],
    mut reserved_aliases: BTreeSet<String>,
) -> Result<(String, Vec<ParsedAggregate>), ShardLoomError> {
    let mut rewritten = String::with_capacity(raw.len());
    let mut aggregates: Vec<ParsedAggregate> = Vec::new();
    let mut cursor = 0;
    let mut index = 0;
    let bytes = raw.as_bytes();
    let mut in_string = false;
    while index < raw.len() {
        if bytes[index] == b'\'' {
            in_string = !in_string;
            index += 1;
            continue;
        }
        // Subquery aggregates belong to their own source and grouping scope.
        // Leave their complete text for the ordinary recursive SQL binder.
        if !in_string
            && bytes[index] == b'('
            && starts_with_keyword(raw[index + 1..].trim_start(), "select")
        {
            index = matching_closing_parenthesis(raw, index)?.ok_or_else(|| {
                unsupported_sql_error("HAVING subquery requires closing parentheses")
            })? + 1;
            continue;
        }
        if !in_string && let Some(function_name) = aggregate_function_prefix_at(raw, index) {
            let open_index = index + function_name.len();
            let close_index = aggregate_expression_end(raw, open_index)?;
            let expression = &raw[index..=close_index];
            let mut aggregate = parse_aggregate_projection(expression)?.ok_or_else(|| {
                unsupported_sql_error("HAVING requires an admitted native aggregate function")
            })?;
            let existing = selected
                .iter()
                .chain(&aggregates)
                .find(|candidate| candidate.same_value(&aggregate));
            let alias = if let Some(existing) = existing {
                existing.output_name()
            } else {
                let alias_base = sanitize_having_aggregate_alias(&aggregate.output_name());
                let mut suffix = aggregates.len() + 1;
                let alias = loop {
                    let candidate = format!("__having_{alias_base}_{suffix}");
                    if reserved_aliases.insert(candidate.clone()) {
                        break candidate;
                    }
                    suffix += 1;
                };
                aggregate.alias = Some(alias.clone());
                aggregates.push(aggregate);
                alias
            };
            rewritten.push_str(&raw[cursor..index]);
            rewritten.push_str(&alias);
            index = close_index + 1;
            cursor = index;
            continue;
        }
        index += 1;
    }

    rewritten.push_str(&raw[cursor..]);
    Ok((rewritten, aggregates))
}

fn reserved_having_aggregate_aliases(projection_list: &ParsedProjectionList) -> BTreeSet<String> {
    let mut reserved = projection_list
        .aggregates
        .iter()
        .map(ParsedAggregate::output_name)
        .collect::<BTreeSet<_>>();
    for output in &projection_list.projection_order {
        match output {
            ParsedProjectionOutput::Raw(column) if column == "*" => {}
            ParsedProjectionOutput::Raw(column)
            | ParsedProjectionOutput::Aggregate(column)
            | ParsedProjectionOutput::Literal(column)
            | ParsedProjectionOutput::Complex(column)
            | ParsedProjectionOutput::Cast(column)
            | ParsedProjectionOutput::NullCoalesce(column)
            | ParsedProjectionOutput::NullIf(column)
            | ParsedProjectionOutput::Conditional(column)
            | ParsedProjectionOutput::Predicate(column)
            | ParsedProjectionOutput::NumericArithmetic(column)
            | ParsedProjectionOutput::NumericAbs(column)
            | ParsedProjectionOutput::NumericRounding(column)
            | ParsedProjectionOutput::GenericExpression(column)
            | ParsedProjectionOutput::DateArithmetic(column)
            | ParsedProjectionOutput::TimestampArithmetic(column)
            | ParsedProjectionOutput::StringLength(column)
            | ParsedProjectionOutput::StringTransform(column)
            | ParsedProjectionOutput::StringFunction(column)
            | ParsedProjectionOutput::BinaryHelper(column)
            | ParsedProjectionOutput::BinaryByteLength(column)
            | ParsedProjectionOutput::DateExtract(column)
            | ParsedProjectionOutput::TimestampExtract(column)
            | ParsedProjectionOutput::Window(column) => {
                reserved.insert(column.clone());
            }
        }
    }
    reserved
}

fn aggregate_function_prefix_at(raw: &str, index: usize) -> Option<&str> {
    if !sql_identifier_boundary_before(raw, index) {
        return None;
    }
    let rest = raw.get(index..)?;
    let length = rest
        .bytes()
        .take_while(|byte| byte.is_ascii_alphanumeric() || *byte == b'_')
        .count();
    AggregateFunction::parse(rest.get(..length)?)?;
    let trailing = rest.get(length..)?;
    let end = length + trailing.len() - trailing.trim_start().len();
    (rest.as_bytes().get(end) == Some(&b'(')).then_some(&rest[..end])
}

fn sql_identifier_boundary_before(raw: &str, index: usize) -> bool {
    if index == 0 {
        return true;
    }
    raw.as_bytes()
        .get(index - 1)
        .is_none_or(|byte| !byte.is_ascii_alphanumeric() && *byte != b'_')
}

fn sanitize_having_aggregate_alias(value: &str) -> String {
    value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect::<String>()
        .trim_matches('_')
        .to_string()
}

#[allow(clippy::too_many_lines)]
fn parse_projection_list(raw: &str) -> Result<ParsedProjectionList, ShardLoomError> {
    const KEYWORD: &str = "replace or add";
    if let Some(modifier) = raw.trim().strip_prefix('*') {
        let modifier = modifier.trim_start();
        if modifier
            .get(..KEYWORD.len())
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case(KEYWORD))
        {
            let entries = modifier[KEYWORD.len()..].trim();
            if !entries.starts_with('(')
                || matching_closing_parenthesis(entries, 0)? != Some(entries.len() - 1)
            {
                return Err(unsupported_sql_error(
                    "REPLACE OR ADD requires one parenthesized expression list",
                ));
            }
            let mut parsed = parse_projection_list(&entries[1..entries.len() - 1])?;
            let mut aliases = BTreeSet::new();
            if parsed.replace_or_add
                || !parsed.aggregates.is_empty()
                || !parsed.window_projections.is_empty()
                || parsed.projection_order.iter().any(|output| {
                    output
                        .computed_alias()
                        .is_none_or(|alias| !aliases.insert(alias.to_owned()))
                })
            {
                return Err(unsupported_sql_error(
                    "REPLACE OR ADD requires scalar expressions with distinct explicit aliases",
                ));
            }
            parsed.replace_or_add = true;
            parsed
                .projection_order
                .insert(0, ParsedProjectionOutput::Raw("*".into()));
            parsed.projections.insert(0, "*".into());
            return Ok(parsed);
        }
    }
    let entries = split_sql_csv(raw)?;
    if entries.is_empty() {
        return Err(unsupported_sql_error("SELECT list must not be empty"));
    }
    let mut projection_order = Vec::with_capacity(entries.len());
    let mut projections = Vec::with_capacity(entries.len());
    let mut literal_projections = Vec::new();
    let mut complex_projections = Vec::new();
    let mut cast_projections = Vec::new();
    let mut null_coalesce_projections = Vec::new();
    let mut nullif_projections = Vec::new();
    let mut conditional_projections = Vec::new();
    let mut predicate_projections = Vec::new();
    let mut numeric_arithmetic_projections = Vec::new();
    let mut numeric_abs_projections = Vec::new();
    let mut numeric_rounding_projections = Vec::new();
    let mut generic_expression_projections = Vec::new();
    let mut date_arithmetic_projections = Vec::new();
    let mut timestamp_arithmetic_projections = Vec::new();
    let mut string_length_projections = Vec::new();
    let mut string_transform_projections = Vec::new();
    let mut string_function_projections = Vec::new();
    let mut binary_helper_projections = Vec::new();
    let mut binary_byte_length_projections = Vec::new();
    let mut date_extract_projections = Vec::new();
    let mut timestamp_extract_projections = Vec::new();
    let mut window_projections = Vec::new();
    let mut aggregates = Vec::new();
    for (ordinal, projection) in entries.into_iter().enumerate() {
        let projection = projection.trim();
        if projection == "*" {
            projection_order.push(ParsedProjectionOutput::Raw("*".to_string()));
            projections.push("*".to_string());
        } else if let Some(window_projection) = parse_window_projection(projection)? {
            projection_order.push(ParsedProjectionOutput::Window(
                window_projection.alias.clone(),
            ));
            window_projections.push(window_projection);
        } else if let Some(aggregate) = parse_aggregate_projection(projection)? {
            projection_order.push(ParsedProjectionOutput::Aggregate(aggregate.output_name()));
            aggregates.push(aggregate);
        } else if let Some(complex_projection) = parse_complex_projection(projection)? {
            projection_order.push(ParsedProjectionOutput::Complex(
                complex_projection.alias.clone(),
            ));
            complex_projections.push(complex_projection);
        } else if let Some(conditional_projection) = parse_conditional_projection(projection)? {
            projection_order.push(ParsedProjectionOutput::Conditional(
                conditional_projection.alias.clone(),
            ));
            conditional_projections.push(conditional_projection);
        } else if let Some(predicate_projection) = parse_predicate_projection(projection)? {
            projection_order.push(ParsedProjectionOutput::Predicate(
                predicate_projection.alias.clone(),
            ));
            predicate_projections.push(predicate_projection);
        } else if let Some(generic_projection) = parse_generic_expression_projection(projection)? {
            projection_order.push(ParsedProjectionOutput::GenericExpression(
                generic_projection.alias.clone(),
            ));
            generic_expression_projections.push(generic_projection);
        } else if let Some(arithmetic_projection) = parse_numeric_arithmetic_projection(projection)?
        {
            projection_order.push(ParsedProjectionOutput::NumericArithmetic(
                arithmetic_projection.alias.clone(),
            ));
            numeric_arithmetic_projections.push(arithmetic_projection);
        } else if let Some(abs_projection) = parse_numeric_abs_projection(projection)? {
            projection_order.push(ParsedProjectionOutput::NumericAbs(
                abs_projection.alias.clone(),
            ));
            numeric_abs_projections.push(abs_projection);
        } else if let Some(rounding_projection) = parse_numeric_rounding_projection(projection)? {
            projection_order.push(ParsedProjectionOutput::NumericRounding(
                rounding_projection.alias.clone(),
            ));
            numeric_rounding_projections.push(rounding_projection);
        } else if let Some(cast_projection) = parse_cast_projection(projection)? {
            projection_order.push(ParsedProjectionOutput::Cast(cast_projection.alias.clone()));
            cast_projections.push(cast_projection);
        } else if let Some(null_coalesce_projection) = parse_null_coalesce_projection(projection)? {
            projection_order.push(ParsedProjectionOutput::NullCoalesce(
                null_coalesce_projection.alias.clone(),
            ));
            null_coalesce_projections.push(null_coalesce_projection);
        } else if let Some(nullif_projection) = parse_nullif_projection(projection)? {
            projection_order.push(ParsedProjectionOutput::NullIf(
                nullif_projection.alias.clone(),
            ));
            nullif_projections.push(nullif_projection);
        } else if let Some(date_arithmetic_projection) =
            parse_date_arithmetic_projection(projection)?
        {
            projection_order.push(ParsedProjectionOutput::DateArithmetic(
                date_arithmetic_projection.alias.clone(),
            ));
            date_arithmetic_projections.push(date_arithmetic_projection);
        } else if let Some(timestamp_arithmetic_projection) =
            parse_timestamp_arithmetic_projection(projection)?
        {
            projection_order.push(ParsedProjectionOutput::TimestampArithmetic(
                timestamp_arithmetic_projection.alias.clone(),
            ));
            timestamp_arithmetic_projections.push(timestamp_arithmetic_projection);
        } else if let Some(length_projection) = parse_string_length_projection(projection)? {
            projection_order.push(ParsedProjectionOutput::StringLength(
                length_projection.alias.clone(),
            ));
            string_length_projections.push(length_projection);
        } else if let Some(transform_projection) = parse_string_transform_projection(projection)? {
            projection_order.push(ParsedProjectionOutput::StringTransform(
                transform_projection.alias.clone(),
            ));
            string_transform_projections.push(transform_projection);
        } else if let Some(function_projection) = parse_string_function_projection(projection)? {
            projection_order.push(ParsedProjectionOutput::StringFunction(
                function_projection.alias.clone(),
            ));
            string_function_projections.push(function_projection);
        } else if let Some(binary_projection) = parse_binary_helper_projection(projection)? {
            projection_order.push(ParsedProjectionOutput::BinaryHelper(
                binary_projection.alias.clone(),
            ));
            binary_helper_projections.push(binary_projection);
        } else if let Some(byte_length_projection) =
            parse_binary_byte_length_projection(projection)?
        {
            projection_order.push(ParsedProjectionOutput::BinaryByteLength(
                byte_length_projection.alias.clone(),
            ));
            binary_byte_length_projections.push(byte_length_projection);
        } else if let Some(date_projection) = parse_date_extract_projection(projection)? {
            projection_order.push(ParsedProjectionOutput::DateExtract(
                date_projection.alias.clone(),
            ));
            date_extract_projections.push(date_projection);
        } else if let Some(timestamp_projection) = parse_timestamp_extract_projection(projection)? {
            projection_order.push(ParsedProjectionOutput::TimestampExtract(
                timestamp_projection.alias.clone(),
            ));
            timestamp_extract_projections.push(timestamp_projection);
        } else if let Some(literal_projection) = parse_literal_projection(projection)? {
            projection_order.push(ParsedProjectionOutput::Literal(
                literal_projection.alias.clone(),
            ));
            literal_projections.push(literal_projection);
        } else if validate_sql_column_ref(projection).is_ok() {
            projection_order.push(ParsedProjectionOutput::Raw(projection.to_string()));
            projections.push(projection.to_string());
        } else {
            // An explicit CTE column-name list renames these outputs after
            // binding. It must not require redundant aliases on its inputs.
            // Preserve established names above; other scalar expressions use
            // their one-based SELECT position when no alias was supplied.
            let alias = format!("expression_{}", ordinal + 1);
            let expression =
                parse_numeric_scalar_expression(projection, &format!("project.implicit.{alias}"))?;
            projection_order.push(ParsedProjectionOutput::GenericExpression(alias.clone()));
            generic_expression_projections.push(ParsedGenericExpressionProjection {
                alias,
                source_columns: expression_source_columns(&expression),
                operator_families: expression_operator_families(&expression),
                binary_operator_count: expression_binary_operator_count(&expression),
                expression,
            });
        }
    }
    let has_star_projection = projection_order
        .iter()
        .any(|output| matches!(output, ParsedProjectionOutput::Raw(column) if column == "*"));
    if has_star_projection {
        let star_count = projection_order
            .iter()
            .filter(|output| matches!(output, ParsedProjectionOutput::Raw(column) if column == "*"))
            .count();
        if star_count > 1 {
            return Err(unsupported_sql_error(
                "SELECT * may appear only once in this scoped smoke",
            ));
        }
        if !aggregates.is_empty() {
            return Err(unsupported_sql_error(
                "SELECT * cannot be mixed with aggregate functions in this scoped smoke",
            ));
        }
        if projection_order
            .iter()
            .any(|output| matches!(output, ParsedProjectionOutput::Raw(column) if column != "*"))
        {
            return Err(unsupported_sql_error(
                "SELECT * can be mixed only with computed, literal, or window projections in this scoped smoke",
            ));
        }
    }
    Ok(ParsedProjectionList {
        replace_or_add: false,
        projection_order,
        projections,
        literal_projections,
        complex_projections,
        cast_projections,
        null_coalesce_projections,
        nullif_projections,
        conditional_projections,
        predicate_projections,
        numeric_arithmetic_projections,
        numeric_abs_projections,
        numeric_rounding_projections,
        generic_expression_projections,
        date_arithmetic_projections,
        timestamp_arithmetic_projections,
        string_length_projections,
        string_transform_projections,
        string_function_projections,
        binary_helper_projections,
        binary_byte_length_projections,
        date_extract_projections,
        timestamp_extract_projections,
        window_projections,
        aggregates,
    })
}

fn parse_literal_projection(raw: &str) -> Result<Option<ParsedLiteralProjection>, ShardLoomError> {
    if let Ok(value) = raw.parse::<i64>() {
        return Ok(Some(ParsedLiteralProjection {
            alias: if value < 0 {
                format!("literal_neg{}", value.unsigned_abs())
            } else {
                format!("literal_{value}")
            },
            value: ScalarValue::Int64(value),
        }));
    }
    if ["null", "true", "false"]
        .iter()
        .any(|literal| raw.eq_ignore_ascii_case(literal))
    {
        return Ok(Some(ParsedLiteralProjection {
            alias: raw.to_ascii_lowercase(),
            value: parse_top_level_projection_literal_value(raw)?,
        }));
    }
    let Some(as_index) = find_keyword_outside_quotes(raw, "as") else {
        return Ok(None);
    };
    let literal_raw = raw[..as_index].trim();
    let alias = raw[as_index + "as".len()..].trim();
    if literal_raw.is_empty() || alias.is_empty() {
        return Err(unsupported_sql_error(
            "literal projections must be written as <literal> AS <column>",
        ));
    }
    validate_sql_identifier(alias)?;
    let value = parse_top_level_projection_literal_value(literal_raw)?;
    Ok(Some(ParsedLiteralProjection {
        alias: alias.to_string(),
        value,
    }))
}

fn parse_complex_projection(raw: &str) -> Result<Option<ParsedComplexProjection>, ShardLoomError> {
    let Some(as_index) = find_keyword_outside_quotes_and_parentheses(raw, "as")? else {
        return Ok(None);
    };
    let expression_raw = raw[..as_index].trim();
    let alias = raw[as_index + "as".len()..].trim();
    if expression_raw.is_empty() || alias.is_empty() {
        return Err(unsupported_sql_error(
            "complex projections must be written as ARRAY[...] AS <column> or STRUCT(...) AS <column>",
        ));
    }
    validate_sql_identifier(alias)?;
    if let Some(values) = parse_array_literal_projection_values(expression_raw)? {
        return Ok(Some(ParsedComplexProjection {
            alias: alias.to_string(),
            kind: ParsedComplexProjectionKind::ArrayLiteral(values),
        }));
    }
    if let Some(columns) = parse_struct_projection_columns(expression_raw)? {
        return Ok(Some(ParsedComplexProjection {
            alias: alias.to_string(),
            kind: ParsedComplexProjectionKind::StructColumns(columns),
        }));
    }
    Ok(None)
}

fn parse_array_literal_projection_values(
    raw: &str,
) -> Result<Option<Vec<ScalarValue>>, ShardLoomError> {
    let trimmed = raw.trim();
    if !trimmed
        .get(.."array".len())
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("array"))
        || !keyword_boundary(trimmed, 0, "array".len())
    {
        return Ok(None);
    }
    if trimmed.len() > 65_536 {
        return Err(unsupported_sql_error(
            "scalar expression exceeds 65536 bytes",
        ));
    }
    let rest = trimmed["array".len()..].trim_start();
    if !rest.starts_with('[') {
        return Err(unsupported_sql_error(
            "ARRAY projections must use ARRAY[<scalar literal>, ...] syntax in this scoped runtime slice",
        ));
    }
    let close_index = matching_closing_square_bracket(rest, 0)?.ok_or_else(|| {
        unsupported_sql_error("ARRAY projection literal square brackets are not balanced")
    })?;
    if !rest[close_index + 1..].trim().is_empty() {
        return Err(unsupported_sql_error(
            "ARRAY projection literals must be a single ARRAY[...] expression",
        ));
    }
    let inner = rest[1..close_index].trim();
    if inner.is_empty() {
        return Ok(Some(Vec::new()));
    }
    let values = split_sql_csv(inner)?;
    // The ARRAY node itself consumes one node. A containing scalar declaration
    // accounts for these children against its complete recursive budget too.
    if values.len() > 4095 {
        return Err(unsupported_sql_error(
            "scalar expressions exceed 24 levels or 4096 nodes",
        ));
    }
    values
        .into_iter()
        .map(|value| {
            parse_projection_literal_value(&value).map_err(|_| {
                unsupported_sql_error(
                    "ARRAY projection elements admit scalar SQL literals only in this scoped runtime slice",
                )
            })
        })
        .collect::<Result<Vec<_>, _>>()
        .map(Some)
}

fn parse_struct_projection_columns(raw: &str) -> Result<Option<Vec<String>>, ShardLoomError> {
    let trimmed = raw.trim();
    if !trimmed
        .get(.."struct".len())
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("struct"))
        || !keyword_boundary(trimmed, 0, "struct".len())
    {
        return Ok(None);
    }
    let rest = trimmed["struct".len()..].trim_start();
    if !rest.starts_with('(') {
        return Err(unsupported_sql_error(
            "STRUCT projections must use STRUCT(<source column>, ...) syntax in this scoped runtime slice",
        ));
    }
    let close_index = matching_closing_parenthesis(rest, 0)?
        .ok_or_else(|| unsupported_sql_error("STRUCT projection parentheses are not balanced"))?;
    if !rest[close_index + 1..].trim().is_empty() {
        return Err(unsupported_sql_error(
            "STRUCT projections must be a single STRUCT(...) expression",
        ));
    }
    let inner = rest[1..close_index].trim();
    if inner.is_empty() {
        return Err(unsupported_sql_error(
            "STRUCT projections require at least one source column",
        ));
    }
    let mut seen = BTreeSet::new();
    let mut columns = Vec::new();
    for column in split_sql_csv(inner)? {
        validate_sql_column_ref(&column).map_err(|_| {
            unsupported_sql_error(
                "STRUCT projections admit source columns only; field access, expressions, literals, and row constructors remain blocked",
            )
        })?;
        if !seen.insert(column.clone()) {
            return Err(unsupported_sql_error(
                "STRUCT projection source columns must be unique in this scoped runtime slice",
            ));
        }
        columns.push(column);
    }
    Ok(Some(columns))
}

fn parse_window_projection(raw: &str) -> Result<Option<ParsedWindowProjection>, ShardLoomError> {
    let Some(as_index) = find_keyword_outside_quotes_and_parentheses(raw, "as")? else {
        if find_keyword_outside_quotes_and_parentheses(raw, "over")?.is_some() {
            return Err(unsupported_sql_error(
                "window projections require <function> OVER (...) AS <column>",
            ));
        }
        return Ok(None);
    };
    let expression_raw = raw[..as_index].trim();
    let alias = raw[as_index + "as".len()..].trim();
    let Some(over_index) = find_keyword_outside_quotes_and_parentheses(expression_raw, "over")?
    else {
        return Ok(None);
    };
    let function_raw = expression_raw[..over_index].trim();
    let spec_raw = expression_raw[over_index + "over".len()..].trim();
    let Some(function) = parse_window_function(function_raw)? else {
        return Err(unsupported_sql_error(
            "window function has no admitted native declaration",
        ));
    };
    if alias.is_empty() {
        return Err(unsupported_sql_error(
            "window projections require an output alias",
        ));
    }
    validate_sql_identifier(alias)?;
    let (partition_by, order_by, frame) = window_frames::parse_spec(spec_raw)?;
    if order_by.keys.is_empty()
        && !matches!(
            function,
            WindowFunction::Aggregate(_)
                | WindowFunction::FirstValue(_)
                | WindowFunction::LastValue(_)
                | WindowFunction::NthValue { .. }
                | WindowFunction::Fill { .. }
        )
    {
        return Err(unsupported_sql_error(
            "window projections require ORDER BY for deterministic ranking or offset semantics",
        ));
    }
    Ok(Some(ParsedWindowProjection {
        alias: alias.to_string(),
        function,
        partition_by,
        order_by,
        frame,
    }))
}

fn parse_window_function(raw: &str) -> Result<Option<WindowFunction>, ShardLoomError> {
    let compact = raw
        .chars()
        .filter(|ch| !ch.is_whitespace())
        .collect::<String>()
        .to_ascii_lowercase();
    match compact.as_str() {
        "row_number()" => return Ok(Some(WindowFunction::RowNumber)),
        "rank()" => return Ok(Some(WindowFunction::Rank)),
        "dense_rank()" => return Ok(Some(WindowFunction::DenseRank)),
        "percent_rank()" => return Ok(Some(WindowFunction::PercentRank)),
        "cume_dist()" => return Ok(Some(WindowFunction::CumeDist)),
        _ => {}
    }

    if let Some(args) = parse_window_function_args(raw, "lag")? {
        return parse_offset_window_function("LAG", args).map(Some);
    }
    if let Some(args) = parse_window_function_args(raw, "lead")? {
        return parse_offset_window_function("LEAD", args).map(Some);
    }
    if let Some(args) = parse_window_function_args(raw, "ntile")? {
        return parse_ntile_window_function(args).map(Some);
    }
    if let Some(aggregate) = parse_aggregate_projection(raw)? {
        return Ok(Some(WindowFunction::Aggregate(aggregate)));
    }
    for (name, method) in [
        ("forward_fill", VortexRelationalFillMethod::Forward),
        ("backward_fill", VortexRelationalFillMethod::Backward),
        ("interpolate_linear", VortexRelationalFillMethod::Linear),
    ] {
        if let Some(arguments) = parse_window_function_args(raw, name)? {
            let args = split_sql_csv(arguments)?;
            if args.len() != 1 {
                return Err(unsupported_sql_error(&format!(
                    "{name} window function requires exactly one scalar expression",
                )));
            }
            return Ok(Some(WindowFunction::Fill {
                expression: scalar_expression::parse(&args[0], "window.argument")?,
                method,
            }));
        }
    }
    window_frames::parse_value_function(raw)
}

fn parse_window_function_args<'a>(
    raw: &'a str,
    function_name: &str,
) -> Result<Option<&'a str>, ShardLoomError> {
    let trimmed = raw.trim();
    if trimmed.len() < function_name.len() {
        return Ok(None);
    }
    let Some(function_prefix) = trimmed.get(..function_name.len()) else {
        return Ok(None);
    };
    if !function_prefix.eq_ignore_ascii_case(function_name) {
        return Ok(None);
    }
    let after_name = trimmed
        .get(function_name.len()..)
        .expect("function prefix slice succeeded");
    let after_name_trimmed = after_name.trim_start();
    if !after_name_trimmed.starts_with('(') {
        return Ok(None);
    }
    let open_index = trimmed.len() - after_name_trimmed.len();
    let close_index = matching_closing_parenthesis(trimmed, open_index)?.ok_or_else(|| {
        unsupported_sql_error(&format!(
            "{function_name} window function parentheses must be balanced"
        ))
    })?;
    if !trimmed[close_index + 1..].trim().is_empty() {
        return Err(unsupported_sql_error(&format!(
            "{function_name} window function must be a single function call"
        )));
    }
    Ok(Some(trimmed[open_index + 1..close_index].trim()))
}

fn parse_offset_window_function(
    function_name: &str,
    args_raw: &str,
) -> Result<WindowFunction, ShardLoomError> {
    let args = split_sql_csv(args_raw)?;
    if !(1..=2).contains(&args.len()) {
        return Err(unsupported_sql_error(&format!(
            "{function_name} window function requires a value column and optional positive integer offset"
        )));
    }
    let column = args[0].trim();
    validate_sql_column_ref(column)?;
    let offset = if let Some(offset_raw) = args.get(1) {
        parse_window_offset(function_name, offset_raw)?
    } else {
        1
    };
    match function_name {
        "LAG" => Ok(WindowFunction::Lag {
            column: column.to_string(),
            offset,
        }),
        "LEAD" => Ok(WindowFunction::Lead {
            column: column.to_string(),
            offset,
        }),
        _ => Err(ShardLoomError::InvalidOperation(format!(
            "unknown offset window function {function_name}"
        ))),
    }
}

fn parse_ntile_window_function(args_raw: &str) -> Result<WindowFunction, ShardLoomError> {
    let args = split_sql_csv(args_raw)?;
    if args.len() != 1 {
        return Err(unsupported_sql_error(
            "NTILE window function requires exactly one positive integer bucket count",
        ));
    }
    let bucket_count = parse_positive_window_integer(
        "NTILE",
        "bucket count",
        args.first().expect("length checked"),
    )?;
    Ok(WindowFunction::Ntile { bucket_count })
}

fn parse_window_offset(function_name: &str, raw: &str) -> Result<usize, ShardLoomError> {
    parse_positive_window_integer(function_name, "offset", raw)
}

fn parse_positive_window_integer(
    function_name: &str,
    value_label: &str,
    raw: &str,
) -> Result<usize, ShardLoomError> {
    let trimmed = raw.trim();
    if trimmed.is_empty() || !trimmed.chars().all(|ch| ch.is_ascii_digit()) {
        return Err(unsupported_sql_error(&format!(
            "{function_name} window {value_label} must be a positive integer literal"
        )));
    }
    let offset = trimmed.parse::<usize>().map_err(|_| {
        unsupported_sql_error(&format!(
            "{function_name} window {value_label} must be a positive integer literal"
        ))
    })?;
    if offset == 0 {
        return Err(unsupported_sql_error(&format!(
            "{function_name} window {value_label} must be at least 1"
        )));
    }
    Ok(offset)
}

fn parse_window_partition_by(raw: &str) -> Result<Vec<String>, ShardLoomError> {
    if raw.trim().is_empty() {
        return Ok(Vec::new());
    }
    let partition_index = find_keyword_outside_quotes_and_parentheses(raw, "partition by")?
        .ok_or_else(|| {
            unsupported_sql_error(
                "window specifications admit PARTITION BY followed by optional ORDER BY and frame bounds",
            )
        })?;
    if partition_index != 0 {
        return Err(unsupported_sql_error(
            "window PARTITION BY must appear before ORDER BY",
        ));
    }
    let partition_raw = raw[partition_index + "partition by".len()..].trim();
    if partition_raw.is_empty() {
        return Err(unsupported_sql_error(
            "window PARTITION BY requires at least one column",
        ));
    }
    let columns = split_sql_csv(partition_raw)?;
    let mut parsed = Vec::with_capacity(columns.len());
    for column in columns {
        validate_sql_column_ref(&column)?;
        if parsed.iter().any(|existing| existing == &column) {
            return Err(unsupported_sql_error(
                "window PARTITION BY columns must be unique",
            ));
        }
        parsed.push(column);
    }
    Ok(parsed)
}

fn parse_numeric_arithmetic_projection(
    raw: &str,
) -> Result<Option<ParsedNumericArithmeticProjection>, ShardLoomError> {
    let Some(as_index) = find_keyword_outside_quotes(raw, "as") else {
        return Ok(parse_unaliased_additive_projection(raw));
    };
    let expression_raw = raw[..as_index].trim();
    let alias = raw[as_index + "as".len()..].trim();
    let tokens = split_whitespace_outside_quotes(expression_raw)?;
    let Some(op_index) = tokens
        .iter()
        .position(|token| parse_numeric_arithmetic_op(token).is_some())
    else {
        return Ok(None);
    };
    if expression_raw.is_empty() || alias.is_empty() || tokens.len() != 3 || op_index != 1 {
        return Err(unsupported_sql_error(
            "numeric arithmetic projections must be written as <column> (+|-|*|/) <numeric-literal> AS <column>",
        ));
    }
    validate_sql_column_ref(&tokens[0])?;
    validate_sql_identifier(alias)?;
    let op = parse_numeric_arithmetic_op(&tokens[1]).expect("arithmetic op was detected");
    let rhs = parse_numeric_arithmetic_literal(&tokens[2])?;
    Ok(Some(ParsedNumericArithmeticProjection {
        alias: alias.to_string(),
        column: tokens[0].clone(),
        op,
        rhs,
    }))
}

/// Preserve the established SQL names for unaliased column +/- integer keys
/// while putting their evaluation through the ordinary typed expression IR.
fn parse_unaliased_additive_projection(raw: &str) -> Option<ParsedNumericArithmeticProjection> {
    let expression = scalar_expression::parse(raw, "project.additive").ok()?;
    let ExpressionKind::Binary { left, op, right } = &expression.kind else {
        return None;
    };
    let ExpressionKind::Column(column) = &left.kind else {
        return None;
    };
    let ExpressionKind::Literal(ScalarValue::Int64(value)) = right.kind else {
        return None;
    };
    let (op, offset) = match op {
        BinaryOp::Add => (NumericArithmeticOp::Add, value),
        BinaryOp::Subtract => (NumericArithmeticOp::Subtract, value.checked_neg()?),
        _ => return None,
    };
    let name = column
        .as_str()
        .strip_prefix("f.")
        .unwrap_or(column.as_str());
    let alias = format!(
        "{}_{}{}",
        name.replace('.', "_"),
        if offset < 0 { "minus" } else { "plus" },
        offset.unsigned_abs()
    );
    Some(ParsedNumericArithmeticProjection {
        alias,
        column: column.as_str().to_owned(),
        op,
        rhs: ScalarValue::Int64(value),
    })
}

fn parse_numeric_abs_projection(
    raw: &str,
) -> Result<Option<ParsedNumericAbsProjection>, ShardLoomError> {
    let Some(as_index) = find_keyword_outside_quotes(raw, "as") else {
        return Ok(None);
    };
    let expression_raw = raw[..as_index].trim();
    if !expression_raw.to_ascii_uppercase().starts_with("ABS(") {
        return Ok(None);
    }
    let alias = raw[as_index + "as".len()..].trim();
    let Some(open_index) = expression_raw.find('(') else {
        return Err(unsupported_sql_error(
            "numeric abs projections must be written as ABS(<column>) AS <column>",
        ));
    };
    let Some(close_index) = expression_raw.rfind(')') else {
        return Err(unsupported_sql_error(
            "numeric abs projections must be written as ABS(<column>) AS <column>",
        ));
    };
    if close_index + 1 != expression_raw.len() {
        return Err(unsupported_sql_error(
            "numeric abs projections must be written as ABS(<column>) AS <column>",
        ));
    }
    let column = expression_raw[open_index + 1..close_index].trim();
    if column.is_empty() || alias.is_empty() {
        return Err(unsupported_sql_error(
            "numeric abs projections must be written as ABS(<column>) AS <column>",
        ));
    }
    validate_sql_column_ref(column)?;
    validate_sql_identifier(alias)?;
    Ok(Some(ParsedNumericAbsProjection {
        alias: alias.to_string(),
        column: column.to_string(),
    }))
}

fn parse_numeric_rounding_projection(
    raw: &str,
) -> Result<Option<ParsedNumericRoundingProjection>, ShardLoomError> {
    let Some(as_index) = find_keyword_outside_quotes(raw, "as") else {
        return Ok(None);
    };
    let expression_raw = raw[..as_index].trim();
    let Some((op, open_index)) = parse_numeric_rounding_function_prefix(expression_raw) else {
        return Ok(None);
    };
    let alias = raw[as_index + "as".len()..].trim();
    let Some(close_index) = expression_raw.rfind(')') else {
        return Err(unsupported_sql_error(
            "numeric rounding projections must be written as FLOOR/CEIL/ROUND(<column>) AS <column>",
        ));
    };
    if close_index + 1 != expression_raw.len() {
        return Err(unsupported_sql_error(
            "numeric rounding projections must be written as FLOOR/CEIL/ROUND(<column>) AS <column>",
        ));
    }
    let column = expression_raw[open_index + 1..close_index].trim();
    if column.is_empty() || alias.is_empty() {
        return Err(unsupported_sql_error(
            "numeric rounding projections must be written as FLOOR/CEIL/ROUND(<column>) AS <column>",
        ));
    }
    validate_sql_column_ref(column)?;
    validate_sql_identifier(alias)?;
    Ok(Some(ParsedNumericRoundingProjection {
        alias: alias.to_string(),
        column: column.to_string(),
        op,
    }))
}

fn parse_cast_projection(raw: &str) -> Result<Option<ParsedCastProjection>, ShardLoomError> {
    let Some(as_index) = find_keyword_outside_quotes_and_parentheses(raw, "as")? else {
        return Ok(None);
    };
    let expression_raw = raw[..as_index].trim();
    let alias = raw[as_index + "as".len()..].trim();
    let Some((mode, inner)) = parse_cast_call_expression(expression_raw)? else {
        return Ok(None);
    };
    if alias.is_empty() {
        return Err(unsupported_sql_error(
            "CAST/TRY_CAST projections require an output alias",
        ));
    }
    validate_sql_identifier(alias)?;
    let Some(inner_as_index) = find_keyword_outside_quotes(inner, "as") else {
        return Err(unsupported_sql_error(
            "CAST/TRY_CAST projections must use CAST(<column> AS <dtype>) syntax",
        ));
    };
    let column = inner[..inner_as_index].trim();
    let target_raw = inner[inner_as_index + "as".len()..].trim();
    let target_dtype = parse_cast_target_dtype(target_raw)?;
    let (column, expression, source_columns) = parse_cast_source_expression(
        column,
        &target_dtype,
        &format!("project.cast_arg.{alias}"),
        "CAST/TRY_CAST binary projections require at least one source column expression",
    )?;
    Ok(Some(ParsedCastProjection {
        alias: alias.to_string(),
        column,
        expression,
        source_columns,
        target_dtype,
        mode,
    }))
}

fn parse_cast_source_expression(
    source_raw: &str,
    target_dtype: &LogicalDType,
    id_prefix: &str,
    empty_source_message: &str,
) -> Result<(String, Expression, Vec<String>), ShardLoomError> {
    if matches!(target_dtype, LogicalDType::Binary) {
        let expression = parse_string_scalar_expression(source_raw, id_prefix)?;
        let source_columns = expression_source_columns(&expression);
        if source_columns.is_empty() {
            return Err(unsupported_sql_error(empty_source_message));
        }
        let column = source_columns
            .first()
            .cloned()
            .ok_or_else(|| unsupported_sql_error(empty_source_message))?;
        return Ok((column, expression, source_columns));
    }
    validate_sql_column_ref(source_raw)?;
    Ok((
        source_raw.to_string(),
        Expression::column(
            ExprId::new(format!("{id_prefix}.{source_raw}"))?,
            ColumnRef::new(source_raw.to_string())?,
        ),
        vec![source_raw.to_string()],
    ))
}

fn parse_cast_call_expression(raw: &str) -> Result<Option<(CastMode, &str)>, ShardLoomError> {
    let Some((mode, open_index)) = parse_cast_function_prefix(raw) else {
        return Ok(None);
    };
    let close_index = matching_closing_parenthesis(raw, open_index)?.ok_or_else(|| {
        unsupported_sql_error(
            "CAST/TRY_CAST expressions must be written as CAST(<column> AS <dtype>) or TRY_CAST(<column> AS <dtype>)",
        )
    })?;
    if !raw[close_index + 1..].trim().is_empty() {
        return Err(unsupported_sql_error(&format!(
            "{} expressions must be a single call",
            mode.function_label()
        )));
    }
    Ok(Some((mode, raw[open_index + 1..close_index].trim())))
}

fn parse_cast_function_prefix(raw: &str) -> Option<(CastMode, usize)> {
    let trimmed = raw.trim();
    for (name, mode) in [("try_cast", CastMode::Try), ("cast", CastMode::Strict)] {
        let len = name.len();
        if trimmed
            .get(..len)
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case(name))
            && trimmed.as_bytes().get(len) == Some(&b'(')
        {
            return Some((mode, len));
        }
    }
    None
}

fn parse_null_coalesce_projection(
    raw: &str,
) -> Result<Option<ParsedNullCoalesceProjection>, ShardLoomError> {
    let Some(as_index) = find_keyword_outside_quotes_and_parentheses(raw, "as")? else {
        return Ok(None);
    };
    let expression_raw = raw[..as_index].trim();
    let alias = raw[as_index + "as".len()..].trim();
    if !expression_raw
        .get(..9)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("coalesce("))
    {
        return Ok(None);
    }
    if alias.is_empty() {
        return Err(unsupported_sql_error(
            "COALESCE projections require an output alias",
        ));
    }
    validate_sql_identifier(alias)?;
    let open_index = "coalesce".len();
    let close_index =
        matching_closing_parenthesis(expression_raw, open_index)?.ok_or_else(|| {
            unsupported_sql_error(
                "COALESCE projections must use COALESCE(<column>, <literal>) AS <column>",
            )
        })?;
    if !expression_raw[close_index + 1..].trim().is_empty() {
        return Err(unsupported_sql_error(
            "COALESCE projections must be a single COALESCE(<column>, <literal>) expression before AS",
        ));
    }
    let inner = expression_raw[open_index + 1..close_index].trim();
    let args = split_sql_csv(inner)?;
    let [column_raw, fallback_raw] = args.as_slice() else {
        return Err(unsupported_sql_error(
            "COALESCE projections require exactly two arguments: <column>, <literal>",
        ));
    };
    let (column, source_cast_dtype) = parse_null_coalesce_column_arg(column_raw)?;
    let fallback = parse_projection_literal_value(fallback_raw)?;
    if matches!(fallback, ScalarValue::Null) {
        return Err(unsupported_sql_error(
            "COALESCE projections require a non-NULL fallback literal in this scoped runtime slice",
        ));
    }
    Ok(Some(ParsedNullCoalesceProjection {
        alias: alias.to_string(),
        column,
        source_cast_dtype,
        fallback,
    }))
}

fn parse_nullif_projection(raw: &str) -> Result<Option<ParsedNullIfProjection>, ShardLoomError> {
    let Some(as_index) = find_keyword_outside_quotes_and_parentheses(raw, "as")? else {
        return Ok(None);
    };
    let expression_raw = raw[..as_index].trim();
    let alias = raw[as_index + "as".len()..].trim();
    if !expression_raw
        .get(..7)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("nullif("))
    {
        return Ok(None);
    }
    if alias.is_empty() {
        return Err(unsupported_sql_error(
            "NULLIF projections require an output alias",
        ));
    }
    validate_sql_identifier(alias)?;
    let open_index = "nullif".len();
    let close_index =
        matching_closing_parenthesis(expression_raw, open_index)?.ok_or_else(|| {
            unsupported_sql_error(
                "NULLIF projections must use NULLIF(<column>, <literal>) AS <column>",
            )
        })?;
    if !expression_raw[close_index + 1..].trim().is_empty() {
        return Err(unsupported_sql_error(
            "NULLIF projections must be a single NULLIF(<column>, <literal>) expression before AS",
        ));
    }
    let inner = expression_raw[open_index + 1..close_index].trim();
    let args = split_sql_csv(inner)?;
    let [column_raw, sentinel_raw] = args.as_slice() else {
        return Err(unsupported_sql_error(
            "NULLIF projections require exactly two arguments: <column>, <literal>",
        ));
    };
    let (column, source_cast_dtype) = parse_null_coalesce_column_arg(column_raw)?;
    let sentinel = parse_projection_literal_value(sentinel_raw)?;
    if matches!(sentinel, ScalarValue::Null) {
        return Err(unsupported_sql_error(
            "NULLIF projections require a non-NULL sentinel literal in this scoped runtime slice",
        ));
    }
    Ok(Some(ParsedNullIfProjection {
        alias: alias.to_string(),
        column,
        source_cast_dtype,
        sentinel,
    }))
}

fn parse_conditional_projection(
    raw: &str,
) -> Result<Option<ParsedConditionalProjection>, ShardLoomError> {
    let Some(as_index) = find_keyword_outside_quotes_and_parentheses(raw, "as")? else {
        return Ok(None);
    };
    let expression_raw = raw[..as_index].trim();
    let alias = raw[as_index + "as".len()..].trim();
    if !expression_raw
        .get(..4)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("case"))
    {
        return Ok(None);
    }
    if !keyword_boundary(expression_raw, 0, 4) {
        return Ok(None);
    }
    if alias.is_empty() {
        return Err(unsupported_sql_error(
            "CASE projections require an output alias",
        ));
    }
    validate_sql_identifier(alias)?;
    let when_index = find_keyword_outside_quotes_and_parentheses(expression_raw, "when")?
        .ok_or_else(|| {
            unsupported_sql_error(
                "CASE projections must use CASE WHEN <predicate> THEN <literal-or-column> ELSE <literal-or-column> END AS <column>",
            )
        })?;
    if !expression_raw[..when_index]
        .trim()
        .eq_ignore_ascii_case("case")
    {
        return Err(unsupported_sql_error(
            "CASE projections admit only one CASE WHEN expression",
        ));
    }
    let then_index = find_keyword_outside_quotes_and_parentheses(expression_raw, "then")?
        .ok_or_else(|| unsupported_sql_error("CASE projections require a THEN branch"))?;
    let else_index = find_keyword_outside_quotes_and_parentheses(expression_raw, "else")?
        .ok_or_else(|| unsupported_sql_error("CASE projections require an ELSE branch"))?;
    let end_index = find_keyword_outside_quotes_and_parentheses(expression_raw, "end")?
        .ok_or_else(|| unsupported_sql_error("CASE projections require an END marker before AS"))?;
    if !(when_index < then_index && then_index < else_index && else_index < end_index) {
        return Err(unsupported_sql_error(
            "CASE projections must use CASE WHEN <predicate> THEN <literal-or-column> ELSE <literal-or-column> END AS <column>",
        ));
    }
    if !expression_raw[end_index + "end".len()..].trim().is_empty() {
        return Err(unsupported_sql_error(
            "CASE projections must be a single CASE WHEN expression before AS",
        ));
    }
    let predicate_raw = expression_raw[when_index + "when".len()..then_index].trim();
    let then_raw = expression_raw[then_index + "then".len()..else_index].trim();
    let else_raw = expression_raw[else_index + "else".len()..end_index].trim();
    if predicate_raw.is_empty() || then_raw.is_empty() || else_raw.is_empty() {
        return Err(unsupported_sql_error(
            "CASE projections require non-empty predicate, THEN branch, and ELSE branch",
        ));
    }
    if [then_raw, else_raw].iter().any(|raw| {
        parse_projection_literal_value(raw).is_ok_and(|value| matches!(value, ScalarValue::Null))
            || (parse_projection_literal_value(raw).is_err()
                && validate_sql_column_ref(raw).is_err())
    }) {
        // The generic scalar declaration owns composed branches; the native
        // binder determines their common type before selected evaluation.
        return Ok(None);
    }
    let predicate = parse_predicate(predicate_raw)?;
    let then_branch = parse_conditional_projection_branch(then_raw, "THEN")?;
    let else_branch = parse_conditional_projection_branch(else_raw, "ELSE")?;
    let then_dtype = then_branch.literal_dtype();
    let else_dtype = else_branch.literal_dtype();
    if let (Some(then_dtype), Some(else_dtype)) = (&then_dtype, &else_dtype)
        && then_dtype != else_dtype
    {
        return Err(unsupported_sql_error(&format!(
            "CASE projection THEN/ELSE branches must have matching dtypes; got {} and {}",
            then_dtype.as_str(),
            else_dtype.as_str()
        )));
    }
    Ok(Some(ParsedConditionalProjection {
        alias: alias.to_string(),
        predicate,
        then_branch,
        else_branch,
        then_dtype,
        else_dtype,
    }))
}

fn parse_conditional_projection_branch(
    raw: &str,
    branch_label: &str,
) -> Result<ParsedConditionalBranch, ShardLoomError> {
    if let Ok(value) = parse_projection_literal_value(raw) {
        if matches!(value, ScalarValue::Null) {
            return Err(unsupported_sql_error(&format!(
                "CASE projections require a non-NULL {branch_label} branch literal in this scoped runtime slice",
            )));
        }
        Ok(ParsedConditionalBranch::Literal(value))
    } else {
        validate_sql_column_ref(raw).map_err(|_| {
            unsupported_sql_error(
                "CASE projection branches must be literals or source columns in this scoped runtime slice",
            )
        })?;
        Ok(ParsedConditionalBranch::Column(raw.to_string()))
    }
}

fn parse_predicate_projection(
    raw: &str,
) -> Result<Option<ParsedPredicateProjection>, ShardLoomError> {
    let Some(as_index) = find_keyword_outside_quotes_and_parentheses(raw, "as")? else {
        return Ok(None);
    };
    let expression_raw = raw[..as_index].trim();
    let alias = raw[as_index + "as".len()..].trim();
    if expression_raw.is_empty() || alias.is_empty() {
        return Ok(None);
    }
    if !is_explicit_predicate_projection_shape(expression_raw)? {
        return Ok(None);
    }
    validate_sql_identifier(alias)?;
    let predicate = parse_predicate(expression_raw)?;
    Ok(Some(ParsedPredicateProjection {
        alias: alias.to_string(),
        predicate,
    }))
}

fn is_explicit_predicate_projection_shape(raw: &str) -> Result<bool, ShardLoomError> {
    // A scalar SELECT owns the predicates within its relation. Removing its
    // enclosing parentheses must not promote an inner comparison to this SELECT.
    let inner = trim_enclosing_scalar_expression_parentheses(raw)?;
    if find_keyword_outside_quotes_and_parentheses(inner, "select")? == Some(0) {
        return Ok(false);
    }
    if inner
        .get(..5)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("case "))
    {
        return Ok(false);
    }
    if parse_regex_function_prefix(inner).is_some() {
        return Ok(true);
    }
    for keyword in [
        "is", "not", "in", "exists", "like", "rlike", "regexp", "between", "and", "or",
    ] {
        if find_keyword_outside_quotes_and_parentheses(inner, keyword)?.is_some() {
            return Ok(true);
        }
    }
    Ok(find_top_level_comparison_operator(inner)?.is_some())
}

fn parse_generic_expression_projection(
    raw: &str,
) -> Result<Option<ParsedGenericExpressionProjection>, ShardLoomError> {
    let Some(as_index) = find_keyword_outside_quotes_and_parentheses(raw, "as")? else {
        return Ok(None);
    };
    let expression_raw = raw[..as_index].trim();
    let alias = raw[as_index + "as".len()..].trim();
    if expression_raw.is_empty() || alias.is_empty() {
        return Ok(None);
    }
    if let Some(expression) = parse_column_null_selection(expression_raw, alias)? {
        validate_sql_identifier(alias)?;
        return Ok(Some(ParsedGenericExpressionProjection {
            alias: alias.to_owned(),
            source_columns: expression_source_columns(&expression),
            operator_families: expression_operator_families(&expression),
            binary_operator_count: 0,
            expression: expression.into(),
        }));
    }
    if validate_sql_column_ref(expression_raw).is_ok() && parse_sql_literal(expression_raw).is_err()
    {
        validate_sql_identifier(alias)?;
        return Ok(Some(ParsedGenericExpressionProjection {
            alias: alias.to_string(),
            expression: Expression::column(
                ExprId::new(format!("project.alias.{alias}"))?,
                ColumnRef::new(expression_raw)?,
            )
            .into(),
            source_columns: vec![expression_raw.to_owned()],
            operator_families: Vec::new(),
            binary_operator_count: 0,
        }));
    }
    let contains_temporal_difference =
        expression_contains_temporal_difference_call(expression_raw)?;
    let has_numeric_operator = expression_contains_numeric_operator(expression_raw)?;
    let composed = scalar_expression::composed(expression_raw)?;
    if is_simple_numeric_arithmetic_projection_shape(expression_raw)?
        || (!has_numeric_operator && !contains_temporal_difference && !composed)
    {
        return Ok(None);
    }
    validate_sql_identifier(alias)?;
    let expression =
        parse_numeric_scalar_expression(expression_raw, &format!("project.generic.{alias}"))?;
    let source_columns = expression_source_columns(&expression);
    let operator_families = expression_operator_families(&expression);
    let binary_operator_count = expression_binary_operator_count(&expression);
    if binary_operator_count == 0 && !expression_has_temporal_difference(&expression) && !composed {
        return Ok(None);
    }
    Ok(Some(ParsedGenericExpressionProjection {
        alias: alias.to_string(),
        expression,
        source_columns,
        operator_families,
        binary_operator_count,
    }))
}

fn parse_column_null_selection(
    raw: &str,
    alias: &str,
) -> Result<Option<Expression>, ShardLoomError> {
    let Some(name) = ["coalesce", "nullif"].into_iter().find(|name| {
        raw.get(..name.len())
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case(name))
            && raw.as_bytes().get(name.len()) == Some(&b'(')
    }) else {
        return Ok(None);
    };
    let Some(close) = matching_closing_parenthesis(raw, name.len())? else {
        return Ok(None);
    };
    if close + 1 != raw.len() {
        return Ok(None);
    }
    let arguments = split_sql_csv(&raw[name.len() + 1..close])?;
    if arguments.len() != 2
        || arguments.iter().any(|argument| {
            validate_sql_column_ref(argument).is_err() || parse_sql_literal(argument).is_ok()
        })
    {
        return Ok(None);
    }
    let args = arguments
        .iter()
        .enumerate()
        .map(|(index, argument)| {
            Ok(Expression::column(
                ExprId::new(format!("project.{name}.{alias}.{index}"))?,
                ColumnRef::new(argument.clone())?,
            ))
        })
        .collect::<Result<Vec<_>, ShardLoomError>>()?;
    Ok(Some(Expression::new(
        ExprId::new(format!("project.{name}.{alias}"))?,
        ExpressionKind::FunctionCall {
            name: name.to_owned(),
            args,
        },
    )))
}

fn is_simple_numeric_arithmetic_projection_shape(raw: &str) -> Result<bool, ShardLoomError> {
    let tokens = split_whitespace_outside_quotes(raw)?;
    let Some(op_index) = tokens
        .iter()
        .position(|token| parse_numeric_arithmetic_op(token).is_some())
    else {
        return Ok(false);
    };
    if tokens.len() != 3 || op_index != 1 {
        return Ok(false);
    }
    if validate_sql_column_ref(&tokens[0]).is_err() {
        return Ok(false);
    }
    Ok(parse_numeric_arithmetic_literal(&tokens[2]).is_ok())
}

#[cfg(test)]
#[path = "sql_aggregate_expression_tests.rs"]
mod aggregate_expression_tests;
#[path = "sql_scalar_expression.rs"]
mod scalar_expression;

fn parse_numeric_scalar_expression(
    raw: &str,
    id_prefix: &str,
) -> Result<scalar_expression::ParsedScalarExpression, ShardLoomError> {
    scalar_expression::parse(raw, id_prefix)
}

fn expression_contains_temporal_difference_call(raw: &str) -> Result<bool, ShardLoomError> {
    let trimmed = trim_enclosing_scalar_expression_parentheses(raw)?;
    Ok(temporal_difference_function_prefix(trimmed).is_some())
}

fn temporal_difference_function_prefix(raw: &str) -> Option<(&'static str, LogicalDType)> {
    for (name, dtype) in [
        ("date_diff_days", LogicalDType::Date32),
        ("timestamp_diff_seconds", LogicalDType::TimestampMicros),
    ] {
        let len = name.len();
        if raw
            .get(..len)
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case(name))
            && raw.as_bytes().get(len) == Some(&b'(')
        {
            return Some((name, dtype));
        }
    }
    None
}

fn trim_enclosing_scalar_expression_parentheses(mut raw: &str) -> Result<&str, ShardLoomError> {
    raw = raw.trim();
    loop {
        if !raw.starts_with('(') {
            return Ok(raw);
        }
        let Some(close_index) = matching_closing_parenthesis(raw, 0)? else {
            return Err(unsupported_sql_error(
                "generic numeric expression parentheses must be balanced",
            ));
        };
        if close_index != raw.len() - 1 {
            return Ok(raw);
        }
        raw = raw[1..close_index].trim();
        if raw.is_empty() {
            return Err(unsupported_sql_error(
                "generic numeric expression parentheses must contain an expression",
            ));
        }
    }
}

fn expression_contains_numeric_operator(raw: &str) -> Result<bool, ShardLoomError> {
    let mut chars = raw.char_indices().peekable();
    let mut in_quote = false;
    let mut depth = 0_u32;
    while let Some((index, ch)) = chars.next() {
        if ch == '\'' {
            if in_quote && chars.peek().is_some_and(|(_, next)| *next == '\'') {
                let _ = chars.next();
            } else {
                in_quote = !in_quote;
            }
            continue;
        }
        if in_quote {
            continue;
        }
        match ch {
            '(' => depth += 1,
            ')' => {
                depth = depth.checked_sub(1).ok_or_else(|| {
                    unsupported_sql_error("generic numeric expression parentheses are not balanced")
                })?;
            }
            '+' | '-' | '*' | '/' | '%' if !is_unary_numeric_sign(raw, index, ch) => {
                return Ok(true);
            }
            _ => {}
        }
    }
    if in_quote {
        return Err(unsupported_sql_error("SQL string literal is not closed"));
    }
    if depth != 0 {
        return Err(unsupported_sql_error(
            "generic numeric expression parentheses are not balanced",
        ));
    }
    Ok(false)
}

fn is_unary_numeric_sign(raw: &str, index: usize, ch: char) -> bool {
    if !matches!(ch, '+' | '-') {
        return false;
    }
    let before = raw[..index]
        .chars()
        .rev()
        .find(|candidate| !candidate.is_whitespace());
    let after = raw[index + ch.len_utf8()..]
        .chars()
        .find(|candidate| !candidate.is_whitespace());
    let sign_position =
        before.is_none_or(|candidate| matches!(candidate, '(' | ',' | '+' | '-' | '*' | '/' | '%'));
    let exponent_sign = raw[..index].ends_with(['e', 'E']) && {
        let mantissa = raw[..index - 1]
            .rsplit(|c: char| c.is_whitespace() || matches!(c, '(' | ',' | '+' | '-' | '*' | '/'))
            .next()
            .unwrap_or_default();
        !mantissa.is_empty() && mantissa.parse::<f64>().is_ok()
    };
    (sign_position && after.is_some())
        || (exponent_sign && after.is_some_and(|ch| ch.is_ascii_digit()))
}

fn expression_source_columns(expression: &Expression) -> Vec<String> {
    let mut columns = BTreeSet::new();
    collect_expression_source_columns(expression, &mut columns);
    columns.into_iter().collect()
}

fn collect_expression_source_columns(expression: &Expression, columns: &mut BTreeSet<String>) {
    match &expression.kind {
        ExpressionKind::Column(column) => {
            columns.insert(column.as_str().to_string());
        }
        ExpressionKind::Alias { expr, .. }
        | ExpressionKind::Cast { expr, .. }
        | ExpressionKind::TryCast { expr, .. }
        | ExpressionKind::Unary { expr, .. } => collect_expression_source_columns(expr, columns),
        ExpressionKind::List { values } => {
            for value in values {
                collect_expression_source_columns(value, columns);
            }
        }
        ExpressionKind::Struct { fields } => {
            for (_name, expression) in fields {
                collect_expression_source_columns(expression, columns);
            }
        }
        ExpressionKind::Binary { left, right, .. }
        | ExpressionKind::Compare { left, right, .. } => {
            collect_expression_source_columns(left, columns);
            collect_expression_source_columns(right, columns);
        }
        ExpressionKind::FunctionCall { args, .. } => {
            for arg in args {
                collect_expression_source_columns(arg, columns);
            }
        }
        ExpressionKind::Literal(_)
        | ExpressionKind::RelationalValue { .. }
        | ExpressionKind::Unsupported { .. } => {}
    }
}

fn expression_has_temporal_difference(expression: &Expression) -> bool {
    match &expression.kind {
        ExpressionKind::FunctionCall { name, .. }
            if name.eq_ignore_ascii_case("date_diff_days")
                || name.eq_ignore_ascii_case("timestamp_diff_seconds") =>
        {
            true
        }
        ExpressionKind::FunctionCall { args, .. } => {
            args.iter().any(expression_has_temporal_difference)
        }
        ExpressionKind::List { values } => values.iter().any(expression_has_temporal_difference),
        ExpressionKind::Struct { fields } => fields
            .iter()
            .any(|(_name, expression)| expression_has_temporal_difference(expression)),
        ExpressionKind::Alias { expr, .. }
        | ExpressionKind::Cast { expr, .. }
        | ExpressionKind::TryCast { expr, .. }
        | ExpressionKind::Unary { expr, .. } => expression_has_temporal_difference(expr),
        ExpressionKind::Binary { left, right, .. }
        | ExpressionKind::Compare { left, right, .. } => {
            expression_has_temporal_difference(left) || expression_has_temporal_difference(right)
        }
        ExpressionKind::Literal(_)
        | ExpressionKind::Column(_)
        | ExpressionKind::RelationalValue { .. }
        | ExpressionKind::Unsupported { .. } => false,
    }
}

fn expression_pair_has_temporal_difference(left: &Expression, right: &Expression) -> bool {
    expression_has_temporal_difference(left) || expression_has_temporal_difference(right)
}

fn expression_operator_families(expression: &Expression) -> Vec<String> {
    let mut families = BTreeSet::new();
    collect_expression_operator_families(expression, &mut families);
    families.into_iter().collect()
}

fn collect_expression_operator_families(expression: &Expression, families: &mut BTreeSet<String>) {
    match &expression.kind {
        ExpressionKind::RelationalValue { .. } => {
            families.insert("relational_value".into());
        }
        ExpressionKind::Cast { expr, .. } => {
            families.insert("cast".to_string());
            collect_expression_operator_families(expr, families);
        }
        ExpressionKind::TryCast { expr, .. } => {
            families.insert("try_cast".to_string());
            collect_expression_operator_families(expr, families);
        }
        ExpressionKind::Binary { left, op, right } => {
            families.insert(
                match op {
                    BinaryOp::Add
                    | BinaryOp::Subtract
                    | BinaryOp::Multiply
                    | BinaryOp::Divide
                    | BinaryOp::Remainder => "numeric_binary",
                    BinaryOp::And | BinaryOp::Or => "logical_predicate",
                }
                .to_string(),
            );
            collect_expression_operator_families(left, families);
            collect_expression_operator_families(right, families);
        }
        ExpressionKind::FunctionCall { name, args } => {
            families.insert(generic_function_operator_family(name).to_string());
            for arg in args {
                collect_expression_operator_families(arg, families);
            }
        }
        ExpressionKind::List { values } => {
            families.insert("list_construct".to_string());
            for value in values {
                collect_expression_operator_families(value, families);
            }
        }
        ExpressionKind::Struct { fields } => {
            families.insert("struct_construct".to_string());
            for (_name, expression) in fields {
                collect_expression_operator_families(expression, families);
            }
        }
        ExpressionKind::Alias { expr, .. } | ExpressionKind::Unary { expr, .. } => {
            collect_expression_operator_families(expr, families);
        }
        ExpressionKind::Compare { left, right, .. } => {
            collect_expression_operator_families(left, families);
            collect_expression_operator_families(right, families);
        }
        ExpressionKind::Literal(_)
        | ExpressionKind::Column(_)
        | ExpressionKind::Unsupported { .. } => {}
    }
}

fn generic_function_operator_family(name: &str) -> &'static str {
    match name.trim().to_ascii_lowercase().as_str() {
        "abs" | "numeric_abs" => "numeric_abs",
        "floor" | "ceil" | "ceiling" | "round" | "numeric_floor" | "numeric_ceil"
        | "numeric_round" => "numeric_rounding",
        "date_diff_days" | "timestamp_diff_seconds" => "temporal_difference",
        _ => "function",
    }
}

fn expression_binary_operator_count(expression: &Expression) -> usize {
    match &expression.kind {
        ExpressionKind::Binary { left, right, .. } => {
            1 + expression_binary_operator_count(left) + expression_binary_operator_count(right)
        }
        ExpressionKind::Alias { expr, .. }
        | ExpressionKind::Cast { expr, .. }
        | ExpressionKind::TryCast { expr, .. }
        | ExpressionKind::Unary { expr, .. } => expression_binary_operator_count(expr),
        ExpressionKind::Compare { left, right, .. } => {
            expression_binary_operator_count(left) + expression_binary_operator_count(right)
        }
        ExpressionKind::FunctionCall { args, .. } => {
            args.iter().map(expression_binary_operator_count).sum()
        }
        ExpressionKind::List { values } => {
            values.iter().map(expression_binary_operator_count).sum()
        }
        ExpressionKind::Struct { fields } => fields
            .iter()
            .map(|(_name, expression)| expression_binary_operator_count(expression))
            .sum(),
        ExpressionKind::Literal(_)
        | ExpressionKind::Column(_)
        | ExpressionKind::RelationalValue { .. }
        | ExpressionKind::Unsupported { .. } => 0,
    }
}

fn expression_pair_source_columns(left: &Expression, right: &Expression) -> Vec<String> {
    let mut columns = BTreeSet::new();
    collect_expression_source_columns(left, &mut columns);
    collect_expression_source_columns(right, &mut columns);
    columns.into_iter().collect()
}

fn expression_pair_operator_families(left: &Expression, right: &Expression) -> Vec<String> {
    let mut families = BTreeSet::new();
    collect_expression_operator_families(left, &mut families);
    collect_expression_operator_families(right, &mut families);
    families.into_iter().collect()
}

fn parse_null_coalesce_column_arg(
    raw: &str,
) -> Result<(String, Option<LogicalDType>), ShardLoomError> {
    let trimmed = raw.trim();
    if !trimmed
        .get(..5)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("cast("))
    {
        validate_sql_column_ref(trimmed)?;
        return Ok((trimmed.to_string(), None));
    }
    let close_index = matching_closing_parenthesis(trimmed, 4)?.ok_or_else(|| {
        unsupported_sql_error(
            "COALESCE CAST arguments must use CAST(<column> AS date32) or CAST(<column> AS timestamp_micros)",
        )
    })?;
    if !trimmed[close_index + 1..].trim().is_empty() {
        return Err(unsupported_sql_error(
            "COALESCE CAST arguments must be a single CAST(<column> AS dtype) expression",
        ));
    }
    let inner = trimmed[5..close_index].trim();
    let as_index = find_keyword_outside_quotes(inner, "as").ok_or_else(|| {
        unsupported_sql_error("COALESCE CAST arguments must use CAST(<column> AS dtype)")
    })?;
    let column = inner[..as_index].trim();
    let target_raw = inner[as_index + 2..].trim();
    validate_sql_column_ref(column)?;
    let target_dtype = parse_cast_target_dtype(target_raw)?;
    match target_dtype {
        LogicalDType::Date32 | LogicalDType::TimestampMicros => {
            Ok((column.to_string(), Some(target_dtype)))
        }
        _ => Err(unsupported_sql_error(
            "COALESCE CAST arguments currently admit date32 or timestamp_micros target dtypes only",
        )),
    }
}

fn parse_date_arithmetic_projection(
    raw: &str,
) -> Result<Option<ParsedDateArithmeticProjection>, ShardLoomError> {
    let Some(as_index) = find_keyword_outside_quotes_and_parentheses(raw, "as")? else {
        return Ok(None);
    };
    let expression_raw = raw[..as_index].trim();
    let alias = raw[as_index + "as".len()..].trim();
    let Some((function_name, op)) = [
        ("date_add_days", DateArithmeticOp::AddDays),
        ("date_sub_days", DateArithmeticOp::SubDays),
    ]
    .into_iter()
    .find(|(name, _)| {
        expression_raw
            .get(..name.len())
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case(name))
            && expression_raw.as_bytes().get(name.len()) == Some(&b'(')
    }) else {
        return Ok(None);
    };
    if alias.is_empty() {
        return Err(unsupported_sql_error(
            "date arithmetic projections require an output alias",
        ));
    }
    validate_sql_identifier(alias)?;
    let open_index = function_name.len();
    let close_index = matching_closing_parenthesis(expression_raw, open_index)?.ok_or_else(|| {
        unsupported_sql_error(
            "date arithmetic projections must use DATE_ADD_DAYS(<column>, <days>) AS <column> or DATE_SUB_DAYS(<column>, <days>) AS <column>",
        )
    })?;
    if !expression_raw[close_index + 1..].trim().is_empty() {
        return Err(unsupported_sql_error(
            "date arithmetic projections must be a single DATE_ADD_DAYS/DATE_SUB_DAYS expression before AS",
        ));
    }
    let inner = expression_raw[open_index + 1..close_index].trim();
    let args = split_sql_csv(inner)?;
    let [column_raw, day_count_raw] = args.as_slice() else {
        return Err(unsupported_sql_error(
            "date arithmetic projections require exactly two arguments: <column>, <days>",
        ));
    };
    Ok(Some(ParsedDateArithmeticProjection {
        alias: alias.to_string(),
        column: parse_date_arithmetic_column_arg(column_raw)?,
        op,
        day_count: parse_date_arithmetic_days(day_count_raw)?,
    }))
}

fn parse_timestamp_arithmetic_projection(
    raw: &str,
) -> Result<Option<ParsedTimestampArithmeticProjection>, ShardLoomError> {
    let Some(as_index) = find_keyword_outside_quotes_and_parentheses(raw, "as")? else {
        return Ok(None);
    };
    let expression_raw = raw[..as_index].trim();
    let alias = raw[as_index + "as".len()..].trim();
    let Some((function_name, op)) = [
        ("timestamp_add_seconds", TimestampArithmeticOp::AddSeconds),
        ("timestamp_sub_seconds", TimestampArithmeticOp::SubSeconds),
    ]
    .into_iter()
    .find(|(name, _)| {
        expression_raw
            .get(..name.len())
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case(name))
            && expression_raw.as_bytes().get(name.len()) == Some(&b'(')
    }) else {
        return Ok(None);
    };
    if alias.is_empty() {
        return Err(unsupported_sql_error(
            "timestamp arithmetic projections require an output alias",
        ));
    }
    validate_sql_identifier(alias)?;
    let open_index = function_name.len();
    let close_index = matching_closing_parenthesis(expression_raw, open_index)?.ok_or_else(|| {
        unsupported_sql_error(
            "timestamp arithmetic projections must use TIMESTAMP_ADD_SECONDS(<column>, <seconds>) AS <column> or TIMESTAMP_SUB_SECONDS(<column>, <seconds>) AS <column>",
        )
    })?;
    if !expression_raw[close_index + 1..].trim().is_empty() {
        return Err(unsupported_sql_error(
            "timestamp arithmetic projections must be a single TIMESTAMP_ADD_SECONDS/TIMESTAMP_SUB_SECONDS expression before AS",
        ));
    }
    let inner = expression_raw[open_index + 1..close_index].trim();
    let args = split_sql_csv(inner)?;
    let [column_raw, second_count_raw] = args.as_slice() else {
        return Err(unsupported_sql_error(
            "timestamp arithmetic projections require exactly two arguments: <column>, <seconds>",
        ));
    };
    Ok(Some(ParsedTimestampArithmeticProjection {
        alias: alias.to_string(),
        column: parse_timestamp_arithmetic_column_arg(column_raw)?,
        op,
        second_count: parse_timestamp_arithmetic_seconds(second_count_raw)?,
    }))
}

fn parse_string_scalar_expression(
    raw: &str,
    id_prefix: &str,
) -> Result<Expression, ShardLoomError> {
    let trimmed = trim_enclosing_string_expression_parentheses(raw)?;
    if let Some(expression) = parse_string_transform_call_expression(trimmed, id_prefix)? {
        return Ok(expression);
    }
    if let Some(call) = parse_string_function_call_expression(trimmed, id_prefix)? {
        return Ok(call.expression);
    }
    if let Ok(value) = parse_projection_literal_value(trimmed) {
        return Ok(Expression::literal(
            ExprId::new(format!("{id_prefix}.literal"))?,
            value,
        ));
    }
    validate_sql_column_ref(trimmed)?;
    Ok(Expression::column(
        ExprId::new(format!("{id_prefix}.{trimmed}"))?,
        ColumnRef::new(trimmed.to_string())?,
    ))
}

fn trim_enclosing_string_expression_parentheses(mut raw: &str) -> Result<&str, ShardLoomError> {
    raw = raw.trim();
    loop {
        if !raw.starts_with('(') {
            return Ok(raw);
        }
        let Some(close_index) = matching_closing_parenthesis(raw, 0)? else {
            return Err(unsupported_sql_error(
                "string expression parentheses must be balanced",
            ));
        };
        if close_index != raw.len() - 1 {
            return Ok(raw);
        }
        raw = raw[1..close_index].trim();
        if raw.is_empty() {
            return Err(unsupported_sql_error(
                "string expression parentheses must contain an expression",
            ));
        }
    }
}

fn parse_string_transform_prefix(raw: &str) -> Option<(StringTransformOp, usize)> {
    [
        ("lower", StringTransformOp::Lower),
        ("upper", StringTransformOp::Upper),
        ("trim", StringTransformOp::Trim),
    ]
    .into_iter()
    .find_map(|(name, op)| {
        raw.get(..name.len())
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case(name))
            .then_some(())
            .filter(|()| raw.as_bytes().get(name.len()) == Some(&b'('))
            .map(|()| (op, name.len()))
    })
}

fn parse_string_transform_call_expression(
    raw: &str,
    id_prefix: &str,
) -> Result<Option<Expression>, ShardLoomError> {
    let trimmed = raw.trim();
    let Some((op, open_index)) = parse_string_transform_prefix(trimmed) else {
        return Ok(None);
    };
    let close_index = matching_closing_parenthesis(trimmed, open_index)?.ok_or_else(|| {
        unsupported_sql_error(
            "string transform expressions must use LOWER|UPPER|TRIM(<string-expression>)",
        )
    })?;
    if !trimmed[close_index + 1..].trim().is_empty() {
        return Err(unsupported_sql_error(
            "string transform expressions must be a single function call",
        ));
    }
    let inner = trimmed[open_index + 1..close_index].trim();
    let args = split_sql_csv(inner)?;
    let [arg] = args.as_slice() else {
        return Err(unsupported_sql_error(
            "string transform expressions require exactly one argument",
        ));
    };
    Ok(Some(Expression::new(
        ExprId::new(id_prefix.to_string())?,
        ExpressionKind::FunctionCall {
            name: op.function_name().to_string(),
            args: vec![parse_string_scalar_expression(
                arg,
                &format!("{id_prefix}.arg"),
            )?],
        },
    )))
}

fn parse_string_length_call_expression(
    raw: &str,
    id_prefix: &str,
) -> Result<Option<Expression>, ShardLoomError> {
    let trimmed = raw.trim();
    if !trimmed
        .get(..6)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("length"))
        || trimmed.as_bytes().get(6) != Some(&b'(')
    {
        return Ok(None);
    }
    let open_index = "length".len();
    let close_index = matching_closing_parenthesis(trimmed, open_index)?.ok_or_else(|| {
        unsupported_sql_error("string length expressions must use LENGTH(<string-expression>)")
    })?;
    if !trimmed[close_index + 1..].trim().is_empty() {
        return Err(unsupported_sql_error(
            "string length expressions must be a single LENGTH function call",
        ));
    }
    let inner = trimmed[open_index + 1..close_index].trim();
    let args = split_sql_csv(inner)?;
    let [arg] = args.as_slice() else {
        return Err(unsupported_sql_error(
            "string length expressions require exactly one argument",
        ));
    };
    Ok(Some(Expression::new(
        ExprId::new(id_prefix.to_string())?,
        ExpressionKind::FunctionCall {
            name: "length".to_string(),
            args: vec![parse_string_scalar_expression(
                arg,
                &format!("{id_prefix}.arg"),
            )?],
        },
    )))
}

fn string_expression_literal_count(expression: &Expression) -> usize {
    match &expression.kind {
        ExpressionKind::Literal(_) => 1,
        ExpressionKind::Alias { expr, .. }
        | ExpressionKind::Cast { expr, .. }
        | ExpressionKind::TryCast { expr, .. }
        | ExpressionKind::Unary { expr, .. } => string_expression_literal_count(expr),
        ExpressionKind::Binary { left, right, .. }
        | ExpressionKind::Compare { left, right, .. } => {
            string_expression_literal_count(left) + string_expression_literal_count(right)
        }
        ExpressionKind::FunctionCall { args, .. } => {
            args.iter().map(string_expression_literal_count).sum()
        }
        ExpressionKind::List { values } => values.iter().map(string_expression_literal_count).sum(),
        ExpressionKind::Struct { fields } => fields
            .iter()
            .map(|(_name, expression)| string_expression_literal_count(expression))
            .sum(),
        ExpressionKind::Column(_)
        | ExpressionKind::RelationalValue { .. }
        | ExpressionKind::Unsupported { .. } => 0,
    }
}

fn parse_string_transform_projection(
    raw: &str,
) -> Result<Option<ParsedStringTransformProjection>, ShardLoomError> {
    let Some(as_index) = find_keyword_outside_quotes_and_parentheses(raw, "as")? else {
        return Ok(None);
    };
    let expression_raw = raw[..as_index].trim();
    let alias = raw[as_index + "as".len()..].trim();
    let Some((op, _)) = parse_string_transform_prefix(expression_raw) else {
        return Ok(None);
    };
    if alias.is_empty() {
        return Err(unsupported_sql_error(
            "string transform projections require an output alias",
        ));
    }
    validate_sql_identifier(alias)?;
    let expression = parse_string_scalar_expression(
        expression_raw,
        &format!("project.string_transform.{alias}"),
    )?;
    let source_columns = expression_source_columns(&expression);
    if source_columns.is_empty() {
        return Err(unsupported_sql_error(
            "string transform projections require at least one source column argument",
        ));
    }
    Ok(Some(ParsedStringTransformProjection {
        alias: alias.to_string(),
        expression,
        op,
        source_columns,
    }))
}

fn parse_string_length_projection(
    raw: &str,
) -> Result<Option<ParsedStringLengthProjection>, ShardLoomError> {
    let Some(as_index) = find_keyword_outside_quotes_and_parentheses(raw, "as")? else {
        return Ok(None);
    };
    let expression_raw = raw[..as_index].trim();
    let alias = raw[as_index + "as".len()..].trim();
    let Some(expression) = parse_string_length_call_expression(
        expression_raw,
        &format!("project.string_length.{alias}"),
    )?
    else {
        return Ok(None);
    };
    if alias.is_empty() {
        return Err(unsupported_sql_error(
            "string length projections require an output alias",
        ));
    }
    validate_sql_identifier(alias)?;
    let source_columns = expression_source_columns(&expression);
    if source_columns.is_empty() {
        return Err(unsupported_sql_error(
            "string length projections require at least one source column argument",
        ));
    }
    Ok(Some(ParsedStringLengthProjection {
        alias: alias.to_string(),
        expression,
        source_columns,
    }))
}

fn parse_string_function_projection(
    raw: &str,
) -> Result<Option<ParsedStringFunctionProjection>, ShardLoomError> {
    let Some(as_index) = find_keyword_outside_quotes_and_parentheses(raw, "as")? else {
        return Ok(None);
    };
    let expression_raw = raw[..as_index].trim();
    let alias = raw[as_index + "as".len()..].trim();
    let Some(call) = parse_string_function_call_expression(
        expression_raw,
        &format!("project.string_function.{alias}"),
    )?
    else {
        return Ok(None);
    };
    if alias.is_empty() {
        return Err(unsupported_sql_error(
            "string function projections require an output alias",
        ));
    }
    validate_sql_identifier(alias)?;
    if call.source_columns.is_empty() {
        return Err(unsupported_sql_error(
            "string function projections require at least one source column argument",
        ));
    }
    Ok(Some(ParsedStringFunctionProjection {
        alias: alias.to_string(),
        expression: call.expression,
        op: call.op,
        source_columns: call.source_columns,
        literal_count: call.literal_count,
    }))
}

fn parse_binary_helper_projection(
    raw: &str,
) -> Result<Option<ParsedBinaryHelperProjection>, ShardLoomError> {
    let Some(as_index) = find_keyword_outside_quotes_and_parentheses(raw, "as")? else {
        return Ok(None);
    };
    let expression_raw = raw[..as_index].trim();
    let alias = raw[as_index + "as".len()..].trim();
    let Some((op, open_index)) = parse_binary_helper_function_prefix(expression_raw) else {
        return Ok(None);
    };
    if alias.is_empty() {
        return Err(unsupported_sql_error(
            "binary helper projections require an output alias",
        ));
    }
    validate_sql_identifier(alias)?;
    let close_index = matching_closing_parenthesis(expression_raw, open_index)?.ok_or_else(|| {
        unsupported_sql_error(
            "binary helper projections must use UNHEX(<utf8-expression>) AS <column> or FROM_BASE64(<utf8-expression>) AS <column>",
        )
    })?;
    if !expression_raw[close_index + 1..].trim().is_empty() {
        return Err(unsupported_sql_error(
            "binary helper projections must be a single UNHEX/FROM_BASE64 call before AS",
        ));
    }
    let args = split_sql_csv(expression_raw[open_index + 1..close_index].trim())?;
    let [value_expression_raw] = args.as_slice() else {
        return Err(unsupported_sql_error(
            "binary helper projections require exactly one UTF-8 expression argument",
        ));
    };
    let expression = parse_string_scalar_expression(
        value_expression_raw,
        &format!("project.binary_helper_arg.{alias}"),
    )?;
    let source_columns = expression_source_columns(&expression);
    if source_columns.is_empty() {
        return Err(unsupported_sql_error(
            "binary helper projections require at least one source column argument",
        ));
    }
    Ok(Some(ParsedBinaryHelperProjection {
        alias: alias.to_string(),
        expression,
        source_columns,
        op,
    }))
}

fn parse_binary_byte_length_projection(
    raw: &str,
) -> Result<Option<ParsedBinaryByteLengthProjection>, ShardLoomError> {
    let Some(as_index) = find_keyword_outside_quotes_and_parentheses(raw, "as")? else {
        return Ok(None);
    };
    let expression_raw = raw[..as_index].trim();
    let alias = raw[as_index + "as".len()..].trim();
    let Some((expression, source_columns, argument_family)) =
        parse_binary_byte_length_call_expression(
            expression_raw,
            &format!("project.binary_byte_length.{alias}"),
        )?
    else {
        return Ok(None);
    };
    if alias.is_empty() {
        return Err(unsupported_sql_error(
            "binary byte length projections require an output alias",
        ));
    }
    validate_sql_identifier(alias)?;
    Ok(Some(ParsedBinaryByteLengthProjection {
        alias: alias.to_string(),
        expression,
        source_columns,
        argument_family,
    }))
}

fn parse_binary_helper_function_prefix(raw: &str) -> Option<(BinaryHelperOp, usize)> {
    let trimmed = raw.trim();
    for (name, op) in [
        ("from_base64", BinaryHelperOp::FromBase64),
        ("unhex", BinaryHelperOp::Unhex),
    ] {
        let len = name.len();
        if trimmed
            .get(..len)
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case(name))
            && trimmed.as_bytes().get(len) == Some(&b'(')
        {
            return Some((op, len));
        }
    }
    None
}

fn parse_binary_byte_length_function_prefix(raw: &str) -> Option<usize> {
    let trimmed = raw.trim();
    for name in ["octet_length", "byte_length"] {
        let len = name.len();
        if trimmed
            .get(..len)
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case(name))
            && trimmed.as_bytes().get(len) == Some(&b'(')
        {
            return Some(len);
        }
    }
    None
}

fn parse_binary_byte_length_call_expression(
    raw: &str,
    id_prefix: &str,
) -> Result<Option<(Expression, Vec<String>, BinaryByteLengthArgumentFamily)>, ShardLoomError> {
    let Some(open_index) = parse_binary_byte_length_function_prefix(raw) else {
        return Ok(None);
    };
    let close_index = matching_closing_parenthesis(raw, open_index)?.ok_or_else(|| {
        unsupported_sql_error(
            "binary byte length expressions must use BYTE_LENGTH(<binary-expression>) or OCTET_LENGTH(<binary-expression>)",
        )
    })?;
    if !raw[close_index + 1..].trim().is_empty() {
        return Err(unsupported_sql_error(
            "binary byte length expressions must be a single BYTE_LENGTH/OCTET_LENGTH call",
        ));
    }
    let args = split_sql_csv(raw[open_index + 1..close_index].trim())?;
    let [arg_raw] = args.as_slice() else {
        return Err(unsupported_sql_error(
            "binary byte length expressions require exactly one binary expression argument",
        ));
    };
    let (binary_expression, source_columns, argument_family) =
        parse_binary_scalar_expression(arg_raw, &format!("{id_prefix}.arg"))?;
    let expression = Expression::new(
        ExprId::new(id_prefix.to_string())?,
        ExpressionKind::FunctionCall {
            name: "byte_length".to_string(),
            args: vec![binary_expression],
        },
    );
    Ok(Some((expression, source_columns, argument_family)))
}

fn parse_binary_scalar_expression(
    raw: &str,
    id_prefix: &str,
) -> Result<(Expression, Vec<String>, BinaryByteLengthArgumentFamily), ShardLoomError> {
    let trimmed = raw.trim();
    if let Some((op, open_index)) = parse_binary_helper_function_prefix(trimmed) {
        let close_index = matching_closing_parenthesis(trimmed, open_index)?.ok_or_else(|| {
            unsupported_sql_error(
                "binary byte length helper arguments must use UNHEX(<utf8-expression>) or FROM_BASE64(<utf8-expression>)",
            )
        })?;
        if !trimmed[close_index + 1..].trim().is_empty() {
            return Err(unsupported_sql_error(
                "binary byte length helper arguments must be a single UNHEX/FROM_BASE64 call",
            ));
        }
        let args = split_sql_csv(trimmed[open_index + 1..close_index].trim())?;
        let [value_expression_raw] = args.as_slice() else {
            return Err(unsupported_sql_error(
                "binary byte length helper arguments require exactly one UTF-8 expression",
            ));
        };
        let value_expression = parse_string_scalar_expression(
            value_expression_raw,
            &format!("{id_prefix}.binary_helper_arg"),
        )?;
        let source_columns = expression_source_columns(&value_expression);
        if source_columns.is_empty() {
            return Err(unsupported_sql_error(
                "binary byte length helper arguments require at least one source column expression",
            ));
        }
        return Ok((
            Expression::new(
                ExprId::new(format!("{id_prefix}.binary_helper"))?,
                ExpressionKind::FunctionCall {
                    name: op.function_name().to_string(),
                    args: vec![value_expression],
                },
            ),
            source_columns,
            BinaryByteLengthArgumentFamily::Helper(op),
        ));
    }
    if let Some((mode, inner)) = parse_cast_call_expression(trimmed)? {
        let Some(as_index) = find_keyword_outside_quotes(inner, "as") else {
            return Err(unsupported_sql_error(
                "binary byte length CAST arguments must use CAST(<utf8-expression> AS binary) syntax",
            ));
        };
        let source_raw = inner[..as_index].trim();
        let target_raw = inner[as_index + "as".len()..].trim();
        let target_dtype = parse_cast_target_dtype(target_raw)?;
        if !matches!(target_dtype, LogicalDType::Binary) {
            return Err(unsupported_sql_error(
                "binary byte length CAST arguments must target binary, blob, or varbinary",
            ));
        }
        let (_column, source_expression, source_columns) = parse_cast_source_expression(
            source_raw,
            &target_dtype,
            &format!("{id_prefix}.cast_arg"),
            "binary byte length CAST arguments require at least one source column expression",
        )?;
        return Ok((
            mode.build_expression(
                ExprId::new(format!("{id_prefix}.cast"))?,
                source_expression,
                target_dtype,
            ),
            source_columns,
            BinaryByteLengthArgumentFamily::Cast(mode),
        ));
    }
    Err(unsupported_sql_error(
        "binary byte length expressions admit BYTE_LENGTH(UNHEX(<utf8-expression>)), BYTE_LENGTH(FROM_BASE64(<utf8-expression>)), or BYTE_LENGTH(CAST(<utf8-expression> AS binary)) only",
    ))
}

fn parse_binary_helper_predicate(raw: &str) -> Result<Option<ParsedPredicate>, ShardLoomError> {
    let trimmed = raw.trim();
    let Some((op, open_index)) = parse_binary_helper_function_prefix(trimmed) else {
        return Ok(None);
    };
    let close_index = matching_closing_parenthesis(trimmed, open_index)?.ok_or_else(|| {
        unsupported_sql_error(
            "binary helper predicates must use UNHEX(<utf8-expression>) <op> X'<hex>' or FROM_BASE64(<utf8-expression>) <op> X'<hex>'",
        )
    })?;
    let args = split_sql_csv(trimmed[open_index + 1..close_index].trim())?;
    let [value_expression_raw] = args.as_slice() else {
        return Err(unsupported_sql_error(
            "binary helper predicates require exactly one UTF-8 expression argument",
        ));
    };
    let expression =
        parse_string_scalar_expression(value_expression_raw, "where.binary_helper_arg")?;
    let source_columns = expression_source_columns(&expression);
    if source_columns.is_empty() {
        return Err(unsupported_sql_error(
            "binary helper predicates require at least one source column argument",
        ));
    }
    let tail = trimmed[close_index + 1..].trim();
    if tail.is_empty() {
        return Err(unsupported_sql_error(
            "binary helper predicates require a comparison operator and explicit binary literal",
        ));
    }
    let tokens = split_whitespace_outside_quotes(tail)?;
    let (comparison_raw, value) = match tokens.as_slice() {
        [comparison_raw, literal_raw] => (
            comparison_raw,
            parse_binary_helper_predicate_literal(literal_raw)?,
        ),
        [comparison_raw, binary_keyword, literal_raw] => {
            let bytes = parse_sql_binary_keyword_literal(binary_keyword, literal_raw)?.ok_or_else(
                || {
                    unsupported_sql_error(
                        "binary helper predicates admit X'<hex>', BINARY '<utf8>', or BLOB '<utf8>' literals only",
                    )
                },
            )?;
            (comparison_raw, bytes)
        }
        _ => {
            return Err(unsupported_sql_error(
                "binary helper predicates admit UNHEX(<utf8-expression>) <op> X'<hex>' or FROM_BASE64(<utf8-expression>) <op> BINARY/BLOB '<utf8>' only",
            ));
        }
    };
    Ok(Some(ParsedPredicate::BinaryHelperCompare(
        ParsedBinaryHelperPredicate {
            expression,
            source_columns,
            op,
            comparison: parse_comparison_op(comparison_raw)?,
            value,
        },
    )))
}

fn parse_binary_helper_predicate_literal(raw: &str) -> Result<Vec<u8>, ShardLoomError> {
    let trimmed = raw.trim();
    if is_sql_binary_hex_literal(trimmed) {
        return parse_sql_binary_hex_literal(trimmed);
    }
    Err(unsupported_sql_error(
        "binary helper predicates admit X'<hex>', BINARY '<utf8>', or BLOB '<utf8>' literals only",
    ))
}

fn parse_binary_byte_length_predicate(
    raw: &str,
) -> Result<Option<ParsedPredicate>, ShardLoomError> {
    let trimmed = raw.trim();
    let Some(open_index) = parse_binary_byte_length_function_prefix(trimmed) else {
        return Ok(None);
    };
    let close_index = matching_closing_parenthesis(trimmed, open_index)?.ok_or_else(|| {
        unsupported_sql_error(
            "binary byte length predicates must use BYTE_LENGTH(<binary-expression>) <op> <int-literal>",
        )
    })?;
    let expression_raw = trimmed[..=close_index].trim();
    let Some((expression, source_columns, argument_family)) =
        parse_binary_byte_length_call_expression(expression_raw, "where.binary_byte_length")?
    else {
        return Ok(None);
    };
    let tail = trimmed[close_index + 1..].trim();
    if tail.is_empty() {
        return Err(unsupported_sql_error(
            "binary byte length predicates require a comparison operator and int64 literal",
        ));
    }
    let tokens = split_whitespace_outside_quotes(tail)?;
    let [op_raw, literal_raw] = tokens.as_slice() else {
        return Err(unsupported_sql_error(
            "binary byte length predicates admit BYTE_LENGTH(<binary-expression>) <op> <int-literal> only",
        ));
    };
    let value @ ScalarValue::Int64(_) = parse_sql_literal(literal_raw)? else {
        return Err(unsupported_sql_error(
            "binary byte length predicates compare against int64 literals only",
        ));
    };
    Ok(Some(ParsedPredicate::BinaryByteLengthCompare(
        ParsedBinaryByteLengthPredicate {
            expression,
            source_columns,
            argument_family,
            comparison: parse_comparison_op(op_raw)?,
            value,
        },
    )))
}

fn parse_string_function_call_expression(
    raw: &str,
    id_prefix: &str,
) -> Result<Option<ParsedStringFunctionCall>, ShardLoomError> {
    let trimmed = raw.trim();
    let Some((op, open_index)) = parse_string_function_prefix(trimmed) else {
        return Ok(None);
    };
    let close_index = matching_closing_parenthesis(trimmed, open_index)?.ok_or_else(|| {
        unsupported_sql_error(
            "string function expressions must use CONCAT(...), SUBSTR|SUBSTRING(...), LEFT|RIGHT(...), or REPLACE(...)",
        )
    })?;
    if !trimmed[close_index + 1..].trim().is_empty() {
        return Err(unsupported_sql_error(
            "string function expressions must be a single function call",
        ));
    }
    let inner = trimmed[open_index + 1..close_index].trim();
    let args = split_sql_csv(inner)?;
    let parsed_args = parse_string_function_args(op, &args, id_prefix)?;
    Ok(Some(ParsedStringFunctionCall {
        expression: Expression::new(
            ExprId::new(id_prefix.to_string())?,
            ExpressionKind::FunctionCall {
                name: op.function_name().to_string(),
                args: parsed_args.expression_args,
            },
        ),
        op,
        source_columns: parsed_args.source_columns,
        literal_count: parsed_args.literal_count,
    }))
}

struct ParsedStringFunctionArgs {
    expression_args: Vec<Expression>,
    source_columns: Vec<String>,
    literal_count: usize,
}

fn parse_string_function_args(
    op: StringFunctionOp,
    args: &[String],
    id_prefix: &str,
) -> Result<ParsedStringFunctionArgs, ShardLoomError> {
    match op {
        StringFunctionOp::Concat => parse_concat_string_function_args(args, id_prefix),
        StringFunctionOp::Substr => parse_substr_string_function_args(args, id_prefix),
        StringFunctionOp::Left => parse_left_right_string_function_args(args, id_prefix, "LEFT"),
        StringFunctionOp::Right => parse_left_right_string_function_args(args, id_prefix, "RIGHT"),
        StringFunctionOp::Replace => parse_replace_string_function_args(args, id_prefix),
    }
}

fn parse_concat_string_function_args(
    args: &[String],
    id_prefix: &str,
) -> Result<ParsedStringFunctionArgs, ShardLoomError> {
    if args.len() < 2 {
        return Err(unsupported_sql_error(
            "CONCAT string function expressions require at least two arguments",
        ));
    }
    let mut source_columns = Vec::new();
    let mut literal_count = 0_usize;
    let mut expression_args = Vec::with_capacity(args.len());
    for (index, arg) in args.iter().enumerate() {
        let expression = parse_string_scalar_expression(arg, &format!("{id_prefix}.arg{index}"))?;
        push_unique_string_function_source_columns(
            &mut source_columns,
            expression_source_columns(&expression),
        );
        literal_count += string_expression_literal_count(&expression);
        expression_args.push(expression);
    }
    Ok(ParsedStringFunctionArgs {
        expression_args,
        source_columns,
        literal_count,
    })
}

fn parse_substr_string_function_args(
    args: &[String],
    id_prefix: &str,
) -> Result<ParsedStringFunctionArgs, ShardLoomError> {
    let [value_raw, start_raw, length_raw] = args else {
        return Err(unsupported_sql_error(
            "SUBSTR/SUBSTRING string function expressions require exactly three arguments: <column>, <start>, <length>",
        ));
    };
    let value_expression = parse_string_scalar_expression(value_raw, &format!("{id_prefix}.arg0"))?;
    let value_literal_count = string_expression_literal_count(&value_expression);
    let start = parse_string_function_int_literal(start_raw, "substring start")?;
    if start < 1 {
        return Err(unsupported_sql_error(
            "SUBSTR/SUBSTRING string function expressions require a 1-based start index >= 1",
        ));
    }
    let length = parse_string_function_int_literal(length_raw, "substring length")?;
    if length < 0 {
        return Err(unsupported_sql_error(
            "SUBSTR/SUBSTRING string function expressions require a non-negative length",
        ));
    }
    let mut source_columns = Vec::new();
    push_unique_string_function_source_columns(
        &mut source_columns,
        expression_source_columns(&value_expression),
    );
    if source_columns.is_empty() {
        return Err(unsupported_sql_error(
            "SUBSTR/SUBSTRING string function expressions require a source column first argument",
        ));
    }
    Ok(ParsedStringFunctionArgs {
        expression_args: vec![
            value_expression,
            Expression::literal(
                ExprId::new(format!("{id_prefix}.start"))?,
                ScalarValue::Int64(start),
            ),
            Expression::literal(
                ExprId::new(format!("{id_prefix}.length"))?,
                ScalarValue::Int64(length),
            ),
        ],
        source_columns,
        literal_count: value_literal_count + 2,
    })
}

fn parse_left_right_string_function_args(
    args: &[String],
    id_prefix: &str,
    function_name: &str,
) -> Result<ParsedStringFunctionArgs, ShardLoomError> {
    let [value_raw, count_raw] = args else {
        return Err(unsupported_sql_error(&format!(
            "{function_name} string function expressions require exactly two arguments: <column>, <count>"
        )));
    };
    let value_expression = parse_string_scalar_expression(value_raw, &format!("{id_prefix}.arg0"))?;
    let value_literal_count = string_expression_literal_count(&value_expression);
    let count = parse_string_function_int_literal(count_raw, "left/right count")?;
    if count < 0 {
        return Err(unsupported_sql_error(
            "LEFT/RIGHT string function expressions require a non-negative count",
        ));
    }
    let mut source_columns = Vec::new();
    push_unique_string_function_source_columns(
        &mut source_columns,
        expression_source_columns(&value_expression),
    );
    if source_columns.is_empty() {
        return Err(unsupported_sql_error(&format!(
            "{function_name} string function expressions require a source column first argument"
        )));
    }
    Ok(ParsedStringFunctionArgs {
        expression_args: vec![
            value_expression,
            Expression::literal(
                ExprId::new(format!("{id_prefix}.count"))?,
                ScalarValue::Int64(count),
            ),
        ],
        source_columns,
        literal_count: value_literal_count + 1,
    })
}

fn parse_replace_string_function_args(
    args: &[String],
    id_prefix: &str,
) -> Result<ParsedStringFunctionArgs, ShardLoomError> {
    let [value_raw, needle_raw, replacement_raw] = args else {
        return Err(unsupported_sql_error(
            "REPLACE string function expressions require exactly three arguments: <column>, <string-literal>, <string-literal>",
        ));
    };
    let value_expression = parse_string_scalar_expression(value_raw, &format!("{id_prefix}.arg0"))?;
    let value_literal_count = string_expression_literal_count(&value_expression);
    let needle = parse_sql_string_literal(needle_raw)?;
    if needle.is_empty() {
        return Err(unsupported_sql_error(
            "REPLACE string function expressions require a non-empty search literal",
        ));
    }
    let replacement = parse_sql_string_literal(replacement_raw)?;
    let mut source_columns = Vec::new();
    push_unique_string_function_source_columns(
        &mut source_columns,
        expression_source_columns(&value_expression),
    );
    if source_columns.is_empty() {
        return Err(unsupported_sql_error(
            "REPLACE string function expressions require a source column first argument",
        ));
    }
    Ok(ParsedStringFunctionArgs {
        expression_args: vec![
            value_expression,
            Expression::literal(
                ExprId::new(format!("{id_prefix}.needle"))?,
                ScalarValue::Utf8(needle),
            ),
            Expression::literal(
                ExprId::new(format!("{id_prefix}.replacement"))?,
                ScalarValue::Utf8(replacement),
            ),
        ],
        source_columns,
        literal_count: value_literal_count + 2,
    })
}

fn parse_string_function_prefix(raw: &str) -> Option<(StringFunctionOp, usize)> {
    [
        ("concat", StringFunctionOp::Concat),
        ("substr", StringFunctionOp::Substr),
        ("substring", StringFunctionOp::Substr),
        ("left", StringFunctionOp::Left),
        ("right", StringFunctionOp::Right),
        ("replace", StringFunctionOp::Replace),
    ]
    .into_iter()
    .find_map(|(name, op)| {
        raw.get(..name.len())
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case(name))
            .then_some(())
            .filter(|()| raw.as_bytes().get(name.len()) == Some(&b'('))
            .map(|()| (op, name.len()))
    })
}

fn parse_string_function_int_literal(raw: &str, label: &str) -> Result<i64, ShardLoomError> {
    match parse_sql_literal(raw)? {
        ScalarValue::Int64(value) => Ok(value),
        _ => Err(unsupported_sql_error(&format!(
            "{label} must be an int64 literal"
        ))),
    }
}

fn push_unique_string_function_source_columns(
    columns: &mut Vec<String>,
    source_columns: Vec<String>,
) {
    for column in source_columns {
        if !columns.iter().any(|candidate| candidate == &column) {
            columns.push(column);
        }
    }
}

fn parse_date_extract_projection(
    raw: &str,
) -> Result<Option<ParsedDateExtractProjection>, ShardLoomError> {
    let Some(as_index) = find_keyword_outside_quotes_and_parentheses(raw, "as")? else {
        return Ok(None);
    };
    let expression_raw = raw[..as_index].trim();
    let alias = raw[as_index + "as".len()..].trim();
    let Some((function_name, op)) = [
        ("date_year", DateExtractOp::Year),
        ("date_month", DateExtractOp::Month),
        ("date_day", DateExtractOp::Day),
    ]
    .into_iter()
    .find(|(name, _)| {
        expression_raw
            .get(..name.len())
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case(name))
            && expression_raw.as_bytes().get(name.len()) == Some(&b'(')
    }) else {
        return Ok(None);
    };
    if alias.is_empty() {
        return Err(unsupported_sql_error(
            "date extract projections require an output alias",
        ));
    }
    validate_sql_identifier(alias)?;
    let open_index = function_name.len();
    let close_index = matching_closing_parenthesis(expression_raw, open_index)?.ok_or_else(|| {
        unsupported_sql_error(
            "date extract projections must use DATE_YEAR(<column>) AS <column>, DATE_MONTH(<column>) AS <column>, or DATE_DAY(<column>) AS <column>",
        )
    })?;
    if !expression_raw[close_index + 1..].trim().is_empty() {
        return Err(unsupported_sql_error(
            "date extract projections must be a single DATE_YEAR/MONTH/DAY expression before AS",
        ));
    }
    let inner = expression_raw[open_index + 1..close_index].trim();
    if inner.is_empty() {
        return Err(unsupported_sql_error(
            "date extract projections require one source column argument",
        ));
    }
    Ok(Some(ParsedDateExtractProjection {
        alias: alias.to_string(),
        column: parse_date_arithmetic_column_arg(inner)?,
        op,
    }))
}

fn parse_timestamp_extract_projection(
    raw: &str,
) -> Result<Option<ParsedTimestampExtractProjection>, ShardLoomError> {
    let Some(as_index) = find_keyword_outside_quotes_and_parentheses(raw, "as")? else {
        return Ok(None);
    };
    let expression_raw = raw[..as_index].trim();
    let alias = raw[as_index + "as".len()..].trim();
    let Some((function_name, op)) = [
        ("timestamp_year", TimestampExtractOp::Year),
        ("timestamp_month", TimestampExtractOp::Month),
        ("timestamp_day", TimestampExtractOp::Day),
        ("timestamp_hour", TimestampExtractOp::Hour),
        ("timestamp_minute", TimestampExtractOp::Minute),
        ("timestamp_second", TimestampExtractOp::Second),
    ]
    .into_iter()
    .find(|(name, _)| {
        expression_raw
            .get(..name.len())
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case(name))
            && expression_raw.as_bytes().get(name.len()) == Some(&b'(')
    }) else {
        return Ok(None);
    };
    if alias.is_empty() {
        return Err(unsupported_sql_error(
            "timestamp extract projections require an output alias",
        ));
    }
    validate_sql_identifier(alias)?;
    let open_index = function_name.len();
    let close_index = matching_closing_parenthesis(expression_raw, open_index)?.ok_or_else(|| {
        unsupported_sql_error(
            "timestamp extract projections must use TIMESTAMP_YEAR/MONTH/DAY/HOUR/MINUTE/SECOND(<column>) AS <column>",
        )
    })?;
    if !expression_raw[close_index + 1..].trim().is_empty() {
        return Err(unsupported_sql_error(
            "timestamp extract projections must be a single TIMESTAMP_YEAR/MONTH/DAY/HOUR/MINUTE/SECOND expression before AS",
        ));
    }
    let inner = expression_raw[open_index + 1..close_index].trim();
    if inner.is_empty() {
        return Err(unsupported_sql_error(
            "timestamp extract projections require one source column argument",
        ));
    }
    Ok(Some(ParsedTimestampExtractProjection {
        alias: alias.to_string(),
        column: parse_timestamp_extract_column_arg(inner)?,
        op,
    }))
}

fn parse_aggregate_projection(raw: &str) -> Result<Option<ParsedAggregate>, ShardLoomError> {
    let (expression_raw, alias_raw) =
        if let Some(as_index) = find_keyword_outside_quotes_and_parentheses(raw, "as")? {
            let expression_raw = raw[..as_index].trim();
            let alias_raw = raw[as_index + "as".len()..].trim();
            (expression_raw, Some(alias_raw))
        } else {
            (raw.trim(), None)
        };
    let Some(open_index) = expression_raw.find('(') else {
        return Ok(None);
    };
    let function_raw = expression_raw[..open_index].trim();
    let Some(function) = AggregateFunction::parse(function_raw) else {
        return Ok(None);
    };
    let alias = if let Some(alias_raw) = alias_raw {
        if alias_raw.is_empty() {
            return Err(unsupported_sql_error(
                "aggregate aliases must use AS <column> with a non-empty output name",
            ));
        }
        validate_sql_identifier(alias_raw)?;
        Some(alias_raw.to_string())
    } else {
        None
    };
    let close_index =
        matching_closing_parenthesis(expression_raw, open_index)?.ok_or_else(|| {
            unsupported_sql_error("aggregate expression requires closing parentheses")
        })?;
    let filter = parse_aggregate_filter(expression_raw[close_index + 1..].trim())?;
    let argument = expression_raw[open_index + 1..close_index].trim();
    if argument.is_empty() {
        return Err(unsupported_sql_error(
            "aggregate expressions require one scalar argument or COUNT(*)",
        ));
    }
    let (argument, order_by) = parse_aggregate_order(function, argument)?;
    let (distinct, argument) = parse_aggregate_distinct(function, argument)?;
    let arguments = split_sql_csv(argument)?;
    let expected = if function.requires_secondary()
        || function.requires_quantile_level()
        || function.requires_separator()
    {
        2
    } else {
        1
    };
    let decimal_output = parse_aggregate_decimal_output(function, &arguments, expected)?;
    if arguments.len() != expected && decimal_output.is_none() {
        return Err(unsupported_sql_error(&format!(
            "{} requires exactly {expected} aggregate arguments",
            function.as_str()
        )));
    }
    let argument = arguments[0].as_str();
    let quantile_level = function
        .requires_quantile_level()
        .then(|| parse_quantile_level(&arguments[1]))
        .transpose()?;
    let separator = function
        .requires_separator()
        .then(|| parse_sql_string_literal(&arguments[1]))
        .transpose()?;
    let argument = if argument == "*" {
        if distinct {
            return Err(unsupported_sql_error(
                "COUNT(DISTINCT *) is not admitted; use COUNT(DISTINCT <column>)",
            ));
        }
        if function != AggregateFunction::Count {
            return Err(unsupported_sql_error(
                "only COUNT(*) is admitted in this scoped aggregate smoke",
            ));
        }
        ParsedAggregateArgument::All
    } else {
        parse_aggregate_argument(argument)?
    };
    Ok(Some(ParsedAggregate {
        function,
        argument,
        secondary_argument: if function.requires_secondary() {
            Some(parse_aggregate_argument(&arguments[1])?)
        } else {
            None
        },
        quantile_level,
        decimal_output,
        filter,
        order_by,
        alias,
        distinct,
        separator,
    }))
}

fn parse_aggregate_decimal_output(
    function: AggregateFunction,
    arguments: &[String],
    expected: usize,
) -> Result<Option<shardloom_core::decimal_output::DecimalOutput>, ShardLoomError> {
    if arguments.len() != expected + 3 {
        return Ok(None);
    }
    if !matches!(
        function,
        AggregateFunction::Sum
            | AggregateFunction::Avg
            | AggregateFunction::Median
            | AggregateFunction::QuantileCont
    ) {
        return Err(unsupported_sql_error(
            "explicit decimal output requires SUM, AVG, MEDIAN or QUANTILE_CONT",
        ));
    }
    parse_decimal_output(&arguments[expected..]).map(Some)
}

fn parse_decimal_output(
    arguments: &[String],
) -> Result<shardloom_core::decimal_output::DecimalOutput, ShardLoomError> {
    use shardloom_core::decimal_output::{DecimalOutput, DecimalRounding};
    let [precision, scale, rounding] = arguments else {
        return Err(unsupported_sql_error(
            "decimal output requires literal precision, scale and rounding",
        ));
    };
    let integer = |value: &str| {
        value.trim().parse::<u8>().map_err(|_| {
            unsupported_sql_error("decimal precision and scale require integer literals in 0..38")
        })
    };
    let rounding = DecimalRounding::parse(&parse_sql_string_literal(rounding)?)
        .map_err(|error| unsupported_sql_error(&error.to_string()))?;
    DecimalOutput::new(integer(precision)?, integer(scale)?, rounding)
        .map_err(|error| unsupported_sql_error(&error.to_string()))
}

fn aggregate_expression_end(raw: &str, open: usize) -> Result<usize, ShardLoomError> {
    let close = matching_closing_parenthesis(raw, open)?
        .ok_or_else(|| unsupported_sql_error("aggregate function requires closing parentheses"))?;
    let tail = raw[close + 1..].trim_start();
    let Some(filter) = strip_leading_keyword(tail, "filter") else {
        return Ok(close);
    };
    let filter = filter.trim_start();
    if !filter.starts_with('(') {
        return Err(unsupported_sql_error(
            "aggregate FILTER requires (WHERE predicate)",
        ));
    }
    let filter_open = raw.len() - filter.len();
    matching_closing_parenthesis(raw, filter_open)?
        .ok_or_else(|| unsupported_sql_error("aggregate FILTER requires closing parentheses"))
}

fn parse_aggregate_filter(raw: &str) -> Result<Option<ParsedAggregateArgument>, ShardLoomError> {
    if raw.is_empty() {
        return Ok(None);
    }
    let tail = strip_leading_keyword(raw, "filter")
        .ok_or_else(|| unsupported_sql_error("aggregate suffix must be FILTER (WHERE predicate)"))?
        .trim();
    if !tail.starts_with('(')
        || matching_closing_parenthesis(tail, 0)? != Some(tail.len().saturating_sub(1))
    {
        return Err(unsupported_sql_error(
            "aggregate FILTER requires (WHERE predicate)",
        ));
    }
    let predicate = strip_leading_keyword(tail[1..tail.len() - 1].trim(), "where")
        .ok_or_else(|| unsupported_sql_error("aggregate FILTER requires WHERE"))?
        .trim();
    if predicate.is_empty() {
        return Err(unsupported_sql_error(
            "aggregate FILTER requires a Boolean predicate",
        ));
    }
    parse_aggregate_argument(predicate).map(Some)
}

fn parse_aggregate_order(
    function: AggregateFunction,
    argument: &str,
) -> Result<(&str, Option<ParsedOrderBy>), ShardLoomError> {
    let Some(index) = find_keyword_outside_quotes_and_parentheses(argument, "order by")? else {
        return Ok((argument, None));
    };
    if !function.is_selector()
        && !matches!(
            function,
            AggregateFunction::List | AggregateFunction::StringAgg
        )
    {
        return Err(unsupported_sql_error(
            "aggregate-local ORDER BY is supported for FIRST, LAST, ARG_MIN, ARG_MAX, LIST, ARRAY_AGG and STRING_AGG",
        ));
    }
    Ok((
        argument[..index].trim(),
        parse_order_by(Some(argument[index + "order by".len()..].trim()))?,
    ))
}

fn parse_aggregate_distinct(
    function: AggregateFunction,
    argument: &str,
) -> Result<(bool, &str), ShardLoomError> {
    let Some(argument) = strip_leading_keyword(argument, "distinct") else {
        return Ok((false, argument));
    };
    function.native_name(true)?;
    let argument = argument.trim();
    if argument.is_empty() {
        return Err(unsupported_sql_error(
            "DISTINCT aggregate requires one scalar argument",
        ));
    }
    Ok((true, argument))
}

fn parse_quantile_level(raw: &str) -> Result<VortexQuantileLevel, ShardLoomError> {
    let level = match parse_sql_literal(raw)? {
        ScalarValue::Float64(value) => value,
        ScalarValue::Int64(0) | ScalarValue::UInt64(0) => 0.0,
        ScalarValue::Int64(1) | ScalarValue::UInt64(1) => 1.0,
        _ => {
            return Err(unsupported_sql_error(
                "quantile level must be a finite numeric literal between 0 and 1 inclusive",
            ));
        }
    };
    VortexQuantileLevel::new(level).map_err(|error| unsupported_sql_error(&error.to_string()))
}

fn parse_aggregate_argument(argument: &str) -> Result<ParsedAggregateArgument, ShardLoomError> {
    let expression = scalar_expression::parse(argument, "aggregate.argument")?;
    Ok(if let ExpressionKind::Column(column) = &expression.kind {
        ParsedAggregateArgument::Column(column.as_str().to_owned())
    } else {
        ParsedAggregateArgument::Computed {
            raw: argument.to_owned(),
            source_columns: expression_source_columns(&expression),
            expression: Box::new(expression),
        }
    })
}

fn parse_group_by_list(
    raw: Option<&str>,
    projections: &ParsedProjectionList,
) -> Result<Vec<String>, ShardLoomError> {
    let Some(raw) = raw else {
        return Ok(Vec::new());
    };
    if raw.trim().is_empty() {
        return Err(unsupported_sql_error("GROUP BY columns must not be empty"));
    }
    let columns = split_sql_csv(raw)?;
    if columns.is_empty() {
        return Err(unsupported_sql_error("GROUP BY columns must not be empty"));
    }
    let mut parsed = Vec::with_capacity(columns.len());
    for mut column in columns {
        if let Ok(ordinal) = column.parse::<usize>() {
            let output = ordinal
                .checked_sub(1)
                .and_then(|index| projections.projection_order.get(index))
                .ok_or_else(|| {
                    unsupported_sql_error("GROUP BY ordinal is outside the SELECT list")
                })?;
            column = match output {
                ParsedProjectionOutput::Raw(name) if name != "*" => name.clone(),
                ParsedProjectionOutput::Aggregate(_) | ParsedProjectionOutput::Window(_) => {
                    return Err(unsupported_sql_error(
                        "GROUP BY ordinal cannot reference an aggregate or window",
                    ));
                }
                output => output.computed_alias().map(str::to_owned).ok_or_else(|| {
                    unsupported_sql_error("GROUP BY ordinal requires a named SELECT expression")
                })?,
            };
        }
        if let Some(alias) = selected_expression_alias(&column, projections) {
            column = alias;
        }
        validate_sql_column_ref(&column)?;
        if parsed.iter().any(|existing| existing == &column) {
            return Err(unsupported_sql_error(
                "GROUP BY duplicate columns are not admitted in this scoped smoke",
            ));
        }
        parsed.push(column);
    }
    Ok(parsed)
}

fn selected_additive_alias(raw: &str, projections: &ParsedProjectionList) -> Option<String> {
    let expression = parse_unaliased_additive_projection(raw)?;
    projections
        .numeric_arithmetic_projections
        .iter()
        .find(|projection| {
            projection.column == expression.column
                && projection.op == expression.op
                && projection.rhs == expression.rhs
        })
        .map(|projection| projection.alias.clone())
        .or_else(|| {
            projections.generic_expression_projections.iter().find_map(|projection| {
                let ExpressionKind::Binary { left, op, right } = &projection.expression.kind else {
                    return None;
                };
                (matches!(&left.kind, ExpressionKind::Column(column) if column.as_str() == expression.column)
                    && *op == expression.op.binary_op()
                    && matches!(&right.kind, ExpressionKind::Literal(value) if value == &expression.rhs))
                    .then(|| projection.alias.clone())
            })
        })
}

fn selected_expression_alias(raw: &str, projections: &ParsedProjectionList) -> Option<String> {
    selected_additive_alias(raw, projections).or_else(|| {
        if !scalar_expression::composed(raw).ok()? {
            return None;
        }
        projections
            .generic_expression_projections
            .iter()
            .find_map(|projection| {
                // Reuse the selected expression's ID namespace so equality compares
                // complete parsed declarations, including constants and bindings.
                let expression =
                    scalar_expression::parse(raw, &format!("project.generic.{}", projection.alias))
                        .ok()?;
                (expression == projection.expression).then(|| projection.alias.clone())
            })
    })
}

/// Resolve selected scalar and aggregate expressions to their bound names.
/// Unprojected ORDER BY aggregates share the existing hidden HAVING reductions.
/// Direction/null syntax and duplicate keys still use the ordinary order parser.
fn parse_selected_order_by(
    raw: Option<&str>,
    projections: &ParsedProjectionList,
    hidden: &mut Vec<ParsedAggregate>,
) -> Result<Option<ParsedOrderBy>, ShardLoomError> {
    let Some(raw) = raw else {
        return Ok(None);
    };
    let mut entries = Vec::new();
    for entry in split_sql_csv(raw)? {
        let tokens = split_whitespace_outside_quotes(&entry)?;
        let mut end = tokens.len();
        if end >= 2 && tokens[end - 2].eq_ignore_ascii_case("nulls") {
            end -= 2;
        }
        if end > 0
            && ["asc", "desc"]
                .iter()
                .any(|word| tokens[end - 1].eq_ignore_ascii_case(word))
        {
            end -= 1;
        }
        let expression = tokens[..end].join(" ");
        let mut alias = selected_expression_alias(&expression, projections);
        if alias.is_none() && parse_aggregate_projection(&expression)?.is_some() {
            let selected = projections
                .aggregates
                .iter()
                .chain(hidden.iter())
                .cloned()
                .collect::<Vec<_>>();
            let mut reserved = reserved_having_aggregate_aliases(projections);
            reserved.extend(hidden.iter().map(ParsedAggregate::output_name));
            let (name, additions) =
                rewrite_having_aggregate_predicate(&expression, &selected, reserved)?;
            hidden.extend(additions);
            alias = Some(name);
        }
        entries.push(alias.map_or(entry, |alias| {
            format!("{alias} {}", tokens[end..].join(" "))
        }));
    }
    parse_order_by(Some(&entries.join(",")))
}

fn parse_order_by(raw: Option<&str>) -> Result<Option<ParsedOrderBy>, ShardLoomError> {
    let Some(raw) = raw else {
        return Ok(None);
    };
    if raw.trim().is_empty() {
        return Err(unsupported_sql_error("ORDER BY clause must not be empty"));
    }
    let entries = split_sql_csv(raw)?;
    if entries.is_empty() {
        return Err(unsupported_sql_error("ORDER BY clause must not be empty"));
    }
    let mut keys = Vec::with_capacity(entries.len());
    for entry in entries {
        let tokens = split_whitespace_outside_quotes(&entry)?;
        let (column, direction, null_ordering) = match tokens.as_slice() {
            [column] => (column, SortDirection::Asc, None),
            [column, direction] if direction.eq_ignore_ascii_case("asc") => {
                (column, SortDirection::Asc, None)
            }
            [column, direction] if direction.eq_ignore_ascii_case("desc") => {
                (column, SortDirection::Desc, None)
            }
            [column, nulls, position] if nulls.eq_ignore_ascii_case("nulls") => (
                column,
                SortDirection::Asc,
                parse_sort_null_ordering(position)?,
            ),
            [column, direction, nulls, position]
                if nulls.eq_ignore_ascii_case("nulls") && direction.eq_ignore_ascii_case("asc") =>
            {
                (
                    column,
                    SortDirection::Asc,
                    parse_sort_null_ordering(position)?,
                )
            }
            [column, direction, nulls, position]
                if nulls.eq_ignore_ascii_case("nulls")
                    && direction.eq_ignore_ascii_case("desc") =>
            {
                (
                    column,
                    SortDirection::Desc,
                    parse_sort_null_ordering(position)?,
                )
            }
            _ => {
                return Err(unsupported_sql_error(
                    "ORDER BY top-N smoke admits <column> [ASC|DESC] [NULLS FIRST|LAST] keys only",
                ));
            }
        };
        validate_sql_column_ref(column)?;
        if keys
            .iter()
            .any(|existing: &ParsedOrderKey| existing.column == *column)
        {
            return Err(unsupported_sql_error(
                "ORDER BY duplicate sort keys are not admitted in this scoped top-N smoke",
            ));
        }
        keys.push(ParsedOrderKey {
            column: column.clone(),
            direction,
            null_ordering,
        });
    }
    Ok(Some(ParsedOrderBy { keys }))
}

fn parse_sort_null_ordering(raw: &str) -> Result<Option<SortNullOrdering>, ShardLoomError> {
    if raw.eq_ignore_ascii_case("first") {
        Ok(Some(SortNullOrdering::First))
    } else if raw.eq_ignore_ascii_case("last") {
        Ok(Some(SortNullOrdering::Last))
    } else {
        Err(unsupported_sql_error(
            "ORDER BY NULLS clause must use FIRST or LAST in this scoped top-N smoke",
        ))
    }
}

fn parse_source_clause(raw: &str) -> Result<ParsedSourceClause, ShardLoomError> {
    relation_sources::parse_source_clause(raw)
}

fn find_join_keyword(raw: &str) -> Result<Option<(usize, usize, ParsedJoinType)>, ShardLoomError> {
    let mut best = None;
    for (keyword, join_type) in [
        ("left outer join", ParsedJoinType::LeftOuterEqui),
        ("left join", ParsedJoinType::LeftOuterEqui),
        ("right outer join", ParsedJoinType::RightOuterEqui),
        ("right join", ParsedJoinType::RightOuterEqui),
        ("full outer join", ParsedJoinType::FullOuterEqui),
        ("full join", ParsedJoinType::FullOuterEqui),
        ("left semi join", ParsedJoinType::LeftSemiEqui),
        ("semi join", ParsedJoinType::LeftSemiEqui),
        ("left anti join", ParsedJoinType::LeftAntiEqui),
        ("anti join", ParsedJoinType::LeftAntiEqui),
        ("cross join", ParsedJoinType::Cross),
        ("inner join", ParsedJoinType::InnerEqui),
        ("join", ParsedJoinType::InnerEqui),
    ] {
        let Some(index) = find_keyword_outside_quotes_and_parentheses(raw, keyword)? else {
            continue;
        };
        let candidate = (index, keyword.len(), join_type);
        if best.is_none_or(|(best_index, best_len, _)| {
            index < best_index || (index == best_index && keyword.len() > best_len)
        }) {
            best = Some(candidate);
        }
    }
    Ok(best)
}

fn parse_join_on(raw: &str) -> Result<ParsedJoinOn, ShardLoomError> {
    parse_join_on_complex_key_blocker(raw)?;
    match parse_join_on_key_pairs(raw) {
        Ok(key_pairs) => Ok(ParsedJoinOn {
            key_pairs,
            predicate: None,
            predicate_family: ParsedJoinOnPredicateFamily::EquiKeys,
        }),
        Err(key_error) => {
            let predicate = parse_join_on_predicate(raw)?;
            if predicate.columns().is_empty() {
                return Err(key_error);
            }
            let predicate_family = join_on_predicate_family(&predicate);
            Ok(ParsedJoinOn {
                key_pairs: Vec::new(),
                predicate: Some(predicate),
                predicate_family,
            })
        }
    }
}

fn parse_join_on_complex_key_blocker(raw: &str) -> Result<(), ShardLoomError> {
    if contains_array_literal_outside_quotes(raw)
        || contains_function_call_outside_quotes(raw, "struct")
        || contains_function_call_outside_quotes(raw, "row")
    {
        return Err(unsupported_sql_error(
            "JOIN ON complex key expressions are not admitted by the current join profile; use scalar qualified columns or admitted scalar expression predicates",
        ));
    }
    Ok(())
}

fn parse_join_on_key_pairs(raw: &str) -> Result<Vec<ParsedJoinKeyPair>, ShardLoomError> {
    let tokens = split_whitespace_outside_quotes(raw)?;
    if tokens.len() < 3 {
        return Err(unsupported_sql_error(
            "JOIN smoke ON clause must be <left_alias>.<column> = <right_alias>.<column>",
        ));
    }
    let mut key_pairs = Vec::new();
    let mut index = 0;
    loop {
        if index + 2 >= tokens.len() {
            return Err(unsupported_sql_error(
                "JOIN smoke ON clause must be one or more equi-join predicates joined by AND",
            ));
        }
        let left = &tokens[index];
        let op = &tokens[index + 1];
        let right = &tokens[index + 2];
        if op != "=" {
            return Err(unsupported_sql_error(
                "JOIN smoke admits equi-join ON predicates only",
            ));
        }
        key_pairs.push(ParsedJoinKeyPair {
            left: parse_qualified_column_ref(left)?,
            right: parse_qualified_column_ref(right)?,
        });
        index += 3;
        if index == tokens.len() {
            break;
        }
        if !tokens[index].eq_ignore_ascii_case("and") {
            return Err(unsupported_sql_error(
                "JOIN smoke ON clause must be one or more equi-join predicates joined by AND",
            ));
        }
        index += 1;
    }
    Ok(key_pairs)
}

fn parse_join_on_predicate(raw: &str) -> Result<ParsedPredicate, ShardLoomError> {
    let raw = trim_enclosing_predicate_parentheses(raw)?;
    if let Some(predicate) = parse_join_on_logical_predicate(raw)? {
        return Ok(predicate);
    }
    if let Some(predicate) = parse_join_on_column_compare_predicate(raw)? {
        return Ok(predicate);
    }
    if let Some(predicate) = parse_generic_expression_predicate(raw)? {
        return Ok(predicate);
    }
    // A qualified column compared with a literal is an ON residual, evaluated
    // before outer null extension just like a column-to-column comparison.
    parse_token_predicate(raw)
}

fn parse_join_on_logical_predicate(raw: &str) -> Result<Option<ParsedPredicate>, ShardLoomError> {
    if let Some(or_index) = find_keyword_outside_quotes_and_parentheses(raw, "or")? {
        return parse_join_on_logical_binary_predicate(raw, or_index, "or", LogicalPredicateOp::Or)
            .map(Some);
    }
    if let Some(and_index) = find_keyword_outside_quotes_and_parentheses(raw, "and")? {
        return parse_join_on_logical_binary_predicate(
            raw,
            and_index,
            "and",
            LogicalPredicateOp::And,
        )
        .map(Some);
    }
    Ok(None)
}

fn parse_join_on_logical_binary_predicate(
    raw: &str,
    keyword_index: usize,
    keyword: &str,
    op: LogicalPredicateOp,
) -> Result<ParsedPredicate, ShardLoomError> {
    let left_raw = raw[..keyword_index].trim();
    let right_raw = raw[keyword_index + keyword.len()..].trim();
    if left_raw.is_empty() || right_raw.is_empty() {
        return Err(unsupported_sql_error(
            "JOIN ON logical predicates require non-empty predicates on both sides",
        ));
    }
    Ok(ParsedPredicate::Logical {
        op,
        left: Box::new(parse_join_on_predicate(left_raw)?),
        right: Box::new(parse_join_on_predicate(right_raw)?),
    })
}

fn parse_join_on_column_compare_predicate(
    raw: &str,
) -> Result<Option<ParsedPredicate>, ShardLoomError> {
    let tokens = split_whitespace_outside_quotes(raw)?;
    let [left, op_raw, right] = tokens.as_slice() else {
        return Ok(None);
    };
    if parse_sql_literal(left).is_ok() || parse_sql_literal(right).is_ok() {
        return Ok(None);
    }
    validate_sql_column_ref(left)?;
    validate_sql_column_ref(right)?;
    Ok(Some(ParsedPredicate::ColumnCompare {
        left_column: left.clone(),
        op: parse_comparison_op(op_raw)?,
        right_column: right.clone(),
    }))
}

fn join_on_predicate_family(predicate: &ParsedPredicate) -> ParsedJoinOnPredicateFamily {
    match predicate {
        ParsedPredicate::ColumnCompare { .. } => ParsedJoinOnPredicateFamily::ColumnCompare,
        ParsedPredicate::GenericExpressionCompare { .. } => {
            ParsedJoinOnPredicateFamily::GenericExpression
        }
        ParsedPredicate::Logical { .. } => ParsedJoinOnPredicateFamily::Logical,
        _ => ParsedJoinOnPredicateFamily::GenericExpression,
    }
}

fn parse_source_path(raw: &str) -> Result<PathBuf, ShardLoomError> {
    let path = if raw.starts_with('\'') {
        parse_sql_string_literal(raw)?
    } else {
        if raw.split_whitespace().count() != 1 {
            return Err(unsupported_sql_error(
                "FROM source must be a single local CSV/JSONL/JSON/Parquet/Arrow IPC/Avro/ORC path or single-quoted path",
            ));
        }
        raw.to_string()
    };
    let path = PathBuf::from(path);
    Ok(path)
}

fn parse_predicate(raw: &str) -> Result<ParsedPredicate, ShardLoomError> {
    let raw = trim_enclosing_predicate_parentheses(raw)?;
    if raw.eq_ignore_ascii_case("true") {
        return Ok(ParsedPredicate::All);
    }
    if raw.eq_ignore_ascii_case("false") {
        return Ok(ParsedPredicate::Not {
            inner: Box::new(ParsedPredicate::All),
        });
    }
    if let Some(predicate) = parse_logical_predicate(raw)? {
        return Ok(predicate);
    }
    if let Some(predicate) = parse_quantified_subquery_predicate(raw)? {
        return Ok(predicate);
    }
    if let Some(predicate) = parse_exists_subquery_predicate(raw)? {
        return Ok(predicate);
    }
    // A nested SELECT owns its operators; '*' inside an IN relation is not
    // multiplication in the enclosing scalar predicate.
    if let Some(predicate) = parse_in_list_predicate(raw)? {
        return Ok(predicate);
    }
    if let Some(predicate) = parse_scalar_null_or_boolean_predicate(raw)? {
        return Ok(predicate);
    }
    if let Some(predicate) = parse_null_safe_comparison_predicate(raw)? {
        return Ok(predicate);
    }
    if let Some(predicate) = parse_between_predicate(raw)? {
        return Ok(predicate);
    }
    if let Some(predicate) = parse_date_extract_predicate(raw)? {
        return Ok(predicate);
    }
    if let Some(predicate) = parse_timestamp_extract_predicate(raw)? {
        return Ok(predicate);
    }
    if let Some(predicate) = parse_date_arithmetic_predicate(raw)? {
        return Ok(predicate);
    }
    if let Some(predicate) = parse_timestamp_arithmetic_predicate(raw)? {
        return Ok(predicate);
    }
    if let Some(predicate) = parse_cast_predicate(raw)? {
        return Ok(predicate);
    }
    if let Some(predicate) = parse_generic_expression_predicate(raw)? {
        return Ok(predicate);
    }
    if let Some(predicate) = parse_numeric_arithmetic_predicate(raw)? {
        return Ok(predicate);
    }
    if let Some(predicate) = parse_numeric_abs_predicate(raw)? {
        return Ok(predicate);
    }
    if let Some(predicate) = parse_numeric_rounding_predicate(raw)? {
        return Ok(predicate);
    }
    if let Some(predicate) = parse_string_length_predicate(raw)? {
        return Ok(predicate);
    }
    if let Some(predicate) = parse_string_transform_predicate(raw)? {
        return Ok(predicate);
    }
    if let Some(predicate) = parse_string_function_predicate(raw)? {
        return Ok(predicate);
    }
    if let Some(predicate) = parse_binary_helper_predicate(raw)? {
        return Ok(predicate);
    }
    if let Some(predicate) = parse_binary_byte_length_predicate(raw)? {
        return Ok(predicate);
    }
    if let Some(predicate) = parse_regex_function_predicate(raw)? {
        return Ok(predicate);
    }
    parse_quantified_subquery_blocker(raw)?;
    parse_token_predicate(raw)
}

enum NullSafeComparisonRhs {
    Literal(ScalarValue),
    Column(String),
}

/// Scalar null tests and boolean expressions use the shared expression binder.
/// Simple column predicates keep their metadata and encoded predicate strategy.
fn parse_scalar_null_or_boolean_predicate(
    raw: &str,
) -> Result<Option<ParsedPredicate>, ShardLoomError> {
    let expression = if let Some(index) = find_keyword_outside_quotes_and_parentheses(raw, "is")? {
        let source = raw[..index].trim();
        let tail = raw[index + 2..].split_whitespace().collect::<Vec<_>>();
        let op = match tail.as_slice() {
            [null] if null.eq_ignore_ascii_case("null") => UnaryOp::IsNull,
            [not, null] if not.eq_ignore_ascii_case("not") && null.eq_ignore_ascii_case("null") => {
                UnaryOp::IsNotNull
            }
            _ => return Ok(None),
        };
        if validate_sql_column_ref(source).is_ok() && parse_sql_literal(source).is_err() {
            return Ok(None);
        }
        let mut expression = parse_numeric_scalar_expression(source, "where.scalar.null_arg")?;
        expression.expression = Expression::new(
            ExprId::new("where.scalar.null_test")?,
            ExpressionKind::Unary {
                op,
                expr: Box::new(expression.expression),
            },
        );
        expression
    } else {
        if find_top_level_comparison_operator(raw)?.is_some()
            || !(scalar_expression::composed(raw)? || parse_cast_call_expression(raw)?.is_some())
        {
            return Ok(None);
        }
        parse_numeric_scalar_expression(raw, "where.scalar.boolean")?
    };
    let right = Expression::literal(
        ExprId::new("where.scalar.true")?,
        ScalarValue::Boolean(true),
    );
    let source_columns = expression_pair_source_columns(&expression, &right);
    let operator_families = expression_pair_operator_families(&expression, &right);
    let binary_operator_count = expression_binary_operator_count(&expression);
    Ok(Some(ParsedPredicate::GenericExpressionCompare {
        left: Box::new(expression),
        comparison: ComparisonOp::Eq,
        right: Box::new(right.into()),
        source_columns,
        operator_families,
        binary_operator_count,
    }))
}

fn parse_null_safe_comparison_predicate(
    raw: &str,
) -> Result<Option<ParsedPredicate>, ShardLoomError> {
    let tokens = split_whitespace_outside_quotes(raw)?;
    let Some(is_index) = tokens
        .iter()
        .position(|token| token.eq_ignore_ascii_case("is"))
    else {
        return Ok(None);
    };
    if is_index != 1 {
        return Ok(None);
    }
    let (is_distinct, rhs_start) = match tokens[is_index + 1..] {
        [ref distinct_keyword, ref from_keyword, ..]
            if distinct_keyword.eq_ignore_ascii_case("distinct")
                && from_keyword.eq_ignore_ascii_case("from") =>
        {
            (true, is_index + 3)
        }
        [ref not_keyword, ref distinct_keyword, ref from_keyword, ..]
            if not_keyword.eq_ignore_ascii_case("not")
                && distinct_keyword.eq_ignore_ascii_case("distinct")
                && from_keyword.eq_ignore_ascii_case("from") =>
        {
            (false, is_index + 4)
        }
        _ => return Ok(None),
    };
    let column = tokens[0].clone();
    validate_sql_column_ref(&column)?;
    let rhs = parse_null_safe_comparison_rhs(&tokens[rhs_start..])?;
    let distinct_predicate = null_safe_distinct_predicate(&column, rhs);
    if is_distinct {
        Ok(Some(distinct_predicate))
    } else {
        Ok(Some(ParsedPredicate::Not {
            inner: Box::new(distinct_predicate),
        }))
    }
}

fn parse_null_safe_comparison_rhs(
    tokens: &[String],
) -> Result<NullSafeComparisonRhs, ShardLoomError> {
    match tokens {
        [date_keyword, literal_raw] if date_keyword.eq_ignore_ascii_case("date") => Ok(
            NullSafeComparisonRhs::Literal(parse_sql_date_literal(literal_raw)?),
        ),
        [timestamp_keyword, literal_raw] if timestamp_keyword.eq_ignore_ascii_case("timestamp") => {
            Ok(NullSafeComparisonRhs::Literal(parse_sql_timestamp_literal(
                literal_raw,
            )?))
        }
        [binary_keyword, literal_raw]
            if binary_keyword.eq_ignore_ascii_case("binary")
                || binary_keyword.eq_ignore_ascii_case("blob") =>
        {
            let bytes = parse_sql_binary_keyword_literal(binary_keyword, literal_raw)?.ok_or_else(
                || {
                    unsupported_sql_error(
                        "IS [NOT] DISTINCT FROM binary predicates require X'<hex>', BINARY/BLOB '<utf8>', or NULL",
                    )
                },
            )?;
            Ok(NullSafeComparisonRhs::Literal(ScalarValue::Binary(bytes)))
        }
        [literal_or_column] => {
            if let Some(value) = parse_direct_binary_predicate_literal(literal_or_column)? {
                return Ok(NullSafeComparisonRhs::Literal(value));
            }
            match parse_sql_literal(literal_or_column) {
                Ok(value) => Ok(NullSafeComparisonRhs::Literal(value)),
                Err(_) if validate_sql_column_ref(literal_or_column).is_ok() => {
                    Ok(NullSafeComparisonRhs::Column(literal_or_column.clone()))
                }
                Err(error) => Err(error),
            }
        }
        _ => Err(unsupported_sql_error(
            "IS [NOT] DISTINCT FROM admits <column> IS [NOT] DISTINCT FROM <literal>, DATE <date-literal>, TIMESTAMP <timestamp-literal>, BINARY/BLOB <literal>, or <column> only",
        )),
    }
}

fn null_safe_distinct_predicate(column: &str, rhs: NullSafeComparisonRhs) -> ParsedPredicate {
    match rhs {
        NullSafeComparisonRhs::Literal(ScalarValue::Null) => ParsedPredicate::IsNotNull {
            column: column.to_string(),
        },
        NullSafeComparisonRhs::Literal(value) => ParsedPredicate::Logical {
            op: LogicalPredicateOp::Or,
            left: Box::new(ParsedPredicate::IsNull {
                column: column.to_string(),
            }),
            right: Box::new(ParsedPredicate::Compare {
                column: column.to_string(),
                op: ComparisonOp::NotEq,
                value,
            }),
        },
        NullSafeComparisonRhs::Column(right_column) => null_safe_distinct_column_predicate(
            &ParsedPredicate::IsNull {
                column: column.to_string(),
            },
            &ParsedPredicate::IsNotNull {
                column: column.to_string(),
            },
            &ParsedPredicate::IsNull {
                column: right_column.clone(),
            },
            &ParsedPredicate::IsNotNull {
                column: right_column.clone(),
            },
            ParsedPredicate::ColumnCompare {
                left_column: column.to_string(),
                op: ComparisonOp::NotEq,
                right_column,
            },
        ),
    }
}

fn null_safe_distinct_column_predicate(
    left_is_null: &ParsedPredicate,
    left_is_not_null: &ParsedPredicate,
    right_is_null: &ParsedPredicate,
    right_is_not_null: &ParsedPredicate,
    not_equal: ParsedPredicate,
) -> ParsedPredicate {
    let left_null_only = ParsedPredicate::Logical {
        op: LogicalPredicateOp::And,
        left: Box::new(left_is_null.clone()),
        right: Box::new(right_is_not_null.clone()),
    };
    let right_null_only = ParsedPredicate::Logical {
        op: LogicalPredicateOp::And,
        left: Box::new(left_is_not_null.clone()),
        right: Box::new(right_is_null.clone()),
    };
    let both_not_null = ParsedPredicate::Logical {
        op: LogicalPredicateOp::And,
        left: Box::new(left_is_not_null.clone()),
        right: Box::new(right_is_not_null.clone()),
    };
    let non_null_not_equal = ParsedPredicate::Logical {
        op: LogicalPredicateOp::And,
        left: Box::new(both_not_null),
        right: Box::new(not_equal),
    };
    ParsedPredicate::Logical {
        op: LogicalPredicateOp::Or,
        left: Box::new(ParsedPredicate::Logical {
            op: LogicalPredicateOp::Or,
            left: Box::new(left_null_only),
            right: Box::new(right_null_only),
        }),
        right: Box::new(non_null_not_equal),
    }
}

fn split_predicate_tokens(raw: &str) -> Result<Vec<String>, ShardLoomError> {
    if let Some((index, op)) = find_top_level_comparison_operator(raw)? {
        let mut tokens = split_whitespace_outside_quotes(raw[..index].trim())?;
        tokens.push(op.to_owned());
        tokens.extend(split_whitespace_outside_quotes(
            raw[index + op.len()..].trim(),
        )?);
        Ok(tokens)
    } else {
        split_whitespace_outside_quotes(raw)
    }
}

fn parse_token_predicate(raw: &str) -> Result<ParsedPredicate, ShardLoomError> {
    let tokens = split_predicate_tokens(raw)?;
    if let Some(predicate) = parse_boolean_predicate_tokens(tokens.as_slice())? {
        return Ok(predicate);
    }
    match tokens.as_slice() {
        [column, is_keyword, null_keyword]
            if is_keyword.eq_ignore_ascii_case("is")
                && null_keyword.eq_ignore_ascii_case("null") =>
        {
            validate_sql_column_ref(column)?;
            Ok(ParsedPredicate::IsNull {
                column: (*column).clone(),
            })
        }
        [column, is_keyword, not_keyword, null_keyword]
            if is_keyword.eq_ignore_ascii_case("is")
                && not_keyword.eq_ignore_ascii_case("not")
                && null_keyword.eq_ignore_ascii_case("null") =>
        {
            validate_sql_column_ref(column)?;
            Ok(ParsedPredicate::IsNotNull {
                column: (*column).clone(),
            })
        }
        [column, op_raw, date_keyword, literal_raw]
            if date_keyword.eq_ignore_ascii_case("date") =>
        {
            validate_sql_column_ref(column)?;
            let op = parse_comparison_op(op_raw)?;
            let value = parse_sql_date_literal(literal_raw)?;
            Ok(ParsedPredicate::Compare {
                column: (*column).clone(),
                op,
                value,
            })
        }
        [column, op_raw, timestamp_keyword, literal_raw]
            if timestamp_keyword.eq_ignore_ascii_case("timestamp") =>
        {
            validate_sql_column_ref(column)?;
            let op = parse_comparison_op(op_raw)?;
            let value = parse_sql_timestamp_literal(literal_raw)?;
            Ok(ParsedPredicate::Compare {
                column: (*column).clone(),
                op,
                value,
            })
        }
        [column, op_raw, literal_raw] => {
            parse_literal_or_pattern_predicate(column, op_raw, literal_raw)
        }
        [column, op_raw, binary_keyword, literal_raw]
            if binary_keyword.eq_ignore_ascii_case("binary")
                || binary_keyword.eq_ignore_ascii_case("blob") =>
        {
            parse_binary_keyword_literal_predicate(column, op_raw, binary_keyword, literal_raw)
        }
        [
            column,
            like_keyword,
            literal_raw,
            escape_keyword,
            escape_raw,
        ] if like_keyword.eq_ignore_ascii_case("like")
            && escape_keyword.eq_ignore_ascii_case("escape") =>
        {
            parse_like_predicate(column, literal_raw, Some(escape_raw))
        }
        [column, not_keyword, like_keyword, literal_raw]
            if not_keyword.eq_ignore_ascii_case("not")
                && like_keyword.eq_ignore_ascii_case("like") =>
        {
            parse_negated_like_predicate(column, literal_raw, None)
        }
        [
            column,
            not_keyword,
            like_keyword,
            literal_raw,
            escape_keyword,
            escape_raw,
        ] if not_keyword.eq_ignore_ascii_case("not")
            && like_keyword.eq_ignore_ascii_case("like")
            && escape_keyword.eq_ignore_ascii_case("escape") =>
        {
            parse_negated_like_predicate(column, literal_raw, Some(escape_raw))
        }
        [column, not_keyword, regex_op_raw, literal_raw]
            if not_keyword.eq_ignore_ascii_case("not")
                && is_regex_predicate_operator(regex_op_raw) =>
        {
            parse_negated_regex_predicate(column, literal_raw)
        }
        _ => Err(unsupported_where_predicate_shape_error()),
    }
}

fn parse_literal_or_pattern_predicate(
    column: &str,
    op_raw: &str,
    literal_raw: &str,
) -> Result<ParsedPredicate, ShardLoomError> {
    validate_sql_column_ref(column)?;
    if op_raw.eq_ignore_ascii_case("like") {
        return parse_like_predicate(column, literal_raw, None);
    }
    if is_regex_predicate_operator(op_raw) {
        return Ok(ParsedPredicate::StringMatch {
            column: column.to_string(),
            op: StringPredicateOp::RegexMatch,
            value: parse_regex_pattern_literal(literal_raw)?,
            like_escape: None,
        });
    }
    let op = parse_comparison_op(op_raw)?;
    let value = if let Some(value) = parse_direct_binary_predicate_literal(literal_raw)? {
        value
    } else {
        match parse_sql_literal(literal_raw) {
            Ok(value) => value,
            Err(_) if validate_sql_column_ref(literal_raw).is_ok() => {
                return Ok(ParsedPredicate::ColumnCompare {
                    left_column: column.to_string(),
                    op,
                    right_column: literal_raw.to_string(),
                });
            }
            Err(error) => {
                return Err(error);
            }
        }
    };
    Ok(ParsedPredicate::Compare {
        column: column.to_string(),
        op,
        value,
    })
}

fn parse_binary_keyword_literal_predicate(
    column: &str,
    op_raw: &str,
    binary_keyword: &str,
    literal_raw: &str,
) -> Result<ParsedPredicate, ShardLoomError> {
    validate_sql_column_ref(column)?;
    let op = parse_comparison_op(op_raw)?;
    let bytes = parse_sql_binary_keyword_literal(binary_keyword, literal_raw)?.ok_or_else(|| {
        unsupported_sql_error(
            "binary predicates require X'<hex>', BINARY/BLOB '<utf8>', or explicit CAST(<column> AS binary) syntax",
        )
    })?;
    Ok(ParsedPredicate::Compare {
        column: column.to_string(),
        op,
        value: ScalarValue::Binary(bytes),
    })
}

fn parse_direct_binary_predicate_literal(raw: &str) -> Result<Option<ScalarValue>, ShardLoomError> {
    let trimmed = raw.trim();
    if is_sql_binary_hex_literal(trimmed) {
        return parse_sql_binary_hex_literal(trimmed)
            .map(ScalarValue::Binary)
            .map(Some);
    }
    Ok(None)
}

fn parse_like_predicate(
    column: &str,
    literal_raw: &str,
    escape_raw: Option<&str>,
) -> Result<ParsedPredicate, ShardLoomError> {
    validate_sql_column_ref(column)?;
    let pattern = parse_sql_string_literal(literal_raw)?;
    let like_escape = escape_raw.map(parse_like_escape_character).transpose()?;
    let (op, value) = parse_like_string_predicate(&pattern, like_escape)?;
    Ok(ParsedPredicate::StringMatch {
        column: column.to_string(),
        op,
        value,
        like_escape,
    })
}

fn parse_like_escape_character(raw: &str) -> Result<char, ShardLoomError> {
    let value = parse_sql_string_literal(raw)?;
    let mut chars = value.chars();
    let Some(ch) = chars.next() else {
        return Err(unsupported_sql_error(
            "LIKE ESCAPE clause requires a single-character string literal",
        ));
    };
    if chars.next().is_some() {
        return Err(unsupported_sql_error(
            "LIKE ESCAPE clause requires a single-character string literal",
        ));
    }
    Ok(ch)
}

fn parse_negated_like_predicate(
    column: &str,
    literal_raw: &str,
    escape_raw: Option<&str>,
) -> Result<ParsedPredicate, ShardLoomError> {
    validate_sql_column_ref(column)?;
    let pattern = parse_sql_string_literal(literal_raw)?;
    let like_escape = escape_raw.map(parse_like_escape_character).transpose()?;
    let (op, value) = parse_like_string_predicate(&pattern, like_escape)?;
    Ok(ParsedPredicate::Not {
        inner: Box::new(ParsedPredicate::StringMatch {
            column: column.to_string(),
            op,
            value,
            like_escape,
        }),
    })
}

fn parse_negated_regex_predicate(
    column: &str,
    literal_raw: &str,
) -> Result<ParsedPredicate, ShardLoomError> {
    validate_sql_column_ref(column)?;
    Ok(ParsedPredicate::Not {
        inner: Box::new(ParsedPredicate::StringMatch {
            column: column.to_string(),
            op: StringPredicateOp::RegexMatch,
            value: parse_regex_pattern_literal(literal_raw)?,
            like_escape: None,
        }),
    })
}

fn unsupported_where_predicate_shape_error() -> ShardLoomError {
    unsupported_sql_error(
        "WHERE admits only <column>, <column> IS [NOT] TRUE/FALSE, <column> <op> <literal>, <column> <op> DATE <date-literal>, <column> <op> TIMESTAMP <timestamp-literal>, <column> IS [NOT] DISTINCT FROM <literal-or-column>, <column> [NOT] BETWEEN <literal> AND <literal>, <column> (+|-|*|/) <numeric-literal> <op> <numeric-literal>, generalized numeric expression trees or temporal differences <op> numeric expression/literal, ABS/FLOOR/CEIL/ROUND(<column>) <op> <numeric-literal>, LENGTH(<column>) <op> <int-literal>, CONCAT/SUBSTR/SUBSTRING/LEFT/RIGHT/REPLACE string function expressions <op> <string-literal>, DATE_YEAR/MONTH/DAY(<column>) <op> <int-literal>, TIMESTAMP_YEAR/MONTH/DAY/HOUR/MINUTE/SECOND(<column>) <op> <int-literal>, DATE_ADD_DAYS(<column>, <days>) <op> DATE <date-literal>, DATE_SUB_DAYS(<column>, <days>) <op> DATE <date-literal>, TIMESTAMP_ADD_SECONDS(<column>, <seconds>) <op> TIMESTAMP <timestamp-literal>, TIMESTAMP_SUB_SECONDS(<column>, <seconds>) <op> TIMESTAMP <timestamp-literal>, LOWER/UPPER/TRIM(<column>) <op> <string-literal>, <column> [NOT] IN (<literal>,...), <column> [NOT] LIKE <string-pattern> [ESCAPE <single-character-string-literal>], <column> [NOT] RLIKE|REGEXP <regex-pattern>, REGEXP_LIKE(<column>, <regex-pattern>), <column> IS NULL, <column> IS NOT NULL, admitted predicates joined by AND/OR/NOT, or balanced grouping parentheses around admitted predicates",
    )
}

fn parse_boolean_predicate_tokens(
    tokens: &[String],
) -> Result<Option<ParsedPredicate>, ShardLoomError> {
    match tokens {
        [column] => {
            validate_sql_column_ref(column)?;
            Ok(Some(ParsedPredicate::BooleanPredicate {
                column: (*column).clone(),
                expected: true,
                null_is_false: false,
                negated: false,
            }))
        }
        [column, is_keyword, truth_keyword]
            if is_keyword.eq_ignore_ascii_case("is")
                && (truth_keyword.eq_ignore_ascii_case("true")
                    || truth_keyword.eq_ignore_ascii_case("false")) =>
        {
            validate_sql_column_ref(column)?;
            Ok(Some(ParsedPredicate::BooleanPredicate {
                column: (*column).clone(),
                expected: truth_keyword.eq_ignore_ascii_case("true"),
                null_is_false: true,
                negated: false,
            }))
        }
        [column, is_keyword, not_keyword, truth_keyword]
            if is_keyword.eq_ignore_ascii_case("is")
                && not_keyword.eq_ignore_ascii_case("not")
                && (truth_keyword.eq_ignore_ascii_case("true")
                    || truth_keyword.eq_ignore_ascii_case("false")) =>
        {
            validate_sql_column_ref(column)?;
            Ok(Some(ParsedPredicate::BooleanPredicate {
                column: (*column).clone(),
                expected: truth_keyword.eq_ignore_ascii_case("true"),
                null_is_false: true,
                negated: true,
            }))
        }
        _ => Ok(None),
    }
}

fn parse_logical_predicate(raw: &str) -> Result<Option<ParsedPredicate>, ShardLoomError> {
    if let Some(or_index) = find_keyword_outside_quotes_and_parentheses(raw, "or")? {
        return parse_logical_binary_predicate(raw, or_index, "or", LogicalPredicateOp::Or)
            .map(Some);
    }
    let Some(and_index) = find_keyword_outside_quotes_and_parentheses(raw, "and")? else {
        let trimmed = raw.trim_start();
        if trimmed
            .get(..3)
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case("not"))
            && keyword_boundary(trimmed, 0, 3)
        {
            let inner_raw = trimmed[3..].trim();
            if inner_raw.is_empty() {
                return Err(unsupported_sql_error(
                    "NOT predicates must have a predicate after NOT",
                ));
            }
            return Ok(Some(ParsedPredicate::Not {
                inner: Box::new(parse_predicate(inner_raw)?),
            }));
        }
        return Ok(None);
    };
    parse_logical_binary_predicate(raw, and_index, "and", LogicalPredicateOp::And).map(Some)
}

fn parse_between_predicate(raw: &str) -> Result<Option<ParsedPredicate>, ShardLoomError> {
    let tokens = split_whitespace_outside_quotes(raw)?;
    let Some(between_index) = tokens
        .iter()
        .position(|token| token.eq_ignore_ascii_case("between"))
    else {
        return Ok(None);
    };

    let negated = match between_index {
        1 => false,
        2 if tokens[1].eq_ignore_ascii_case("not") => true,
        _ => {
            return Err(unsupported_sql_error(
                "BETWEEN predicates admit <column> [NOT] BETWEEN <lower> AND <upper> only",
            ));
        }
    };

    let column = tokens[0].clone();
    validate_sql_column_ref(&column)?;
    let lower_start = between_index + 1;
    let Some(and_offset) = tokens[lower_start..]
        .iter()
        .position(|token| token.eq_ignore_ascii_case("and"))
    else {
        return Err(unsupported_sql_error(
            "BETWEEN predicates require an AND separator between lower and upper bounds",
        ));
    };
    let and_index = lower_start + and_offset;
    let lower_tokens = &tokens[lower_start..and_index];
    let upper_tokens = &tokens[and_index + 1..];
    if lower_tokens.is_empty() || upper_tokens.is_empty() {
        return Err(unsupported_sql_error(
            "BETWEEN predicates require non-empty lower and upper literal bounds",
        ));
    }
    let lower = parse_between_bound_literal(lower_tokens)?;
    let upper = parse_between_bound_literal(upper_tokens)?;
    let between = ParsedPredicate::Logical {
        op: LogicalPredicateOp::And,
        left: Box::new(ParsedPredicate::Compare {
            column: column.clone(),
            op: ComparisonOp::GtEq,
            value: lower,
        }),
        right: Box::new(ParsedPredicate::Compare {
            column,
            op: ComparisonOp::LtEq,
            value: upper,
        }),
    };
    if negated {
        Ok(Some(ParsedPredicate::Not {
            inner: Box::new(between),
        }))
    } else {
        Ok(Some(between))
    }
}

fn parse_between_bound_literal(tokens: &[String]) -> Result<ScalarValue, ShardLoomError> {
    match tokens {
        [date_keyword, literal_raw] if date_keyword.eq_ignore_ascii_case("date") => {
            parse_sql_date_literal(literal_raw)
        }
        [timestamp_keyword, literal_raw] if timestamp_keyword.eq_ignore_ascii_case("timestamp") => {
            parse_sql_timestamp_literal(literal_raw)
        }
        [literal_raw] => parse_sql_literal(literal_raw),
        _ => Err(unsupported_sql_error(
            "BETWEEN bounds admit scalar, DATE 'YYYY-MM-DD', or TIMESTAMP 'YYYY-MM-DDTHH:MM:SS(.ffffff)(Z|+HH:MM|-HH:MM)' literals only",
        )),
    }
}

fn trim_enclosing_predicate_parentheses(mut raw: &str) -> Result<&str, ShardLoomError> {
    raw = raw.trim();
    loop {
        validate_balanced_predicate_parentheses(raw)?;
        if !raw.starts_with('(') {
            return Ok(raw);
        }
        let Some(close_index) = matching_closing_parenthesis(raw, 0)? else {
            return Err(unsupported_sql_error(
                "WHERE predicate grouping parentheses must be balanced",
            ));
        };
        if close_index != raw.len() - 1 {
            return Ok(raw);
        }
        raw = raw[1..close_index].trim();
        if raw.is_empty() {
            return Err(unsupported_sql_error(
                "WHERE predicate grouping parentheses must contain a predicate",
            ));
        }
    }
}

fn parse_logical_binary_predicate(
    raw: &str,
    op_index: usize,
    op_text: &str,
    op: LogicalPredicateOp,
) -> Result<ParsedPredicate, ShardLoomError> {
    let left_raw = raw[..op_index].trim();
    let right_raw = raw[op_index + op_text.len()..].trim();
    if left_raw.is_empty() || right_raw.is_empty() {
        return Err(unsupported_sql_error(
            "logical predicates must have a predicate on both sides",
        ));
    }
    Ok(ParsedPredicate::Logical {
        op,
        left: Box::new(parse_predicate(left_raw)?),
        right: Box::new(parse_predicate(right_raw)?),
    })
}

fn parse_cast_predicate(raw: &str) -> Result<Option<ParsedPredicate>, ShardLoomError> {
    let normalized;
    let trimmed = if let Some(value) = unwrap_leading_parenthesized_expression(raw)? {
        normalized = value;
        normalized.as_str()
    } else {
        raw.trim()
    };
    let Some((mode, open_index)) = parse_cast_function_prefix(trimmed) else {
        return Ok(None);
    };
    let close_index = matching_closing_parenthesis(trimmed, open_index)?.ok_or_else(|| {
        unsupported_sql_error(
            "CAST/TRY_CAST predicates must be written as CAST(<column> AS <dtype>) <op> <literal>",
        )
    })?;
    let inner = trimmed[open_index + 1..close_index].trim();
    let tail = trimmed[close_index + 1..].trim();
    if inner.is_empty() || tail.is_empty() {
        return Err(unsupported_sql_error(
            "CAST/TRY_CAST predicates require a source column, target dtype, comparison operator, and literal",
        ));
    }
    let as_index = find_keyword_outside_quotes(inner, "as").ok_or_else(|| {
        unsupported_sql_error("CAST/TRY_CAST predicates must use CAST(<column> AS <dtype>) syntax")
    })?;
    let column = inner[..as_index].trim();
    let target_raw = inner[as_index + 2..].trim();
    let target_dtype = parse_cast_target_dtype(target_raw)?;

    let tokens = split_whitespace_outside_quotes(tail)?;
    if tokens
        .first()
        .is_some_and(|token| parse_numeric_arithmetic_op(token).is_some())
    {
        return Ok(None);
    }
    let (op, value) = parse_cast_predicate_literal(&target_dtype, tokens.as_slice())?;
    let (column, expression, source_columns) = parse_cast_source_expression(
        column,
        &target_dtype,
        "where.cast_arg",
        "CAST/TRY_CAST binary predicates require at least one source column expression",
    )?;
    Ok(Some(ParsedPredicate::CastCompare {
        column,
        expression: Box::new(expression),
        source_columns,
        target_dtype,
        mode,
        op,
        value,
    }))
}

fn unwrap_leading_parenthesized_expression(raw: &str) -> Result<Option<String>, ShardLoomError> {
    let mut current = raw.trim();
    let mut normalized: Option<String> = None;
    while current.starts_with('(') {
        let Some(close_index) = matching_closing_parenthesis(current, 0)? else {
            return Err(unsupported_sql_error(
                "CAST/TRY_CAST predicate expression parentheses must be balanced",
            ));
        };
        if close_index == current.len() - 1 {
            return Ok(normalized);
        }
        let inner = current[1..close_index].trim();
        let tail = current[close_index + 1..].trim();
        if inner.is_empty() || tail.is_empty() {
            return Ok(normalized);
        }
        normalized = Some(format!("{inner} {tail}"));
        current = normalized.as_deref().expect("normalized expression set");
    }
    Ok(normalized)
}

fn parse_cast_predicate_literal(
    target_dtype: &LogicalDType,
    tokens: &[String],
) -> Result<(ComparisonOp, ScalarValue), ShardLoomError> {
    if matches!(target_dtype, LogicalDType::Binary) {
        return parse_binary_cast_predicate_literal(tokens);
    }
    if decimal_dtype_precision_scale(target_dtype).is_some() {
        return parse_decimal_cast_predicate_literal(target_dtype, tokens);
    }
    let parsed = match tokens {
        [op_raw, date_keyword, literal_raw] if date_keyword.eq_ignore_ascii_case("date") => (
            parse_comparison_op(op_raw)?,
            parse_sql_date_literal(literal_raw)?,
        ),
        [op_raw, timestamp_keyword, literal_raw]
            if timestamp_keyword.eq_ignore_ascii_case("timestamp") =>
        {
            (
                parse_comparison_op(op_raw)?,
                parse_sql_timestamp_literal(literal_raw)?,
            )
        }
        [op_raw, literal_raw] => (
            parse_comparison_op(op_raw)?,
            parse_sql_literal(literal_raw)?,
        ),
        _ => {
            return Err(unsupported_sql_error(
                "CAST/TRY_CAST predicates admit CAST(<column> AS <dtype>) <op> <literal> only",
            ));
        }
    };
    Ok(parsed)
}

fn parse_decimal_cast_predicate_literal(
    target_dtype: &LogicalDType,
    tokens: &[String],
) -> Result<(ComparisonOp, ScalarValue), ShardLoomError> {
    let [op_raw, literal_raw] = tokens else {
        return Err(unsupported_sql_error(
            "decimal CAST predicates admit CAST(<column> AS decimal128(p,s)) <op> <numeric-or-string-literal> only",
        ));
    };
    let op = parse_comparison_op(op_raw)?;
    let source = parse_decimal_cast_predicate_literal_source(literal_raw, target_dtype)?;
    Ok((op, cast_scalar_literal_to_dtype(source, target_dtype)?))
}

fn parse_decimal_cast_predicate_literal_source(
    raw: &str,
    target_dtype: &LogicalDType,
) -> Result<ScalarValue, ShardLoomError> {
    let trimmed = raw.trim();
    if trimmed.eq_ignore_ascii_case("null") {
        return Ok(ScalarValue::Null);
    }
    if trimmed.starts_with('\'') {
        return parse_sql_string_literal(trimmed).map(ScalarValue::Utf8);
    }
    if trimmed.is_empty() {
        return Err(unsupported_sql_error(
            "decimal CAST predicate literal must not be empty",
        ));
    }
    if let Some((_, scale)) = decimal_dtype_precision_scale(target_dtype)
        && let Some(normalized) = normalize_decimal_literal_to_target_scale(trimmed, scale)
    {
        return Ok(ScalarValue::Utf8(normalized));
    }
    Ok(ScalarValue::Utf8(trimmed.to_string()))
}

fn normalize_decimal_literal_to_target_scale(raw: &str, target_scale: u8) -> Option<String> {
    let dot_index = raw.find('.')?;
    let integer = &raw[..dot_index];
    let fraction = &raw[dot_index + 1..];
    let unsigned_integer = integer
        .strip_prefix('+')
        .or_else(|| integer.strip_prefix('-'))
        .unwrap_or(integer);
    if unsigned_integer.is_empty()
        || fraction.is_empty()
        || !unsigned_integer.chars().all(|ch| ch.is_ascii_digit())
        || !fraction.chars().all(|ch| ch.is_ascii_digit())
    {
        return None;
    }
    let target_scale = usize::from(target_scale);
    if fraction.len() <= target_scale || !fraction[target_scale..].chars().all(|ch| ch == '0') {
        return None;
    }
    if target_scale == 0 {
        Some(integer.to_string())
    } else {
        Some(format!("{}.{}", integer, &fraction[..target_scale]))
    }
}

fn cast_scalar_literal_to_dtype(
    source: ScalarValue,
    target_dtype: &LogicalDType,
) -> Result<ScalarValue, ShardLoomError> {
    let expression = Expression::cast(
        ExprId::new("sql.cast.literal")?,
        Expression::literal(ExprId::new("sql.cast.literal.source")?, source),
        target_dtype.clone(),
    );
    let report = evaluate_expression(&expression, &ExpressionInputRow::new());
    if !report.has_errors()
        && let Some(value) = report.value
    {
        return Ok(value);
    }
    let reason = report
        .diagnostics
        .first()
        .map_or("literal cannot be cast to requested dtype", |diagnostic| {
            diagnostic.message.as_str()
        });
    Err(unsupported_sql_error(&format!(
        "CAST predicate literal cannot be cast to {}: {reason}",
        target_dtype.as_str()
    )))
}

fn parse_binary_cast_predicate_literal(
    tokens: &[String],
) -> Result<(ComparisonOp, ScalarValue), ShardLoomError> {
    let (op_raw, value) = match tokens {
        [op_raw, literal_raw] => (op_raw, parse_binary_predicate_literal(literal_raw)?),
        [op_raw, binary_keyword, literal_raw] => {
            let bytes = parse_sql_binary_keyword_literal(binary_keyword, literal_raw)?
                .ok_or_else(|| {
                    unsupported_sql_error(
                        "binary CAST predicates admit X'<hex>', BINARY/BLOB '<utf8>', single-quoted UTF-8 byte literals, or NULL",
                    )
                })?;
            (op_raw, ScalarValue::Binary(bytes))
        }
        _ => {
            return Err(unsupported_sql_error(
                "binary CAST predicates admit CAST(<column> AS binary) <op> X'<hex>', BINARY/BLOB '<utf8>', single-quoted UTF-8 byte literals, or NULL",
            ));
        }
    };
    let op = parse_comparison_op(op_raw)?;
    Ok((op, value))
}

fn parse_binary_predicate_literal(raw: &str) -> Result<ScalarValue, ShardLoomError> {
    let trimmed = raw.trim();
    if trimmed.eq_ignore_ascii_case("null") {
        return Ok(ScalarValue::Null);
    }
    if is_sql_binary_hex_literal(trimmed) {
        return parse_sql_binary_hex_literal(trimmed).map(ScalarValue::Binary);
    }
    if let Some(bytes) = parse_sql_binary_text_literal(trimmed)? {
        return Ok(ScalarValue::Binary(bytes));
    }
    if trimmed.starts_with('\'') {
        return parse_sql_string_literal(trimmed)
            .map(|value| ScalarValue::Binary(value.into_bytes()));
    }
    Err(unsupported_sql_error(
        "binary CAST predicates admit X'<hex>', BINARY/BLOB '<utf8>', single-quoted UTF-8 byte literals, or NULL",
    ))
}

fn parse_date_arithmetic_predicate(raw: &str) -> Result<Option<ParsedPredicate>, ShardLoomError> {
    let trimmed = raw.trim();
    let Some((function_name, op)) = [
        ("date_add_days", DateArithmeticOp::AddDays),
        ("date_sub_days", DateArithmeticOp::SubDays),
    ]
    .into_iter()
    .find(|(name, _)| {
        trimmed
            .get(..name.len())
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case(name))
            && trimmed.as_bytes().get(name.len()) == Some(&b'(')
    }) else {
        return Ok(None);
    };

    let open_index = function_name.len();
    let close_index = matching_closing_parenthesis(trimmed, open_index)?.ok_or_else(|| {
        unsupported_sql_error(
            "date arithmetic predicates must use DATE_ADD_DAYS(<column>, <days>) or DATE_SUB_DAYS(<column>, <days>)",
        )
    })?;
    let inner = trimmed[open_index + 1..close_index].trim();
    let tail = trimmed[close_index + 1..].trim();
    if inner.is_empty() || tail.is_empty() {
        return Err(unsupported_sql_error(
            "date arithmetic predicates require a source column, day count, comparison operator, and DATE literal",
        ));
    }
    let args = split_sql_csv(inner)?;
    let [column_raw, day_count_raw] = args.as_slice() else {
        return Err(unsupported_sql_error(
            "date arithmetic predicates require exactly two arguments: <column>, <days>",
        ));
    };
    let column = parse_date_arithmetic_column_arg(column_raw)?;
    let day_count = parse_date_arithmetic_days(day_count_raw)?;
    let tokens = split_whitespace_outside_quotes(tail)?;
    let [op_raw, date_keyword, literal_raw] = tokens.as_slice() else {
        return Err(unsupported_sql_error(
            "date arithmetic predicates admit DATE_ADD_DAYS(<column>, <days>) <op> DATE <date-literal> or DATE_SUB_DAYS(<column>, <days>) <op> DATE <date-literal>",
        ));
    };
    if !date_keyword.eq_ignore_ascii_case("date") {
        return Err(unsupported_sql_error(
            "date arithmetic predicates compare against DATE 'YYYY-MM-DD' literals only",
        ));
    }
    Ok(Some(ParsedPredicate::DateArithmeticCompare {
        column,
        op,
        day_count,
        comparison: parse_comparison_op(op_raw)?,
        value: parse_sql_date_literal(literal_raw)?,
    }))
}

fn parse_timestamp_arithmetic_predicate(
    raw: &str,
) -> Result<Option<ParsedPredicate>, ShardLoomError> {
    let trimmed = raw.trim();
    let Some((function_name, op)) = [
        ("timestamp_add_seconds", TimestampArithmeticOp::AddSeconds),
        ("timestamp_sub_seconds", TimestampArithmeticOp::SubSeconds),
    ]
    .into_iter()
    .find(|(name, _)| {
        trimmed
            .get(..name.len())
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case(name))
            && trimmed.as_bytes().get(name.len()) == Some(&b'(')
    }) else {
        return Ok(None);
    };

    let open_index = function_name.len();
    let close_index = matching_closing_parenthesis(trimmed, open_index)?.ok_or_else(|| {
        unsupported_sql_error(
            "timestamp arithmetic predicates must use TIMESTAMP_ADD_SECONDS(<column>, <seconds>) or TIMESTAMP_SUB_SECONDS(<column>, <seconds>)",
        )
    })?;
    let inner = trimmed[open_index + 1..close_index].trim();
    let tail = trimmed[close_index + 1..].trim();
    if inner.is_empty() || tail.is_empty() {
        return Err(unsupported_sql_error(
            "timestamp arithmetic predicates require a source column, second count, comparison operator, and TIMESTAMP literal",
        ));
    }
    let args = split_sql_csv(inner)?;
    let [column_raw, second_count_raw] = args.as_slice() else {
        return Err(unsupported_sql_error(
            "timestamp arithmetic predicates require exactly two arguments: <column>, <seconds>",
        ));
    };
    let column = parse_timestamp_arithmetic_column_arg(column_raw)?;
    let second_count = parse_timestamp_arithmetic_seconds(second_count_raw)?;
    let tokens = split_whitespace_outside_quotes(tail)?;
    let [op_raw, timestamp_keyword, literal_raw] = tokens.as_slice() else {
        return Err(unsupported_sql_error(
            "timestamp arithmetic predicates admit TIMESTAMP_ADD_SECONDS(<column>, <seconds>) <op> TIMESTAMP <timestamp-literal> or TIMESTAMP_SUB_SECONDS(<column>, <seconds>) <op> TIMESTAMP <timestamp-literal>",
        ));
    };
    if !timestamp_keyword.eq_ignore_ascii_case("timestamp") {
        return Err(unsupported_sql_error(
            "timestamp arithmetic predicates compare against TIMESTAMP 'YYYY-MM-DDTHH:MM:SS(.ffffff)(Z|+HH:MM|-HH:MM)' literals only",
        ));
    }
    Ok(Some(ParsedPredicate::TimestampArithmeticCompare {
        column,
        op,
        second_count,
        comparison: parse_comparison_op(op_raw)?,
        value: parse_sql_timestamp_literal(literal_raw)?,
    }))
}

fn parse_date_extract_predicate(raw: &str) -> Result<Option<ParsedPredicate>, ShardLoomError> {
    let trimmed = raw.trim();
    let Some((function_name, op)) = [
        ("date_year", DateExtractOp::Year),
        ("date_month", DateExtractOp::Month),
        ("date_day", DateExtractOp::Day),
    ]
    .into_iter()
    .find(|(name, _)| {
        trimmed
            .get(..name.len())
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case(name))
            && trimmed.as_bytes().get(name.len()) == Some(&b'(')
    }) else {
        return Ok(None);
    };

    let open_index = function_name.len();
    let close_index = matching_closing_parenthesis(trimmed, open_index)?.ok_or_else(|| {
        unsupported_sql_error(
            "date extract predicates must use DATE_YEAR(<column>), DATE_MONTH(<column>), or DATE_DAY(<column>)",
        )
    })?;
    let inner = trimmed[open_index + 1..close_index].trim();
    let tail = trimmed[close_index + 1..].trim();
    if inner.is_empty() || tail.is_empty() {
        return Err(unsupported_sql_error(
            "date extract predicates require a source column, comparison operator, and integer literal",
        ));
    }
    let column = parse_date_arithmetic_column_arg(inner)?;
    let tokens = split_whitespace_outside_quotes(tail)?;
    let [op_raw, literal_raw] = tokens.as_slice() else {
        return Err(unsupported_sql_error(
            "date extract predicates admit DATE_YEAR(<column>) <op> <int-literal>, DATE_MONTH(<column>) <op> <int-literal>, or DATE_DAY(<column>) <op> <int-literal>",
        ));
    };
    Ok(Some(ParsedPredicate::DateExtractCompare {
        column,
        op,
        comparison: parse_comparison_op(op_raw)?,
        value: parse_date_extract_literal(literal_raw)?,
    }))
}

fn parse_timestamp_extract_predicate(raw: &str) -> Result<Option<ParsedPredicate>, ShardLoomError> {
    let trimmed = raw.trim();
    let Some((function_name, op)) = [
        ("timestamp_year", TimestampExtractOp::Year),
        ("timestamp_month", TimestampExtractOp::Month),
        ("timestamp_day", TimestampExtractOp::Day),
        ("timestamp_hour", TimestampExtractOp::Hour),
        ("timestamp_minute", TimestampExtractOp::Minute),
        ("timestamp_second", TimestampExtractOp::Second),
    ]
    .into_iter()
    .find(|(name, _)| {
        trimmed
            .get(..name.len())
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case(name))
            && trimmed.as_bytes().get(name.len()) == Some(&b'(')
    }) else {
        return Ok(None);
    };

    let open_index = function_name.len();
    let close_index = matching_closing_parenthesis(trimmed, open_index)?.ok_or_else(|| {
        unsupported_sql_error(
            "timestamp extract predicates must use TIMESTAMP_YEAR/MONTH/DAY/HOUR/MINUTE/SECOND(<column>)",
        )
    })?;
    let inner = trimmed[open_index + 1..close_index].trim();
    let tail = trimmed[close_index + 1..].trim();
    if inner.is_empty() || tail.is_empty() {
        return Err(unsupported_sql_error(
            "timestamp extract predicates require a source column, comparison operator, and integer literal",
        ));
    }
    let column = parse_timestamp_extract_column_arg(inner)?;
    let tokens = split_whitespace_outside_quotes(tail)?;
    let [op_raw, literal_raw] = tokens.as_slice() else {
        return Err(unsupported_sql_error(
            "timestamp extract predicates admit TIMESTAMP_YEAR/MONTH/DAY/HOUR/MINUTE/SECOND(<column>) <op> <int-literal>",
        ));
    };
    Ok(Some(ParsedPredicate::TimestampExtractCompare {
        column,
        op,
        comparison: parse_comparison_op(op_raw)?,
        value: parse_date_extract_literal(literal_raw)?,
    }))
}

fn parse_timestamp_extract_column_arg(raw: &str) -> Result<String, ShardLoomError> {
    let trimmed = raw.trim();
    if !trimmed
        .get(..5)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("cast("))
    {
        validate_sql_column_ref(trimmed)?;
        return Ok(trimmed.to_string());
    }
    let close_index = matching_closing_parenthesis(trimmed, 4)?.ok_or_else(|| {
        unsupported_sql_error(
            "timestamp extract CAST arguments must use CAST(<column> AS timestamp_micros)",
        )
    })?;
    if !trimmed[close_index + 1..].trim().is_empty() {
        return Err(unsupported_sql_error(
            "timestamp extract CAST arguments must be a single CAST(<column> AS timestamp_micros) expression",
        ));
    }
    let inner = trimmed[5..close_index].trim();
    let as_index = find_keyword_outside_quotes(inner, "as").ok_or_else(|| {
        unsupported_sql_error(
            "timestamp extract CAST arguments must use CAST(<column> AS timestamp_micros)",
        )
    })?;
    let column = inner[..as_index].trim();
    let target_raw = inner[as_index + 2..].trim();
    validate_sql_column_ref(column)?;
    if !matches!(
        parse_cast_target_dtype(target_raw)?,
        LogicalDType::TimestampMicros
    ) {
        return Err(unsupported_sql_error(
            "timestamp extract CAST arguments support timestamp_micros target dtype only",
        ));
    }
    Ok(column.to_string())
}

fn parse_timestamp_arithmetic_column_arg(raw: &str) -> Result<String, ShardLoomError> {
    let trimmed = raw.trim();
    if !trimmed
        .get(..5)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("cast("))
    {
        validate_sql_column_ref(trimmed)?;
        return Ok(trimmed.to_string());
    }
    let close_index = matching_closing_parenthesis(trimmed, 4)?.ok_or_else(|| {
        unsupported_sql_error(
            "timestamp arithmetic CAST arguments must use CAST(<column> AS timestamp_micros)",
        )
    })?;
    if !trimmed[close_index + 1..].trim().is_empty() {
        return Err(unsupported_sql_error(
            "timestamp arithmetic CAST arguments must be a single CAST(<column> AS timestamp_micros) expression",
        ));
    }
    let inner = trimmed[5..close_index].trim();
    let as_index = find_keyword_outside_quotes(inner, "as").ok_or_else(|| {
        unsupported_sql_error(
            "timestamp arithmetic CAST arguments must use CAST(<column> AS timestamp_micros)",
        )
    })?;
    let column = inner[..as_index].trim();
    let target_raw = inner[as_index + 2..].trim();
    validate_sql_column_ref(column)?;
    if !matches!(
        parse_cast_target_dtype(target_raw)?,
        LogicalDType::TimestampMicros
    ) {
        return Err(unsupported_sql_error(
            "timestamp arithmetic CAST arguments support timestamp_micros target dtype only",
        ));
    }
    Ok(column.to_string())
}

fn parse_date_extract_literal(raw: &str) -> Result<ScalarValue, ShardLoomError> {
    match parse_sql_literal(raw.trim())? {
        ScalarValue::Int64(value) => Ok(ScalarValue::Int64(value)),
        _ => Err(unsupported_sql_error(
            "date extract predicates compare against int64 literals only",
        )),
    }
}

fn parse_date_arithmetic_column_arg(raw: &str) -> Result<String, ShardLoomError> {
    let trimmed = raw.trim();
    if !trimmed
        .get(..5)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("cast("))
    {
        validate_sql_column_ref(trimmed)?;
        return Ok(trimmed.to_string());
    }
    let close_index = matching_closing_parenthesis(trimmed, 4)?.ok_or_else(|| {
        unsupported_sql_error("date arithmetic CAST arguments must use CAST(<column> AS date32)")
    })?;
    if !trimmed[close_index + 1..].trim().is_empty() {
        return Err(unsupported_sql_error(
            "date arithmetic CAST arguments must be a single CAST(<column> AS date32) expression",
        ));
    }
    let inner = trimmed[5..close_index].trim();
    let as_index = find_keyword_outside_quotes(inner, "as").ok_or_else(|| {
        unsupported_sql_error("date arithmetic CAST arguments must use CAST(<column> AS date32)")
    })?;
    let column = inner[..as_index].trim();
    let target_raw = inner[as_index + 2..].trim();
    validate_sql_column_ref(column)?;
    if !matches!(parse_cast_target_dtype(target_raw)?, LogicalDType::Date32) {
        return Err(unsupported_sql_error(
            "date arithmetic CAST arguments support date32 target dtype only",
        ));
    }
    Ok(column.to_string())
}

fn parse_date_arithmetic_days(raw: &str) -> Result<i32, ShardLoomError> {
    let trimmed = raw.trim();
    if starts_with_interval_keyword(trimmed) {
        let interval = parse_sql_interval_literal(trimmed)?;
        if interval.unit != SqlIntervalUnit::Day {
            return Err(unsupported_sql_error(
                "date arithmetic interval literals admit DAY units only",
            ));
        }
        let value = i32::try_from(interval.value).map_err(|_| {
            unsupported_sql_error("date arithmetic day count must fit in signed 32-bit days")
        })?;
        if i64::from(value).abs() > i64::from(MAX_DATE_ARITHMETIC_DAYS) {
            return Err(unsupported_sql_error(&format!(
                "date arithmetic day count admits absolute values <= {MAX_DATE_ARITHMETIC_DAYS}"
            )));
        }
        return Ok(value);
    }
    if trimmed.is_empty()
        || !trimmed
            .chars()
            .enumerate()
            .all(|(index, ch)| ch.is_ascii_digit() || (index == 0 && matches!(ch, '+' | '-')))
        || matches!(trimmed, "+" | "-")
    {
        return Err(unsupported_sql_error(
            "date arithmetic day count must be a signed integer literal",
        ));
    }
    let value = trimmed.parse::<i32>().map_err(|_| {
        unsupported_sql_error("date arithmetic day count must fit in signed 32-bit days")
    })?;
    if i64::from(value).abs() > i64::from(MAX_DATE_ARITHMETIC_DAYS) {
        return Err(unsupported_sql_error(&format!(
            "date arithmetic day count admits absolute values <= {MAX_DATE_ARITHMETIC_DAYS}"
        )));
    }
    Ok(value)
}

fn parse_timestamp_arithmetic_seconds(raw: &str) -> Result<i64, ShardLoomError> {
    let trimmed = raw.trim();
    if starts_with_interval_keyword(trimmed) {
        let interval = parse_sql_interval_literal(trimmed)?;
        let value = interval
            .value
            .checked_mul(interval.unit.seconds_multiplier())
            .ok_or_else(|| {
                unsupported_sql_error(
                    "timestamp arithmetic interval literal must fit in signed 64-bit seconds",
                )
            })?;
        if value
            .checked_abs()
            .is_none_or(|abs| abs > MAX_TIMESTAMP_ARITHMETIC_SECONDS)
        {
            return Err(unsupported_sql_error(&format!(
                "timestamp arithmetic second count admits absolute values <= {MAX_TIMESTAMP_ARITHMETIC_SECONDS}"
            )));
        }
        return Ok(value);
    }
    if trimmed.is_empty()
        || !trimmed
            .chars()
            .enumerate()
            .all(|(index, ch)| ch.is_ascii_digit() || (index == 0 && matches!(ch, '+' | '-')))
        || matches!(trimmed, "+" | "-")
    {
        return Err(unsupported_sql_error(
            "timestamp arithmetic second count must be a signed integer literal",
        ));
    }
    let value = trimmed.parse::<i64>().map_err(|_| {
        unsupported_sql_error("timestamp arithmetic second count must fit in signed 64-bit seconds")
    })?;
    if value
        .checked_abs()
        .is_none_or(|abs| abs > MAX_TIMESTAMP_ARITHMETIC_SECONDS)
    {
        return Err(unsupported_sql_error(&format!(
            "timestamp arithmetic second count admits absolute values <= {MAX_TIMESTAMP_ARITHMETIC_SECONDS}"
        )));
    }
    Ok(value)
}

fn starts_with_interval_keyword(raw: &str) -> bool {
    let keyword = "interval";
    raw.get(..keyword.len())
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case(keyword))
        && keyword_boundary(raw, 0, keyword.len())
}

fn parse_sql_interval_literal(raw: &str) -> Result<SqlIntervalLiteral, ShardLoomError> {
    let tokens = split_whitespace_outside_quotes(raw)?;
    let [keyword, value_raw, unit_raw] = tokens.as_slice() else {
        return Err(unsupported_sql_error(
            "ANSI INTERVAL literals in scoped temporal arithmetic must use INTERVAL '<signed integer>' <unit>",
        ));
    };
    if !keyword.eq_ignore_ascii_case("interval") {
        return Err(unsupported_sql_error(
            "ANSI INTERVAL literals in scoped temporal arithmetic must start with INTERVAL",
        ));
    }
    let value_text = parse_sql_string_literal(value_raw)?;
    let value = parse_interval_literal_integer(&value_text)?;
    Ok(SqlIntervalLiteral {
        value,
        unit: SqlIntervalUnit::parse(unit_raw)?,
    })
}

fn parse_interval_literal_integer(raw: &str) -> Result<i64, ShardLoomError> {
    let trimmed = raw.trim();
    if trimmed.is_empty()
        || !trimmed
            .chars()
            .enumerate()
            .all(|(index, ch)| ch.is_ascii_digit() || (index == 0 && matches!(ch, '+' | '-')))
        || matches!(trimmed, "+" | "-")
    {
        return Err(unsupported_sql_error(
            "ANSI INTERVAL literal value must be a signed integer string literal",
        ));
    }
    trimmed.parse::<i64>().map_err(|_| {
        unsupported_sql_error("ANSI INTERVAL literal value must fit in signed 64-bit units")
    })
}

fn decimal_dtype_precision_scale(dtype: &LogicalDType) -> Option<(u8, u8)> {
    parse_decimal_cast_target_dtype(dtype.as_str())
        .ok()
        .flatten()
}

fn parse_numeric_arithmetic_predicate(
    raw: &str,
) -> Result<Option<ParsedPredicate>, ShardLoomError> {
    let tokens = split_whitespace_outside_quotes(raw)?;
    let Some(op_index) = tokens
        .iter()
        .position(|token| parse_numeric_arithmetic_op(token).is_some())
    else {
        return Ok(None);
    };
    if tokens.len() != 5 || op_index != 1 {
        return Err(unsupported_sql_error(
            "numeric arithmetic predicates admit <column> (+|-|*|/) <numeric-literal> <op> <numeric-literal> only",
        ));
    }
    let column = &tokens[0];
    validate_sql_column_ref(column)?;
    let op = parse_numeric_arithmetic_op(&tokens[1]).expect("arithmetic op was detected");
    let rhs = parse_numeric_arithmetic_literal(&tokens[2])?;
    let comparison = parse_comparison_op(&tokens[3])?;
    let value = parse_numeric_arithmetic_literal(&tokens[4])?;
    Ok(Some(ParsedPredicate::NumericArithmeticCompare {
        column: column.clone(),
        op,
        rhs,
        comparison,
        value,
    }))
}

fn parse_numeric_arithmetic_op(raw: &str) -> Option<NumericArithmeticOp> {
    match raw.trim() {
        "+" => Some(NumericArithmeticOp::Add),
        "-" => Some(NumericArithmeticOp::Subtract),
        "*" => Some(NumericArithmeticOp::Multiply),
        "/" => Some(NumericArithmeticOp::Divide),
        _ => None,
    }
}

fn parse_numeric_rounding_function_prefix(raw: &str) -> Option<(NumericRoundingOp, usize)> {
    let trimmed = raw.trim();
    for (name, op) in [
        ("floor", NumericRoundingOp::Floor),
        ("ceil", NumericRoundingOp::Ceil),
        ("ceiling", NumericRoundingOp::Ceil),
        ("round", NumericRoundingOp::Round),
    ] {
        let len = name.len();
        if trimmed
            .get(..len)
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case(name))
            && trimmed.as_bytes().get(len) == Some(&b'(')
        {
            return Some((op, len));
        }
    }
    None
}

fn parse_numeric_arithmetic_literal(raw: &str) -> Result<ScalarValue, ShardLoomError> {
    match parse_sql_literal(raw)? {
        value @ (ScalarValue::Int64(_) | ScalarValue::Float64(_)) => Ok(value),
        _ => Err(unsupported_sql_error(
            "numeric arithmetic expressions admit int64 or finite float64 literals only",
        )),
    }
}

fn parse_generic_expression_predicate(
    raw: &str,
) -> Result<Option<ParsedPredicate>, ShardLoomError> {
    let Some((comparison_index, comparison_raw)) = find_top_level_comparison_operator(raw)? else {
        return Ok(None);
    };
    if is_simple_numeric_arithmetic_predicate_shape(raw)? {
        return Ok(None);
    }
    let left_raw = raw[..comparison_index].trim();
    let right_raw = raw[comparison_index + comparison_raw.len()..].trim();
    if left_raw.is_empty() || right_raw.is_empty() {
        return Err(unsupported_sql_error(
            "generic numeric expression predicates require expressions on both sides of a comparison operator",
        ));
    }
    let contains_temporal_difference = expression_contains_temporal_difference_call(left_raw)?
        || expression_contains_temporal_difference_call(right_raw)?;
    let numeric = expression_contains_numeric_operator(left_raw)?
        || expression_contains_numeric_operator(right_raw)?;
    let composed =
        scalar_expression::composed(left_raw)? || scalar_expression::composed(right_raw)?;
    if !numeric && !contains_temporal_difference && !composed {
        return Ok(None);
    }
    let left = parse_numeric_scalar_expression(left_raw, "where.generic.left")?;
    let right = parse_numeric_scalar_expression(right_raw, "where.generic.right")?;
    let source_columns = expression_pair_source_columns(&left, &right);
    let operator_families = expression_pair_operator_families(&left, &right);
    let binary_operator_count =
        expression_binary_operator_count(&left) + expression_binary_operator_count(&right);
    if binary_operator_count == 0
        && !expression_pair_has_temporal_difference(&left, &right)
        && !composed
    {
        return Ok(None);
    }
    Ok(Some(ParsedPredicate::GenericExpressionCompare {
        left: Box::new(left),
        comparison: parse_comparison_op(comparison_raw)?,
        right: Box::new(right),
        source_columns,
        operator_families,
        binary_operator_count,
    }))
}

fn is_simple_numeric_arithmetic_predicate_shape(raw: &str) -> Result<bool, ShardLoomError> {
    let tokens = split_whitespace_outside_quotes(raw)?;
    let Some(op_index) = tokens
        .iter()
        .position(|token| parse_numeric_arithmetic_op(token).is_some())
    else {
        return Ok(false);
    };
    if tokens.len() != 5 || op_index != 1 {
        return Ok(false);
    }
    if validate_sql_column_ref(&tokens[0]).is_err() {
        return Ok(false);
    }
    Ok(parse_numeric_arithmetic_literal(&tokens[2]).is_ok()
        && parse_comparison_op(&tokens[3]).is_ok()
        && parse_numeric_arithmetic_literal(&tokens[4]).is_ok())
}

fn parse_numeric_abs_predicate(raw: &str) -> Result<Option<ParsedPredicate>, ShardLoomError> {
    let trimmed = raw.trim();
    if !trimmed
        .get(..3)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("abs"))
        || trimmed.as_bytes().get(3) != Some(&b'(')
    {
        return Ok(None);
    }
    let open_index = "abs".len();
    let close_index = matching_closing_parenthesis(trimmed, open_index)?
        .ok_or_else(|| unsupported_sql_error("numeric abs predicates must use ABS(<column>)"))?;
    let inner = trimmed[open_index + 1..close_index].trim();
    let tail = trimmed[close_index + 1..].trim();
    if inner.is_empty() || tail.is_empty() {
        return Err(unsupported_sql_error(
            "numeric abs predicates require a source column, comparison operator, and numeric literal",
        ));
    }
    validate_sql_column_ref(inner)?;
    let tokens = split_whitespace_outside_quotes(tail)?;
    let [op_raw, literal_raw] = tokens.as_slice() else {
        return Err(unsupported_sql_error(
            "numeric abs predicates admit ABS(<column>) <op> <numeric-literal> only",
        ));
    };
    Ok(Some(ParsedPredicate::NumericAbsCompare {
        column: inner.to_string(),
        comparison: parse_comparison_op(op_raw)?,
        value: parse_numeric_arithmetic_literal(literal_raw)?,
    }))
}

fn parse_numeric_rounding_predicate(raw: &str) -> Result<Option<ParsedPredicate>, ShardLoomError> {
    let trimmed = raw.trim();
    let Some((op, open_index)) = parse_numeric_rounding_function_prefix(trimmed) else {
        return Ok(None);
    };
    let close_index = matching_closing_parenthesis(trimmed, open_index)?.ok_or_else(|| {
        unsupported_sql_error("numeric rounding predicates must use FLOOR/CEIL/ROUND(<column>)")
    })?;
    let inner = trimmed[open_index + 1..close_index].trim();
    let tail = trimmed[close_index + 1..].trim();
    if inner.is_empty() || tail.is_empty() {
        return Err(unsupported_sql_error(
            "numeric rounding predicates require a source column, comparison operator, and numeric literal",
        ));
    }
    validate_sql_column_ref(inner)?;
    let tokens = split_whitespace_outside_quotes(tail)?;
    let [op_raw, literal_raw] = tokens.as_slice() else {
        return Err(unsupported_sql_error(
            "numeric rounding predicates admit FLOOR/CEIL/ROUND(<column>) <op> <numeric-literal> only",
        ));
    };
    Ok(Some(ParsedPredicate::NumericRoundingCompare {
        column: inner.to_string(),
        op,
        comparison: parse_comparison_op(op_raw)?,
        value: parse_numeric_arithmetic_literal(literal_raw)?,
    }))
}

fn parse_string_length_predicate(raw: &str) -> Result<Option<ParsedPredicate>, ShardLoomError> {
    let trimmed = raw.trim();
    if !trimmed
        .get(.."length".len())
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("length"))
        || trimmed.as_bytes().get("length".len()) != Some(&b'(')
    {
        return Ok(None);
    }
    let close_index = matching_closing_parenthesis(trimmed, "length".len())?.ok_or_else(|| {
        unsupported_sql_error("string length predicates must use LENGTH(<string-expression>)")
    })?;
    let expression_raw = trimmed[..=close_index].trim();
    let expression = parse_string_length_call_expression(expression_raw, "where.string_length")?
        .expect("string length prefix produced a string length expression");
    let tail = trimmed[close_index + 1..].trim();
    if tail.is_empty() {
        return Err(unsupported_sql_error(
            "string length predicates require a source column, comparison operator, and int64 literal",
        ));
    }
    let source_columns = expression_source_columns(&expression);
    if source_columns.is_empty() {
        return Err(unsupported_sql_error(
            "string length predicates require at least one source column argument",
        ));
    }
    let tokens = split_whitespace_outside_quotes(tail)?;
    let [op_raw, literal_raw] = tokens.as_slice() else {
        return Err(unsupported_sql_error(
            "string length predicates admit LENGTH(<column>) <op> <int-literal> only",
        ));
    };
    let value @ ScalarValue::Int64(_) = parse_sql_literal(literal_raw)? else {
        return Err(unsupported_sql_error(
            "string length predicates compare against int64 literals only",
        ));
    };
    Ok(Some(ParsedPredicate::StringLengthCompare {
        expression: Box::new(expression),
        comparison: parse_comparison_op(op_raw)?,
        value,
        source_columns,
    }))
}

fn parse_string_transform_predicate(raw: &str) -> Result<Option<ParsedPredicate>, ShardLoomError> {
    let trimmed = raw.trim();
    let Some((op, open_index)) = parse_string_transform_prefix(trimmed) else {
        return Ok(None);
    };
    let close_index = matching_closing_parenthesis(trimmed, open_index)?.ok_or_else(|| {
        unsupported_sql_error(
            "string transform predicates must use LOWER|UPPER|TRIM(<string-expression>)",
        )
    })?;
    let expression_raw = trimmed[..=close_index].trim();
    let expression = parse_string_scalar_expression(expression_raw, "where.string_transform")?;
    let tail = trimmed[close_index + 1..].trim();
    if tail.is_empty() {
        return Err(unsupported_sql_error(
            "string transform predicates require a source column, comparison operator, and string literal",
        ));
    }
    let source_columns = expression_source_columns(&expression);
    if source_columns.is_empty() {
        return Err(unsupported_sql_error(
            "string transform predicates require at least one source column argument",
        ));
    }
    let tokens = split_whitespace_outside_quotes(tail)?;
    let [op_raw, literal_raw] = tokens.as_slice() else {
        return Err(unsupported_sql_error(
            "string transform predicates admit LOWER/UPPER/TRIM(<column>) <op> <string-literal> only",
        ));
    };
    let literal = parse_sql_string_literal(literal_raw)?;
    Ok(Some(ParsedPredicate::StringTransformCompare {
        expression: Box::new(expression),
        op,
        comparison: parse_comparison_op(op_raw)?,
        value: ScalarValue::Utf8(literal),
        source_columns,
    }))
}

fn parse_string_function_predicate(raw: &str) -> Result<Option<ParsedPredicate>, ShardLoomError> {
    let Some((comparison_index, comparison_raw)) = find_top_level_comparison_operator(raw)? else {
        return Ok(None);
    };
    let left_raw = raw[..comparison_index].trim();
    let right_raw = raw[comparison_index + comparison_raw.len()..].trim();
    let Some(call) = parse_string_function_call_expression(left_raw, "where.string_function")?
    else {
        return Ok(None);
    };
    if right_raw.is_empty() {
        return Err(unsupported_sql_error(
            "string function predicates require a string literal right-hand side",
        ));
    }
    if call.source_columns.is_empty() {
        return Err(unsupported_sql_error(
            "string function predicates require at least one source column argument",
        ));
    }
    let literal = parse_sql_string_literal(right_raw)?;
    Ok(Some(ParsedPredicate::StringFunctionCompare {
        expression: Box::new(call.expression),
        op: call.op,
        comparison: parse_comparison_op(comparison_raw)?,
        value: ScalarValue::Utf8(literal),
        source_columns: call.source_columns,
        literal_count: call.literal_count + 1,
    }))
}

fn parse_sql_date_literal(raw: &str) -> Result<ScalarValue, ShardLoomError> {
    let value = parse_sql_string_literal(raw)?;
    parse_iso_date32(&value)
        .map(ScalarValue::Date32)
        .map_err(|_| unsupported_sql_error("DATE literals must use DATE 'YYYY-MM-DD'"))
}

fn parse_sql_timestamp_literal(raw: &str) -> Result<ScalarValue, ShardLoomError> {
    let value = parse_sql_string_literal(raw)?;
    parse_iso_timestamp_micros(&value)
        .map(ScalarValue::TimestampMicros)
        .map_err(|_| {
            unsupported_sql_error(
                "TIMESTAMP literals must use TIMESTAMP 'YYYY-MM-DDTHH:MM:SS(.ffffff)(Z|+HH:MM|-HH:MM)'",
            )
        })
}

fn parse_in_list_predicate(raw: &str) -> Result<Option<ParsedPredicate>, ShardLoomError> {
    let Some(in_index) = find_keyword_outside_quotes_and_parentheses(raw, "in")? else {
        return Ok(None);
    };
    let column_raw = raw[..in_index].trim();
    let (column_raw, negated) = strip_trailing_not_keyword(column_raw);
    let tail = raw[in_index + "in".len()..].trim();
    if column_raw.starts_with('(') {
        return parse_row_value_in_list_predicate(column_raw, tail, negated).map(Some);
    }
    if column_raw.contains(',') {
        return Err(unsupported_sql_error(
            "comma-separated IN column lists must use row-value syntax (<column>,...) [NOT] IN ((<literal>,...),...) or one scalar IN predicate per admitted column",
        ));
    }
    let column_tokens = split_whitespace_outside_quotes(column_raw)?;
    let column = match column_tokens.as_slice() {
        [column] => column.as_str(),
        _ => {
            return Err(unsupported_sql_error(
                "IN predicates must use <column> [NOT] IN (<literal>,...) syntax",
            ));
        }
    };
    validate_sql_column_ref(column)?;
    if !tail.starts_with('(') || !tail.ends_with(')') {
        return Err(unsupported_sql_error(
            "IN predicates must use <column> [NOT] IN (<literal>,...) syntax",
        ));
    }
    let values_raw = tail[1..tail.len() - 1].trim();
    if values_raw.is_empty() {
        return Err(unsupported_sql_error(
            "IN predicates require at least one literal value",
        ));
    }
    if values_raw
        .get(..6)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("select"))
        && keyword_boundary(values_raw, 0, 6)
    {
        let predicate = parse_in_subquery_predicate(column, values_raw)?;
        if negated {
            return Ok(Some(ParsedPredicate::Not {
                inner: Box::new(predicate),
            }));
        }
        return Ok(Some(predicate));
    }
    if values_raw.ends_with(',') {
        return Err(unsupported_sql_error(
            "IN predicates require non-empty literal values",
        ));
    }
    let entries = split_sql_csv(values_raw)?;
    if entries.len() > MAX_IN_LIST_VALUES {
        return Err(unsupported_sql_error(&format!(
            "IN predicates admit at most {MAX_IN_LIST_VALUES} literal values in this scoped runtime slice"
        )));
    }
    let values = entries
        .iter()
        .map(|entry| parse_in_list_literal(entry))
        .collect::<Result<Vec<_>, ShardLoomError>>()?;
    let has_date = values
        .iter()
        .any(|value| matches!(value, ScalarValue::Date32(_)));
    let has_timestamp = values
        .iter()
        .any(|value| matches!(value, ScalarValue::TimestampMicros(_)));
    let has_non_date = values
        .iter()
        .any(|value| !matches!(value, ScalarValue::Date32(_) | ScalarValue::Null));
    let has_non_timestamp = values
        .iter()
        .any(|value| !matches!(value, ScalarValue::TimestampMicros(_) | ScalarValue::Null));
    if has_date && has_non_date {
        return Err(unsupported_sql_error(
            "IN predicates do not admit mixed DATE and non-DATE literal lists in this scoped runtime slice",
        ));
    }
    if has_timestamp && has_non_timestamp {
        return Err(unsupported_sql_error(
            "IN predicates do not admit mixed TIMESTAMP and non-TIMESTAMP literal lists in this scoped runtime slice",
        ));
    }
    let predicate = ParsedPredicate::InList {
        column: column.to_string(),
        values,
    };
    if negated {
        Ok(Some(ParsedPredicate::Not {
            inner: Box::new(predicate),
        }))
    } else {
        Ok(Some(predicate))
    }
}

fn strip_trailing_not_keyword(raw: &str) -> (&str, bool) {
    let trimmed = raw.trim_end();
    let lower = trimmed.to_ascii_lowercase();
    let Some(index) = lower.rfind("not") else {
        return (trimmed, false);
    };
    if keyword_boundary(trimmed, index, "not".len())
        && trimmed[index + "not".len()..].trim().is_empty()
    {
        (trimmed[..index].trim_end(), true)
    } else {
        (trimmed, false)
    }
}

fn parse_row_value_in_list_predicate(
    columns_raw: &str,
    tail: &str,
    negated: bool,
) -> Result<ParsedPredicate, ShardLoomError> {
    if !tail.starts_with('(') || !tail.ends_with(')') {
        return Err(unsupported_sql_error(
            "row-value IN predicates must use (<column>,...) [NOT] IN ((<literal>,...),...) syntax",
        ));
    }
    let columns = parse_row_value_in_columns(columns_raw)?;
    let values_raw = tail[1..tail.len() - 1].trim();
    if values_raw
        .get(..6)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("select"))
        && keyword_boundary(values_raw, 0, 6)
    {
        return parse_row_value_in_subquery_predicate(columns, values_raw, negated);
    }
    if values_raw.is_empty() {
        return Err(unsupported_sql_error(
            "row-value IN predicates require at least one literal tuple",
        ));
    }
    if values_raw.ends_with(',') {
        return Err(unsupported_sql_error(
            "row-value IN predicates require non-empty literal tuples",
        ));
    }
    let entries = split_sql_csv(values_raw)?;
    if entries.len() > MAX_IN_LIST_VALUES {
        return Err(unsupported_sql_error(&format!(
            "row-value IN predicates admit at most {MAX_IN_LIST_VALUES} literal tuples in this scoped runtime slice"
        )));
    }
    let tuples = entries
        .iter()
        .map(|entry| parse_row_value_in_tuple(entry, columns.len()))
        .collect::<Result<Vec<_>, ShardLoomError>>()?;
    validate_row_value_in_literal_tuples(&tuples)?;

    let predicate = ParsedPredicate::RowValueInList { columns, tuples };
    if negated {
        Ok(ParsedPredicate::Not {
            inner: Box::new(predicate),
        })
    } else {
        Ok(predicate)
    }
}

fn parse_row_value_in_subquery_predicate(
    columns: Vec<String>,
    raw: &str,
    negated: bool,
) -> Result<ParsedPredicate, ShardLoomError> {
    let raw = raw.trim();
    if is_projected_local_source_subquery(raw)? {
        return parse_projected_row_value_in_subquery_predicate(columns, raw, negated);
    }
    validate_in_subquery_shape(raw)?;
    let from_clause = find_keyword_outside_quotes_and_parentheses(raw, "from")?.ok_or_else(|| {
        unsupported_sql_error(
            "row-value IN subquery predicates admit SELECT <column>,... FROM <local-source> [WHERE <predicate>] [ORDER BY <column>] [LIMIT <n>] only",
        )
    })?;
    let filter_clause = find_keyword_outside_quotes_and_parentheses(raw, "where")?;
    let order_by_clause = find_keyword_outside_quotes_and_parentheses(raw, "order by")?;
    let limit_clause = find_keyword_outside_quotes_and_parentheses(raw, "limit")?;
    validate_in_subquery_clause_order(from_clause, filter_clause, order_by_clause, limit_clause)?;

    let source_ref = parse_local_subquery_source_ref(
        raw,
        from_clause,
        &[filter_clause, order_by_clause, limit_clause],
    )?;
    let source_columns =
        parse_in_subquery_selected_columns(raw, from_clause, source_ref.qualifier.as_deref())?;
    if source_columns.len() != columns.len() {
        return Err(unsupported_sql_error(
            "row-value IN subquery selected-column arity must match the source column count",
        ));
    }
    let predicate = parse_in_subquery_filter(
        raw,
        filter_clause,
        order_by_clause,
        limit_clause,
        source_ref.qualifier.as_deref(),
    )?;
    let order_by = parse_in_subquery_order_by(
        raw,
        order_by_clause,
        limit_clause,
        source_ref.qualifier.as_deref(),
    )?;
    let limit = parse_in_subquery_limit(raw, limit_clause)?;

    let predicate = ParsedPredicate::RowValueInSubquery {
        columns,
        subquery: Box::new(ParsedRowValueInSubquery {
            source_columns,
            source: ParsedRelationSource::Local(source_ref.leaf),
            source_qualifier: source_ref.qualifier,
            predicate: Box::new(predicate),
            order_by,
            limit,
            projected_plan: None,
            source_format: None,
            source_digest: None,
            input_row_count: 0,
            filtered_row_count: 0,
            tuples: Vec::new(),
        }),
    };
    if negated {
        Ok(ParsedPredicate::Not {
            inner: Box::new(predicate),
        })
    } else {
        Ok(predicate)
    }
}

fn parse_projected_row_value_in_subquery_predicate(
    columns: Vec<String>,
    raw: &str,
    negated: bool,
) -> Result<ParsedPredicate, ShardLoomError> {
    let projected_plan = parse_projected_subquery_plan(raw)?;
    let source_columns = projected_subquery_output_columns(&projected_plan)?;
    if source_columns.len() != columns.len() {
        return Err(unsupported_sql_error(
            "row-value IN projected subquery output arity must match the source column count",
        ));
    }
    let predicate = ParsedPredicate::RowValueInSubquery {
        columns,
        subquery: Box::new(ParsedRowValueInSubquery {
            source_columns,
            source: projected_plan.source.clone(),
            source_qualifier: None,
            predicate: Box::new(ParsedPredicate::All),
            order_by: projected_plan.order_by.clone(),
            limit: Some(projected_plan.limit),
            projected_plan: Some(Box::new(projected_plan)),
            source_format: None,
            source_digest: None,
            input_row_count: 0,
            filtered_row_count: 0,
            tuples: Vec::new(),
        }),
    };
    if negated {
        Ok(ParsedPredicate::Not {
            inner: Box::new(predicate),
        })
    } else {
        Ok(predicate)
    }
}

fn parse_row_value_in_columns(raw: &str) -> Result<Vec<String>, ShardLoomError> {
    let trimmed = raw.trim();
    if !trimmed.starts_with('(') || !trimmed.ends_with(')') {
        return Err(unsupported_sql_error(
            "row-value IN predicates must wrap source columns in parentheses",
        ));
    }
    let Some(close_index) = matching_closing_parenthesis(trimmed, 0)? else {
        return Err(unsupported_sql_error(
            "row-value IN source-column parentheses are not balanced",
        ));
    };
    if close_index != trimmed.len() - 1 {
        return Err(unsupported_sql_error(
            "row-value IN source columns must be a single parenthesized column list",
        ));
    }
    let inner = trimmed[1..trimmed.len() - 1].trim();
    if inner.is_empty() || inner.ends_with(',') {
        return Err(unsupported_sql_error(
            "row-value IN predicates require non-empty source columns",
        ));
    }
    let columns = split_sql_csv(inner)?;
    if columns.len() < 2 {
        return Err(unsupported_sql_error(
            "row-value IN predicates require at least two source columns; use scalar IN for one column",
        ));
    }
    let mut seen = BTreeSet::new();
    let mut parsed = Vec::with_capacity(columns.len());
    for column in columns {
        validate_sql_column_ref(&column)?;
        if !seen.insert(column.clone()) {
            return Err(unsupported_sql_error(
                "row-value IN source columns must be unique in this scoped runtime slice",
            ));
        }
        parsed.push(column);
    }
    Ok(parsed)
}

fn parse_row_value_in_tuple(
    raw: &str,
    expected_arity: usize,
) -> Result<Vec<ScalarValue>, ShardLoomError> {
    let trimmed = raw.trim();
    if !trimmed.starts_with('(') || !trimmed.ends_with(')') {
        return Err(unsupported_sql_error(
            "row-value IN literal values must be parenthesized tuples",
        ));
    }
    let Some(close_index) = matching_closing_parenthesis(trimmed, 0)? else {
        return Err(unsupported_sql_error(
            "row-value IN literal tuple parentheses are not balanced",
        ));
    };
    if close_index != trimmed.len() - 1 {
        return Err(unsupported_sql_error(
            "row-value IN literal values must be parenthesized tuples",
        ));
    }
    let inner = trimmed[1..trimmed.len() - 1].trim();
    if inner.is_empty() || inner.ends_with(',') {
        return Err(unsupported_sql_error(
            "row-value IN predicates require non-empty literal tuple values",
        ));
    }
    let entries = split_sql_csv(inner)?;
    if entries.len() != expected_arity {
        return Err(unsupported_sql_error(
            "row-value IN literal tuple arity must match the source column count",
        ));
    }
    entries
        .iter()
        .map(|entry| parse_in_list_literal(entry))
        .collect::<Result<Vec<_>, ShardLoomError>>()
}

fn validate_row_value_in_literal_tuples(tuples: &[Vec<ScalarValue>]) -> Result<(), ShardLoomError> {
    let Some(arity) = tuples.first().map(Vec::len) else {
        return Err(unsupported_sql_error(
            "row-value IN predicates require at least one literal tuple",
        ));
    };
    for column_index in 0..arity {
        let has_date = tuples.iter().any(|tuple| {
            tuple
                .get(column_index)
                .is_some_and(|value| matches!(value, ScalarValue::Date32(_)))
        });
        let has_timestamp = tuples.iter().any(|tuple| {
            tuple
                .get(column_index)
                .is_some_and(|value| matches!(value, ScalarValue::TimestampMicros(_)))
        });
        let has_non_date = tuples.iter().any(|tuple| {
            tuple
                .get(column_index)
                .is_some_and(|value| !matches!(value, ScalarValue::Date32(_) | ScalarValue::Null))
        });
        let has_non_timestamp = tuples.iter().any(|tuple| {
            tuple.get(column_index).is_some_and(|value| {
                !matches!(value, ScalarValue::TimestampMicros(_) | ScalarValue::Null)
            })
        });
        if has_date && has_non_date {
            return Err(unsupported_sql_error(
                "row-value IN predicates do not admit mixed DATE and non-DATE literals at the same tuple position in this scoped runtime slice",
            ));
        }
        if has_timestamp && has_non_timestamp {
            return Err(unsupported_sql_error(
                "row-value IN predicates do not admit mixed TIMESTAMP and non-TIMESTAMP literals at the same tuple position in this scoped runtime slice",
            ));
        }
    }
    Ok(())
}

fn parse_in_subquery_predicate(column: &str, raw: &str) -> Result<ParsedPredicate, ShardLoomError> {
    let raw = raw.trim();
    if is_projected_local_source_subquery(raw)? {
        return parse_projected_in_subquery_predicate(column, raw);
    }
    validate_in_subquery_shape(raw)?;
    let from_clause = find_keyword_outside_quotes_and_parentheses(raw, "from")?.ok_or_else(|| {
        unsupported_sql_error(
            "IN subquery predicates admit SELECT <column> FROM <local-source> [WHERE <predicate>] [ORDER BY <column>] [LIMIT <n>] only",
        )
    })?;
    let filter_clause = find_keyword_outside_quotes_and_parentheses(raw, "where")?;
    let order_by_clause = find_keyword_outside_quotes_and_parentheses(raw, "order by")?;
    let limit_clause = find_keyword_outside_quotes_and_parentheses(raw, "limit")?;
    validate_in_subquery_clause_order(from_clause, filter_clause, order_by_clause, limit_clause)?;

    let source_ref = parse_local_subquery_source_ref(
        raw,
        from_clause,
        &[filter_clause, order_by_clause, limit_clause],
    )?;
    let select_column =
        parse_in_subquery_selected_column(raw, from_clause, source_ref.qualifier.as_deref())?;
    let predicate = parse_in_subquery_filter(
        raw,
        filter_clause,
        order_by_clause,
        limit_clause,
        source_ref.qualifier.as_deref(),
    )?;
    let order_by = parse_in_subquery_order_by(
        raw,
        order_by_clause,
        limit_clause,
        source_ref.qualifier.as_deref(),
    )?;
    let limit = parse_in_subquery_limit(raw, limit_clause)?;

    Ok(ParsedPredicate::InSubquery {
        column: column.to_string(),
        subquery: Box::new(ParsedInSubquery {
            source_column: select_column.clone(),
            source: ParsedRelationSource::Local(source_ref.leaf),
            source_qualifier: source_ref.qualifier,
            predicate: Box::new(predicate),
            order_by,
            limit,
            projected_plan: None,
            source_format: None,
            source_digest: None,
            input_row_count: 0,
            filtered_row_count: 0,
            values: Vec::new(),
        }),
    })
}

fn parse_projected_in_subquery_predicate(
    column: &str,
    raw: &str,
) -> Result<ParsedPredicate, ShardLoomError> {
    let projected_plan = parse_projected_subquery_plan(raw)?;
    let output_columns = projected_subquery_output_columns(&projected_plan)?;
    let [source_column] = output_columns.as_slice() else {
        return Err(unsupported_sql_error(
            "scalar IN projected subqueries must produce exactly one output column",
        ));
    };
    Ok(ParsedPredicate::InSubquery {
        column: column.to_string(),
        subquery: Box::new(ParsedInSubquery {
            source_column: source_column.clone(),
            source: projected_plan.source.clone(),
            source_qualifier: None,
            predicate: Box::new(ParsedPredicate::All),
            order_by: projected_plan.order_by.clone(),
            limit: Some(projected_plan.limit),
            projected_plan: Some(Box::new(projected_plan)),
            source_format: None,
            source_digest: None,
            input_row_count: 0,
            filtered_row_count: 0,
            values: Vec::new(),
        }),
    })
}

fn is_projected_local_source_subquery(raw: &str) -> Result<bool, ShardLoomError> {
    validate_in_subquery_select_prefix(raw)?;
    if find_keyword_outside_quotes_and_parentheses(raw, "join")?.is_some()
        || find_keyword_outside_quotes_and_parentheses(raw, "group by")?.is_some()
        || find_keyword_outside_quotes_and_parentheses(raw, "having")?.is_some()
    {
        return Ok(true);
    }
    let from = find_keyword_outside_quotes_and_parentheses(raw, "from")?
        .ok_or_else(|| unsupported_sql_error("source subquery requires FROM"))?;
    if raw[from + "from".len()..].trim_start().starts_with('(') {
        return Ok(true);
    }
    let select = raw["select".len()..from].trim();
    if find_keyword_outside_quotes_and_parentheses(select, "distinct")? == Some(0) {
        return Ok(true);
    }
    for expression in split_sql_csv(select)? {
        if find_keyword_outside_quotes_and_parentheses(&expression, "as")?.is_some()
            || parse_aggregate_projection(&expression)?.is_some()
        {
            return Ok(true);
        }
    }
    Ok(false)
}

fn parse_projected_subquery_plan(raw: &str) -> Result<ParsedSqlLocalSource, ShardLoomError> {
    let statement = bounded_projected_subquery_statement(raw)?;
    let mut projected_plan = parse_sql_local_source_statement(&statement)?;
    projected_plan.limit_is_synthetic =
        find_keyword_outside_quotes_and_parentheses(raw, "limit")?.is_none();
    let _output_columns = projected_subquery_output_columns(&projected_plan)?;
    validate_projected_subquery_outer_correlation_shapes(&projected_plan, "IN")?;
    Ok(projected_plan)
}

fn validate_projected_subquery_outer_correlation_shapes(
    projected_plan: &ParsedSqlLocalSource,
    subquery_kind: &str,
) -> Result<(), ShardLoomError> {
    validate_outer_correlation_shapes(&projected_plan.predicate, subquery_kind)?;
    validate_outer_correlation_shapes(&projected_plan.having, subquery_kind)
}

fn bounded_projected_subquery_statement(raw: &str) -> Result<String, ShardLoomError> {
    let statement = normalize_and_validate_sql_statement(raw)?;
    if find_keyword_outside_quotes_and_parentheses(&statement, "limit")?.is_some() {
        Ok(statement)
    } else {
        Ok(format!("{statement} LIMIT {MAX_IN_LIST_VALUES}"))
    }
}

fn projected_exists_subquery_projection(
    parsed: &ParsedSqlLocalSource,
) -> Result<(ParsedExistsSubqueryProjectionKind, Vec<String>), ShardLoomError> {
    if parsed.projection_order.len() == 1 {
        match parsed.projection_order.first() {
            Some(ParsedProjectionOutput::Raw(column)) if column == "*" => {
                return Ok((ParsedExistsSubqueryProjectionKind::Wildcard, Vec::new()));
            }
            Some(ParsedProjectionOutput::Literal(_)) => {
                return Ok((ParsedExistsSubqueryProjectionKind::Literal, Vec::new()));
            }
            _ => {}
        }
    }
    let output_columns = projected_subquery_output_columns(parsed).map_err(|_| {
        unsupported_sql_error(
            "projected EXISTS subqueries admit SELECT *, a scalar literal, or explicit output columns",
        )
    })?;
    if output_columns.is_empty() {
        return Err(unsupported_sql_error(
            "projected EXISTS subqueries require at least one output column, SELECT *, or a scalar literal",
        ));
    }
    Ok((
        ParsedExistsSubqueryProjectionKind::ColumnList,
        output_columns,
    ))
}

fn projected_subquery_output_columns(
    parsed: &ParsedSqlLocalSource,
) -> Result<Vec<String>, ShardLoomError> {
    if parsed
        .projection_order
        .iter()
        .any(|output| matches!(output, ParsedProjectionOutput::Raw(column) if column == "*"))
    {
        return Err(unsupported_sql_error(
            "projected IN subqueries require explicit output columns; SELECT * is not admitted for membership materialization",
        ));
    }
    if parsed.has_complex_projection() {
        return Err(unsupported_sql_error(
            "projected subqueries do not admit ARRAY or STRUCT projection outputs for membership materialization; scoped complex equality is limited to SELECT DISTINCT and UNION DISTINCT result-boundary rows",
        ));
    }
    let columns = if parsed.is_grouped_aggregate() && parsed.grouping_sets.is_none() {
        parsed
            .group_by
            .iter()
            .cloned()
            .chain(parsed.aggregates.iter().map(ParsedAggregate::output_name))
            .collect()
    } else if parsed.is_aggregate() && parsed.grouping_sets.is_none() {
        parsed
            .aggregates
            .iter()
            .map(ParsedAggregate::output_name)
            .collect()
    } else {
        parsed
            .projection_order
            .iter()
            .map(|output| match output {
                ParsedProjectionOutput::Raw(column)
                | ParsedProjectionOutput::Aggregate(column)
                | ParsedProjectionOutput::Literal(column)
                | ParsedProjectionOutput::Complex(column)
                | ParsedProjectionOutput::Cast(column)
                | ParsedProjectionOutput::NullCoalesce(column)
                | ParsedProjectionOutput::NullIf(column)
                | ParsedProjectionOutput::Conditional(column)
                | ParsedProjectionOutput::Predicate(column)
                | ParsedProjectionOutput::NumericArithmetic(column)
                | ParsedProjectionOutput::NumericAbs(column)
                | ParsedProjectionOutput::NumericRounding(column)
                | ParsedProjectionOutput::GenericExpression(column)
                | ParsedProjectionOutput::DateArithmetic(column)
                | ParsedProjectionOutput::TimestampArithmetic(column)
                | ParsedProjectionOutput::StringLength(column)
                | ParsedProjectionOutput::StringTransform(column)
                | ParsedProjectionOutput::StringFunction(column)
                | ParsedProjectionOutput::BinaryHelper(column)
                | ParsedProjectionOutput::BinaryByteLength(column)
                | ParsedProjectionOutput::DateExtract(column)
                | ParsedProjectionOutput::TimestampExtract(column)
                | ParsedProjectionOutput::Window(column) => column.clone(),
            })
            .collect()
    };
    Ok(columns)
}

fn parse_quantified_subquery_predicate(
    raw: &str,
) -> Result<Option<ParsedPredicate>, ShardLoomError> {
    let Some((comparison_index, comparison_raw)) = find_top_level_comparison_operator(raw)? else {
        return Ok(None);
    };
    let column = raw[..comparison_index].trim();
    let tail = raw[comparison_index + comparison_raw.len()..].trim_start();
    let Some((quantifier, quantifier_len)) = parse_quantified_subquery_quantifier_prefix(tail)
    else {
        return Ok(None);
    };
    validate_sql_column_ref(column).map_err(|_| {
        unsupported_sql_error(
            "ANY and ALL subquery predicates admit <column> <comparison> ANY|ALL (SELECT <column> FROM <local-source> [WHERE <predicate>] [ORDER BY <column>] [LIMIT <n>]) only",
        )
    })?;
    let tail = tail[quantifier_len..].trim_start();
    if !tail.starts_with('(') {
        return Err(unsupported_sql_error(
            "ANY and ALL subquery predicates require ANY|ALL (SELECT <column> FROM <local-source> [WHERE <predicate>] [ORDER BY <column>] [LIMIT <n>])",
        ));
    }
    let Some(close_index) = matching_closing_parenthesis(tail, 0)? else {
        return Err(unsupported_sql_error(
            "ANY and ALL subquery predicate parentheses must be balanced",
        ));
    };
    if close_index != tail.len() - 1 {
        return Err(unsupported_sql_error(
            "ANY and ALL subquery predicates admit one parenthesized SELECT subquery only",
        ));
    }
    let comparison = parse_comparison_op(comparison_raw)?;
    let parsed_subquery = parse_in_subquery_predicate(column, &tail[1..close_index])?;
    let ParsedPredicate::InSubquery { subquery, .. } = parsed_subquery else {
        return Err(ShardLoomError::InvalidOperation(
            "internal error: quantified subquery parser did not produce a scalar subquery"
                .to_string(),
        ));
    };
    Ok(Some(ParsedPredicate::QuantifiedSubquery {
        column: column.to_string(),
        comparison,
        quantifier,
        subquery,
    }))
}

fn parse_quantified_subquery_quantifier_prefix(
    raw: &str,
) -> Option<(ParsedQuantifiedSubqueryQuantifier, usize)> {
    for (keyword, quantifier) in [
        ("any", ParsedQuantifiedSubqueryQuantifier::Any),
        ("all", ParsedQuantifiedSubqueryQuantifier::All),
    ] {
        if raw
            .get(..keyword.len())
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case(keyword))
            && keyword_boundary(raw, 0, keyword.len())
        {
            return Some((quantifier, keyword.len()));
        }
    }
    None
}

fn parse_exists_subquery_predicate(raw: &str) -> Result<Option<ParsedPredicate>, ShardLoomError> {
    let trimmed = raw.trim_start();
    if !trimmed
        .get(.."exists".len())
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("exists"))
        || !keyword_boundary(trimmed, 0, "exists".len())
    {
        return Ok(None);
    }
    let tail = trimmed["exists".len()..].trim_start();
    if !tail.starts_with('(') {
        return Err(unsupported_sql_error(
            "EXISTS subquery predicates require EXISTS (SELECT <projection> FROM <local-source> [WHERE <predicate>] [ORDER BY <column>] [LIMIT <n>])",
        ));
    }
    let Some(close_index) = matching_closing_parenthesis(tail, 0)? else {
        return Err(unsupported_sql_error(
            "EXISTS subquery predicate parentheses are not balanced",
        ));
    };
    if close_index != tail.len() - 1 {
        return Err(unsupported_sql_error(
            "EXISTS subquery predicates admit one parenthesized SELECT subquery only",
        ));
    }
    let subquery = parse_exists_subquery(&tail[1..close_index])?;
    Ok(Some(ParsedPredicate::ExistsSubquery {
        subquery: Box::new(subquery),
    }))
}

fn parse_exists_subquery(raw: &str) -> Result<ParsedExistsSubquery, ShardLoomError> {
    let raw = raw.trim();
    if is_projected_local_source_subquery(raw)? {
        return parse_projected_exists_subquery(raw);
    }
    validate_exists_subquery_shape(raw)?;
    let from_clause = find_keyword_outside_quotes_and_parentheses(raw, "from")?.ok_or_else(|| {
        unsupported_sql_error(
            "EXISTS subquery predicates admit SELECT <projection> FROM <local-source> [WHERE <predicate>] [ORDER BY <column>] [LIMIT <n>] only",
        )
    })?;
    let filter_clause = find_keyword_outside_quotes_and_parentheses(raw, "where")?;
    let order_by_clause = find_keyword_outside_quotes_and_parentheses(raw, "order by")?;
    let limit_clause = find_keyword_outside_quotes_and_parentheses(raw, "limit")?;
    validate_in_subquery_clause_order(from_clause, filter_clause, order_by_clause, limit_clause)?;

    let source_ref = parse_local_subquery_source_ref(
        raw,
        from_clause,
        &[filter_clause, order_by_clause, limit_clause],
    )?;
    let (projection_kind, selected_columns) =
        parse_exists_subquery_projection(raw, from_clause, source_ref.qualifier.as_deref())?;
    let predicate = parse_exists_subquery_filter(
        raw,
        filter_clause,
        order_by_clause,
        limit_clause,
        source_ref.qualifier.as_deref(),
    )?;
    let order_by = parse_in_subquery_order_by(
        raw,
        order_by_clause,
        limit_clause,
        source_ref.qualifier.as_deref(),
    )?;
    let limit = parse_exists_subquery_limit(raw, limit_clause)?;

    Ok(ParsedExistsSubquery {
        projection_kind,
        selected_columns,
        source: ParsedRelationSource::Local(source_ref.leaf),
        source_qualifier: source_ref.qualifier,
        predicate: Box::new(predicate),
        order_by,
        limit,
        projected_plan: None,
        source_format: None,
        source_digest: None,
        input_row_count: 0,
        filtered_row_count: 0,
        bounded_row_count: 0,
        exists: false,
    })
}

fn parse_projected_exists_subquery(raw: &str) -> Result<ParsedExistsSubquery, ShardLoomError> {
    let from = find_keyword_outside_quotes_and_parentheses(raw, "from")?
        .ok_or_else(|| unsupported_sql_error("EXISTS source subquery requires FROM"))?;
    let selected = raw["select".len()..from].trim();
    // EXISTS observes presence, including for NULL or other admitted constants.
    // Give a bare literal an internal output name before the general SELECT
    // parser, which otherwise requires every computed output to have an alias.
    let literal_statement;
    let parsed_raw = if parse_projection_literal_value(selected).is_ok() {
        literal_statement = format!("SELECT 1 AS __shardloom_exists_literal {}", &raw[from..]);
        literal_statement.as_str()
    } else {
        raw
    };
    let statement = bounded_projected_subquery_statement(parsed_raw)?;
    let mut projected_plan = parse_sql_local_source_statement(&statement)?;
    projected_plan.limit_is_synthetic =
        find_keyword_outside_quotes_and_parentheses(raw, "limit")?.is_none();
    validate_projected_subquery_outer_correlation_shapes(&projected_plan, "EXISTS")?;
    let (projection_kind, selected_columns) =
        projected_exists_subquery_projection(&projected_plan)?;
    Ok(ParsedExistsSubquery {
        projection_kind,
        selected_columns,
        source: projected_plan.source.clone(),
        source_qualifier: None,
        predicate: Box::new(ParsedPredicate::All),
        order_by: projected_plan.order_by.clone(),
        limit: Some(projected_plan.limit),
        projected_plan: Some(Box::new(projected_plan)),
        source_format: None,
        source_digest: None,
        input_row_count: 0,
        filtered_row_count: 0,
        bounded_row_count: 0,
        exists: false,
    })
}

fn validate_in_subquery_shape(raw: &str) -> Result<(), ShardLoomError> {
    validate_in_subquery_select_prefix(raw)?;
    if find_keyword_outside_quotes_and_parentheses(&raw["select".len()..], "select")?.is_some() {
        return Err(unsupported_sql_error(
            "nested IN subqueries are not admitted by the current advanced predicate profile",
        ));
    }
    if find_keyword_outside_quotes_and_parentheses(raw, "join")?.is_some() {
        return Err(unsupported_sql_error(
            "non-projected joined IN subqueries are not admitted by the current advanced predicate profile; use an explicit projected local-source SELECT with matching output arity",
        ));
    }
    if find_keyword_outside_quotes_and_parentheses(raw, "group by")?.is_some()
        || find_keyword_outside_quotes_and_parentheses(raw, "having")?.is_some()
    {
        return Err(unsupported_sql_error(
            "non-projected grouped or HAVING IN subqueries are not admitted by the current advanced predicate profile; use an explicit projected local-source SELECT with matching output arity",
        ));
    }
    Ok(())
}

fn validate_in_subquery_select_prefix(raw: &str) -> Result<(), ShardLoomError> {
    if !raw
        .get(.."select".len())
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("select"))
        || !keyword_boundary(raw, 0, "select".len())
    {
        return Err(unsupported_sql_error(
            "IN subquery predicates admit SELECT <column> FROM <local-source> [WHERE <predicate>] [ORDER BY <column>] [LIMIT <n>] only",
        ));
    }
    if find_keyword_outside_quotes_and_parentheses(&raw["select".len()..], "select")?.is_some() {
        return Err(unsupported_sql_error(
            "top-level multiple SELECT subqueries are not admitted by the current advanced predicate profile",
        ));
    }
    Ok(())
}

fn validate_exists_subquery_shape(raw: &str) -> Result<(), ShardLoomError> {
    if !raw
        .get(.."select".len())
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("select"))
        || !keyword_boundary(raw, 0, "select".len())
    {
        return Err(unsupported_sql_error(
            "EXISTS subquery predicates admit SELECT <projection> FROM <local-source> [WHERE <predicate>] [ORDER BY <column>] [LIMIT <n>] only",
        ));
    }
    if find_keyword_outside_quotes_and_parentheses(&raw["select".len()..], "select")?.is_some() {
        return Err(unsupported_sql_error(
            "nested EXISTS subqueries are not admitted by the current advanced predicate profile",
        ));
    }
    if find_keyword_outside_quotes_and_parentheses(raw, "join")?.is_some() {
        return Err(unsupported_sql_error(
            "joined EXISTS subqueries are not admitted by the current advanced predicate profile; materialize the joined source explicitly before this scoped runtime path",
        ));
    }
    if find_keyword_outside_quotes_and_parentheses(raw, "group by")?.is_some()
        || find_keyword_outside_quotes_and_parentheses(raw, "having")?.is_some()
    {
        return Err(unsupported_sql_error(
            "grouped and HAVING EXISTS subqueries are not admitted by the current advanced predicate profile; only bounded local projection subqueries are admitted",
        ));
    }
    Ok(())
}

fn parse_exists_subquery_projection(
    raw: &str,
    from_clause: usize,
    source_qualifier: Option<&str>,
) -> Result<(ParsedExistsSubqueryProjectionKind, Vec<String>), ShardLoomError> {
    if from_clause <= "select".len() {
        return Err(unsupported_sql_error(
            "EXISTS subquery predicates require a selected projection",
        ));
    }
    let select_raw = raw["select".len()..from_clause].trim();
    if select_raw.is_empty() {
        return Err(unsupported_sql_error(
            "EXISTS subquery predicates require a selected projection",
        ));
    }
    if select_raw == "*" {
        return Ok((ParsedExistsSubqueryProjectionKind::Wildcard, Vec::new()));
    }
    if parse_projection_literal_value(select_raw).is_ok() {
        return Ok((ParsedExistsSubqueryProjectionKind::Literal, Vec::new()));
    }
    if select_raw.starts_with('(') {
        return Err(unsupported_sql_error(
            "EXISTS subquery selected projection must be *, a scalar literal, or a plain column list",
        ));
    }
    let columns = split_sql_csv(select_raw)?;
    if columns.is_empty() || columns.iter().any(|column| column.trim().is_empty()) {
        return Err(unsupported_sql_error(
            "EXISTS subquery predicates require a selected projection",
        ));
    }
    let mut seen = BTreeSet::new();
    let mut parsed = Vec::with_capacity(columns.len());
    for column in columns {
        let normalized = normalize_local_subquery_column_ref(
            &column,
            source_qualifier,
            "EXISTS subquery projection",
            false,
        )?;
        if !seen.insert(normalized.clone()) {
            return Err(unsupported_sql_error(
                "EXISTS subquery selected columns must be unique in this scoped runtime slice",
            ));
        }
        parsed.push(normalized);
    }
    Ok((ParsedExistsSubqueryProjectionKind::ColumnList, parsed))
}

fn parse_in_subquery_selected_column(
    raw: &str,
    from_clause: usize,
    source_qualifier: Option<&str>,
) -> Result<String, ShardLoomError> {
    let columns = parse_in_subquery_selected_columns(raw, from_clause, source_qualifier)?;
    let [select_column] = columns.as_slice() else {
        return Err(unsupported_sql_error(
            "multi-column IN subqueries require row-value source columns; scalar IN subqueries select exactly one source column",
        ));
    };
    Ok(select_column.clone())
}

fn parse_in_subquery_selected_columns(
    raw: &str,
    from_clause: usize,
    source_qualifier: Option<&str>,
) -> Result<Vec<String>, ShardLoomError> {
    if from_clause <= "select".len() {
        return Err(unsupported_sql_error(
            "IN subquery predicates require selected source columns",
        ));
    }
    let select_raw = raw["select".len()..from_clause].trim();
    if select_raw.starts_with('(') {
        return Err(unsupported_sql_error(
            "IN subquery selected columns must be a plain SELECT column list, not a row-constructor expression",
        ));
    }
    reject_complex_subquery_membership_projection(select_raw)?;
    let columns = split_sql_csv(select_raw)?;
    if columns.is_empty() || columns.iter().any(|column| column.trim().is_empty()) {
        return Err(unsupported_sql_error(
            "IN subquery predicates require selected source columns",
        ));
    }
    let mut seen = BTreeSet::new();
    let mut parsed = Vec::with_capacity(columns.len());
    for column in columns {
        let normalized = normalize_local_subquery_column_ref(
            &column,
            source_qualifier,
            "IN subquery selected columns",
            false,
        )?;
        if !seen.insert(normalized.clone()) {
            return Err(unsupported_sql_error(
                "IN subquery selected columns must be unique in this scoped runtime slice",
            ));
        }
        parsed.push(normalized);
    }
    Ok(parsed)
}

fn reject_complex_subquery_membership_projection(select_raw: &str) -> Result<(), ShardLoomError> {
    if let Ok(projection_list) = parse_projection_list(select_raw)
        && !projection_list.complex_projections.is_empty()
    {
        return Err(unsupported_sql_error(
            "projected subqueries do not admit ARRAY or STRUCT projection outputs for membership materialization; scoped complex equality is limited to SELECT DISTINCT and UNION DISTINCT result-boundary rows",
        ));
    }
    Ok(())
}

fn parse_local_subquery_source_ref(
    raw: &str,
    from_clause: usize,
    optional_clauses: &[Option<usize>],
) -> Result<ParsedLocalSubquerySourceRef, ShardLoomError> {
    let source_end = earliest_optional_clause_index_after(from_clause, optional_clauses, raw.len());
    let source_raw = raw[from_clause + "from".len()..source_end].trim();
    if source_raw.is_empty() || split_sql_csv(source_raw)?.len() != 1 {
        return Err(unsupported_sql_error(
            "local subquery predicates admit exactly one local source path with optional AS <alias>",
        ));
    }
    let (source, explicit_alias) = relation_sources::parse_source(source_raw, false)?;
    let ParsedRelationSource::Local(leaf) = source else {
        return Err(unsupported_sql_error(
            "derived subqueries require the shared projected relation parser",
        ));
    };
    let qualifier = if let Some(alias) = explicit_alias {
        Some(alias)
    } else if leaf.memory_input.is_none() {
        inferred_local_source_qualifier(&leaf.path)?
    } else {
        None
    };
    Ok(ParsedLocalSubquerySourceRef { leaf, qualifier })
}

fn inferred_local_source_qualifier(source_path: &Path) -> Result<Option<String>, ShardLoomError> {
    let Some(stem) = source_path.file_stem().and_then(|stem| stem.to_str()) else {
        return Ok(None);
    };
    if validate_sql_identifier(stem).is_err() {
        return Ok(None);
    }
    if stem.eq_ignore_ascii_case(OUTER_CORRELATION_ALIAS) {
        return Err(unsupported_sql_error(
            "local subquery inferred source qualifier 'outer' is reserved for correlated outer-row references; use an explicit non-reserved AS <alias>",
        ));
    }
    Ok(Some(stem.to_string()))
}

fn normalize_local_subquery_column_ref(
    column_ref: &str,
    source_qualifier: Option<&str>,
    context: &str,
    allow_outer: bool,
) -> Result<String, ShardLoomError> {
    if !column_ref.contains('.') {
        validate_sql_identifier(column_ref)?;
        return Ok(column_ref.to_string());
    }
    let qualified = parse_qualified_column_ref(column_ref)?;
    if source_qualifier.is_some_and(|alias| qualified.alias.eq_ignore_ascii_case(alias)) {
        return Ok(qualified.column);
    }
    if allow_outer && qualified.alias == OUTER_CORRELATION_ALIAS {
        return Ok(column_ref.to_string());
    }
    let admitted = if allow_outer {
        "the subquery source qualifier or outer.<column>"
    } else {
        "the subquery source qualifier"
    };
    Err(unsupported_sql_error(&format!(
        "qualified {context} references admit only {admitted} in this scoped runtime slice; use FROM <local-source> AS <alias> or a SQL-identifier file stem for source-qualified local columns"
    )))
}

fn normalize_sql_source_qualifier_refs(
    raw: &str,
    source_qualifier: Option<&str>,
) -> Result<String, ShardLoomError> {
    let Some(source_qualifier) = source_qualifier else {
        return Ok(raw.to_string());
    };
    let mut output = String::with_capacity(raw.len());
    let mut index = 0;
    let mut in_quote = false;
    let mut skip_select_depth = 0_u32;
    while index < raw.len() {
        let ch = raw[index..]
            .chars()
            .next()
            .expect("index remains on a char boundary");
        if ch == '\'' {
            push_sql_quote(raw, &mut index, &mut in_quote, &mut output);
            continue;
        }
        if in_quote {
            output.push(ch);
            index += ch.len_utf8();
            continue;
        }
        if ch == '(' {
            output.push(ch);
            index += ch.len_utf8();
            if skip_select_depth > 0 {
                skip_select_depth += 1;
            } else if starts_with_keyword(raw[index..].trim_start(), "select") {
                skip_select_depth = 1;
            }
            continue;
        }
        if ch == ')' {
            output.push(ch);
            index += ch.len_utf8();
            skip_select_depth = skip_select_depth.saturating_sub(1);
            continue;
        }
        if skip_select_depth > 0 {
            output.push(ch);
            index += ch.len_utf8();
            continue;
        }
        if !is_identifier_start(ch) {
            output.push(ch);
            index += ch.len_utf8();
            continue;
        }
        index = push_normalized_source_qualified_ref(raw, index, source_qualifier, &mut output);
    }
    if in_quote {
        return Err(unsupported_sql_error("SQL string literal is not closed"));
    }
    Ok(output)
}

fn push_sql_quote(raw: &str, index: &mut usize, in_quote: &mut bool, output: &mut String) {
    output.push('\'');
    *index += 1;
    if *in_quote && raw[*index..].starts_with('\'') {
        output.push('\'');
        *index += 1;
    } else {
        *in_quote = !*in_quote;
    }
}

fn push_normalized_source_qualified_ref(
    raw: &str,
    alias_start: usize,
    source_qualifier: &str,
    output: &mut String,
) -> usize {
    let dot_index = consume_identifier(raw, alias_start);
    if !raw[dot_index..].starts_with('.') {
        output.push_str(&raw[alias_start..dot_index]);
        return dot_index;
    }

    let column_start = dot_index + 1;
    let Some(column_first) = raw[column_start..].chars().next() else {
        output.push_str(&raw[alias_start..=dot_index]);
        return column_start;
    };
    if !is_identifier_start(column_first) {
        output.push_str(&raw[alias_start..=dot_index]);
        return column_start;
    }

    let column_end = consume_identifier(raw, column_start);
    let alias = &raw[alias_start..dot_index];
    let column = &raw[column_start..column_end];
    if alias.eq_ignore_ascii_case(source_qualifier) {
        output.push_str(column);
    } else {
        output.push_str(&raw[alias_start..column_end]);
    }
    column_end
}

fn consume_identifier(raw: &str, mut index: usize) -> usize {
    while index < raw.len() {
        let next = raw[index..]
            .chars()
            .next()
            .expect("index remains on a char boundary");
        if !is_identifier_char(next) {
            break;
        }
        index += next.len_utf8();
    }
    index
}

fn validate_subquery_order_by_qualified_columns(
    order_by: Option<&ParsedOrderBy>,
) -> Result<(), ShardLoomError> {
    let Some(order_by) = order_by else {
        return Ok(());
    };
    for key in &order_by.keys {
        if key.column.contains('.') {
            return Err(unsupported_sql_error(
                "qualified local subquery ORDER BY references admit only the subquery source qualifier in this scoped runtime slice",
            ));
        }
    }
    Ok(())
}

fn parse_in_subquery_filter(
    raw: &str,
    filter_clause: Option<usize>,
    order_by_clause: Option<usize>,
    limit_clause: Option<usize>,
    source_qualifier: Option<&str>,
) -> Result<ParsedPredicate, ShardLoomError> {
    if let Some(index) = filter_clause {
        let end = earliest_optional_clause_index_after(
            index,
            &[order_by_clause, limit_clause],
            raw.len(),
        );
        let predicate_raw = raw[index + "where".len()..end].trim();
        if predicate_raw.is_empty() {
            return Err(unsupported_sql_error(
                "IN subquery WHERE predicates must not be empty",
            ));
        }
        let predicate_raw = normalize_sql_source_qualifier_refs(predicate_raw, source_qualifier)?;
        let predicate = parse_predicate(&predicate_raw)?;
        validate_in_subquery_filter_predicate(&predicate)?;
        Ok(predicate)
    } else {
        Ok(ParsedPredicate::All)
    }
}

fn parse_exists_subquery_filter(
    raw: &str,
    filter_clause: Option<usize>,
    order_by_clause: Option<usize>,
    limit_clause: Option<usize>,
    source_qualifier: Option<&str>,
) -> Result<ParsedPredicate, ShardLoomError> {
    if let Some(index) = filter_clause {
        let end = earliest_optional_clause_index_after(
            index,
            &[order_by_clause, limit_clause],
            raw.len(),
        );
        let predicate_raw = raw[index + "where".len()..end].trim();
        if predicate_raw.is_empty() {
            return Err(unsupported_sql_error(
                "EXISTS subquery WHERE predicates must not be empty",
            ));
        }
        let predicate_raw = normalize_sql_source_qualifier_refs(predicate_raw, source_qualifier)?;
        let predicate = parse_predicate(&predicate_raw)?;
        validate_exists_subquery_filter_predicate(&predicate)?;
        Ok(predicate)
    } else {
        Ok(ParsedPredicate::All)
    }
}

fn parse_exists_subquery_limit(
    raw: &str,
    limit_clause: Option<usize>,
) -> Result<Option<usize>, ShardLoomError> {
    if let Some(index) = limit_clause {
        let limit_raw = raw[index + "limit".len()..].trim();
        Ok(Some(parse_limit(limit_raw)?))
    } else {
        Ok(None)
    }
}

fn parse_in_subquery_order_by(
    raw: &str,
    order_by_clause: Option<usize>,
    limit_clause: Option<usize>,
    source_qualifier: Option<&str>,
) -> Result<Option<ParsedOrderBy>, ShardLoomError> {
    let order_by = if let Some(index) = order_by_clause {
        let end = limit_clause.unwrap_or(raw.len());
        let order_by_raw = raw[index + "order by".len()..end].trim();
        let order_by_raw = normalize_sql_source_qualifier_refs(order_by_raw, source_qualifier)?;
        let order_by = parse_order_by(Some(&order_by_raw))?;
        validate_subquery_order_by_qualified_columns(order_by.as_ref())?;
        order_by
    } else {
        None
    };
    Ok(order_by)
}

fn parse_in_subquery_limit(
    raw: &str,
    limit_clause: Option<usize>,
) -> Result<Option<usize>, ShardLoomError> {
    let limit = if let Some(index) = limit_clause {
        let limit_raw = raw[index + "limit".len()..].trim();
        let limit = parse_limit(limit_raw)?;
        Some(limit)
    } else {
        None
    };
    Ok(limit)
}

fn validate_in_subquery_clause_order(
    from_clause: usize,
    filter_clause: Option<usize>,
    order_by_clause: Option<usize>,
    limit_clause: Option<usize>,
) -> Result<(), ShardLoomError> {
    if filter_clause.is_some_and(|index| index <= from_clause)
        || order_by_clause.is_some_and(|index| index <= from_clause)
        || limit_clause.is_some_and(|index| index <= from_clause)
        || filter_clause
            .zip(order_by_clause)
            .is_some_and(|(filter, order_by)| filter > order_by)
        || filter_clause
            .zip(limit_clause)
            .is_some_and(|(filter, limit)| filter > limit)
        || order_by_clause
            .zip(limit_clause)
            .is_some_and(|(order_by, limit)| order_by > limit)
    {
        return Err(unsupported_sql_error(
            "IN subquery predicates require SELECT <column> FROM <local-source> [WHERE <predicate>] [ORDER BY <column>] [LIMIT <n>] clause order",
        ));
    }
    Ok(())
}

fn validate_exists_subquery_filter_predicate(
    predicate: &ParsedPredicate,
) -> Result<(), ShardLoomError> {
    if predicate.uses_generic_expression() {
        return Err(unsupported_sql_error(
            "EXISTS subquery WHERE predicates do not admit generic expression trees in this scoped runtime slice",
        ));
    }
    validate_subquery_qualified_columns(predicate, "EXISTS")?;
    Ok(())
}

fn validate_in_subquery_filter_predicate(
    predicate: &ParsedPredicate,
) -> Result<(), ShardLoomError> {
    if predicate.uses_generic_expression() {
        return Err(unsupported_sql_error(
            "IN subquery WHERE predicates do not admit generic expression trees in this scoped runtime slice",
        ));
    }
    validate_subquery_qualified_columns(predicate, "IN")?;
    Ok(())
}

fn validate_subquery_qualified_columns(
    predicate: &ParsedPredicate,
    subquery_kind: &str,
) -> Result<(), ShardLoomError> {
    for column in predicate.columns() {
        if !column.contains('.') {
            continue;
        }
        let qualified = parse_qualified_column_ref(column)?;
        if qualified.alias != OUTER_CORRELATION_ALIAS {
            return Err(unsupported_sql_error(&format!(
                "qualified {subquery_kind} subquery predicates admit only outer.<column> references or the subquery source qualifier in this scoped runtime slice"
            )));
        }
    }
    validate_outer_correlation_shapes(predicate, subquery_kind)
}

fn validate_outer_correlation_shapes(
    predicate: &ParsedPredicate,
    subquery_kind: &str,
) -> Result<(), ShardLoomError> {
    match predicate {
        ParsedPredicate::ColumnCompare {
            left_column,
            right_column,
            ..
        } => {
            let left_outer = is_outer_correlation_ref(left_column);
            let right_outer = is_outer_correlation_ref(right_column);
            if left_outer && right_outer {
                return Err(unsupported_sql_error(&format!(
                    "correlated {subquery_kind} subquery predicates require exactly one outer.<column> reference per column comparison"
                )));
            }
            Ok(())
        }
        ParsedPredicate::Logical { left, right, .. } => {
            validate_outer_correlation_shapes(left, subquery_kind)?;
            validate_outer_correlation_shapes(right, subquery_kind)
        }
        ParsedPredicate::Not { inner } => validate_outer_correlation_shapes(inner, subquery_kind),
        ParsedPredicate::InSubquery { subquery, .. }
        | ParsedPredicate::QuantifiedSubquery { subquery, .. } => {
            validate_outer_correlation_shapes(&subquery.predicate, subquery_kind)
        }
        ParsedPredicate::RowValueInSubquery { subquery, .. } => {
            validate_outer_correlation_shapes(&subquery.predicate, subquery_kind)
        }
        ParsedPredicate::ExistsSubquery { subquery } => {
            validate_outer_correlation_shapes(&subquery.predicate, subquery_kind)
        }
        _ if predicate.uses_outer_correlation() => Err(unsupported_sql_error(&format!(
            "correlated {subquery_kind} subquery predicates admit outer.<column> references only in column-to-column comparisons"
        ))),
        _ => Ok(()),
    }
}

fn parse_quantified_subquery_blocker(raw: &str) -> Result<(), ShardLoomError> {
    for keyword in ["any", "all"] {
        let Some(index) = find_keyword_outside_quotes_and_parentheses(raw, keyword)? else {
            continue;
        };
        let tail = raw[index + keyword.len()..].trim_start();
        if tail.starts_with('(') {
            let Some(close_index) = matching_closing_parenthesis(tail, 0)? else {
                return Err(unsupported_sql_error(
                    "ANY and ALL subquery predicate parentheses must be balanced",
                ));
            };
            let inner = tail[1..close_index].trim_start();
            if inner
                .get(.."select".len())
                .is_some_and(|prefix| prefix.eq_ignore_ascii_case("select"))
                && keyword_boundary(inner, 0, "select".len())
            {
                return Err(unsupported_sql_error(
                    "ANY and ALL subquery predicates admit only <column> <comparison> ANY|ALL (SELECT <column> FROM <local-source> [WHERE <predicate>] [ORDER BY <column>] [LIMIT <n>]) in this scoped runtime slice",
                ));
            }
        }
    }
    Ok(())
}

fn earliest_optional_clause_index_after(
    start: usize,
    indexes: &[Option<usize>],
    default: usize,
) -> usize {
    indexes
        .iter()
        .flatten()
        .copied()
        .filter(|index| *index > start)
        .min()
        .unwrap_or(default)
}

fn parse_in_list_literal(raw: &str) -> Result<ScalarValue, ShardLoomError> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(unsupported_sql_error(
            "IN predicates require non-empty literal values",
        ));
    }
    if trimmed
        .get(..4)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("date"))
        && keyword_boundary(trimmed, 0, 4)
    {
        return parse_sql_date_literal(trimmed[4..].trim());
    }
    if trimmed
        .get(..9)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("timestamp"))
        && keyword_boundary(trimmed, 0, 9)
    {
        return parse_sql_timestamp_literal(trimmed[9..].trim());
    }
    parse_sql_literal(trimmed)
}

fn parse_regex_function_predicate(raw: &str) -> Result<Option<ParsedPredicate>, ShardLoomError> {
    let trimmed = raw.trim();
    let Some((function_name, open_index)) = parse_regex_function_prefix(trimmed) else {
        return Ok(None);
    };
    let close_index = matching_closing_parenthesis(trimmed, open_index)?.ok_or_else(|| {
        unsupported_sql_error(
            "regex predicates must use REGEXP_LIKE(<column>, <regex-pattern>) or REGEXP(<column>, <regex-pattern>)",
        )
    })?;
    if !trimmed[close_index + 1..].trim().is_empty() {
        return Err(unsupported_sql_error(&format!(
            "{function_name} regex predicate must be a single function call"
        )));
    }
    let inner = trimmed[open_index + 1..close_index].trim();
    let args = split_sql_csv(inner)?;
    let [column_raw, pattern_raw] = args.as_slice() else {
        return Err(unsupported_sql_error(&format!(
            "{function_name} regex predicate requires exactly two arguments: <column>, <regex-pattern>"
        )));
    };
    let column = column_raw.trim();
    validate_sql_column_ref(column)?;
    Ok(Some(ParsedPredicate::StringMatch {
        column: column.to_string(),
        op: StringPredicateOp::RegexMatch,
        value: parse_regex_pattern_literal(pattern_raw)?,
        like_escape: None,
    }))
}

fn parse_regex_function_prefix(raw: &str) -> Option<(&'static str, usize)> {
    ["regexp_like", "regex_match", "regexp", "regex", "rlike"]
        .into_iter()
        .find_map(|name| {
            raw.get(..name.len())
                .is_some_and(|prefix| prefix.eq_ignore_ascii_case(name))
                .then_some(())
                .filter(|()| raw.as_bytes().get(name.len()) == Some(&b'('))
                .map(|()| (name, name.len()))
        })
}

fn is_regex_predicate_operator(raw: &str) -> bool {
    raw.eq_ignore_ascii_case("rlike") || raw.eq_ignore_ascii_case("regexp")
}

fn parse_regex_pattern_literal(raw: &str) -> Result<String, ShardLoomError> {
    let pattern = parse_sql_string_literal(raw)?;
    validate_regex_pattern(&pattern)?;
    Ok(pattern)
}

fn validate_regex_pattern(pattern: &str) -> Result<(), ShardLoomError> {
    Regex::new(pattern)
        .map(|_| ())
        .map_err(|error| unsupported_sql_error(&format!("regex pattern is invalid: {error}")))
}

fn parse_like_string_predicate(
    pattern: &str,
    escape: Option<char>,
) -> Result<(StringPredicateOp, String), ShardLoomError> {
    let percent_count = pattern.chars().filter(|ch| *ch == '%').count();
    if escape.is_none() && !pattern.contains('_') {
        match (
            pattern.strip_prefix('%'),
            pattern.strip_suffix('%'),
            percent_count,
        ) {
            (Some(inner), Some(_), 2) if pattern.len() >= 3 => {
                let needle = inner.strip_suffix('%').unwrap_or(inner);
                if !needle.is_empty() && !needle.contains('%') {
                    return Ok((StringPredicateOp::Contains, needle.to_string()));
                }
            }
            (None, Some(prefix), 1) if !prefix.is_empty() => {
                return Ok((StringPredicateOp::StartsWith, prefix.to_string()));
            }
            (Some(suffix), None, 1) if !suffix.is_empty() => {
                return Ok((StringPredicateOp::EndsWith, suffix.to_string()));
            }
            _ => {}
        }
    }
    like_pattern_to_regex(pattern, escape).map(|regex| (StringPredicateOp::LikePattern, regex))
}

fn like_pattern_to_regex(pattern: &str, escape: Option<char>) -> Result<String, ShardLoomError> {
    let mut regex = String::from(r"\A");
    let mut chars = pattern.chars();
    while let Some(ch) = chars.next() {
        if escape.is_some_and(|escape| ch == escape) {
            let Some(next) = chars.next() else {
                return Err(unsupported_sql_error(
                    "LIKE ESCAPE pattern cannot end with the escape character",
                ));
            };
            if next != '%' && next != '_' && Some(next) != escape {
                return Err(unsupported_sql_error(
                    "LIKE ESCAPE may only escape %, _, or the escape character",
                ));
            }
            push_regex_escaped_char(&mut regex, next);
            continue;
        }
        match ch {
            '%' => regex.push_str("(?s:.*)"),
            '_' => regex.push_str("(?s:.)"),
            ch => push_regex_escaped_char(&mut regex, ch),
        }
    }
    regex.push_str(r"\z");
    validate_regex_pattern(&regex)?;
    Ok(regex)
}

fn push_regex_escaped_char(regex: &mut String, ch: char) {
    match ch {
        '\\' | '.' | '+' | '*' | '?' | '(' | ')' | '|' | '[' | ']' | '{' | '}' | '^' | '$' => {
            regex.push('\\');
            regex.push(ch);
        }
        ch => regex.push(ch),
    }
}

fn parse_comparison_op(raw: &str) -> Result<ComparisonOp, ShardLoomError> {
    match raw {
        "=" => Ok(ComparisonOp::Eq),
        "!=" | "<>" => Ok(ComparisonOp::NotEq),
        ">" => Ok(ComparisonOp::Gt),
        ">=" => Ok(ComparisonOp::GtEq),
        "<" => Ok(ComparisonOp::Lt),
        "<=" => Ok(ComparisonOp::LtEq),
        _ => Err(unsupported_sql_error(
            "WHERE comparison operator must be one of =, !=, <>, >, >=, <, <=",
        )),
    }
}

fn find_top_level_comparison_operator(
    raw: &str,
) -> Result<Option<(usize, &'static str)>, ShardLoomError> {
    Ok(
        shardloom_plan::sql_binder::find_top_level_comparison_operator(raw)?
            .map(|(index, op, _)| (index, op)),
    )
}

fn parse_limit(raw: &str) -> Result<usize, ShardLoomError> {
    if raw.split_whitespace().count() != 1 {
        return Err(unsupported_sql_error(
            "LIMIT admits a single non-negative integer literal only",
        ));
    }
    let value = raw.parse::<usize>().map_err(|_| {
        unsupported_sql_error("LIMIT admits a single non-negative integer literal only")
    })?;
    Ok(value)
}

fn parse_projection_literal_value(raw: &str) -> Result<ScalarValue, ShardLoomError> {
    let trimmed = raw.trim();
    if trimmed
        .get(..4)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("date"))
        && keyword_boundary(trimmed, 0, 4)
    {
        return parse_sql_date_literal(trimmed[4..].trim());
    }
    if trimmed
        .get(..9)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("timestamp"))
        && keyword_boundary(trimmed, 0, 9)
    {
        return parse_sql_timestamp_literal(trimmed[9..].trim());
    }
    parse_sql_literal(trimmed)
}

fn parse_top_level_projection_literal_value(raw: &str) -> Result<ScalarValue, ShardLoomError> {
    let trimmed = raw.trim();
    if is_sql_binary_hex_literal(trimmed) {
        return parse_sql_binary_hex_literal(trimmed).map(ScalarValue::Binary);
    }
    if let Some(value) = parse_sql_binary_text_literal(trimmed)? {
        return Ok(ScalarValue::Binary(value));
    }
    parse_projection_literal_value(trimmed)
}

fn parse_sql_literal(raw: &str) -> Result<ScalarValue, ShardLoomError> {
    let value = raw.trim();
    if value.eq_ignore_ascii_case("null") {
        return Ok(ScalarValue::Null);
    }
    if value.eq_ignore_ascii_case("true") {
        return Ok(ScalarValue::Boolean(true));
    }
    if value.eq_ignore_ascii_case("false") {
        return Ok(ScalarValue::Boolean(false));
    }
    if value.starts_with('\'') {
        return parse_sql_string_literal(value).map(ScalarValue::Utf8);
    }
    if let Ok(parsed) = value.parse::<i64>() {
        return Ok(ScalarValue::Int64(parsed));
    }
    if let Ok(parsed) = value.parse::<u64>() {
        return Ok(ScalarValue::UInt64(parsed));
    }
    if let Ok(parsed) = value.parse::<f64>()
        && parsed.is_finite()
    {
        return Ok(ScalarValue::Float64(parsed));
    }
    Err(unsupported_sql_error(
        "SQL local-source literals are limited to int64, finite float64, boolean, null, and single-quoted UTF-8 strings",
    ))
}

fn is_sql_binary_hex_literal(value: &str) -> bool {
    value
        .get(..1)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("x"))
        && value.get(1..).is_some_and(|tail| tail.starts_with('\''))
}

fn parse_sql_binary_hex_literal(raw: &str) -> Result<Vec<u8>, ShardLoomError> {
    let Some(string_literal) = raw.get(1..) else {
        return Err(unsupported_sql_error(
            "binary hex literals must be written as X'<hex bytes>'",
        ));
    };
    let body = parse_sql_string_literal(string_literal)?;
    if body.len() % 2 != 0 {
        return Err(unsupported_sql_error(
            "binary hex literals require an even number of hexadecimal digits",
        ));
    }
    if !body.chars().all(|ch| ch.is_ascii_hexdigit()) {
        return Err(unsupported_sql_error(
            "binary hex literals admit hexadecimal digits only",
        ));
    }
    body.as_bytes()
        .chunks(2)
        .map(|pair| {
            let text = std::str::from_utf8(pair).map_err(|_| {
                unsupported_sql_error("binary hex literals admit hexadecimal digits only")
            })?;
            u8::from_str_radix(text, 16).map_err(|_| {
                unsupported_sql_error("binary hex literals admit hexadecimal digits only")
            })
        })
        .collect()
}

fn parse_sql_binary_text_literal(raw: &str) -> Result<Option<Vec<u8>>, ShardLoomError> {
    let trimmed = raw.trim();
    for keyword in ["binary", "blob"] {
        if let Some(literal) = strip_sql_binary_literal_keyword(trimmed, keyword) {
            return parse_sql_string_literal(literal).map(|value| Some(value.into_bytes()));
        }
    }
    Ok(None)
}

fn parse_sql_binary_keyword_literal(
    keyword_raw: &str,
    literal_raw: &str,
) -> Result<Option<Vec<u8>>, ShardLoomError> {
    for keyword in ["binary", "blob"] {
        if keyword_raw.eq_ignore_ascii_case(keyword) {
            return parse_sql_string_literal(literal_raw).map(|value| Some(value.into_bytes()));
        }
    }
    Ok(None)
}

fn strip_sql_binary_literal_keyword<'a>(raw: &'a str, keyword: &str) -> Option<&'a str> {
    raw.get(..keyword.len())
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case(keyword))
        .then_some(())?;
    if !keyword_boundary(raw, 0, keyword.len()) {
        return None;
    }
    Some(raw[keyword.len()..].trim())
}

/// Split raw SQL expressions without decoding their string literals. Each
/// expression parser owns the single unescape, including nested query arguments.
fn split_whitespace_outside_quotes(raw: &str) -> Result<Vec<String>, ShardLoomError> {
    let mut values = Vec::new();
    let mut current = String::new();
    let mut chars = raw.chars().peekable();
    let mut in_quote = false;
    while let Some(ch) = chars.next() {
        match ch {
            '\'' if in_quote && chars.peek() == Some(&'\'') => {
                // Tokenization preserves the SQL spelling. Literal parsing owns
                // unescaping, including predicates inside a derived relation.
                current.push('\'');
                current.push('\'');
                let _ = chars.next();
            }
            '\'' => {
                in_quote = !in_quote;
                current.push(ch);
            }
            ch if ch.is_whitespace() && !in_quote => {
                if !current.is_empty() {
                    values.push(current);
                    current = String::new();
                }
            }
            _ => current.push(ch),
        }
    }
    if in_quote {
        return Err(unsupported_sql_error("SQL string literal is not closed"));
    }
    if !current.is_empty() {
        values.push(current);
    }
    Ok(values)
}

fn strip_leading_keyword<'a>(raw: &'a str, keyword: &str) -> Option<&'a str> {
    let trimmed = raw.trim_start();
    if !trimmed
        .get(..keyword.len())
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case(keyword))
    {
        return None;
    }
    if !keyword_boundary(trimmed, 0, keyword.len()) {
        return None;
    }
    // The shared scalar parser owns parentheses, including a scalar query's
    // required enclosing pair. DISTINCT only changes aggregate semantics.
    Some(&trimmed[keyword.len()..])
}

fn parse_sql_string_literal(raw: &str) -> Result<String, ShardLoomError> {
    if !raw.starts_with('\'') || !raw.ends_with('\'') || raw.len() < 2 {
        return Err(unsupported_sql_error(
            "SQL string literals must be single quoted",
        ));
    }
    let body = &raw[1..raw.len() - 1];
    let mut output = String::new();
    let mut chars = body.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '\'' {
            if chars.peek() == Some(&'\'') {
                output.push('\'');
                let _ = chars.next();
            } else {
                return Err(unsupported_sql_error(
                    "single quotes inside SQL string literals must be escaped as doubled quotes",
                ));
            }
        } else {
            output.push(ch);
        }
    }
    Ok(output)
}

fn find_keyword_outside_quotes(raw: &str, keyword: &str) -> Option<usize> {
    shardloom_plan::sql_binder::find_keyword_anywhere_outside_quotes(raw, keyword)
}

fn top_level_sql_union_operators(raw: &str) -> Result<Vec<SqlUnionOperator>, ShardLoomError> {
    shardloom_plan::sql_binder::find_top_level_set_operators(raw)?
        .into_iter()
        .map(|span| {
            let mode = match span.mode {
                shardloom_plan::sql_binder::SqlSetOpMode::UnionDistinct => SqlUnionMode::Distinct,
                shardloom_plan::sql_binder::SqlSetOpMode::UnionAll => SqlUnionMode::All,
                shardloom_plan::sql_binder::SqlSetOpMode::IntersectDistinct => {
                    SqlUnionMode::IntersectDistinct
                }
                shardloom_plan::sql_binder::SqlSetOpMode::ExceptDistinct => {
                    SqlUnionMode::ExceptDistinct
                }
                shardloom_plan::sql_binder::SqlSetOpMode::IntersectAll
                | shardloom_plan::sql_binder::SqlSetOpMode::ExceptAll => {
                    return Err(unsupported_sql_error(
                        "INTERSECT ALL and EXCEPT ALL are not admitted in this scoped local-source runtime; use distinct set semantics only",
                    ));
                }
            };
            Ok(SqlUnionOperator {
                index: span.index,
                len: span.len,
                mode,
            })
        })
        .collect()
}

fn top_level_keyword_indexes(raw: &str, keyword: &str) -> Result<Vec<usize>, ShardLoomError> {
    shardloom_plan::sql_binder::top_level_keyword_offsets(raw, keyword)
}

fn starts_with_keyword(raw: &str, keyword: &str) -> bool {
    raw.get(..keyword.len())
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case(keyword))
        && keyword_boundary(raw, 0, keyword.len())
}

fn find_keyword_outside_quotes_and_parentheses(
    raw: &str,
    keyword: &str,
) -> Result<Option<usize>, ShardLoomError> {
    shardloom_plan::sql_binder::find_top_level_keyword(raw, keyword)
}

fn contains_keyword_outside_quotes(raw: &str, keyword: &str) -> bool {
    find_keyword_outside_quotes(raw, keyword).is_some()
}

fn keyword_boundary(raw: &str, index: usize, len: usize) -> bool {
    let before = raw[..index].chars().next_back();
    let after = raw[index + len..].chars().next();
    !before.is_some_and(is_identifier_char) && !after.is_some_and(is_identifier_char)
}

fn validate_balanced_predicate_parentheses(raw: &str) -> Result<(), ShardLoomError> {
    let _ = find_keyword_outside_quotes_and_parentheses(raw, "__shardloom_never_matches__")?;
    Ok(())
}

fn matching_closing_parenthesis(
    raw: &str,
    open_index: usize,
) -> Result<Option<usize>, ShardLoomError> {
    let mut chars = raw.char_indices().peekable();
    let mut in_quote = false;
    let mut depth = 0_u32;
    let mut seen_open = false;
    while let Some((index, ch)) = chars.next() {
        if index < open_index {
            continue;
        }
        if ch == '\'' {
            if in_quote && chars.peek().is_some_and(|(_, next)| *next == '\'') {
                let _ = chars.next();
            } else {
                in_quote = !in_quote;
            }
            continue;
        }
        if in_quote {
            continue;
        }
        match ch {
            '(' => {
                depth += 1;
                seen_open = true;
            }
            ')' => {
                depth = depth.checked_sub(1).ok_or_else(|| {
                    unsupported_sql_error("WHERE predicate grouping parentheses must be balanced")
                })?;
                if seen_open && depth == 0 {
                    return Ok(Some(index));
                }
            }
            _ => {}
        }
    }
    if in_quote {
        return Err(unsupported_sql_error("SQL string literal is not closed"));
    }
    Ok(None)
}

fn matching_closing_square_bracket(
    raw: &str,
    open_index: usize,
) -> Result<Option<usize>, ShardLoomError> {
    let mut chars = raw.char_indices().peekable();
    let mut in_quote = false;
    let mut depth = 0_u32;
    let mut seen_open = false;
    while let Some((index, ch)) = chars.next() {
        if index < open_index {
            continue;
        }
        if ch == '\'' {
            if in_quote && chars.peek().is_some_and(|(_, next)| *next == '\'') {
                let _ = chars.next();
            } else {
                in_quote = !in_quote;
            }
            continue;
        }
        if in_quote {
            continue;
        }
        match ch {
            '[' => {
                depth += 1;
                seen_open = true;
            }
            ']' => {
                depth = depth.checked_sub(1).ok_or_else(|| {
                    unsupported_sql_error("SQL expression square brackets must be balanced")
                })?;
                if seen_open && depth == 0 {
                    return Ok(Some(index));
                }
            }
            _ => {}
        }
    }
    if in_quote {
        return Err(unsupported_sql_error("SQL string literal is not closed"));
    }
    Ok(None)
}

fn validate_sql_column_ref(value: &str) -> Result<(), ShardLoomError> {
    if value.contains('.') {
        let _ = parse_qualified_column_ref(value)?;
        Ok(())
    } else {
        validate_sql_identifier(value)
    }
}

fn parse_qualified_column_ref(value: &str) -> Result<QualifiedColumn, ShardLoomError> {
    let Some((alias, column)) = value.split_once('.') else {
        return Err(unsupported_sql_error(
            "qualified JOIN columns must use <alias>.<column> syntax",
        ));
    };
    if column.contains('.') {
        return Err(unsupported_sql_error(
            "qualified JOIN columns may contain exactly one alias separator",
        ));
    }
    validate_sql_identifier(alias)?;
    validate_sql_identifier(column)?;
    Ok(QualifiedColumn {
        alias: alias.to_string(),
        column: column.to_string(),
    })
}

#[cfg(test)]
#[allow(clippy::too_many_lines)]
#[path = "sql_syntax_tests.rs"]
mod tests;
