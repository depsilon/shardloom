//! SQL declarations lowered into the shared native execution engine.

use super::*;
use regex::Regex;
use shardloom_core::{
    BinaryOp, ColumnRef, ComparisonOp, ExprId, Expression, ExpressionKind, UnaryOp,
    evaluate_expression, parse_iso_date32, parse_iso_timestamp_micros,
};
use shardloom_vortex::relational_query::VortexRelationalWindowFrame;

#[path = "sql_window_frames.rs"]
mod window_frames;

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
use relation_sources::{ParsedRelationLeaf, ParsedRelationSource};

#[cfg(all(feature = "vortex-local-primitives", unix))]
#[path = "sql_native_relational.rs"]
pub(crate) mod native_relational;

fn find_top_level_numeric_operator(
    raw: &str,
    operators: &[char],
) -> Result<Option<(usize, char)>, ShardLoomError> {
    let mut chars = raw.char_indices().peekable();
    let mut in_quote = false;
    let mut depth = 0_u32;
    let mut candidate = None;
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
            '(' => {
                depth += 1;
                continue;
            }
            ')' => {
                depth = depth.checked_sub(1).ok_or_else(|| {
                    unsupported_sql_error("generic numeric expression parentheses are not balanced")
                })?;
                continue;
            }
            _ => {}
        }
        if depth == 0 && operators.contains(&ch) && !is_unary_numeric_sign(raw, index, ch) {
            candidate = Some((index, ch));
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
    Ok(candidate)
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
    order_by: Option<ParsedOrderBy>,
    source: ParsedRelationSource,
    source_alias: Option<String>,
    join: Option<ParsedJoin>,
    predicate: ParsedPredicate,
    having: ParsedPredicate,
    limit: usize,
    /// Parser-only bounds for the decoded reference must not truncate native SQL.
    limit_is_synthetic: bool,
    normalized_statement: String,
}

#[derive(Debug, Clone, PartialEq)]
struct ParsedAggregate {
    function: AggregateFunction,
    argument: ParsedAggregateArgument,
    alias: Option<String>,
    distinct: bool,
}

#[derive(Debug, Clone, PartialEq)]
enum ParsedAggregateArgument {
    All,
    Column(String),
    Computed {
        raw: String,
        expression: Box<Expression>,
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
    expression: Expression,
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
    Lag { column: String, offset: usize },
    Lead { column: String, offset: usize },
    Ntile { bucket_count: usize },
    PercentRank,
    CumeDist,
    Aggregate(ParsedAggregate),
    FirstValue(Expression),
    LastValue(Expression),
    NthValue { expression: Expression, index: u64 },
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
        left: Box<Expression>,
        comparison: ComparisonOp,
        right: Box<Expression>,
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
}

#[cfg(all(test, feature = "vortex-write"))]
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
        !self.aggregates.is_empty() || !self.having_aggregates.is_empty()
    }

    fn is_grouped_aggregate(&self) -> bool {
        !self.group_by.is_empty() && self.is_aggregate()
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
    const fn as_str(self) -> &'static str {
        match self {
            Self::Count => "count",
            Self::Sum => "sum",
            Self::Avg => "avg",
            Self::Min => "min",
            Self::Max => "max",
        }
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
            } => generic_expression_compare_expression(left, *comparison, right),
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
    let mut chars = raw.char_indices().peekable();
    let mut in_quote = false;
    let mut depth = 0_u32;
    let mut bracket_depth = 0_u32;
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
            '(' => {
                depth += 1;
                continue;
            }
            ')' => {
                depth = depth.checked_sub(1).ok_or_else(|| {
                    unsupported_sql_error("WHERE predicate grouping parentheses must be balanced")
                })?;
                continue;
            }
            '[' => {
                bracket_depth += 1;
                continue;
            }
            ']' => {
                bracket_depth = bracket_depth.checked_sub(1).ok_or_else(|| {
                    unsupported_sql_error("SQL expression square brackets are not balanced")
                })?;
                continue;
            }
            _ => {}
        }
        if depth == 0 && bracket_depth == 0 {
            let remaining = &raw[index..];
            if remaining
                .get(.."from".len())
                .is_some_and(|prefix| prefix.eq_ignore_ascii_case("from"))
                && keyword_boundary(raw, index, "from".len())
                && !from_keyword_belongs_to_null_safe_comparison(raw, index)?
            {
                return Ok(Some(index));
            }
        }
    }
    if in_quote {
        return Err(unsupported_sql_error("SQL string literal is not closed"));
    }
    if depth != 0 {
        return Err(unsupported_sql_error(
            "WHERE predicate grouping parentheses must be balanced",
        ));
    }
    if bracket_depth != 0 {
        return Err(unsupported_sql_error(
            "SQL expression square brackets are not balanced",
        ));
    }
    Ok(None)
}

fn from_keyword_belongs_to_null_safe_comparison(
    raw: &str,
    from_index: usize,
) -> Result<bool, ShardLoomError> {
    let tokens = split_whitespace_outside_quotes(raw[..from_index].trim_end())?;
    Ok(matches!(
        tokens.as_slice(),
        [.., is_keyword, distinct_keyword]
            if is_keyword.eq_ignore_ascii_case("is")
                && distinct_keyword.eq_ignore_ascii_case("distinct")
    ) || matches!(
        tokens.as_slice(),
        [.., is_keyword, not_keyword, distinct_keyword]
            if is_keyword.eq_ignore_ascii_case("is")
                && not_keyword.eq_ignore_ascii_case("not")
                && distinct_keyword.eq_ignore_ascii_case("distinct")
    ))
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
    Ok(format!("{branch} LIMIT {MAX_LIMIT_ROWS}"))
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

    let projection_list = parse_projection_list(select_list)?;
    let (having_raw, having_aggregates) = if let Some(having_raw) = having_raw {
        let (rewritten, aggregates) = rewrite_having_aggregate_predicate(
            having_raw,
            reserved_having_aggregate_aliases(&projection_list),
        )?;
        (Some(rewritten), aggregates)
    } else {
        (None, Vec::new())
    };
    let group_by = parse_group_by_list(group_by_raw)?;
    let order_by = parse_order_by(order_by_raw)?;
    let source_clause = parse_normalized_source_clause(source_raw, memory_source)?;
    let predicate = predicate_raw.map_or(Ok(ParsedPredicate::All), parse_predicate)?;
    let having = having_raw
        .as_deref()
        .map_or(Ok(ParsedPredicate::All), parse_predicate)?;
    let limit = parse_limit(limit_raw)?;

    Ok(parsed_sql_local_source_from_parts(
        ParsedSqlLocalSourceParts {
            statement,
            projection_list,
            distinct_projection,
            having_aggregates,
            group_by,
            order_by,
            source_clause,
            predicate,
            having,
            limit,
        },
    ))
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
        order_by,
        source: source_clause.source,
        source_alias: source_clause.source_alias,
        join: source_clause.join,
        predicate,
        having,
        limit,
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
        || contains_function_call_outside_quotes(statement, "variant_get")
        || contains_cast_target_matching(statement, "cast", target_is_variant_dtype)?
        || contains_cast_target_matching(statement, "try_cast", target_is_variant_dtype)?
    {
        return Err(unsupported_sql_error(
            "variant access semantics are not admitted by the current complex dtype profile",
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
    mut reserved_aliases: BTreeSet<String>,
) -> Result<(String, Vec<ParsedAggregate>), ShardLoomError> {
    let mut rewritten = String::with_capacity(raw.len());
    let mut aggregates = Vec::new();
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
        if !in_string && let Some(function_name) = aggregate_function_prefix_at(raw, index) {
            let open_index = index + function_name.len();
            let close_index = matching_closing_parenthesis(raw, open_index)?.ok_or_else(|| {
                unsupported_sql_error(
                    "HAVING aggregate predicates require complete aggregate function calls",
                )
            })?;
            let expression = &raw[index..=close_index];
            let mut aggregate = parse_aggregate_projection(expression)?.ok_or_else(|| {
                unsupported_sql_error(
                    "HAVING aggregate predicates admit COUNT, SUM, AVG, MIN, or MAX only",
                )
            })?;
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

fn aggregate_function_prefix_at(raw: &str, index: usize) -> Option<&'static str> {
    if !sql_identifier_boundary_before(raw, index) {
        return None;
    }
    for name in ["count", "sum", "avg", "min", "max"] {
        let end = index + name.len();
        if raw
            .get(index..end)
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case(name))
            && raw.as_bytes().get(end) == Some(&b'(')
        {
            return Some(name);
        }
    }
    None
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
    for projection in entries {
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
        } else {
            validate_sql_column_ref(projection)?;
            projection_order.push(ParsedProjectionOutput::Raw(projection.to_string()));
            projections.push(projection.to_string());
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
    split_sql_csv(inner)?
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
    if offset == 0 || offset > MAX_INPUT_ROWS {
        return Err(unsupported_sql_error(&format!(
            "{function_name} window {value_label} must be between 1 and {MAX_INPUT_ROWS}"
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
        return Ok(None);
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
    if raw
        .get(..5)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("case "))
    {
        return Ok(false);
    }
    if parse_regex_function_prefix(raw.trim()).is_some() {
        return Ok(true);
    }
    let tokens = split_whitespace_outside_quotes(raw)?;
    if tokens.len() > 1
        && tokens.iter().any(|token| {
            matches!(
                token.to_ascii_lowercase().as_str(),
                "=" | "!="
                    | "<>"
                    | "<"
                    | "<="
                    | ">"
                    | ">="
                    | "is"
                    | "not"
                    | "in"
                    | "like"
                    | "rlike"
                    | "regexp"
                    | "between"
                    | "and"
                    | "or"
            )
        })
    {
        return Ok(true);
    }
    Ok(find_top_level_comparison_operator(trim_enclosing_predicate_parentheses(raw)?)?.is_some())
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
            expression,
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
            ),
            source_columns: vec![expression_raw.to_owned()],
            operator_families: Vec::new(),
            binary_operator_count: 0,
        }));
    }
    let contains_temporal_difference =
        expression_contains_temporal_difference_call(expression_raw)?;
    let has_numeric_operator = expression_contains_numeric_operator(expression_raw)?;
    let composed = !has_numeric_operator && scalar_expression::composed(expression_raw)?;
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
) -> Result<Expression, ShardLoomError> {
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
        ExpressionKind::Literal(_) | ExpressionKind::Unsupported { .. } => {}
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
    if trimmed.starts_with('\'') {
        return Ok(Expression::literal(
            ExprId::new(format!("{id_prefix}.literal"))?,
            ScalarValue::Utf8(parse_sql_string_literal(trimmed)?),
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
        ExpressionKind::Column(_) | ExpressionKind::Unsupported { .. } => 0,
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
    let function = match function_raw.to_ascii_lowercase().as_str() {
        "count" => AggregateFunction::Count,
        "sum" => AggregateFunction::Sum,
        "avg" => AggregateFunction::Avg,
        "min" => AggregateFunction::Min,
        "max" => AggregateFunction::Max,
        _ => return Ok(None),
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
    if !expression_raw.ends_with(')') {
        return Err(unsupported_sql_error(
            "aggregate expressions must be written as function(argument) or function(argument) AS alias",
        ));
    }
    let argument = expression_raw[open_index + 1..expression_raw.len() - 1].trim();
    if argument.is_empty() {
        return Err(unsupported_sql_error(
            "aggregate expressions require one scalar argument or COUNT(*)",
        ));
    }
    let (distinct, argument) = if let Some(argument) = strip_leading_keyword(argument, "distinct")?
    {
        if function != AggregateFunction::Count {
            return Err(unsupported_sql_error(
                "DISTINCT aggregate runtime currently admits COUNT(DISTINCT <argument>) only",
            ));
        }
        let argument = argument.trim();
        if argument.is_empty() {
            return Err(unsupported_sql_error(
                "COUNT(DISTINCT ...) requires one scalar argument",
            ));
        }
        (true, argument)
    } else {
        (false, argument)
    };
    if argument == "*" {
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
        return Ok(Some(ParsedAggregate {
            function,
            argument: ParsedAggregateArgument::All,
            alias,
            distinct,
        }));
    }
    let expression = scalar_expression::parse(argument, "aggregate.argument")?;
    let argument = if let ExpressionKind::Column(column) = &expression.kind {
        ParsedAggregateArgument::Column(column.as_str().to_owned())
    } else {
        ParsedAggregateArgument::Computed {
            raw: argument.to_owned(),
            source_columns: expression_source_columns(&expression),
            expression: Box::new(expression),
        }
    };
    Ok(Some(ParsedAggregate {
        function,
        argument,
        alias,
        distinct,
    }))
}

fn parse_group_by_list(raw: Option<&str>) -> Result<Vec<String>, ShardLoomError> {
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
    for column in columns {
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
        Expression::new(
            ExprId::new("where.scalar.null_test")?,
            ExpressionKind::Unary {
                op,
                expr: Box::new(parse_numeric_scalar_expression(
                    source,
                    "where.scalar.null_arg",
                )?),
            },
        )
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
        right: Box::new(right),
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
    let composed = !numeric
        && (scalar_expression::composed(left_raw)? || scalar_expression::composed(right_raw)?);
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
    let columns = if parsed.is_grouped_aggregate() {
        parsed
            .group_by
            .iter()
            .cloned()
            .chain(parsed.aggregates.iter().map(ParsedAggregate::output_name))
            .collect()
    } else if parsed.is_aggregate() {
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
    let mut chars = raw.char_indices().peekable();
    let mut in_quote = false;
    let mut depth = 0_u32;
    let mut candidate = None;
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
            '(' => {
                depth += 1;
                continue;
            }
            ')' => {
                depth = depth.checked_sub(1).ok_or_else(|| {
                    unsupported_sql_error(
                        "generic numeric expression predicate parentheses are not balanced",
                    )
                })?;
                continue;
            }
            _ => {}
        }
        if depth == 0 {
            let tail = &raw[index..];
            let Some(op) = ["!=", "<>", ">=", "<=", "=", ">", "<"]
                .into_iter()
                .find(|op| tail.starts_with(op))
            else {
                continue;
            };
            if candidate.is_some() {
                return Err(unsupported_sql_error(
                    "generic numeric expression predicates admit exactly one comparison operator",
                ));
            }
            candidate = Some((index, op));
            for _ in 1..op.chars().count() {
                let _ = chars.next();
            }
        }
    }
    if in_quote {
        return Err(unsupported_sql_error("SQL string literal is not closed"));
    }
    if depth != 0 {
        return Err(unsupported_sql_error(
            "generic numeric expression predicate parentheses are not balanced",
        ));
    }
    Ok(candidate)
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
fn split_sql_csv(raw: &str) -> Result<Vec<String>, ShardLoomError> {
    let mut values = Vec::new();
    let mut current = String::new();
    let mut chars = raw.chars().peekable();
    let mut in_quote = false;
    let mut depth = 0_u32;
    let mut bracket_depth = 0_u32;
    while let Some(ch) = chars.next() {
        match ch {
            '\'' if in_quote && chars.peek() == Some(&'\'') => {
                current.push('\'');
                current.push('\'');
                let _ = chars.next();
            }
            '\'' => {
                in_quote = !in_quote;
                current.push(ch);
            }
            '(' if !in_quote => {
                depth += 1;
                current.push(ch);
            }
            ')' if !in_quote => {
                depth = depth.checked_sub(1).ok_or_else(|| {
                    unsupported_sql_error("SQL expression parentheses are not balanced")
                })?;
                current.push(ch);
            }
            '[' if !in_quote => {
                bracket_depth += 1;
                current.push(ch);
            }
            ']' if !in_quote => {
                bracket_depth = bracket_depth.checked_sub(1).ok_or_else(|| {
                    unsupported_sql_error("SQL expression square brackets are not balanced")
                })?;
                current.push(ch);
            }
            ',' if !in_quote && depth == 0 && bracket_depth == 0 => {
                values.push(current.trim().to_string());
                current = String::new();
            }
            _ => current.push(ch),
        }
    }
    if in_quote {
        return Err(unsupported_sql_error("SQL string literal is not closed"));
    }
    if depth != 0 {
        return Err(unsupported_sql_error(
            "SQL expression parentheses are not balanced",
        ));
    }
    if bracket_depth != 0 {
        return Err(unsupported_sql_error(
            "SQL expression square brackets are not balanced",
        ));
    }
    if !current.trim().is_empty() {
        values.push(current.trim().to_string());
    }
    Ok(values)
}

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

fn strip_leading_keyword<'a>(
    raw: &'a str,
    keyword: &str,
) -> Result<Option<&'a str>, ShardLoomError> {
    let trimmed = raw.trim_start();
    if !trimmed
        .get(..keyword.len())
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case(keyword))
    {
        return Ok(None);
    }
    if !keyword_boundary(trimmed, 0, keyword.len()) {
        return Ok(None);
    }
    let tail = &trimmed[keyword.len()..];
    if tail.trim_start().starts_with('(') {
        return Err(unsupported_sql_error(&format!(
            "{keyword} must be followed by a scoped expression, not another parenthesized expression"
        )));
    }
    Ok(Some(tail))
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
    let lower_keyword = keyword.to_ascii_lowercase();
    let chars = raw.char_indices().peekable();
    let mut in_quote = false;
    for (index, ch) in chars {
        if ch == '\'' {
            in_quote = !in_quote;
            continue;
        }
        if in_quote {
            continue;
        }
        let remaining = &raw[index..];
        if remaining
            .get(..lower_keyword.len())
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case(&lower_keyword))
            && keyword_boundary(raw, index, lower_keyword.len())
        {
            return Some(index);
        }
    }
    None
}

fn top_level_sql_union_operators(raw: &str) -> Result<Vec<SqlUnionOperator>, ShardLoomError> {
    let mut operators = Vec::new();
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
            '(' => {
                depth += 1;
                continue;
            }
            ')' => {
                depth = depth.checked_sub(1).ok_or_else(|| {
                    unsupported_sql_error("WHERE predicate grouping parentheses must be balanced")
                })?;
                continue;
            }
            _ => {}
        }
        if depth != 0 {
            continue;
        }
        let Some((mut len, mode)) = top_level_sql_set_operator_at(raw, index) else {
            continue;
        };
        let tail = &raw[index + len..];
        let whitespace_len = tail.len() - tail.trim_start().len();
        let tail_trimmed = &tail[whitespace_len..];
        let mode = match mode {
            SqlUnionMode::Distinct => {
                if starts_with_keyword(tail_trimmed, "all") {
                    len += whitespace_len + "all".len();
                    SqlUnionMode::All
                } else if starts_with_keyword(tail_trimmed, "distinct") {
                    len += whitespace_len + "distinct".len();
                    SqlUnionMode::Distinct
                } else {
                    SqlUnionMode::Distinct
                }
            }
            SqlUnionMode::IntersectDistinct | SqlUnionMode::ExceptDistinct => {
                if starts_with_keyword(tail_trimmed, "all") {
                    return Err(unsupported_sql_error(
                        "INTERSECT ALL and EXCEPT ALL are not admitted in this scoped local-source runtime; use distinct set semantics only",
                    ));
                }
                if starts_with_keyword(tail_trimmed, "distinct") {
                    len += whitespace_len + "distinct".len();
                }
                mode
            }
            SqlUnionMode::All => SqlUnionMode::All,
        };
        operators.push(SqlUnionOperator { index, len, mode });
    }
    if in_quote {
        return Err(unsupported_sql_error("SQL string literal is not closed"));
    }
    if depth != 0 {
        return Err(unsupported_sql_error(
            "WHERE predicate grouping parentheses must be balanced",
        ));
    }
    Ok(operators)
}

fn top_level_sql_set_operator_at(raw: &str, index: usize) -> Option<(usize, SqlUnionMode)> {
    for (keyword, mode) in [
        ("intersect", SqlUnionMode::IntersectDistinct),
        ("except", SqlUnionMode::ExceptDistinct),
        ("union", SqlUnionMode::Distinct),
    ] {
        if raw
            .get(index..index + keyword.len())
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case(keyword))
            && keyword_boundary(raw, index, keyword.len())
        {
            return Some((keyword.len(), mode));
        }
    }
    None
}

fn top_level_keyword_indexes(raw: &str, keyword: &str) -> Result<Vec<usize>, ShardLoomError> {
    let lower_keyword = keyword.to_ascii_lowercase();
    let mut indexes = Vec::new();
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
            '(' => {
                depth += 1;
                continue;
            }
            ')' => {
                depth = depth.checked_sub(1).ok_or_else(|| {
                    unsupported_sql_error("WHERE predicate grouping parentheses must be balanced")
                })?;
                continue;
            }
            _ => {}
        }
        if depth == 0 {
            let remaining = &raw[index..];
            if remaining
                .get(..lower_keyword.len())
                .is_some_and(|prefix| prefix.eq_ignore_ascii_case(&lower_keyword))
                && keyword_boundary(raw, index, lower_keyword.len())
            {
                indexes.push(index);
            }
        }
    }
    if in_quote {
        return Err(unsupported_sql_error("SQL string literal is not closed"));
    }
    if depth != 0 {
        return Err(unsupported_sql_error(
            "WHERE predicate grouping parentheses must be balanced",
        ));
    }
    Ok(indexes)
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
    let lower_keyword = keyword.to_ascii_lowercase();
    let mut chars = raw.char_indices().peekable();
    let mut in_quote = false;
    let mut depth = 0_u32;
    let mut bracket_depth = 0_u32;
    let mut skip_next_and_for_between = false;
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
            '(' => {
                depth += 1;
                continue;
            }
            ')' => {
                depth = depth.checked_sub(1).ok_or_else(|| {
                    unsupported_sql_error("WHERE predicate grouping parentheses must be balanced")
                })?;
                continue;
            }
            '[' => {
                bracket_depth += 1;
                continue;
            }
            ']' => {
                bracket_depth = bracket_depth.checked_sub(1).ok_or_else(|| {
                    unsupported_sql_error("SQL expression square brackets are not balanced")
                })?;
                continue;
            }
            _ => {}
        }
        if depth == 0 && bracket_depth == 0 {
            let remaining = &raw[index..];
            if lower_keyword == "and"
                && remaining
                    .get(.."between".len())
                    .is_some_and(|prefix| prefix.eq_ignore_ascii_case("between"))
                && keyword_boundary(raw, index, "between".len())
            {
                skip_next_and_for_between = true;
            }
            if remaining
                .get(..lower_keyword.len())
                .is_some_and(|prefix| prefix.eq_ignore_ascii_case(&lower_keyword))
                && keyword_boundary(raw, index, lower_keyword.len())
            {
                if lower_keyword == "and" && skip_next_and_for_between {
                    skip_next_and_for_between = false;
                    continue;
                }
                return Ok(Some(index));
            }
        }
    }
    if in_quote {
        return Err(unsupported_sql_error("SQL string literal is not closed"));
    }
    if depth != 0 {
        return Err(unsupported_sql_error(
            "WHERE predicate grouping parentheses must be balanced",
        ));
    }
    if bracket_depth != 0 {
        return Err(unsupported_sql_error(
            "SQL expression square brackets are not balanced",
        ));
    }
    Ok(None)
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
mod tests {
    use super::*;

    #[test]
    fn compact_comparisons_preserve_operators_and_quoted_literals() {
        for spelling in ["=", "!=", "<>", "<", "<=", ">", ">="] {
            let expected = parse_comparison_op(spelling).unwrap();
            assert!(matches!(
                parse_predicate(&format!("value{spelling}-3")).unwrap(),
                ParsedPredicate::Compare { column, op, value: ScalarValue::Int64(-3) }
                    if column == "value" && op == expected
            ));
            assert!(is_explicit_predicate_projection_shape(&format!("value{spelling}-3")).unwrap());
        }
        assert!(matches!(
            parse_predicate("label='a>=b''<c'").unwrap(),
            ParsedPredicate::Compare { column, op: ComparisonOp::Eq, value: ScalarValue::Utf8(value) }
                if column == "label" && value == "a>=b'<c"
        ));
        for compound in [
            "value=1 AND metric>=2",
            "value=1 OR metric>=2",
            "(value>=2)",
        ] {
            assert!(is_explicit_predicate_projection_shape(compound).unwrap());
            assert!(parse_predicate(compound).is_ok(), "{compound}");
        }
        for invalid in ["value==3", "value<3>1", "value>=", "=3"] {
            assert!(parse_predicate(invalid).is_err(), "{invalid}");
        }
    }

    static SQL_LOCAL_SOURCE_TEST_PATH_COUNTER: std::sync::atomic::AtomicU64 =
        std::sync::atomic::AtomicU64::new(0);

    fn sql_local_source_test_path(extension: &str) -> PathBuf {
        let mut path = std::env::temp_dir();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock")
            .as_nanos();
        let counter =
            SQL_LOCAL_SOURCE_TEST_PATH_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        path.push(format!(
            "shardloom-sql-local-source-{}-{nanos}-{counter}.{extension}",
            std::process::id(),
        ));
        path
    }

    fn field_map(fields: Vec<(String, String)>) -> BTreeMap<String, String> {
        fields.into_iter().collect()
    }

    fn assert_field_eq(fields: &BTreeMap<String, String>, key: &str, expected: &str) {
        assert_eq!(
            fields.get(key).map(String::as_str),
            Some(expected),
            "field {key}"
        );
    }

    #[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
    fn assert_ingest_stream_lane_recipe(fields: &BTreeMap<String, String>, requested: usize) {
        // Small finite-budget fixtures run ready source, conversion, and
        // provider tasks on the shared native runtime. Task-window capacity
        // does not imply dedicated threads.
        let (source, conversion, provider, prefetch) = match requested {
            2 => (0, 0, 1, 2),
            4 => (0, 0, 3, 4),
            _ => panic!("fixture has no independently specified lane recipe"),
        };
        let topology = fields["vortex_writer_physical_design_writer_queue_topology"]
            .split(';')
            .filter_map(|item| item.split_once('='))
            .collect::<BTreeMap<_, _>>();
        for (key, expected) in [
            ("ingest_cpu_requested", requested),
            ("ingest_cpu_configured", requested),
            ("ingest_cpu_caller", 1),
            ("ingest_cpu_source_drivers", source),
            ("ingest_cpu_conversion_drivers", conversion),
            ("ingest_cpu_provider_drivers", provider),
            ("array_prefetch_window", prefetch),
        ] {
            assert_eq!(topology[key].parse::<usize>().unwrap(), expected, "{key}");
        }
        assert_eq!(1 + source + conversion + provider, requested);
        assert_eq!(
            topology["driver_lifetime"],
            "joined_before_artifact_call_returns"
        );
        assert_eq!(
            topology["ingest_cpu_scope"],
            "shardloom_owned_drivers_excludes_blocking_io_and_source_library_internal_threads"
        );
        assert_eq!(
            topology["lane_reassignment"],
            "ready_source_conversion_and_provider_tasks_share_executor"
        );
        assert_eq!(
            topology["array_build_workers_scope"],
            "concurrent_tasks_not_dedicated_threads"
        );
        assert_field_eq(
            fields,
            "vortex_writer_runtime_requested_parallelism",
            &requested.to_string(),
        );
        assert_field_eq(
            fields,
            "vortex_writer_runtime_applied_parallelism",
            &requested.to_string(),
        );
        assert_field_eq(
            fields,
            "vortex_writer_runtime_background_workers",
            &provider.to_string(),
        );
        assert_field_eq(
            fields,
            "vortex_writer_physical_design_source_executor_applied_parallelism",
            "1",
        );
        assert_field_eq(
            fields,
            "vortex_writer_physical_design_array_build_worker_count",
            &prefetch.to_string(),
        );
        assert_field_eq(
            fields,
            "vortex_array_build_prefetch_window",
            &prefetch.to_string(),
        );
    }

    #[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
    fn assert_field_contains(fields: &BTreeMap<String, String>, key: &str, expected: &str) {
        let value = fields
            .get(key)
            .unwrap_or_else(|| panic!("field {key} must be present"));
        assert!(
            value.contains(expected),
            "field {key} value {value:?} must contain {expected:?}"
        );
    }

    #[test]
    fn local_source_read_budget_rejects_oversized_regular_file() {
        let path = sql_local_source_test_path("csv");
        let file = fs::File::create(&path).expect("create sparse local source");
        file.set_len(MAX_LOCAL_SOURCE_BYTES + 1)
            .expect("set sparse source length");

        let error = read_local_source_bytes_with_budget(&path, "csv", Some(MAX_LOCAL_SOURCE_BYTES))
            .expect_err("oversized read blocked");
        let _ = fs::remove_file(&path);

        assert!(
            error
                .to_string()
                .contains("scoped local-source evidence reads admit at most"),
            "{error}"
        );
    }

    #[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
    #[test]
    fn product_columnar_source_fingerprint_is_metadata_only_by_default() {
        let path = sql_local_source_test_path("parquet");
        fs::write(&path, b"same-sized content variant one").expect("write source");

        let first = fingerprint_local_source_file_with_budget_report(
            &path,
            "Parquet",
            None,
            SourceFingerprintPolicy::MetadataOnly,
        )
        .expect("fingerprint first source");
        fs::write(&path, b"same-sized content variant two").expect("rewrite source");
        let second = fingerprint_local_source_file_with_budget_report(
            &path,
            "Parquet",
            None,
            SourceFingerprintPolicy::MetadataOnly,
        )
        .expect("fingerprint second source");

        assert_eq!(first.bytes, second.bytes);
        assert!(first.digest.starts_with("fnv64:"));
        assert!(second.digest.starts_with("fnv64:"));
        assert_eq!(first.fingerprint_kind, "local_file_metadata_size_mtime");
        assert_eq!(first.fingerprint_policy, "metadata_only");
        assert_eq!(
            first.identity_source,
            "local_file_metadata_fast_prepare_identity"
        );
        assert!(!first.content_fingerprint_requested);
        assert!(!first.content_fingerprint_performed);
        assert_eq!(first.byte_acquisition_millis, 0);
        assert_eq!(first.full_body_millis, 0);
        assert!(!second.content_fingerprint_performed);

        fs::remove_file(&path).expect("remove source");
    }

    #[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
    #[test]
    fn product_columnar_source_content_fingerprint_is_explicit_proof() {
        let path = sql_local_source_test_path("parquet");
        fs::write(&path, b"same-sized content variant one").expect("write source");

        let first = fingerprint_local_source_file_with_budget_report(
            &path,
            "Parquet",
            None,
            SourceFingerprintPolicy::ContentDigest,
        )
        .expect("fingerprint first source");
        fs::write(&path, b"same-sized content variant two").expect("rewrite source");
        let second = fingerprint_local_source_file_with_budget_report(
            &path,
            "Parquet",
            None,
            SourceFingerprintPolicy::ContentDigest,
        )
        .expect("fingerprint second source");

        assert_eq!(first.bytes, second.bytes);
        assert!(first.digest.starts_with("fnv64:"));
        assert!(second.digest.starts_with("fnv64:"));
        assert_eq!(first.fingerprint_kind, "local_file_content_digest");
        assert_eq!(first.fingerprint_policy, "content_digest");
        assert_eq!(first.identity_source, "local_file_explicit_proof_digest");
        assert!(first.content_fingerprint_requested);
        assert!(first.content_fingerprint_performed);
        assert_ne!(first.digest, second.digest);
        assert_eq!(first.full_body_millis, 0);
        assert_eq!(second.full_body_millis, 0);

        fs::remove_file(&path).expect("remove source");
    }

    #[test]
    fn product_local_workflow_profile_disables_synthetic_caps() {
        let plan = LocalSourceReadPlan::required(
            BTreeSet::from(["id".to_string()]),
            "test_product_local_required_columns",
        );
        let mut content = String::from("id,label\n");
        for row_id in 0..=MAX_INPUT_ROWS {
            writeln!(&mut content, "{row_id},label-{row_id}").expect("write csv row");
        }

        let smoke_error = parse_csv_source_content_with_plan(&content, &plan, Some(MAX_INPUT_ROWS))
            .expect_err("smoke row cap remains enforced");
        let (header, rows) = parse_csv_source_content_with_plan(&content, &plan, None)
            .expect("product local workflow has no synthetic row cap");

        assert!(
            smoke_error
                .to_string()
                .contains("supports at most 50000 CSV data rows"),
            "{smoke_error}"
        );
        assert_eq!(header, vec!["id", "label"]);
        assert_eq!(rows.len(), MAX_INPUT_ROWS + 1);
        assert_eq!(
            SqlLocalSourceRuntimeProfile::ProductLocalWorkflow.input_row_cap_label(),
            "none_synthetic_row_cap_disabled"
        );

        assert!(
            !SqlLocalSourceRuntimeProfile::ProductLocalWorkflow.synthetic_input_row_cap_enabled()
        );
        assert!(
            !SqlLocalSourceRuntimeProfile::ProductLocalWorkflow.synthetic_output_row_cap_enabled()
        );
        assert!(
            !SqlLocalSourceRuntimeProfile::ProductLocalWorkflow.synthetic_source_byte_cap_enabled()
        );
        assert!(
            !SqlLocalSourceRuntimeProfile::ProductLocalWorkflow
                .synthetic_join_candidate_cap_enabled()
        );
        assert_eq!(
            SqlLocalSourceRuntimeProfile::ProductLocalWorkflow
                .read_limits()
                .output_rows,
            None
        );
        assert_eq!(
            SqlLocalSourceRuntimeProfile::ProductLocalWorkflow
                .read_limits()
                .join_candidate_rows,
            None
        );
        assert!(SqlLocalSourceRuntimeProfile::Smoke.synthetic_input_row_cap_enabled());
        assert!(SqlLocalSourceRuntimeProfile::Smoke.synthetic_output_row_cap_enabled());
        assert!(SqlLocalSourceRuntimeProfile::Smoke.synthetic_source_byte_cap_enabled());
        assert!(SqlLocalSourceRuntimeProfile::Smoke.synthetic_join_candidate_cap_enabled());

        assert_eq!(
            SqlLocalSourceRuntimeProfile::Smoke
                .read_limits()
                .output_rows,
            Some(MAX_LIMIT_ROWS)
        );
        assert_eq!(
            SqlLocalSourceRuntimeProfile::Smoke
                .read_limits()
                .join_candidate_rows,
            Some(MAX_JOIN_CANDIDATE_ROWS)
        );
    }

    #[test]
    fn source_unit_split_row_ranges_use_exact_source_units_only() {
        assert_eq!(
            source_unit_split_row_ranges(10, Some(&[(0, 2), (2, 7), (7, 10)])),
            vec![(0, 2), (2, 7), (7, 10)]
        );
        assert_eq!(source_unit_split_row_ranges(10, None), vec![(0, 10)]);
        assert_eq!(
            source_unit_split_row_ranges(10, Some(&[(0, 3), (4, 10)])),
            vec![(0, 10)]
        );
        assert_eq!(
            source_unit_split_row_ranges(10, Some(&[(0, 3), (3, 9)])),
            vec![(0, 10)]
        );
        assert_eq!(
            source_unit_split_row_ranges(0, Some(&[(0, 0)])),
            Vec::<(usize, usize)>::new()
        );
    }

    #[test]
    fn public_workflow_preparation_preserves_observed_stage_scopes_without_invented_zeros() {
        let raw_fields = vec![
            (
                "vortex_ingest_stream_validation_work_nanos".to_string(),
                "452".to_string(),
            ),
            (
                "vortex_ingest_stage_time_scope".to_string(),
                "overlapping_elapsed_work".to_string(),
            ),
            (
                "vortex_shared_native_memory_peak_reserved_bytes".to_string(),
                "8192".to_string(),
            ),
            (
                "vortex_shared_native_memory_exclusions".to_string(),
                "provider_bypass;rss".to_string(),
            ),
            (
                "vortex_ingest_unapproved_internal_field".to_string(),
                "private".to_string(),
            ),
        ];
        let fields = field_map(public_workflow_preparation_fields(&raw_fields));
        assert_field_eq(
            &fields,
            "public_workflow_preparation_vortex_ingest_stream_validation_work_nanos",
            "452",
        );
        assert_field_eq(
            &fields,
            "public_workflow_preparation_vortex_ingest_stage_time_scope",
            "overlapping_elapsed_work",
        );
        assert_field_eq(
            &fields,
            "public_workflow_preparation_vortex_shared_native_memory_peak_reserved_bytes",
            "8192",
        );
        assert_field_eq(
            &fields,
            "public_workflow_preparation_vortex_shared_native_memory_exclusions",
            "provider_bypass;rss",
        );
        assert!(
            !fields.contains_key("public_workflow_preparation_vortex_ingest_text_zstd_work_nanos")
        );
        assert!(
            !fields.contains_key(
                "public_workflow_preparation_vortex_ingest_unapproved_internal_field"
            )
        );
    }

    #[test]
    fn public_workflow_preparation_fields_keep_product_stream_source_evidence() {
        let raw_fields = vec![
            ("source_state_stream_batch_size".to_string(), "262144".to_string()),
            (
                "source_state_stream_unit_count_hint".to_string(),
                "8".to_string(),
            ),
            (
                "source_state_stream_unit_hint_kind".to_string(),
                "parquet_adaptive_row_group_task_count".to_string(),
            ),
            (
                "source_state_stream_policy".to_string(),
                "product_columnar_stream_batch_size_262144_rows;source_unit_interface=capillary_source_extents;source_unit_byte_range_count=8;source_unit_physical_bytes=8192;source_unit_byte_range_sample=0..1024|1024..2048;source_unit_physical_source=file_local_range_read_eligible;source_unit_scheduler_wait_status=bounded_ordered_delivery;source_unit_decode_wait_status=measured_in_stream_source_pull_micros;source_unit_writer_starvation_status=surfaced_as_prepare_writer_backpressure_or_timer_overlap;source_unit_physical_bandwidth_status=derivable_from_source_bytes_and_prepare_source_hydration_millis;parquet_extent_row_groups=8;parquet_extent_row_groups_with_byte_ranges=8;parquet_extent_column_chunks=64;parquet_extent_column_chunks_with_byte_ranges=64;parquet_extent_compressed_bytes=8192;parquet_extent_uncompressed_bytes=16384;parquet_extent_codec_summary=SNAPPY:64".to_string(),
            ),
            (
                "source_state_dictionary_preservation_status".to_string(),
                "parquet_arrow_reader_preserves_physical_columnar_values_when_provider_surfaces_dictionary;parquet_extent_dictionary_pages=8;parquet_extent_statistics=64;parquet_extent_row_group_summary=rg=0,rows=2,chunks=8,compressed_bytes=1024,uncompressed_bytes=2048,start=0,len=1024"
                    .to_string(),
            ),
            (
                "not_selected_internal_field".to_string(),
                "should_drop".to_string(),
            ),
        ];
        let fields = field_map(public_workflow_preparation_fields(&raw_fields));

        assert_field_eq(
            &fields,
            "public_workflow_preparation_source_state_stream_batch_size",
            "262144",
        );
        assert_field_eq(
            &fields,
            "public_workflow_preparation_source_state_stream_unit_count_hint",
            "8",
        );
        assert_field_eq(
            &fields,
            "public_workflow_preparation_source_state_stream_unit_hint_kind",
            "parquet_adaptive_row_group_task_count",
        );
        assert_field_eq(
            &fields,
            "public_workflow_preparation_source_state_stream_policy",
            "product_columnar_stream_batch_size_262144_rows;source_unit_interface=capillary_source_extents;source_unit_byte_range_count=8;source_unit_physical_bytes=8192;source_unit_byte_range_sample=0..1024|1024..2048;source_unit_physical_source=file_local_range_read_eligible;source_unit_scheduler_wait_status=bounded_ordered_delivery;source_unit_decode_wait_status=measured_in_stream_source_pull_micros;source_unit_writer_starvation_status=surfaced_as_prepare_writer_backpressure_or_timer_overlap;source_unit_physical_bandwidth_status=derivable_from_source_bytes_and_prepare_source_hydration_millis;parquet_extent_row_groups=8;parquet_extent_row_groups_with_byte_ranges=8;parquet_extent_column_chunks=64;parquet_extent_column_chunks_with_byte_ranges=64;parquet_extent_compressed_bytes=8192;parquet_extent_uncompressed_bytes=16384;parquet_extent_codec_summary=SNAPPY:64",
        );
        assert_field_eq(
            &fields,
            "public_workflow_preparation_source_state_stream_unit_interface",
            "capillary_source_extents",
        );
        assert_field_eq(
            &fields,
            "public_workflow_preparation_source_state_stream_unit_byte_range_count",
            "8",
        );
        assert_field_eq(
            &fields,
            "public_workflow_preparation_source_state_stream_unit_physical_bytes",
            "8192",
        );
        assert_field_eq(
            &fields,
            "public_workflow_preparation_source_state_stream_unit_physical_source",
            "file_local_range_read_eligible",
        );
        assert_field_eq(
            &fields,
            "public_workflow_preparation_source_state_parquet_extent_row_group_count",
            "8",
        );
        assert_field_eq(
            &fields,
            "public_workflow_preparation_source_state_parquet_extent_column_chunk_count",
            "64",
        );
        assert_field_eq(
            &fields,
            "public_workflow_preparation_source_state_parquet_extent_compressed_bytes",
            "8192",
        );
        assert_field_eq(
            &fields,
            "public_workflow_preparation_source_state_parquet_extent_codec_summary",
            "SNAPPY:64",
        );
        assert_field_eq(
            &fields,
            "public_workflow_preparation_source_state_dictionary_preservation_status",
            "parquet_arrow_reader_preserves_physical_columnar_values_when_provider_surfaces_dictionary;parquet_extent_dictionary_pages=8;parquet_extent_statistics=64;parquet_extent_row_group_summary=rg=0,rows=2,chunks=8,compressed_bytes=1024,uncompressed_bytes=2048,start=0,len=1024",
        );
        assert_field_eq(
            &fields,
            "public_workflow_preparation_source_state_parquet_extent_dictionary_page_count",
            "8",
        );
        assert_field_eq(
            &fields,
            "public_workflow_preparation_source_state_parquet_extent_statistics_count",
            "64",
        );
        assert!(!fields.contains_key("public_workflow_preparation_not_selected_internal_field"));
    }

    #[test]
    fn layout_writer_provider_uses_batch_presence_for_zero_row_columnar_sources() {
        let source_adapter =
            LocalInputAdapterSelection::infer_from_path(Path::new("schema-only.parquet"))
                .expect("infer parquet adapter");
        let source = VortexIngestSourceData {
            source_format: source_adapter.source_format,
            source_adapter,
            header: vec!["id".to_string()],
            column_arrow_dtypes: vec![Some(DataType::Int64)],
            read_plan: LocalSourceReadPlan::full("test_zero_row_columnar_source"),
            materialized_columns: vec!["id".to_string()],
            reader_projection_columns: vec!["id".to_string()],
            projection_pushdown_status: LocalSourceProjectionPushdownStatus::NotRequestedFullRead,
            source_bytes: 128,
            source_digest: "fnv64:test".to_string(),
            source_fingerprint: SourceFingerprintEvidence::metadata_only(),
            row_count: 0,
            row_count_known: true,
            source_split_row_ranges: vec![(0, 0)],
            source_metadata_scout_millis: 0,
            source_byte_acquisition_millis: 0,
            source_full_body_millis: 0,
            read_millis: 0,
            compatibility_parse_millis: 0,
            source_to_columnar_millis: 0,
            record_batch_count: 1,
            source_stream_batch_size: 0,
            source_stream_unit_count_hint: Some(1),
            source_stream_unit_hint_kind: "test_record_batch_count".to_string(),
            source_stream_policy: "test_source_defined_record_batches".to_string(),
            source_dictionary_preservation_status: "test_not_applicable".to_string(),
            ingest_executor_status: "serial_pull_reader".to_string(),
            ingest_executor_kind: "test_record_batch_reader".to_string(),
            ingest_executor_requested_parallelism: 1,
            ingest_executor_applied_parallelism: 1,
            ingest_executor_unit_count_hint: Some(1),
            materialization_layout: "arrow_record_batch_columnar_source_state",
            parse_normalization: "structured_reader_to_arrow_record_batches",
            columnar_source_preserved: true,
        };

        assert_eq!(layout_writer_provider_kind(&source), "vortex_array_kernel");
        assert_eq!(
            layout_writer_provider_surface(&source),
            "ArrayRef::from_arrow(RecordBatch);VortexSession::write_options().write(ArrayStream)"
        );
    }

    #[test]
    fn layout_writer_provider_uses_streaming_vortex_provider_for_non_empty_columnar_source() {
        let source_adapter = LocalInputAdapterSelection::infer_from_path(Path::new("hits.parquet"))
            .expect("infer parquet adapter");
        let source = VortexIngestSourceData {
            source_format: source_adapter.source_format,
            source_adapter,
            header: vec!["URL".to_string()],
            column_arrow_dtypes: vec![Some(DataType::Utf8)],
            read_plan: LocalSourceReadPlan::full("test_streaming_columnar_source"),
            materialized_columns: vec!["URL".to_string()],
            reader_projection_columns: vec!["URL".to_string()],
            projection_pushdown_status: LocalSourceProjectionPushdownStatus::NotRequestedFullRead,
            source_bytes: 1024,
            source_digest: "fnv64:test".to_string(),
            source_fingerprint: SourceFingerprintEvidence::metadata_only(),
            row_count: 100_000_000,
            row_count_known: true,
            source_split_row_ranges: vec![(0, 100_000_000)],
            source_metadata_scout_millis: 0,
            source_byte_acquisition_millis: 0,
            source_full_body_millis: 0,
            read_millis: 0,
            compatibility_parse_millis: 0,
            source_to_columnar_millis: 0,
            record_batch_count: 0,
            source_stream_batch_size: 262_144,
            source_stream_unit_count_hint: Some(8),
            source_stream_unit_hint_kind: "parquet_adaptive_row_group_task_count".to_string(),
            source_stream_policy: "product_columnar_stream_batch_size_262144_rows".to_string(),
            source_dictionary_preservation_status:
                "parquet_arrow_reader_preserves_physical_columnar_values_when_provider_surfaces_dictionary"
                    .to_string(),
            ingest_executor_status: "bounded_capillary_prefetch_active".to_string(),
            ingest_executor_kind: "source_reader_to_vortex_writer_prefetch_pipeline".to_string(),
            ingest_executor_requested_parallelism: 2,
            ingest_executor_applied_parallelism: 2,
            ingest_executor_unit_count_hint: None,
            materialization_layout: "streaming_arrow_record_batch_columnar_source_state",
            parse_normalization: "structured_reader_to_streaming_arrow_record_batches",
            columnar_source_preserved: true,
        };

        assert_eq!(layout_writer_provider_kind(&source), "vortex_array_kernel");
        assert_eq!(
            layout_writer_provider_surface(&source),
            "ArrayRef::from_arrow(RecordBatch);streaming ArrayIterator;VortexSession::write_options().write(ArrayStream)"
        );
        assert_eq!(
            layout_workload_constitution(&source),
            "product_vortex_prepare_once;format=parquet;scale=large_olap;adapter=streaming_columnar_source_state;profile=url_text_olap;layout_family=url_domain_dictionary_length_layout;text_domain=true;time_bucket=false;counter=false;key_profile=dictionary_text_keys;dictionary=source_dictionary_or_derived_dictionary_evidence"
        );
        assert_eq!(
            layout_expected_read_tradeoff(&source),
            "prefer_metadata_pruning_dictionary_domain_length_and_time_bucket_execution"
        );
        assert_eq!(
            layout_expected_write_tradeoff(&source),
            "prefer_column_family_fast_zstd_for_payload_text_and_embedded_layout_statistics"
        );
    }

    #[test]
    fn layout_writer_compression_candidates_use_source_dtypes_before_name_heuristics() {
        let source_adapter = LocalInputAdapterSelection::infer_from_path(Path::new("hits.parquet"))
            .expect("infer parquet adapter");
        let source = VortexIngestSourceData {
            source_format: source_adapter.source_format,
            source_adapter,
            header: vec![
                "URL".to_string(),
                "Title".to_string(),
                "ClientIP".to_string(),
                "__shardloom_derived_utf8_len_URL".to_string(),
                "SearchPhrase".to_string(),
                "BrowserCountry".to_string(),
                "ParamCurrency".to_string(),
                "Params".to_string(),
            ],
            column_arrow_dtypes: vec![
                Some(DataType::Utf8),
                Some(DataType::Dictionary(
                    Box::new(DataType::Int32),
                    Box::new(DataType::Utf8),
                )),
                Some(DataType::UInt32),
                Some(DataType::UInt32),
                None,
                Some(DataType::Utf8),
                Some(DataType::Utf8),
                Some(DataType::Utf8),
            ],
            read_plan: LocalSourceReadPlan::full("test_writer_candidates"),
            materialized_columns: vec!["URL".to_string(), "Title".to_string()],
            reader_projection_columns: vec!["URL".to_string(), "Title".to_string()],
            projection_pushdown_status: LocalSourceProjectionPushdownStatus::NotRequestedFullRead,
            source_bytes: 1024,
            source_digest: "fnv64:test".to_string(),
            source_fingerprint: SourceFingerprintEvidence::metadata_only(),
            row_count: 100_000_000,
            row_count_known: true,
            source_split_row_ranges: vec![(0, 100_000_000)],
            source_metadata_scout_millis: 0,
            source_byte_acquisition_millis: 0,
            source_full_body_millis: 0,
            read_millis: 0,
            compatibility_parse_millis: 0,
            source_to_columnar_millis: 0,
            record_batch_count: 0,
            source_stream_batch_size: 262_144,
            source_stream_unit_count_hint: Some(8),
            source_stream_unit_hint_kind: "test_units".to_string(),
            source_stream_policy: "product_columnar_stream_batch_size_262144_rows".to_string(),
            source_dictionary_preservation_status:
                "parquet_arrow_reader_preserves_physical_columnar_values_when_provider_surfaces_dictionary"
                    .to_string(),
            ingest_executor_status: "bounded_capillary_prefetch_active".to_string(),
            ingest_executor_kind: "source_reader_to_vortex_writer_prefetch_pipeline".to_string(),
            ingest_executor_requested_parallelism: 2,
            ingest_executor_applied_parallelism: 2,
            ingest_executor_unit_count_hint: None,
            materialization_layout: "streaming_arrow_record_batch_columnar_source_state",
            parse_normalization: "structured_reader_to_streaming_arrow_record_batches",
            columnar_source_preserved: true,
        };

        assert_eq!(
            layout_writer_compression_candidate_fields(&source),
            vec![
                "URL".to_string(),
                "Title".to_string(),
                "SearchPhrase".to_string(),
                "BrowserCountry".to_string(),
                "ParamCurrency".to_string(),
                "Params".to_string(),
            ]
        );
    }

    #[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
    #[test]
    fn large_text_domain_columnar_source_does_not_synthesize_full_url_metadata() {
        use arrow_array::{Int64Array, RecordBatch, RecordBatchReader, StringArray};
        use arrow_schema::{DataType, Field, Schema, SchemaRef};
        use std::collections::VecDeque;
        use std::sync::Arc;

        struct TestRecordBatchReader {
            schema: SchemaRef,
            batches: VecDeque<RecordBatch>,
        }

        impl Iterator for TestRecordBatchReader {
            type Item = std::result::Result<RecordBatch, arrow_schema::ArrowError>;

            fn next(&mut self) -> Option<Self::Item> {
                self.batches.pop_front().map(Ok)
            }
        }

        impl RecordBatchReader for TestRecordBatchReader {
            fn schema(&self) -> SchemaRef {
                Arc::clone(&self.schema)
            }
        }

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("Referer", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec![
                    Some("https://www.google.com/search"),
                    Some("https://example.com/page"),
                    None,
                ])),
            ],
        )
        .expect("record batch");
        let source = shardloom_vortex::FlatLocalColumnarStreamSource {
            header: vec!["id".to_string(), "Referer".to_string()],
            column_dtypes: vec![Some(LogicalDType::Int64), Some(LogicalDType::Utf8)],
            column_arrow_dtypes: vec![Some(DataType::Int64), Some(DataType::Utf8)],
            materialized_columns: vec!["id".to_string(), "Referer".to_string()],
            reader_projection_columns: vec!["id".to_string(), "Referer".to_string()],
            row_count_hint: Some(100_000_000),
            record_batch_count_hint: Some(1),
            source_stream_batch_size:
                shardloom_vortex::universal_format_io::PRODUCT_COLUMNAR_LARGE_STREAM_RECORD_BATCH_ROWS,
            source_stream_unit_count_hint: Some(1),
            source_stream_unit_row_ranges: Some(vec![(0, 3)]),
            source_stream_unit_hint_kind: "test_large_text_domain_record_batch".to_string(),
            source_stream_policy: "product_columnar_stream_batch_size_262144_rows".to_string(),
            source_dictionary_preservation_status:
                "parquet_arrow_reader_preserves_physical_columnar_values_when_provider_surfaces_dictionary"
                    .to_string(),
            ingest_executor_status: "serial_pull_reader".to_string(),
            ingest_executor_kind: "test_large_text_domain_record_batch_reader".to_string(),
            ingest_executor_requested_parallelism: 1,
            ingest_executor_applied_parallelism: 1,
            ingest_executor_unit_count_hint: Some(1),
            source_identities: Vec::new(),
            #[cfg(feature = "vortex-write")]
            ingest_runtime: None,
            embedded_derived_build_micros: shardloom_vortex::new_embedded_derived_build_micros_counter(
            ),
            reader: Box::new(TestRecordBatchReader {
                schema,
                batches: VecDeque::from([batch]),
            }),
        };

        let mut source =
            with_layout_advised_embedded_derived_columns_columnar_stream_source(source);

        assert!(
            source.source_dictionary_preservation_status.contains(
                "source_native_embedded_derived_columns=not_available_for_current_arrow_layout"
            ),
            "{}",
            source.source_dictionary_preservation_status
        );
        assert!(
            !source
                .source_dictionary_preservation_status
                .contains("__shardloom_derived_url_domain_Referer"),
            "{}",
            source.source_dictionary_preservation_status
        );
        let names = source
            .reader
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect::<Vec<_>>();
        assert_eq!(names, vec!["id".to_string(), "Referer".to_string()]);
        let batch = source.reader.next().expect("batch").expect("batch ok");
        assert_eq!(batch.num_columns(), 2);
    }

    #[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
    #[test]
    fn large_plain_utf8_parquet_guard_uses_lean_runtime_metadata_profile() {
        large_utf8_parquet_guard_prepared_batch(false);
    }

    #[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
    #[test]
    fn large_utf8_view_parquet_guard_uses_same_lean_runtime_metadata() {
        use arrow_array::{StringArray, StringViewArray};

        let plain = large_utf8_parquet_guard_prepared_batch(false);
        let views = large_utf8_parquet_guard_prepared_batch(true);
        assert_eq!(plain.num_rows(), views.num_rows());
        assert_eq!(plain.num_columns(), views.num_columns());
        for (expected, actual) in plain.columns().iter().zip(views.columns()) {
            if let Some(expected) = expected.as_any().downcast_ref::<StringArray>() {
                let actual = actual
                    .as_any()
                    .downcast_ref::<StringViewArray>()
                    .expect("string views");
                assert_eq!(
                    expected.iter().collect::<Vec<_>>(),
                    actual.iter().collect::<Vec<_>>()
                );
            } else {
                assert_eq!(expected.to_data(), actual.to_data());
            }
        }
    }

    #[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
    fn large_utf8_parquet_guard_prepared_batch(use_views: bool) -> arrow_array::RecordBatch {
        use arrow_array::{
            ArrayRef, Int64Array, RecordBatch, RecordBatchReader, StringArray, StringViewArray,
        };
        use arrow_schema::{DataType, Field, Schema, SchemaRef};
        use std::collections::VecDeque;
        use std::sync::Arc;

        struct TestRecordBatchReader {
            schema: SchemaRef,
            batches: VecDeque<RecordBatch>,
        }

        impl Iterator for TestRecordBatchReader {
            type Item = std::result::Result<RecordBatch, arrow_schema::ArrowError>;

            fn next(&mut self) -> Option<Self::Item> {
                self.batches.pop_front().map(Ok)
            }
        }

        impl RecordBatchReader for TestRecordBatchReader {
            fn schema(&self) -> SchemaRef {
                Arc::clone(&self.schema)
            }
        }

        let text_type = if use_views {
            DataType::Utf8View
        } else {
            DataType::Utf8
        };
        let text_array = |values: Vec<Option<&str>>| -> ArrayRef {
            if use_views {
                Arc::new(StringViewArray::from(values))
            } else {
                Arc::new(StringArray::from(values))
            }
        };
        let schema = Arc::new(Schema::new(vec![
            Field::new("URL", text_type.clone(), true),
            Field::new("SearchPhrase", text_type.clone(), true),
            Field::new("Title", text_type, true),
            Field::new("EventTime", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                text_array(vec![
                    Some("https://www.google.com/a"),
                    Some("https://docs.rs/b"),
                    None,
                    Some("https://example.com/日本語"),
                ]),
                text_array(vec![Some("rust"), Some("shardloom"), None, Some("日本語")]),
                text_array(vec![
                    Some("Google title"),
                    Some("Other title"),
                    None,
                    Some("Longer UTF-8 title 日本語"),
                ]),
                Arc::new(Int64Array::from(vec![60_i64, 121, 180, 241])) as ArrayRef,
            ],
        )
        .expect("record batch");
        let source = shardloom_vortex::FlatLocalColumnarStreamSource {
            header: vec![
                "URL".to_string(),
                "SearchPhrase".to_string(),
                "Title".to_string(),
                "EventTime".to_string(),
            ],
            column_dtypes: vec![
                Some(LogicalDType::Utf8),
                Some(LogicalDType::Utf8),
                Some(LogicalDType::Utf8),
                Some(LogicalDType::Int64),
            ],
            column_arrow_dtypes: schema
                .fields()
                .iter()
                .map(|field| Some(field.data_type().clone()))
                .collect(),
            materialized_columns: vec![
                "URL".to_string(),
                "SearchPhrase".to_string(),
                "Title".to_string(),
                "EventTime".to_string(),
            ],
            reader_projection_columns: vec![
                "URL".to_string(),
                "SearchPhrase".to_string(),
                "Title".to_string(),
                "EventTime".to_string(),
            ],
            row_count_hint: Some(100_000_000),
            record_batch_count_hint: Some(1),
            source_stream_batch_size:
                shardloom_vortex::universal_format_io::PRODUCT_COLUMNAR_LARGE_STREAM_RECORD_BATCH_ROWS,
            source_stream_unit_count_hint: Some(1),
            source_stream_unit_row_ranges: Some(vec![(0, 4)]),
            source_stream_unit_hint_kind: "test_plain_utf8_large_parquet_guard".to_string(),
            source_stream_policy: "product_columnar_stream_batch_size_262144_rows".to_string(),
            source_dictionary_preservation_status: if use_views {
                "parquet_arrow_reader_uses_utf8_view_for_large_olap_text_zstd_artifact_size_guard"
            } else {
                "parquet_arrow_reader_uses_plain_utf8_for_large_olap_text_zstd_artifact_size_guard"
            }.to_string(),
            ingest_executor_status: "serial_pull_reader".to_string(),
            ingest_executor_kind: "test_plain_utf8_large_parquet_reader".to_string(),
            ingest_executor_requested_parallelism: 1,
            ingest_executor_applied_parallelism: 1,
            ingest_executor_unit_count_hint: Some(1),
            source_identities: Vec::new(),
            #[cfg(feature = "vortex-write")]
            ingest_runtime: None,
            embedded_derived_build_micros: shardloom_vortex::new_embedded_derived_build_micros_counter(
            ),
            reader: Box::new(TestRecordBatchReader {
                schema,
                batches: VecDeque::from([batch]),
            }),
        };

        let mut source =
            with_layout_advised_embedded_derived_columns_columnar_stream_source(source);

        assert!(
            source.source_dictionary_preservation_status.contains(
                "embedded_derived_column_mode=source_native_lean_runtime_dictionary_or_typed_time_only"
            ),
            "{}",
            source.source_dictionary_preservation_status
        );
        let names = source
            .reader
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect::<Vec<_>>();
        assert!(names.contains(&"__shardloom_derived_utf8_len_URL".to_string()));
        assert!(names.contains(&"__shardloom_derived_url_domain_URL".to_string()));
        assert!(names.contains(&"__shardloom_derived_utf8_len_SearchPhrase".to_string()));
        assert!(names.contains(&"__shardloom_derived_extract_minute_EventTime".to_string()));
        assert!(names.contains(&"__shardloom_derived_date_trunc_minute_EventTime".to_string()));
        assert!(!names.contains(&"__shardloom_derived_utf8_len_Title".to_string()));
        let batch = source.reader.next().expect("batch").expect("batch ok");
        assert_eq!(batch.num_columns(), 9);
        assert_eq!(batch.num_rows(), 4);
        batch
    }

    #[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
    #[test]
    fn large_dictionary_columnar_source_uses_lean_runtime_metadata_profile() {
        use arrow_array::builder::StringDictionaryBuilder;
        use arrow_array::types::Int32Type;
        use arrow_array::{ArrayRef, Int64Array, RecordBatch, RecordBatchReader};
        use arrow_schema::{DataType, Field, Schema, SchemaRef};
        use std::collections::VecDeque;
        use std::sync::Arc;

        struct TestRecordBatchReader {
            schema: SchemaRef,
            batches: VecDeque<RecordBatch>,
        }

        impl Iterator for TestRecordBatchReader {
            type Item = std::result::Result<RecordBatch, arrow_schema::ArrowError>;

            fn next(&mut self) -> Option<Self::Item> {
                self.batches.pop_front().map(Ok)
            }
        }

        impl RecordBatchReader for TestRecordBatchReader {
            fn schema(&self) -> SchemaRef {
                Arc::clone(&self.schema)
            }
        }

        let dictionary_array = |values: &[Option<&str>]| -> ArrayRef {
            let mut builder = StringDictionaryBuilder::<Int32Type>::new();
            for value in values {
                if let Some(value) = value {
                    builder.append(value).expect("append dictionary value");
                } else {
                    builder.append_null();
                }
            }
            Arc::new(builder.finish()) as ArrayRef
        };
        let dictionary_type =
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
        let schema = Arc::new(Schema::new(vec![
            Field::new("URL", dictionary_type.clone(), true),
            Field::new("Referer", dictionary_type.clone(), true),
            Field::new("SearchPhrase", dictionary_type.clone(), true),
            Field::new("Title", dictionary_type.clone(), true),
            Field::new("OriginalURL", dictionary_type, true),
            Field::new("EventTime", DataType::Int64, false),
            Field::new("ClientEventTime", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                dictionary_array(&[Some("https://www.google.com/a"), Some("https://docs.rs/b")]),
                dictionary_array(&[Some("https://ref.example/a"), Some("https://google.com/r")]),
                dictionary_array(&[Some("rust"), Some("shardloom")]),
                dictionary_array(&[Some("Google title"), Some("Other title")]),
                dictionary_array(&[Some("https://original.example/a"), Some("https://origin/b")]),
                Arc::new(Int64Array::from(vec![60_i64, 121])) as ArrayRef,
                Arc::new(Int64Array::from(vec![300_i64, 360])) as ArrayRef,
            ],
        )
        .expect("record batch");
        let source = shardloom_vortex::FlatLocalColumnarStreamSource {
            header: vec![
                "URL".to_string(),
                "Referer".to_string(),
                "SearchPhrase".to_string(),
                "Title".to_string(),
                "OriginalURL".to_string(),
                "EventTime".to_string(),
                "ClientEventTime".to_string(),
            ],
            column_dtypes: vec![
                Some(LogicalDType::Utf8),
                Some(LogicalDType::Utf8),
                Some(LogicalDType::Utf8),
                Some(LogicalDType::Utf8),
                Some(LogicalDType::Utf8),
                Some(LogicalDType::Int64),
                Some(LogicalDType::Int64),
            ],
            column_arrow_dtypes: schema
                .fields()
                .iter()
                .map(|field| Some(field.data_type().clone()))
                .collect(),
            materialized_columns: vec![
                "URL".to_string(),
                "Referer".to_string(),
                "SearchPhrase".to_string(),
                "Title".to_string(),
                "OriginalURL".to_string(),
                "EventTime".to_string(),
                "ClientEventTime".to_string(),
            ],
            reader_projection_columns: vec![
                "URL".to_string(),
                "Referer".to_string(),
                "SearchPhrase".to_string(),
                "Title".to_string(),
                "OriginalURL".to_string(),
                "EventTime".to_string(),
                "ClientEventTime".to_string(),
            ],
            row_count_hint: Some(100_000_000),
            record_batch_count_hint: Some(1),
            source_stream_batch_size:
                shardloom_vortex::universal_format_io::PRODUCT_COLUMNAR_LARGE_STREAM_RECORD_BATCH_ROWS,
            source_stream_unit_count_hint: Some(1),
            source_stream_unit_row_ranges: Some(vec![(0, 2)]),
            source_stream_unit_hint_kind: "test_lean_dictionary_record_batch".to_string(),
            source_stream_policy: "product_columnar_stream_batch_size_262144_rows".to_string(),
            source_dictionary_preservation_status:
                "parquet_arrow_reader_requested_dictionary_preservation_for_string_derived_columns"
                    .to_string(),
            ingest_executor_status: "serial_pull_reader".to_string(),
            ingest_executor_kind: "test_dictionary_record_batch_reader".to_string(),
            ingest_executor_requested_parallelism: 1,
            ingest_executor_applied_parallelism: 1,
            ingest_executor_unit_count_hint: Some(1),
            source_identities: Vec::new(),
            #[cfg(feature = "vortex-write")]
            ingest_runtime: None,
            embedded_derived_build_micros: shardloom_vortex::new_embedded_derived_build_micros_counter(
            ),
            reader: Box::new(TestRecordBatchReader {
                schema,
                batches: VecDeque::from([batch]),
            }),
        };

        let mut source =
            with_layout_advised_embedded_derived_columns_columnar_stream_source(source);

        assert!(
            source.source_dictionary_preservation_status.contains(
                "embedded_derived_column_mode=source_native_lean_runtime_dictionary_or_typed_time_only"
            ),
            "{}",
            source.source_dictionary_preservation_status
        );
        let names = source
            .reader
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect::<Vec<_>>();
        assert!(names.contains(&"__shardloom_derived_utf8_len_URL".to_string()));
        assert!(names.contains(&"__shardloom_derived_url_domain_URL".to_string()));
        assert!(names.contains(&"__shardloom_derived_utf8_len_Referer".to_string()));
        assert!(names.contains(&"__shardloom_derived_url_domain_Referer".to_string()));
        assert!(names.contains(&"__shardloom_derived_utf8_len_SearchPhrase".to_string()));
        assert!(names.contains(&"__shardloom_derived_extract_minute_EventTime".to_string()));
        assert!(names.contains(&"__shardloom_derived_date_trunc_minute_EventTime".to_string()));
        assert!(!names.contains(&"__shardloom_derived_utf8_len_Title".to_string()));
        assert!(!names.contains(&"__shardloom_derived_utf8_len_OriginalURL".to_string()));
        assert!(!names.contains(&"__shardloom_derived_url_domain_OriginalURL".to_string()));
        assert!(!names.contains(&"__shardloom_derived_extract_minute_ClientEventTime".to_string()));
        assert!(
            !names.contains(&"__shardloom_derived_date_trunc_minute_ClientEventTime".to_string())
        );
        assert_eq!(
            source
                .reader
                .next()
                .expect("batch")
                .expect("batch ok")
                .num_columns(),
            14
        );
    }

    #[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
    #[test]
    fn source_native_time_metadata_does_not_force_full_text_domain_metadata() {
        use arrow_array::{Array as _, Int64Array, RecordBatch, RecordBatchReader, StringArray};
        use arrow_schema::{DataType, Field, Schema, SchemaRef};
        use std::collections::VecDeque;
        use std::sync::Arc;

        struct TestRecordBatchReader {
            schema: SchemaRef,
            batches: VecDeque<RecordBatch>,
        }

        impl Iterator for TestRecordBatchReader {
            type Item = std::result::Result<RecordBatch, arrow_schema::ArrowError>;

            fn next(&mut self) -> Option<Self::Item> {
                self.batches.pop_front().map(Ok)
            }
        }

        impl RecordBatchReader for TestRecordBatchReader {
            fn schema(&self) -> SchemaRef {
                Arc::clone(&self.schema)
            }
        }

        let schema = Arc::new(Schema::new(vec![
            Field::new("EventTime", DataType::Int64, false),
            Field::new("Referer", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![60_i64, 121, 180])),
                Arc::new(StringArray::from(vec![
                    Some("https://www.google.com/search"),
                    Some("https://example.com/page"),
                    None,
                ])),
            ],
        )
        .expect("record batch");
        let source = shardloom_vortex::FlatLocalColumnarStreamSource {
            header: vec!["EventTime".to_string(), "Referer".to_string()],
            column_dtypes: vec![Some(LogicalDType::Int64), Some(LogicalDType::Utf8)],
            column_arrow_dtypes: vec![Some(DataType::Int64), Some(DataType::Utf8)],
            materialized_columns: vec!["EventTime".to_string(), "Referer".to_string()],
            reader_projection_columns: vec!["EventTime".to_string(), "Referer".to_string()],
            row_count_hint: Some(100_000_000),
            record_batch_count_hint: Some(1),
            source_stream_batch_size:
                shardloom_vortex::universal_format_io::PRODUCT_COLUMNAR_LARGE_STREAM_RECORD_BATCH_ROWS,
            source_stream_unit_count_hint: Some(1),
            source_stream_unit_row_ranges: Some(vec![(0, 3)]),
            source_stream_unit_hint_kind: "test_time_plus_text_record_batch".to_string(),
            source_stream_policy: "test_time_plus_text_units".to_string(),
            source_dictionary_preservation_status:
                "parquet_arrow_reader_preserves_source_native_time_metadata".to_string(),
            ingest_executor_status: "serial_pull_reader".to_string(),
            ingest_executor_kind: "test_time_plus_text_record_batch_reader".to_string(),
            ingest_executor_requested_parallelism: 1,
            ingest_executor_applied_parallelism: 1,
            ingest_executor_unit_count_hint: Some(1),
            source_identities: Vec::new(),
            #[cfg(feature = "vortex-write")]
            ingest_runtime: None,
            embedded_derived_build_micros: shardloom_vortex::new_embedded_derived_build_micros_counter(
            ),
            reader: Box::new(TestRecordBatchReader {
                schema,
                batches: VecDeque::from([batch]),
            }),
        };

        let mut source =
            with_layout_advised_embedded_derived_columns_columnar_stream_source(source);

        assert!(
            source.source_dictionary_preservation_status.contains(
                "embedded_derived_column_mode=source_native_dictionary_or_typed_time_only"
            ),
            "{}",
            source.source_dictionary_preservation_status
        );
        let names = source
            .reader
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect::<Vec<_>>();
        assert!(names.contains(&"__shardloom_derived_extract_minute_EventTime".to_string()));
        assert!(names.contains(&"__shardloom_derived_date_trunc_minute_EventTime".to_string()));
        assert!(!names.contains(&"__shardloom_derived_utf8_len_Referer".to_string()));
        assert!(!names.contains(&"__shardloom_derived_url_domain_Referer".to_string()));

        let batch = source.reader.next().expect("batch").expect("batch ok");
        let event_minutes = batch
            .column(2)
            .as_any()
            .downcast_ref::<arrow_array::UInt8Array>()
            .expect("minute uint8");
        assert_eq!(event_minutes.value(0), 1);
        assert_eq!(event_minutes.value(1), 2);
        assert_eq!(event_minutes.value(2), 3);
        let event_minute_buckets = batch
            .column(3)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .expect("minute bucket int64");
        assert_eq!(event_minute_buckets.value(0), 60);
        assert_eq!(event_minute_buckets.value(1), 120);
        assert_eq!(event_minute_buckets.value(2), 180);
    }

    #[test]
    fn public_workflow_preparation_keeps_layout_profile_fields() {
        let raw_fields = vec![
            (
                "vortex_layout_write_advisor_source_scale".to_string(),
                "large_olap".to_string(),
            ),
            (
                "vortex_layout_write_advisor_profile_family".to_string(),
                "url_time_counter_olap".to_string(),
            ),
            (
                "vortex_layout_write_advisor_prepared_layout_family".to_string(),
                "url_time_counter_dictionary_stats_layout".to_string(),
            ),
            (
                "vortex_layout_write_advisor_text_domain_columns".to_string(),
                "true".to_string(),
            ),
            (
                "vortex_layout_write_advisor_time_bucket_columns".to_string(),
                "true".to_string(),
            ),
            (
                "vortex_layout_write_advisor_counter_columns".to_string(),
                "true".to_string(),
            ),
            (
                "vortex_layout_write_advisor_key_profile".to_string(),
                "high_cardinality_numeric_text_time_keys".to_string(),
            ),
            (
                "vortex_layout_write_advisor_dictionary_profile".to_string(),
                "source_dictionary_or_derived_dictionary_evidence".to_string(),
            ),
            (
                "vortex_layout_write_advisor_expected_read_tradeoff".to_string(),
                "prefer_metadata_pruning_dictionary_domain_length_and_time_bucket_execution"
                    .to_string(),
            ),
            (
                "vortex_layout_write_advisor_expected_write_tradeoff".to_string(),
                "prefer_column_family_fast_zstd_for_payload_text_and_embedded_layout_statistics"
                    .to_string(),
            ),
            (
                "vortex_writer_compression_field_count".to_string(),
                "3".to_string(),
            ),
            (
                "vortex_writer_compression_field_names".to_string(),
                "URL,Referer,SearchPhrase".to_string(),
            ),
            (
                "vortex_writer_compression_decision_count".to_string(),
                "3".to_string(),
            ),
            (
                "vortex_writer_compression_decisions".to_string(),
                "field=URL;decision=compress;codec=zstd|field=Referer;decision=compress;codec=zstd|field=SearchPhrase;decision=compress;codec=zstd".to_string(),
            ),
        ];
        let fields = field_map(public_workflow_preparation_fields(&raw_fields));

        assert_field_eq(
            &fields,
            "public_workflow_preparation_vortex_layout_write_advisor_source_scale",
            "large_olap",
        );
        assert_field_eq(
            &fields,
            "public_workflow_preparation_vortex_layout_write_advisor_profile_family",
            "url_time_counter_olap",
        );
        assert_field_eq(
            &fields,
            "public_workflow_preparation_vortex_layout_write_advisor_prepared_layout_family",
            "url_time_counter_dictionary_stats_layout",
        );
        assert_field_eq(
            &fields,
            "public_workflow_preparation_vortex_layout_write_advisor_text_domain_columns",
            "true",
        );
        assert_field_eq(
            &fields,
            "public_workflow_preparation_vortex_layout_write_advisor_time_bucket_columns",
            "true",
        );
        assert_field_eq(
            &fields,
            "public_workflow_preparation_vortex_layout_write_advisor_counter_columns",
            "true",
        );
        assert_field_eq(
            &fields,
            "public_workflow_preparation_vortex_layout_write_advisor_key_profile",
            "high_cardinality_numeric_text_time_keys",
        );
        assert_field_eq(
            &fields,
            "public_workflow_preparation_vortex_layout_write_advisor_dictionary_profile",
            "source_dictionary_or_derived_dictionary_evidence",
        );
        assert_field_eq(
            &fields,
            "public_workflow_preparation_vortex_layout_write_advisor_expected_read_tradeoff",
            "prefer_metadata_pruning_dictionary_domain_length_and_time_bucket_execution",
        );
        assert_field_eq(
            &fields,
            "public_workflow_preparation_vortex_layout_write_advisor_expected_write_tradeoff",
            "prefer_column_family_fast_zstd_for_payload_text_and_embedded_layout_statistics",
        );
        assert_field_eq(
            &fields,
            "public_workflow_preparation_vortex_writer_compression_field_count",
            "3",
        );
        assert_field_eq(
            &fields,
            "public_workflow_preparation_vortex_writer_compression_field_names",
            "URL,Referer,SearchPhrase",
        );
        assert_field_eq(
            &fields,
            "public_workflow_preparation_vortex_writer_compression_decision_count",
            "3",
        );
        assert_field_eq(
            &fields,
            "public_workflow_preparation_vortex_writer_compression_decisions",
            "field=URL;decision=compress;codec=zstd|field=Referer;decision=compress;codec=zstd|field=SearchPhrase;decision=compress;codec=zstd",
        );
    }

    #[test]
    fn layout_writer_provider_preserves_unknown_streaming_row_count_as_vortex_provider() {
        let source_adapter = LocalInputAdapterSelection::infer_from_path(Path::new("hits.orc"))
            .expect("infer ORC adapter");
        let source = VortexIngestSourceData {
            source_format: source_adapter.source_format,
            source_adapter,
            header: vec!["URL".to_string()],
            column_arrow_dtypes: vec![Some(DataType::Utf8)],
            read_plan: LocalSourceReadPlan::full("test_unknown_streaming_columnar_source"),
            materialized_columns: vec!["URL".to_string()],
            reader_projection_columns: vec!["URL".to_string()],
            projection_pushdown_status: LocalSourceProjectionPushdownStatus::NotRequestedFullRead,
            source_bytes: 1024,
            source_digest: "fnv64:test".to_string(),
            source_fingerprint: SourceFingerprintEvidence::metadata_only(),
            row_count: 0,
            row_count_known: false,
            source_split_row_ranges: vec![(0, 0)],
            source_metadata_scout_millis: 0,
            source_byte_acquisition_millis: 0,
            source_full_body_millis: 0,
            read_millis: 0,
            compatibility_parse_millis: 0,
            source_to_columnar_millis: 0,
            record_batch_count: 0,
            source_stream_batch_size: 262_144,
            source_stream_unit_count_hint: None,
            source_stream_unit_hint_kind: "orc_stream_record_batches_unknown_before_read"
                .to_string(),
            source_stream_policy: "product_columnar_stream_batch_size_262144_rows".to_string(),
            source_dictionary_preservation_status:
                "orc_arrow_reader_typed_batches_preserved_dictionary_contract_not_declared"
                    .to_string(),
            ingest_executor_status: "bounded_capillary_prefetch_active".to_string(),
            ingest_executor_kind: "source_reader_to_vortex_writer_prefetch_pipeline".to_string(),
            ingest_executor_requested_parallelism: 2,
            ingest_executor_applied_parallelism: 2,
            ingest_executor_unit_count_hint: None,
            materialization_layout: "streaming_arrow_record_batch_columnar_source_state",
            parse_normalization: "structured_reader_to_streaming_arrow_record_batches",
            columnar_source_preserved: true,
        };

        assert_eq!(layout_writer_provider_kind(&source), "vortex_array_kernel");
        assert_eq!(
            layout_writer_provider_surface(&source),
            "ArrayRef::from_arrow(RecordBatch);streaming ArrayIterator;VortexSession::write_options().write(ArrayStream)"
        );
    }

    #[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
    #[test]
    fn partitioned_columnar_stream_reader_enforces_global_row_budget() {
        use std::collections::VecDeque;
        use std::sync::Arc;

        use arrow_array::{Int64Array, RecordBatch};
        use arrow_schema::{DataType, Field, Schema, SchemaRef};

        struct TestRecordBatchReader {
            schema: SchemaRef,
            batches: VecDeque<RecordBatch>,
        }

        impl Iterator for TestRecordBatchReader {
            type Item = std::result::Result<RecordBatch, ArrowError>;

            fn next(&mut self) -> Option<Self::Item> {
                self.batches.pop_front().map(Ok)
            }
        }

        impl RecordBatchReader for TestRecordBatchReader {
            fn schema(&self) -> SchemaRef {
                Arc::clone(&self.schema)
            }
        }

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let first_batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![1, 2]))],
        )
        .expect("first batch");
        let second_batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![3, 4]))],
        )
        .expect("second batch");
        let mut readers: VecDeque<Box<dyn RecordBatchReader + Send>> = VecDeque::new();
        readers.push_back(Box::new(TestRecordBatchReader {
            schema: Arc::clone(&schema),
            batches: VecDeque::from([first_batch]),
        }));
        readers.push_back(Box::new(TestRecordBatchReader {
            schema: Arc::clone(&schema),
            batches: VecDeque::from([second_batch]),
        }));
        let mut reader = PartitionedColumnarStreamReader {
            schema,
            readers,
            max_rows: 3,
            row_count: 0,
            source_label: "Parquet",
            path: "target/parts".to_string(),
            failed: false,
        };

        let first = reader
            .next()
            .expect("first partition batch")
            .expect("batch");
        assert_eq!(first.num_rows(), 2);
        let error = reader
            .next()
            .expect("global row budget error")
            .expect_err("second partition exceeds aggregate budget");
        assert!(error.to_string().contains(
            "exceeds the configured local source row budget of 3 across partition files"
        ));
        assert!(reader.next().is_none());
    }

    #[cfg(feature = "vortex-write")]
    fn vortex_ingest_reuse_test_root(name: &str) -> PathBuf {
        let mut path = std::env::temp_dir();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock")
            .as_nanos();
        path.push(format!(
            "shardloom-vortex-ingest-reuse-{name}-{}-{nanos}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&path);
        fs::create_dir_all(&path).expect("create vortex ingest reuse root");
        path
    }

    #[cfg(feature = "vortex-write")]
    fn vortex_ingest_reuse_request(
        source_path: PathBuf,
        target_path: PathBuf,
        allow_overwrite: bool,
    ) -> VortexIngestRequest {
        VortexIngestRequest {
            source_path,
            source_format_override: None,
            target_path,
            allow_overwrite,
            certification_level: shardloom_vortex::VortexIngestCertificationLevel::IngestCertified,
            runtime_profile: SqlLocalSourceRuntimeProfile::Smoke,
            memory_gb: 4,
            max_parallelism: 2,
            source_fingerprint_policy: SourceFingerprintPolicy::DEFAULT_PUBLIC_PREPARE,
            delta: None,
            prepared_source_binding: None,
        }
    }

    #[cfg(feature = "vortex-write")]
    fn prepared_vortex_ingest_report(outcome: VortexIngestOutcome) -> Box<VortexIngestReport> {
        let VortexIngestOutcome::Prepared(report) = outcome;
        report
    }

    #[cfg(feature = "vortex-write")]
    #[test]
    #[allow(clippy::too_many_lines)]
    fn vortex_ingest_public_prepare_writes_only_single_vortex_artifact() {
        let root = vortex_ingest_reuse_test_root("single-artifact");
        let source = root.join("input.csv");
        let target = root.join("prepared.vortex");
        fs::write(&source, "id,label,amount\n1,alpha,10\n2,beta,20\n").expect("write reuse source");

        let first = run_vortex_prepare(vortex_ingest_reuse_request(
            source.clone(),
            target.clone(),
            false,
        ))
        .expect("first vortex_ingest run writes artifact");
        let first_report = prepared_vortex_ingest_report(first);
        let first_fields = field_map(first_report.fields());
        assert_field_eq(&first_fields, "vortex_ingest_performed", "true");
        assert_field_eq(
            &first_fields,
            "vortex_writer_layout_block_target_bytes",
            "1048576",
        );
        assert_field_eq(&first_fields, "prepared_state_reuse_hit", "false");
        assert_field_eq(
            &first_fields,
            "prepared_state_invalidation_reason",
            "not_applicable_single_vortex_artifact",
        );
        assert_field_eq(
            &first_fields,
            "prepared_state_reuse_scope",
            "single_vortex_artifact_no_sidecar",
        );
        assert_field_eq(
            &first_fields,
            "prepared_state_reuse_manifest_path",
            "not_applicable_single_vortex_artifact",
        );
        assert_field_eq(
            &first_fields,
            "prepared_olap_state_status",
            "prepared_olap_state_ready",
        );
        assert_field_eq(
            &first_fields,
            "prepared_olap_state_query_time_contract",
            "single_vortex_artifact_native_runtime_no_query_answer_sidecar",
        );
        assert_field_eq(&first_fields, "prepared_olap_state_blocker_id", "none");
        assert!(
            first_fields
                .get("prepared_olap_state_embedded_layout_statistics_contract")
                .is_some_and(|value| value
                    .contains("metadata_first_when_vortex_layout_statistics_are_available"))
        );
        assert!(
            first_fields
                .get("prepared_state_reuse_manifest_digest")
                .is_some_and(|value| value == "not_applicable_single_vortex_artifact")
        );
        let manifest_path = shardloom_vortex::vortex_prepared_state_reuse_manifest_path(&target)
            .expect("manifest path");
        assert!(
            !manifest_path.exists(),
            "public Vortex prepare must not create artifact-adjacent prepared-state reuse manifest"
        );
        assert!(
            !target
                .parent()
                .expect("target parent")
                .join(".shardloom")
                .join("prepared.vortex.prepared-olap-state.manifest")
                .exists(),
            "public Vortex prepare must not create prepared OLAP sidecar manifests"
        );
        let artifact_digest = first_fields
            .get("vortex_artifact_digest")
            .expect("artifact digest")
            .clone();

        let second = run_vortex_prepare(vortex_ingest_reuse_request(
            source.clone(),
            target.clone(),
            true,
        ))
        .expect("second vortex_ingest run rewrites artifact when overwrite is explicit");
        let second_report = prepared_vortex_ingest_report(second);
        let second_fields = field_map(second_report.fields());
        assert_field_eq(&second_fields, "vortex_ingest_performed", "true");
        assert_field_eq(
            &second_fields,
            "vortex_ingest_status",
            "prepared_state_created",
        );
        assert_field_eq(&second_fields, "prepared_state_created", "true");
        assert_field_eq(&second_fields, "prepared_state_reused", "false");
        assert_field_eq(&second_fields, "prepared_state_reuse_hit", "false");
        assert_field_eq(
            &second_fields,
            "prepared_state_reuse_scope",
            "single_vortex_artifact_no_sidecar",
        );
        assert_field_eq(
            &second_fields,
            "prepared_olap_state_status",
            "prepared_olap_state_ready",
        );
        assert_field_eq(
            &second_fields,
            "prepared_olap_state_query_time_contract",
            "single_vortex_artifact_native_runtime_no_query_answer_sidecar",
        );
        assert_field_eq(&second_fields, "prepared_olap_state_blocker_id", "none");
        assert_field_eq(&second_fields, "fallback_attempted", "false");
        assert_field_eq(&second_fields, "external_engine_invoked", "false");
        assert_eq!(
            second_fields.get("vortex_artifact_digest"),
            Some(&artifact_digest)
        );
        assert!(
            !manifest_path.exists(),
            "public Vortex overwrite must still leave no prepared-state reuse manifest"
        );

        fs::remove_dir_all(root).expect("remove reuse hit root");
    }

    #[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
    #[test]
    fn vortex_ingest_text_sources_use_typed_record_batch_stream() {
        let root = vortex_ingest_reuse_test_root("text-record-batch-stream");
        let cases = [
            (
                "csv",
                "input.csv",
                "id,label,amount\n1,alpha,10\n2,beta,20\n",
                "inferred_csv_record_batch_stream_batch_size_65536_rows",
            ),
            (
                "jsonl",
                "input.jsonl",
                "{\"id\":1,\"label\":\"alpha\",\"amount\":10}\n{\"id\":2,\"label\":\"beta\",\"amount\":20}\n",
                "inferred_jsonl_record_batch_stream_batch_size_65536_rows",
            ),
        ];

        for (label, file_name, body, stream_policy) in cases {
            let source = root.join(file_name);
            let target = root.join(format!("{label}.vortex"));
            fs::write(&source, body).expect("write text source");

            let outcome = run_vortex_prepare(vortex_ingest_reuse_request(source, target, false))
                .expect("text source writes Vortex artifact through Universal Ingest");
            let report = prepared_vortex_ingest_report(outcome);
            let fields = field_map(report.fields());

            assert_field_eq(&fields, "vortex_ingest_performed", "true");
            assert_field_eq(
                &fields,
                "source_state_materialization_layout",
                "inferred_text_to_streaming_arrow_record_batch_source_state",
            );
            assert_field_eq(
                &fields,
                "source_state_parse_normalization",
                "inferred_text_to_record_batch_stream",
            );
            assert_field_eq(&fields, "source_state_columnar_preserved", "true");
            assert_field_eq(&fields, "source_state_record_batch_count", "1");
            assert_field_eq(&fields, "source_state_stream_policy", stream_policy);
            assert_field_eq(
                &fields,
                "source_state_stream_unit_hint_kind",
                "inferred_text_record_batch_stream",
            );
            assert_field_eq(
                &fields,
                "source_state_dictionary_preservation_status",
                "inferred_text_typed_builders_preserve_inferred_scalar_types",
            );
            assert_field_eq(
                &fields,
                "source_state_ingest_executor_status",
                "bounded_shared_runtime_source",
            );
            assert_field_eq(
                &fields,
                "source_state_ingest_executor_kind",
                "ordered_source_tasks_on_shared_native_ingest_runtime",
            );
            assert_field_eq(
                &fields,
                "source_state_ingest_executor_requested_parallelism",
                "2",
            );
            assert_field_eq(
                &fields,
                "source_state_ingest_executor_applied_parallelism",
                "1",
            );
            assert_field_eq(&fields, "source_state_ingest_executor_unit_count_hint", "1");
            assert_ingest_stream_lane_recipe(&fields, 2);
            assert_field_eq(
                &fields,
                "vortex_array_build_provider_surface",
                "ArrayRef::from_arrow(RecordBatch);ordered_morsel_vortex_array_prefetch;streaming ArrayIterator",
            );
            assert_field_eq(
                &fields,
                "vortex_array_build_strategy",
                "ordered_morsel_vortex_array_prefetch_threadlocal_conversion_merge",
            );
            assert_field_eq(&fields, "vortex_array_build_prefetch_window", "2");
            assert_field_eq(
                &fields,
                "vortex_array_build_input_layout",
                "streaming_arrow_record_batch_columnar_source_state",
            );
            assert_field_eq(&fields, "vortex_array_build_record_batch_count", "1");
            assert_field_eq(
                &fields,
                "vortex_writer_coalescing_policy_status",
                "native_within_source_batch_only;cross_batch_coalescing_disabled",
            );
            assert_field_eq(
                &fields,
                "universal_ingest_timing_split_schema_version",
                "shardloom.universal_ingest_timing_split.v1",
            );
            assert_field_eq(
                &fields,
                "universal_ingest_timing_split_status",
                "streaming_source_pull_decode_derive_and_arrow_to_vortex_convert_timing_recorded",
            );
            assert_field_eq(
                &fields,
                "universal_ingest_stream_timing_overlap_policy",
                "capillary_prefetch_may_overlap_decode_derive_with_encode_write_wall_time",
            );
            for field in [
                "universal_ingest_source_read_millis",
                "universal_ingest_decode_derive_millis",
                "universal_ingest_decode_millis",
                "universal_ingest_derived_metadata_build_millis",
                "universal_ingest_arrow_to_vortex_convert_millis",
                "universal_ingest_encode_write_wall_millis",
                "universal_ingest_footer_register_millis",
                "universal_ingest_reopen_verify_millis",
                "universal_ingest_prepare_source_hydration_millis",
                "universal_ingest_prepare_nested_source_batch_production_millis",
                "universal_ingest_prepare_route_bookkeeping_millis",
                "universal_ingest_prepare_scheduler_wait_millis",
                "universal_ingest_prepare_writer_backpressure_or_timer_overlap_millis",
                "universal_ingest_prepare_evidence_emit_millis",
                "vortex_writer_runtime_requested_parallelism",
                "vortex_writer_runtime_applied_parallelism",
                "vortex_writer_runtime_background_workers",
                "vortex_compression_millis",
                "vortex_encode_write_millis",
                "vortex_final_commit_millis",
            ] {
                fields
                    .get(field)
                    .unwrap_or_else(|| panic!("{field} should be emitted"))
                    .parse::<u128>()
                    .unwrap_or_else(|_| panic!("{field} should be numeric"));
            }
            assert_field_eq(
                &fields,
                "vortex_array_build_manual_scalar_copy_avoided",
                "true",
            );
            assert_field_eq(
                &fields,
                "vortex_preparation_spine_decode_boundary_status",
                "no_scalar_row_decode_for_streamed_batches",
            );
            assert_field_eq(&fields, "fallback_attempted", "false");
            assert_field_eq(&fields, "external_engine_invoked", "false");
        }

        fs::remove_dir_all(root).expect("remove text stream root");
    }

    #[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
    #[test]
    fn vortex_ingest_max_parallelism_propagates_to_public_prepare_evidence() {
        let root = vortex_ingest_reuse_test_root("max-parallelism-propagation");
        let source = root.join("input.csv");
        let target = root.join("prepared.vortex");
        fs::write(&source, "id,label\n1,alpha\n2,beta\n").expect("write csv source");

        let mut request = vortex_ingest_reuse_request(source, target, false);
        request.memory_gb = 6;
        request.max_parallelism = 4;

        let outcome = run_vortex_prepare(request)
            .expect("csv source writes Vortex artifact with explicit max parallelism");
        let report = prepared_vortex_ingest_report(outcome);
        let raw_fields = report.fields();
        let fields = field_map(raw_fields.clone());
        let public_fields = field_map(public_workflow_preparation_fields(&raw_fields));

        assert_field_eq(&fields, "vortex_ingest_requested_memory_gb", "6");
        assert_field_eq(&fields, "vortex_ingest_requested_max_parallelism", "4");
        assert_field_eq(
            &fields,
            "vortex_layout_write_advisor_writer_parallelism_budget",
            "4",
        );
        assert_field_eq(
            &fields,
            "source_state_ingest_executor_status",
            "bounded_shared_runtime_source",
        );
        assert_field_eq(
            &fields,
            "source_state_ingest_executor_requested_parallelism",
            "4",
        );
        assert_field_eq(
            &fields,
            "source_state_ingest_executor_applied_parallelism",
            "1",
        );
        assert_field_eq(&fields, "vortex_array_build_prefetch_window", "4");
        assert_ingest_stream_lane_recipe(&fields, 4);
        assert_field_eq(
            &fields,
            "vortex_array_build_strategy",
            "ordered_morsel_vortex_array_prefetch_threadlocal_conversion_merge",
        );
        assert_field_eq(&fields, "vortex_writer_physical_design_status", "applied");
        assert_field_eq(
            &fields,
            "vortex_writer_physical_design_provider_decision",
            "use_vortex_native_provider",
        );
        assert_field_eq(
            &fields,
            "vortex_writer_physical_design_source_executor_requested_parallelism",
            "4",
        );
        assert_field_eq(
            &fields,
            "vortex_writer_physical_design_source_executor_applied_parallelism",
            "1",
        );
        assert_field_eq(
            &fields,
            "vortex_writer_physical_design_array_build_prefetch_window",
            "4",
        );
        assert_field_eq(
            &fields,
            "vortex_writer_physical_design_array_build_worker_count",
            "4",
        );
        assert_field_eq(
            &fields,
            "vortex_writer_physical_design_writer_backpressure_policy",
            "reserved_inflight_window_bounds_queued_active_and_ordered_results",
        );
        assert_field_eq(
            &fields,
            "vortex_writer_physical_design_no_fallback_policy",
            "native_vortex_writer_provider_only_no_spark_datafusion_duckdb_polars_velox_fallback",
        );
        assert_field_eq(
            &fields,
            "vortex_segment_metadata_status",
            "admitted_footer_segment_metadata",
        );
        assert_field_eq(&fields, "vortex_segment_metadata_row_count", "2");
        assert_field_eq(&fields, "vortex_segment_metadata_row_count_proven", "true");
        assert_field_eq(
            &fields,
            "vortex_segment_metadata_metadata_count_admission",
            "admitted_metadata_count_from_prepared_footer_or_writer_summary",
        );
        assert_field_eq(
            &fields,
            "vortex_segment_metadata_predicate_pruning_admission",
            "admitted_conservative_metadata_pruning_for_predicate_ranges_and_domain_absence",
        );
        assert_field_eq(
            &fields,
            "vortex_segment_metadata_no_false_negative_policy",
            "unknown_or_inconclusive_segments_are_read_no_false_negative_pruning",
        );
        assert_field_eq(
            &fields,
            "vortex_segment_metadata_query_answer_sidecar_status",
            "disabled_rejected_for_public_default_runtime",
        );
        assert_field_eq(&fields, "vortex_capillary_preparation_max_parallelism", "4");
        assert_field_eq(
            &fields,
            "vortex_capillary_preparation_memory_budget_bytes",
            &(6_u64 * 1024 * 1024 * 1024).to_string(),
        );
        assert_field_eq(
            &public_fields,
            "public_workflow_preparation_vortex_ingest_requested_memory_gb",
            "6",
        );
        assert_field_eq(
            &public_fields,
            "public_workflow_preparation_vortex_layout_write_advisor_writer_parallelism_budget",
            "4",
        );
        assert!(
            public_fields
                .contains_key("public_workflow_preparation_universal_ingest_timing_split_status"),
            "public workflow preparation projection should expose ingest timing split status"
        );
        assert!(
            public_fields.contains_key(
                "public_workflow_preparation_universal_ingest_prepare_known_component_millis"
            ),
            "public workflow preparation projection should expose known prepare attribution"
        );
        assert!(
            public_fields.contains_key(
                "public_workflow_preparation_universal_ingest_prepare_unattributed_millis"
            ),
            "public workflow preparation projection should expose residual prepare attribution"
        );
        assert!(
            public_fields.contains_key(
                "public_workflow_preparation_universal_ingest_prepare_attribution_status"
            ),
            "public workflow preparation projection should expose prepare attribution status"
        );
        assert!(
            public_fields.contains_key(
                "public_workflow_preparation_universal_ingest_prepare_source_hydration_millis"
            ),
            "public workflow preparation projection should expose source hydration attribution"
        );
        assert!(
            public_fields.contains_key(
                "public_workflow_preparation_universal_ingest_prepare_nested_source_batch_production_millis"
            ),
            "public workflow preparation projection should expose nested source batch production timing"
        );
        assert!(
            public_fields.contains_key(
                "public_workflow_preparation_universal_ingest_prepare_writer_backpressure_or_timer_overlap_millis"
            ),
            "public workflow preparation projection should expose residual writer/backpressure attribution"
        );
        let prepare_total_millis = fields
            .get("prepare_once_millis")
            .expect("prepare total field")
            .parse::<u128>()
            .expect("prepare total millis");
        let known_prepare_millis = fields
            .get("universal_ingest_prepare_known_component_millis")
            .expect("known prepare attribution")
            .parse::<u128>()
            .expect("known prepare attribution millis");
        let unattributed_prepare_millis = fields
            .get("universal_ingest_prepare_unattributed_millis")
            .expect("unattributed prepare attribution")
            .parse::<u128>()
            .expect("unattributed prepare attribution millis");
        assert!(known_prepare_millis <= prepare_total_millis);
        assert!(unattributed_prepare_millis <= prepare_total_millis);
        assert_eq!(
            known_prepare_millis.saturating_add(unattributed_prepare_millis),
            prepare_total_millis
        );
        assert!(
            public_fields
                .contains_key("public_workflow_preparation_vortex_writer_stats_concurrency"),
            "public workflow preparation projection should expose writer stats concurrency"
        );
        assert!(
            public_fields.contains_key(
                "public_workflow_preparation_vortex_writer_compression_decision_count"
            ),
            "public workflow preparation projection should expose writer compression decisions"
        );
        assert!(
            public_fields
                .contains_key("public_workflow_preparation_vortex_writer_compression_decisions"),
            "public workflow preparation projection should expose writer compression decision details"
        );
        assert!(
            public_fields
                .contains_key("public_workflow_preparation_vortex_writer_profile_selection_reason"),
            "public workflow preparation projection should expose writer profile reason"
        );
        assert!(
            public_fields
                .contains_key("public_workflow_preparation_vortex_writer_profile_regression_guard"),
            "public workflow preparation projection should expose writer profile regression guard"
        );
        assert_field_eq(
            &public_fields,
            "public_workflow_preparation_vortex_array_build_prefetch_window",
            "4",
        );
        assert_field_eq(
            &public_fields,
            "public_workflow_preparation_vortex_array_build_strategy",
            "ordered_morsel_vortex_array_prefetch_threadlocal_conversion_merge",
        );
        assert_field_eq(
            &public_fields,
            "public_workflow_preparation_vortex_writer_physical_design_status",
            "applied",
        );
        assert_field_eq(
            &public_fields,
            "public_workflow_preparation_vortex_writer_physical_design_array_build_prefetch_window",
            "4",
        );
        assert_field_eq(
            &public_fields,
            "public_workflow_preparation_vortex_writer_physical_design_array_build_worker_count",
            "4",
        );
        assert_field_eq(
            &public_fields,
            "public_workflow_preparation_vortex_writer_physical_design_no_fallback_policy",
            "native_vortex_writer_provider_only_no_spark_datafusion_duckdb_polars_velox_fallback",
        );
        assert_field_eq(
            &public_fields,
            "public_workflow_preparation_vortex_segment_metadata_status",
            "admitted_footer_segment_metadata",
        );
        assert_field_eq(
            &public_fields,
            "public_workflow_preparation_vortex_segment_metadata_metadata_count_admission",
            "admitted_metadata_count_from_prepared_footer_or_writer_summary",
        );
        assert_field_eq(
            &public_fields,
            "public_workflow_preparation_vortex_segment_metadata_predicate_pruning_admission",
            "admitted_conservative_metadata_pruning_for_predicate_ranges_and_domain_absence",
        );
        assert_field_eq(
            &public_fields,
            "public_workflow_preparation_vortex_segment_metadata_no_false_negative_policy",
            "unknown_or_inconclusive_segments_are_read_no_false_negative_pruning",
        );
        assert_field_eq(&fields, "fallback_attempted", "false");
        assert_field_eq(&fields, "external_engine_invoked", "false");

        fs::remove_dir_all(root).expect("remove max parallelism root");
    }

    #[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
    #[test]
    fn vortex_ingest_parquet_public_prepare_uses_row_group_capillary_executor() {
        use arrow_array::{Int64Array, RecordBatch, StringArray};
        use arrow_schema::{DataType, Field, Schema};
        use parquet::arrow::ArrowWriter;
        use parquet::file::properties::WriterProperties;
        use std::sync::Arc;

        let root = vortex_ingest_reuse_test_root("parquet-row-group-public-prepare");
        let source = root.join("input.parquet");
        let target = root.join("prepared.vortex");
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("URL", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
                Arc::new(StringArray::from(vec![
                    Some("https://example.com/a"),
                    Some("https://example.com/b"),
                    Some("https://example.net/c"),
                    None,
                ])),
            ],
        )
        .expect("parquet batch");
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(1))
            .build();
        let file = fs::File::create(&source).expect("create parquet source");
        let mut writer =
            ArrowWriter::try_new(file, Arc::clone(&schema), Some(props)).expect("parquet writer");
        writer.write(&batch).expect("write parquet batch");
        writer.close().expect("close parquet writer");

        let mut request = vortex_ingest_reuse_request(source, target, false);
        request.max_parallelism = 2;
        let outcome = run_vortex_prepare(request)
            .expect("parquet source writes Vortex artifact through public prepare");
        let report = prepared_vortex_ingest_report(outcome);
        let fields = field_map(report.fields());

        assert_field_eq(&fields, "vortex_ingest_requested_max_parallelism", "2");
        assert_field_eq(
            &fields,
            "vortex_layout_write_advisor_writer_parallelism_budget",
            "2",
        );
        assert_field_eq(
            &fields,
            "source_state_stream_unit_hint_kind",
            "parquet_adaptive_row_group_task_count",
        );
        assert_field_eq(
            &fields,
            "source_state_ingest_executor_status",
            "bounded_shared_runtime_source",
        );
        assert_field_eq(
            &fields,
            "source_state_ingest_executor_kind",
            "parquet_ordered_source_tasks_on_shared_native_ingest_runtime",
        );
        assert_field_eq(
            &fields,
            "source_state_ingest_executor_requested_parallelism",
            "2",
        );
        assert_field_eq(
            &fields,
            "source_state_ingest_executor_applied_parallelism",
            "1",
        );
        assert_field_eq(
            &fields,
            "vortex_array_build_strategy",
            "ordered_morsel_vortex_array_prefetch_threadlocal_conversion_merge",
        );
        assert_ingest_stream_lane_recipe(&fields, 2);
        assert_field_eq(&fields, "vortex_writer_stats_concurrency", "1");
        assert_field_eq(&fields, "fallback_attempted", "false");
        assert_field_eq(&fields, "external_engine_invoked", "false");

        fs::remove_dir_all(root).expect("remove parquet public prepare root");
    }

    #[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
    #[test]
    fn vortex_ingest_inferred_text_sources_stream_without_scalar_row_bridge() {
        let root = vortex_ingest_reuse_test_root("inferred-text-stream");
        let cases = [
            (
                "csv",
                "input.csv",
                "id,label,amount\n1,alpha,10\n2,beta,20\n",
                "inferred_csv_record_batch_stream_batch_size_65536_rows",
            ),
            (
                "jsonl",
                "input.jsonl",
                "{\"id\":1,\"label\":\"alpha\",\"amount\":10}\n{\"id\":2,\"label\":99,\"amount\":20.5}\n",
                "inferred_jsonl_record_batch_stream_batch_size_65536_rows",
            ),
        ];

        for (label, file_name, body, stream_policy) in cases {
            let source = root.join(file_name);
            let target = root.join(format!("{label}.vortex"));
            fs::write(&source, body).expect("write inferred text source");

            let outcome = run_vortex_prepare(vortex_ingest_reuse_request(source, target, false))
                .expect("inferred text source writes Vortex artifact through direct stream");
            let report = prepared_vortex_ingest_report(outcome);
            let fields = field_map(report.fields());

            assert_eq!(report.vortex_report.row_count, 2);
            assert_field_eq(&fields, "vortex_ingest_performed", "true");
            assert_field_eq(
                &fields,
                "source_state_materialization_layout",
                "inferred_text_to_streaming_arrow_record_batch_source_state",
            );
            assert_field_eq(
                &fields,
                "source_state_parse_normalization",
                "inferred_text_to_record_batch_stream",
            );
            assert_field_eq(&fields, "source_state_columnar_preserved", "true");
            assert_field_eq(&fields, "source_state_stream_policy", stream_policy);
            assert_field_eq(
                &fields,
                "source_state_dictionary_preservation_status",
                "inferred_text_typed_builders_preserve_inferred_scalar_types",
            );
            assert_field_eq(&fields, "source_state_record_batch_count", "1");
            assert_field_eq(&fields, "compatibility_parse_millis", "0");
            assert_field_eq(
                &fields,
                "source_read_buffer_carry_status",
                "metadata_only_source_identity_columnar_reader_not_preopened",
            );
            assert_field_eq(
                &fields,
                "source_fingerprint_kind",
                "local_file_metadata_size_mtime",
            );
            assert_field_eq(&fields, "source_fingerprint_policy", "metadata_only");
            assert_field_eq(&fields, "source_content_fingerprint_performed", "false");
            assert_ingest_stream_lane_recipe(&fields, 2);
            assert_field_eq(
                &fields,
                "vortex_array_build_provider_surface",
                "ArrayRef::from_arrow(RecordBatch);ordered_morsel_vortex_array_prefetch;streaming ArrayIterator",
            );
            assert_field_eq(&fields, "vortex_array_build_prefetch_window", "2");
            assert_field_eq(
                &fields,
                "vortex_array_build_strategy",
                "ordered_morsel_vortex_array_prefetch_threadlocal_conversion_merge",
            );
            assert_field_eq(
                &fields,
                "vortex_preparation_spine_decode_boundary_status",
                "no_scalar_row_decode_for_streamed_batches",
            );
            assert_field_eq(&fields, "fallback_attempted", "false");
            assert_field_eq(&fields, "external_engine_invoked", "false");
        }

        fs::remove_dir_all(root).expect("remove inferred text stream root");
    }

    #[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
    fn direct_jsonl_test_reader(
        body: &str,
        columns: &[(&str, LogicalDType)],
        batch_size: usize,
        max_input_rows: Option<usize>,
    ) -> (PathBuf, SchemaDeclaredTextRecordBatchReader) {
        let root = vortex_ingest_reuse_test_root("direct-jsonl");
        let path = root.join("input.jsonl");
        fs::write(&path, body).unwrap();
        let header = columns
            .iter()
            .map(|(name, _)| (*name).to_owned())
            .collect::<Vec<_>>();
        let schema = Arc::new(Schema::new(
            columns
                .iter()
                .map(|(name, dtype)| {
                    Field::new(
                        *name,
                        schema_declared_text_arrow_dtype(dtype, name, "test").unwrap(),
                        true,
                    )
                })
                .collect::<Vec<_>>(),
        ));
        let reader = SchemaDeclaredTextRecordBatchReader::new(
            LocalSourceFormat::JsonLines,
            schema,
            header,
            columns
                .iter()
                .map(|(_, dtype)| Some(dtype.clone()))
                .collect(),
            BufReader::new(fs::File::open(path).unwrap()),
            TextRecordBatchReaderConfig {
                max_input_rows,
                batch_size,
            },
            "direct JSONL test".into(),
        );
        (root, reader)
    }

    #[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
    #[test]
    fn direct_jsonl_builder_preserves_duplicate_coercion_missing_and_batch_values() {
        let columns = [
            ("id", LogicalDType::Int64),
            ("unsigned", LogicalDType::UInt64),
            ("ratio", LogicalDType::Float64),
            ("truth", LogicalDType::Boolean),
            ("label", LogicalDType::Utf8),
            ("bin", LogicalDType::Binary),
            ("date", LogicalDType::Date32),
            ("time", LogicalDType::TimestampMicros),
            ("nested", LogicalDType::Utf8),
        ];
        let body = concat!(
            "\n\u{feff}",
            r#"{"id":"discard","id":9223372036854775807,"unsigned":12,"ratio":3,"truth":true,"label":"é\n\uD834\uDD1E","bin":"ab","date":"1970-01-02","time":"1970-01-01T00:00:01Z","nested":{"b":[true,null],"a":1},"ignored":[{"a":"escaped\""}]}"#,
            "\n\n",
            r#"{"ignored":null}"#,
            "\n",
            r#"{"label":false,"id":-9223372036854775808,}"#,
            "\n"
        );
        let (root, mut reader) = direct_jsonl_test_reader(body, &columns, 2, None);
        let expected = [
            vec![
                ScalarValue::Int64(i64::MAX),
                ScalarValue::UInt64(12),
                ScalarValue::Float64(3.0),
                ScalarValue::Boolean(true),
                ScalarValue::Utf8("é\n𝄞".into()),
                ScalarValue::Binary(b"ab".to_vec()),
                ScalarValue::Date32(1),
                ScalarValue::TimestampMicros(1_000_000),
                ScalarValue::Utf8(r#"{"a":1,"b":[true,null]}"#.into()),
            ],
            vec![ScalarValue::Null; columns.len()],
            vec![
                ScalarValue::Int64(i64::MIN),
                ScalarValue::Null,
                ScalarValue::Null,
                ScalarValue::Null,
                ScalarValue::Utf8("false".into()),
                ScalarValue::Null,
                ScalarValue::Null,
                ScalarValue::Null,
                ScalarValue::Null,
            ],
        ];
        for rows in expected.chunks(2) {
            let named_rows = rows
                .iter()
                .map(|row| {
                    reader
                        .header
                        .iter()
                        .cloned()
                        .zip(row.iter().cloned())
                        .collect()
                })
                .collect::<Vec<_>>();
            let expected_batch =
                shardloom_vortex::universal_format_io::flat_rows_to_record_batch_with_schema(
                    Arc::clone(&reader.schema),
                    &reader.header,
                    &named_rows,
                    "expected",
                )
                .unwrap();
            assert_eq!(reader.next_record_batch().unwrap().unwrap(), expected_batch);
        }
        assert!(reader.next_record_batch().unwrap().is_none());
        assert_eq!(reader.next_row_number, 3);
        fs::remove_dir_all(root).unwrap();
    }

    #[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
    #[test]
    fn direct_jsonl_builder_rejects_invalid_rows_before_emitting_a_batch() {
        let cases = [
            (r#"{"v":-1}"#, LogicalDType::UInt64),
            (r#"{"v":9223372036854775808}"#, LogicalDType::UInt64),
            (r#"{"v":1.5}"#, LogicalDType::Int64),
            (r#"{"v":1}"#, LogicalDType::Boolean),
            (r#"{"v":true}"#, LogicalDType::Float64),
            (r#"{"v":1}"#, LogicalDType::Binary),
            (r#"{"v":"not-date"}"#, LogicalDType::Date32),
            (r#"{"v":"not-time"}"#, LogicalDType::TimestampMicros),
            (r#"{"v":"\x","v":"okay"}"#, LogicalDType::Utf8),
            (r#"{"v":1} trailing"#, LogicalDType::Int64),
            (r#"{"ignored":[1,}"#, LogicalDType::Int64),
            (r#"{"ignored":"\x"}"#, LogicalDType::Int64),
            ("{}", LogicalDType::Int64),
        ];
        for (invalid, dtype) in cases {
            let (root, mut reader) = direct_jsonl_test_reader(
                &format!("{{\"v\":null}}\n{invalid}\n"),
                &[("v", dtype)],
                3,
                None,
            );
            assert!(reader.next_record_batch().is_err(), "{invalid}");
            fs::remove_dir_all(root).unwrap();
        }
        let (root, mut reader) = direct_jsonl_test_reader(
            "{\"v\":1}\n{\"v\":2}\n{\"v\":3}\n",
            &[("v", LogicalDType::Int64)],
            3,
            Some(2),
        );
        assert!(reader.next_record_batch().is_err());
        fs::remove_dir_all(root).unwrap();
    }

    #[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
    #[test]
    fn direct_jsonl_builder_handles_empty_and_exact_batch_boundaries() {
        for row_count in 0..=5 {
            let (root, mut reader) = direct_jsonl_test_reader(
                &"{\"v\":null}\n".repeat(row_count),
                &[("v", LogicalDType::Int64)],
                2,
                None,
            );
            let mut observed = Vec::new();
            while let Some(batch) = reader.next_record_batch().unwrap() {
                assert_eq!(batch.column(0).null_count(), batch.num_rows());
                observed.push(batch.num_rows());
            }
            assert_eq!(observed.iter().sum::<usize>(), row_count);
            assert_eq!(observed.len(), row_count.div_ceil(2));
            assert!(reader.next_record_batch().unwrap().is_none());
            fs::remove_dir_all(root).unwrap();
        }
    }

    #[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
    #[test]
    fn vortex_ingest_schema_declared_text_sources_stream_without_scalar_row_bridge() {
        let root = vortex_ingest_reuse_test_root("schema-declared-text-stream");
        let cases = [
            (
                "csv",
                "input.csv",
                "id,label,amount\n1,alpha,10\n2,beta,20\n",
                "schema_declared_csv_record_batch_stream_batch_size_65536_rows",
            ),
            (
                "jsonl",
                "input.jsonl",
                "{\"id\":1,\"label\":\"alpha\",\"amount\":10}\n{\"id\":2,\"label\":\"beta\",\"amount\":20}\n",
                "schema_declared_jsonl_record_batch_stream_batch_size_65536_rows",
            ),
        ];
        let hints = vec![
            ("id".to_string(), LogicalDType::Int64),
            ("label".to_string(), LogicalDType::Utf8),
            ("amount".to_string(), LogicalDType::Int64),
        ];

        for (label, file_name, body, stream_policy) in cases {
            let source = root.join(file_name);
            let target = root.join(format!("{label}.vortex"));
            fs::write(&source, body).expect("write text source");

            let outcome = run_vortex_ingest_prepare_once_with_schema(
                vortex_ingest_reuse_request(source, target, false),
                &hints,
            )
            .expect("schema-declared text source writes Vortex artifact through direct stream");
            let report = prepared_vortex_ingest_report(outcome);
            let fields = field_map(report.fields());

            assert_field_eq(&fields, "vortex_ingest_performed", "true");
            assert_field_eq(
                &fields,
                "source_state_materialization_layout",
                "schema_declared_text_to_streaming_arrow_record_batch_source_state",
            );
            assert_field_eq(
                &fields,
                "source_state_parse_normalization",
                "schema_declared_text_to_record_batch_stream",
            );
            assert_field_eq(&fields, "source_state_columnar_preserved", "true");
            assert_field_eq(&fields, "source_state_stream_policy", stream_policy);
            assert_field_eq(
                &fields,
                "source_read_buffer_carry_status",
                "metadata_only_source_identity_columnar_reader_not_preopened",
            );
            assert_field_eq(
                &fields,
                "source_fingerprint_kind",
                "local_file_metadata_size_mtime",
            );
            assert_field_eq(&fields, "source_fingerprint_policy", "metadata_only");
            assert_field_eq(&fields, "source_content_fingerprint_performed", "false");
            assert_field_eq(
                &fields,
                "source_state_dictionary_preservation_status",
                "schema_declared_text_typed_builders_preserve_declared_scalar_types",
            );
            assert_field_eq(&fields, "source_state_record_batch_count", "1");
            assert_field_eq(&fields, "compatibility_parse_millis", "0");
            assert_ingest_stream_lane_recipe(&fields, 2);
            assert_field_eq(
                &fields,
                "vortex_array_build_provider_surface",
                "ArrayRef::from_arrow(RecordBatch);ordered_morsel_vortex_array_prefetch;streaming ArrayIterator",
            );
            assert_field_eq(&fields, "vortex_array_build_prefetch_window", "2");
            assert_field_eq(
                &fields,
                "vortex_array_build_strategy",
                "ordered_morsel_vortex_array_prefetch_threadlocal_conversion_merge",
            );
            assert_field_eq(
                &fields,
                "vortex_preparation_spine_decode_boundary_status",
                "no_scalar_row_decode_for_streamed_batches",
            );
            assert_field_eq(&fields, "fallback_attempted", "false");
            assert_field_eq(&fields, "external_engine_invoked", "false");
        }

        fs::remove_dir_all(root).expect("remove schema declared text stream root");
    }

    #[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
    #[test]
    fn whole_json_typed_prepare_preserves_derived_output_and_failed_overwrite() {
        let root = vortex_ingest_reuse_test_root("whole-json-typed");
        let source = root.join("input.json");
        let target = root.join("prepared.vortex");
        fs::write(&source, r#"[{"id":1,"URL":"https://example.com/a","n":null},{"URL":"https://example.org/b","id":2,"n":null}]"#).unwrap();
        let request = vortex_ingest_reuse_request(source.clone(), target.clone(), false);
        let report = prepared_vortex_ingest_report(run_vortex_prepare(request).unwrap());
        assert_eq!(report.vortex_report.row_count, 2);
        let fields = field_map(report.fields());
        assert_field_eq(
            &fields,
            "source_state_projection_pushdown_status",
            "not_requested_full_read",
        );
        assert_field_eq(
            &fields,
            "source_read_buffer_carry_status",
            "read_once_buffer_carried_to_text_parser",
        );
        assert_field_eq(
            &fields,
            "source_read_mmap_eligibility_status",
            "not_used_owned_text_buffer_default",
        );
        assert_field_eq(&fields, "source_content_fingerprint_performed", "true");
        assert_eq!(
            report.source.materialization_layout,
            "whole_json_typed_columns_with_batched_writer"
        );
        assert!(
            report
                .vortex_report
                .column_family_summary()
                .contains("__shardloom_derived_url_domain_URL:")
        );
        let original = fs::read(&target).unwrap();
        for invalid in [
            r#"[{"id":1},{"id":2}] garbage"#,
            r#"[{"id":1},{"id":"changed"}]"#,
        ] {
            fs::write(&source, invalid).unwrap();
            assert!(
                run_vortex_prepare(vortex_ingest_reuse_request(
                    source.clone(),
                    target.clone(),
                    true
                ))
                .is_err()
            );
            assert_eq!(fs::read(&target).unwrap(), original);
            assert_eq!(fs::read_dir(&root).unwrap().count(), 2);
        }
        fs::remove_dir_all(root).unwrap();
    }

    #[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
    #[test]
    fn vortex_ingest_streaming_text_routes_preserve_hidden_derived_columns() {
        let root = vortex_ingest_reuse_test_root("streaming-derived-columns");
        let inferred_source = root.join("inferred.csv");
        let inferred_target = root.join("inferred.vortex");
        fs::write(
            &inferred_source,
            "id,URL,SearchPhrase\n1,https://www.example.com/a,alpha\n2,https://[2001:db8::1]:8443/b,beta\n",
        )
        .expect("write inferred csv source");

        let inferred = run_vortex_prepare(vortex_ingest_reuse_request(
            inferred_source,
            inferred_target,
            false,
        ))
        .expect("inferred text stream writes Vortex artifact");
        let inferred_report = prepared_vortex_ingest_report(inferred);
        let inferred_fields = field_map(inferred_report.fields());
        assert!(
            inferred_report
                .vortex_report
                .column_family_summary()
                .contains("__shardloom_derived_utf8_len_URL:"),
            "{}",
            inferred_report.vortex_report.column_family_summary()
        );
        assert!(
            inferred_report
                .vortex_report
                .column_family_summary()
                .contains("__shardloom_derived_url_domain_URL:"),
            "{}",
            inferred_report.vortex_report.column_family_summary()
        );
        assert_field_contains(
            &inferred_fields,
            "source_state_dictionary_preservation_status",
            "__shardloom_derived_url_domain_URL",
        );

        let schema_source = root.join("schema.csv");
        let schema_target = root.join("schema.vortex");
        fs::write(
            &schema_source,
            "id,URL,SearchPhrase\n1,https://www.example.net/a,alpha\n2,https://www.example.org/b,beta\n",
        )
        .expect("write schema csv source");
        let full_hints = vec![
            ("id".to_string(), LogicalDType::Int64),
            ("URL".to_string(), LogicalDType::Utf8),
            ("SearchPhrase".to_string(), LogicalDType::Utf8),
        ];
        let schema_declared = run_vortex_ingest_prepare_once_with_schema(
            vortex_ingest_reuse_request(schema_source, schema_target, false),
            &full_hints,
        )
        .expect("schema-declared text stream writes Vortex artifact");
        let schema_report = prepared_vortex_ingest_report(schema_declared);
        let schema_fields = field_map(schema_report.fields());
        assert!(
            schema_report
                .vortex_report
                .column_family_summary()
                .contains("__shardloom_derived_url_domain_URL:"),
            "{}",
            schema_report.vortex_report.column_family_summary()
        );
        assert_field_contains(
            &schema_fields,
            "source_state_dictionary_preservation_status",
            "__shardloom_derived_utf8_len_SearchPhrase",
        );

        fs::remove_dir_all(root).expect("remove streaming derived columns root");
    }

    #[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
    #[test]
    fn vortex_ingest_partial_jsonl_schema_hints_preserve_undeclared_columns() {
        let root = vortex_ingest_reuse_test_root("partial-jsonl-schema-hints");
        let source = root.join("input.jsonl");
        let target = root.join("prepared.vortex");
        fs::write(
            &source,
            "{\"id\":1,\"URL\":\"https://www.example.com/a\",\"amount\":10}\n{\"id\":2,\"URL\":\"https://www.example.net/b\",\"amount\":20}\n",
        )
        .expect("write partial schema jsonl source");
        let hints = vec![("id".to_string(), LogicalDType::Int64)];

        let outcome = run_vortex_ingest_prepare_once_with_schema(
            vortex_ingest_reuse_request(source, target, false),
            &hints,
        )
        .expect("schema-hinted JSONL preserves inferred fields");
        let report = prepared_vortex_ingest_report(outcome);
        let fields = field_map(report.fields());
        let summary = report.vortex_report.column_family_summary();
        assert!(summary.contains("id:int64"), "{summary}");
        assert!(summary.contains("URL:utf8"), "{summary}");
        assert!(summary.contains("amount:int64"), "{summary}");
        assert!(
            summary.contains("__shardloom_derived_url_domain_URL:"),
            "{summary}"
        );
        assert_field_eq(
            &fields,
            "source_state_stream_policy",
            "schema_hinted_jsonl_record_batch_stream_batch_size_65536_rows",
        );
        assert_field_contains(
            &fields,
            "source_state_dictionary_preservation_status",
            "__shardloom_derived_url_domain_URL",
        );
        assert_field_eq(&fields, "fallback_attempted", "false");
        assert_field_eq(&fields, "external_engine_invoked", "false");

        fs::remove_dir_all(root).expect("remove partial jsonl schema root");
    }

    #[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
    #[test]
    fn vortex_ingest_partial_csv_schema_hints_preserve_undeclared_columns() {
        let root = vortex_ingest_reuse_test_root("partial-csv-schema-hints");
        let source = root.join("input.csv");
        let target = root.join("prepared.vortex");
        fs::write(
            &source,
            "id,URL,event_date,amount\n1,https://www.example.com/a,2026-06-22,10\n2,https://www.example.net/b,2026-06-23,20\n",
        )
        .expect("write partial schema csv source");
        let hints = vec![
            ("event_date".to_string(), LogicalDType::Date32),
            ("id".to_string(), LogicalDType::Int64),
        ];

        let outcome = run_vortex_ingest_prepare_once_with_schema(
            vortex_ingest_reuse_request(source, target, false),
            &hints,
        )
        .expect("schema-hinted CSV preserves inferred fields");
        let report = prepared_vortex_ingest_report(outcome);
        let fields = field_map(report.fields());
        let summary = report.vortex_report.column_family_summary();
        assert!(summary.contains("id:int64"), "{summary}");
        assert!(summary.contains("URL:utf8"), "{summary}");
        assert!(summary.contains("event_date:date32"), "{summary}");
        assert!(summary.contains("amount:int64"), "{summary}");
        assert!(
            summary.contains("__shardloom_derived_url_domain_URL:"),
            "{summary}"
        );
        assert_field_eq(
            &fields,
            "source_state_stream_policy",
            "schema_hinted_csv_record_batch_stream_batch_size_65536_rows",
        );
        assert_field_contains(
            &fields,
            "source_state_dictionary_preservation_status",
            "__shardloom_derived_url_domain_URL",
        );
        assert_field_eq(&fields, "fallback_attempted", "false");
        assert_field_eq(&fields, "external_engine_invoked", "false");

        fs::remove_dir_all(root).expect("remove partial csv schema root");
    }

    #[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
    #[test]
    fn schema_declared_text_stream_preserves_smoke_cap_and_product_no_cap() {
        let root = vortex_ingest_reuse_test_root("schema-declared-product-cap");
        let source = root.join("large.csv");
        let smoke_target = root.join("smoke.vortex");
        let product_target = root.join("product.vortex");
        let inferred_smoke_target = root.join("inferred-smoke.vortex");
        let inferred_product_target = root.join("inferred-product.vortex");
        let hints = vec![
            ("id".to_string(), LogicalDType::Int64),
            ("label".to_string(), LogicalDType::Utf8),
        ];
        let mut body = String::from("id,label\n");
        for row_id in 0..=MAX_INPUT_ROWS {
            writeln!(&mut body, "{row_id},label-{row_id}").expect("write csv row");
        }
        fs::write(&source, body).expect("write large schema-declared csv");

        let smoke_error = run_vortex_ingest_prepare_once_with_schema(
            vortex_ingest_reuse_request(source.clone(), smoke_target, false),
            &hints,
        )
        .expect_err("smoke schema-declared stream preserves the diagnostic row cap");
        assert!(
            smoke_error
                .to_string()
                .contains("supports at most 50000 CSV data rows"),
            "{smoke_error}"
        );

        let mut product_request =
            vortex_ingest_reuse_request(source.clone(), product_target.clone(), false);
        product_request.runtime_profile = SqlLocalSourceRuntimeProfile::ProductLocalWorkflow;
        let product = run_vortex_ingest_prepare_once_with_schema(product_request, &hints)
            .expect("product schema-declared stream has no synthetic row cap");
        let product_report = prepared_vortex_ingest_report(product);
        let fields = field_map(product_report.fields());
        assert_eq!(
            product_report.vortex_report.row_count,
            u64::try_from(MAX_INPUT_ROWS + 1).expect("test row count fits u64")
        );
        assert_field_eq(
            &fields,
            "source_state_materialization_layout",
            "schema_declared_text_to_streaming_arrow_record_batch_source_state",
        );
        assert_field_eq(&fields, "source_state_stream_batch_size", "262144");
        assert_field_eq(
            &fields,
            "source_state_stream_policy",
            "schema_declared_csv_record_batch_stream_batch_size_262144_rows",
        );
        assert_field_eq(
            &fields,
            "source_read_buffer_carry_status",
            "metadata_only_source_identity_columnar_reader_not_preopened",
        );
        assert_field_eq(&fields, "source_fingerprint_policy", "metadata_only");
        assert_field_eq(&fields, "source_content_fingerprint_performed", "false");
        assert_field_eq(&fields, "fallback_attempted", "false");
        assert_field_eq(&fields, "external_engine_invoked", "false");
        assert!(product_target.exists());

        let inferred_smoke_error = run_vortex_prepare(vortex_ingest_reuse_request(
            source.clone(),
            inferred_smoke_target,
            false,
        ))
        .expect_err("smoke inferred stream preserves the diagnostic row cap");
        assert!(
            inferred_smoke_error
                .to_string()
                .contains("supports at most 50000 CSV data rows"),
            "{inferred_smoke_error}"
        );

        let mut inferred_product_request =
            vortex_ingest_reuse_request(source.clone(), inferred_product_target.clone(), false);
        inferred_product_request.runtime_profile =
            SqlLocalSourceRuntimeProfile::ProductLocalWorkflow;
        let inferred_product = run_vortex_prepare(inferred_product_request)
            .expect("product inferred stream has no synthetic row cap");
        let inferred_product_report = prepared_vortex_ingest_report(inferred_product);
        let inferred_fields = field_map(inferred_product_report.fields());
        assert_eq!(
            inferred_product_report.vortex_report.row_count,
            u64::try_from(MAX_INPUT_ROWS + 1).expect("test row count fits u64")
        );
        assert_field_eq(
            &inferred_fields,
            "source_state_materialization_layout",
            "inferred_text_to_streaming_arrow_record_batch_source_state",
        );
        assert_field_eq(&inferred_fields, "source_state_stream_batch_size", "262144");
        assert_field_eq(
            &inferred_fields,
            "source_state_stream_policy",
            "inferred_csv_record_batch_stream_batch_size_262144_rows",
        );
        assert_field_eq(
            &inferred_fields,
            "source_read_buffer_carry_status",
            "metadata_only_source_identity_columnar_reader_not_preopened",
        );
        assert_field_eq(
            &inferred_fields,
            "source_fingerprint_policy",
            "metadata_only",
        );
        assert_field_eq(
            &inferred_fields,
            "source_content_fingerprint_performed",
            "false",
        );
        assert_field_eq(&inferred_fields, "fallback_attempted", "false");
        assert_field_eq(&inferred_fields, "external_engine_invoked", "false");
        assert!(inferred_product_target.exists());

        fs::remove_dir_all(root).expect("remove schema declared product cap root");
    }

    #[cfg(feature = "vortex-write")]
    #[test]
    fn vortex_ingest_source_drift_with_overwrite_rewrites_single_artifact_without_sidecars() {
        let root = vortex_ingest_reuse_test_root("drift-rewrite");
        let source_dir = root.join("source");
        let target_dir = root.join("prepared");
        fs::create_dir_all(&source_dir).expect("create source dir");
        fs::create_dir_all(&target_dir).expect("create target dir");
        let source = source_dir.join("input.csv");
        let target = target_dir.join("prepared.vortex");
        fs::write(&source, "id,label,amount\n1,alpha,10\n").expect("write initial source");

        let first = run_vortex_prepare(vortex_ingest_reuse_request(
            source.clone(),
            target.clone(),
            false,
        ))
        .expect("first vortex_ingest run writes artifact");
        assert!(matches!(first, VortexIngestOutcome::Prepared(_)));
        let base_artifact = fs::read(&target).expect("read base artifact");
        let manifest_path = shardloom_vortex::vortex_prepared_state_reuse_manifest_path(&target)
            .expect("manifest path");
        assert!(!manifest_path.exists());

        fs::write(&source, "id,label,amount\n1,alpha,10\n2,beta,99\n").expect("mutate source");
        let second = run_vortex_prepare(vortex_ingest_reuse_request(
            source.clone(),
            target.clone(),
            true,
        ))
        .expect("changed source rewrites single Vortex artifact");
        let second_report = prepared_vortex_ingest_report(second);
        let second_fields = field_map(second_report.fields());
        assert_ne!(
            fs::read(&target).expect("read rewritten artifact"),
            base_artifact,
            "changed source with allow_overwrite must replace the single .vortex artifact"
        );
        assert_field_eq(&second_fields, "vortex_ingest_performed", "true");
        assert_field_eq(
            &second_fields,
            "vortex_ingest_status",
            "prepared_state_created",
        );
        assert_field_eq(&second_fields, "prepared_state_created", "true");
        assert_field_eq(&second_fields, "prepared_state_reused", "false");
        assert_field_eq(&second_fields, "prepared_state_reuse_hit", "false");
        assert_field_eq(
            &second_fields,
            "prepared_state_invalidation_reason",
            "not_applicable_single_vortex_artifact",
        );
        assert_field_eq(&second_fields, "fallback_attempted", "false");
        assert_field_eq(&second_fields, "external_engine_invoked", "false");
        assert!(!manifest_path.exists());
        assert!(
            fs::read_dir(target_dir.join(".shardloom"))
                .map_or(true, |mut entries| entries.next().is_none()),
            "public source drift rewrite must not leave target-adjacent sidecar files"
        );

        fs::remove_dir_all(root).expect("remove reuse drift root");
    }

    #[test]
    fn differential_preparation_schema_mismatch_rejects_overlay_without_fallback() {
        let report = shardloom_vortex::evaluate_vortex_differential_preparation(
            shardloom_vortex::VortexDifferentialPreparationInput {
                update_mode: shardloom_vortex::VortexDifferentialUpdateMode::AppendOnly,
                base_source_state_id: "base-source".to_string(),
                base_source_state_digest: "fnv64:base-source".to_string(),
                base_prepared_state_id: "base-prepared".to_string(),
                base_prepared_state_digest: "fnv64:base-prepared".to_string(),
                base_row_count: 1,
                base_schema_digest: "fnv64:base-schema".to_string(),
                base_column_family_summary: "id:int64,label:utf8".to_string(),
                delta_source_state_id: "delta-source".to_string(),
                delta_source_state_digest: "fnv64:delta-source".to_string(),
                delta_row_count: 1,
                delta_schema_digest: "fnv64:changed-schema".to_string(),
                delta_column_family_summary: "id:int64,label:utf8,extra:utf8".to_string(),
                delta_manifest_digest: "fnv64:delta-manifest".to_string(),
                changed_byte_range_refs: "target/input.csv#bytes=10..20".to_string(),
                changed_row_range_refs: "target/input.csv#rows=1..2".to_string(),
                changed_segment_refs: "delta-segment".to_string(),
                delta_artifact_ref: "target/input.delta.vortex".to_string(),
                delta_artifact_digest: "fnv64:delta-artifact".to_string(),
                native_io_certificate_refs: "base=base-prepared;delta=delta-prepared".to_string(),
            },
        );

        assert!(!report.is_admitted());
        assert_eq!(report.status, "blocked_schema_mismatch");
        assert_eq!(
            report.schema_compatibility_status,
            "blocked_source_schema_or_column_family_mismatch"
        );
        assert!(!report.overlay_applied);
        assert!(!report.fallback_attempted);
        assert!(!report.external_engine_invoked);
    }

    #[test]
    fn parses_scoped_sql_local_source_statement() {
        let parsed = parse_sql_local_source_statement(
            "SELECT id,label FROM 'target/input.csv' WHERE amount >= 10 LIMIT 5",
        )
        .expect("statement parses");

        assert_eq!(parsed.projections, vec!["id", "label"]);
        assert_eq!(parsed.aggregates, [] as [ParsedAggregate; 0]);
        assert_eq!(parsed.group_by, [] as [String; 0]);
        assert!(parsed.order_by.is_none());
        assert_eq!(
            parsed.source.local_path().unwrap(),
            Path::new("target/input.csv")
        );
        assert_eq!(parsed.limit, 5);
        assert!(matches!(
            parsed.predicate,
            ParsedPredicate::Compare {
                ref column,
                op: ComparisonOp::GtEq,
                value: ScalarValue::Int64(10)
            } if column == "amount"
        ));
    }

    #[test]
    fn parses_scoped_sql_local_source_statement_without_predicate() {
        let parsed =
            parse_sql_local_source_statement("SELECT id,label FROM 'target/input.csv' LIMIT 5")
                .expect("statement parses without a predicate");

        assert_eq!(parsed.projections, vec!["id", "label"]);
        assert_eq!(parsed.aggregates, [] as [ParsedAggregate; 0]);
        assert_eq!(parsed.group_by, [] as [String; 0]);
        assert!(parsed.order_by.is_none());
        assert_eq!(
            parsed.source.local_path().unwrap(),
            Path::new("target/input.csv")
        );
        assert_eq!(parsed.limit, 5);
        assert!(parsed.predicate.is_all());
    }

    #[test]
    fn parses_scoped_select_distinct_projection_statement() {
        let parsed = parse_sql_local_source_statement(
            "SELECT DISTINCT region,label FROM 'target/input.csv' WHERE amount >= 10 ORDER BY region,label LIMIT 5",
        )
        .expect("SELECT DISTINCT statement parses");

        assert_eq!(parsed.projections, vec!["region", "label"]);
        assert_eq!(parsed.aggregates, [] as [ParsedAggregate; 0]);
    }

    #[test]
    fn parses_scoped_literal_projection_statement() {
        let parsed = parse_sql_local_source_statement(
            "SELECT id,label,'north' AS segment,DATE '2026-05-19' AS batch_date,X'00ff10' AS payload,BINARY 'ok' AS text_payload,BLOB 'raw' AS blob_payload FROM 'target/input.csv' WHERE amount >= 10 LIMIT 5",
        )
        .expect("literal projection statement parses");

        assert_eq!(parsed.projections, vec!["id", "label"]);
        assert_eq!(parsed.literal_projections.len(), 5);
        assert_eq!(parsed.literal_projections[0].alias, "segment");
        assert_eq!(
            parsed.literal_projections[0].value,
            ScalarValue::Utf8("north".to_string())
        );
        assert_eq!(parsed.literal_projections[1].alias, "batch_date");
        assert!(matches!(
            parsed.literal_projections[1].value,
            ScalarValue::Date32(_)
        ));
        assert_eq!(parsed.literal_projections[2].alias, "payload");
        assert_eq!(
            parsed.literal_projections[2].value,
            ScalarValue::Binary(vec![0x00, 0xff, 0x10])
        );
        assert_eq!(parsed.literal_projections[3].alias, "text_payload");
        assert_eq!(
            parsed.literal_projections[3].value,
            ScalarValue::Binary(b"ok".to_vec())
        );
        assert_eq!(parsed.literal_projections[4].alias, "blob_payload");
        assert_eq!(
            parsed.literal_projections[4].value,
            ScalarValue::Binary(b"raw".to_vec())
        );
    }

    #[test]
    fn binary_hex_literal_projection_blocks_malformed_literals_without_fallback() {
        for (statement, expected) in [
            (
                "SELECT id,X'0' AS payload FROM 'target/input.csv' LIMIT 5",
                "binary hex literals require an even number of hexadecimal digits",
            ),
            (
                "SELECT id,X'00xz' AS payload FROM 'target/input.csv' LIMIT 5",
                "binary hex literals admit hexadecimal digits only",
            ),
        ] {
            let error = parse_sql_local_source_statement(statement)
                .expect_err("malformed binary hex literal remains blocked");
            assert!(
                error.to_string().contains(expected),
                "expected {expected:?}, got {error}"
            );
            assert!(error.to_string().contains("external_engine_invoked=false"));
        }
    }

    #[test]
    fn binary_text_literal_projection_blocks_malformed_literals_without_fallback() {
        for (statement, expected) in [
            (
                "SELECT id,BINARY alpha AS payload FROM 'target/input.csv' LIMIT 5",
                "SQL string literals must be single quoted",
            ),
            (
                "SELECT id,BLOB 'raw AS payload FROM 'target/input.csv' LIMIT 5",
                "SQL string literal is not closed",
            ),
        ] {
            let error = parse_sql_local_source_statement(statement)
                .expect_err("malformed binary text literal remains blocked");
            assert!(
                error.to_string().contains(expected),
                "expected {expected:?}, got {error}"
            );
            assert!(error.to_string().contains("external_engine_invoked=false"));
        }
    }

    #[test]
    fn binary_literals_are_admitted_in_direct_predicates() {
        for (statement, expected_op, expected_value) in [
            (
                "SELECT id FROM 'target/input.csv' WHERE payload = X'616c706861' LIMIT 5",
                ComparisonOp::Eq,
                b"alpha".to_vec(),
            ),
            (
                "SELECT id FROM 'target/input.csv' WHERE payload > BINARY 'alpha' LIMIT 5",
                ComparisonOp::Gt,
                b"alpha".to_vec(),
            ),
            (
                "SELECT id FROM 'target/input.csv' WHERE payload <= BLOB 'raw' LIMIT 5",
                ComparisonOp::LtEq,
                b"raw".to_vec(),
            ),
        ] {
            let parsed = parse_sql_local_source_statement(statement)
                .expect("direct binary predicate statement parses");

            assert!(
                matches!(
                    parsed.predicate,
                    ParsedPredicate::Compare {
                        ref column,
                        op,
                        value: ScalarValue::Binary(ref value),
                    } if column == "payload"
                        && op == expected_op
                        && value.as_slice() == expected_value.as_slice()
                ),
                "unexpected predicate for {statement}: {:?}",
                parsed.predicate
            );
        }

        let error = parse_sql_local_source_statement(
            "SELECT id FROM 'target/input.csv' WHERE label = X'0' LIMIT 5",
        )
        .expect_err("malformed binary hex predicate literal remains blocked");
        assert!(
            error
                .to_string()
                .contains("binary hex literals require an even number")
        );
        assert!(error.to_string().contains("external_engine_invoked=false"));

        for statement in [
            "SELECT id,CASE WHEN id = 1 THEN X'00' ELSE X'ff' END AS payload FROM 'target/input.csv' LIMIT 5",
            "SELECT id,CASE WHEN id = 1 THEN BINARY 'x' ELSE BLOB 'y' END AS payload FROM 'target/input.csv' LIMIT 5",
        ] {
            let parsed = parse_sql_local_source_statement(statement)
                .expect("binary conditional declarations lower to the native expression binder");
            assert_eq!(parsed.generic_expression_projections.len(), 1);
            assert_eq!(parsed.generic_expression_projections[0].alias, "payload");
        }
    }

    #[test]
    fn binary_helper_predicates_are_admitted() {
        for (
            statement,
            expected_op,
            expected_comparison,
            expected_value,
            expected_source_columns,
        ) in [
            (
                "SELECT id FROM 'target/input.csv' WHERE UNHEX(hex_payload) = X'00ff10' LIMIT 5",
                BinaryHelperOp::Unhex,
                ComparisonOp::Eq,
                vec![0x00, 0xff, 0x10],
                vec!["hex_payload"],
            ),
            (
                "SELECT id FROM 'target/input.csv' WHERE FROM_BASE64(b64_payload) >= BINARY 'alpha' LIMIT 5",
                BinaryHelperOp::FromBase64,
                ComparisonOp::GtEq,
                b"alpha".to_vec(),
                vec!["b64_payload"],
            ),
            (
                "SELECT id FROM 'target/input.csv' WHERE FROM_BASE64(b64_payload) < BLOB 'raw' LIMIT 5",
                BinaryHelperOp::FromBase64,
                ComparisonOp::Lt,
                b"raw".to_vec(),
                vec!["b64_payload"],
            ),
            (
                "SELECT id FROM 'target/input.csv' WHERE FROM_BASE64(CONCAT(b64_prefix,b64_suffix)) = BINARY 'alpha' LIMIT 5",
                BinaryHelperOp::FromBase64,
                ComparisonOp::Eq,
                b"alpha".to_vec(),
                vec!["b64_prefix", "b64_suffix"],
            ),
        ] {
            let parsed = parse_sql_local_source_statement(statement)
                .expect("binary helper predicate parses");

            assert!(
                matches!(
                    &parsed.predicate,
                    ParsedPredicate::BinaryHelperCompare(ParsedBinaryHelperPredicate {
                        source_columns,
                        op,
                        comparison,
                        value,
                        ..
                    }) if source_columns
                        .iter()
                        .map(String::as_str)
                        .collect::<Vec<_>>()
                        == expected_source_columns
                        && *op == expected_op
                        && *comparison == expected_comparison
                        && value.as_slice() == expected_value.as_slice()
                ),
                "unexpected helper predicate for {statement}: {:?}",
                parsed.predicate
            );
        }

        let error = parse_sql_local_source_statement(
            "SELECT id FROM 'target/input.csv' WHERE UNHEX(hex_payload) = NULL LIMIT 5",
        )
        .expect_err("NULL helper predicate literal remains blocked");
        assert!(
            error
                .to_string()
                .contains("binary helper predicates admit X'<hex>'"),
            "{error}"
        );
        assert!(error.to_string().contains("external_engine_invoked=false"));
    }

    #[test]
    fn parses_scoped_complex_projection_statement() {
        let parsed = parse_sql_local_source_statement(
            "SELECT id,ARRAY[1,2,NULL] AS values,STRUCT(label, amount) AS payload FROM 'target/input.csv' LIMIT 5",
        )
        .expect("scoped complex projections parse");

        assert_eq!(parsed.projections, vec!["id"]);
        assert_eq!(parsed.complex_projections.len(), 2);
        assert_eq!(parsed.complex_projections[0].alias, "values");
        assert_eq!(
            parsed.complex_projections[0].kind,
            ParsedComplexProjectionKind::ArrayLiteral(vec![
                ScalarValue::Int64(1),
                ScalarValue::Int64(2),
                ScalarValue::Null
            ])
        );
        assert_eq!(parsed.complex_projections[1].alias, "payload");
        assert_eq!(
            parsed.complex_projections[1].kind,
            ParsedComplexProjectionKind::StructColumns(vec![
                "label".to_string(),
                "amount".to_string()
            ])
        );
        assert!(parsed.has_complex_projection());
    }

    #[test]
    fn parses_scoped_binary_helper_projection_statement() {
        let parsed = parse_sql_local_source_statement(
            "SELECT id,UNHEX(LOWER(TRIM(hex_payload))) AS payload_hex,FROM_BASE64(CONCAT(b64_prefix,b64_suffix)) AS payload_b64 FROM 'target/input.csv' WHERE id >= 1 LIMIT 5",
        )
        .expect("binary helper projection statement parses");

        assert_eq!(parsed.projections, vec!["id"]);
        assert_eq!(parsed.binary_helper_projections.len(), 2);
        assert_eq!(parsed.binary_helper_projections[0].alias, "payload_hex");
        assert_eq!(
            parsed.binary_helper_projections[0].source_columns,
            vec!["hex_payload"]
        );
        assert_eq!(
            parsed.binary_helper_projections[0].op,
            BinaryHelperOp::Unhex
        );
        assert_eq!(parsed.binary_helper_projections[1].alias, "payload_b64");
        assert_eq!(
            parsed.binary_helper_projections[1].source_columns,
            vec!["b64_prefix", "b64_suffix"]
        );
        assert_eq!(
            parsed.binary_helper_projections[1].op,
            BinaryHelperOp::FromBase64
        );
    }

    #[test]
    fn parses_scoped_binary_byte_length_projection_and_predicate_statement() {
        let parsed = parse_sql_local_source_statement(
            "SELECT id,BYTE_LENGTH(UNHEX(LOWER(TRIM(hex_payload)))) AS payload_len,OCTET_LENGTH(CAST(CONCAT(label_prefix,label_suffix) AS binary)) AS label_len FROM 'target/input.csv' WHERE BYTE_LENGTH(FROM_BASE64(CONCAT(b64_prefix,b64_suffix))) >= 4 LIMIT 5",
        )
        .expect("binary byte length statement parses");

        assert_eq!(parsed.projections, vec!["id"]);
        assert_eq!(parsed.binary_byte_length_projections.len(), 2);
        assert_eq!(
            parsed.binary_byte_length_projections[0].alias,
            "payload_len"
        );
        assert_eq!(
            parsed.binary_byte_length_projections[0].argument_family,
            BinaryByteLengthArgumentFamily::Helper(BinaryHelperOp::Unhex)
        );
        assert_eq!(
            parsed.binary_byte_length_projections[0].source_columns,
            vec!["hex_payload"]
        );
        assert_eq!(
            parsed.binary_byte_length_projections[1].argument_family,
            BinaryByteLengthArgumentFamily::Cast(CastMode::Strict)
        );
        assert_eq!(
            parsed.binary_byte_length_projections[1].source_columns,
            vec!["label_prefix", "label_suffix"]
        );

        assert!(matches!(
            parsed.predicate,
            ParsedPredicate::BinaryByteLengthCompare(ParsedBinaryByteLengthPredicate {
                argument_family: BinaryByteLengthArgumentFamily::Helper(BinaryHelperOp::FromBase64),
                comparison: ComparisonOp::GtEq,
                value: ScalarValue::Int64(4),
                ..
            })
        ));
    }

    #[test]
    fn parses_scoped_numeric_arithmetic_projection_statement() {
        let parsed = parse_sql_local_source_statement(
            "SELECT id,amount + 5 AS adjusted,ratio * 2.0 AS doubled FROM 'target/input.csv' WHERE amount >= 10 LIMIT 5",
        )
        .expect("numeric arithmetic projection statement parses");

        assert_eq!(parsed.projections, vec!["id"]);
        assert_eq!(
            parsed.literal_projections,
            [] as [ParsedLiteralProjection; 0]
        );
        assert_eq!(parsed.numeric_arithmetic_projections.len(), 2);
        assert_eq!(parsed.numeric_arithmetic_projections[0].alias, "adjusted");
        assert_eq!(parsed.numeric_arithmetic_projections[0].column, "amount");
        assert_eq!(
            parsed.numeric_arithmetic_projections[0].op,
            NumericArithmeticOp::Add
        );
        assert_eq!(
            parsed.numeric_arithmetic_projections[0].rhs,
            ScalarValue::Int64(5)
        );
        assert_eq!(parsed.numeric_arithmetic_projections[1].alias, "doubled");
    }

    #[test]
    fn parses_scoped_decimal_generic_arithmetic_projection_statement() {
        let parsed = parse_sql_local_source_statement(
            "SELECT id,CAST(amount AS decimal128(10,2)) + CAST('1.25' AS decimal128(10,2)) AS adjusted FROM 'target/input.csv' WHERE id >= 1 LIMIT 5",
        )
        .expect("decimal generic arithmetic projection statement parses");

        assert_eq!(parsed.projections, vec!["id"]);
        assert_eq!(
            parsed.numeric_arithmetic_projections,
            [] as [ParsedNumericArithmeticProjection; 0]
        );
        assert_eq!(parsed.generic_expression_projections.len(), 1);
        assert_eq!(parsed.generic_expression_projections[0].alias, "adjusted");
    }

    #[test]
    fn parses_star_plus_computed_projection_statement() {
        let parsed = parse_sql_local_source_statement(
            "SELECT *,amount + 5 AS adjusted,LOWER(label) AS normalized FROM 'target/input.jsonl' WHERE amount >= 10 LIMIT 5",
        )
        .expect("star plus computed projection statement parses");

        assert_eq!(parsed.projections, vec!["*"]);
        assert_eq!(parsed.numeric_arithmetic_projections.len(), 1);
        assert_eq!(parsed.numeric_arithmetic_projections[0].alias, "adjusted");
        assert_eq!(parsed.string_transform_projections.len(), 1);
        assert_eq!(parsed.string_transform_projections[0].alias, "normalized");
    }

    #[test]
    fn parses_computed_projection_order_by_topn_statement() {
        let parsed = parse_sql_local_source_statement(
            "SELECT id,amount + 5 AS adjusted FROM 'target/input.csv' WHERE amount >= 10 ORDER BY adjusted DESC LIMIT 2",
        )
        .expect("computed projection order-by statement parses");

        assert_eq!(parsed.projections, vec!["id"]);
        assert_eq!(parsed.numeric_arithmetic_projections.len(), 1);
        assert_eq!(parsed.numeric_arithmetic_projections[0].alias, "adjusted");
        let order_by = parsed.order_by.as_ref().expect("order by parsed");
        assert_eq!(order_by.keys.len(), 1);
        assert_eq!(order_by.keys[0].column, "adjusted");
        assert_eq!(order_by.keys[0].direction, SortDirection::Desc);
    }

    #[test]
    fn parses_scoped_window_row_number_statement() {
        let parsed = parse_sql_local_source_statement(
            "SELECT id,region,amount,ROW_NUMBER() OVER (PARTITION BY region ORDER BY amount DESC) AS rn FROM 'target/input.csv' WHERE amount >= 10 LIMIT 4",
        )
        .expect("window row-number statement parses");

        assert_eq!(parsed.projections, vec!["id", "region", "amount"]);
        assert_eq!(parsed.window_projections.len(), 1);
        let window = &parsed.window_projections[0];
        assert_eq!(window.alias, "rn");
        assert_eq!(window.function, WindowFunction::RowNumber);
        assert_eq!(window.partition_by, vec!["region".to_string()]);
    }

    #[test]
    fn parses_scoped_window_ranking_statement() {
        let parsed = parse_sql_local_source_statement(
            "SELECT id,region,RANK() OVER (PARTITION BY region ORDER BY amount DESC) AS r,DENSE_RANK() OVER (PARTITION BY region ORDER BY amount DESC) AS dr FROM 'target/input.csv' LIMIT 6",
        )
        .expect("window ranking statement parses");

        assert_eq!(parsed.projections, vec!["id", "region"]);
        assert_eq!(parsed.window_projections.len(), 2);
        assert_eq!(parsed.window_projections[0].alias, "r");
        assert_eq!(parsed.window_projections[0].function, WindowFunction::Rank);
        assert_eq!(parsed.window_projections[1].alias, "dr");
        assert_eq!(
            parsed.window_projections[1].function,
            WindowFunction::DenseRank
        );
    }

    #[test]
    fn parses_scoped_window_offset_statement() {
        let parsed = parse_sql_local_source_statement(
            "SELECT id,region,LAG(label) OVER (PARTITION BY region ORDER BY amount ASC) AS previous_label,LEAD(label, 2) OVER (PARTITION BY region ORDER BY amount ASC) AS next2_label FROM 'target/input.csv' LIMIT 6",
        )
        .expect("window offset statement parses");

        assert_eq!(parsed.projections, vec!["id", "region"]);
        assert_eq!(parsed.window_projections.len(), 2);
        assert_eq!(parsed.window_projections[0].alias, "previous_label");
        assert_eq!(
            parsed.window_projections[0].function,
            WindowFunction::Lag {
                column: "label".to_string(),
                offset: 1,
            }
        );
        assert_eq!(parsed.window_projections[1].alias, "next2_label");
        assert_eq!(
            parsed.window_projections[1].function,
            WindowFunction::Lead {
                column: "label".to_string(),
                offset: 2,
            }
        );
    }

    #[test]
    fn parses_scoped_window_distribution_statement() {
        let parsed = parse_sql_local_source_statement(
            "SELECT id,region,NTILE(4) OVER (PARTITION BY region ORDER BY amount DESC) AS bucket,PERCENT_RANK() OVER (PARTITION BY region ORDER BY amount DESC) AS percent_rank,CUME_DIST() OVER (PARTITION BY region ORDER BY amount DESC) AS cume_dist FROM 'target/input.csv' LIMIT 6",
        )
        .expect("window distribution statement parses");

        assert_eq!(parsed.projections, vec!["id", "region"]);
        assert_eq!(parsed.window_projections.len(), 3);
        assert_eq!(parsed.window_projections[0].alias, "bucket");
        assert_eq!(
            parsed.window_projections[0].function,
            WindowFunction::Ntile { bucket_count: 4 }
        );
        assert_eq!(parsed.window_projections[1].alias, "percent_rank");
        assert_eq!(
            parsed.window_projections[1].function,
            WindowFunction::PercentRank
        );
        assert_eq!(parsed.window_projections[2].alias, "cume_dist");
        assert_eq!(
            parsed.window_projections[2].function,
            WindowFunction::CumeDist
        );
    }

    #[test]
    fn parser_blocks_invalid_window_offset_without_fallback() {
        let error = parse_sql_local_source_statement(
            "SELECT id,LAG(label, 0) OVER (ORDER BY amount ASC) AS previous_label FROM 'target/input.csv' LIMIT 6",
        )
        .expect_err("zero offset is not admitted");

        assert!(
            error
                .to_string()
                .contains("LAG window offset must be between 1 and 50000")
        );
    }

    #[test]
    fn parser_blocks_invalid_window_bucket_count_without_fallback() {
        let error = parse_sql_local_source_statement(
            "SELECT id,NTILE(0) OVER (ORDER BY amount ASC) AS bucket FROM 'target/input.csv' LIMIT 6",
        )
        .expect_err("zero bucket count is not admitted");

        assert!(
            error
                .to_string()
                .contains("NTILE window bucket count must be between 1 and 50000")
        );
    }

    #[test]
    fn parser_blocks_star_plus_raw_projection_without_fallback() {
        let error = parse_sql_local_source_statement(
            "SELECT *,id FROM 'target/input.csv' WHERE amount >= 10 LIMIT 5",
        )
        .expect_err("star plus raw projection is not admitted");

        assert!(error.to_string().contains(
            "SELECT * can be mixed only with computed, literal, or window projections in this scoped smoke"
        ));
    }

    #[test]
    fn parser_blocks_repeated_wildcard_projection_without_fallback() {
        let error = parse_sql_local_source_statement(
            "SELECT *,* FROM 'target/input.csv' WHERE amount >= 10 LIMIT 5",
        )
        .expect_err("repeated wildcard projection is not admitted");

        assert!(
            error
                .to_string()
                .contains("SELECT * may appear only once in this scoped smoke")
        );
    }

    #[test]
    fn parser_admits_left_literal_first_argument() {
        let parsed = parse_sql_local_source_statement(
            "SELECT id,LEFT('literal', 2) AS prefix FROM 'target/input.csv' LIMIT 5",
        )
        .expect("constant scalar calls lower to the native expression binder");
        assert_eq!(parsed.generic_expression_projections.len(), 1);
        assert_eq!(parsed.generic_expression_projections[0].alias, "prefix");
    }

    #[test]
    fn parser_uses_earliest_join_keyword_for_join_type() {
        let raw = "'target/fact.csv' AS f JOIN 'target/dim.csv' AS d ON f.id = d.id LEFT JOIN tail";
        let (index, keyword_len, join_type) =
            find_join_keyword(raw).unwrap().expect("join keyword found");

        assert_eq!(&raw[index..index + keyword_len], "JOIN");
        assert_eq!(join_type, ParsedJoinType::InnerEqui);
    }

    #[test]
    fn parses_generic_expression_projection_statement() {
        let parsed = parse_sql_local_source_statement(
            "SELECT id,(amount + tax) * 2 AS gross,ABS(amount - tax) AS spread FROM 'target/input.csv' WHERE amount >= 10 LIMIT 5",
        )
        .expect("generic expression projection statement parses");

        assert_eq!(parsed.projections, vec!["id"]);
        assert_eq!(
            parsed.numeric_arithmetic_projections,
            [] as [ParsedNumericArithmeticProjection; 0]
        );
        assert_eq!(parsed.generic_expression_projections.len(), 2);
        assert_eq!(parsed.generic_expression_projections[0].alias, "gross");
        assert_eq!(
            parsed.generic_expression_projections[0].source_columns,
            vec!["amount".to_string(), "tax".to_string()]
        );
    }

    #[test]
    fn parses_temporal_difference_generic_expression_statement() {
        let parsed = parse_sql_local_source_statement(
            "SELECT id,DATE_DIFF_DAYS(CAST(end_date AS date32), start_date) AS age_days,TIMESTAMP_DIFF_SECONDS(CAST(end_ts AS timestamp_micros), start_ts) AS elapsed_seconds FROM 'target/input.csv' WHERE DATE_DIFF_DAYS(end_date, DATE '2026-05-19') >= 2 LIMIT 5",
        )
        .expect("temporal difference generic expression statement parses");

        assert_eq!(parsed.projections, vec!["id"]);
        assert_eq!(parsed.generic_expression_projections.len(), 2);

        assert!(parsed.predicate.uses_generic_expression());

        assert_eq!(parsed.predicate.columns(), vec!["end_date"]);
    }

    #[test]
    fn parses_scoped_numeric_abs_projection_statement() {
        let parsed = parse_sql_local_source_statement(
            "SELECT id,ABS(amount) AS magnitude FROM 'target/input.csv' WHERE ABS(amount) >= 4 LIMIT 5",
        )
        .expect("numeric abs projection statement parses");

        assert_eq!(parsed.projections, vec!["id"]);
        assert_eq!(
            parsed.literal_projections,
            [] as [ParsedLiteralProjection; 0]
        );
        assert_eq!(parsed.numeric_abs_projections.len(), 1);
        assert_eq!(parsed.numeric_abs_projections[0].alias, "magnitude");
        assert_eq!(parsed.numeric_abs_projections[0].column, "amount");
    }

    #[test]
    fn parses_scoped_numeric_rounding_projection_statement() {
        let parsed = parse_sql_local_source_statement(
            "SELECT id,FLOOR(amount) AS bucket,CEIL(ratio) AS upper FROM 'target/input.csv' WHERE ROUND(amount) >= 4 LIMIT 5",
        )
        .expect("numeric rounding projection statement parses");

        assert_eq!(parsed.projections, vec!["id"]);
        assert_eq!(parsed.numeric_rounding_projections.len(), 2);
        assert_eq!(parsed.numeric_rounding_projections[0].alias, "bucket");
        assert_eq!(parsed.numeric_rounding_projections[0].column, "amount");
        assert_eq!(
            parsed.numeric_rounding_projections[0].op,
            NumericRoundingOp::Floor
        );
        assert_eq!(parsed.numeric_rounding_projections[1].alias, "upper");
        assert_eq!(parsed.numeric_rounding_projections[1].column, "ratio");
        assert_eq!(
            parsed.numeric_rounding_projections[1].op,
            NumericRoundingOp::Ceil
        );
    }

    #[test]
    fn parses_scoped_cast_projection_statement() {
        let statement = concat!(
            "SELECT id,CAST(amount AS float64) AS amount_float,",
            "CAST(event_date AS date32) AS event_day,",
            "CAST(CONCAT(label_prefix,label_suffix) AS binary) AS label_bytes ",
            "FROM 'target/input.csv' WHERE id >= 1 LIMIT 5"
        );
        let parsed =
            parse_sql_local_source_statement(statement).expect("cast projection statement parses");

        assert_eq!(parsed.projections, vec!["id"]);
        assert_eq!(
            parsed.literal_projections,
            [] as [ParsedLiteralProjection; 0]
        );
        assert_eq!(parsed.cast_projections.len(), 3);
        assert_eq!(parsed.cast_projections[0].alias, "amount_float");
        assert_eq!(parsed.cast_projections[0].column, "amount");
        assert_eq!(
            parsed.cast_projections[0].target_dtype,
            LogicalDType::Float64
        );
        assert_eq!(parsed.cast_projections[0].mode, CastMode::Strict);
        assert_eq!(parsed.cast_projections[1].alias, "event_day");
        assert_eq!(parsed.cast_projections[1].column, "event_date");
        assert_eq!(
            parsed.cast_projections[1].target_dtype,
            LogicalDType::Date32
        );
        assert_eq!(parsed.cast_projections[1].mode, CastMode::Strict);
        assert_eq!(parsed.cast_projections[2].alias, "label_bytes");
        assert_eq!(parsed.cast_projections[2].column, "label_prefix");
        assert_eq!(
            parsed.cast_projections[2].source_columns,
            vec!["label_prefix".to_string(), "label_suffix".to_string()]
        );
        assert_eq!(
            parsed.cast_projections[2].target_dtype,
            LogicalDType::Binary
        );
        assert_eq!(parsed.cast_projections[2].mode, CastMode::Strict);
    }

    #[test]
    fn parses_scoped_try_cast_projection_statement() {
        let parsed = parse_sql_local_source_statement(
            "SELECT id,TRY_CAST(raw_amount AS int64) AS amount_i64 FROM 'target/input.csv' WHERE id >= 1 LIMIT 5",
        )
        .expect("try_cast projection statement parses");

        assert_eq!(parsed.projections, vec!["id"]);
        assert_eq!(parsed.cast_projections.len(), 1);
        assert_eq!(parsed.cast_projections[0].alias, "amount_i64");
        assert_eq!(parsed.cast_projections[0].column, "raw_amount");
        assert_eq!(parsed.cast_projections[0].target_dtype, LogicalDType::Int64);
        assert_eq!(parsed.cast_projections[0].mode, CastMode::Try);
    }

    #[test]
    fn parses_scoped_date_arithmetic_projection_statement() {
        let parsed = parse_sql_local_source_statement(
            "SELECT id,DATE_ADD_DAYS(CAST(event_date AS date32), 7) AS next_week,DATE_SUB_DAYS(event_date, 1) AS prior_day FROM 'target/input.csv' WHERE id >= 1 LIMIT 5",
        )
        .expect("date arithmetic projection statement parses");

        assert_eq!(parsed.projections, vec!["id"]);
        assert_eq!(parsed.date_arithmetic_projections.len(), 2);
        assert_eq!(parsed.date_arithmetic_projections[0].alias, "next_week");
        assert_eq!(parsed.date_arithmetic_projections[0].column, "event_date");
        assert_eq!(
            parsed.date_arithmetic_projections[0].op,
            DateArithmeticOp::AddDays
        );
        assert_eq!(parsed.date_arithmetic_projections[0].day_count, 7);
        assert_eq!(parsed.date_arithmetic_projections[1].alias, "prior_day");
        assert_eq!(parsed.date_arithmetic_projections[1].column, "event_date");
        assert_eq!(
            parsed.date_arithmetic_projections[1].op,
            DateArithmeticOp::SubDays
        );
        assert_eq!(parsed.date_arithmetic_projections[1].day_count, 1);
    }

    #[test]
    fn parses_scoped_timestamp_arithmetic_projection_and_predicate_statement() {
        let parsed = parse_sql_local_source_statement(
            "SELECT id,TIMESTAMP_ADD_SECONDS(CAST(event_ts AS timestamp_micros), 90) AS shifted_ts,TIMESTAMP_SUB_SECONDS(event_ts, 45) AS prior_ts FROM 'target/input.csv' WHERE TIMESTAMP_ADD_SECONDS(event_ts, 60) >= TIMESTAMP '2026-05-19T12:35:45Z' LIMIT 5",
        )
        .expect("timestamp arithmetic projection statement parses");

        assert_eq!(parsed.projections, vec!["id"]);
        assert_eq!(parsed.timestamp_arithmetic_projections.len(), 2);
        assert_eq!(
            parsed.timestamp_arithmetic_projections[0].alias,
            "shifted_ts"
        );
        assert_eq!(
            parsed.timestamp_arithmetic_projections[0].column,
            "event_ts"
        );
        assert_eq!(
            parsed.timestamp_arithmetic_projections[0].op,
            TimestampArithmeticOp::AddSeconds
        );
        assert_eq!(parsed.timestamp_arithmetic_projections[0].second_count, 90);
        assert_eq!(parsed.timestamp_arithmetic_projections[1].alias, "prior_ts");
        assert_eq!(
            parsed.timestamp_arithmetic_projections[1].op,
            TimestampArithmeticOp::SubSeconds
        );

        assert!(matches!(
            parsed.predicate,
            ParsedPredicate::TimestampArithmeticCompare {
                ref column,
                op: TimestampArithmeticOp::AddSeconds,
                second_count: 60,
                comparison: ComparisonOp::GtEq,
                value: ScalarValue::TimestampMicros(_),
            } if column == "event_ts"
        ));
    }

    #[test]
    fn parses_scoped_interval_literal_temporal_arithmetic_statement() {
        let parsed = parse_sql_local_source_statement(
            "SELECT id,DATE_ADD_DAYS(event_date, INTERVAL '1' DAY) AS next_day,DATE_SUB_DAYS(event_date, INTERVAL '2' DAYS) AS prior_two,TIMESTAMP_ADD_SECONDS(event_ts, INTERVAL '90' SECOND) AS shifted_ts,TIMESTAMP_SUB_SECONDS(event_ts, INTERVAL '1' MINUTE) AS prior_minute FROM 'target/input.csv' WHERE TIMESTAMP_ADD_SECONDS(event_ts, INTERVAL '1' HOUR) >= TIMESTAMP '2026-05-19T13:34:45Z' LIMIT 5",
        )
        .expect("scoped interval literal temporal arithmetic parses");

        assert_eq!(parsed.projections, vec!["id"]);
        assert_eq!(parsed.date_arithmetic_projections.len(), 2);

        assert_eq!(parsed.timestamp_arithmetic_projections.len(), 2);

        assert!(matches!(
            parsed.predicate,
            ParsedPredicate::TimestampArithmeticCompare {
                ref column,
                op: TimestampArithmeticOp::AddSeconds,
                second_count: 3600,
                comparison: ComparisonOp::GtEq,
                value: ScalarValue::TimestampMicros(_),
            } if column == "event_ts"
        ));
    }

    #[test]
    fn parses_scoped_null_coalesce_projection_statement() {
        let parsed = parse_sql_local_source_statement(
            "SELECT id,COALESCE(label, 'unknown') AS label_clean,COALESCE(event_date, DATE '2026-01-01') AS event_day FROM 'target/input.csv' WHERE id >= 1 LIMIT 5",
        )
        .expect("null coalesce projection statement parses");

        assert_eq!(parsed.projections, vec!["id"]);
        assert_eq!(parsed.null_coalesce_projections.len(), 2);
        assert_eq!(parsed.null_coalesce_projections[0].alias, "label_clean");
        assert_eq!(parsed.null_coalesce_projections[0].column, "label");
        assert_eq!(
            parsed.null_coalesce_projections[0].fallback,
            ScalarValue::Utf8("unknown".to_string())
        );
        assert_eq!(parsed.null_coalesce_projections[1].alias, "event_day");
        assert_eq!(parsed.null_coalesce_projections[1].column, "event_date");
        assert!(matches!(
            parsed.null_coalesce_projections[1].fallback,
            ScalarValue::Date32(_)
        ));
    }

    #[test]
    fn parses_scoped_nullif_projection_statement() {
        let parsed = parse_sql_local_source_statement(
            "SELECT id,NULLIF(label, 'missing') AS label_clean,NULLIF(CAST(event_date AS date32), DATE '2026-01-01') AS event_day FROM 'target/input.csv' WHERE id >= 1 LIMIT 5",
        )
        .expect("nullif projection statement parses");

        assert_eq!(parsed.projections, vec!["id"]);
        assert_eq!(parsed.nullif_projections.len(), 2);
        assert_eq!(parsed.nullif_projections[0].alias, "label_clean");
        assert_eq!(parsed.nullif_projections[0].column, "label");
        assert_eq!(
            parsed.nullif_projections[0].sentinel,
            ScalarValue::Utf8("missing".to_string())
        );
        assert_eq!(parsed.nullif_projections[1].alias, "event_day");
        assert_eq!(parsed.nullif_projections[1].column, "event_date");
        assert_eq!(
            parsed.nullif_projections[1].source_cast_dtype,
            Some(LogicalDType::Date32)
        );
        assert!(matches!(
            parsed.nullif_projections[1].sentinel,
            ScalarValue::Date32(_)
        ));
    }

    #[test]
    fn parses_scoped_conditional_projection_statement() {
        let parsed = parse_sql_local_source_statement(
            "SELECT id,CASE WHEN amount >= 10 THEN 'large' ELSE 'small' END AS size_band,CASE WHEN event_date >= DATE '2026-01-01' THEN DATE '2026-12-31' ELSE DATE '2025-12-31' END AS cutoff_day FROM 'target/input.csv' WHERE id >= 1 LIMIT 5",
        )
        .expect("conditional projection statement parses");

        assert_eq!(parsed.projections, vec!["id"]);
        assert_eq!(parsed.conditional_projections.len(), 2);
        assert_eq!(parsed.conditional_projections[0].alias, "size_band");

        assert_eq!(
            parsed.conditional_projections[0].then_branch,
            ParsedConditionalBranch::Literal(ScalarValue::Utf8("large".to_string()))
        );
        assert_eq!(
            parsed.conditional_projections[0].else_branch,
            ParsedConditionalBranch::Literal(ScalarValue::Utf8("small".to_string()))
        );
        assert_eq!(parsed.conditional_projections[1].alias, "cutoff_day");
        assert!(matches!(
            parsed.conditional_projections[1].then_branch,
            ParsedConditionalBranch::Literal(ScalarValue::Date32(_))
        ));
    }

    #[test]
    fn parses_scoped_conditional_projection_source_column_branches() {
        let parsed = parse_sql_local_source_statement(
            "SELECT id,CASE WHEN amount >= 10 THEN preferred_label ELSE fallback_label END AS label_out FROM 'target/input.csv' WHERE id >= 1 LIMIT 5",
        )
        .expect("conditional projection column branch statement parses");

        assert_eq!(parsed.projections, vec!["id"]);
        assert_eq!(parsed.conditional_projections.len(), 1);
        assert_eq!(parsed.conditional_projections[0].alias, "label_out");
        assert_eq!(
            parsed.conditional_projections[0].then_branch,
            ParsedConditionalBranch::Column("preferred_label".to_string())
        );
        assert_eq!(
            parsed.conditional_projections[0].else_branch,
            ParsedConditionalBranch::Column("fallback_label".to_string())
        );
    }

    #[test]
    fn parses_scoped_predicate_projection_statement() {
        let parsed = parse_sql_local_source_statement(
            "SELECT id,amount >= 10 AS is_large,label IS NULL AS missing_label,active IS NOT TRUE AS inactive_or_unknown FROM 'target/input.csv' WHERE id >= 1 LIMIT 5",
        )
        .expect("predicate projection statement parses");

        assert_eq!(parsed.projections, vec!["id"]);
        assert_eq!(parsed.predicate_projections.len(), 3);
        assert_eq!(parsed.predicate_projections[0].alias, "is_large");
    }

    #[test]
    fn parses_scoped_correlated_predicate_projection_statement() {
        let parsed = parse_sql_local_source_statement(
            "SELECT id,id IN (SELECT id FROM 'target/allowed.csv' WHERE id = outer.id LIMIT 5) AS matched FROM 'target/input.csv' LIMIT 5",
        )
        .expect("correlated predicate projection statement parses");

        assert_eq!(parsed.projections, vec!["id"]);
        assert_eq!(parsed.predicate_projections.len(), 1);
        assert_eq!(parsed.predicate_projections[0].alias, "matched");

        assert!(parsed.uses_outer_correlation());
    }

    #[test]
    fn parses_scoped_correlated_conditional_projection_statement() {
        let parsed = parse_sql_local_source_statement(
            "SELECT id,CASE WHEN id IN (SELECT id FROM 'target/allowed.csv' WHERE id = outer.id LIMIT 5) THEN 'yes' ELSE 'no' END AS matched FROM 'target/input.csv' LIMIT 5",
        )
        .expect("correlated CASE projection statement parses");

        assert_eq!(parsed.projections, vec!["id"]);
        assert_eq!(parsed.conditional_projections.len(), 1);
        assert_eq!(parsed.conditional_projections[0].alias, "matched");

        assert!(parsed.uses_outer_correlation());
    }

    #[test]
    fn parses_generic_expression_compare_projection_as_predicate_projection() {
        let parsed = parse_sql_local_source_statement(
            "SELECT id,amount + fee >= 10 AS is_profitable FROM 'target/input.csv' WHERE id >= 1 LIMIT 5",
        )
        .expect("generic expression predicate projection parses");

        assert_eq!(parsed.predicate_projections.len(), 1);
        assert_eq!(parsed.predicate_projections[0].alias, "is_profitable");

        assert_eq!(
            parsed.generic_expression_projections,
            [] as [ParsedGenericExpressionProjection; 0]
        );
    }

    #[test]
    fn parses_scoped_string_transform_projection_statement() {
        let parsed = parse_sql_local_source_statement(
            "SELECT id,LOWER(label) AS lowered,UPPER(label) AS raised,TRIM(label) AS trimmed FROM 'target/input.csv' WHERE id >= 1 LIMIT 5",
        )
        .expect("string transform projection statement parses");

        assert_eq!(parsed.projections, vec!["id"]);
        assert_eq!(
            parsed.literal_projections,
            [] as [ParsedLiteralProjection; 0]
        );
        assert_eq!(
            parsed.numeric_arithmetic_projections,
            [] as [ParsedNumericArithmeticProjection; 0]
        );
        assert_eq!(parsed.string_transform_projections.len(), 3);
        assert_eq!(parsed.string_transform_projections[0].alias, "lowered");
        assert_eq!(
            parsed.string_transform_projections[0].source_columns,
            vec!["label".to_string()]
        );
        assert_eq!(
            parsed.string_transform_projections[0].op,
            StringTransformOp::Lower
        );
    }

    #[test]
    fn parses_scoped_string_length_projection_statement() {
        let parsed = parse_sql_local_source_statement(
            "SELECT id,LENGTH(label) AS label_len FROM 'target/input.csv' WHERE id >= 1 LIMIT 5",
        )
        .expect("string length projection statement parses");

        assert_eq!(parsed.projections, vec!["id"]);
        assert_eq!(
            parsed.literal_projections,
            [] as [ParsedLiteralProjection; 0]
        );
        assert_eq!(
            parsed.string_transform_projections,
            [] as [ParsedStringTransformProjection; 0]
        );
        assert_eq!(parsed.string_length_projections.len(), 1);
        assert_eq!(parsed.string_length_projections[0].alias, "label_len");
        assert_eq!(
            parsed.string_length_projections[0].source_columns,
            vec!["label".to_string()]
        );
    }

    #[test]
    fn parses_scoped_string_function_projection_and_predicate_statement() {
        let parsed = parse_sql_local_source_statement(
            "SELECT id,CONCAT(label, '-', segment) AS label_key,SUBSTR(label, 2, 3) AS middle,LEFT(label, 2) AS prefix,RIGHT(label, 2) AS suffix,REPLACE(label, 'a', '') AS scrubbed FROM 'target/input.csv' WHERE CONCAT(label, '-', segment) = 'alpha-north' LIMIT 5",
        )
        .expect("string function projection statement parses");

        assert_eq!(parsed.projections, vec!["id"]);
        assert_eq!(parsed.string_function_projections.len(), 5);
        assert_eq!(parsed.string_function_projections[0].alias, "label_key");
        assert_eq!(
            parsed.string_function_projections[0].op,
            StringFunctionOp::Concat
        );
        assert_eq!(
            parsed.string_function_projections[0].source_columns,
            vec!["label".to_string(), "segment".to_string()]
        );
        assert_eq!(parsed.string_function_projections[0].literal_count, 1);
        assert_eq!(
            parsed.string_function_projections[1].op,
            StringFunctionOp::Substr
        );
        assert_eq!(parsed.string_function_projections[1].literal_count, 2);
        assert_eq!(
            parsed.string_function_projections[2].op,
            StringFunctionOp::Left
        );
        assert_eq!(parsed.string_function_projections[2].literal_count, 1);
        assert_eq!(
            parsed.string_function_projections[3].op,
            StringFunctionOp::Right
        );
        assert_eq!(parsed.string_function_projections[3].literal_count, 1);
        assert_eq!(
            parsed.string_function_projections[4].op,
            StringFunctionOp::Replace
        );
        assert_eq!(parsed.string_function_projections[4].literal_count, 2);

        assert!(matches!(
            parsed.predicate,
            ParsedPredicate::StringFunctionCompare {
                op: StringFunctionOp::Concat,
                ref source_columns,
                literal_count: 2,
                value: ScalarValue::Utf8(ref value),
                ..
            } if source_columns == &vec!["label".to_string(), "segment".to_string()]
                && value == "alpha-north"
        ));
    }

    #[test]
    fn parses_composed_string_expression_projection_and_predicate_statement() {
        let parsed = parse_sql_local_source_statement(
            "SELECT id,CONCAT(LOWER(TRIM(label)), '-', UPPER(segment)) AS label_key,LENGTH(REPLACE(TRIM(label), ' ', '')) AS compact_len FROM 'target/input.csv' WHERE CONCAT(LOWER(TRIM(label)), '-', UPPER(segment)) = 'alpha-north' AND LENGTH(REPLACE(TRIM(label), ' ', '')) >= 10 LIMIT 5",
        )
        .expect("composed string expression statement parses");

        assert_eq!(parsed.projections, vec!["id"]);
        assert_eq!(parsed.string_function_projections.len(), 1);
        assert_eq!(parsed.string_function_projections[0].alias, "label_key");
        assert_eq!(
            parsed.string_function_projections[0].source_columns,
            vec!["label".to_string(), "segment".to_string()]
        );
        assert_eq!(parsed.string_function_projections[0].literal_count, 1);
        assert_eq!(parsed.string_length_projections.len(), 1);
        assert_eq!(parsed.string_length_projections[0].alias, "compact_len");
        assert_eq!(
            parsed.string_length_projections[0].source_columns,
            vec!["label".to_string()]
        );
    }

    #[test]
    fn parses_scoped_temporal_extract_projection_statement() {
        let parsed = parse_sql_local_source_statement(
            "SELECT id,DATE_YEAR(CAST(event_date AS date32)) AS event_year,TIMESTAMP_HOUR(CAST(event_ts AS timestamp_micros)) AS event_hour FROM 'target/input.csv' WHERE id >= 1 LIMIT 5",
        )
        .expect("temporal extract projection statement parses");

        assert_eq!(parsed.projections, vec!["id"]);
        assert_eq!(parsed.date_extract_projections.len(), 1);
        assert_eq!(parsed.date_extract_projections[0].alias, "event_year");
        assert_eq!(parsed.date_extract_projections[0].column, "event_date");
        assert_eq!(parsed.date_extract_projections[0].op, DateExtractOp::Year);
        assert_eq!(parsed.timestamp_extract_projections.len(), 1);
        assert_eq!(parsed.timestamp_extract_projections[0].alias, "event_hour");
        assert_eq!(parsed.timestamp_extract_projections[0].column, "event_ts");
        assert_eq!(
            parsed.timestamp_extract_projections[0].op,
            TimestampExtractOp::Hour
        );
    }

    #[test]
    fn null_coalesce_projection_null_fallback_is_admitted() {
        let parsed = parse_sql_local_source_statement(
            "SELECT id,COALESCE(label, NULL) AS label_clean FROM 'target/input.csv' LIMIT 5",
        )
        .expect("NULL coalesce declarations lower to the native expression binder");
        assert_eq!(parsed.generic_expression_projections.len(), 1);
        assert_eq!(
            parsed.generic_expression_projections[0].alias,
            "label_clean"
        );
    }

    #[test]
    fn nullif_projection_null_sentinel_is_admitted() {
        let parsed = parse_sql_local_source_statement(
            "SELECT id,NULLIF(label, NULL) AS label_clean FROM 'target/input.csv' LIMIT 5",
        )
        .expect("NULL sentinel declarations lower to the native expression binder");
        assert_eq!(parsed.generic_expression_projections.len(), 1);
        assert_eq!(
            parsed.generic_expression_projections[0].alias,
            "label_clean"
        );
    }

    #[test]
    fn conditional_projection_null_branch_is_admitted() {
        let parsed = parse_sql_local_source_statement(
            "SELECT id,CASE WHEN amount >= 10 THEN NULL ELSE 'small' END AS size_band FROM 'target/input.csv' LIMIT 5",
        )
        .expect("NULL CASE branch declarations lower to the native expression binder");
        assert_eq!(parsed.generic_expression_projections.len(), 1);
        assert_eq!(parsed.generic_expression_projections[0].alias, "size_band");
    }

    #[test]
    fn conditional_projection_mixed_branch_dtype_is_blocked() {
        let error = parse_sql_local_source_statement(
            "SELECT id,CASE WHEN amount >= 10 THEN 'large' ELSE 0 END AS size_band FROM 'target/input.csv' LIMIT 5",
        )
        .expect_err("mixed CASE branch dtypes are blocked during parsing");

        assert!(
            error
                .to_string()
                .contains("CASE projection THEN/ELSE branches must have matching dtypes"),
            "{error}"
        );
    }

    #[test]
    fn numeric_arithmetic_divide_by_zero_remains_a_runtime_value_check() {
        parse_sql_local_source_statement(
            "SELECT id,amount / 0 AS ratio FROM 'target/input.csv' LIMIT 5",
        )
        .expect("projection syntax does not evaluate rows");
        parse_sql_local_source_statement(
            "SELECT id FROM 'target/input.csv' WHERE amount / 0 > 1 LIMIT 5",
        )
        .expect("predicate syntax does not evaluate rows");
    }

    #[test]
    fn parses_scoped_scalar_aggregate_statement() {
        let parsed = parse_sql_local_source_statement(
            "SELECT count(*),sum(amount),avg(amount),min(amount),max(amount) FROM 'target/input.csv' WHERE amount >= 10 LIMIT 1",
        )
        .expect("aggregate statement parses");

        assert_eq!(parsed.projections, [] as [String; 0]);
        assert_eq!(parsed.aggregates.len(), 5);

        assert_eq!(parsed.aggregates[2].output_name(), "avg_amount");
        assert_eq!(parsed.group_by, [] as [String; 0]);
        assert!(parsed.order_by.is_none());
        assert_eq!(
            parsed.source.local_path().unwrap(),
            Path::new("target/input.csv")
        );
    }

    #[test]
    fn parses_scoped_aggregate_alias_statement() {
        let parsed = parse_sql_local_source_statement(
            "SELECT count(*) AS rows,sum(amount) AS total_amount FROM 'target/input.csv' WHERE amount >= 10 LIMIT 1",
        )
        .expect("aggregate alias statement parses");

        assert_eq!(parsed.projections, [] as [String; 0]);
        assert_eq!(parsed.aggregates.len(), 2);

        assert_eq!(parsed.aggregates[0].output_name(), "rows");
        assert_eq!(parsed.aggregates[0].alias.as_deref(), Some("rows"));

        assert_eq!(parsed.aggregates[1].output_name(), "total_amount");
        assert_eq!(parsed.group_by, [] as [String; 0]);
    }

    #[test]
    fn parses_scoped_count_distinct_aggregate_statement() {
        let parsed = parse_sql_local_source_statement(
            "SELECT region,count(DISTINCT customer_id) AS unique_customers,count(*) AS rows FROM 'target/input.csv' WHERE amount >= 10 GROUP BY region LIMIT 10",
        )
        .expect("count distinct aggregate statement parses");

        assert_eq!(parsed.projections, vec!["region"]);
        assert_eq!(parsed.group_by, vec!["region"]);
        assert_eq!(parsed.aggregates.len(), 2);

        assert_eq!(parsed.aggregates[0].output_name(), "unique_customers");
        assert!(parsed.aggregates[0].distinct);
    }

    #[test]
    fn count_distinct_unsupported_shapes_are_blocked() {
        let sum_distinct = parse_sql_local_source_statement(
            "SELECT sum(DISTINCT amount) FROM 'target/input.csv' LIMIT 1",
        )
        .expect_err("SUM DISTINCT is blocked");
        assert!(
            sum_distinct
                .to_string()
                .contains("COUNT(DISTINCT <argument>) only"),
            "{sum_distinct}"
        );

        let count_distinct_star = parse_sql_local_source_statement(
            "SELECT count(DISTINCT *) FROM 'target/input.csv' LIMIT 1",
        )
        .expect_err("COUNT DISTINCT star is blocked");
        assert!(
            count_distinct_star
                .to_string()
                .contains("COUNT(DISTINCT *) is not admitted"),
            "{count_distinct_star}"
        );
    }

    #[test]
    fn parses_scoped_group_by_aggregate_statement() {
        let parsed = parse_sql_local_source_statement(
            "SELECT region,count(*),sum(amount) FROM 'target/input.csv' WHERE amount >= 0 GROUP BY region LIMIT 10",
        )
        .expect("group-by aggregate statement parses");

        assert_eq!(parsed.projections, vec!["region"]);
        assert_eq!(parsed.group_by, vec!["region"]);
        assert!(parsed.order_by.is_none());
        assert_eq!(parsed.aggregates.len(), 2);
    }

    #[test]
    fn parses_scoped_aggregate_having_statement() {
        let parsed = parse_sql_local_source_statement(
            "SELECT region,count(*) AS rows,sum(amount) AS total_amount FROM 'target/input.csv' WHERE amount >= 0 GROUP BY region HAVING total_amount >= 10 AND rows >= 2 ORDER BY total_amount DESC LIMIT 10",
        )
        .expect("aggregate HAVING statement parses");
        assert_eq!(parsed.group_by, vec!["region"]);
        assert_eq!(parsed.aggregates.len(), 2);

        assert_eq!(parsed.having.columns(), vec!["total_amount", "rows"]);
    }

    #[test]
    fn parses_scoped_having_exists_subquery_statement() {
        let parsed = parse_sql_local_source_statement(
            "SELECT region,count(*) AS rows,sum(amount) AS total FROM 'target/input.csv' GROUP BY region HAVING EXISTS (SELECT * FROM 'target/allowed.csv' WHERE active IS TRUE ORDER BY score DESC LIMIT 1) ORDER BY total DESC LIMIT 10",
        )
        .expect("HAVING EXISTS subquery statement parses");

        assert_eq!(parsed.group_by, vec!["region"]);
        assert_eq!(parsed.aggregates.len(), 2);
    }

    #[test]
    fn parses_scoped_having_quantified_subquery_statement() {
        let parsed = parse_sql_local_source_statement(
            "SELECT region,count(*) AS rows,sum(amount) AS total FROM 'target/input.csv' GROUP BY region HAVING total > ALL (SELECT threshold FROM 'target/thresholds.csv' WHERE active IS TRUE ORDER BY score DESC LIMIT 2) ORDER BY total DESC LIMIT 10",
        )
        .expect("HAVING quantified subquery statement parses");

        assert_eq!(parsed.group_by, vec!["region"]);
        assert_eq!(parsed.aggregates.len(), 2);
    }

    #[test]
    fn parses_scoped_having_unprojected_aggregate_functions() {
        let parsed = parse_sql_local_source_statement(
            "SELECT region,count(*) AS rows FROM 'target/input.csv' WHERE amount >= 0 GROUP BY region HAVING sum(amount) >= 10 AND count(*) >= 2 AND count(DISTINCT id) >= 2 LIMIT 10",
        )
        .expect("aggregate HAVING statement with unprojected aggregate functions parses");
        assert_eq!(parsed.group_by, vec!["region"]);
        assert_eq!(parsed.aggregates.len(), 1);

        assert_eq!(parsed.aggregates[0].output_name(), "rows");
        assert_eq!(parsed.having_aggregates.len(), 3);
    }

    #[test]
    fn having_hidden_aggregate_aliases_do_not_collide_with_visible_outputs() {
        let parsed = parse_sql_local_source_statement(
            "SELECT region,count(*) AS __having_sum_amount_1 FROM 'target/input.csv' GROUP BY region HAVING sum(amount) >= 10 LIMIT 10",
        )
        .expect("statement parses");

        assert_eq!(parsed.aggregates[0].output_name(), "__having_sum_amount_1");
        assert_eq!(parsed.having_aggregates.len(), 1);
        assert_eq!(
            parsed.having_aggregates[0].output_name(),
            "__having_sum_amount_2"
        );
    }

    #[test]
    fn parses_scoped_multi_key_group_by_aggregate_statement() {
        let parsed = parse_sql_local_source_statement(
            "SELECT region,segment,count(*),sum(amount) FROM 'target/input.csv' WHERE amount >= 0 GROUP BY region,segment LIMIT 10",
        )
        .expect("multi-key group-by aggregate statement parses");

        assert_eq!(parsed.projections, vec!["region", "segment"]);
        assert_eq!(parsed.group_by, vec!["region", "segment"]);
        assert!(parsed.order_by.is_none());
        assert_eq!(parsed.aggregates.len(), 2);
    }

    #[test]
    fn parses_scoped_order_by_topn_statement() {
        let parsed = parse_sql_local_source_statement(
            "SELECT id,label FROM 'target/input.csv' WHERE amount >= 0 ORDER BY amount DESC LIMIT 3",
        )
        .expect("order-by statement parses");

        assert_eq!(parsed.projections, vec!["id", "label"]);
        assert_eq!(parsed.aggregates, [] as [ParsedAggregate; 0]);
        assert_eq!(parsed.group_by, [] as [String; 0]);
        let order_by = parsed.order_by.as_ref().expect("order by parsed");

        assert_eq!(order_by.keys[0].column, "amount");
        assert_eq!(order_by.keys[0].direction, SortDirection::Desc);
        assert_eq!(parsed.limit, 3);
    }

    #[test]
    fn parses_scoped_explicit_null_order_by_topn_statement() {
        let parsed = parse_sql_local_source_statement(
            "SELECT id,label FROM 'target/input.csv' ORDER BY amount ASC NULLS FIRST,id DESC NULLS LAST LIMIT 4",
        )
        .expect("explicit null-order statement parses");

        let order_by = parsed.order_by.as_ref().expect("order by parsed");

        assert_eq!(
            order_by.keys[0].null_ordering,
            Some(SortNullOrdering::First)
        );
        assert_eq!(order_by.keys[1].null_ordering, Some(SortNullOrdering::Last));
    }

    #[test]
    fn parses_scoped_multi_key_order_by_topn_statement() {
        let parsed = parse_sql_local_source_statement(
            "SELECT id,label FROM 'target/input.csv' WHERE amount >= 0 ORDER BY amount DESC,id ASC LIMIT 3",
        )
        .expect("multi-key order-by statement parses");

        let order_by = parsed.order_by.as_ref().expect("order by parsed");
        assert_eq!(
            order_by
                .keys
                .iter()
                .map(|key| (key.column.as_str(), key.direction))
                .collect::<Vec<_>>(),
            [("amount", SortDirection::Desc), ("id", SortDirection::Asc)]
        );

        assert_eq!(parsed.limit, 3);
    }

    #[test]
    fn csv_scalar_inexact_exponent_decimals_fall_through_to_float_parser() {
        match parse_csv_scalar("1e-1") {
            ScalarValue::Float64(value) => assert!((value - 0.1).abs() < f64::EPSILON),
            other => panic!("expected 1e-1 to parse as Float64, got {other:?}"),
        }
        match parse_csv_scalar("1.5e0") {
            ScalarValue::Float64(value) => assert!((value - 1.5).abs() < f64::EPSILON),
            other => panic!("expected 1.5e0 to parse as Float64, got {other:?}"),
        }
        match parse_csv_scalar("2e0") {
            ScalarValue::Int64(value) => assert_eq!(value, 2),
            other => panic!("expected exact exponent integer to parse as Int64, got {other:?}"),
        }
    }

    #[test]
    fn parses_scoped_cast_predicate_statement() {
        let parsed = parse_sql_local_source_statement(
            "SELECT id,amount FROM 'target/input.jsonl' WHERE CAST(amount AS int64) >= 10 LIMIT 5",
        )
        .expect("cast predicate statement parses");

        assert_eq!(parsed.projections, vec!["id", "amount"]);
        assert_eq!(
            parsed.source.local_path().unwrap(),
            Path::new("target/input.jsonl")
        );
        assert!(matches!(
            parsed.predicate,
            ParsedPredicate::CastCompare {
                ref column,
                target_dtype: LogicalDType::Int64,
                mode: CastMode::Strict,
                op: ComparisonOp::GtEq,
                value: ScalarValue::Int64(10),
                ..
            } if column == "amount"
        ));
    }

    #[test]
    fn parses_parenthesized_cast_predicate_expression_statement() {
        let parsed = parse_sql_local_source_statement(
            "SELECT id,CAST(raw_amount AS float64) AS amount_float FROM 'target/input.csv' WHERE (CAST(raw_amount AS float64)) >= 0 LIMIT 5",
        )
        .expect("parenthesized cast predicate expression statement parses");

        assert!(matches!(
            parsed.predicate,
            ParsedPredicate::CastCompare {
                ref column,
                target_dtype: LogicalDType::Float64,
                mode: CastMode::Strict,
                op: ComparisonOp::GtEq,
                value: ScalarValue::Int64(0),
                ..
            } if column == "raw_amount"
        ));
    }

    #[test]
    fn parses_scoped_try_cast_predicate_statement() {
        let parsed = parse_sql_local_source_statement(
            "SELECT id,raw_amount FROM 'target/input.csv' WHERE TRY_CAST(raw_amount AS int64) >= 10 LIMIT 5",
        )
        .expect("try_cast predicate statement parses");

        assert!(matches!(
            parsed.predicate,
            ParsedPredicate::CastCompare {
                ref column,
                target_dtype: LogicalDType::Int64,
                mode: CastMode::Try,
                op: ComparisonOp::GtEq,
                value: ScalarValue::Int64(10),
                ..
            } if column == "raw_amount"
        ));
    }

    #[test]
    fn parses_scoped_cast_date_literal_predicate_statement() {
        let parsed = parse_sql_local_source_statement(
            "SELECT id,event_date FROM 'target/input.jsonl' WHERE CAST(event_date AS date32) >= DATE '2026-05-19' LIMIT 5",
        )
        .expect("cast date literal predicate statement parses");

        assert!(matches!(
            parsed.predicate,
            ParsedPredicate::CastCompare {
                ref column,
                target_dtype: LogicalDType::Date32,
                mode: CastMode::Strict,
                op: ComparisonOp::GtEq,
                value: ScalarValue::Date32(_),
                ..
            } if column == "event_date"
        ));
    }

    #[test]
    fn parses_scoped_binary_cast_predicate_statement() {
        let parsed = parse_sql_local_source_statement(
            "SELECT id,label_prefix,label_suffix FROM 'target/input.csv' WHERE CAST(CONCAT(label_prefix,label_suffix) AS binary) = BINARY 'alpha' LIMIT 5",
        )
        .expect("binary cast predicate statement parses");

        assert!(matches!(
            parsed.predicate,
            ParsedPredicate::CastCompare {
                ref column,
                ref source_columns,
                target_dtype: LogicalDType::Binary,
                mode: CastMode::Strict,
                op: ComparisonOp::Eq,
                value: ScalarValue::Binary(ref value),
                ..
            } if column == "label_prefix"
                && source_columns == &vec!["label_prefix".to_string(), "label_suffix".to_string()]
                && value == b"alpha"
        ));
    }

    #[test]
    fn parses_scoped_decimal_cast_projection_and_predicate_statement() {
        let parsed = parse_sql_local_source_statement(
            "SELECT id,CAST(amount AS decimal128(10,2)) AS amount_decimal,TRY_CAST(raw_amount AS numeric(10,2)) AS raw_decimal FROM 'target/input.csv' WHERE CAST(amount AS decimal(10,2)) >= 10.00 LIMIT 5",
        )
        .expect("decimal cast projection and predicate statement parses");

        assert_eq!(parsed.projections, vec!["id"]);
        assert_eq!(parsed.cast_projections.len(), 2);
        assert_eq!(parsed.cast_projections[0].alias, "amount_decimal");
        assert_eq!(
            parsed.cast_projections[0].target_dtype,
            decimal128_dtype(10, 2)
        );
        assert_eq!(parsed.cast_projections[1].alias, "raw_decimal");
        assert_eq!(
            parsed.cast_projections[1].target_dtype,
            decimal128_dtype(10, 2)
        );
        assert!(matches!(
            parsed.predicate,
            ParsedPredicate::CastCompare {
                ref column,
                ref target_dtype,
                mode: CastMode::Strict,
                op: ComparisonOp::GtEq,
                value: ScalarValue::Decimal128 {
                    value: 1000,
                    precision: 10,
                    scale: 2
                },
                ..
            } if column == "amount" && target_dtype == &decimal128_dtype(10, 2)
        ));
    }

    #[test]
    fn parses_scoped_binary_cast_ordering_predicate_statement() {
        let parsed = parse_sql_local_source_statement(
            "SELECT id,label FROM 'target/input.csv' WHERE CAST(LOWER(TRIM(label)) AS binary) > BINARY 'alpha' LIMIT 5",
        )
        .expect("binary cast ordering predicate parses");

        assert!(matches!(
            parsed.predicate,
            ParsedPredicate::CastCompare {
                ref column,
                target_dtype: LogicalDType::Binary,
                mode: CastMode::Strict,
                op: ComparisonOp::Gt,
                value: ScalarValue::Binary(ref value),
                ..
            } if column == "label" && value == b"alpha"
        ));
    }

    #[test]
    fn parses_scoped_string_length_predicate_statement() {
        let parsed = parse_sql_local_source_statement(
            "SELECT id,label FROM 'target/input.csv' WHERE LENGTH(label) >= 4 LIMIT 5",
        )
        .expect("string length predicate statement parses");

        assert_eq!(parsed.projections, vec!["id", "label"]);
        assert!(matches!(
            parsed.predicate,
            ParsedPredicate::StringLengthCompare {
                ref source_columns,
                comparison: ComparisonOp::GtEq,
                value: ScalarValue::Int64(4),
                ..
            } if source_columns == &vec!["label".to_string()]
        ));
    }

    #[test]
    fn parses_scoped_numeric_abs_predicate_statement() {
        let parsed = parse_sql_local_source_statement(
            "SELECT id,amount FROM 'target/input.csv' WHERE ABS(amount) >= 4 LIMIT 5",
        )
        .expect("numeric abs predicate statement parses");

        assert_eq!(parsed.projections, vec!["id", "amount"]);
        assert!(matches!(
            parsed.predicate,
            ParsedPredicate::NumericAbsCompare {
                ref column,
                comparison: ComparisonOp::GtEq,
                value: ScalarValue::Int64(4)
            } if column == "amount"
        ));
    }

    #[test]
    fn parses_scoped_numeric_rounding_predicate_statement() {
        let parsed = parse_sql_local_source_statement(
            "SELECT id,amount FROM 'target/input.csv' WHERE FLOOR(amount) >= 4 LIMIT 5",
        )
        .expect("numeric rounding predicate statement parses");

        assert_eq!(parsed.projections, vec!["id", "amount"]);
        assert!(matches!(
            parsed.predicate,
            ParsedPredicate::NumericRoundingCompare {
                ref column,
                op: NumericRoundingOp::Floor,
                comparison: ComparisonOp::GtEq,
                value: ScalarValue::Int64(4)
            } if column == "amount"
        ));
    }

    #[test]
    fn parses_generic_expression_predicate_statement() {
        let parsed = parse_sql_local_source_statement(
            "SELECT id,amount FROM 'target/input.csv' WHERE (amount + tax) * 2 >= 40 AND ABS(amount - tax) > 8 LIMIT 5",
        )
        .expect("generic expression predicate statement parses");

        assert_eq!(parsed.projections, vec!["id", "amount"]);

        assert!(parsed.predicate.uses_generic_expression());

        assert_eq!(
            parsed.predicate.columns(),
            vec!["amount", "tax", "amount", "tax"]
        );
    }

    #[test]
    fn parses_scoped_in_predicate_statement() {
        let parsed = parse_sql_local_source_statement(
            "SELECT id,label FROM 'target/input.csv' WHERE label IN ('alpha','gamma') LIMIT 5",
        )
        .expect("IN predicate statement parses");

        assert_eq!(parsed.projections, vec!["id", "label"]);
        assert_eq!(
            parsed.source.local_path().unwrap(),
            Path::new("target/input.csv")
        );
        assert!(matches!(
            parsed.predicate,
            ParsedPredicate::InList {
                ref column,
                ref values,
            } if column == "label"
                && values == &vec![
                    ScalarValue::Utf8("alpha".to_string()),
                    ScalarValue::Utf8("gamma".to_string()),
                ]
        ));

        assert_eq!(parsed.predicate.columns(), vec!["label"]);
    }

    #[test]
    fn parses_scoped_not_in_predicate_statement() {
        let parsed = parse_sql_local_source_statement(
            "SELECT id,label FROM 'target/input.csv' WHERE label NOT IN ('alpha','gamma') LIMIT 5",
        )
        .expect("NOT IN predicate statement parses");

        assert!(matches!(
            parsed.predicate,
            ParsedPredicate::Not { ref inner }
                if matches!(
                    inner.as_ref(),
                    ParsedPredicate::InList {
                        column,
                        values,
                    } if column == "label"
                        && values == &vec![
                            ScalarValue::Utf8("alpha".to_string()),
                            ScalarValue::Utf8("gamma".to_string()),
                        ]
                )
        ));

        assert_eq!(parsed.predicate.columns(), vec!["label"]);
    }

    #[test]
    fn parses_scoped_row_value_in_predicate_statement() {
        let parsed = parse_sql_local_source_statement(
            "SELECT id,label FROM 'target/input.csv' WHERE (id,label) IN ((1,'alpha'),(3,'gamma'),(5,NULL)) LIMIT 5",
        )
        .expect("row-value IN predicate statement parses");

        assert_eq!(parsed.projections, vec!["id", "label"]);
        assert!(matches!(
            parsed.predicate,
            ParsedPredicate::RowValueInList {
                ref columns,
                ref tuples,
            } if columns == &vec!["id".to_string(), "label".to_string()]
                && tuples == &vec![
                    vec![ScalarValue::Int64(1), ScalarValue::Utf8("alpha".to_string())],
                    vec![ScalarValue::Int64(3), ScalarValue::Utf8("gamma".to_string())],
                    vec![ScalarValue::Int64(5), ScalarValue::Null],
                ]
        ));

        assert_eq!(parsed.predicate.columns(), vec!["id", "label"]);
    }

    #[test]
    fn parses_scoped_row_value_not_in_predicate_statement() {
        let parsed = parse_sql_local_source_statement(
            "SELECT id,label FROM 'target/input.csv' WHERE (id,label) NOT IN ((1,'alpha'),(3,'gamma')) LIMIT 5",
        )
        .expect("row-value NOT IN predicate statement parses");

        assert!(matches!(
            parsed.predicate,
            ParsedPredicate::Not { ref inner }
                if matches!(
                    inner.as_ref(),
                    ParsedPredicate::RowValueInList {
                        columns,
                        tuples,
                    } if columns == &vec!["id".to_string(), "label".to_string()]
                        && tuples.len() == 2
                )
        ));
    }

    #[test]
    fn parses_scoped_date_in_predicate_statement() {
        let parsed = parse_sql_local_source_statement(
            "SELECT id,event_date FROM 'target/input.csv' WHERE event_date IN (DATE '2026-05-18', DATE '2026-05-20') LIMIT 5",
        )
        .expect("DATE IN predicate statement parses");

        assert!(matches!(
            parsed.predicate,
            ParsedPredicate::InList {
                ref column,
                ref values,
            } if column == "event_date"
                && values.len() == 2
                && values.iter().all(|value| matches!(value, ScalarValue::Date32(_)))
        ));
    }

    #[test]
    fn parses_scoped_in_subquery_predicate_statement() {
        let parsed = parse_sql_local_source_statement(
            "SELECT id,label FROM 'target/input.csv' WHERE id IN (SELECT id FROM 'target/allowed.csv') LIMIT 5",
        )
        .expect("IN subquery predicate statement parses");

        assert_eq!(parsed.projections, vec!["id", "label"]);
        assert!(matches!(
            parsed.predicate,
            ParsedPredicate::InSubquery {
                ref column,
                ref subquery
            } if column == "id"
                && subquery.source_column == "id"
                && subquery.source.local_path().unwrap() == Path::new("target/allowed.csv")
                && subquery.values.is_empty()
        ));
    }

    #[test]
    fn parses_filtered_ordered_limited_in_subquery_predicate_statement() {
        let parsed = parse_sql_local_source_statement(
            "SELECT id,label FROM 'target/input.csv' WHERE id IN (SELECT id FROM 'target/allowed.csv' WHERE active IS TRUE ORDER BY score DESC LIMIT 2) LIMIT 5",
        )
        .expect("filtered ordered IN subquery predicate statement parses");
        let ParsedPredicate::InSubquery { subquery, .. } = &parsed.predicate else {
            panic!("expected membership subquery");
        };
        assert_eq!(subquery.limit, Some(2));
        assert!(!matches!(*subquery.predicate, ParsedPredicate::All));
        assert_eq!(subquery.order_by.as_ref().unwrap().keys[0].column, "score");
    }

    #[test]
    fn parses_nested_local_in_subquery_predicate_statement() {
        let parsed = parse_sql_local_source_statement(
            "SELECT id,label FROM 'target/input.csv' WHERE id IN (SELECT allowed_id FROM 'target/allowed.csv' WHERE allowed_id IN (SELECT id FROM 'target/nested.csv' WHERE active IS TRUE ORDER BY score DESC LIMIT 2) ORDER BY priority DESC LIMIT 3) LIMIT 5",
        )
        .expect("nested local IN subquery predicate statement parses");

        assert!(matches!(
            parsed.predicate,
            ParsedPredicate::InSubquery {
                ref column,
                ref subquery
            } if column == "id"
                && subquery.source_column == "allowed_id"
                && subquery.source.local_path().unwrap() == Path::new("target/allowed.csv")
                && matches!(
                    subquery.predicate.as_ref(),
                    ParsedPredicate::InSubquery {
                        column,
                        subquery
                    } if column == "allowed_id"
                        && subquery.source_column == "id"
                        && subquery.source.local_path().unwrap() == Path::new("target/nested.csv")
                )
        ));
    }

    #[test]
    fn parses_scoped_quantified_subquery_predicate_statement() {
        let parsed = parse_sql_local_source_statement(
            "SELECT id,label FROM 'target/input.csv' WHERE amount >= ALL (SELECT threshold FROM 'target/allowed.csv' WHERE active IS TRUE ORDER BY score DESC LIMIT 2) LIMIT 5",
        )
        .expect("quantified subquery predicate statement parses");

        assert_eq!(parsed.projections, vec!["id", "label"]);
    }

    #[test]
    fn parses_scoped_exists_subquery_predicate_statement() {
        let parsed = parse_sql_local_source_statement(
            "SELECT id,label FROM 'target/input.csv' WHERE EXISTS (SELECT * FROM 'target/allowed.csv' WHERE active IS TRUE ORDER BY score DESC LIMIT 1) LIMIT 5",
        )
        .expect("EXISTS subquery predicate statement parses");

        assert_eq!(parsed.projections, vec!["id", "label"]);
    }

    #[test]
    fn parses_scoped_not_exists_subquery_predicate_statement() {
        let parsed = parse_sql_local_source_statement(
            "SELECT id,label FROM 'target/input.csv' WHERE NOT EXISTS (SELECT 1 FROM 'target/blocked.csv' WHERE active IS TRUE LIMIT 1) LIMIT 5",
        )
        .expect("NOT EXISTS subquery predicate statement parses");

        assert!(matches!(
            parsed.predicate,
            ParsedPredicate::Not { ref inner }
                if matches!(
                    inner.as_ref(),
                    ParsedPredicate::ExistsSubquery { subquery }
                        if subquery.projection_kind == ParsedExistsSubqueryProjectionKind::Literal
                            && subquery.source.local_path().unwrap() == Path::new("target/blocked.csv")
                            && subquery.limit == Some(1)
                )
        ));
    }

    #[test]
    fn parses_scoped_row_value_in_subquery_predicate_statement() {
        let parsed = parse_sql_local_source_statement(
            "SELECT id,label FROM 'target/input.csv' WHERE (id,label) IN (SELECT allowed_id,allowed_label FROM 'target/allowed.csv' WHERE active IS TRUE ORDER BY score DESC LIMIT 3) LIMIT 5",
        )
        .expect("row-value IN subquery predicate statement parses");

        assert_eq!(parsed.projections, vec!["id", "label"]);
        assert!(matches!(
            parsed.predicate,
            ParsedPredicate::RowValueInSubquery {
                ref columns,
                ref subquery
            } if columns == &vec!["id".to_string(), "label".to_string()]
                && subquery.source_columns
                    == vec!["allowed_id".to_string(), "allowed_label".to_string()]
                && subquery.source.local_path().unwrap() == Path::new("target/allowed.csv")
                && subquery.tuples.is_empty()
        ));
    }

    #[test]
    fn in_subquery_admits_source_qualifier_from_file_stem() {
        let parsed = parse_sql_local_source_statement(
            "SELECT id FROM 'target/input.csv' WHERE id IN (SELECT allowed.id FROM 'target/allowed.csv' WHERE allowed.active IS TRUE ORDER BY allowed.id DESC LIMIT 5) LIMIT 5",
        )
        .expect("file-stem source qualifier is admitted");

        match parsed.predicate {
            ParsedPredicate::InSubquery { subquery, .. } => {
                assert_eq!(subquery.source_column, "id");
                assert!(matches!(
                    subquery.predicate.as_ref(),
                    ParsedPredicate::BooleanPredicate { column, expected: true, .. }
                    if column == "active"
                ));
                let order_by = subquery.order_by.expect("source-qualified order by");
                assert_eq!(order_by.keys[0].column, "id");
                assert_eq!(order_by.keys[0].direction, SortDirection::Desc);
            }
            other => panic!("expected source-qualified IN subquery, got {other:?}"),
        }
    }

    #[test]
    fn in_subquery_rejects_reserved_outer_file_stem_qualifier_without_fallback() {
        let error = parse_sql_local_source_statement(
            "SELECT id FROM 'target/input.csv' WHERE id IN (SELECT outer.id FROM 'target/outer.csv' WHERE outer.active IS TRUE LIMIT 5) LIMIT 5",
        )
        .expect_err("reserved inferred outer qualifier remains blocked");

        assert!(
            error
                .to_string()
                .contains("inferred source qualifier 'outer' is reserved"),
            "got {error}"
        );
        assert!(error.to_string().contains("external_engine_invoked=false"));
    }

    #[test]
    fn in_predicate_blocks_unadmitted_literal_lists_without_fallback() {
        let empty_error = parse_sql_local_source_statement(
            "SELECT id FROM 'target/input.csv' WHERE label IN () LIMIT 5",
        )
        .expect_err("empty IN list remains blocked");
        assert!(
            empty_error
                .to_string()
                .contains("IN predicates require at least one literal value")
        );
        assert!(
            empty_error
                .to_string()
                .contains("external_engine_invoked=false")
        );

        let null_admitted = parse_sql_local_source_statement(
            "SELECT id FROM 'target/input.csv' WHERE label IN ('alpha', NULL) LIMIT 5",
        )
        .expect("NULL IN list values use SQL three-valued semantics");

        assert!(matches!(
            null_admitted.predicate,
            ParsedPredicate::InList {
                ref column,
                ref values,
            } if column == "label"
                && values == &vec![
                    ScalarValue::Utf8("alpha".to_string()),
                    ScalarValue::Null,
                ]
        ));

        let mixed_date_error = parse_sql_local_source_statement(
            "SELECT id FROM 'target/input.csv' WHERE label IN (DATE '2026-05-19', 'alpha') LIMIT 5",
        )
        .expect_err("mixed DATE/non-DATE IN lists remain blocked");
        assert!(
            mixed_date_error
                .to_string()
                .contains("IN predicates do not admit mixed DATE and non-DATE literal lists")
        );
        assert!(
            mixed_date_error
                .to_string()
                .contains("external_engine_invoked=false")
        );

        let trailing_error = parse_sql_local_source_statement(
            "SELECT id FROM 'target/input.csv' WHERE label IN ('alpha',) LIMIT 5",
        )
        .expect_err("trailing empty IN list values remain blocked");
        assert!(
            trailing_error
                .to_string()
                .contains("IN predicates require non-empty literal values")
        );
        assert!(
            trailing_error
                .to_string()
                .contains("external_engine_invoked=false")
        );
    }

    #[test]
    fn in_subquery_blocks_unadmitted_advanced_shapes_without_fallback() {
        parse_sql_local_source_statement(
            "SELECT id FROM 'target/input.csv' WHERE id > ANY (SELECT id FROM 'target/allowed.csv' WHERE id IN (SELECT id FROM 'target/nested.csv')) LIMIT 5",
        )
        .expect("quantified nested IN subquery predicate is admitted");

        for (statement, expected) in [
            (
                "SELECT id FROM 'target/input.csv' WHERE id IN (SELECT id,label FROM 'target/allowed.csv') LIMIT 5",
                "multi-column IN subqueries require row-value source columns",
            ),
            (
                "SELECT id FROM 'target/input.csv' WHERE (id,label) IN (SELECT id FROM 'target/allowed.csv') LIMIT 5",
                "row-value IN subquery selected-column arity must match the source column count",
            ),
            (
                "SELECT id FROM 'target/input.csv' WHERE id IN (SELECT id FROM 'target/allowed.csv' AS allowed WHERE blocked.id = id) LIMIT 5",
                "qualified IN subquery predicates admit only outer.<column> references or the subquery source qualifier",
            ),
            (
                "SELECT id FROM 'target/input.csv' WHERE id IN (SELECT id FROM 'target/allowed.csv' GROUP BY id HAVING outer.amount > 10 LIMIT 5) LIMIT 5",
                "correlated IN subquery predicates admit outer.<column> references only in column-to-column comparisons",
            ),
        ] {
            let error = parse_sql_local_source_statement(statement)
                .expect_err("unsupported advanced subquery shape remains blocked");
            assert!(
                error.to_string().contains(expected),
                "error {error:?} did not contain {expected:?}"
            );
            assert!(error.to_string().contains("external_engine_invoked=false"));
        }
    }

    #[test]
    fn correlated_subquery_outer_references_outside_column_comparisons_block_without_fallback() {
        for (statement, expected) in [
            (
                "SELECT id FROM 'target/input.csv' WHERE id IN (SELECT id FROM 'target/allowed.csv' WHERE outer.amount > 10) LIMIT 5",
                "correlated IN subquery predicates admit outer.<column> references only in column-to-column comparisons",
            ),
            (
                "SELECT id FROM 'target/input.csv' WHERE id IN (SELECT id FROM 'target/allowed.csv' WHERE outer.id = outer.amount) LIMIT 5",
                "correlated IN subquery predicates require exactly one outer.<column> reference per column comparison",
            ),
            (
                "SELECT id FROM 'target/input.csv' WHERE EXISTS (SELECT * FROM 'target/allowed.csv' WHERE outer.amount IS NOT NULL LIMIT 1) LIMIT 5",
                "correlated EXISTS subquery predicates admit outer.<column> references only in column-to-column comparisons",
            ),
            (
                "SELECT id FROM 'target/input.csv' WHERE (id,label) IN (SELECT id,label FROM 'target/allowed.csv' WHERE outer.amount > 10) LIMIT 5",
                "correlated IN subquery predicates admit outer.<column> references only in column-to-column comparisons",
            ),
            (
                "SELECT id FROM 'target/input.csv' WHERE amount > ALL (SELECT min_amount FROM 'target/thresholds.csv' WHERE outer.amount > 10 LIMIT 5) LIMIT 5",
                "correlated IN subquery predicates admit outer.<column> references only in column-to-column comparisons",
            ),
            (
                "SELECT id FROM 'target/input.csv' WHERE id IN (SELECT id FROM 'target/grouped.csv' GROUP BY id HAVING outer.amount > 10 LIMIT 5) LIMIT 5",
                "correlated IN subquery predicates admit outer.<column> references only in column-to-column comparisons",
            ),
        ] {
            let error = parse_sql_local_source_statement(statement).expect_err(
                "outer correlation outside admitted column comparisons remains blocked",
            );
            assert!(
                error.to_string().contains(expected),
                "error {error:?} did not contain {expected:?}"
            );
            assert!(error.to_string().contains("external_engine_invoked=false"));
        }
    }

    #[test]
    fn row_value_in_predicate_blocks_malformed_literal_tuples_without_fallback() {
        for (statement, expected) in [
            (
                "SELECT id FROM 'target/input.csv' WHERE (id,label) IN () LIMIT 5",
                "row-value IN predicates require at least one literal tuple",
            ),
            (
                "SELECT id FROM 'target/input.csv' WHERE (id,label) IN ((1,'alpha'),) LIMIT 5",
                "row-value IN predicates require non-empty literal tuples",
            ),
            (
                "SELECT id FROM 'target/input.csv' WHERE (id,label) IN ((1,'alpha',10)) LIMIT 5",
                "row-value IN literal tuple arity must match the source column count",
            ),
            (
                "SELECT id FROM 'target/input.csv' WHERE (id,label) IN (1,'alpha') LIMIT 5",
                "row-value IN literal values must be parenthesized tuples",
            ),
            (
                "SELECT id FROM 'target/input.csv' WHERE (event_date,label) IN ((DATE '2026-05-19','alpha'),('2026-05-20','beta')) LIMIT 5",
                "row-value IN predicates do not admit mixed DATE and non-DATE literals",
            ),
        ] {
            let error = parse_sql_local_source_statement(statement)
                .expect_err("malformed row-value IN shape remains blocked");
            assert!(
                error.to_string().contains(expected),
                "error {error:?} did not contain {expected:?}"
            );
            assert!(error.to_string().contains("external_engine_invoked=false"));
        }
    }

    #[test]
    fn parses_scoped_logical_and_predicate_statement() {
        let parsed = parse_sql_local_source_statement(
            "SELECT id,label FROM 'target/input.csv' WHERE amount >= 10 AND label LIKE '%ta' LIMIT 5",
        )
        .expect("logical AND statement parses");

        assert_eq!(parsed.predicate.columns(), vec!["amount", "label"]);
        assert!(matches!(
            parsed.predicate,
            ParsedPredicate::Logical {
                op: LogicalPredicateOp::And,
                ..
            }
        ));
    }

    #[test]
    fn parses_scoped_logical_or_predicate_statement() {
        let parsed = parse_sql_local_source_statement(
            "SELECT id,label FROM 'target/input.csv' WHERE amount >= 10 OR label LIKE '%ta' LIMIT 5",
        )
        .expect("logical OR statement parses");

        assert_eq!(parsed.predicate.columns(), vec!["amount", "label"]);
        assert!(matches!(
            parsed.predicate,
            ParsedPredicate::Logical {
                op: LogicalPredicateOp::Or,
                ..
            }
        ));
    }

    #[test]
    fn logical_or_preserves_and_precedence_without_fallback() {
        let parsed = parse_sql_local_source_statement(
            "SELECT id,label FROM 'target/input.csv' WHERE id >= 1 OR amount >= 10 AND label LIKE '%ta' LIMIT 5",
        )
        .expect("logical OR/AND statement parses");

        assert_eq!(parsed.predicate.columns(), vec!["id", "amount", "label"]);
        assert!(matches!(
            parsed.predicate,
            ParsedPredicate::Logical {
                op: LogicalPredicateOp::Or,
                ..
            }
        ));
    }

    #[test]
    fn parses_scoped_logical_not_predicate_statement() {
        let parsed = parse_sql_local_source_statement(
            "SELECT id,label FROM 'target/input.csv' WHERE NOT label LIKE '%ta' LIMIT 5",
        )
        .expect("logical NOT statement parses");

        assert_eq!(parsed.predicate.columns(), vec!["label"]);
        assert!(matches!(parsed.predicate, ParsedPredicate::Not { .. }));
    }

    #[test]
    fn logical_not_preserves_or_precedence_without_fallback() {
        let parsed = parse_sql_local_source_statement(
            "SELECT id,label FROM 'target/input.csv' WHERE NOT id >= 1 OR amount >= 10 LIMIT 5",
        )
        .expect("logical NOT/OR statement parses");

        assert_eq!(parsed.predicate.columns(), vec!["id", "amount"]);
        assert!(matches!(
            parsed.predicate,
            ParsedPredicate::Logical {
                op: LogicalPredicateOp::Or,
                ..
            }
        ));
    }

    #[test]
    fn parses_parenthesized_scoped_logical_predicate_statement() {
        let parsed = parse_sql_local_source_statement(
            "SELECT id,label FROM 'target/input.csv' WHERE amount >= 10 AND (label LIKE '%ta' OR label LIKE 'gam%') LIMIT 5",
        )
        .expect("parenthesized logical statement parses");

        assert_eq!(parsed.predicate.columns(), vec!["amount", "label", "label"]);
        assert!(matches!(
            parsed.predicate,
            ParsedPredicate::Logical {
                op: LogicalPredicateOp::And,
                right,
                ..
            } if matches!(
                *right,
                ParsedPredicate::Logical {
                    op: LogicalPredicateOp::Or,
                    ..
                }
            )
        ));
    }

    #[test]
    fn parenthesized_logical_predicates_override_default_precedence_without_fallback() {
        let parsed = parse_sql_local_source_statement(
            "SELECT id,label FROM 'target/input.csv' WHERE (id >= 1 OR amount >= 10) AND label LIKE '%ta' LIMIT 5",
        )
        .expect("parenthesized logical statement parses");

        assert_eq!(parsed.predicate.columns(), vec!["id", "amount", "label"]);
        assert!(matches!(
            parsed.predicate,
            ParsedPredicate::Logical {
                op: LogicalPredicateOp::And,
                left,
                ..
            } if matches!(
                *left,
                ParsedPredicate::Logical {
                    op: LogicalPredicateOp::Or,
                    ..
                }
            )
        ));
    }

    #[test]
    fn parser_blocks_unbalanced_predicate_parentheses_without_fallback() {
        let error = parse_sql_local_source_statement(
            "SELECT id FROM 'target/input.csv' WHERE (id >= 1 OR amount >= 10 LIMIT 5",
        )
        .expect_err("unbalanced predicate parentheses remain blocked");

        assert!(error.to_string().contains("parentheses must be balanced"));
        assert!(error.to_string().contains("external_engine_invoked=false"));
    }

    #[test]
    fn cast_predicate_blocks_unadmitted_dtype() {
        let error = parse_sql_local_source_statement(
            "SELECT id FROM 'target/input.jsonl' WHERE CAST(amount AS decimal128(39,2)) >= 10 LIMIT 5",
        )
        .expect_err("invalid decimal cast target remains blocked");

        assert!(
            error
                .to_string()
                .contains("decimal CAST precision/scale must satisfy 1 <= precision <= 38")
        );
        assert!(error.to_string().contains("external_engine_invoked=false"));
    }

    #[test]
    fn parser_blocks_advanced_scalar_policy_constructs_without_fallback() {
        for (statement, expected) in [
            (
                "SELECT id,TIMESTAMP '2026-05-19T12:34:56Z' AT TIME ZONE 'America/Chicago' AS local_ts FROM 'target/input.csv' LIMIT 5",
                "timezone database semantics are not admitted",
            ),
            (
                "SELECT id,TIMEZONE('America/Chicago', event_ts) AS local_ts FROM 'target/input.csv' LIMIT 5",
                "timezone database semantics are not admitted",
            ),
            (
                "SELECT id,CONVERT_TIMEZONE('UTC','America/Chicago',event_ts) AS local_ts FROM 'target/input.csv' LIMIT 5",
                "timezone database semantics are not admitted",
            ),
            (
                "SELECT id,CAST(event_ts AS TIMESTAMPTZ) AS local_ts FROM 'target/input.csv' LIMIT 5",
                "timezone database semantics are not admitted",
            ),
            (
                "SELECT id,TIMESTAMPTZ '2026-05-19 12:34:56 America/Chicago' AS local_ts FROM 'target/input.csv' LIMIT 5",
                "timezone database semantics are not admitted",
            ),
            (
                "SELECT id,CAST(event_ts AS TIMESTAMP_TZ) AS local_ts FROM 'target/input.csv' LIMIT 5",
                "timezone database semantics are not admitted",
            ),
            (
                "SELECT id,CAST(event_ts AS TIMESTAMP WITH LOCAL TIME ZONE) AS local_ts FROM 'target/input.csv' LIMIT 5",
                "timezone database semantics are not admitted",
            ),
            (
                "SELECT id,label COLLATE nocase AS folded FROM 'target/input.csv' LIMIT 5",
                "SQL COLLATE, ILIKE, and locale-aware collation/case-folding semantics are not admitted",
            ),
            (
                "SELECT id FROM 'target/input.csv' WHERE label ILIKE 'a%' LIMIT 5",
                "SQL COLLATE, ILIKE, and locale-aware collation/case-folding semantics are not admitted",
            ),
            (
                "SELECT id,TRY_CAST(label AS decimal128(39,2)) AS unsupported FROM 'target/input.csv' LIMIT 5",
                "decimal CAST precision/scale must satisfy 1 <= precision <= 38",
            ),
        ] {
            let error = parse_sql_local_source_statement(statement)
                .expect_err("advanced scalar policy construct remains blocked");

            assert!(
                error.to_string().contains(expected),
                "expected {expected:?}, got {error}"
            );
            assert!(error.to_string().contains("external_engine_invoked=false"));
        }

        let parsed = parse_sql_local_source_statement(
            "SELECT id,timezone FROM 'target/input.csv' WHERE timezone = 'UTC' LIMIT 5",
        )
        .expect("timezone column name is not a timezone database function");
        assert_eq!(parsed.projections, vec!["id", "timezone"]);

        let parsed = parse_sql_local_source_statement(
            "SELECT id,timestamptz,timestamp_tz FROM 'target/input.csv' WHERE timestamptz = 'UTC' LIMIT 5",
        )
        .expect("timezone dtype spellings are only blocked as type syntax");
        assert_eq!(
            parsed.projections,
            vec!["id", "timestamptz", "timestamp_tz"]
        );
    }

    #[test]
    fn parser_blocks_unscoped_interval_literal_shapes_without_fallback() {
        for (statement, expected) in [
            (
                "SELECT id,event_date + INTERVAL '1' DAY AS next_day FROM 'target/input.csv' LIMIT 5",
                "arbitrary ANSI INTERVAL arithmetic is not admitted",
            ),
            (
                "SELECT id FROM 'target/input.csv' WHERE event_ts >= TIMESTAMP '2026-05-19T12:00:00Z' - INTERVAL '1' HOUR LIMIT 5",
                "arbitrary ANSI INTERVAL arithmetic is not admitted",
            ),
            (
                "SELECT id,DATE_ADD_DAYS(event_date, INTERVAL '1' HOUR) AS next_hour FROM 'target/input.csv' LIMIT 5",
                "date arithmetic interval literals admit DAY units only",
            ),
            (
                "SELECT id,TIMESTAMP_ADD_SECONDS(event_ts, INTERVAL '1.5' SECOND) AS shifted FROM 'target/input.csv' LIMIT 5",
                "ANSI INTERVAL literal value must be a signed integer string literal",
            ),
            (
                "SELECT id,TIMESTAMP_ADD_SECONDS(event_ts, INTERVAL '1' MONTH) AS shifted FROM 'target/input.csv' LIMIT 5",
                "ANSI INTERVAL literals in scoped temporal arithmetic admit DAY, HOUR, MINUTE, or SECOND units only",
            ),
            (
                "SELECT id,DATE_ADD_DAYS(event_date, intervaé) AS shifted FROM 'target/input.csv' LIMIT 5",
                "SQL identifiers may contain only ASCII letters, numbers, and underscores",
            ),
        ] {
            let error =
                parse_sql_local_source_statement(statement).expect_err("interval shape is blocked");

            assert!(
                error.to_string().contains(expected),
                "expected {expected:?}, got {error}"
            );
            assert!(error.to_string().contains("external_engine_invoked=false"));
        }

        let parsed = parse_sql_local_source_statement(
            "SELECT id,interval FROM 'target/input.csv' WHERE interval = 1 LIMIT 5",
        )
        .expect("interval is allowed as an ordinary column name");
        assert_eq!(parsed.projections, vec!["id", "interval"]);
    }

    #[test]
    fn parser_allows_regex_named_columns_without_treating_identifiers_as_regex() {
        let parsed = parse_sql_local_source_statement(
            "SELECT regex, regexp FROM 'target/input.csv' WHERE regex = 'alpha' LIMIT 5",
        )
        .expect("regex-like identifiers are ordinary columns");

        assert_eq!(
            parsed.source.local_path().unwrap(),
            Path::new("target/input.csv")
        );
        assert_eq!(parsed.limit, 5);
    }

    #[test]
    fn parser_admits_scoped_like_wildcard_predicates_without_fallback() {
        let parsed = parse_sql_local_source_statement(
            "SELECT id,label FROM 'target/input.csv' WHERE label LIKE 'a%a' LIMIT 5",
        )
        .expect("scoped LIKE multi-wildcard predicate is admitted");
        match parsed.predicate {
            ParsedPredicate::StringMatch {
                column, op, value, ..
            } => {
                assert_eq!(column, "label");
                assert_eq!(op, StringPredicateOp::LikePattern);
                assert_eq!(value, r"\Aa(?s:.*)a\z");
            }
            other => panic!("expected LIKE StringMatch predicate, got {other:?}"),
        }

        let parsed = parse_sql_local_source_statement(
            "SELECT id,label FROM 'target/input.csv' WHERE label LIKE '_l%' LIMIT 5",
        )
        .expect("scoped LIKE single-character wildcard predicate is admitted");
        match parsed.predicate {
            ParsedPredicate::StringMatch {
                column, op, value, ..
            } => {
                assert_eq!(column, "label");
                assert_eq!(op, StringPredicateOp::LikePattern);
                assert_eq!(value, r"\A(?s:.)l(?s:.*)\z");
            }
            other => panic!("expected LIKE StringMatch predicate, got {other:?}"),
        }

        let parsed = parse_sql_local_source_statement(
            "SELECT id,label FROM 'target/input.csv' WHERE label NOT LIKE 'a_p%' LIMIT 5",
        )
        .expect("scoped NOT LIKE wildcard predicate is admitted");
        match parsed.predicate {
            ParsedPredicate::Not { inner } => match *inner {
                ParsedPredicate::StringMatch {
                    column, op, value, ..
                } => {
                    assert_eq!(column, "label");
                    assert_eq!(op, StringPredicateOp::LikePattern);
                    assert_eq!(value, r"\Aa(?s:.)p(?s:.*)\z");
                }
                other => panic!("expected inner LIKE StringMatch predicate, got {other:?}"),
            },
            other => panic!("expected negated LIKE predicate, got {other:?}"),
        }
    }

    #[test]
    fn parser_admits_scoped_like_escape_predicates_without_fallback() {
        let parsed = parse_sql_local_source_statement(
            "SELECT id,label FROM 'target/input.csv' WHERE label LIKE 'a!_%' ESCAPE '!' LIMIT 5",
        )
        .expect("scoped LIKE ESCAPE predicate is admitted");
        match parsed.predicate {
            ParsedPredicate::StringMatch {
                column,
                op,
                value,
                like_escape,
            } => {
                assert_eq!(column, "label");
                assert_eq!(op, StringPredicateOp::LikePattern);
                assert_eq!(value, r"\Aa_(?s:.*)\z");
                assert_eq!(like_escape, Some('!'));
            }
            other => panic!("expected LIKE ESCAPE StringMatch predicate, got {other:?}"),
        }

        let parsed = parse_sql_local_source_statement(
            "SELECT id,label FROM 'target/input.csv' WHERE label NOT LIKE 'a!_%' ESCAPE '!' LIMIT 5",
        )
        .expect("scoped NOT LIKE ESCAPE predicate is admitted");
        match parsed.predicate {
            ParsedPredicate::Not { inner } => match *inner {
                ParsedPredicate::StringMatch {
                    column,
                    op,
                    value,
                    like_escape,
                } => {
                    assert_eq!(column, "label");
                    assert_eq!(op, StringPredicateOp::LikePattern);
                    assert_eq!(value, r"\Aa_(?s:.*)\z");
                    assert_eq!(like_escape, Some('!'));
                }
                other => panic!("expected inner LIKE ESCAPE StringMatch predicate, got {other:?}"),
            },
            other => panic!("expected negated LIKE ESCAPE predicate, got {other:?}"),
        }
    }

    #[test]
    fn parser_blocks_malformed_like_escape_clauses_without_fallback() {
        for (statement, expected) in [
            (
                "SELECT id,label FROM 'target/input.csv' WHERE label LIKE 'a!_%' ESCAPE '' LIMIT 5",
                "LIKE ESCAPE clause requires a single-character string literal",
            ),
            (
                "SELECT id,label FROM 'target/input.csv' WHERE label LIKE 'a!_%' ESCAPE '!!' LIMIT 5",
                "LIKE ESCAPE clause requires a single-character string literal",
            ),
            (
                "SELECT id,label FROM 'target/input.csv' WHERE label LIKE 'a!' ESCAPE '!' LIMIT 5",
                "LIKE ESCAPE pattern cannot end with the escape character",
            ),
            (
                "SELECT id,label FROM 'target/input.csv' WHERE label LIKE 'a!x%' ESCAPE '!' LIMIT 5",
                "LIKE ESCAPE may only escape %, _, or the escape character",
            ),
        ] {
            let error =
                parse_sql_local_source_statement(statement).expect_err("malformed ESCAPE blocks");

            assert!(
                error.to_string().contains(expected),
                "expected {expected:?}, got {error}"
            );
            assert!(error.to_string().contains("external_engine_invoked=false"));
        }
    }

    #[test]
    fn parser_admits_scoped_regex_predicates_without_fallback() {
        let parsed = parse_sql_local_source_statement(
            "SELECT id,label FROM 'target/input.csv' WHERE label RLIKE '^(alpha|gamma)$' LIMIT 5",
        )
        .expect("scoped RLIKE predicate is admitted");
        match parsed.predicate {
            ParsedPredicate::StringMatch {
                column, op, value, ..
            } => {
                assert_eq!(column, "label");
                assert_eq!(op, StringPredicateOp::RegexMatch);
                assert_eq!(value, "^(alpha|gamma)$");
            }
            other => panic!("expected regex StringMatch predicate, got {other:?}"),
        }

        let parsed = parse_sql_local_source_statement(
            "SELECT id,REGEXP_LIKE(label, '^a') AS matched FROM 'target/input.csv' LIMIT 5",
        )
        .expect("scoped REGEXP_LIKE predicate projection is admitted");
        assert_eq!(parsed.predicate_projections.len(), 1);
        assert_eq!(parsed.predicate_projections[0].alias, "matched");
        match &parsed.predicate_projections[0].predicate {
            ParsedPredicate::StringMatch {
                column, op, value, ..
            } => {
                assert_eq!(column, "label");
                assert_eq!(*op, StringPredicateOp::RegexMatch);
                assert_eq!(value, "^a");
            }
            other => panic!("expected regex predicate projection, got {other:?}"),
        }
    }

    #[test]
    fn parser_blocks_invalid_regex_patterns_without_fallback() {
        let error = parse_sql_local_source_statement(
            "SELECT id,label FROM 'target/input.csv' WHERE label REGEXP '[' LIMIT 5",
        )
        .expect_err("invalid regex pattern remains blocked");

        assert!(error.to_string().contains("regex pattern is invalid"));
        assert!(error.to_string().contains("external_engine_invoked=false"));
    }

    #[test]
    fn parser_allows_decimal_policy_words_outside_cast_targets() {
        let parsed = parse_sql_local_source_statement(
            "SELECT id,CAST(amount AS float64) AS numeric FROM 'target/input.csv' LIMIT 5",
        )
        .expect("numeric alias is not a decimal cast target");

        assert_eq!(parsed.cast_projections[0].alias, "numeric");
        assert_eq!(
            parsed.cast_projections[0].target_dtype,
            LogicalDType::Float64
        );
    }

    #[test]
    fn parser_blocks_unadmitted_complex_dtype_policy_constructs_without_fallback() {
        for (statement, expected) in [
            (
                "SELECT id,ARRAY(1,2) AS values FROM 'target/input.csv' LIMIT 5",
                "list and array accessors, function constructors, casts, and equality semantics are not admitted",
            ),
            (
                "SELECT id,LIST_EXTRACT(payload, 1) AS item FROM 'target/input.csv' LIMIT 5",
                "list and array accessors, function constructors, casts, and equality semantics are not admitted",
            ),
            (
                "SELECT CAST(payload AS list) AS payload FROM 'target/input.csv' LIMIT 5",
                "list and array accessors, function constructors, casts, and equality semantics are not admitted",
            ),
            (
                "SELECT id,ROW(label, amount) AS payload FROM 'target/input.csv' LIMIT 5",
                "row constructors plus struct casts, equality, and access semantics are not admitted",
            ),
            (
                "SELECT TRY_CAST(payload AS struct) AS payload FROM 'target/input.csv' LIMIT 5",
                "row constructors plus struct casts, equality, and access semantics are not admitted",
            ),
            (
                "SELECT id,VARIANT_GET(payload, 'field') AS field FROM 'target/input.csv' LIMIT 5",
                "variant access semantics are not admitted",
            ),
            (
                "SELECT CAST(payload AS union) AS payload FROM 'target/input.csv' LIMIT 5",
                "union dtype casts are not admitted",
            ),
        ] {
            let error = parse_sql_local_source_statement(statement)
                .expect_err("complex dtype policy construct remains blocked");

            assert!(
                error.to_string().contains(expected),
                "expected {expected:?}, got {error}"
            );
            assert!(error.to_string().contains("external_engine_invoked=false"));
        }
    }

    #[test]
    fn parser_blocks_complex_subquery_membership_materialization_without_fallback() {
        let error = parse_sql_local_source_statement(
            "SELECT id FROM 'target/input.csv' WHERE id IN (SELECT ARRAY[1] AS value_list FROM 'target/input.csv') LIMIT 5",
        )
        .expect_err("complex projected subquery membership remains blocked");

        assert!(
            error.to_string().contains(
                "projected subqueries do not admit ARRAY or STRUCT projection outputs for membership materialization"
            ),
            "unexpected error: {error}"
        );
        assert!(error.to_string().contains("external_engine_invoked=false"));
    }

    #[test]
    fn parses_scoped_sql_union_all_statement() {
        let parsed = parse_sql_local_source_union_statement(
            "SELECT id,label FROM 'target/left.csv' WHERE amount >= 10 UNION ALL SELECT id,label FROM 'target/right.csv' WHERE amount >= 20 ORDER BY id DESC LIMIT 3",
        )
        .expect("scoped union statement parses");

        assert_eq!(parsed.mode, SqlUnionMode::All);
        assert_eq!(parsed.branches.len(), 2);
        assert_eq!(parsed.branch_statements.len(), 2);
        assert!(parsed.branch_statements[0].ends_with("LIMIT 10000"));
        assert_eq!(parsed.limit, 3);
        let order_by = parsed.order_by.expect("global order by parsed");
        assert_eq!(order_by.keys[0].column, "id");
        assert_eq!(order_by.keys[0].direction, SortDirection::Desc);
    }

    #[test]
    fn parses_scoped_sql_intersect_and_except_statements() {
        let intersect = parse_sql_local_source_union_statement(
            "SELECT id,label FROM 'target/left.csv' INTERSECT DISTINCT SELECT id,label FROM 'target/right.csv' ORDER BY id ASC LIMIT 3",
        )
        .expect("scoped intersect statement parses");
        assert_eq!(intersect.mode, SqlUnionMode::IntersectDistinct);
        assert_eq!(intersect.branches.len(), 2);
        assert_eq!(intersect.limit, 3);
        let order_by = intersect.order_by.expect("global order by parsed");
        assert_eq!(order_by.keys[0].column, "id");
        assert_eq!(order_by.keys[0].direction, SortDirection::Asc);

        let except = parse_sql_local_source_union_statement(
            "SELECT id,label FROM 'target/left.csv' EXCEPT SELECT id,label FROM 'target/right.csv' LIMIT 5",
        )
        .expect("scoped except statement parses");
        assert_eq!(except.mode, SqlUnionMode::ExceptDistinct);
        assert_eq!(except.branches.len(), 2);
        assert_eq!(except.limit, 5);
        assert!(except.order_by.is_none());
    }

    #[test]
    fn parser_blocks_common_table_expressions_without_fallback() {
        for statement in [
            "WITH recent AS (SELECT id FROM 'target/input.csv' LIMIT 5) SELECT id FROM recent LIMIT 5",
            "WITH RECURSIVE r AS (SELECT id FROM 'target/input.csv' LIMIT 5) SELECT id FROM r LIMIT 5",
        ] {
            let error = parse_sql_local_source_statement(statement)
                .expect_err("CTE syntax remains a deterministic parser blocker");
            assert!(
                error
                    .to_string()
                    .contains("SQL common table expressions (WITH/RECURSIVE) are not admitted"),
                "got {error}"
            );
            assert!(error.to_string().contains("cte_plan_nodes"));
            assert!(error.to_string().contains("external_engine_invoked=false"));
        }

        let error = parse_sql_local_source_union_statement(
            "WITH recent AS (SELECT id FROM 'target/input.csv' LIMIT 5) SELECT id FROM recent UNION SELECT id FROM 'target/other.csv' LIMIT 5",
        )
        .expect_err("CTE set-operation syntax remains blocked");
        assert!(
            error
                .to_string()
                .contains("SQL common table expressions (WITH/RECURSIVE) are not admitted"),
            "got {error}"
        );
        assert!(
            error
                .to_string()
                .contains("no fallback execution was attempted")
        );
        assert!(error.to_string().contains("external_engine_invoked=false"));
    }

    #[test]
    fn parser_blocks_intersect_all_and_except_all_without_fallback() {
        for statement in [
            "SELECT id FROM 'target/left.csv' INTERSECT ALL SELECT id FROM 'target/right.csv' LIMIT 5",
            "SELECT id FROM 'target/left.csv' EXCEPT ALL SELECT id FROM 'target/right.csv' LIMIT 5",
        ] {
            let error = parse_sql_local_source_union_statement(statement)
                .expect_err("ALL set operation remains blocked");
            assert!(
                error
                    .to_string()
                    .contains("INTERSECT ALL and EXCEPT ALL are not admitted"),
                "got {error}"
            );
            assert!(error.to_string().contains("external_engine_invoked=false"));
        }
    }

    #[test]
    fn parser_blocks_union_branch_local_limit_without_fallback() {
        let error = parse_sql_local_source_union_statement(
            "SELECT id FROM 'target/left.csv' LIMIT 2 UNION SELECT id FROM 'target/right.csv' LIMIT 5",
        )
        .expect_err("branch-local limit remains blocked");

        assert!(
            error
                .to_string()
                .contains("SQL set-operation branch-local LIMIT clauses are not admitted"),
            "got {error}"
        );
        assert!(error.to_string().contains("external_engine_invoked=false"));
    }

    #[test]
    fn timestamp_literal_blocks_named_timezones_without_fallback() {
        let error = parse_sql_local_source_statement(
            "SELECT id,TIMESTAMP '2026-05-19T12:34:56Z' AT TIME ZONE 'America/Chicago' AS local_ts FROM 'target/input.csv' LIMIT 5",
        )
        .expect_err("named timezone semantics remain blocked");

        assert!(
            error
                .to_string()
                .contains("timezone database semantics are not admitted")
        );
        assert!(error.to_string().contains("external_engine_invoked=false"));
    }

    #[test]
    fn parses_scoped_inner_equi_join_statement() {
        let parsed = parse_sql_local_source_statement(
            "SELECT f.id,d.segment FROM 'target/fact.csv' AS f JOIN 'target/dim.csv' AS d ON f.customer_id = d.customer_id WHERE f.amount >= 10 LIMIT 3",
        )
        .expect("join statement parses");

        assert_eq!(parsed.projections, vec!["f.id", "d.segment"]);
        assert_eq!(parsed.aggregates, [] as [ParsedAggregate; 0]);
        assert_eq!(parsed.group_by, [] as [String; 0]);
        assert!(parsed.order_by.is_none());
        assert_eq!(
            parsed.source.local_path().unwrap(),
            Path::new("target/fact.csv")
        );
        assert_eq!(parsed.source_alias.as_deref(), Some("f"));
        let join = parsed.join.as_ref().expect("join parsed");
        assert_eq!(
            join.right_source.local_path().unwrap(),
            Path::new("target/dim.csv")
        );
        assert_eq!(join.right_alias, "d");

        assert!(matches!(
            parsed.predicate,
            ParsedPredicate::Compare {
                ref column,
                op: ComparisonOp::GtEq,
                value: ScalarValue::Int64(10)
            } if column == "f.amount"
        ));
    }

    #[test]
    fn parses_scoped_multi_key_inner_equi_join_statement() {
        let parsed = parse_sql_local_source_statement(
            "SELECT f.id,d.segment FROM 'target/fact.csv' AS f JOIN 'target/dim.csv' AS d ON f.customer_id = d.customer_id AND f.region = d.region WHERE f.amount >= 10 LIMIT 3",
        )
        .expect("multi-key join statement parses");

        let join = parsed.join.as_ref().expect("join parsed");
        assert_eq!(join.key_pairs.len(), 2);
        assert_eq!(join.join_type, ParsedJoinType::InnerEqui);
        assert_eq!(join.key_pairs[0].left.column, "customer_id");
        assert_eq!(join.key_pairs[1].right.column, "region");
    }

    #[test]
    fn parses_scoped_join_computed_projection_topn_statement() {
        let parsed = parse_sql_local_source_statement(
            "SELECT f.id,d.segment,f.amount + d.discount AS adjusted,CONCAT(d.segment,'-',f.region) AS segment_region FROM 'target/fact.csv' AS f JOIN 'target/dim.csv' AS d ON f.customer_id = d.customer_id AND f.region = d.region WHERE f.amount >= 10 ORDER BY f.amount DESC LIMIT 3",
        )
        .expect("join computed projection top-N statement parses");

        assert_eq!(parsed.projections, vec!["f.id", "d.segment"]);
        assert_eq!(parsed.generic_expression_projections.len(), 1);
        assert_eq!(
            parsed.generic_expression_projections[0].source_columns,
            vec!["d.discount", "f.amount"]
        );

        assert_eq!(parsed.string_function_projections.len(), 1);

        let order_by = parsed.order_by.as_ref().expect("order by parsed");

        assert_eq!(order_by.keys[0].column, "f.amount");
        assert_eq!(order_by.keys[0].direction, SortDirection::Desc);
    }

    #[test]
    fn parses_scoped_join_scalar_expression_on_statement() {
        let parsed = parse_sql_local_source_statement(
            "SELECT f.id,d.segment FROM 'target/fact.csv' AS f INNER JOIN 'target/dim.csv' AS d ON f.amount + d.discount >= 25 LIMIT 5",
        )
        .expect("scalar expression join statement parses");

        let join = parsed.join.as_ref().expect("join parsed");
        assert_eq!(join.key_pairs, [] as [ParsedJoinKeyPair; 0]);
        assert_eq!(
            join.on_predicate_family,
            ParsedJoinOnPredicateFamily::GenericExpression
        );
        assert!(join.on_predicate.is_some());
    }

    #[test]
    fn parses_scoped_join_logical_or_on_statement() {
        let parsed = parse_sql_local_source_statement(
            "SELECT f.id,d.segment FROM 'target/fact.csv' AS f INNER JOIN 'target/dim.csv' AS d ON f.customer_id = d.customer_id OR f.region = d.region LIMIT 5",
        )
        .expect("logical OR join statement parses");

        let join = parsed.join.as_ref().expect("join parsed");

        assert!(matches!(
            join.on_predicate.as_ref().expect("ON predicate"),
            ParsedPredicate::Logical {
                op: LogicalPredicateOp::Or,
                ..
            }
        ));
    }

    #[test]
    fn parser_blocks_logical_or_on_non_inner_join_without_fallback() {
        let error = parse_sql_local_source_statement(
            "SELECT f.id,d.segment FROM 'target/fact.csv' AS f LEFT JOIN 'target/dim.csv' AS d ON f.customer_id = d.customer_id OR f.region = d.region LIMIT 5",
        )
        .expect_err("outer OR join remains blocked");

        assert!(
            error
                .to_string()
                .contains("logical OR JOIN ON predicates are admitted only for INNER JOIN"),
            "got {error}"
        );
        assert!(error.to_string().contains("external_engine_invoked=false"));
    }

    #[test]
    fn parses_scoped_join_group_by_aggregate_statement() {
        let parsed = parse_sql_local_source_statement(
            "SELECT d.segment,sum(f.amount) AS total_amount,count(*) AS rows FROM 'target/fact.csv' AS f INNER JOIN 'target/dim.csv' AS d ON f.customer_id = d.customer_id AND f.region = d.region WHERE f.amount >= 10 GROUP BY d.segment LIMIT 10",
        )
        .expect("join group-by aggregate statement parses");

        assert_eq!(parsed.projections, vec!["d.segment"]);
        assert_eq!(parsed.group_by, vec!["d.segment"]);
        assert!(parsed.order_by.is_none());
        assert_eq!(
            parsed.source.local_path().unwrap(),
            Path::new("target/fact.csv")
        );
        assert_eq!(parsed.source_alias.as_deref(), Some("f"));
        let join = parsed.join.as_ref().expect("join parsed");
        assert_eq!(
            join.right_source.local_path().unwrap(),
            Path::new("target/dim.csv")
        );
        assert_eq!(join.right_alias, "d");

        assert_eq!(parsed.aggregates.len(), 2);

        assert_eq!(parsed.aggregates[0].output_name(), "total_amount");
        assert_eq!(parsed.aggregates[0].column(), Some("f.amount"));

        assert_eq!(parsed.aggregates[1].output_name(), "rows");
    }

    #[test]
    fn join_parser_blocks_complex_on_keys_without_fallback() {
        for statement in [
            "SELECT f.id,d.segment FROM 'target/fact.csv' AS f JOIN 'target/dim.csv' AS d ON ARRAY[f.customer_id] = ARRAY[d.customer_id] LIMIT 5",
            "SELECT f.id,d.segment FROM 'target/fact.csv' AS f JOIN 'target/dim.csv' AS d ON STRUCT(f.customer_id, f.region) = STRUCT(d.customer_id, d.region) LIMIT 5",
        ] {
            let error = parse_sql_local_source_statement(statement)
                .expect_err("complex join ON keys remain blocked");

            assert!(
                error
                    .to_string()
                    .contains("JOIN ON complex key expressions are not admitted"),
                "unexpected error for {statement}: {error}"
            );
            assert!(error.to_string().contains("external_engine_invoked=false"));
        }
    }

    #[test]
    fn parser_blocks_unbounded_or_remote_sql() {
        assert!(parse_sql_local_source_statement("SELECT id FROM 'target/input.csv'").is_err());
        assert!(
            parse_sql_local_source_statement(
                "SELECT id FROM 's3://bucket/input.csv' WHERE id = 1 LIMIT 5"
            )
            .is_ok(),
            "URI blocking happens at source admission"
        );
        assert!(reject_remote_source_path(Path::new("s3://bucket/input.csv")).is_err());
    }

    #[test]
    fn csv_parser_handles_basic_quoted_fields() {
        let row = split_csv_record("id,label").expect("record parses");
        assert_eq!(row, vec!["id", "label"]);
        let row = split_csv_record("1,\"hello, world\"").expect("record parses");
        assert_eq!(row, vec!["1", "hello, world"]);
    }

    #[test]
    fn csv_source_read_plan_materializes_required_columns_only() {
        let plan = LocalSourceReadPlan::required(
            BTreeSet::from(["id".to_string(), "amount".to_string()]),
            "test_required_columns",
        );

        let (header, rows) = parse_csv_source_content_with_plan(
            "id,label,amount\n1,alpha,8\n",
            &plan,
            Some(MAX_INPUT_ROWS),
        )
        .expect("CSV parses with read plan");

        assert_eq!(header, vec!["id", "label", "amount"]);
        assert_eq!(rows[0].get("id"), Some(&ScalarValue::Int64(1)));
        assert_eq!(rows[0].get("amount"), Some(&ScalarValue::Int64(8)));
        assert!(!rows[0].contains_key("label"));
    }

    #[test]
    fn jsonl_source_read_plan_skips_unselected_nested_and_escaped_values() {
        let plan = LocalSourceReadPlan::required(
            BTreeSet::from(["id".to_string(), "label".to_string()]),
            "test_projected_jsonl_columns",
        );

        let (header, rows) = parse_jsonl_source_content_with_plan(
            "{\"id\":1,\"payload\":{\"nested\":[1,2]},\"label\":\"alpha\",\"tail\":\"\\u263A\"}\n",
            &plan,
            Some(MAX_INPUT_ROWS),
        )
        .expect("projected JSONL parse skips unselected values");

        assert_eq!(header, vec!["id", "payload", "label", "tail"]);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get("id"), Some(&ScalarValue::Int64(1)));
        assert_eq!(
            rows[0].get("label"),
            Some(&ScalarValue::Utf8("alpha".into()))
        );
        assert!(!rows[0].contains_key("payload"));
        assert!(!rows[0].contains_key("tail"));
    }

    #[test]
    fn jsonl_source_read_plan_normalizes_selected_nested_values_as_utf8_json_payload() {
        let plan = LocalSourceReadPlan::required(
            BTreeSet::from(["id".to_string(), "payload".to_string()]),
            "test_selected_jsonl_nested_column",
        );

        let (header, rows) = parse_jsonl_source_content_with_plan(
            "{\"id\":1,\"payload\":{\"nested\":[1,2]}}\n",
            &plan,
            Some(MAX_INPUT_ROWS),
        )
        .expect("selected nested JSONL value is normalized as UTF-8 JSON payload");

        assert_eq!(header, vec!["id", "payload"]);
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].get("payload"),
            Some(&ScalarValue::Utf8("{\"nested\":[1,2]}".to_string()))
        );
    }

    #[test]
    fn json_parser_handles_flat_array_and_missing_fields() {
        let (header, rows) =
            parse_json_source_content("[{\"id\":1,\"label\":\"alpha\"},{\"id\":2,\"score\":2.5}]")
                .expect("json array parses");

        assert_eq!(header, vec!["id", "label", "score"]);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].get("id"), Some(&ScalarValue::Int64(1)));
        assert_eq!(
            rows[0].get("label"),
            Some(&ScalarValue::Utf8("alpha".into()))
        );
        assert_eq!(rows[0].get("score"), Some(&ScalarValue::Null));
        assert_eq!(rows[1].get("label"), Some(&ScalarValue::Null));
        assert_eq!(rows[1].get("score"), Some(&ScalarValue::Float64(2.5)));
    }

    #[test]
    fn json_parser_handles_single_flat_object() {
        let (header, rows) =
            parse_json_source_content("{\"id\":1,\"label\":\"alpha\"}").expect("json parses");

        assert_eq!(header, vec!["id", "label"]);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get("id"), Some(&ScalarValue::Int64(1)));
    }

    #[test]
    fn json_parser_normalizes_nested_values_as_utf8_json_payloads() {
        let (header, rows) = parse_json_source_content("[{\"id\":1,\"payload\":{\"x\":1}}]")
            .expect("nested JSON values normalize");
        assert_eq!(header, vec!["id", "payload"]);
        assert_eq!(
            rows[0].get("payload"),
            Some(&ScalarValue::Utf8("{\"x\":1}".to_string()))
        );
    }
}
