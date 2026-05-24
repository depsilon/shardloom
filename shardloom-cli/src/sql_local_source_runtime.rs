//! Scoped local-source SQL runtime smoke.
//!
//! This module intentionally admits one small SQL shape over local
//! CSV/JSONL/JSON and feature-gated local Parquet/Arrow IPC/Avro/ORC:
//! `SELECT <columns> FROM <local.csv|local.jsonl|local.json|local.parquet|local.arrow|local.ipc|local.avro|local.orc> [WHERE <scoped predicate>] [ORDER BY <column> [ASC|DESC][, ...]] LIMIT <n>`
//! plus explicit local single- and multi-key inner equi-join shapes.
//! It uses ShardLoom-owned parsing/binding plus the core expression semantics
//! baseline. It does not invoke `DataFusion`, `DuckDB`, `SQLite`, `Spark`,
//! `Polars`, `pandas`, object stores, catalogs, or Vortex query-engine
//! integrations. Local Vortex output is a scoped writer sink behind
//! `vortex-write`, not a Vortex query-engine integration or table commit.

use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
    fmt::Write as _,
    fs,
    path::{Path, PathBuf},
    process::ExitCode,
    time::Instant,
};

use shardloom_core::{
    BinaryOp, ColumnRef, CommandStatus, ComparisonOp, ExprId, Expression, ExpressionInputRow,
    ExpressionKind, LogicalDType, OutputFormat, ScalarValue, ShardLoomError, UnaryOp,
    WorkspaceSafeLocalWriteReport, evaluate_filter, evaluate_projection, format_iso_date32,
    format_iso_timestamp_micros, parse_iso_date32, parse_iso_timestamp_micros,
};

use crate::{
    cli_output::{emit, emit_error},
    cli_unknown_arg_error,
};

const COMMAND: &str = "sql-local-source-smoke";
const VORTEX_INGEST_COMMAND: &str = "vortex-ingest-smoke";
const SCHEMA_VERSION: &str = "shardloom.sql_local_source_smoke.v1";
const VORTEX_INGEST_SCHEMA_VERSION: &str = "shardloom.vortex_ingest_smoke.v1";
const LOCAL_SOURCE_STATE_SCHEMA_VERSION: &str = "shardloom.local_source_state.v1";
const LOCAL_INPUT_ADAPTER_REGISTRY_VERSION: &str = "shardloom.local_input_adapter_registry.v1";
const JSONL_OUTPUT_CERTIFICATE_ID: &str = "sql-local-source.csv.local-jsonl-output.native-io.v1";
const CSV_OUTPUT_CERTIFICATE_ID: &str = "sql-local-source.csv.local-csv-output.native-io.v1";
const PARQUET_OUTPUT_CERTIFICATE_ID: &str = "sql-local-source.local-parquet-output.native-io.v1";
const ARROW_IPC_OUTPUT_CERTIFICATE_ID: &str =
    "sql-local-source.local-arrow-ipc-output.native-io.v1";
const AVRO_OUTPUT_CERTIFICATE_ID: &str = "sql-local-source.local-avro-output.native-io.v1";
const ORC_OUTPUT_CERTIFICATE_ID: &str = "sql-local-source.local-orc-output.native-io.v1";
const VORTEX_OUTPUT_CERTIFICATE_ID: &str = "sql-local-source.local-vortex-output.native-io.v1";
const MAX_INPUT_ROWS: usize = 50_000;
const MAX_LIMIT_ROWS: usize = 10_000;
const MAX_JOIN_CANDIDATE_ROWS: usize = MAX_INPUT_ROWS;
const MAX_IN_LIST_VALUES: usize = 32;
const MAX_DATE_ARITHMETIC_DAYS: i32 = 366_000;
const MAX_TIMESTAMP_ARITHMETIC_SECONDS: i64 = (MAX_DATE_ARITHMETIC_DAYS as i64) * 86_400;

#[derive(Debug, Clone, PartialEq, Eq)]
struct LocalSourceReadPlan {
    required_columns: Option<BTreeSet<String>>,
    reason: &'static str,
}

impl LocalSourceReadPlan {
    fn full(reason: &'static str) -> Self {
        Self {
            required_columns: None,
            reason,
        }
    }

    fn required(columns: BTreeSet<String>, reason: &'static str) -> Self {
        Self {
            required_columns: Some(columns),
            reason,
        }
    }

    fn should_materialize(&self, column: &str) -> bool {
        match self.required_columns.as_ref() {
            Some(columns) => columns.contains(column),
            None => true,
        }
    }

    fn materialized_columns(&self, header: &[String]) -> Vec<String> {
        header
            .iter()
            .filter(|column| self.should_materialize(column))
            .cloned()
            .collect()
    }

    fn requested_columns(&self) -> String {
        self.required_columns.as_ref().map_or_else(
            || "all".to_string(),
            |columns| {
                if columns.is_empty() {
                    "none".to_string()
                } else {
                    columns.iter().cloned().collect::<Vec<_>>().join(",")
                }
            },
        )
    }

    const fn status(&self) -> &'static str {
        if self.required_columns.is_some() {
            "required_columns"
        } else {
            "full_columns"
        }
    }

    #[cfg(feature = "universal-format-io")]
    fn required_columns_vec(&self) -> Option<Vec<String>> {
        self.required_columns
            .as_ref()
            .map(|columns| columns.iter().cloned().collect())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SqlLocalSourceRequest {
    statement: String,
    output_format: SqlLocalSourceOutputFormat,
    output_path: Option<PathBuf>,
    fanout_outputs: Vec<SqlLocalSourceOutputTarget>,
    allow_overwrite: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SqlLocalSourceOutputTarget {
    format: SqlLocalSourceOutputFormat,
    path: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SqlRenderedOutput {
    format: SqlLocalSourceOutputFormat,
    path: PathBuf,
    payload: SqlOutputPayload,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum SqlOutputPayload {
    Bytes {
        content: Vec<u8>,
        digest: String,
        bytes: u64,
    },
    Vortex,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SqlWrittenOutput {
    format: SqlLocalSourceOutputFormat,
    path: PathBuf,
    digest: String,
    bytes: u64,
    write_millis: u128,
    replay: SqlOutputReplayEvidence,
    workspace_write_report: WorkspaceSafeLocalWriteReport,
    vortex_report: Option<shardloom_vortex::VortexPreparedStateWriteReport>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SqlOutputReplayEvidence {
    verified: bool,
    status: &'static str,
    replay_millis: u128,
    fidelity_status: &'static str,
    fidelity_loss: &'static str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SqlLocalSourceOutputFormat {
    InlineJsonl,
    Csv,
    Parquet,
    ArrowIpc,
    Avro,
    Orc,
    Vortex,
}

impl SqlLocalSourceOutputFormat {
    fn parse(value: &str) -> Result<Self, ShardLoomError> {
        match value.trim().to_ascii_lowercase().as_str() {
            "inline-jsonl" | "jsonl" | "json-lines" | "ndjson" => Ok(Self::InlineJsonl),
            "csv" => Ok(Self::Csv),
            "parquet" => Ok(Self::Parquet),
            "arrow" | "arrow-ipc" | "arrow_ipc" | "ipc" | "feather" => Ok(Self::ArrowIpc),
            "avro" => Ok(Self::Avro),
            "orc" => Ok(Self::Orc),
            "vortex" | "vtx" => Ok(Self::Vortex),
            other => Err(ShardLoomError::InvalidOperation(format!(
                "unsupported SQL local-source output format {other:?}; scoped local SQL supports local JSONL, CSV, and feature-gated Parquet/Arrow IPC/Avro/ORC/Vortex only"
            ))),
        }
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::InlineJsonl => "inline_jsonl",
            Self::Csv => "csv",
            Self::Parquet => "parquet",
            Self::ArrowIpc => "arrow_ipc",
            Self::Avro => "avro",
            Self::Orc => "orc",
            Self::Vortex => "vortex",
        }
    }

    const fn sink_format(self) -> &'static str {
        match self {
            Self::InlineJsonl => "jsonl",
            Self::Csv => "csv",
            Self::Parquet => "parquet",
            Self::ArrowIpc => "arrow_ipc",
            Self::Avro => "avro",
            Self::Orc => "orc",
            Self::Vortex => "vortex",
        }
    }

    const fn certificate_status(self) -> &'static str {
        match self {
            Self::InlineJsonl => "certified_local_jsonl_sink",
            Self::Csv => "certified_local_csv_sink",
            Self::Parquet => "certified_local_parquet_sink",
            Self::ArrowIpc => "certified_local_arrow_ipc_sink",
            Self::Avro => "certified_local_avro_sink",
            Self::Orc => "certified_local_orc_sink",
            Self::Vortex => "certified_local_vortex_sink",
        }
    }

    const fn certificate_ref(self) -> &'static str {
        match self {
            Self::InlineJsonl => JSONL_OUTPUT_CERTIFICATE_ID,
            Self::Csv => CSV_OUTPUT_CERTIFICATE_ID,
            Self::Parquet => PARQUET_OUTPUT_CERTIFICATE_ID,
            Self::ArrowIpc => ARROW_IPC_OUTPUT_CERTIFICATE_ID,
            Self::Avro => AVRO_OUTPUT_CERTIFICATE_ID,
            Self::Orc => ORC_OUTPUT_CERTIFICATE_ID,
            Self::Vortex => VORTEX_OUTPUT_CERTIFICATE_ID,
        }
    }

    fn render_rows(
        self,
        columns: &[String],
        rows: &[Vec<(String, ScalarValue)>],
    ) -> Result<Vec<u8>, ShardLoomError> {
        match self {
            Self::InlineJsonl => Ok(rows_to_jsonl(rows).into_bytes()),
            Self::Csv => Ok(rows_to_csv(columns, rows).into_bytes()),
            Self::Parquet => encode_parquet_output_rows(columns, rows),
            Self::ArrowIpc => encode_arrow_ipc_output_rows(columns, rows),
            Self::Avro => encode_avro_output_rows(columns, rows),
            Self::Orc => encode_orc_output_rows(columns, rows),
            Self::Vortex => Err(unsupported_sql_error(
                "local Vortex SQL output uses the Vortex writer path, not byte rendering",
            )),
        }
    }

    fn inline_result_rows(
        self,
        columns: &[String],
        rows: &[Vec<(String, ScalarValue)>],
    ) -> Result<Vec<u8>, ShardLoomError> {
        if matches!(self, Self::Vortex) {
            Ok(rows_to_jsonl(rows).into_bytes())
        } else {
            self.render_rows(columns, rows)
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
struct ParsedSqlLocalSource {
    projection_order: Vec<ParsedProjectionOutput>,
    projections: Vec<String>,
    literal_projections: Vec<ParsedLiteralProjection>,
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
    date_extract_projections: Vec<ParsedDateExtractProjection>,
    timestamp_extract_projections: Vec<ParsedTimestampExtractProjection>,
    aggregates: Vec<ParsedAggregate>,
    group_by: Vec<String>,
    order_by: Option<ParsedOrderBy>,
    source_path: PathBuf,
    source_alias: Option<String>,
    join: Option<ParsedJoin>,
    predicate: ParsedPredicate,
    limit: usize,
    normalized_statement: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ParsedAggregate {
    function: AggregateFunction,
    column: Option<String>,
    alias: Option<String>,
    distinct: bool,
}

#[derive(Debug, Clone, PartialEq)]
struct ParsedLiteralProjection {
    alias: String,
    value: ScalarValue,
}

#[derive(Debug, Clone, PartialEq)]
struct ParsedCastProjection {
    alias: String,
    column: String,
    target_dtype: LogicalDType,
    mode: CastMode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CastMode {
    Strict,
    Try,
}

impl CastMode {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Strict => "strict",
            Self::Try => "try",
        }
    }

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
    fn source_column(&self) -> Option<&str> {
        match self {
            Self::Literal(_) => None,
            Self::Column(column) => Some(column.as_str()),
        }
    }

    fn literal_dtype(&self) -> Option<LogicalDType> {
        match self {
            Self::Literal(value) => Some(value.dtype()),
            Self::Column(_) => None,
        }
    }

    fn dtype_label(&self, resolved_dtype: Option<&LogicalDType>) -> String {
        resolved_dtype
            .cloned()
            .or_else(|| self.literal_dtype())
            .map_or_else(
                || "source_column".to_string(),
                |dtype| dtype.as_str().to_string(),
            )
    }

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
    column: String,
}

#[derive(Debug, Clone, PartialEq)]
struct ParsedStringTransformProjection {
    alias: String,
    column: String,
    op: StringTransformOp,
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

trait ProjectionAlias {
    fn alias(&self) -> &str;
}

macro_rules! impl_projection_alias {
    ($($projection:ty),+ $(,)?) => {
        $(
            impl ProjectionAlias for $projection {
                fn alias(&self) -> &str {
                    &self.alias
                }
            }
        )+
    };
}

impl_projection_alias!(
    ParsedLiteralProjection,
    ParsedCastProjection,
    ParsedNullCoalesceProjection,
    ParsedNullIfProjection,
    ParsedConditionalProjection,
    ParsedPredicateProjection,
    ParsedNumericArithmeticProjection,
    ParsedNumericAbsProjection,
    ParsedNumericRoundingProjection,
    ParsedGenericExpressionProjection,
    ParsedDateArithmeticProjection,
    ParsedTimestampArithmeticProjection,
    ParsedStringLengthProjection,
    ParsedStringTransformProjection,
    ParsedStringFunctionProjection,
    ParsedDateExtractProjection,
    ParsedTimestampExtractProjection,
);

#[derive(Debug, Clone, PartialEq, Eq)]
struct ParsedOrderBy {
    keys: Vec<ParsedOrderKey>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ParsedOrderKey {
    column: String,
    direction: SortDirection,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ParsedJoin {
    right_source_path: PathBuf,
    right_alias: String,
    key_pairs: Vec<ParsedJoinKeyPair>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ParsedJoinKeyPair {
    left: QualifiedColumn,
    right: QualifiedColumn,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ParsedSourceClause {
    source_path: PathBuf,
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

#[derive(Debug, Clone, PartialEq)]
enum SortValue {
    Int(i64),
    Float(f64),
    Utf8(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SortValueFamily {
    Numeric,
    Utf8,
}

impl SortValue {
    fn try_from_scalar(value: &ScalarValue) -> Result<Self, ShardLoomError> {
        match value {
            ScalarValue::Int64(value) => Ok(Self::Int(*value)),
            ScalarValue::UInt64(value) => {
                let value = i64::try_from(*value).map_err(|_| {
                    unsupported_sql_error(
                        "ORDER BY unsigned values above int64 are not admitted in this scoped top-N smoke",
                    )
                })?;
                Ok(Self::Int(value))
            }
            ScalarValue::Float64(value) if value.is_finite() => Ok(Self::Float(*value)),
            ScalarValue::Utf8(value) => Ok(Self::Utf8(value.clone())),
            ScalarValue::Null => Err(unsupported_sql_error(
                "ORDER BY NULL ordering is not admitted in this scoped top-N smoke",
            )),
            _ => Err(unsupported_sql_error(
                "ORDER BY top-N smoke admits numeric or UTF-8 sort columns only",
            )),
        }
    }

    fn family(&self) -> SortValueFamily {
        match self {
            Self::Int(_) | Self::Float(_) => SortValueFamily::Numeric,
            Self::Utf8(_) => SortValueFamily::Utf8,
        }
    }
}

impl Eq for SortValue {}

impl Ord for SortValue {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Self::Int(_) | Self::Float(_), Self::Int(_) | Self::Float(_)) => {
                self.as_f64().total_cmp(&other.as_f64())
            }
            (Self::Utf8(left), Self::Utf8(right)) => left.cmp(right),
            _ => self.family_rank().cmp(&other.family_rank()),
        }
    }
}

impl PartialOrd for SortValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl SortValue {
    fn as_f64(&self) -> f64 {
        match self {
            Self::Int(value) => i64_to_f64(*value),
            Self::Float(value) => *value,
            Self::Utf8(_) => f64::NAN,
        }
    }

    fn family_rank(&self) -> u8 {
        match self.family() {
            SortValueFamily::Numeric => 0,
            SortValueFamily::Utf8 => 1,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
struct ParsedProjectionList {
    projection_order: Vec<ParsedProjectionOutput>,
    projections: Vec<String>,
    literal_projections: Vec<ParsedLiteralProjection>,
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
    date_extract_projections: Vec<ParsedDateExtractProjection>,
    timestamp_extract_projections: Vec<ParsedTimestampExtractProjection>,
    aggregates: Vec<ParsedAggregate>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ParsedProjectionOutput {
    Raw(String),
    Literal(String),
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
    DateExtract(String),
    TimestampExtract(String),
}

#[derive(Debug, Clone, PartialEq)]
struct ParsedInSubquery {
    source_column: String,
    source_path: PathBuf,
    source_format: Option<LocalSourceFormat>,
    source_digest: Option<String>,
    values: Vec<ScalarValue>,
}

#[derive(Debug, Clone, PartialEq)]
enum ParsedPredicate {
    All,
    Compare {
        column: String,
        op: ComparisonOp,
        value: ScalarValue,
    },
    CastCompare {
        column: String,
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
        column: String,
        comparison: ComparisonOp,
        value: ScalarValue,
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
    InSubquery {
        column: String,
        subquery: Box<ParsedInSubquery>,
    },
    StringMatch {
        column: String,
        op: StringPredicateOp,
        value: String,
    },
    StringTransformCompare {
        column: String,
        op: StringTransformOp,
        comparison: ComparisonOp,
        value: ScalarValue,
    },
    StringFunctionCompare {
        expression: Box<Expression>,
        op: StringFunctionOp,
        comparison: ComparisonOp,
        value: ScalarValue,
        source_columns: Vec<String>,
        literal_count: usize,
    },
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

    const fn as_str(self) -> &'static str {
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

    const fn as_str(self) -> &'static str {
        match self {
            Self::AddSeconds => "timestamp_add_seconds",
            Self::SubSeconds => "timestamp_sub_seconds",
        }
    }
}

impl DateExtractOp {
    const fn function_name(self) -> &'static str {
        match self {
            Self::Year => "date_year",
            Self::Month => "date_month",
            Self::Day => "date_day",
        }
    }

    const fn as_str(self) -> &'static str {
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

    const fn as_str(self) -> &'static str {
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
        }
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::StartsWith => "starts_with",
            Self::Contains => "contains",
            Self::EndsWith => "ends_with",
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

    const fn as_str(self) -> &'static str {
        match self {
            Self::Lower => "lower",
            Self::Upper => "upper",
            Self::Trim => "trim",
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

    const fn as_str(self) -> &'static str {
        match self {
            Self::Concat => "concat",
            Self::Substr => "substr",
            Self::Left => "left",
            Self::Right => "right",
            Self::Replace => "replace",
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

    const fn as_str(self) -> &'static str {
        match self {
            Self::Add => "add",
            Self::Subtract => "subtract",
            Self::Multiply => "multiply",
            Self::Divide => "divide",
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

    const fn as_str(self) -> &'static str {
        match self {
            Self::Floor => "floor",
            Self::Ceil => "ceil",
            Self::Round => "round",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LocalSourceFormat {
    Csv,
    Json,
    JsonLines,
    Parquet,
    ArrowIpc,
    Avro,
    Orc,
}

impl LocalSourceFormat {
    fn from_path(path: &Path) -> Result<Self, ShardLoomError> {
        let Some(extension) = path.extension().and_then(|value| value.to_str()) else {
            return Err(unsupported_sql_error(
                "GAR-RUNTIME-IMPL-4F admits local CSV, JSONL/NDJSON, flat JSON, and feature-gated Parquet/Arrow IPC/Avro/ORC sources only in this slice",
            ));
        };
        match extension.to_ascii_lowercase().as_str() {
            "csv" => Ok(Self::Csv),
            "json" => Ok(Self::Json),
            "jsonl" | "ndjson" => Ok(Self::JsonLines),
            "parquet" => Ok(Self::Parquet),
            "arrow" | "ipc" | "feather" => Ok(Self::ArrowIpc),
            "avro" => Ok(Self::Avro),
            "orc" => Ok(Self::Orc),
            _ => Err(unsupported_sql_error(
                "GAR-RUNTIME-IMPL-4F admits local CSV, JSONL/NDJSON, flat JSON, and feature-gated Parquet/Arrow IPC/Avro/ORC sources only in this slice",
            )),
        }
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::Csv => "csv",
            Self::Json => "json",
            Self::JsonLines => "jsonl",
            Self::Parquet => "parquet",
            Self::ArrowIpc => "arrow_ipc",
            Self::Avro => "avro",
            Self::Orc => "orc",
        }
    }

    const fn row_label(self) -> &'static str {
        match self {
            Self::Csv => "CSV",
            Self::Json => "JSON",
            Self::JsonLines => "JSONL",
            Self::Parquet => "Parquet",
            Self::ArrowIpc => "Arrow IPC",
            Self::Avro => "Avro",
            Self::Orc => "ORC",
        }
    }

    const fn scalar_parse_normalization(self) -> &'static str {
        match self {
            Self::Csv | Self::Json | Self::JsonLines => "local_text_to_scalar_rows",
            Self::Parquet | Self::ArrowIpc | Self::Avro | Self::Orc => {
                "arrow_record_batch_to_scalar_rows"
            }
        }
    }

    fn projection_pushdown_status(
        self,
        read_plan: &LocalSourceReadPlan,
    ) -> LocalSourceProjectionPushdownStatus {
        if read_plan.required_columns.is_none() {
            return LocalSourceProjectionPushdownStatus::NotRequestedFullRead;
        }
        match self {
            Self::Csv | Self::Json | Self::JsonLines => {
                LocalSourceProjectionPushdownStatus::TextParserColumnPruning
            }
            Self::Parquet | Self::ArrowIpc | Self::Avro | Self::Orc => {
                LocalSourceProjectionPushdownStatus::ReaderLevelProjection
            }
        }
    }

    #[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
    const fn preserves_columnar_vortex_ingest_source_state(self) -> bool {
        matches!(
            self,
            Self::Parquet | Self::ArrowIpc | Self::Avro | Self::Orc
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LocalSourceProjectionPushdownStatus {
    NotRequestedFullRead,
    TextParserColumnPruning,
    ReaderLevelProjection,
}

impl LocalSourceProjectionPushdownStatus {
    const fn as_str(self) -> &'static str {
        match self {
            Self::NotRequestedFullRead => "not_requested_full_read",
            Self::TextParserColumnPruning => "local_text_parser_column_pruning",
            Self::ReaderLevelProjection => "reader_level_projection",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
struct LocalSourceReadContent {
    header: Vec<String>,
    rows: Vec<ExpressionInputRow>,
    reader_projection_columns: Option<Vec<String>>,
    source_to_columnar_millis: u128,
    record_batch_count: usize,
    materialization_layout: &'static str,
    parse_normalization: &'static str,
    columnar_source_preserved: bool,
}

impl LocalSourceReadContent {
    fn text(
        source_format: LocalSourceFormat,
        header: Vec<String>,
        rows: Vec<ExpressionInputRow>,
    ) -> Self {
        Self {
            header,
            rows,
            reader_projection_columns: None,
            source_to_columnar_millis: 0,
            record_batch_count: 0,
            materialization_layout: "scalar_row_map",
            parse_normalization: source_format.scalar_parse_normalization(),
            columnar_source_preserved: false,
        }
    }

    #[cfg(feature = "universal-format-io")]
    fn columnar_then_scalar(
        header: Vec<String>,
        rows: Vec<ExpressionInputRow>,
        reader_projection_columns: Vec<String>,
        source_to_columnar_millis: u128,
        record_batch_count: usize,
    ) -> Self {
        Self {
            header,
            rows,
            reader_projection_columns: Some(reader_projection_columns),
            source_to_columnar_millis,
            record_batch_count,
            materialization_layout: "arrow_record_batch_columnar_source_state_then_scalar_row_map",
            parse_normalization: "structured_reader_to_arrow_record_batches_then_scalar_rows",
            columnar_source_preserved: true,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
struct CsvSourceData {
    source_format: LocalSourceFormat,
    header: Vec<String>,
    rows: Vec<ExpressionInputRow>,
    read_plan: LocalSourceReadPlan,
    materialized_columns: Vec<String>,
    reader_projection_columns: Vec<String>,
    projection_pushdown_status: LocalSourceProjectionPushdownStatus,
    source_bytes: u64,
    source_digest: String,
    read_millis: u128,
    parse_millis: u128,
    source_to_columnar_millis: u128,
    record_batch_count: usize,
    materialization_layout: &'static str,
    parse_normalization: &'static str,
    columnar_source_preserved: bool,
}

impl CsvSourceData {
    fn materialized_columns_field(&self) -> String {
        if self.materialized_columns.is_empty() {
            "none".to_string()
        } else {
            self.materialized_columns.join(",")
        }
    }

    fn reader_projection_columns_field(&self) -> String {
        if self.reader_projection_columns.is_empty() {
            "none".to_string()
        } else {
            self.reader_projection_columns.join(",")
        }
    }

    fn pruned_column_count(&self) -> usize {
        self.header
            .len()
            .saturating_sub(self.materialized_columns.len())
    }

    fn column_pruning_applied(&self) -> bool {
        self.pruned_column_count() > 0
    }

    const fn materialization_layout(&self) -> &'static str {
        self.materialization_layout
    }

    const fn parse_normalization(&self) -> &'static str {
        self.parse_normalization
    }

    const fn columnar_source_preserved(&self) -> bool {
        self.columnar_source_preserved
    }

    fn source_state_id(&self) -> String {
        format!(
            "local-{}-{}",
            self.source_format.as_str(),
            self.source_digest.replace(':', "-")
        )
    }

    fn source_state_digest(&self, source_schema_digest: &str) -> String {
        fnv64_digest(&format!(
            "{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}",
            self.source_format.as_str(),
            self.source_digest,
            source_schema_digest,
            self.rows.len(),
            self.source_bytes,
            self.read_plan.requested_columns(),
            self.materialized_columns_field(),
            self.reader_projection_columns_field(),
            self.projection_pushdown_status.as_str(),
            self.materialization_layout,
            self.columnar_source_preserved,
            self.record_batch_count
        ))
    }
}

#[derive(Debug, Clone, PartialEq)]
struct VortexIngestSourceData {
    source_format: LocalSourceFormat,
    header: Vec<String>,
    read_plan: LocalSourceReadPlan,
    materialized_columns: Vec<String>,
    reader_projection_columns: Vec<String>,
    projection_pushdown_status: LocalSourceProjectionPushdownStatus,
    source_bytes: u64,
    source_digest: String,
    row_count: usize,
    read_millis: u128,
    compatibility_parse_millis: u128,
    source_to_columnar_millis: u128,
    record_batch_count: usize,
    materialization_layout: &'static str,
    parse_normalization: &'static str,
    columnar_source_preserved: bool,
}

impl VortexIngestSourceData {
    fn from_scalar_source(source: CsvSourceData) -> Self {
        Self {
            row_count: source.rows.len(),
            compatibility_parse_millis: source.parse_millis,
            source_to_columnar_millis: source.source_to_columnar_millis,
            record_batch_count: source.record_batch_count,
            materialization_layout: source.materialization_layout(),
            parse_normalization: source.parse_normalization(),
            columnar_source_preserved: source.columnar_source_preserved(),
            source_format: source.source_format,
            header: source.header,
            read_plan: source.read_plan,
            materialized_columns: source.materialized_columns,
            reader_projection_columns: source.reader_projection_columns,
            projection_pushdown_status: source.projection_pushdown_status,
            source_bytes: source.source_bytes,
            source_digest: source.source_digest,
            read_millis: source.read_millis,
        }
    }

    #[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
    fn from_columnar_source(
        source_format: LocalSourceFormat,
        columnar_source: &shardloom_vortex::FlatLocalColumnarSource,
        source_bytes: u64,
        source_digest: String,
        read_millis: u128,
        source_to_columnar_millis: u128,
    ) -> Self {
        Self {
            source_format,
            header: columnar_source.header.clone(),
            read_plan: LocalSourceReadPlan::full("full_columnar_source_state_default"),
            materialized_columns: columnar_source.materialized_columns.clone(),
            reader_projection_columns: columnar_source.reader_projection_columns.clone(),
            projection_pushdown_status: LocalSourceProjectionPushdownStatus::NotRequestedFullRead,
            source_bytes,
            source_digest,
            row_count: columnar_source.row_count,
            read_millis,
            compatibility_parse_millis: 0,
            source_to_columnar_millis,
            record_batch_count: columnar_source.batches.len(),
            materialization_layout: "arrow_record_batch_columnar_source_state",
            parse_normalization: "structured_reader_to_arrow_record_batches",
            columnar_source_preserved: true,
        }
    }

    fn materialized_columns_field(&self) -> String {
        if self.materialized_columns.is_empty() {
            "none".to_string()
        } else {
            self.materialized_columns.join(",")
        }
    }

    fn reader_projection_columns_field(&self) -> String {
        if self.reader_projection_columns.is_empty() {
            "none".to_string()
        } else {
            self.reader_projection_columns.join(",")
        }
    }

    fn pruned_column_count(&self) -> usize {
        self.header
            .len()
            .saturating_sub(self.materialized_columns.len())
    }

    fn column_pruning_applied(&self) -> bool {
        self.pruned_column_count() > 0
    }

    fn source_state_id(&self) -> String {
        format!(
            "local-{}-{}",
            self.source_format.as_str(),
            self.source_digest.replace(':', "-")
        )
    }

    fn source_state_digest(&self, source_schema_digest: &str) -> String {
        fnv64_digest(&format!(
            "{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}",
            self.source_format.as_str(),
            self.source_digest,
            source_schema_digest,
            self.row_count,
            self.source_bytes,
            self.read_plan.requested_columns(),
            self.materialized_columns_field(),
            self.reader_projection_columns_field(),
            self.projection_pushdown_status.as_str(),
            self.materialization_layout,
            self.columnar_source_preserved
        ))
    }
}

#[derive(Debug, Clone, PartialEq)]
struct GroupedAggregateBucket {
    values: Vec<(String, ScalarValue)>,
    row_indexes: Vec<usize>,
}

#[derive(Debug, Clone, PartialEq)]
struct JoinEvaluationOutput {
    joined_row_count: usize,
    selected_row_count: usize,
    output_rows: Vec<Vec<(String, ScalarValue)>>,
}

#[derive(Debug, Clone, PartialEq)]
struct SqlLocalSourceReport {
    request: SqlLocalSourceRequest,
    parsed: ParsedSqlLocalSource,
    source: CsvSourceData,
    right_source: Option<CsvSourceData>,
    selected_row_count: usize,
    joined_row_count: usize,
    output_rows: Vec<Vec<(String, ScalarValue)>>,
    result_jsonl: String,
    plan_digest: String,
    source_schema_digest: String,
    result_digest: String,
    output_digest: String,
    output_write_millis: u128,
    output_bytes: u64,
    written_outputs: Vec<SqlWrittenOutput>,
    operator_compute_millis: u128,
    evidence_render_millis: u128,
    total_runtime_millis: u128,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct VortexIngestRequest {
    source_path: PathBuf,
    target_path: PathBuf,
    allow_overwrite: bool,
    certification_level: shardloom_vortex::VortexIngestCertificationLevel,
}

#[derive(Debug, Clone, PartialEq)]
struct VortexIngestReport {
    request: VortexIngestRequest,
    source: VortexIngestSourceData,
    source_schema_digest: String,
    source_state_id: String,
    source_state_digest: String,
    prepared_state_id: String,
    prepared_state_digest: String,
    prepare_once_total_millis: u128,
    evidence_render_millis: u128,
    vortex_report: shardloom_vortex::VortexPreparedStateWriteReport,
}

pub(crate) fn handle_sql_local_source_smoke(
    mut args: impl Iterator<Item = String>,
    format: OutputFormat,
) -> ExitCode {
    let Some(statement_raw) = args.next() else {
        eprintln!(
            "usage: shardloom {COMMAND} <sql-statement> [--output-format inline-jsonl|csv|parquet|arrow-ipc|avro|orc|vortex] [--output local.jsonl|local.csv|local.parquet|local.arrow|local.avro|local.orc|local.vortex] [--fanout-output format=local-path]... [--allow-overwrite] [--format text|json]"
        );
        return ExitCode::from(2);
    };

    let request = match parse_sql_local_source_request(statement_raw, args) {
        Ok(request) => request,
        Err(error) => {
            return emit_error(COMMAND, format, "SQL local-source smoke failed", &error);
        }
    };
    let report = match run_sql_local_source_smoke(&request) {
        Ok(report) => report,
        Err(error) => {
            return emit_error(COMMAND, format, "SQL local-source smoke failed", &error);
        }
    };

    emit(
        COMMAND,
        format,
        CommandStatus::Success,
        format!(
            "SQL local-source smoke returned {} bounded row(s)",
            report.output_rows.len()
        ),
        report.to_text(),
        Vec::new(),
        report.fields(),
    );
    ExitCode::SUCCESS
}

fn parse_sql_local_source_request(
    statement: String,
    mut args: impl Iterator<Item = String>,
) -> Result<SqlLocalSourceRequest, ShardLoomError> {
    let mut output_format = SqlLocalSourceOutputFormat::InlineJsonl;
    let mut output_path = None;
    let mut fanout_outputs = Vec::new();
    let mut allow_overwrite = false;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--output-format" => {
                let Some(value) = args.next() else {
                    return Err(ShardLoomError::InvalidOperation(
                        "--output-format requires a value".to_string(),
                    ));
                };
                output_format = SqlLocalSourceOutputFormat::parse(&value)?;
            }
            "--output" => {
                let Some(value) = args.next() else {
                    return Err(ShardLoomError::InvalidOperation(
                        "--output requires a value".to_string(),
                    ));
                };
                output_path = Some(normalize_local_output_path(&value)?);
            }
            "--fanout-output" => {
                let Some(value) = args.next() else {
                    return Err(ShardLoomError::InvalidOperation(
                        "--fanout-output requires a value like csv=local.csv".to_string(),
                    ));
                };
                fanout_outputs.push(parse_sql_local_source_fanout_output(&value)?);
            }
            "--allow-overwrite" => allow_overwrite = true,
            extra => {
                return Err(cli_unknown_arg_error(COMMAND, extra));
            }
        }
    }

    validate_sql_local_source_output_request(
        output_format,
        output_path.as_deref(),
        &fanout_outputs,
    )?;

    Ok(SqlLocalSourceRequest {
        statement,
        output_format,
        output_path,
        fanout_outputs,
        allow_overwrite,
    })
}

#[allow(clippy::too_many_lines)]
pub(crate) fn handle_vortex_ingest_smoke(
    mut args: impl Iterator<Item = String>,
    format: OutputFormat,
) -> ExitCode {
    let Some(source_path_raw) = args.next() else {
        eprintln!(
            "usage: shardloom {VORTEX_INGEST_COMMAND} <local-source-path> <target.vortex> [--allow-overwrite] [--certification-level ingest_minimal|ingest_certified|ingest_full_replay] [--format text|json]"
        );
        return ExitCode::from(2);
    };
    let Some(target_path_raw) = args.next() else {
        eprintln!(
            "usage: shardloom {VORTEX_INGEST_COMMAND} <local-source-path> <target.vortex> [--allow-overwrite] [--certification-level ingest_minimal|ingest_certified|ingest_full_replay] [--format text|json]"
        );
        return ExitCode::from(2);
    };
    let mut allow_overwrite = false;
    let mut certification_level = shardloom_vortex::VortexIngestCertificationLevel::IngestCertified;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--allow-overwrite" => allow_overwrite = true,
            "--certification-level" => {
                let Some(value) = args.next() else {
                    return emit_error(
                        VORTEX_INGEST_COMMAND,
                        format,
                        "vortex_ingest smoke failed",
                        &ShardLoomError::InvalidOperation(
                            "--certification-level requires ingest_minimal, ingest_certified, or ingest_full_replay".to_string(),
                        ),
                    );
                };
                certification_level =
                    match shardloom_vortex::VortexIngestCertificationLevel::parse(&value) {
                        Ok(level) => level,
                        Err(error) => {
                            return emit_error(
                                VORTEX_INGEST_COMMAND,
                                format,
                                "vortex_ingest smoke failed",
                                &error,
                            );
                        }
                    };
            }
            extra => {
                return emit_error(
                    VORTEX_INGEST_COMMAND,
                    format,
                    "vortex_ingest smoke failed",
                    &cli_unknown_arg_error(VORTEX_INGEST_COMMAND, extra),
                );
            }
        }
    }

    let source_path = Path::new(source_path_raw.trim()).to_path_buf();
    let target_path = match normalize_local_vortex_ingest_target_path(&target_path_raw) {
        Ok(path) => path,
        Err(error) => {
            return emit_error(
                VORTEX_INGEST_COMMAND,
                format,
                "vortex_ingest smoke failed",
                &error,
            );
        }
    };
    let request = VortexIngestRequest {
        source_path,
        target_path,
        allow_overwrite,
        certification_level,
    };

    if !shardloom_vortex::vortex_ingest_write_feature_enabled() {
        emit(
            VORTEX_INGEST_COMMAND,
            format,
            CommandStatus::Unsupported,
            "vortex_ingest feature gate is not enabled".to_string(),
            "local vortex_ingest runtime requires shardloom-cli --features vortex-write; fallback execution remains disabled"
                .to_string(),
            Vec::new(),
            vortex_ingest_feature_blocked_fields(&request),
        );
        return ExitCode::from(1);
    }

    let report = match run_vortex_ingest_smoke(request) {
        Ok(report) => report,
        Err(error) => {
            return emit_error(
                VORTEX_INGEST_COMMAND,
                format,
                "vortex_ingest smoke failed",
                &error,
            );
        }
    };

    emit(
        VORTEX_INGEST_COMMAND,
        format,
        CommandStatus::Success,
        format!(
            "vortex_ingest prepared {} local row(s) into {}",
            report.vortex_report.row_count,
            report.request.target_path.display()
        ),
        report.to_text(),
        Vec::new(),
        report.fields(),
    );
    ExitCode::SUCCESS
}

fn validate_sql_local_source_output_request(
    output_format: SqlLocalSourceOutputFormat,
    output_path: Option<&Path>,
    fanout_outputs: &[SqlLocalSourceOutputTarget],
) -> Result<(), ShardLoomError> {
    if output_path.is_none()
        && matches!(
            output_format,
            SqlLocalSourceOutputFormat::Csv
                | SqlLocalSourceOutputFormat::Parquet
                | SqlLocalSourceOutputFormat::ArrowIpc
                | SqlLocalSourceOutputFormat::Avro
                | SqlLocalSourceOutputFormat::Orc
                | SqlLocalSourceOutputFormat::Vortex
        )
    {
        return Err(ShardLoomError::InvalidOperation(
            "SQL local-source CSV, Parquet, Arrow IPC, Avro, ORC, or Vortex output requires --output <local path>".to_string(),
        ));
    }
    let mut paths = BTreeSet::new();
    if let Some(output_path) = output_path {
        paths.insert(normalized_output_path_key(output_path));
    }
    for target in fanout_outputs {
        if !paths.insert(normalized_output_path_key(&target.path)) {
            return Err(ShardLoomError::InvalidOperation(format!(
                "SQL local-source fanout output path is duplicated: {}; no fallback execution was attempted",
                target.path.display()
            )));
        }
    }
    Ok(())
}

fn parse_sql_local_source_fanout_output(
    value: &str,
) -> Result<SqlLocalSourceOutputTarget, ShardLoomError> {
    let Some((format_raw, path_raw)) = value.split_once('=') else {
        return Err(ShardLoomError::InvalidOperation(
            "--fanout-output must use format=local-path, for example csv=out.csv".to_string(),
        ));
    };
    let format = SqlLocalSourceOutputFormat::parse(format_raw)?;
    Ok(SqlLocalSourceOutputTarget {
        format,
        path: normalize_local_output_path(path_raw)?,
    })
}

fn normalized_output_path_key(path: &Path) -> String {
    path.to_string_lossy()
        .replace('\\', "/")
        .to_ascii_lowercase()
}

fn run_vortex_ingest_smoke(
    request: VortexIngestRequest,
) -> Result<VortexIngestReport, ShardLoomError> {
    #[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
    {
        let source_format = LocalSourceFormat::from_path(&request.source_path)?;
        if source_format.preserves_columnar_vortex_ingest_source_state() {
            return run_columnar_vortex_ingest_smoke(request, source_format);
        }
    }
    run_scalar_vortex_ingest_smoke(request)
}

fn run_scalar_vortex_ingest_smoke(
    request: VortexIngestRequest,
) -> Result<VortexIngestReport, ShardLoomError> {
    let prepare_start = Instant::now();
    let source = read_local_source(&request.source_path)?;
    let rows = ordered_source_rows(&source.header, &source.rows)?;
    let vortex_request = shardloom_vortex::VortexPreparedStateWriteRequest::new(
        &request.target_path,
        source.header.clone(),
        rows,
    )
    .allow_overwrite(request.allow_overwrite)
    .certification_level(request.certification_level);
    let vortex_report = shardloom_vortex::write_flat_scalar_vortex_prepared_state(vortex_request)?;
    let prepare_once_total_millis = prepare_start.elapsed().as_millis();

    let evidence_start = Instant::now();
    let source = VortexIngestSourceData::from_scalar_source(source);
    let source_schema_digest = fnv64_digest(&source.header.join(","));
    let source_state_id = source_state_id_for_source(&source);
    let source_state_digest = source_state_digest_for_source(&source, &source_schema_digest);
    let prepared_state_digest = fnv64_digest(&format!(
        "{}|{}|{}|{}",
        source_state_digest,
        vortex_report.artifact_digest,
        vortex_report.column_family_summary(),
        vortex_report.row_count
    ));
    let prepared_state_id = format!(
        "vortex-prepared-state-{}",
        prepared_state_digest.replace(':', "-")
    );
    let evidence_render_millis = evidence_start.elapsed().as_millis();

    Ok(VortexIngestReport {
        request,
        source,
        source_schema_digest,
        source_state_id,
        source_state_digest,
        prepared_state_id,
        prepared_state_digest,
        prepare_once_total_millis,
        evidence_render_millis,
        vortex_report,
    })
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
fn run_columnar_vortex_ingest_smoke(
    request: VortexIngestRequest,
    source_format: LocalSourceFormat,
) -> Result<VortexIngestReport, ShardLoomError> {
    reject_remote_source_path(&request.source_path)?;
    let prepare_start = Instant::now();
    let read_start = Instant::now();
    let bytes = fs::read(&request.source_path).map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "failed to read local {} source {}: {error}",
            source_format.row_label(),
            request.source_path.display(),
        ))
    })?;
    let read_millis = read_start.elapsed().as_millis();
    let source_bytes = u64::try_from(bytes.len()).map_err(|_| {
        ShardLoomError::InvalidOperation(format!(
            "{} source length does not fit in u64",
            source_format.row_label()
        ))
    })?;
    let source_digest = fnv64_digest_bytes(&bytes);
    let source_to_columnar_start = Instant::now();
    let columnar_source =
        read_columnar_vortex_ingest_source(source_format, &request.source_path, MAX_INPUT_ROWS)?;
    let source_to_columnar_millis = source_to_columnar_start.elapsed().as_millis();
    for column in &columnar_source.header {
        validate_sql_identifier(column)?;
    }

    let source = VortexIngestSourceData::from_columnar_source(
        source_format,
        &columnar_source,
        source_bytes,
        source_digest,
        read_millis,
        source_to_columnar_millis,
    );
    let vortex_request = shardloom_vortex::VortexPreparedStateColumnarWriteRequest::new(
        &request.target_path,
        columnar_source,
    )
    .allow_overwrite(request.allow_overwrite)
    .certification_level(request.certification_level);
    let vortex_report =
        shardloom_vortex::write_flat_columnar_vortex_prepared_state(vortex_request)?;
    let prepare_once_total_millis = prepare_start.elapsed().as_millis();

    let evidence_start = Instant::now();
    let source_schema_digest = fnv64_digest(&source.header.join(","));
    let source_state_id = source_state_id_for_source(&source);
    let source_state_digest = source_state_digest_for_source(&source, &source_schema_digest);
    let prepared_state_digest = fnv64_digest(&format!(
        "{}|{}|{}|{}",
        source_state_digest,
        vortex_report.artifact_digest,
        vortex_report.column_family_summary(),
        vortex_report.row_count
    ));
    let prepared_state_id = format!(
        "vortex-prepared-state-{}",
        prepared_state_digest.replace(':', "-")
    );
    let evidence_render_millis = evidence_start.elapsed().as_millis();

    Ok(VortexIngestReport {
        request,
        source,
        source_schema_digest,
        source_state_id,
        source_state_digest,
        prepared_state_id,
        prepared_state_digest,
        prepare_once_total_millis,
        evidence_render_millis,
        vortex_report,
    })
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
fn read_columnar_vortex_ingest_source(
    source_format: LocalSourceFormat,
    path: &Path,
    max_rows: usize,
) -> Result<shardloom_vortex::FlatLocalColumnarSource, ShardLoomError> {
    match source_format {
        LocalSourceFormat::Parquet => {
            shardloom_vortex::read_flat_parquet_columnar_source(path, max_rows)
        }
        LocalSourceFormat::ArrowIpc => {
            shardloom_vortex::read_flat_arrow_ipc_columnar_source(path, max_rows)
        }
        LocalSourceFormat::Avro => shardloom_vortex::read_flat_avro_columnar_source(path, max_rows),
        LocalSourceFormat::Orc => shardloom_vortex::read_flat_orc_columnar_source(path, max_rows),
        LocalSourceFormat::Csv | LocalSourceFormat::Json | LocalSourceFormat::JsonLines => {
            Err(ShardLoomError::InvalidOperation(format!(
                "local {} source does not have a columnar vortex_ingest SourceState route; no fallback execution was attempted",
                source_format.row_label()
            )))
        }
    }
}

fn output_column_names(parsed: &ParsedSqlLocalSource, source: &CsvSourceData) -> Vec<String> {
    parsed.output_columns(&source.header)
}

#[allow(clippy::too_many_lines)]
impl VortexIngestReport {
    fn fields(&self) -> Vec<(String, String)> {
        let certified_reopen = self.request.certification_level
            == shardloom_vortex::VortexIngestCertificationLevel::IngestCertified;
        let certification_status = if certified_reopen {
            "fixture_smoke_certified"
        } else {
            "minimal_ingest_evidence_reported"
        };
        let certification_blocker_id = if certified_reopen {
            "not_claim_grade_fixture_smoke"
        } else {
            "not_claim_grade_ingest_minimal_no_reopen_or_replay"
        };
        let native_io_certificate_status = if certified_reopen {
            "certified_local_vortex_ingest_smoke"
        } else {
            "minimal_local_vortex_ingest_digest_only"
        };
        let source_backed_scan_evidence_status = if certified_reopen {
            "scoped_reopen_row_count_scan"
        } else {
            "not_performed_ingest_minimal"
        };
        let source_backed_scan_provider_kind = if certified_reopen {
            "vortex_scan"
        } else {
            "not_invoked"
        };
        let source_backed_scan_provider_surface = if certified_reopen {
            "VortexFile::scan"
        } else {
            "not_invoked"
        };
        let claim_gate_status = if certified_reopen {
            "fixture_smoke_only"
        } else {
            "not_claim_grade"
        };
        let claim_gate_reason = if certified_reopen {
            "one_scoped_local_vortex_ingest_prepare_once_smoke"
        } else {
            "ingest_minimal_records_artifact_digest_without_reopen_or_result_replay"
        };
        let mut fields = vec![
            (
                "schema_version".to_string(),
                VORTEX_INGEST_SCHEMA_VERSION.to_string(),
            ),
            ("execution_mode".to_string(), "prepared_vortex".to_string()),
            (
                "selected_execution_mode".to_string(),
                "prepared_vortex".to_string(),
            ),
            ("engine_mode".to_string(), "batch".to_string()),
            ("runtime_execution".to_string(), "true".to_string()),
            (
                "support_status".to_string(),
                "fixture_smoke_supported".to_string(),
            ),
            ("source_io_performed".to_string(), "true".to_string()),
            (
                "source_kind".to_string(),
                "local_non_vortex_file".to_string(),
            ),
            (
                "source_format".to_string(),
                self.source.source_format.as_str().to_string(),
            ),
            (
                "source_adapter_id".to_string(),
                source_adapter_id_for_format(self.source.source_format).to_string(),
            ),
            (
                "source_adapter_status".to_string(),
                "smoke_supported".to_string(),
            ),
            (
                "source_adapter_blocker_id".to_string(),
                "none_scoped_vortex_ingest_smoke_only".to_string(),
            ),
            ("ingress_route".to_string(), "vortex_ingest".to_string()),
            (
                "ingress_route_label".to_string(),
                "Vortex ingest / prepare once route".to_string(),
            ),
            ("ingress_status".to_string(), "smoke_supported".to_string()),
            (
                "ingress_certification_level".to_string(),
                self.vortex_report.certification_level.clone(),
            ),
            ("vortex_ingest_performed".to_string(), "true".to_string()),
            (
                "vortex_ingest_status".to_string(),
                "prepared_state_created".to_string(),
            ),
            (
                "vortex_ingest_blocker_id".to_string(),
                "none_scoped_local_vortex_ingest_smoke".to_string(),
            ),
            (
                "prepared_state_id".to_string(),
                self.prepared_state_id.clone(),
            ),
            (
                "prepared_state_digest".to_string(),
                self.prepared_state_digest.clone(),
            ),
            ("prepared_state_created".to_string(), "true".to_string()),
            ("prepared_state_reused".to_string(), "false".to_string()),
            ("prepared_state_reuse_hit".to_string(), "false".to_string()),
            (
                "execution_route_label".to_string(),
                "Prepared Vortex route".to_string(),
            ),
            (
                "certification_policy".to_string(),
                format!(
                    "scoped_vortex_ingest_lifecycle_{}",
                    self.vortex_report.certification_level
                ),
            ),
            (
                "certification_status".to_string(),
                certification_status.to_string(),
            ),
            (
                "certification_blocker_id".to_string(),
                certification_blocker_id.to_string(),
            ),
            (
                "source_path".to_string(),
                self.request.source_path.display().to_string(),
            ),
            (
                "source_bytes".to_string(),
                self.source.source_bytes.to_string(),
            ),
            (
                "source_digest".to_string(),
                self.source.source_digest.clone(),
            ),
            (
                "source_fingerprint_kind".to_string(),
                "local_file_content_digest".to_string(),
            ),
            ("source_state_id".to_string(), self.source_state_id.clone()),
            (
                "source_state_digest".to_string(),
                self.source_state_digest.clone(),
            ),
            (
                "source_state_contract_schema_version".to_string(),
                LOCAL_SOURCE_STATE_SCHEMA_VERSION.to_string(),
            ),
            (
                "local_input_adapter_registry_version".to_string(),
                LOCAL_INPUT_ADAPTER_REGISTRY_VERSION.to_string(),
            ),
            (
                "source_state_read_plan".to_string(),
                self.source.read_plan.status().to_string(),
            ),
            (
                "source_state_read_plan_reason".to_string(),
                self.source.read_plan.reason.to_string(),
            ),
            (
                "source_state_requested_columns".to_string(),
                self.source.read_plan.requested_columns(),
            ),
            (
                "source_state_projection_pushdown_status".to_string(),
                self.source.projection_pushdown_status.as_str().to_string(),
            ),
            (
                "source_state_materialization_layout".to_string(),
                self.source.materialization_layout.to_string(),
            ),
            (
                "source_state_parse_normalization".to_string(),
                self.source.parse_normalization.to_string(),
            ),
            (
                "source_state_columnar_preserved".to_string(),
                self.source.columnar_source_preserved.to_string(),
            ),
            (
                "source_state_record_batch_count".to_string(),
                self.source.record_batch_count.to_string(),
            ),
            (
                "source_state_materialized_column_count".to_string(),
                self.source.materialized_columns.len().to_string(),
            ),
            (
                "source_state_materialized_columns".to_string(),
                self.source.materialized_columns_field(),
            ),
            (
                "source_state_reader_projection_column_count".to_string(),
                self.source.reader_projection_columns.len().to_string(),
            ),
            (
                "source_state_reader_projection_columns".to_string(),
                self.source.reader_projection_columns_field(),
            ),
            (
                "source_state_pruned_column_count".to_string(),
                self.source.pruned_column_count().to_string(),
            ),
            (
                "source_state_column_pruning_applied".to_string(),
                self.source.column_pruning_applied().to_string(),
            ),
            (
                "source_state_reuse_allowed".to_string(),
                "false".to_string(),
            ),
            ("source_state_reuse_hit".to_string(), "false".to_string()),
            (
                "source_state_reuse_reason".to_string(),
                "created_for_scoped_vortex_ingest_smoke".to_string(),
            ),
            (
                "source_schema_digest".to_string(),
                self.source_schema_digest.clone(),
            ),
            (
                "source_column_count".to_string(),
                self.source.header.len().to_string(),
            ),
            ("source_columns".to_string(), self.source.header.join(",")),
            (
                "input_row_count".to_string(),
                self.source.row_count.to_string(),
            ),
            (
                "target_vortex_path".to_string(),
                self.request.target_path.display().to_string(),
            ),
            (
                "vortex_artifact_ref".to_string(),
                self.vortex_report.target_path.display().to_string(),
            ),
            (
                "vortex_artifact_digest".to_string(),
                self.vortex_report.artifact_digest.clone(),
            ),
            (
                "prepared_artifact_ref".to_string(),
                self.vortex_report.target_path.display().to_string(),
            ),
            (
                "prepared_artifact_digest".to_string(),
                self.vortex_report.artifact_digest.clone(),
            ),
            (
                "prepared_artifact_reuse_eligible".to_string(),
                "true".to_string(),
            ),
            (
                "layout_summary".to_string(),
                self.vortex_report.layout_summary(),
            ),
            (
                "encoding_summary".to_string(),
                self.vortex_report.encoding_summary(),
            ),
            (
                "statistics_summary".to_string(),
                self.vortex_report.statistics_summary(),
            ),
            (
                "column_family_summary".to_string(),
                self.vortex_report.column_family_summary(),
            ),
            ("vortex_prepare_included".to_string(), "true".to_string()),
            (
                "vortex_write_reopen_included".to_string(),
                "true".to_string(),
            ),
            (
                "compatibility_import_included".to_string(),
                "false".to_string(),
            ),
            (
                "preparation_included_in_timing".to_string(),
                self.vortex_report.preparation_included.to_string(),
            ),
            (
                "query_timing_starts_after_preparation".to_string(),
                self.vortex_report
                    .query_timing_starts_after_preparation
                    .to_string(),
            ),
            (
                "timing_scope".to_string(),
                self.vortex_report.timing_scope.clone(),
            ),
            (
                "certification_level".to_string(),
                self.vortex_report.certification_level.clone(),
            ),
            (
                "warm_query_timing_included".to_string(),
                "false".to_string(),
            ),
            (
                "prepare_once_millis".to_string(),
                self.prepare_once_total_millis.to_string(),
            ),
            (
                "source_read_millis".to_string(),
                self.source.read_millis.to_string(),
            ),
            (
                "compatibility_parse_millis".to_string(),
                self.source.compatibility_parse_millis.to_string(),
            ),
            (
                "source_to_columnar_millis".to_string(),
                self.source.source_to_columnar_millis.to_string(),
            ),
            (
                "vortex_array_build_millis".to_string(),
                self.vortex_report
                    .array_build_micros
                    .div_ceil(1000)
                    .to_string(),
            ),
            (
                "vortex_array_build_provider_kind".to_string(),
                self.vortex_report.array_build_provider_kind.clone(),
            ),
            (
                "vortex_array_build_provider_surface".to_string(),
                self.vortex_report.array_build_provider_surface.clone(),
            ),
            (
                "vortex_array_build_strategy".to_string(),
                self.vortex_report.array_build_strategy.clone(),
            ),
            (
                "vortex_array_build_input_layout".to_string(),
                self.vortex_report.array_build_input_layout.clone(),
            ),
            (
                "vortex_array_build_record_batch_count".to_string(),
                self.vortex_report
                    .array_build_record_batch_count
                    .to_string(),
            ),
            (
                "vortex_array_build_manual_scalar_copy_avoided".to_string(),
                self.vortex_report.manual_scalar_copy_avoided.to_string(),
            ),
            (
                "vortex_ingest_millis".to_string(),
                self.vortex_report.write_micros.div_ceil(1000).to_string(),
            ),
            (
                "vortex_write_millis".to_string(),
                self.vortex_report.write_micros.div_ceil(1000).to_string(),
            ),
            (
                "vortex_digest_millis".to_string(),
                self.vortex_report.digest_micros.div_ceil(1000).to_string(),
            ),
            (
                "vortex_reopen_millis".to_string(),
                self.vortex_report
                    .reopen_scan_micros
                    .div_ceil(1000)
                    .to_string(),
            ),
            (
                "vortex_reopen_verify_millis".to_string(),
                self.vortex_report
                    .reopen_scan_micros
                    .div_ceil(1000)
                    .to_string(),
            ),
            (
                "vortex_scan_millis".to_string(),
                self.vortex_report
                    .reopen_scan_micros
                    .div_ceil(1000)
                    .to_string(),
            ),
            ("warm_query_millis".to_string(), "0".to_string()),
            (
                "evidence_render_millis".to_string(),
                self.evidence_render_millis.to_string(),
            ),
            (
                "total_runtime_millis".to_string(),
                self.prepare_once_total_millis.to_string(),
            ),
            (
                "writer_row_count".to_string(),
                self.vortex_report.writer_row_count.to_string(),
            ),
            (
                "reopen_row_count".to_string(),
                self.vortex_report.reopen_row_count.to_string(),
            ),
            (
                "reopen_verification_status".to_string(),
                self.vortex_report.reopen_verification_status.clone(),
            ),
            (
                "vortex_artifact_bytes".to_string(),
                self.vortex_report.bytes_written.to_string(),
            ),
            (
                "source_backed_scan_evidence_status".to_string(),
                source_backed_scan_evidence_status.to_string(),
            ),
            (
                "source_backed_scan_provider_kind".to_string(),
                source_backed_scan_provider_kind.to_string(),
            ),
            (
                "source_backed_scan_provider_surface".to_string(),
                source_backed_scan_provider_surface.to_string(),
            ),
            (
                "source_backed_scan_rows_scanned".to_string(),
                self.vortex_report.reopen_row_count.to_string(),
            ),
            (
                "materialization_boundary".to_string(),
                format!(
                    "local_{}_{}_to_vortex_prepared_state",
                    self.source.source_format.as_str(),
                    self.source.materialization_layout
                ),
            ),
            (
                "legacy_materialization_boundary".to_string(),
                format!(
                    "local_{}_row_materialization_to_vortex_prepared_state",
                    self.source.source_format.as_str()
                ),
            ),
            ("data_decoded".to_string(), "true".to_string()),
            ("data_materialized".to_string(), "true".to_string()),
            (
                "source_native_io_certificate_status".to_string(),
                "scoped_compatibility_source_certificate".to_string(),
            ),
            (
                "native_io_certificate_status".to_string(),
                native_io_certificate_status.to_string(),
            ),
            (
                "output_native_io_certificate_status".to_string(),
                "not_requested".to_string(),
            ),
            (
                "upstream_vortex_write_called".to_string(),
                self.vortex_report.upstream_vortex_write_called.to_string(),
            ),
            (
                "upstream_vortex_scan_called".to_string(),
                self.vortex_report.upstream_vortex_scan_called.to_string(),
            ),
            ("object_store_io".to_string(), "false".to_string()),
            ("fallback_attempted".to_string(), "false".to_string()),
            (
                "fallback_execution_allowed".to_string(),
                "false".to_string(),
            ),
            ("external_engine_invoked".to_string(), "false".to_string()),
            (
                "claim_gate_status".to_string(),
                claim_gate_status.to_string(),
            ),
            (
                "claim_gate_reason".to_string(),
                claim_gate_reason.to_string(),
            ),
            ("performance_claim_allowed".to_string(), "false".to_string()),
            ("production_claim_allowed".to_string(), "false".to_string()),
            (
                "sql_dataframe_runtime_claim_allowed".to_string(),
                "false".to_string(),
            ),
            (
                "object_store_lakehouse_claim_allowed".to_string(),
                "false".to_string(),
            ),
        ];
        fields.extend(
            self.vortex_report
                .workspace_write_report
                .evidence_fields("vortex_ingest_output"),
        );
        fields
    }

    fn to_text(&self) -> String {
        format!(
            "ShardLoom vortex_ingest smoke\nsource: {}\ntarget: {}\nsource format: {}\nrows prepared: {}\ncolumns: {}\ncertification level: {}\nreopen verification: {}\nprepared state: {}\nfallback execution: disabled",
            self.request.source_path.display(),
            self.request.target_path.display(),
            self.source.source_format.as_str(),
            self.vortex_report.row_count,
            self.source.header.join(","),
            self.vortex_report.certification_level,
            self.vortex_report.reopen_verification_status,
            self.prepared_state_id
        )
    }
}

fn vortex_ingest_feature_blocked_fields(request: &VortexIngestRequest) -> Vec<(String, String)> {
    vec![
        (
            "schema_version".to_string(),
            VORTEX_INGEST_SCHEMA_VERSION.to_string(),
        ),
        ("execution_mode".to_string(), "prepared_vortex".to_string()),
        (
            "selected_execution_mode".to_string(),
            "prepared_vortex".to_string(),
        ),
        ("engine_mode".to_string(), "batch".to_string()),
        ("runtime_execution".to_string(), "false".to_string()),
        ("support_status".to_string(), "blocked".to_string()),
        (
            "source_path".to_string(),
            request.source_path.display().to_string(),
        ),
        (
            "target_vortex_path".to_string(),
            request.target_path.display().to_string(),
        ),
        ("source_io_performed".to_string(), "false".to_string()),
        ("ingress_route".to_string(), "vortex_ingest".to_string()),
        (
            "ingress_route_label".to_string(),
            "Vortex ingest / prepare once route".to_string(),
        ),
        ("vortex_ingest_performed".to_string(), "false".to_string()),
        (
            "vortex_ingest_status".to_string(),
            "blocked_feature_gate".to_string(),
        ),
        (
            "certification_level".to_string(),
            request.certification_level.as_str().to_string(),
        ),
        (
            "certification_status".to_string(),
            "blocked_feature_gate".to_string(),
        ),
        (
            "vortex_ingest_blocker_id".to_string(),
            "vortex_ingest.requires_vortex_write_feature".to_string(),
        ),
        ("prepared_state_created".to_string(), "false".to_string()),
        ("prepared_state_reused".to_string(), "false".to_string()),
        ("prepared_state_reuse_hit".to_string(), "false".to_string()),
        ("timing_scope".to_string(), "ingest_only".to_string()),
        (
            "claim_gate_status".to_string(),
            "not_claim_grade".to_string(),
        ),
        ("fallback_attempted".to_string(), "false".to_string()),
        (
            "fallback_execution_allowed".to_string(),
            "false".to_string(),
        ),
        ("external_engine_invoked".to_string(), "false".to_string()),
        ("object_store_io".to_string(), "false".to_string()),
        ("performance_claim_allowed".to_string(), "false".to_string()),
        ("production_claim_allowed".to_string(), "false".to_string()),
    ]
}

fn ordered_source_rows(
    header: &[String],
    rows: &[ExpressionInputRow],
) -> Result<Vec<Vec<(String, ScalarValue)>>, ShardLoomError> {
    rows.iter()
        .enumerate()
        .map(|(row_index, row)| {
            header
                .iter()
                .map(|column| {
                    row.get(column)
                        .cloned()
                        .map(|value| (column.clone(), value))
                        .ok_or_else(|| {
                            ShardLoomError::InvalidOperation(format!(
                                "local vortex_ingest row {} is missing column '{column}'; no fallback execution was attempted",
                                row_index + 1
                            ))
                        })
                })
                .collect()
        })
        .collect()
}

fn source_state_id_for_source(source: &VortexIngestSourceData) -> String {
    source.source_state_id()
}

fn source_state_digest_for_source(
    source: &VortexIngestSourceData,
    source_schema_digest: &str,
) -> String {
    source.source_state_digest(source_schema_digest)
}

fn source_adapter_id_for_format(source_format: LocalSourceFormat) -> &'static str {
    match source_format {
        LocalSourceFormat::Csv => "local_csv_input_adapter",
        LocalSourceFormat::Json => "local_json_input_adapter",
        LocalSourceFormat::JsonLines => "local_jsonl_input_adapter",
        LocalSourceFormat::Parquet => "local_parquet_input_adapter",
        LocalSourceFormat::ArrowIpc => "local_arrow_ipc_input_adapter",
        LocalSourceFormat::Avro => "local_avro_input_adapter",
        LocalSourceFormat::Orc => "local_orc_input_adapter",
    }
}

fn source_read_plan_for_sql(parsed: &ParsedSqlLocalSource) -> LocalSourceReadPlan {
    if parsed.is_join() {
        return LocalSourceReadPlan::full("join_requires_full_qualified_source_state");
    }
    if parsed.projections.iter().any(|column| column == "*") {
        return LocalSourceReadPlan::full("select_star_requires_full_source_state");
    }

    let mut columns = BTreeSet::new();
    for column in &parsed.projections {
        columns.insert(column.clone());
    }
    for column in &parsed.group_by {
        columns.insert(column.clone());
    }
    for aggregate in &parsed.aggregates {
        if let Some(column) = aggregate.column.as_ref() {
            columns.insert(column.clone());
        }
    }
    if let Some(order_by) = parsed.order_by.as_ref().filter(|_| !parsed.is_aggregate()) {
        for key in &order_by.keys {
            columns.insert(key.column.clone());
        }
    }
    for column in parsed.predicate.columns() {
        columns.insert(column.to_string());
    }
    push_projection_required_columns(parsed, &mut columns);

    LocalSourceReadPlan::required(columns, "sql_required_source_columns")
}

fn push_projection_required_columns(parsed: &ParsedSqlLocalSource, columns: &mut BTreeSet<String>) {
    for projection in &parsed.cast_projections {
        columns.insert(projection.column.clone());
    }
    for projection in &parsed.null_coalesce_projections {
        columns.insert(projection.column.clone());
    }
    for projection in &parsed.nullif_projections {
        columns.insert(projection.column.clone());
    }
    for projection in &parsed.conditional_projections {
        for column in projection.predicate.columns() {
            columns.insert(column.to_string());
        }
        if let Some(column) = projection.then_branch.source_column() {
            columns.insert(column.to_string());
        }
        if let Some(column) = projection.else_branch.source_column() {
            columns.insert(column.to_string());
        }
    }
    for projection in &parsed.predicate_projections {
        for column in projection.predicate.columns() {
            columns.insert(column.to_string());
        }
    }
    for projection in &parsed.numeric_arithmetic_projections {
        columns.insert(projection.column.clone());
    }
    for projection in &parsed.numeric_abs_projections {
        columns.insert(projection.column.clone());
    }
    for projection in &parsed.numeric_rounding_projections {
        columns.insert(projection.column.clone());
    }
    for projection in &parsed.generic_expression_projections {
        columns.extend(projection.source_columns.iter().cloned());
    }
    for projection in &parsed.date_arithmetic_projections {
        columns.insert(projection.column.clone());
    }
    for projection in &parsed.timestamp_arithmetic_projections {
        columns.insert(projection.column.clone());
    }
    for projection in &parsed.string_length_projections {
        columns.insert(projection.column.clone());
    }
    for projection in &parsed.string_transform_projections {
        columns.insert(projection.column.clone());
    }
    for projection in &parsed.string_function_projections {
        columns.extend(projection.source_columns.iter().cloned());
    }
    for projection in &parsed.date_extract_projections {
        columns.insert(projection.column.clone());
    }
    for projection in &parsed.timestamp_extract_projections {
        columns.insert(projection.column.clone());
    }
}

fn run_sql_local_source_smoke(
    request: &SqlLocalSourceRequest,
) -> Result<SqlLocalSourceReport, ShardLoomError> {
    let total_start = Instant::now();
    let mut parsed = parse_sql_local_source_statement(&request.statement)?;
    let source_read_plan = source_read_plan_for_sql(&parsed);
    let mut source = read_local_source_with_plan(&parsed.source_path, &source_read_plan)?;
    let mut right_source = parsed
        .join
        .as_ref()
        .map(|join| read_local_source(&join.right_source_path))
        .transpose()?;
    bind_sql_local_source(
        &parsed,
        &source.header,
        right_source.as_ref().map(|source| source.header.as_slice()),
    )?;
    materialize_in_subquery_predicates(&mut parsed.predicate)?;
    apply_temporal_literal_column_coercions(&parsed, &mut source, right_source.as_mut())?;
    resolve_conditional_projection_branch_dtypes(&mut parsed, &source)?;
    validate_null_coalesce_projection_values(&parsed, &source, right_source.as_ref())?;
    validate_nullif_projection_values(&parsed, &source, right_source.as_ref())?;

    let compute_start = Instant::now();
    let (selected_row_count, joined_row_count, output_rows) =
        if let Some(right_source) = right_source.as_ref() {
            let join_output = evaluate_join_output(&parsed, &source, right_source)?;
            (
                join_output.selected_row_count,
                join_output.joined_row_count,
                join_output.output_rows,
            )
        } else {
            let selected_row_indexes = selected_row_indexes(&parsed, &source)?;
            let output_rows = if parsed.is_grouped_aggregate() {
                let selected_rows = selected_input_rows(&source, &selected_row_indexes)?;
                evaluate_grouped_aggregate_output(&parsed, &selected_rows)?
            } else if parsed.is_aggregate() {
                let selected_rows = selected_input_rows(&source, &selected_row_indexes)?;
                evaluate_scalar_aggregate_output(&parsed, &selected_rows)?
            } else {
                let selected_row_indexes =
                    ordered_projection_row_indexes(&parsed, &source, &selected_row_indexes)?;
                evaluate_projection_output(&parsed, &source, &selected_row_indexes)?
            };
            (selected_row_indexes.len(), 0, output_rows)
        };
    let operator_compute_millis = compute_start.elapsed().as_millis();

    let evidence_start = Instant::now();
    let output_columns = output_column_names(&parsed, &source);
    let result_jsonl = rows_to_jsonl(&output_rows);
    let result_digest = fnv64_digest(&result_jsonl);
    let inline_output_content = request
        .output_format
        .inline_result_rows(&output_columns, &output_rows)?;
    let inline_output_digest = fnv64_digest_bytes(&inline_output_content);
    let prepared_outputs = prepare_sql_outputs(request, &output_columns, &output_rows)?;
    let source_schema_digest = fnv64_digest(&source.header.join(","));
    let plan_digest = sql_local_source_plan_digest(
        &parsed,
        &source,
        right_source.as_ref(),
        request,
        &source_schema_digest,
    );
    let evidence_render_millis = evidence_start.elapsed().as_millis();
    let inline_output_bytes = u64::try_from(inline_output_content.len()).unwrap_or(u64::MAX);
    preflight_sql_output_writes(request)?;
    let written_outputs = write_sql_outputs(
        prepared_outputs,
        &output_columns,
        &output_rows,
        request.allow_overwrite,
    )?;
    let output_write_millis = written_outputs
        .iter()
        .map(|output| output.write_millis)
        .sum::<u128>();
    let (output_digest, output_bytes) = primary_output_evidence(
        request,
        &written_outputs,
        &inline_output_digest,
        inline_output_bytes,
    );

    Ok(SqlLocalSourceReport {
        request: request.clone(),
        parsed,
        source,
        right_source,
        selected_row_count,
        joined_row_count,
        output_rows,
        result_jsonl,
        plan_digest,
        source_schema_digest,
        result_digest,
        output_digest,
        output_write_millis,
        output_bytes,
        written_outputs,
        operator_compute_millis,
        evidence_render_millis,
        total_runtime_millis: total_start.elapsed().as_millis(),
    })
}

fn sql_local_source_plan_digest(
    parsed: &ParsedSqlLocalSource,
    source: &CsvSourceData,
    right_source: Option<&CsvSourceData>,
    request: &SqlLocalSourceRequest,
    source_schema_digest: &str,
) -> String {
    fnv64_digest(&format!(
        "{}|{}|{}|{}|{}|{}|{}|{}",
        parsed.normalized_statement,
        source_schema_digest,
        source.source_digest,
        right_source.map_or_else(String::new, |source| source.source_digest.clone()),
        parsed.predicate.in_subquery_plan_digest_fragment(),
        request.output_format.as_str(),
        fanout_plan_digest_fragment(request),
        source.read_plan.requested_columns()
    ))
}

fn materialize_in_subquery_predicates(
    predicate: &mut ParsedPredicate,
) -> Result<(), ShardLoomError> {
    match predicate {
        ParsedPredicate::InSubquery { subquery, .. } => {
            if subquery.source_format.is_some() {
                return Ok(());
            }
            let source = read_local_source_with_plan(
                &subquery.source_path,
                &LocalSourceReadPlan::required(
                    BTreeSet::from([subquery.source_column.clone()]),
                    "in_subquery_required_source_column",
                ),
            )?;
            require_header_column(
                &source.header,
                &subquery.source_column,
                "IN subquery source column",
                "subquery source header",
            )?;
            if source.rows.len() > MAX_IN_LIST_VALUES {
                return Err(unsupported_sql_error(&format!(
                    "IN subquery predicates admit at most {MAX_IN_LIST_VALUES} materialized values in this scoped runtime slice"
                )));
            }
            subquery.values = source
                .rows
                .iter()
                .map(|row| {
                    row.get(&subquery.source_column).cloned().ok_or_else(|| {
                        unsupported_sql_error(&format!(
                            "IN subquery source column {:?} is not present in a materialized row",
                            subquery.source_column
                        ))
                    })
                })
                .collect::<Result<Vec<_>, ShardLoomError>>()?;
            subquery.source_format = Some(source.source_format);
            subquery.source_digest = Some(source.source_digest);
            Ok(())
        }
        ParsedPredicate::Logical { left, right, .. } => {
            materialize_in_subquery_predicates(left)?;
            materialize_in_subquery_predicates(right)
        }
        ParsedPredicate::Not { inner } => materialize_in_subquery_predicates(inner),
        ParsedPredicate::All
        | ParsedPredicate::Compare { .. }
        | ParsedPredicate::CastCompare { .. }
        | ParsedPredicate::NumericArithmeticCompare { .. }
        | ParsedPredicate::NumericAbsCompare { .. }
        | ParsedPredicate::NumericRoundingCompare { .. }
        | ParsedPredicate::GenericExpressionCompare { .. }
        | ParsedPredicate::DateArithmeticCompare { .. }
        | ParsedPredicate::TimestampArithmeticCompare { .. }
        | ParsedPredicate::DateExtractCompare { .. }
        | ParsedPredicate::StringLengthCompare { .. }
        | ParsedPredicate::TimestampExtractCompare { .. }
        | ParsedPredicate::BooleanPredicate { .. }
        | ParsedPredicate::IsNull { .. }
        | ParsedPredicate::IsNotNull { .. }
        | ParsedPredicate::InList { .. }
        | ParsedPredicate::StringMatch { .. }
        | ParsedPredicate::StringTransformCompare { .. }
        | ParsedPredicate::StringFunctionCompare { .. } => Ok(()),
    }
}

fn selected_row_indexes(
    parsed: &ParsedSqlLocalSource,
    source: &CsvSourceData,
) -> Result<Vec<usize>, ShardLoomError> {
    if parsed.predicate.is_all() {
        return Ok((0..source.rows.len()).collect());
    }
    let predicate_expression = parsed.predicate.to_expression()?;
    let filter = evaluate_filter(&predicate_expression, &source.rows);
    if filter.has_errors() {
        return Err(ShardLoomError::InvalidOperation(format!(
            "SQL local-source predicate evaluation failed: {}",
            filter
                .diagnostics
                .first()
                .map_or("unknown diagnostic", |diagnostic| diagnostic
                    .reason
                    .as_deref()
                    .unwrap_or(diagnostic.message.as_str()))
        )));
    }
    Ok(filter.selected_row_indexes)
}

fn selected_input_rows<'a>(
    source: &'a CsvSourceData,
    selected_row_indexes: &[usize],
) -> Result<Vec<&'a ExpressionInputRow>, ShardLoomError> {
    selected_row_indexes
        .iter()
        .map(|row_index| {
            source.rows.get(*row_index).ok_or_else(|| {
                ShardLoomError::InvalidOperation("selected row index is out of bounds".to_string())
            })
        })
        .collect()
}

fn evaluate_projection_output(
    parsed: &ParsedSqlLocalSource,
    source: &CsvSourceData,
    selected_row_indexes: &[usize],
) -> Result<Vec<Vec<(String, ScalarValue)>>, ShardLoomError> {
    let projection_expressions = projection_expressions(parsed, source)?;
    let mut output_rows = Vec::new();
    for row_index in selected_row_indexes.iter().take(parsed.limit) {
        let row = source.rows.get(*row_index).ok_or_else(|| {
            ShardLoomError::InvalidOperation("selected row index is out of bounds".to_string())
        })?;
        let projection = evaluate_projection(&projection_expressions, row);
        if projection.has_errors() {
            return Err(ShardLoomError::InvalidOperation(format!(
                "SQL local-source projection evaluation failed: {}",
                projection
                    .diagnostics
                    .first()
                    .map_or("unknown diagnostic", |diagnostic| diagnostic
                        .reason
                        .as_deref()
                        .unwrap_or(diagnostic.message.as_str()))
            )));
        }
        output_rows.push(
            projection
                .projected_columns
                .into_iter()
                .map(|column| (column.name, column.value))
                .collect(),
        );
    }
    Ok(output_rows)
}

fn projection_expressions(
    parsed: &ParsedSqlLocalSource,
    source: &CsvSourceData,
) -> Result<Vec<Expression>, ShardLoomError> {
    ordered_projection_expressions(parsed, &source.header, "project")
}

fn ordered_projection_expressions(
    parsed: &ParsedSqlLocalSource,
    header: &[String],
    raw_expr_prefix: &str,
) -> Result<Vec<Expression>, ShardLoomError> {
    let mut expressions = Vec::new();
    for output in &parsed.projection_order {
        append_ordered_projection_expression(
            &mut expressions,
            parsed,
            output,
            header,
            raw_expr_prefix,
        )?;
    }
    Ok(expressions)
}

#[allow(clippy::too_many_lines)]
fn append_ordered_projection_expression(
    expressions: &mut Vec<Expression>,
    parsed: &ParsedSqlLocalSource,
    output: &ParsedProjectionOutput,
    header: &[String],
    raw_expr_prefix: &str,
) -> Result<(), ShardLoomError> {
    match output {
        ParsedProjectionOutput::Raw(column) if column == "*" => {
            if header.is_empty() {
                return Err(unsupported_sql_error(
                    "SELECT * is not admitted for scoped join projections",
                ));
            }
            for column in header {
                expressions.push(Expression::column(
                    ExprId::new(format!("{raw_expr_prefix}.{column}"))?,
                    ColumnRef::new(column.clone())?,
                ));
            }
        }
        ParsedProjectionOutput::Raw(column) => {
            expressions.push(Expression::column(
                ExprId::new(format!("{raw_expr_prefix}.{column}"))?,
                ColumnRef::new(column.clone())?,
            ));
        }
        ParsedProjectionOutput::Literal(alias) => expressions.push(literal_projection_expression(
            find_projection_by_alias(&parsed.literal_projections, alias, "literal projection")?,
        )?),
        ParsedProjectionOutput::Cast(alias) => expressions.push(cast_projection_expression(
            find_projection_by_alias(&parsed.cast_projections, alias, "cast projection")?,
        )?),
        ParsedProjectionOutput::NullCoalesce(alias) => expressions.push(
            null_coalesce_projection_expression(find_projection_by_alias(
                &parsed.null_coalesce_projections,
                alias,
                "null coalesce projection",
            )?)?,
        ),
        ParsedProjectionOutput::NullIf(alias) => expressions.push(nullif_projection_expression(
            find_projection_by_alias(&parsed.nullif_projections, alias, "nullif projection")?,
        )?),
        ParsedProjectionOutput::Conditional(alias) => expressions.push(
            conditional_projection_expression(find_projection_by_alias(
                &parsed.conditional_projections,
                alias,
                "conditional projection",
            )?)?,
        ),
        ParsedProjectionOutput::Predicate(alias) => {
            expressions.push(predicate_projection_expression(find_projection_by_alias(
                &parsed.predicate_projections,
                alias,
                "predicate projection",
            )?)?);
        }
        ParsedProjectionOutput::NumericArithmetic(alias) => expressions.push(
            numeric_arithmetic_projection_expression(find_projection_by_alias(
                &parsed.numeric_arithmetic_projections,
                alias,
                "numeric arithmetic projection",
            )?)?,
        ),
        ParsedProjectionOutput::NumericAbs(alias) => expressions.push(
            numeric_abs_projection_expression(find_projection_by_alias(
                &parsed.numeric_abs_projections,
                alias,
                "numeric abs projection",
            )?)?,
        ),
        ParsedProjectionOutput::NumericRounding(alias) => expressions.push(
            numeric_rounding_projection_expression(find_projection_by_alias(
                &parsed.numeric_rounding_projections,
                alias,
                "numeric rounding projection",
            )?)?,
        ),
        ParsedProjectionOutput::GenericExpression(alias) => expressions.push(
            generic_expression_projection_expression(find_projection_by_alias(
                &parsed.generic_expression_projections,
                alias,
                "generic expression projection",
            )?)?,
        ),
        ParsedProjectionOutput::DateArithmetic(alias) => expressions.push(
            date_arithmetic_projection_expression(find_projection_by_alias(
                &parsed.date_arithmetic_projections,
                alias,
                "date arithmetic projection",
            )?)?,
        ),
        ParsedProjectionOutput::TimestampArithmetic(alias) => expressions.push(
            timestamp_arithmetic_projection_expression(find_projection_by_alias(
                &parsed.timestamp_arithmetic_projections,
                alias,
                "timestamp arithmetic projection",
            )?)?,
        ),
        ParsedProjectionOutput::StringLength(alias) => expressions.push(
            string_length_projection_expression(find_projection_by_alias(
                &parsed.string_length_projections,
                alias,
                "string length projection",
            )?)?,
        ),
        ParsedProjectionOutput::StringTransform(alias) => expressions.push(
            string_transform_projection_expression(find_projection_by_alias(
                &parsed.string_transform_projections,
                alias,
                "string transform projection",
            )?)?,
        ),
        ParsedProjectionOutput::StringFunction(alias) => expressions.push(
            string_function_projection_expression(find_projection_by_alias(
                &parsed.string_function_projections,
                alias,
                "string function projection",
            )?)?,
        ),
        ParsedProjectionOutput::DateExtract(alias) => expressions.push(
            date_extract_projection_expression(find_projection_by_alias(
                &parsed.date_extract_projections,
                alias,
                "date extract projection",
            )?)?,
        ),
        ParsedProjectionOutput::TimestampExtract(alias) => expressions.push(
            timestamp_extract_projection_expression(find_projection_by_alias(
                &parsed.timestamp_extract_projections,
                alias,
                "timestamp extract projection",
            )?)?,
        ),
    }
    Ok(())
}

fn find_projection_by_alias<'a, T: ProjectionAlias>(
    projections: &'a [T],
    alias: &str,
    family: &str,
) -> Result<&'a T, ShardLoomError> {
    projections
        .iter()
        .find(|projection| projection.alias() == alias)
        .ok_or_else(|| {
            ShardLoomError::InvalidOperation(format!(
                "projection order references missing {family} alias {alias:?}"
            ))
        })
}

fn generic_expression_projection_expression(
    projection: &ParsedGenericExpressionProjection,
) -> Result<Expression, ShardLoomError> {
    Ok(Expression::new(
        ExprId::new(format!("project.alias.{}", projection.alias))?,
        ExpressionKind::Alias {
            expr: Box::new(projection.expression.clone()),
            alias: projection.alias.clone(),
        },
    ))
}

fn cast_projection_expression(
    projection: &ParsedCastProjection,
) -> Result<Expression, ShardLoomError> {
    let cast = projection.mode.build_expression(
        ExprId::new(format!("project.cast.{}", projection.alias))?,
        Expression::column(
            ExprId::new(format!("project.{}", projection.column))?,
            ColumnRef::new(projection.column.clone())?,
        ),
        projection.target_dtype.clone(),
    );
    Ok(Expression::new(
        ExprId::new(format!("project.alias.{}", projection.alias))?,
        ExpressionKind::Alias {
            expr: Box::new(cast),
            alias: projection.alias.clone(),
        },
    ))
}

fn date_arithmetic_projection_expression(
    projection: &ParsedDateArithmeticProjection,
) -> Result<Expression, ShardLoomError> {
    let arithmetic = Expression::new(
        ExprId::new(format!("project.date_arithmetic.{}", projection.alias))?,
        ExpressionKind::FunctionCall {
            name: projection.op.function_name().to_string(),
            args: vec![
                Expression::column(
                    ExprId::new(format!("project.{}", projection.column))?,
                    ColumnRef::new(projection.column.clone())?,
                ),
                Expression::literal(
                    ExprId::new(format!("project.date_arithmetic.days.{}", projection.alias))?,
                    ScalarValue::Int64(i64::from(projection.day_count)),
                ),
            ],
        },
    );
    Ok(Expression::new(
        ExprId::new(format!("project.alias.{}", projection.alias))?,
        ExpressionKind::Alias {
            expr: Box::new(arithmetic),
            alias: projection.alias.clone(),
        },
    ))
}

fn timestamp_arithmetic_projection_expression(
    projection: &ParsedTimestampArithmeticProjection,
) -> Result<Expression, ShardLoomError> {
    let arithmetic = Expression::new(
        ExprId::new(format!("project.timestamp_arithmetic.{}", projection.alias))?,
        ExpressionKind::FunctionCall {
            name: projection.op.function_name().to_string(),
            args: vec![
                Expression::column(
                    ExprId::new(format!("project.{}", projection.column))?,
                    ColumnRef::new(projection.column.clone())?,
                ),
                Expression::literal(
                    ExprId::new(format!(
                        "project.timestamp_arithmetic.seconds.{}",
                        projection.alias
                    ))?,
                    ScalarValue::Int64(projection.second_count),
                ),
            ],
        },
    );
    Ok(Expression::new(
        ExprId::new(format!("project.alias.{}", projection.alias))?,
        ExpressionKind::Alias {
            expr: Box::new(arithmetic),
            alias: projection.alias.clone(),
        },
    ))
}

fn null_coalesce_projection_expression(
    projection: &ParsedNullCoalesceProjection,
) -> Result<Expression, ShardLoomError> {
    let coalesce = Expression::new(
        ExprId::new(format!("project.null_coalesce.{}", projection.alias))?,
        ExpressionKind::FunctionCall {
            name: "coalesce".to_string(),
            args: vec![
                Expression::column(
                    ExprId::new(format!("project.{}", projection.column))?,
                    ColumnRef::new(projection.column.clone())?,
                ),
                Expression::literal(
                    ExprId::new(format!(
                        "project.null_coalesce.literal.{}",
                        projection.alias
                    ))?,
                    projection.fallback.clone(),
                ),
            ],
        },
    );
    Ok(Expression::new(
        ExprId::new(format!("project.alias.{}", projection.alias))?,
        ExpressionKind::Alias {
            expr: Box::new(coalesce),
            alias: projection.alias.clone(),
        },
    ))
}

fn nullif_projection_expression(
    projection: &ParsedNullIfProjection,
) -> Result<Expression, ShardLoomError> {
    let nullif = Expression::new(
        ExprId::new(format!("project.nullif.{}", projection.alias))?,
        ExpressionKind::FunctionCall {
            name: "nullif".to_string(),
            args: vec![
                Expression::column(
                    ExprId::new(format!("project.{}", projection.column))?,
                    ColumnRef::new(projection.column.clone())?,
                ),
                Expression::literal(
                    ExprId::new(format!("project.nullif.literal.{}", projection.alias))?,
                    projection.sentinel.clone(),
                ),
            ],
        },
    );
    Ok(Expression::new(
        ExprId::new(format!("project.alias.{}", projection.alias))?,
        ExpressionKind::Alias {
            expr: Box::new(nullif),
            alias: projection.alias.clone(),
        },
    ))
}

fn conditional_projection_expression(
    projection: &ParsedConditionalProjection,
) -> Result<Expression, ShardLoomError> {
    let case_when = Expression::new(
        ExprId::new(format!("project.conditional.{}", projection.alias))?,
        ExpressionKind::FunctionCall {
            name: "case_when".to_string(),
            args: vec![
                projection.predicate.to_expression()?,
                projection.then_branch.to_expression(ExprId::new(format!(
                    "project.conditional.then.{}",
                    projection.alias
                ))?)?,
                projection.else_branch.to_expression(ExprId::new(format!(
                    "project.conditional.else.{}",
                    projection.alias
                ))?)?,
            ],
        },
    );
    Ok(Expression::new(
        ExprId::new(format!("project.alias.{}", projection.alias))?,
        ExpressionKind::Alias {
            expr: Box::new(case_when),
            alias: projection.alias.clone(),
        },
    ))
}

fn predicate_projection_expression(
    projection: &ParsedPredicateProjection,
) -> Result<Expression, ShardLoomError> {
    Ok(Expression::new(
        ExprId::new(format!("project.alias.{}", projection.alias))?,
        ExpressionKind::Alias {
            expr: Box::new(projection.predicate.to_expression()?),
            alias: projection.alias.clone(),
        },
    ))
}

fn numeric_arithmetic_projection_expression(
    projection: &ParsedNumericArithmeticProjection,
) -> Result<Expression, ShardLoomError> {
    let binary = Expression::new(
        ExprId::new(format!("project.numeric_arithmetic.{}", projection.alias))?,
        ExpressionKind::Binary {
            left: Box::new(Expression::column(
                ExprId::new(format!("project.{}", projection.column))?,
                ColumnRef::new(projection.column.clone())?,
            )),
            op: projection.op.binary_op(),
            right: Box::new(Expression::literal(
                ExprId::new(format!(
                    "project.numeric_arithmetic.literal.{}",
                    projection.alias
                ))?,
                projection.rhs.clone(),
            )),
        },
    );
    Ok(Expression::new(
        ExprId::new(format!("project.alias.{}", projection.alias))?,
        ExpressionKind::Alias {
            expr: Box::new(binary),
            alias: projection.alias.clone(),
        },
    ))
}

fn numeric_abs_projection_expression(
    projection: &ParsedNumericAbsProjection,
) -> Result<Expression, ShardLoomError> {
    let abs = Expression::new(
        ExprId::new(format!("project.numeric_abs.{}", projection.alias))?,
        ExpressionKind::FunctionCall {
            name: "abs".to_string(),
            args: vec![Expression::column(
                ExprId::new(format!("project.{}", projection.column))?,
                ColumnRef::new(projection.column.clone())?,
            )],
        },
    );
    Ok(Expression::new(
        ExprId::new(format!("project.alias.{}", projection.alias))?,
        ExpressionKind::Alias {
            expr: Box::new(abs),
            alias: projection.alias.clone(),
        },
    ))
}

fn numeric_rounding_projection_expression(
    projection: &ParsedNumericRoundingProjection,
) -> Result<Expression, ShardLoomError> {
    let rounded = Expression::new(
        ExprId::new(format!("project.numeric_rounding.{}", projection.alias))?,
        ExpressionKind::FunctionCall {
            name: projection.op.function_name().to_string(),
            args: vec![Expression::column(
                ExprId::new(format!("project.{}", projection.column))?,
                ColumnRef::new(projection.column.clone())?,
            )],
        },
    );
    Ok(Expression::new(
        ExprId::new(format!("project.alias.{}", projection.alias))?,
        ExpressionKind::Alias {
            expr: Box::new(rounded),
            alias: projection.alias.clone(),
        },
    ))
}

fn string_transform_projection_expression(
    projection: &ParsedStringTransformProjection,
) -> Result<Expression, ShardLoomError> {
    let transformed = Expression::new(
        ExprId::new(format!("project.string_transform.{}", projection.alias))?,
        ExpressionKind::FunctionCall {
            name: projection.op.function_name().to_string(),
            args: vec![Expression::column(
                ExprId::new(format!("project.{}", projection.column))?,
                ColumnRef::new(projection.column.clone())?,
            )],
        },
    );
    Ok(Expression::new(
        ExprId::new(format!("project.alias.{}", projection.alias))?,
        ExpressionKind::Alias {
            expr: Box::new(transformed),
            alias: projection.alias.clone(),
        },
    ))
}

fn string_length_projection_expression(
    projection: &ParsedStringLengthProjection,
) -> Result<Expression, ShardLoomError> {
    let length = Expression::new(
        ExprId::new(format!("project.string_length.{}", projection.alias))?,
        ExpressionKind::FunctionCall {
            name: "length".to_string(),
            args: vec![Expression::column(
                ExprId::new(format!("project.{}", projection.column))?,
                ColumnRef::new(projection.column.clone())?,
            )],
        },
    );
    Ok(Expression::new(
        ExprId::new(format!("project.alias.{}", projection.alias))?,
        ExpressionKind::Alias {
            expr: Box::new(length),
            alias: projection.alias.clone(),
        },
    ))
}

fn string_function_projection_expression(
    projection: &ParsedStringFunctionProjection,
) -> Result<Expression, ShardLoomError> {
    Ok(Expression::new(
        ExprId::new(format!("project.alias.{}", projection.alias))?,
        ExpressionKind::Alias {
            expr: Box::new(projection.expression.clone()),
            alias: projection.alias.clone(),
        },
    ))
}

fn date_extract_projection_expression(
    projection: &ParsedDateExtractProjection,
) -> Result<Expression, ShardLoomError> {
    let extracted = Expression::new(
        ExprId::new(format!("project.date_extract.{}", projection.alias))?,
        ExpressionKind::FunctionCall {
            name: projection.op.function_name().to_string(),
            args: vec![Expression::column(
                ExprId::new(format!("project.{}", projection.column))?,
                ColumnRef::new(projection.column.clone())?,
            )],
        },
    );
    Ok(Expression::new(
        ExprId::new(format!("project.alias.{}", projection.alias))?,
        ExpressionKind::Alias {
            expr: Box::new(extracted),
            alias: projection.alias.clone(),
        },
    ))
}

fn timestamp_extract_projection_expression(
    projection: &ParsedTimestampExtractProjection,
) -> Result<Expression, ShardLoomError> {
    let extracted = Expression::new(
        ExprId::new(format!("project.timestamp_extract.{}", projection.alias))?,
        ExpressionKind::FunctionCall {
            name: projection.op.function_name().to_string(),
            args: vec![Expression::column(
                ExprId::new(format!("project.{}", projection.column))?,
                ColumnRef::new(projection.column.clone())?,
            )],
        },
    );
    Ok(Expression::new(
        ExprId::new(format!("project.alias.{}", projection.alias))?,
        ExpressionKind::Alias {
            expr: Box::new(extracted),
            alias: projection.alias.clone(),
        },
    ))
}

fn literal_projection_expression(
    projection: &ParsedLiteralProjection,
) -> Result<Expression, ShardLoomError> {
    let literal = Expression::literal(
        ExprId::new(format!("project.literal.{}", projection.alias))?,
        projection.value.clone(),
    );
    Ok(Expression::new(
        ExprId::new(format!("project.alias.{}", projection.alias))?,
        ExpressionKind::Alias {
            expr: Box::new(literal),
            alias: projection.alias.clone(),
        },
    ))
}

fn ordered_projection_row_indexes(
    parsed: &ParsedSqlLocalSource,
    source: &CsvSourceData,
    selected_row_indexes: &[usize],
) -> Result<Vec<usize>, ShardLoomError> {
    let mut ordered = selected_row_indexes.to_vec();
    let Some(order_by) = parsed.order_by.as_ref() else {
        return Ok(ordered);
    };
    let mut sort_values = Vec::with_capacity(ordered.len());
    for row_index in &ordered {
        let row = source.rows.get(*row_index).ok_or_else(|| {
            ShardLoomError::InvalidOperation("selected row index is out of bounds".to_string())
        })?;
        let values = sort_values_for_row(row, order_by, "ORDER BY column")?;
        sort_values.push((*row_index, values));
    }
    validate_sort_value_families(&sort_values)?;
    sort_values.sort_by(|(left_index, left_values), (right_index, right_values)| {
        let ordering = compare_order_by_values(order_by, left_values, right_values);
        ordering.then_with(|| left_index.cmp(right_index))
    });
    ordered.clear();
    ordered.extend(sort_values.into_iter().map(|(row_index, _value)| row_index));
    Ok(ordered)
}

fn evaluate_join_output(
    parsed: &ParsedSqlLocalSource,
    left_source: &CsvSourceData,
    right_source: &CsvSourceData,
) -> Result<JoinEvaluationOutput, ShardLoomError> {
    let join = parsed.join.as_ref().ok_or_else(|| {
        ShardLoomError::InvalidOperation("join evaluation requested without join plan".to_string())
    })?;
    let left_alias = parsed.source_alias.as_ref().ok_or_else(|| {
        ShardLoomError::InvalidOperation("join evaluation requires a left alias".to_string())
    })?;

    let right_rows_by_key = build_join_right_rows_by_key(join, right_source)?;

    let predicate_expression = if parsed.predicate.is_all() {
        None
    } else {
        Some(parsed.predicate.to_expression()?)
    };
    let projection_expressions = if parsed.is_aggregate() {
        Vec::new()
    } else {
        join_projection_expressions(parsed)?
    };
    let mut joined_row_count = 0usize;
    let mut selected_row_count = 0usize;
    let mut output_rows = Vec::new();
    let mut selected_join_rows = Vec::new();
    for left_row in &left_source.rows {
        let Some(key_parts) = join_key_parts(
            left_row,
            join.key_pairs.iter().map(|pair| &pair.left),
            "left",
        )?
        else {
            continue;
        };
        if let Some(right_matches) = right_rows_by_key.get(&key_parts) {
            for right_row in right_matches {
                if joined_row_count >= MAX_JOIN_CANDIDATE_ROWS {
                    return Err(unsupported_sql_error(&format!(
                        "JOIN candidate row count exceeds scoped smoke cap of {MAX_JOIN_CANDIDATE_ROWS}; duplicate-key joins need a later streaming/hash-join runtime slice"
                    )));
                }
                joined_row_count += 1;
                let joined_row = qualified_join_row(
                    left_alias,
                    &left_source.header,
                    left_row,
                    &join.right_alias,
                    &right_source.header,
                    right_row,
                );
                if evaluate_join_predicate(predicate_expression.as_ref(), &joined_row)? {
                    selected_row_count += 1;
                    selected_join_rows.push(joined_row);
                }
            }
        }
    }
    if parsed.is_grouped_aggregate() {
        let selected_join_row_refs = selected_join_rows.iter().collect::<Vec<_>>();
        output_rows = evaluate_grouped_aggregate_output(parsed, &selected_join_row_refs)?;
    } else if parsed.is_aggregate() {
        let selected_join_row_refs = selected_join_rows.iter().collect::<Vec<_>>();
        output_rows = evaluate_scalar_aggregate_output(parsed, &selected_join_row_refs)?;
    } else {
        let selected_join_row_refs = selected_join_rows.iter().collect::<Vec<_>>();
        let ordered_join_row_indexes = ordered_join_row_indexes(parsed, &selected_join_row_refs)?;
        for row_index in ordered_join_row_indexes.into_iter().take(parsed.limit) {
            let joined_row = selected_join_rows.get(row_index).ok_or_else(|| {
                ShardLoomError::InvalidOperation(
                    "ordered join row index is out of bounds".to_string(),
                )
            })?;
            output_rows.push(evaluate_join_projection(
                &projection_expressions,
                joined_row,
            )?);
        }
    }

    Ok(JoinEvaluationOutput {
        joined_row_count,
        selected_row_count,
        output_rows,
    })
}

fn join_projection_expressions(
    parsed: &ParsedSqlLocalSource,
) -> Result<Vec<Expression>, ShardLoomError> {
    ordered_projection_expressions(parsed, &[], "join.project")
}

fn ordered_join_row_indexes(
    parsed: &ParsedSqlLocalSource,
    rows: &[&ExpressionInputRow],
) -> Result<Vec<usize>, ShardLoomError> {
    let mut ordered = (0..rows.len()).collect::<Vec<_>>();
    let Some(order_by) = parsed.order_by.as_ref() else {
        return Ok(ordered);
    };
    let mut sort_values = Vec::with_capacity(ordered.len());
    for row_index in &ordered {
        let row = rows.get(*row_index).ok_or_else(|| {
            ShardLoomError::InvalidOperation("selected join row index is out of bounds".to_string())
        })?;
        let values = sort_values_for_row(row, order_by, "ORDER BY join column")?;
        sort_values.push((*row_index, values));
    }
    validate_sort_value_families(&sort_values)?;
    sort_values.sort_by(|(left_index, left_values), (right_index, right_values)| {
        let ordering = compare_order_by_values(order_by, left_values, right_values);
        ordering.then_with(|| left_index.cmp(right_index))
    });
    ordered.clear();
    ordered.extend(sort_values.into_iter().map(|(row_index, _value)| row_index));
    Ok(ordered)
}

fn sort_values_for_row(
    row: &ExpressionInputRow,
    order_by: &ParsedOrderBy,
    missing_label: &str,
) -> Result<Vec<SortValue>, ShardLoomError> {
    let mut values = Vec::with_capacity(order_by.keys.len());
    for key in &order_by.keys {
        let value = row.get(&key.column).ok_or_else(|| {
            unsupported_sql_error(&format!(
                "{missing_label} {:?} is not present in the row",
                key.column
            ))
        })?;
        values.push(SortValue::try_from_scalar(value)?);
    }
    Ok(values)
}

fn compare_order_by_values(
    order_by: &ParsedOrderBy,
    left_values: &[SortValue],
    right_values: &[SortValue],
) -> Ordering {
    for (key, (left_value, right_value)) in order_by
        .keys
        .iter()
        .zip(left_values.iter().zip(right_values.iter()))
    {
        let ordering = left_value.cmp(right_value);
        let ordering = match key.direction {
            SortDirection::Asc => ordering,
            SortDirection::Desc => ordering.reverse(),
        };
        if ordering != Ordering::Equal {
            return ordering;
        }
    }
    Ordering::Equal
}

fn ordered_output_row_indexes(
    rows: &[Vec<(String, ScalarValue)>],
    order_by: &ParsedOrderBy,
    missing_label: &str,
) -> Result<Vec<usize>, ShardLoomError> {
    let mut sort_values = Vec::with_capacity(rows.len());
    for (row_index, row) in rows.iter().enumerate() {
        let mut input_row = ExpressionInputRow::new();
        for (column, value) in row {
            input_row.insert(column.clone(), value.clone());
        }
        let values = sort_values_for_row(&input_row, order_by, missing_label)?;
        sort_values.push((row_index, values));
    }
    validate_sort_value_families(&sort_values)?;
    sort_values.sort_by(|(left_index, left_values), (right_index, right_values)| {
        let ordering = compare_order_by_values(order_by, left_values, right_values);
        ordering.then_with(|| left_index.cmp(right_index))
    });
    Ok(sort_values
        .into_iter()
        .map(|(row_index, _values)| row_index)
        .collect())
}

fn validate_sort_value_families(
    sort_values: &[(usize, Vec<SortValue>)],
) -> Result<(), ShardLoomError> {
    let Some((_, first_values)) = sort_values.first() else {
        return Ok(());
    };
    for (key_index, first_value) in first_values.iter().enumerate() {
        let expected_family = first_value.family();
        if sort_values.iter().any(|(_, values)| {
            values
                .get(key_index)
                .is_some_and(|value| value.family() != expected_family)
        }) {
            return Err(unsupported_sql_error(
                "ORDER BY mixed numeric and UTF-8 values within one sort key are not admitted in this scoped top-N smoke",
            ));
        }
    }
    Ok(())
}

fn build_join_right_rows_by_key<'a>(
    join: &ParsedJoin,
    right_source: &'a CsvSourceData,
) -> Result<BTreeMap<Vec<String>, Vec<&'a ExpressionInputRow>>, ShardLoomError> {
    let mut right_rows_by_key: BTreeMap<Vec<String>, Vec<&ExpressionInputRow>> = BTreeMap::new();
    for right_row in &right_source.rows {
        let Some(key_parts) = join_key_parts(
            right_row,
            join.key_pairs.iter().map(|pair| &pair.right),
            "right",
        )?
        else {
            continue;
        };
        right_rows_by_key
            .entry(key_parts)
            .or_default()
            .push(right_row);
    }
    Ok(right_rows_by_key)
}

fn join_key_parts<'a>(
    row: &ExpressionInputRow,
    columns: impl IntoIterator<Item = &'a QualifiedColumn>,
    side: &str,
) -> Result<Option<Vec<String>>, ShardLoomError> {
    let mut parts = Vec::new();
    for column in columns {
        let Some(key_value) = row.get(&column.column) else {
            return Err(unsupported_sql_error(&format!(
                "JOIN {side} key column {:?} is not present in the {side} source row",
                column.column
            )));
        };
        if matches!(key_value, ScalarValue::Null) {
            return Ok(None);
        }
        parts.push(key_value.summary());
    }
    Ok(Some(parts))
}

fn evaluate_join_predicate(
    predicate_expression: Option<&Expression>,
    joined_row: &ExpressionInputRow,
) -> Result<bool, ShardLoomError> {
    if let Some(predicate_expression) = predicate_expression {
        let filter = evaluate_filter(predicate_expression, std::slice::from_ref(joined_row));
        if filter.has_errors() {
            return Err(ShardLoomError::InvalidOperation(format!(
                "SQL local-source join predicate evaluation failed: {}",
                filter
                    .diagnostics
                    .first()
                    .map_or("unknown diagnostic", |diagnostic| diagnostic
                        .message
                        .as_str())
            )));
        }
        if filter.selected_row_count() == 0 {
            return Ok(false);
        }
    }
    Ok(true)
}

fn evaluate_join_projection(
    projection_expressions: &[Expression],
    joined_row: &ExpressionInputRow,
) -> Result<Vec<(String, ScalarValue)>, ShardLoomError> {
    let projection = evaluate_projection(projection_expressions, joined_row);
    if projection.has_errors() {
        return Err(ShardLoomError::InvalidOperation(format!(
            "SQL local-source join projection evaluation failed: {}",
            projection
                .diagnostics
                .first()
                .map_or("unknown diagnostic", |diagnostic| diagnostic
                    .message
                    .as_str())
        )));
    }
    Ok(projection
        .projected_columns
        .into_iter()
        .map(|column| (column.name, column.value))
        .collect())
}

fn apply_temporal_literal_column_coercions(
    parsed: &ParsedSqlLocalSource,
    source: &mut CsvSourceData,
    mut right_source: Option<&mut CsvSourceData>,
) -> Result<(), ShardLoomError> {
    apply_date_literal_predicate_coercions(
        &parsed.predicate,
        parsed,
        source,
        right_source.as_deref_mut(),
    )?;
    apply_conditional_projection_date_coercions(parsed, source, right_source.as_deref_mut())?;
    apply_predicate_projection_date_coercions(parsed, source, right_source.as_deref_mut())?;
    apply_null_coalesce_projection_coercions(parsed, source, right_source.as_deref_mut())?;
    apply_nullif_projection_coercions(parsed, source, right_source.as_deref_mut())?;
    apply_date_arithmetic_projection_coercions(parsed, source, right_source.as_deref_mut())?;
    apply_timestamp_arithmetic_projection_coercions(parsed, source, right_source.as_deref_mut())?;
    apply_date_extract_projection_coercions(parsed, source, right_source.as_deref_mut())?;
    apply_temporal_difference_projection_coercions(parsed, source, right_source.as_deref_mut())?;
    apply_timestamp_literal_predicate_coercions(
        &parsed.predicate,
        parsed,
        source,
        right_source.as_deref_mut(),
    )?;
    apply_conditional_projection_timestamp_coercions(parsed, source, right_source.as_deref_mut())?;
    apply_predicate_projection_timestamp_coercions(parsed, source, right_source.as_deref_mut())?;
    apply_timestamp_extract_projection_coercions(parsed, source, right_source.as_deref_mut())?;
    apply_temporal_difference_predicate_coercions(
        &parsed.predicate,
        parsed,
        source,
        right_source.as_deref_mut(),
    )?;
    apply_predicate_projection_temporal_difference_coercions(parsed, source, right_source)
}

fn apply_conditional_projection_date_coercions(
    parsed: &ParsedSqlLocalSource,
    source: &mut CsvSourceData,
    mut right_source: Option<&mut CsvSourceData>,
) -> Result<(), ShardLoomError> {
    for projection in &parsed.conditional_projections {
        apply_date_literal_predicate_coercions(
            &projection.predicate,
            parsed,
            source,
            right_source.as_deref_mut(),
        )?;
    }
    Ok(())
}

fn apply_conditional_projection_timestamp_coercions(
    parsed: &ParsedSqlLocalSource,
    source: &mut CsvSourceData,
    mut right_source: Option<&mut CsvSourceData>,
) -> Result<(), ShardLoomError> {
    for projection in &parsed.conditional_projections {
        apply_timestamp_literal_predicate_coercions(
            &projection.predicate,
            parsed,
            source,
            right_source.as_deref_mut(),
        )?;
    }
    Ok(())
}

fn apply_predicate_projection_date_coercions(
    parsed: &ParsedSqlLocalSource,
    source: &mut CsvSourceData,
    mut right_source: Option<&mut CsvSourceData>,
) -> Result<(), ShardLoomError> {
    for projection in &parsed.predicate_projections {
        apply_date_literal_predicate_coercions(
            &projection.predicate,
            parsed,
            source,
            right_source.as_deref_mut(),
        )?;
    }
    Ok(())
}

fn apply_predicate_projection_timestamp_coercions(
    parsed: &ParsedSqlLocalSource,
    source: &mut CsvSourceData,
    mut right_source: Option<&mut CsvSourceData>,
) -> Result<(), ShardLoomError> {
    for projection in &parsed.predicate_projections {
        apply_timestamp_literal_predicate_coercions(
            &projection.predicate,
            parsed,
            source,
            right_source.as_deref_mut(),
        )?;
    }
    Ok(())
}

fn apply_predicate_projection_temporal_difference_coercions(
    parsed: &ParsedSqlLocalSource,
    source: &mut CsvSourceData,
    mut right_source: Option<&mut CsvSourceData>,
) -> Result<(), ShardLoomError> {
    for projection in &parsed.predicate_projections {
        apply_temporal_difference_predicate_coercions(
            &projection.predicate,
            parsed,
            source,
            right_source.as_deref_mut(),
        )?;
    }
    Ok(())
}

fn apply_null_coalesce_projection_coercions(
    parsed: &ParsedSqlLocalSource,
    source: &mut CsvSourceData,
    mut right_source: Option<&mut CsvSourceData>,
) -> Result<(), ShardLoomError> {
    for projection in &parsed.null_coalesce_projections {
        let source_dtype = projection
            .source_cast_dtype
            .clone()
            .unwrap_or_else(|| projection.fallback.dtype());
        match source_dtype {
            LogicalDType::Date32 => coerce_projection_column_to_date32(
                parsed,
                source,
                right_source.as_deref_mut(),
                &projection.column,
            )?,
            LogicalDType::TimestampMicros => {
                coerce_projection_column_to_timestamp_micros(
                    parsed,
                    source,
                    right_source.as_deref_mut(),
                    &projection.column,
                )?;
            }
            LogicalDType::Boolean
            | LogicalDType::Int64
            | LogicalDType::UInt64
            | LogicalDType::Float64
            | LogicalDType::Utf8
            | LogicalDType::Binary
            | LogicalDType::Struct
            | LogicalDType::List
            | LogicalDType::Unknown
            | LogicalDType::Extension(_) => {}
        }
    }
    Ok(())
}

fn validate_null_coalesce_projection_values(
    parsed: &ParsedSqlLocalSource,
    source: &CsvSourceData,
    right_source: Option<&CsvSourceData>,
) -> Result<(), ShardLoomError> {
    for projection in &parsed.null_coalesce_projections {
        let fallback_dtype = projection.fallback.dtype();
        let (projection_source, source_column) =
            projection_source_for_column(parsed, source, right_source, &projection.column)?;
        for row in &projection_source.rows {
            let Some(value) = row.get(&source_column) else {
                continue;
            };
            if value.is_null() || value.dtype() == fallback_dtype {
                continue;
            }
            return Err(unsupported_sql_error(&format!(
                "COALESCE projection source column {:?} contains {} values but fallback literal has {} dtype; scoped null coalesce requires matching non-null source and fallback dtypes",
                projection.column,
                value.dtype().as_str(),
                fallback_dtype.as_str()
            )));
        }
    }
    Ok(())
}

fn apply_nullif_projection_coercions(
    parsed: &ParsedSqlLocalSource,
    source: &mut CsvSourceData,
    mut right_source: Option<&mut CsvSourceData>,
) -> Result<(), ShardLoomError> {
    for projection in &parsed.nullif_projections {
        let source_dtype = projection
            .source_cast_dtype
            .clone()
            .unwrap_or_else(|| projection.sentinel.dtype());
        match source_dtype {
            LogicalDType::Date32 => coerce_projection_column_to_date32(
                parsed,
                source,
                right_source.as_deref_mut(),
                &projection.column,
            )?,
            LogicalDType::TimestampMicros => {
                coerce_projection_column_to_timestamp_micros(
                    parsed,
                    source,
                    right_source.as_deref_mut(),
                    &projection.column,
                )?;
            }
            LogicalDType::Boolean
            | LogicalDType::Int64
            | LogicalDType::UInt64
            | LogicalDType::Float64
            | LogicalDType::Utf8
            | LogicalDType::Binary
            | LogicalDType::Struct
            | LogicalDType::List
            | LogicalDType::Unknown
            | LogicalDType::Extension(_) => {}
        }
    }
    Ok(())
}

fn validate_nullif_projection_values(
    parsed: &ParsedSqlLocalSource,
    source: &CsvSourceData,
    right_source: Option<&CsvSourceData>,
) -> Result<(), ShardLoomError> {
    for projection in &parsed.nullif_projections {
        let sentinel_dtype = projection.sentinel.dtype();
        let (projection_source, source_column) =
            projection_source_for_column(parsed, source, right_source, &projection.column)?;
        for row in &projection_source.rows {
            let Some(value) = row.get(&source_column) else {
                continue;
            };
            if value.is_null() || value.dtype() == sentinel_dtype {
                continue;
            }
            return Err(unsupported_sql_error(&format!(
                "NULLIF projection source column {:?} contains {} values but sentinel literal has {} dtype; scoped nullif requires matching non-null source and sentinel dtypes",
                projection.column,
                value.dtype().as_str(),
                sentinel_dtype.as_str()
            )));
        }
    }
    Ok(())
}

fn apply_date_arithmetic_projection_coercions(
    parsed: &ParsedSqlLocalSource,
    source: &mut CsvSourceData,
    mut right_source: Option<&mut CsvSourceData>,
) -> Result<(), ShardLoomError> {
    for projection in &parsed.date_arithmetic_projections {
        coerce_projection_column_to_date32(
            parsed,
            source,
            right_source.as_deref_mut(),
            &projection.column,
        )?;
    }
    Ok(())
}

fn apply_timestamp_arithmetic_projection_coercions(
    parsed: &ParsedSqlLocalSource,
    source: &mut CsvSourceData,
    mut right_source: Option<&mut CsvSourceData>,
) -> Result<(), ShardLoomError> {
    for projection in &parsed.timestamp_arithmetic_projections {
        coerce_projection_column_to_timestamp_micros(
            parsed,
            source,
            right_source.as_deref_mut(),
            &projection.column,
        )?;
    }
    Ok(())
}

fn apply_date_extract_projection_coercions(
    parsed: &ParsedSqlLocalSource,
    source: &mut CsvSourceData,
    mut right_source: Option<&mut CsvSourceData>,
) -> Result<(), ShardLoomError> {
    for projection in &parsed.date_extract_projections {
        coerce_projection_column_to_date32(
            parsed,
            source,
            right_source.as_deref_mut(),
            &projection.column,
        )?;
    }
    Ok(())
}

fn apply_timestamp_extract_projection_coercions(
    parsed: &ParsedSqlLocalSource,
    source: &mut CsvSourceData,
    mut right_source: Option<&mut CsvSourceData>,
) -> Result<(), ShardLoomError> {
    for projection in &parsed.timestamp_extract_projections {
        coerce_projection_column_to_timestamp_micros(
            parsed,
            source,
            right_source.as_deref_mut(),
            &projection.column,
        )?;
    }
    Ok(())
}

fn apply_temporal_difference_projection_coercions(
    parsed: &ParsedSqlLocalSource,
    source: &mut CsvSourceData,
    mut right_source: Option<&mut CsvSourceData>,
) -> Result<(), ShardLoomError> {
    for projection in &parsed.generic_expression_projections {
        apply_temporal_difference_expression_coercions(
            parsed,
            source,
            right_source.as_deref_mut(),
            &projection.expression,
            true,
        )?;
    }
    Ok(())
}

fn apply_temporal_difference_predicate_coercions(
    predicate: &ParsedPredicate,
    parsed: &ParsedSqlLocalSource,
    source: &mut CsvSourceData,
    mut right_source: Option<&mut CsvSourceData>,
) -> Result<(), ShardLoomError> {
    match predicate {
        ParsedPredicate::GenericExpressionCompare { left, right, .. } => {
            apply_temporal_difference_expression_coercions(
                parsed,
                source,
                right_source.as_deref_mut(),
                left,
                false,
            )?;
            apply_temporal_difference_expression_coercions(
                parsed,
                source,
                right_source,
                right,
                false,
            )
        }
        ParsedPredicate::Logical { left, right, .. } => {
            apply_temporal_difference_predicate_coercions(
                left,
                parsed,
                source,
                right_source.as_deref_mut(),
            )?;
            apply_temporal_difference_predicate_coercions(right, parsed, source, right_source)
        }
        ParsedPredicate::Not { inner } => {
            apply_temporal_difference_predicate_coercions(inner, parsed, source, right_source)
        }
        ParsedPredicate::All
        | ParsedPredicate::Compare { .. }
        | ParsedPredicate::CastCompare { .. }
        | ParsedPredicate::NumericArithmeticCompare { .. }
        | ParsedPredicate::NumericAbsCompare { .. }
        | ParsedPredicate::NumericRoundingCompare { .. }
        | ParsedPredicate::DateArithmeticCompare { .. }
        | ParsedPredicate::TimestampArithmeticCompare { .. }
        | ParsedPredicate::DateExtractCompare { .. }
        | ParsedPredicate::TimestampExtractCompare { .. }
        | ParsedPredicate::StringLengthCompare { .. }
        | ParsedPredicate::StringTransformCompare { .. }
        | ParsedPredicate::StringFunctionCompare { .. }
        | ParsedPredicate::BooleanPredicate { .. }
        | ParsedPredicate::IsNull { .. }
        | ParsedPredicate::IsNotNull { .. }
        | ParsedPredicate::InList { .. }
        | ParsedPredicate::InSubquery { .. }
        | ParsedPredicate::StringMatch { .. } => Ok(()),
    }
}

fn apply_temporal_difference_expression_coercions(
    parsed: &ParsedSqlLocalSource,
    source: &mut CsvSourceData,
    mut right_source: Option<&mut CsvSourceData>,
    expression: &Expression,
    projection_context: bool,
) -> Result<(), ShardLoomError> {
    for column in expression_temporal_difference_date_source_columns(expression) {
        if projection_context {
            coerce_projection_column_to_date32(
                parsed,
                source,
                right_source.as_deref_mut(),
                &column,
            )?;
        } else {
            coerce_date_literal_column(&column, parsed, source, right_source.as_deref_mut())?;
        }
    }
    for column in expression_temporal_difference_timestamp_source_columns(expression) {
        if projection_context {
            coerce_projection_column_to_timestamp_micros(
                parsed,
                source,
                right_source.as_deref_mut(),
                &column,
            )?;
        } else {
            coerce_timestamp_literal_column(&column, parsed, source, right_source.as_deref_mut())?;
        }
    }
    Ok(())
}

fn apply_date_literal_predicate_coercions(
    predicate: &ParsedPredicate,
    parsed: &ParsedSqlLocalSource,
    source: &mut CsvSourceData,
    mut right_source: Option<&mut CsvSourceData>,
) -> Result<(), ShardLoomError> {
    match predicate {
        ParsedPredicate::Compare {
            column,
            value: ScalarValue::Date32(_),
            ..
        }
        | ParsedPredicate::DateArithmeticCompare {
            column,
            value: ScalarValue::Date32(_),
            ..
        }
        | ParsedPredicate::DateExtractCompare { column, .. } => {
            coerce_date_literal_column(column, parsed, source, right_source)
        }
        ParsedPredicate::InList { column, values }
            if values
                .iter()
                .any(|value| matches!(value, ScalarValue::Date32(_))) =>
        {
            coerce_date_literal_column(column, parsed, source, right_source)
        }
        ParsedPredicate::Logical { left, right, .. } => {
            apply_date_literal_predicate_coercions(
                left,
                parsed,
                source,
                right_source.as_deref_mut(),
            )?;
            apply_date_literal_predicate_coercions(right, parsed, source, right_source)
        }
        ParsedPredicate::Not { inner } => {
            apply_date_literal_predicate_coercions(inner, parsed, source, right_source)
        }
        ParsedPredicate::All
        | ParsedPredicate::Compare { .. }
        | ParsedPredicate::CastCompare { .. }
        | ParsedPredicate::NumericArithmeticCompare { .. }
        | ParsedPredicate::NumericAbsCompare { .. }
        | ParsedPredicate::NumericRoundingCompare { .. }
        | ParsedPredicate::GenericExpressionCompare { .. }
        | ParsedPredicate::DateArithmeticCompare { .. }
        | ParsedPredicate::TimestampArithmeticCompare { .. }
        | ParsedPredicate::StringLengthCompare { .. }
        | ParsedPredicate::TimestampExtractCompare { .. }
        | ParsedPredicate::StringTransformCompare { .. }
        | ParsedPredicate::StringFunctionCompare { .. }
        | ParsedPredicate::BooleanPredicate { .. }
        | ParsedPredicate::IsNull { .. }
        | ParsedPredicate::IsNotNull { .. }
        | ParsedPredicate::InList { .. }
        | ParsedPredicate::InSubquery { .. }
        | ParsedPredicate::StringMatch { .. } => Ok(()),
    }
}

fn apply_timestamp_literal_predicate_coercions(
    predicate: &ParsedPredicate,
    parsed: &ParsedSqlLocalSource,
    source: &mut CsvSourceData,
    mut right_source: Option<&mut CsvSourceData>,
) -> Result<(), ShardLoomError> {
    match predicate {
        ParsedPredicate::Compare {
            column,
            value: ScalarValue::TimestampMicros(_),
            ..
        }
        | ParsedPredicate::TimestampArithmeticCompare {
            column,
            value: ScalarValue::TimestampMicros(_),
            ..
        }
        | ParsedPredicate::TimestampExtractCompare { column, .. } => {
            coerce_timestamp_literal_column(column, parsed, source, right_source)
        }
        ParsedPredicate::CastCompare {
            column,
            target_dtype: LogicalDType::TimestampMicros,
            ..
        } => coerce_timestamp_literal_column(column, parsed, source, right_source),
        ParsedPredicate::InList { column, values }
            if values
                .iter()
                .any(|value| matches!(value, ScalarValue::TimestampMicros(_))) =>
        {
            coerce_timestamp_literal_column(column, parsed, source, right_source)
        }
        ParsedPredicate::Logical { left, right, .. } => {
            apply_timestamp_literal_predicate_coercions(
                left,
                parsed,
                source,
                right_source.as_deref_mut(),
            )?;
            apply_timestamp_literal_predicate_coercions(right, parsed, source, right_source)
        }
        ParsedPredicate::Not { inner } => {
            apply_timestamp_literal_predicate_coercions(inner, parsed, source, right_source)
        }
        ParsedPredicate::All
        | ParsedPredicate::Compare { .. }
        | ParsedPredicate::CastCompare { .. }
        | ParsedPredicate::NumericArithmeticCompare { .. }
        | ParsedPredicate::NumericAbsCompare { .. }
        | ParsedPredicate::NumericRoundingCompare { .. }
        | ParsedPredicate::GenericExpressionCompare { .. }
        | ParsedPredicate::DateArithmeticCompare { .. }
        | ParsedPredicate::TimestampArithmeticCompare { .. }
        | ParsedPredicate::DateExtractCompare { .. }
        | ParsedPredicate::StringLengthCompare { .. }
        | ParsedPredicate::StringTransformCompare { .. }
        | ParsedPredicate::StringFunctionCompare { .. }
        | ParsedPredicate::BooleanPredicate { .. }
        | ParsedPredicate::IsNull { .. }
        | ParsedPredicate::IsNotNull { .. }
        | ParsedPredicate::InList { .. }
        | ParsedPredicate::InSubquery { .. }
        | ParsedPredicate::StringMatch { .. } => Ok(()),
    }
}

fn coerce_projection_column_to_date32(
    parsed: &ParsedSqlLocalSource,
    source: &mut CsvSourceData,
    right_source: Option<&mut CsvSourceData>,
    column: &str,
) -> Result<(), ShardLoomError> {
    if parsed.is_join() {
        coerce_join_projection_column(parsed, source, right_source, column, &LogicalDType::Date32)
    } else {
        coerce_source_column_to_date32(source, column)
    }
}

fn coerce_projection_column_to_timestamp_micros(
    parsed: &ParsedSqlLocalSource,
    source: &mut CsvSourceData,
    right_source: Option<&mut CsvSourceData>,
    column: &str,
) -> Result<(), ShardLoomError> {
    if parsed.is_join() {
        coerce_join_projection_column(
            parsed,
            source,
            right_source,
            column,
            &LogicalDType::TimestampMicros,
        )
    } else {
        coerce_source_column_to_timestamp_micros(source, column)
    }
}

fn coerce_join_projection_column(
    parsed: &ParsedSqlLocalSource,
    source: &mut CsvSourceData,
    right_source: Option<&mut CsvSourceData>,
    column: &str,
    target_dtype: &LogicalDType,
) -> Result<(), ShardLoomError> {
    let join = parsed.join.as_ref().ok_or_else(|| {
        ShardLoomError::InvalidOperation("join projection coercion requires a join".to_string())
    })?;
    let left_alias = parsed.source_alias.as_ref().ok_or_else(|| {
        ShardLoomError::InvalidOperation(
            "join projection coercion requires a left alias".to_string(),
        )
    })?;
    let qualified = parse_qualified_column_ref(column)?;
    let target_source = if qualified.alias == *left_alias {
        source
    } else if qualified.alias == join.right_alias {
        right_source.ok_or_else(|| {
            ShardLoomError::InvalidOperation(
                "join projection coercion requires a right source".to_string(),
            )
        })?
    } else {
        return Err(unsupported_sql_error(
            "computed JOIN projections require admitted source aliases",
        ));
    };
    match target_dtype {
        LogicalDType::Date32 => coerce_source_column_to_date32(target_source, &qualified.column),
        LogicalDType::TimestampMicros => {
            coerce_source_column_to_timestamp_micros(target_source, &qualified.column)
        }
        _ => Ok(()),
    }
}

fn projection_source_for_column<'a>(
    parsed: &ParsedSqlLocalSource,
    source: &'a CsvSourceData,
    right_source: Option<&'a CsvSourceData>,
    column: &str,
) -> Result<(&'a CsvSourceData, String), ShardLoomError> {
    if let Some(join) = parsed.join.as_ref() {
        let left_alias = parsed.source_alias.as_ref().ok_or_else(|| {
            ShardLoomError::InvalidOperation(
                "join projection validation requires a left alias".to_string(),
            )
        })?;
        let qualified = parse_qualified_column_ref(column)?;
        if qualified.alias == *left_alias {
            Ok((source, qualified.column))
        } else if qualified.alias == join.right_alias {
            let right_source = right_source.ok_or_else(|| {
                ShardLoomError::InvalidOperation(
                    "join projection validation requires a right source".to_string(),
                )
            })?;
            Ok((right_source, qualified.column))
        } else {
            Err(unsupported_sql_error(
                "computed JOIN projections require admitted source aliases",
            ))
        }
    } else {
        Ok((source, column.to_string()))
    }
}

fn coerce_date_literal_column(
    column: &str,
    parsed: &ParsedSqlLocalSource,
    source: &mut CsvSourceData,
    right_source: Option<&mut CsvSourceData>,
) -> Result<(), ShardLoomError> {
    if let Some(join) = parsed.join.as_ref() {
        let left_alias = parsed.source_alias.as_ref().ok_or_else(|| {
            ShardLoomError::InvalidOperation("join date coercion requires a left alias".to_string())
        })?;
        let qualified = parse_qualified_column_ref(column)?;
        if qualified.alias == *left_alias {
            coerce_source_column_to_date32(source, &qualified.column)
        } else if qualified.alias == join.right_alias {
            let Some(right_source) = right_source else {
                return Err(ShardLoomError::InvalidOperation(
                    "join date coercion requires a right source".to_string(),
                ));
            };
            coerce_source_column_to_date32(right_source, &qualified.column)
        } else {
            Err(unsupported_sql_error(
                "DATE literal predicates on JOIN sources require an admitted source alias",
            ))
        }
    } else {
        coerce_source_column_to_date32(source, column)
    }
}

fn coerce_source_column_to_date32(
    source: &mut CsvSourceData,
    column: &str,
) -> Result<(), ShardLoomError> {
    if !source.header.iter().any(|candidate| candidate == column) {
        return Err(unsupported_sql_error(&format!(
            "DATE runtime column {column:?} is not present in the local source"
        )));
    }
    for row in &mut source.rows {
        let Some(value) = row.get_mut(column) else {
            continue;
        };
        match value {
            ScalarValue::Null | ScalarValue::Date32(_) => {}
            ScalarValue::Utf8(raw) => {
                let parsed = parse_iso_date32(raw).map_err(|_| {
                    unsupported_sql_error(&format!(
                        "DATE runtime column {column:?} requires ISO YYYY-MM-DD strings or nulls"
                    ))
                })?;
                *value = ScalarValue::Date32(parsed);
            }
            other => {
                return Err(unsupported_sql_error(&format!(
                    "DATE runtime column {column:?} requires ISO date strings or nulls, got {}",
                    other.dtype().as_str()
                )));
            }
        }
    }
    Ok(())
}

fn coerce_timestamp_literal_column(
    column: &str,
    parsed: &ParsedSqlLocalSource,
    source: &mut CsvSourceData,
    right_source: Option<&mut CsvSourceData>,
) -> Result<(), ShardLoomError> {
    if let Some(join) = parsed.join.as_ref() {
        let left_alias = parsed.source_alias.as_ref().ok_or_else(|| {
            ShardLoomError::InvalidOperation(
                "join timestamp coercion requires a left alias".to_string(),
            )
        })?;
        let qualified = parse_qualified_column_ref(column)?;
        if qualified.alias == *left_alias {
            coerce_source_column_to_timestamp_micros(source, &qualified.column)
        } else if qualified.alias == join.right_alias {
            let Some(right_source) = right_source else {
                return Err(ShardLoomError::InvalidOperation(
                    "join timestamp coercion requires a right source".to_string(),
                ));
            };
            coerce_source_column_to_timestamp_micros(right_source, &qualified.column)
        } else {
            Err(unsupported_sql_error(
                "TIMESTAMP literal predicates on JOIN sources require an admitted source alias",
            ))
        }
    } else {
        coerce_source_column_to_timestamp_micros(source, column)
    }
}

fn coerce_source_column_to_timestamp_micros(
    source: &mut CsvSourceData,
    column: &str,
) -> Result<(), ShardLoomError> {
    if !source.header.iter().any(|candidate| candidate == column) {
        return Err(unsupported_sql_error(&format!(
            "TIMESTAMP runtime column {column:?} is not present in the local source"
        )));
    }
    for row in &mut source.rows {
        let Some(value) = row.get_mut(column) else {
            continue;
        };
        match value {
            ScalarValue::Null | ScalarValue::TimestampMicros(_) => {}
            ScalarValue::Utf8(raw) => {
                let parsed = parse_iso_timestamp_micros(raw).map_err(|_| {
                    unsupported_sql_error(&format!(
                        "TIMESTAMP runtime column {column:?} requires UTC ISO YYYY-MM-DDTHH:MM:SS(.ffffff)Z strings or nulls"
                    ))
                })?;
                *value = ScalarValue::TimestampMicros(parsed);
            }
            other => {
                return Err(unsupported_sql_error(&format!(
                    "TIMESTAMP runtime column {column:?} requires UTC ISO timestamp strings or nulls, got {}",
                    other.dtype().as_str()
                )));
            }
        }
    }
    Ok(())
}

fn qualified_join_row(
    left_alias: &str,
    left_header: &[String],
    left_row: &ExpressionInputRow,
    right_alias: &str,
    right_header: &[String],
    right_row: &ExpressionInputRow,
) -> ExpressionInputRow {
    let mut row = ExpressionInputRow::new();
    for column in left_header {
        if let Some(value) = left_row.get(column) {
            row.insert(qualified_column_name(left_alias, column), value.clone());
        }
    }
    for column in right_header {
        if let Some(value) = right_row.get(column) {
            row.insert(qualified_column_name(right_alias, column), value.clone());
        }
    }
    row
}

fn evaluate_scalar_aggregate_output(
    parsed: &ParsedSqlLocalSource,
    rows: &[&ExpressionInputRow],
) -> Result<Vec<Vec<(String, ScalarValue)>>, ShardLoomError> {
    if parsed.limit == 0 {
        return Ok(Vec::new());
    }
    let row_indexes = (0..rows.len()).collect::<Vec<_>>();
    let mut row = Vec::with_capacity(parsed.aggregates.len());
    for aggregate in &parsed.aggregates {
        row.push((
            aggregate.output_name(),
            evaluate_scalar_aggregate(aggregate, rows, &row_indexes)?,
        ));
    }
    Ok(vec![row])
}

fn evaluate_grouped_aggregate_output(
    parsed: &ParsedSqlLocalSource,
    rows: &[&ExpressionInputRow],
) -> Result<Vec<Vec<(String, ScalarValue)>>, ShardLoomError> {
    if parsed.limit == 0 {
        return Ok(Vec::new());
    }
    let mut groups: BTreeMap<String, GroupedAggregateBucket> = BTreeMap::new();
    for (row_index, row) in rows.iter().enumerate() {
        let group_values = parsed
            .group_by
            .iter()
            .map(|column| {
                let value = row.get(column).cloned().ok_or_else(|| {
                    unsupported_sql_error(&format!(
                        "GROUP BY column {column:?} is not present in the aggregate input row"
                    ))
                })?;
                Ok((column.clone(), value))
            })
            .collect::<Result<Vec<_>, ShardLoomError>>()?;
        let key = group_key(&group_values);
        let entry = groups.entry(key).or_insert_with(|| GroupedAggregateBucket {
            values: group_values,
            row_indexes: Vec::new(),
        });
        entry.row_indexes.push(row_index);
    }

    let mut output_rows = Vec::new();
    for (_key, bucket) in groups {
        let mut row = bucket.values;
        for aggregate in &parsed.aggregates {
            row.push((
                aggregate.output_name(),
                evaluate_scalar_aggregate(aggregate, rows, &bucket.row_indexes)?,
            ));
        }
        output_rows.push(row);
    }
    if let Some(order_by) = parsed.order_by.as_ref() {
        let ordered_indexes =
            ordered_output_row_indexes(&output_rows, order_by, "ORDER BY aggregate output column")?;
        output_rows = ordered_indexes
            .into_iter()
            .take(parsed.limit)
            .map(|row_index| output_rows[row_index].clone())
            .collect();
    } else {
        output_rows.truncate(parsed.limit);
    }
    Ok(output_rows)
}

fn group_key(values: &[(String, ScalarValue)]) -> String {
    values
        .iter()
        .map(|(_name, value)| value.summary())
        .collect::<Vec<_>>()
        .join("\u{1f}")
}

fn qualified_column_name(alias: &str, column: &str) -> String {
    format!("{alias}.{column}")
}

fn join_memory_estimate_bytes(left_source: &CsvSourceData, right_source: &CsvSourceData) -> u64 {
    let row_count = left_source
        .rows
        .len()
        .saturating_add(right_source.rows.len());
    let column_count = left_source
        .header
        .len()
        .saturating_add(right_source.header.len());
    u64::try_from(row_count.saturating_mul(column_count).saturating_mul(64)).unwrap_or(u64::MAX)
}

fn evaluate_scalar_aggregate(
    aggregate: &ParsedAggregate,
    rows: &[&ExpressionInputRow],
    selected_row_indexes: &[usize],
) -> Result<ScalarValue, ShardLoomError> {
    match aggregate.function {
        AggregateFunction::Count => {
            let count = if aggregate.distinct {
                aggregate_count_distinct(aggregate, rows, selected_row_indexes)?
            } else if let Some(column) = aggregate.column.as_deref() {
                selected_row_indexes
                    .iter()
                    .filter_map(|row_index| rows.get(*row_index))
                    .filter(|row| !matches!(row.get(column), None | Some(ScalarValue::Null)))
                    .count()
            } else {
                selected_row_indexes.len()
            };
            i64::try_from(count).map(ScalarValue::Int64).map_err(|_| {
                unsupported_sql_error("COUNT result does not fit in int64 for this scoped smoke")
            })
        }
        AggregateFunction::Sum => aggregate_numeric_sum(aggregate, rows, selected_row_indexes),
        AggregateFunction::Avg => aggregate_numeric_avg(aggregate, rows, selected_row_indexes),
        AggregateFunction::Min => {
            aggregate_numeric_min_max(aggregate, rows, selected_row_indexes, MinMaxMode::Min)
        }
        AggregateFunction::Max => {
            aggregate_numeric_min_max(aggregate, rows, selected_row_indexes, MinMaxMode::Max)
        }
    }
}

fn aggregate_count_distinct(
    aggregate: &ParsedAggregate,
    rows: &[&ExpressionInputRow],
    selected_row_indexes: &[usize],
) -> Result<usize, ShardLoomError> {
    let column = aggregate.required_column()?;
    let mut distinct_values = BTreeSet::new();
    for row_index in selected_row_indexes {
        let row = rows.get(*row_index).ok_or_else(|| {
            ShardLoomError::InvalidOperation("selected row index is out of bounds".to_string())
        })?;
        let Some(value) = row.get(column) else {
            return Err(unsupported_sql_error(&format!(
                "aggregate column {column:?} is not present in the aggregate input row"
            )));
        };
        if value.is_null() {
            continue;
        }
        distinct_values.insert(scalar_distinct_key(value));
    }
    Ok(distinct_values.len())
}

fn scalar_distinct_key(value: &ScalarValue) -> String {
    match value {
        ScalarValue::Null => "null".to_string(),
        ScalarValue::Boolean(value) => format!("bool:{value}"),
        ScalarValue::Int64(value) => format!("i64:{value}"),
        ScalarValue::UInt64(value) => format!("u64:{value}"),
        ScalarValue::Float64(value) if *value == 0.0 => "f64:0".to_string(),
        ScalarValue::Float64(value) => format!("f64:{:016x}", value.to_bits()),
        ScalarValue::Utf8(value) => format!("utf8:{value}"),
        ScalarValue::Binary(value) => {
            let mut out = String::from("binary:");
            for byte in value {
                let _ = write!(out, "{byte:02x}");
            }
            out
        }
        ScalarValue::Date32(value) => format!("date32:{value}"),
        ScalarValue::TimestampMicros(value) => format!("ts_micros:{value}"),
    }
}

fn aggregate_numeric_sum(
    aggregate: &ParsedAggregate,
    rows: &[&ExpressionInputRow],
    selected_row_indexes: &[usize],
) -> Result<ScalarValue, ShardLoomError> {
    let column = aggregate.required_column()?;
    let mut int_sum = 0_i64;
    let mut float_sum = 0.0_f64;
    let mut saw_float = false;
    let mut count = 0_usize;
    for value in aggregate_numeric_values(column, rows, selected_row_indexes)? {
        count += 1;
        match value {
            NumericAggregateValue::Int(value) if !saw_float => {
                int_sum = int_sum.checked_add(value).ok_or_else(|| {
                    unsupported_sql_error("SUM overflowed int64 for this scoped smoke")
                })?;
            }
            NumericAggregateValue::Int(value) => float_sum += i64_to_f64(value),
            NumericAggregateValue::Float(value) => {
                if !saw_float {
                    float_sum = i64_to_f64(int_sum);
                    saw_float = true;
                }
                float_sum += value;
            }
        }
    }
    if count == 0 {
        Ok(ScalarValue::Null)
    } else if saw_float {
        Ok(ScalarValue::Float64(float_sum))
    } else {
        Ok(ScalarValue::Int64(int_sum))
    }
}

fn aggregate_numeric_avg(
    aggregate: &ParsedAggregate,
    rows: &[&ExpressionInputRow],
    selected_row_indexes: &[usize],
) -> Result<ScalarValue, ShardLoomError> {
    let column = aggregate.required_column()?;
    let mut sum = 0.0_f64;
    let mut count = 0_usize;
    for value in aggregate_numeric_values(column, rows, selected_row_indexes)? {
        count += 1;
        sum += value.as_f64();
    }
    if count == 0 {
        Ok(ScalarValue::Null)
    } else {
        Ok(ScalarValue::Float64(sum / usize_to_f64(count)))
    }
}

#[derive(Clone, Copy)]
enum MinMaxMode {
    Min,
    Max,
}

fn aggregate_numeric_min_max(
    aggregate: &ParsedAggregate,
    rows: &[&ExpressionInputRow],
    selected_row_indexes: &[usize],
    mode: MinMaxMode,
) -> Result<ScalarValue, ShardLoomError> {
    let column = aggregate.required_column()?;
    let mut selected: Option<NumericAggregateValue> = None;
    for value in aggregate_numeric_values(column, rows, selected_row_indexes)? {
        let replace = selected.is_none_or(|current| match mode {
            MinMaxMode::Min => value.as_f64() < current.as_f64(),
            MinMaxMode::Max => value.as_f64() > current.as_f64(),
        });
        if replace {
            selected = Some(value);
        }
    }
    Ok(selected.map_or(ScalarValue::Null, NumericAggregateValue::into_scalar))
}

#[derive(Clone, Copy)]
enum NumericAggregateValue {
    Int(i64),
    Float(f64),
}

impl NumericAggregateValue {
    fn as_f64(self) -> f64 {
        match self {
            Self::Int(value) => i64_to_f64(value),
            Self::Float(value) => value,
        }
    }

    fn into_scalar(self) -> ScalarValue {
        match self {
            Self::Int(value) => ScalarValue::Int64(value),
            Self::Float(value) => ScalarValue::Float64(value),
        }
    }
}

fn aggregate_numeric_values(
    column: &str,
    rows: &[&ExpressionInputRow],
    selected_row_indexes: &[usize],
) -> Result<Vec<NumericAggregateValue>, ShardLoomError> {
    let mut values = Vec::new();
    for row_index in selected_row_indexes {
        let row = rows.get(*row_index).ok_or_else(|| {
            ShardLoomError::InvalidOperation("selected row index is out of bounds".to_string())
        })?;
        let Some(value) = row.get(column) else {
            return Err(unsupported_sql_error(&format!(
                "aggregate column {column:?} is not present in the aggregate input row"
            )));
        };
        match value {
            ScalarValue::Null => {}
            ScalarValue::Int64(value) => values.push(NumericAggregateValue::Int(*value)),
            ScalarValue::UInt64(value) => {
                let value = i64::try_from(*value).map_err(|_| {
                    unsupported_sql_error("unsigned aggregate value does not fit in int64")
                })?;
                values.push(NumericAggregateValue::Int(value));
            }
            ScalarValue::Float64(value) if value.is_finite() => {
                values.push(NumericAggregateValue::Float(*value));
            }
            _ => {
                return Err(unsupported_sql_error(&format!(
                    "aggregate function requires numeric column {column:?} in this scoped smoke"
                )));
            }
        }
    }
    Ok(values)
}

fn i64_to_f64(value: i64) -> f64 {
    value
        .to_string()
        .parse::<f64>()
        .expect("i64 decimal text parses as finite f64")
}

fn usize_to_f64(value: usize) -> f64 {
    value
        .to_string()
        .parse::<f64>()
        .expect("usize decimal text parses as finite f64")
}

fn prepare_sql_outputs(
    request: &SqlLocalSourceRequest,
    columns: &[String],
    rows: &[Vec<(String, ScalarValue)>],
) -> Result<Vec<SqlRenderedOutput>, ShardLoomError> {
    let mut prepared = Vec::new();
    if let Some(output_path) = request.output_path.as_ref() {
        prepared.push(prepare_sql_output(
            request.output_format,
            output_path.clone(),
            columns,
            rows,
        )?);
    }
    prepared.extend(
        request
            .fanout_outputs
            .iter()
            .map(|target| prepare_sql_output(target.format, target.path.clone(), columns, rows))
            .collect::<Result<Vec<_>, _>>()?,
    );
    Ok(prepared)
}

fn prepare_sql_output(
    format: SqlLocalSourceOutputFormat,
    path: PathBuf,
    columns: &[String],
    rows: &[Vec<(String, ScalarValue)>],
) -> Result<SqlRenderedOutput, ShardLoomError> {
    let payload = if matches!(format, SqlLocalSourceOutputFormat::Vortex) {
        validate_sql_vortex_output_plan(&path)?;
        SqlOutputPayload::Vortex
    } else {
        let content = format.render_rows(columns, rows)?;
        let digest = fnv64_digest_bytes(&content);
        let bytes = u64::try_from(content.len()).unwrap_or(u64::MAX);
        SqlOutputPayload::Bytes {
            content,
            digest,
            bytes,
        }
    };
    Ok(SqlRenderedOutput {
        format,
        path,
        payload,
    })
}

fn fanout_plan_digest_fragment(request: &SqlLocalSourceRequest) -> String {
    request
        .fanout_outputs
        .iter()
        .map(|target| format!("{}={}", target.format.sink_format(), target.path.display()))
        .collect::<Vec<_>>()
        .join(";")
}

fn preflight_sql_output_writes(request: &SqlLocalSourceRequest) -> Result<(), ShardLoomError> {
    let targets = request
        .output_path
        .iter()
        .chain(request.fanout_outputs.iter().map(|target| &target.path));
    for path in targets {
        let workspace_root = shardloom_core::infer_local_output_workspace_root(path)?;
        shardloom_core::plan_workspace_safe_local_output(
            workspace_root,
            path,
            request.allow_overwrite,
        )?;
    }
    Ok(())
}

fn primary_output_evidence(
    request: &SqlLocalSourceRequest,
    written_outputs: &[SqlWrittenOutput],
    inline_output_digest: &str,
    inline_output_bytes: u64,
) -> (String, u64) {
    let Some(primary_path) = request.output_path.as_ref() else {
        return (inline_output_digest.to_string(), inline_output_bytes);
    };
    written_outputs
        .iter()
        .find(|output| &output.path == primary_path)
        .map_or_else(
            || (inline_output_digest.to_string(), inline_output_bytes),
            |output| (output.digest.clone(), output.bytes),
        )
}

fn write_sql_outputs(
    rendered_outputs: Vec<SqlRenderedOutput>,
    columns: &[String],
    rows: &[Vec<(String, ScalarValue)>],
    allow_overwrite: bool,
) -> Result<Vec<SqlWrittenOutput>, ShardLoomError> {
    rendered_outputs
        .into_iter()
        .map(|rendered| write_sql_output(rendered, columns, rows, allow_overwrite))
        .collect()
}

fn write_sql_output(
    rendered: SqlRenderedOutput,
    columns: &[String],
    rows: &[Vec<(String, ScalarValue)>],
    allow_overwrite: bool,
) -> Result<SqlWrittenOutput, ShardLoomError> {
    match rendered.payload {
        SqlOutputPayload::Bytes {
            content,
            digest,
            bytes,
        } => {
            let write_start = Instant::now();
            let workspace_root = shardloom_core::infer_local_output_workspace_root(&rendered.path)?;
            let workspace_write_report = shardloom_core::write_workspace_safe_bytes(
                workspace_root,
                &rendered.path,
                allow_overwrite,
                "SQL local-source output",
                &content,
            )?;
            let write_millis = write_start.elapsed().as_millis();
            let replay = replay_sql_byte_output(rendered.format, &rendered.path, &digest)?;
            Ok(SqlWrittenOutput {
                format: rendered.format,
                path: rendered.path,
                digest,
                bytes,
                write_millis,
                replay,
                workspace_write_report,
                vortex_report: None,
            })
        }
        SqlOutputPayload::Vortex => write_sql_vortex_output(
            rendered.format,
            rendered.path,
            columns,
            rows,
            allow_overwrite,
        ),
    }
}

fn write_sql_vortex_output(
    format: SqlLocalSourceOutputFormat,
    path: PathBuf,
    columns: &[String],
    rows: &[Vec<(String, ScalarValue)>],
    allow_overwrite: bool,
) -> Result<SqlWrittenOutput, ShardLoomError> {
    let request = shardloom_vortex::VortexPreparedStateWriteRequest::new(
        &path,
        columns.to_vec(),
        rows.to_vec(),
    )
    .allow_overwrite(allow_overwrite);
    let report = shardloom_vortex::write_flat_scalar_vortex_prepared_state(request)?;
    let write_millis = report.write_micros / 1_000;
    let replay = replay_sql_vortex_output(format, &report);
    let workspace_write_report = report.workspace_write_report.clone();
    Ok(SqlWrittenOutput {
        format,
        path,
        digest: report.artifact_digest.clone(),
        bytes: report.bytes_written,
        write_millis,
        replay,
        workspace_write_report,
        vortex_report: Some(report),
    })
}

fn replay_sql_byte_output(
    format: SqlLocalSourceOutputFormat,
    path: &Path,
    expected_digest: &str,
) -> Result<SqlOutputReplayEvidence, ShardLoomError> {
    let replay_start = Instant::now();
    let bytes = fs::read(path).map_err(|error| {
        ShardLoomError::Message(format!(
            "failed to replay local SQL output {}: {error}",
            path.display()
        ))
    })?;
    let replay_digest = fnv64_digest_bytes(&bytes);
    if replay_digest != expected_digest {
        return Err(ShardLoomError::InvalidOperation(format!(
            "local SQL output replay digest mismatch for {}: expected {expected_digest}, got {replay_digest}",
            path.display()
        )));
    }
    Ok(SqlOutputReplayEvidence {
        verified: true,
        status: "verified_local_file_digest",
        replay_millis: replay_start.elapsed().as_millis(),
        fidelity_status: output_fidelity_status(format),
        fidelity_loss: output_fidelity_loss(format),
    })
}

fn replay_sql_vortex_output(
    format: SqlLocalSourceOutputFormat,
    report: &shardloom_vortex::VortexPreparedStateWriteReport,
) -> SqlOutputReplayEvidence {
    SqlOutputReplayEvidence {
        verified: report.upstream_vortex_scan_called,
        status: if report.upstream_vortex_scan_called {
            "verified_vortex_reopen_row_count"
        } else {
            "blocked_missing_vortex_reopen_proof"
        },
        replay_millis: report.reopen_scan_micros / 1_000,
        fidelity_status: output_fidelity_status(format),
        fidelity_loss: output_fidelity_loss(format),
    }
}

fn output_fidelity_status(format: SqlLocalSourceOutputFormat) -> &'static str {
    match format {
        SqlLocalSourceOutputFormat::InlineJsonl => "logical_rows_replay_verified",
        SqlLocalSourceOutputFormat::Csv => {
            "logical_rows_replay_verified_type_metadata_not_preserved"
        }
        SqlLocalSourceOutputFormat::Parquet
        | SqlLocalSourceOutputFormat::ArrowIpc
        | SqlLocalSourceOutputFormat::Avro
        | SqlLocalSourceOutputFormat::Orc => "flat_scalar_schema_replay_verified",
        SqlLocalSourceOutputFormat::Vortex => "vortex_flat_scalar_reopen_verified",
    }
}

fn output_fidelity_loss(format: SqlLocalSourceOutputFormat) -> &'static str {
    match format {
        SqlLocalSourceOutputFormat::InlineJsonl => {
            "jsonl_text_roundtrip_not_full_type_metadata_fidelity"
        }
        SqlLocalSourceOutputFormat::Csv => "csv_text_roundtrip_loses_static_type_metadata",
        SqlLocalSourceOutputFormat::Parquet
        | SqlLocalSourceOutputFormat::ArrowIpc
        | SqlLocalSourceOutputFormat::Avro
        | SqlLocalSourceOutputFormat::Orc => {
            "flat_scalar_only_no_nested_or_full_metadata_fidelity_claim"
        }
        SqlLocalSourceOutputFormat::Vortex => {
            "flat_scalar_only_no_broad_vortex_writer_fidelity_claim"
        }
    }
}

fn bind_sql_local_source(
    parsed: &ParsedSqlLocalSource,
    header: &[String],
    right_header: Option<&[String]>,
) -> Result<(), ShardLoomError> {
    if parsed.is_join() {
        return bind_join_sql_local_source(parsed, header, right_header);
    }
    validate_order_by_source(parsed, header)?;
    validate_computed_projection_shape(parsed)?;
    validate_projection_output_names(parsed)?;
    if parsed.is_grouped_aggregate() {
        validate_grouped_aggregate_sources(parsed, header)?;
    } else if parsed.is_aggregate() {
        validate_scalar_aggregate_sources(parsed, header)?;
    } else {
        validate_projection_source_columns(parsed, header)?;
    }
    validate_aggregate_output_names(parsed)?;
    if !parsed.group_by.is_empty() && !parsed.is_grouped_aggregate() {
        return Err(unsupported_sql_error(
            "GROUP BY requires at least one aggregate function in this scoped smoke",
        ));
    }
    validate_predicate_source_columns(parsed, header)?;
    Ok(())
}

fn validate_order_by_source(
    parsed: &ParsedSqlLocalSource,
    header: &[String],
) -> Result<(), ShardLoomError> {
    if let Some(order_by) = parsed.order_by.as_ref() {
        if parsed.is_aggregate() || parsed.is_grouped_aggregate() {
            let output_columns = parsed.output_columns(header);
            return validate_order_by_output_columns(
                order_by,
                &output_columns,
                "ORDER BY aggregate output column",
            );
        }
        validate_order_by_output_columns(order_by, header, "ORDER BY column")?;
    }
    Ok(())
}

fn validate_order_by_output_columns(
    order_by: &ParsedOrderBy,
    columns: &[String],
    label: &str,
) -> Result<(), ShardLoomError> {
    for key in &order_by.keys {
        if !columns.iter().any(|candidate| candidate == &key.column) {
            return Err(unsupported_sql_error(&format!(
                "{label} {:?} is not present in the row",
                key.column
            )));
        }
    }
    Ok(())
}

fn validate_computed_projection_shape(parsed: &ParsedSqlLocalSource) -> Result<(), ShardLoomError> {
    if parsed.has_computed_projection()
        && (parsed.is_aggregate()
            || !parsed.group_by.is_empty()
            || parsed.order_by.is_some()
            || parsed.projections.is_empty())
    {
        return Err(unsupported_sql_error(
            "computed projection smoke currently admits explicit projection columns or SELECT * plus <literal> AS <column>, CAST(<column> AS <dtype>) AS <column>, COALESCE(<column>, <literal>) AS <column>, CASE WHEN <predicate> THEN <literal-or-column> ELSE <literal-or-column> END AS <column>, admitted predicate expressions AS <column>, <column> (+|-|*|/) <numeric-literal> AS <column>, generalized numeric expression trees AS <column>, DATE_DIFF_DAYS(...) or TIMESTAMP_DIFF_SECONDS(...) AS <column>, DATE_ADD_DAYS|DATE_SUB_DAYS(<column>, <days>) AS <column>, LOWER|UPPER|TRIM(<column>) AS <column>, LENGTH(<column>) AS <column>, CONCAT(<column-or-string-literal>, ...) AS <column>, SUBSTR|SUBSTRING(<column>, <start>, <length>) AS <column>, LEFT|RIGHT(<column>, <count>) AS <column>, REPLACE(<column>, <string-literal>, <string-literal>) AS <column>, DATE_YEAR|DATE_MONTH|DATE_DAY(<column>) AS <column>, or TIMESTAMP_YEAR|TIMESTAMP_MONTH|TIMESTAMP_DAY|TIMESTAMP_HOUR|TIMESTAMP_MINUTE|TIMESTAMP_SECOND(<column>) AS <column> before optional filter/limit only",
        ));
    }
    Ok(())
}

fn validate_grouped_aggregate_sources(
    parsed: &ParsedSqlLocalSource,
    header: &[String],
) -> Result<(), ShardLoomError> {
    if parsed.projections != parsed.group_by {
        return Err(unsupported_sql_error(
            "GROUP BY smoke requires SELECT group columns to exactly match GROUP BY columns before aggregate functions",
        ));
    }
    for column in &parsed.group_by {
        if !header.iter().any(|candidate| candidate == column) {
            return Err(unsupported_sql_error(&format!(
                "GROUP BY column {column:?} is not present in the CSV header"
            )));
        }
    }
    validate_aggregate_source_columns(parsed, header)
}

fn validate_scalar_aggregate_sources(
    parsed: &ParsedSqlLocalSource,
    header: &[String],
) -> Result<(), ShardLoomError> {
    if parsed.has_computed_projection() {
        return Err(unsupported_sql_error(
            "scalar aggregate SELECT list cannot mix aggregate functions with computed projections in this scoped smoke",
        ));
    }
    if !parsed.projections.is_empty() {
        return Err(unsupported_sql_error(
            "scalar aggregate SELECT list cannot mix aggregate functions with raw columns in this scoped smoke",
        ));
    }
    validate_aggregate_source_columns(parsed, header)
}

fn validate_aggregate_output_names(parsed: &ParsedSqlLocalSource) -> Result<(), ShardLoomError> {
    if !parsed.is_aggregate() {
        return Ok(());
    }
    let mut output_names = BTreeSet::new();
    if parsed.is_grouped_aggregate() {
        for column in &parsed.group_by {
            if !output_names.insert(column.clone()) {
                return Err(unsupported_sql_error(
                    "aggregate smoke requires unique output column names",
                ));
            }
        }
    }
    for aggregate in &parsed.aggregates {
        if !output_names.insert(aggregate.output_name()) {
            return Err(unsupported_sql_error(
                "aggregate smoke requires unique output column names",
            ));
        }
    }
    Ok(())
}

fn validate_aggregate_source_columns(
    parsed: &ParsedSqlLocalSource,
    header: &[String],
) -> Result<(), ShardLoomError> {
    for aggregate in &parsed.aggregates {
        if let Some(column) = aggregate.column.as_deref() {
            if !header.iter().any(|candidate| candidate == column) {
                return Err(unsupported_sql_error(&format!(
                    "aggregate column {column:?} is not present in the CSV header"
                )));
            }
        }
    }
    Ok(())
}

fn validate_projection_source_columns(
    parsed: &ParsedSqlLocalSource,
    header: &[String],
) -> Result<(), ShardLoomError> {
    for column in parsed.projection_columns(header) {
        require_header_column(header, &column, "projection column", "CSV header")?;
    }
    validate_numeric_projection_source_columns(parsed, header)?;
    validate_value_projection_source_columns(parsed, header)?;
    validate_text_time_projection_source_columns(parsed, header)
}

fn validate_numeric_projection_source_columns(
    parsed: &ParsedSqlLocalSource,
    header: &[String],
) -> Result<(), ShardLoomError> {
    for projection in &parsed.numeric_arithmetic_projections {
        require_header_column(
            header,
            &projection.column,
            "numeric arithmetic projection source column",
            "CSV header",
        )?;
    }
    for projection in &parsed.numeric_abs_projections {
        require_header_column(
            header,
            &projection.column,
            "numeric abs projection source column",
            "CSV header",
        )?;
    }
    for projection in &parsed.numeric_rounding_projections {
        require_header_column(
            header,
            &projection.column,
            "numeric rounding projection source column",
            "CSV header",
        )?;
    }
    for projection in &parsed.generic_expression_projections {
        for column in &projection.source_columns {
            require_header_column(
                header,
                column,
                "generic expression projection source column",
                "CSV header",
            )?;
        }
    }
    Ok(())
}

fn validate_value_projection_source_columns(
    parsed: &ParsedSqlLocalSource,
    header: &[String],
) -> Result<(), ShardLoomError> {
    for projection in &parsed.cast_projections {
        require_header_column(
            header,
            &projection.column,
            "cast projection source column",
            "local source header",
        )?;
    }
    for projection in &parsed.null_coalesce_projections {
        require_header_column(
            header,
            &projection.column,
            "COALESCE projection source column",
            "local source header",
        )?;
    }
    for projection in &parsed.nullif_projections {
        require_header_column(
            header,
            &projection.column,
            "NULLIF projection source column",
            "local source header",
        )?;
    }
    for projection in &parsed.conditional_projections {
        for column in projection.predicate.columns() {
            require_header_column(
                header,
                column,
                "conditional projection predicate column",
                "CSV header",
            )?;
        }
        if let Some(column) = projection.then_branch.source_column() {
            require_header_column(
                header,
                column,
                "conditional projection THEN branch column",
                "CSV header",
            )?;
        }
        if let Some(column) = projection.else_branch.source_column() {
            require_header_column(
                header,
                column,
                "conditional projection ELSE branch column",
                "CSV header",
            )?;
        }
    }
    for projection in &parsed.predicate_projections {
        for column in projection.predicate.columns() {
            require_header_column(
                header,
                column,
                "predicate projection source column",
                "local source header",
            )?;
        }
    }
    Ok(())
}

fn validate_text_time_projection_source_columns(
    parsed: &ParsedSqlLocalSource,
    header: &[String],
) -> Result<(), ShardLoomError> {
    for projection in &parsed.date_arithmetic_projections {
        require_header_column(
            header,
            &projection.column,
            "date arithmetic projection source column",
            "local source header",
        )?;
    }
    for projection in &parsed.timestamp_arithmetic_projections {
        require_header_column(
            header,
            &projection.column,
            "timestamp arithmetic projection source column",
            "local source header",
        )?;
    }
    for projection in &parsed.string_length_projections {
        require_header_column(
            header,
            &projection.column,
            "string length projection source column",
            "local source header",
        )?;
    }
    for projection in &parsed.string_transform_projections {
        require_header_column(
            header,
            &projection.column,
            "string transform projection source column",
            "CSV header",
        )?;
    }
    for projection in &parsed.string_function_projections {
        for column in &projection.source_columns {
            require_header_column(
                header,
                column,
                "string function projection source column",
                "CSV header",
            )?;
        }
    }
    for projection in &parsed.date_extract_projections {
        require_header_column(
            header,
            &projection.column,
            "date extract projection source column",
            "local source header",
        )?;
    }
    for projection in &parsed.timestamp_extract_projections {
        require_header_column(
            header,
            &projection.column,
            "timestamp extract projection source column",
            "local source header",
        )?;
    }
    Ok(())
}

fn require_header_column(
    header: &[String],
    column: &str,
    context: &str,
    location: &str,
) -> Result<(), ShardLoomError> {
    if header.iter().any(|candidate| candidate == column) {
        Ok(())
    } else {
        Err(unsupported_sql_error(&format!(
            "{context} {column:?} is not present in the {location}"
        )))
    }
}

fn validate_predicate_source_columns(
    parsed: &ParsedSqlLocalSource,
    header: &[String],
) -> Result<(), ShardLoomError> {
    for predicate_column in parsed.predicate.columns() {
        if !header.iter().any(|candidate| candidate == predicate_column) {
            return Err(unsupported_sql_error(&format!(
                "predicate column {predicate_column:?} is not present in the CSV header"
            )));
        }
    }
    Ok(())
}

fn validate_projection_output_names(parsed: &ParsedSqlLocalSource) -> Result<(), ShardLoomError> {
    if !parsed.has_computed_projection() {
        return Ok(());
    }
    let mut output_names = BTreeSet::new();
    for column in &parsed.projections {
        require_unique_projection_output_name(&mut output_names, column)?;
    }
    validate_literal_numeric_output_names(parsed, &mut output_names)?;
    validate_value_output_names(parsed, &mut output_names)?;
    validate_text_time_output_names(parsed, &mut output_names)?;
    Ok(())
}

fn validate_literal_numeric_output_names<'a>(
    parsed: &'a ParsedSqlLocalSource,
    output_names: &mut BTreeSet<&'a str>,
) -> Result<(), ShardLoomError> {
    for projection in &parsed.literal_projections {
        require_unique_projection_output_name(output_names, &projection.alias)?;
    }
    for projection in &parsed.numeric_arithmetic_projections {
        require_unique_projection_output_name(output_names, &projection.alias)?;
    }
    for projection in &parsed.numeric_abs_projections {
        require_unique_projection_output_name(output_names, &projection.alias)?;
    }
    for projection in &parsed.numeric_rounding_projections {
        require_unique_projection_output_name(output_names, &projection.alias)?;
    }
    for projection in &parsed.generic_expression_projections {
        require_unique_projection_output_name(output_names, &projection.alias)?;
    }
    Ok(())
}

fn validate_value_output_names<'a>(
    parsed: &'a ParsedSqlLocalSource,
    output_names: &mut BTreeSet<&'a str>,
) -> Result<(), ShardLoomError> {
    for projection in &parsed.cast_projections {
        require_unique_projection_output_name(output_names, &projection.alias)?;
    }
    for projection in &parsed.null_coalesce_projections {
        require_unique_projection_output_name(output_names, &projection.alias)?;
    }
    for projection in &parsed.nullif_projections {
        require_unique_projection_output_name(output_names, &projection.alias)?;
    }
    for projection in &parsed.conditional_projections {
        require_unique_projection_output_name(output_names, &projection.alias)?;
    }
    for projection in &parsed.predicate_projections {
        require_unique_projection_output_name(output_names, &projection.alias)?;
    }
    Ok(())
}

fn validate_text_time_output_names<'a>(
    parsed: &'a ParsedSqlLocalSource,
    output_names: &mut BTreeSet<&'a str>,
) -> Result<(), ShardLoomError> {
    for projection in &parsed.date_arithmetic_projections {
        require_unique_projection_output_name(output_names, &projection.alias)?;
    }
    for projection in &parsed.timestamp_arithmetic_projections {
        require_unique_projection_output_name(output_names, &projection.alias)?;
    }
    for projection in &parsed.string_length_projections {
        require_unique_projection_output_name(output_names, &projection.alias)?;
    }
    for projection in &parsed.string_transform_projections {
        require_unique_projection_output_name(output_names, &projection.alias)?;
    }
    for projection in &parsed.string_function_projections {
        require_unique_projection_output_name(output_names, &projection.alias)?;
    }
    for projection in &parsed.date_extract_projections {
        require_unique_projection_output_name(output_names, &projection.alias)?;
    }
    for projection in &parsed.timestamp_extract_projections {
        require_unique_projection_output_name(output_names, &projection.alias)?;
    }
    Ok(())
}

fn require_unique_projection_output_name<'a>(
    output_names: &mut BTreeSet<&'a str>,
    name: &'a str,
) -> Result<(), ShardLoomError> {
    if output_names.insert(name) {
        Ok(())
    } else {
        Err(unsupported_sql_error(
            "computed projection smoke requires unique output column names",
        ))
    }
}

fn bind_join_sql_local_source(
    parsed: &ParsedSqlLocalSource,
    left_header: &[String],
    right_header: Option<&[String]>,
) -> Result<(), ShardLoomError> {
    let join = parsed.join.as_ref().ok_or_else(|| {
        ShardLoomError::InvalidOperation("join binder called without join plan".to_string())
    })?;
    let Some(left_alias) = parsed.source_alias.as_ref() else {
        return Err(unsupported_sql_error(
            "JOIN smoke requires an explicit left source alias",
        ));
    };
    let Some(right_header) = right_header else {
        return Err(unsupported_sql_error(
            "JOIN smoke requires a readable local right source",
        ));
    };
    if parsed.has_computed_projection() && parsed.is_aggregate() {
        return Err(unsupported_sql_error(
            "computed JOIN projections cannot be mixed with aggregate functions in this scoped smoke",
        ));
    }
    let qualified_header =
        qualified_join_header(left_alias, left_header, &join.right_alias, right_header);
    if parsed.is_aggregate() || parsed.is_grouped_aggregate() {
        if let Some(order_by) = parsed.order_by.as_ref() {
            let output_columns = parsed.output_columns(&qualified_header);
            validate_order_by_output_columns(
                order_by,
                &output_columns,
                "ORDER BY join aggregate output column",
            )?;
        }
    } else {
        validate_join_order_by_source(parsed, &qualified_header)?;
    }
    validate_join_key_pairs(join, left_alias, left_header, right_header)?;
    if parsed.is_grouped_aggregate() {
        validate_join_grouped_aggregate_sources(
            parsed,
            left_alias,
            left_header,
            &join.right_alias,
            right_header,
        )?;
    } else if parsed.is_aggregate() {
        validate_join_scalar_aggregate_sources(
            parsed,
            left_alias,
            left_header,
            &join.right_alias,
            right_header,
        )?;
    } else {
        if !parsed.group_by.is_empty() {
            return Err(unsupported_sql_error(
                "GROUP BY requires at least one aggregate function in this scoped smoke",
            ));
        }
        if parsed.projections.is_empty() {
            return Err(unsupported_sql_error(
                "JOIN projection smoke requires at least one qualified projection column",
            ));
        }
        if parsed.projections.len() == 1 && parsed.projections[0] == "*" {
            return Err(unsupported_sql_error(
                "JOIN projection smoke requires explicit qualified projection columns",
            ));
        }
        if parsed.has_computed_projection()
            && (parsed.projections.is_empty()
                || (parsed.projections.len() == 1 && parsed.projections[0] == "*"))
        {
            return Err(unsupported_sql_error(
                "computed JOIN projection smoke requires explicit raw qualified projection columns before computed projections",
            ));
        }
        validate_projection_source_columns(parsed, &qualified_header)?;
        validate_projection_output_names(parsed)?;
    }
    validate_aggregate_output_names(parsed)?;
    bind_qualified_predicate(
        &parsed.predicate,
        left_alias,
        left_header,
        &join.right_alias,
        right_header,
    )
}

fn qualified_join_header(
    left_alias: &str,
    left_header: &[String],
    right_alias: &str,
    right_header: &[String],
) -> Vec<String> {
    left_header
        .iter()
        .map(|column| qualified_column_name(left_alias, column))
        .chain(
            right_header
                .iter()
                .map(|column| qualified_column_name(right_alias, column)),
        )
        .collect()
}

fn validate_join_order_by_source(
    parsed: &ParsedSqlLocalSource,
    qualified_header: &[String],
) -> Result<(), ShardLoomError> {
    let Some(order_by) = parsed.order_by.as_ref() else {
        return Ok(());
    };
    for key in &order_by.keys {
        if !qualified_header
            .iter()
            .any(|candidate| candidate == &key.column)
        {
            return Err(unsupported_sql_error(&format!(
                "ORDER BY join column {:?} is not present in the qualified JOIN row",
                key.column
            )));
        }
    }
    Ok(())
}

fn validate_join_key_pairs(
    join: &ParsedJoin,
    left_alias: &str,
    left_header: &[String],
    right_header: &[String],
) -> Result<(), ShardLoomError> {
    let mut left_key_columns = BTreeSet::new();
    let mut right_key_columns = BTreeSet::new();
    for key_pair in &join.key_pairs {
        if key_pair.left.alias != left_alias || key_pair.right.alias != join.right_alias {
            return Err(unsupported_sql_error(
                "JOIN ON must compare the left alias to the right alias in this scoped smoke",
            ));
        }
        if !left_key_columns.insert(key_pair.left.column.clone())
            || !right_key_columns.insert(key_pair.right.column.clone())
        {
            return Err(unsupported_sql_error(
                "JOIN smoke requires unique key columns on each side",
            ));
        }
        if !left_header
            .iter()
            .any(|column| column == &key_pair.left.column)
        {
            return Err(unsupported_sql_error(&format!(
                "JOIN left key column {:?} is not present in the left source header",
                key_pair.left.column
            )));
        }
        if !right_header
            .iter()
            .any(|column| column == &key_pair.right.column)
        {
            return Err(unsupported_sql_error(&format!(
                "JOIN right key column {:?} is not present in the right source header",
                key_pair.right.column
            )));
        }
    }
    Ok(())
}

fn validate_join_grouped_aggregate_sources(
    parsed: &ParsedSqlLocalSource,
    left_alias: &str,
    left_header: &[String],
    right_alias: &str,
    right_header: &[String],
) -> Result<(), ShardLoomError> {
    if parsed.projections != parsed.group_by {
        return Err(unsupported_sql_error(
            "GROUP BY smoke requires SELECT group columns to exactly match GROUP BY columns before aggregate functions",
        ));
    }
    for column in &parsed.group_by {
        bind_qualified_column(column, left_alias, left_header, right_alias, right_header)?;
    }
    validate_join_aggregate_source_columns(
        parsed,
        left_alias,
        left_header,
        right_alias,
        right_header,
    )
}

fn validate_join_scalar_aggregate_sources(
    parsed: &ParsedSqlLocalSource,
    left_alias: &str,
    left_header: &[String],
    right_alias: &str,
    right_header: &[String],
) -> Result<(), ShardLoomError> {
    if !parsed.projections.is_empty() {
        return Err(unsupported_sql_error(
            "scalar aggregate SELECT list cannot mix aggregate functions with raw columns in this scoped smoke",
        ));
    }
    validate_join_aggregate_source_columns(
        parsed,
        left_alias,
        left_header,
        right_alias,
        right_header,
    )
}

fn validate_join_aggregate_source_columns(
    parsed: &ParsedSqlLocalSource,
    left_alias: &str,
    left_header: &[String],
    right_alias: &str,
    right_header: &[String],
) -> Result<(), ShardLoomError> {
    for aggregate in &parsed.aggregates {
        if let Some(column) = aggregate.column.as_deref() {
            bind_qualified_column(column, left_alias, left_header, right_alias, right_header)?;
        }
    }
    Ok(())
}

fn bind_qualified_predicate(
    predicate: &ParsedPredicate,
    left_alias: &str,
    left_header: &[String],
    right_alias: &str,
    right_header: &[String],
) -> Result<(), ShardLoomError> {
    for column in predicate.columns() {
        bind_qualified_column(column, left_alias, left_header, right_alias, right_header)?;
    }
    Ok(())
}

fn bind_qualified_column(
    column_ref: &str,
    left_alias: &str,
    left_header: &[String],
    right_alias: &str,
    right_header: &[String],
) -> Result<(), ShardLoomError> {
    let column = parse_qualified_column_ref(column_ref)?;
    if column.alias == left_alias {
        if left_header
            .iter()
            .any(|candidate| candidate == &column.column)
        {
            Ok(())
        } else {
            Err(unsupported_sql_error(&format!(
                "qualified left column {column_ref:?} is not present in the left source header"
            )))
        }
    } else if column.alias == right_alias {
        if right_header
            .iter()
            .any(|candidate| candidate == &column.column)
        {
            Ok(())
        } else {
            Err(unsupported_sql_error(&format!(
                "qualified right column {column_ref:?} is not present in the right source header"
            )))
        }
    } else {
        Err(unsupported_sql_error(&format!(
            "qualified column {column_ref:?} does not use an admitted JOIN alias"
        )))
    }
}

#[derive(Clone, Copy)]
enum JoinProjectionShape {
    ComputedTopNFilter,
    ComputedTopN,
    RawTopNFilter,
    RawTopN,
    ComputedFilter,
    Computed,
}

impl JoinProjectionShape {
    fn statement_kind(self) -> &'static str {
        match self {
            Self::ComputedTopNFilter => {
                "local_source_inner_equi_join_computed_projection_order_by_topn_filter_limit"
            }
            Self::ComputedTopN => {
                "local_source_inner_equi_join_computed_projection_order_by_topn_limit"
            }
            Self::RawTopNFilter => "local_source_inner_equi_join_order_by_topn_filter_limit",
            Self::RawTopN => "local_source_inner_equi_join_order_by_topn_limit",
            Self::ComputedFilter => "local_source_inner_equi_join_computed_projection_filter_limit",
            Self::Computed => "local_source_inner_equi_join_computed_projection_limit",
        }
    }

    fn execution_certificate_suffix(self) -> &'static str {
        match self {
            Self::ComputedTopNFilter => {
                "inner-equi-join-computed-projection-order-by-topn-filter-limit"
            }
            Self::ComputedTopN => "inner-equi-join-computed-projection-order-by-topn-limit",
            Self::RawTopNFilter => "inner-equi-join-order-by-topn-filter-limit",
            Self::RawTopN => "inner-equi-join-order-by-topn-limit",
            Self::ComputedFilter => "inner-equi-join-computed-projection-filter-limit",
            Self::Computed => "inner-equi-join-computed-projection-limit",
        }
    }

    fn claim_gate_reason_suffix(self) -> &'static str {
        match self {
            Self::ComputedTopNFilter => {
                "inner_equi_join_computed_projection_order_by_topn_filter_limit"
            }
            Self::ComputedTopN => "inner_equi_join_computed_projection_order_by_topn_limit",
            Self::RawTopNFilter => "inner_equi_join_order_by_topn_filter_limit",
            Self::RawTopN => "inner_equi_join_order_by_topn_limit",
            Self::ComputedFilter => "inner_equi_join_computed_projection_filter_limit",
            Self::Computed => "inner_equi_join_computed_projection_limit",
        }
    }
}

impl ParsedSqlLocalSource {
    fn is_join(&self) -> bool {
        self.join.is_some()
    }

    fn is_aggregate(&self) -> bool {
        !self.aggregates.is_empty()
    }

    fn is_grouped_aggregate(&self) -> bool {
        !self.group_by.is_empty() && self.is_aggregate()
    }

    fn has_filter(&self) -> bool {
        !self.predicate.is_all()
    }

    fn has_literal_projection(&self) -> bool {
        !self.literal_projections.is_empty()
    }

    fn has_cast_projection(&self) -> bool {
        !self.cast_projections.is_empty()
    }

    fn has_null_coalesce_projection(&self) -> bool {
        !self.null_coalesce_projections.is_empty()
    }

    fn has_nullif_projection(&self) -> bool {
        !self.nullif_projections.is_empty()
    }

    fn has_conditional_projection(&self) -> bool {
        !self.conditional_projections.is_empty()
    }

    fn has_predicate_projection(&self) -> bool {
        !self.predicate_projections.is_empty()
    }

    fn has_numeric_arithmetic_projection(&self) -> bool {
        !self.numeric_arithmetic_projections.is_empty()
    }

    fn has_numeric_abs_projection(&self) -> bool {
        !self.numeric_abs_projections.is_empty()
    }

    fn has_numeric_rounding_projection(&self) -> bool {
        !self.numeric_rounding_projections.is_empty()
    }

    fn has_generic_expression_projection(&self) -> bool {
        !self.generic_expression_projections.is_empty()
    }

    fn has_date_arithmetic_projection(&self) -> bool {
        !self.date_arithmetic_projections.is_empty()
    }

    fn has_timestamp_arithmetic_projection(&self) -> bool {
        !self.timestamp_arithmetic_projections.is_empty()
    }

    fn has_string_length_projection(&self) -> bool {
        !self.string_length_projections.is_empty()
    }

    fn has_string_transform_projection(&self) -> bool {
        !self.string_transform_projections.is_empty()
    }

    fn has_string_function_projection(&self) -> bool {
        !self.string_function_projections.is_empty()
    }

    fn has_date_extract_projection(&self) -> bool {
        !self.date_extract_projections.is_empty()
    }

    fn has_timestamp_extract_projection(&self) -> bool {
        !self.timestamp_extract_projections.is_empty()
    }

    fn has_computed_projection(&self) -> bool {
        self.has_literal_projection()
            || self.has_cast_projection()
            || self.has_null_coalesce_projection()
            || self.has_nullif_projection()
            || self.has_conditional_projection()
            || self.has_predicate_projection()
            || self.has_numeric_arithmetic_projection()
            || self.has_numeric_abs_projection()
            || self.has_numeric_rounding_projection()
            || self.has_generic_expression_projection()
            || self.has_date_arithmetic_projection()
            || self.has_timestamp_arithmetic_projection()
            || self.has_string_length_projection()
            || self.has_string_transform_projection()
            || self.has_string_function_projection()
            || self.has_date_extract_projection()
            || self.has_timestamp_extract_projection()
    }

    fn join_projection_shape(&self) -> Option<JoinProjectionShape> {
        if !self.is_join() || self.is_aggregate() {
            return None;
        }
        match (
            self.has_computed_projection(),
            self.order_by.is_some(),
            self.has_filter(),
        ) {
            (true, true, true) => Some(JoinProjectionShape::ComputedTopNFilter),
            (true, true, false) => Some(JoinProjectionShape::ComputedTopN),
            (false, true, true) => Some(JoinProjectionShape::RawTopNFilter),
            (false, true, false) => Some(JoinProjectionShape::RawTopN),
            (true, false, true) => Some(JoinProjectionShape::ComputedFilter),
            (true, false, false) => Some(JoinProjectionShape::Computed),
            (false, false, _) => None,
        }
    }

    fn statement_kind(&self) -> &'static str {
        if self.is_join()
            && self.is_grouped_aggregate()
            && self.order_by.is_some()
            && self.has_filter()
        {
            "local_source_inner_equi_join_group_by_aggregate_order_by_topn_filter_limit"
        } else if self.is_join() && self.is_grouped_aggregate() && self.order_by.is_some() {
            "local_source_inner_equi_join_group_by_aggregate_order_by_topn_limit"
        } else if self.is_join()
            && self.is_aggregate()
            && self.order_by.is_some()
            && self.has_filter()
        {
            "local_source_inner_equi_join_aggregate_order_by_topn_filter_limit"
        } else if self.is_join() && self.is_aggregate() && self.order_by.is_some() {
            "local_source_inner_equi_join_aggregate_order_by_topn_limit"
        } else if self.is_join() && self.is_grouped_aggregate() && self.has_filter() {
            "local_source_inner_equi_join_group_by_aggregate_filter_limit"
        } else if self.is_join() && self.is_grouped_aggregate() {
            "local_source_inner_equi_join_group_by_aggregate_limit"
        } else if self.is_join() && self.is_aggregate() && self.has_filter() {
            "local_source_inner_equi_join_aggregate_filter_limit"
        } else if self.is_join() && self.is_aggregate() {
            "local_source_inner_equi_join_aggregate_limit"
        } else if let Some(shape) = self.join_projection_shape() {
            shape.statement_kind()
        } else if self.is_join() && self.has_filter() {
            "local_source_inner_equi_join_filter_limit"
        } else if self.is_join() {
            "local_source_inner_equi_join_limit"
        } else if self.is_grouped_aggregate() && self.order_by.is_some() && self.has_filter() {
            "local_source_group_by_aggregate_order_by_topn_filter_limit"
        } else if self.is_grouped_aggregate() && self.order_by.is_some() {
            "local_source_group_by_aggregate_order_by_topn_limit"
        } else if self.is_aggregate() && self.order_by.is_some() && self.has_filter() {
            "local_source_aggregate_order_by_topn_filter_limit"
        } else if self.is_aggregate() && self.order_by.is_some() {
            "local_source_aggregate_order_by_topn_limit"
        } else if self.order_by.is_some() && self.has_filter() {
            "local_source_order_by_topn_filter_limit"
        } else if self.order_by.is_some() {
            "local_source_order_by_topn_limit"
        } else if self.is_grouped_aggregate() && self.has_filter() {
            "local_source_group_by_aggregate_filter_limit"
        } else if self.is_grouped_aggregate() {
            "local_source_group_by_aggregate_limit"
        } else if self.is_aggregate() && self.has_filter() {
            "local_source_aggregate_filter_limit"
        } else if self.is_aggregate() {
            "local_source_aggregate_limit"
        } else if self.has_literal_projection() && self.has_filter() {
            "local_source_literal_projection_filter_limit"
        } else if self.has_literal_projection() {
            "local_source_literal_projection_limit"
        } else if self.has_computed_projection() && self.has_filter() {
            "local_source_computed_projection_filter_limit"
        } else if self.has_computed_projection() {
            "local_source_computed_projection_limit"
        } else if self.has_filter() {
            "local_source_projection_filter_limit"
        } else {
            "local_source_projection_limit"
        }
    }

    fn execution_certificate_suffix(&self) -> &'static str {
        if self.is_join()
            && self.is_grouped_aggregate()
            && self.order_by.is_some()
            && self.has_filter()
        {
            "inner-equi-join-group-by-aggregate-order-by-topn-filter-limit"
        } else if self.is_join() && self.is_grouped_aggregate() && self.order_by.is_some() {
            "inner-equi-join-group-by-aggregate-order-by-topn-limit"
        } else if self.is_join()
            && self.is_aggregate()
            && self.order_by.is_some()
            && self.has_filter()
        {
            "inner-equi-join-aggregate-order-by-topn-filter-limit"
        } else if self.is_join() && self.is_aggregate() && self.order_by.is_some() {
            "inner-equi-join-aggregate-order-by-topn-limit"
        } else if self.is_join() && self.is_grouped_aggregate() && self.has_filter() {
            "inner-equi-join-group-by-aggregate-filter-limit"
        } else if self.is_join() && self.is_grouped_aggregate() {
            "inner-equi-join-group-by-aggregate-limit"
        } else if self.is_join() && self.is_aggregate() && self.has_filter() {
            "inner-equi-join-aggregate-filter-limit"
        } else if self.is_join() && self.is_aggregate() {
            "inner-equi-join-aggregate-limit"
        } else if let Some(shape) = self.join_projection_shape() {
            shape.execution_certificate_suffix()
        } else if self.is_join() && self.has_filter() {
            "inner-equi-join-filter-limit"
        } else if self.is_join() {
            "inner-equi-join-limit"
        } else if self.is_grouped_aggregate() && self.order_by.is_some() && self.has_filter() {
            "group-by-aggregate-order-by-topn-filter-limit"
        } else if self.is_grouped_aggregate() && self.order_by.is_some() {
            "group-by-aggregate-order-by-topn-limit"
        } else if self.is_aggregate() && self.order_by.is_some() && self.has_filter() {
            "aggregate-order-by-topn-filter-limit"
        } else if self.is_aggregate() && self.order_by.is_some() {
            "aggregate-order-by-topn-limit"
        } else if self.order_by.is_some() && self.has_filter() {
            "order-by-topn-filter-limit"
        } else if self.order_by.is_some() {
            "order-by-topn-limit"
        } else if self.is_grouped_aggregate() && self.has_filter() {
            "group-by-aggregate-filter-limit"
        } else if self.is_grouped_aggregate() {
            "group-by-aggregate-limit"
        } else if self.is_aggregate() && self.has_filter() {
            "aggregate-filter-limit"
        } else if self.is_aggregate() {
            "aggregate-limit"
        } else if self.has_literal_projection() && self.has_filter() {
            "literal-projection-filter-limit"
        } else if self.has_literal_projection() {
            "literal-projection-limit"
        } else if self.has_computed_projection() && self.has_filter() {
            "computed-projection-filter-limit"
        } else if self.has_computed_projection() {
            "computed-projection-limit"
        } else if self.has_filter() {
            "projection-filter-limit"
        } else {
            "projection-limit"
        }
    }

    fn claim_gate_reason_suffix(&self) -> &'static str {
        if self.is_join()
            && self.is_grouped_aggregate()
            && self.order_by.is_some()
            && self.has_filter()
        {
            "inner_equi_join_group_by_aggregate_order_by_topn_filter_limit"
        } else if self.is_join() && self.is_grouped_aggregate() && self.order_by.is_some() {
            "inner_equi_join_group_by_aggregate_order_by_topn_limit"
        } else if self.is_join()
            && self.is_aggregate()
            && self.order_by.is_some()
            && self.has_filter()
        {
            "inner_equi_join_aggregate_order_by_topn_filter_limit"
        } else if self.is_join() && self.is_aggregate() && self.order_by.is_some() {
            "inner_equi_join_aggregate_order_by_topn_limit"
        } else if self.is_join() && self.is_grouped_aggregate() && self.has_filter() {
            "inner_equi_join_group_by_aggregate_filter_limit"
        } else if self.is_join() && self.is_grouped_aggregate() {
            "inner_equi_join_group_by_aggregate_limit"
        } else if self.is_join() && self.is_aggregate() && self.has_filter() {
            "inner_equi_join_aggregate_filter_limit"
        } else if self.is_join() && self.is_aggregate() {
            "inner_equi_join_aggregate_limit"
        } else if let Some(shape) = self.join_projection_shape() {
            shape.claim_gate_reason_suffix()
        } else if self.is_join() && self.has_filter() {
            "inner_equi_join_filter_limit"
        } else if self.is_join() {
            "inner_equi_join_limit"
        } else if self.is_grouped_aggregate() && self.order_by.is_some() && self.has_filter() {
            "group_by_aggregate_order_by_topn_filter_limit"
        } else if self.is_grouped_aggregate() && self.order_by.is_some() {
            "group_by_aggregate_order_by_topn_limit"
        } else if self.is_aggregate() && self.order_by.is_some() && self.has_filter() {
            "scalar_aggregate_order_by_topn_filter_limit"
        } else if self.is_aggregate() && self.order_by.is_some() {
            "scalar_aggregate_order_by_topn_limit"
        } else if self.order_by.is_some() && self.has_filter() {
            "order_by_topn_filter_limit"
        } else if self.order_by.is_some() {
            "order_by_topn_limit"
        } else if self.is_grouped_aggregate() && self.has_filter() {
            "group_by_aggregate_filter_limit"
        } else if self.is_grouped_aggregate() {
            "group_by_aggregate_limit"
        } else if self.is_aggregate() && self.has_filter() {
            "scalar_aggregate_filter_limit"
        } else if self.is_aggregate() {
            "scalar_aggregate_limit"
        } else if self.has_literal_projection() && self.has_filter() {
            "literal_projection_filter_limit"
        } else if self.has_literal_projection() {
            "literal_projection_limit"
        } else if self.has_computed_projection() && self.has_filter() {
            "computed_projection_filter_limit"
        } else if self.has_computed_projection() {
            "computed_projection_limit"
        } else if self.has_filter() {
            "projection_filter_limit"
        } else {
            "projection_limit"
        }
    }

    fn projection_columns(&self, header: &[String]) -> Vec<String> {
        if self.projections.len() == 1 && self.projections[0] == "*" {
            header.to_vec()
        } else {
            self.projections.clone()
        }
    }

    fn output_columns(&self, header: &[String]) -> Vec<String> {
        if self.is_grouped_aggregate() {
            self.group_by
                .iter()
                .cloned()
                .chain(self.aggregates.iter().map(ParsedAggregate::output_name))
                .collect()
        } else if self.is_aggregate() {
            self.aggregates
                .iter()
                .map(ParsedAggregate::output_name)
                .collect()
        } else {
            self.projection_output_columns(header)
        }
    }

    fn projection_output_columns(&self, header: &[String]) -> Vec<String> {
        let mut output_columns = Vec::new();
        for output in &self.projection_order {
            match output {
                ParsedProjectionOutput::Raw(column) if column == "*" => {
                    output_columns.extend(header.iter().cloned());
                }
                ParsedProjectionOutput::Raw(column)
                | ParsedProjectionOutput::Literal(column)
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
                | ParsedProjectionOutput::DateExtract(column)
                | ParsedProjectionOutput::TimestampExtract(column) => {
                    output_columns.push(column.clone());
                }
            }
        }
        output_columns
    }

    fn has_aggregate_aliases(&self) -> bool {
        self.aggregates
            .iter()
            .any(|aggregate| aggregate.alias.is_some())
    }

    fn has_distinct_aggregate(&self) -> bool {
        self.aggregates.iter().any(|aggregate| aggregate.distinct)
    }

    fn aggregate_output_columns(&self) -> String {
        if self.aggregates.is_empty() {
            "not_applicable".to_string()
        } else {
            self.aggregates
                .iter()
                .map(ParsedAggregate::output_name)
                .collect::<Vec<_>>()
                .join(",")
        }
    }

    fn aggregate_aliases(&self) -> String {
        let aliases = self
            .aggregates
            .iter()
            .filter_map(|aggregate| aggregate.alias.as_deref())
            .collect::<Vec<_>>();
        if aliases.is_empty() {
            "not_applicable".to_string()
        } else {
            aliases.join(",")
        }
    }

    fn distinct_aggregate_functions(&self) -> String {
        let functions = self
            .aggregates
            .iter()
            .filter(|aggregate| aggregate.distinct)
            .map(ParsedAggregate::label)
            .collect::<Vec<_>>();
        if functions.is_empty() {
            "not_applicable".to_string()
        } else {
            functions.join(",")
        }
    }

    fn distinct_aggregate_columns(&self) -> String {
        let columns = self
            .aggregates
            .iter()
            .filter(|aggregate| aggregate.distinct)
            .filter_map(|aggregate| aggregate.column.as_deref())
            .collect::<Vec<_>>();
        if columns.is_empty() {
            "not_applicable".to_string()
        } else {
            columns.join(",")
        }
    }

    fn distinct_aggregate_null_semantics(&self) -> String {
        if self.has_distinct_aggregate() {
            "sql_count_distinct_ignores_nulls".to_string()
        } else {
            "not_applicable".to_string()
        }
    }

    fn cast_projection_source_columns(&self) -> String {
        if self.cast_projections.is_empty() {
            "not_applicable".to_string()
        } else {
            self.cast_projections
                .iter()
                .map(|projection| projection.column.as_str())
                .collect::<Vec<_>>()
                .join(",")
        }
    }

    fn cast_projection_output_columns(&self) -> String {
        if self.cast_projections.is_empty() {
            "not_applicable".to_string()
        } else {
            self.cast_projections
                .iter()
                .map(|projection| projection.alias.as_str())
                .collect::<Vec<_>>()
                .join(",")
        }
    }

    fn cast_projection_target_dtypes(&self) -> String {
        if self.cast_projections.is_empty() {
            "not_applicable".to_string()
        } else {
            self.cast_projections
                .iter()
                .map(|projection| projection.target_dtype.as_str().to_string())
                .collect::<Vec<_>>()
                .join(",")
        }
    }

    fn cast_projection_modes(&self) -> String {
        if self.cast_projections.is_empty() {
            "not_applicable".to_string()
        } else {
            self.cast_projections
                .iter()
                .map(|projection| projection.mode.as_str())
                .collect::<Vec<_>>()
                .join(",")
        }
    }

    fn null_coalesce_projection_source_columns(&self) -> String {
        if self.null_coalesce_projections.is_empty() {
            "not_applicable".to_string()
        } else {
            self.null_coalesce_projections
                .iter()
                .map(|projection| projection.column.as_str())
                .collect::<Vec<_>>()
                .join(",")
        }
    }

    fn null_coalesce_projection_output_columns(&self) -> String {
        if self.null_coalesce_projections.is_empty() {
            "not_applicable".to_string()
        } else {
            self.null_coalesce_projections
                .iter()
                .map(|projection| projection.alias.as_str())
                .collect::<Vec<_>>()
                .join(",")
        }
    }

    fn null_coalesce_projection_fallback_dtypes(&self) -> String {
        if self.null_coalesce_projections.is_empty() {
            "not_applicable".to_string()
        } else {
            self.null_coalesce_projections
                .iter()
                .map(|projection| projection.fallback.dtype().as_str().to_string())
                .collect::<Vec<_>>()
                .join(",")
        }
    }

    fn nullif_projection_source_columns(&self) -> String {
        if self.nullif_projections.is_empty() {
            "not_applicable".to_string()
        } else {
            self.nullif_projections
                .iter()
                .map(|projection| projection.column.as_str())
                .collect::<Vec<_>>()
                .join(",")
        }
    }

    fn nullif_projection_output_columns(&self) -> String {
        if self.nullif_projections.is_empty() {
            "not_applicable".to_string()
        } else {
            self.nullif_projections
                .iter()
                .map(|projection| projection.alias.as_str())
                .collect::<Vec<_>>()
                .join(",")
        }
    }

    fn nullif_projection_sentinel_dtypes(&self) -> String {
        if self.nullif_projections.is_empty() {
            "not_applicable".to_string()
        } else {
            self.nullif_projections
                .iter()
                .map(|projection| projection.sentinel.dtype().as_str().to_string())
                .collect::<Vec<_>>()
                .join(",")
        }
    }

    fn conditional_projection_source_columns(&self) -> String {
        if self.conditional_projections.is_empty() {
            "not_applicable".to_string()
        } else {
            self.conditional_projections
                .iter()
                .map(|projection| {
                    let mut columns = projection
                        .predicate
                        .columns()
                        .into_iter()
                        .map(str::to_string)
                        .collect::<BTreeSet<_>>();
                    if let Some(column) = projection.then_branch.source_column() {
                        columns.insert(column.to_string());
                    }
                    if let Some(column) = projection.else_branch.source_column() {
                        columns.insert(column.to_string());
                    }
                    columns.into_iter().collect::<Vec<_>>().join("+")
                })
                .collect::<Vec<_>>()
                .join(",")
        }
    }

    fn conditional_projection_output_columns(&self) -> String {
        if self.conditional_projections.is_empty() {
            "not_applicable".to_string()
        } else {
            self.conditional_projections
                .iter()
                .map(|projection| projection.alias.as_str())
                .collect::<Vec<_>>()
                .join(",")
        }
    }

    fn conditional_projection_predicate_families(&self) -> String {
        if self.conditional_projections.is_empty() {
            "not_applicable".to_string()
        } else {
            self.conditional_projections
                .iter()
                .map(|projection| projection.predicate.family())
                .collect::<Vec<_>>()
                .join(",")
        }
    }

    fn conditional_projection_then_dtypes(&self) -> String {
        if self.conditional_projections.is_empty() {
            "not_applicable".to_string()
        } else {
            self.conditional_projections
                .iter()
                .map(|projection| {
                    projection
                        .then_branch
                        .dtype_label(projection.then_dtype.as_ref())
                })
                .collect::<Vec<_>>()
                .join(",")
        }
    }

    fn conditional_projection_else_dtypes(&self) -> String {
        if self.conditional_projections.is_empty() {
            "not_applicable".to_string()
        } else {
            self.conditional_projections
                .iter()
                .map(|projection| {
                    projection
                        .else_branch
                        .dtype_label(projection.else_dtype.as_ref())
                })
                .collect::<Vec<_>>()
                .join(",")
        }
    }

    fn predicate_projection_source_columns(&self) -> String {
        if self.predicate_projections.is_empty() {
            "not_applicable".to_string()
        } else {
            self.predicate_projections
                .iter()
                .map(|projection| projection.predicate.columns().join("+"))
                .collect::<Vec<_>>()
                .join(",")
        }
    }

    fn predicate_projection_output_columns(&self) -> String {
        if self.predicate_projections.is_empty() {
            "not_applicable".to_string()
        } else {
            self.predicate_projections
                .iter()
                .map(|projection| projection.alias.as_str())
                .collect::<Vec<_>>()
                .join(",")
        }
    }

    fn predicate_projection_predicate_families(&self) -> String {
        if self.predicate_projections.is_empty() {
            "not_applicable".to_string()
        } else {
            self.predicate_projections
                .iter()
                .map(|projection| projection.predicate.family())
                .collect::<Vec<_>>()
                .join(",")
        }
    }

    fn predicate_projection_null_semantics(&self) -> String {
        if self.predicate_projections.is_empty() {
            "not_applicable".to_string()
        } else {
            self.predicate_projections
                .iter()
                .map(|projection| projection.predicate.projection_null_semantics())
                .collect::<Vec<_>>()
                .join(",")
        }
    }

    fn numeric_arithmetic_projection_operators(&self) -> String {
        if self.numeric_arithmetic_projections.is_empty() {
            "not_applicable".to_string()
        } else {
            self.numeric_arithmetic_projections
                .iter()
                .map(|projection| projection.op.as_str())
                .collect::<Vec<_>>()
                .join(",")
        }
    }

    fn numeric_arithmetic_projection_source_columns(&self) -> String {
        if self.numeric_arithmetic_projections.is_empty() {
            "not_applicable".to_string()
        } else {
            self.numeric_arithmetic_projections
                .iter()
                .map(|projection| projection.column.as_str())
                .collect::<Vec<_>>()
                .join(",")
        }
    }

    fn numeric_arithmetic_projection_output_columns(&self) -> String {
        if self.numeric_arithmetic_projections.is_empty() {
            "not_applicable".to_string()
        } else {
            self.numeric_arithmetic_projections
                .iter()
                .map(|projection| projection.alias.as_str())
                .collect::<Vec<_>>()
                .join(",")
        }
    }

    fn numeric_arithmetic_projection_rhs_dtypes(&self) -> String {
        if self.numeric_arithmetic_projections.is_empty() {
            "not_applicable".to_string()
        } else {
            self.numeric_arithmetic_projections
                .iter()
                .map(|projection| projection.rhs.dtype().as_str().to_string())
                .collect::<Vec<_>>()
                .join(",")
        }
    }

    fn numeric_abs_projection_source_columns(&self) -> String {
        if self.numeric_abs_projections.is_empty() {
            "not_applicable".to_string()
        } else {
            self.numeric_abs_projections
                .iter()
                .map(|projection| projection.column.as_str())
                .collect::<Vec<_>>()
                .join(",")
        }
    }

    fn numeric_abs_projection_output_columns(&self) -> String {
        if self.numeric_abs_projections.is_empty() {
            "not_applicable".to_string()
        } else {
            self.numeric_abs_projections
                .iter()
                .map(|projection| projection.alias.as_str())
                .collect::<Vec<_>>()
                .join(",")
        }
    }

    fn numeric_rounding_projection_operators(&self) -> String {
        if self.numeric_rounding_projections.is_empty() {
            "not_applicable".to_string()
        } else {
            self.numeric_rounding_projections
                .iter()
                .map(|projection| projection.op.as_str())
                .collect::<Vec<_>>()
                .join(",")
        }
    }

    fn numeric_rounding_projection_source_columns(&self) -> String {
        if self.numeric_rounding_projections.is_empty() {
            "not_applicable".to_string()
        } else {
            self.numeric_rounding_projections
                .iter()
                .map(|projection| projection.column.as_str())
                .collect::<Vec<_>>()
                .join(",")
        }
    }

    fn numeric_rounding_projection_output_columns(&self) -> String {
        if self.numeric_rounding_projections.is_empty() {
            "not_applicable".to_string()
        } else {
            self.numeric_rounding_projections
                .iter()
                .map(|projection| projection.alias.as_str())
                .collect::<Vec<_>>()
                .join(",")
        }
    }

    fn generic_expression_projection_source_columns(&self) -> String {
        if self.generic_expression_projections.is_empty() {
            "not_applicable".to_string()
        } else {
            self.generic_expression_projections
                .iter()
                .map(|projection| projection.source_columns.join("+"))
                .collect::<Vec<_>>()
                .join(",")
        }
    }

    fn generic_expression_projection_output_columns(&self) -> String {
        if self.generic_expression_projections.is_empty() {
            "not_applicable".to_string()
        } else {
            self.generic_expression_projections
                .iter()
                .map(|projection| projection.alias.as_str())
                .collect::<Vec<_>>()
                .join(",")
        }
    }

    fn generic_expression_projection_operator_families(&self) -> String {
        if self.generic_expression_projections.is_empty() {
            "not_applicable".to_string()
        } else {
            self.generic_expression_projections
                .iter()
                .map(|projection| projection.operator_families.join("+"))
                .collect::<Vec<_>>()
                .join(",")
        }
    }

    fn generic_expression_projection_binary_operator_count(&self) -> usize {
        self.generic_expression_projections
            .iter()
            .map(|projection| projection.binary_operator_count)
            .sum()
    }

    fn date_arithmetic_projection_operators(&self) -> String {
        if self.date_arithmetic_projections.is_empty() {
            "not_applicable".to_string()
        } else {
            self.date_arithmetic_projections
                .iter()
                .map(|projection| projection.op.as_str())
                .collect::<Vec<_>>()
                .join(",")
        }
    }

    fn date_arithmetic_projection_source_columns(&self) -> String {
        if self.date_arithmetic_projections.is_empty() {
            "not_applicable".to_string()
        } else {
            self.date_arithmetic_projections
                .iter()
                .map(|projection| projection.column.as_str())
                .collect::<Vec<_>>()
                .join(",")
        }
    }

    fn date_arithmetic_projection_days(&self) -> String {
        if self.date_arithmetic_projections.is_empty() {
            "not_applicable".to_string()
        } else {
            self.date_arithmetic_projections
                .iter()
                .map(|projection| projection.day_count.to_string())
                .collect::<Vec<_>>()
                .join(",")
        }
    }

    fn date_arithmetic_projection_output_columns(&self) -> String {
        if self.date_arithmetic_projections.is_empty() {
            "not_applicable".to_string()
        } else {
            self.date_arithmetic_projections
                .iter()
                .map(|projection| projection.alias.as_str())
                .collect::<Vec<_>>()
                .join(",")
        }
    }

    fn timestamp_arithmetic_projection_operators(&self) -> String {
        if self.timestamp_arithmetic_projections.is_empty() {
            "not_applicable".to_string()
        } else {
            self.timestamp_arithmetic_projections
                .iter()
                .map(|projection| projection.op.as_str())
                .collect::<Vec<_>>()
                .join(",")
        }
    }

    fn timestamp_arithmetic_projection_source_columns(&self) -> String {
        if self.timestamp_arithmetic_projections.is_empty() {
            "not_applicable".to_string()
        } else {
            self.timestamp_arithmetic_projections
                .iter()
                .map(|projection| projection.column.as_str())
                .collect::<Vec<_>>()
                .join(",")
        }
    }

    fn timestamp_arithmetic_projection_seconds(&self) -> String {
        if self.timestamp_arithmetic_projections.is_empty() {
            "not_applicable".to_string()
        } else {
            self.timestamp_arithmetic_projections
                .iter()
                .map(|projection| projection.second_count.to_string())
                .collect::<Vec<_>>()
                .join(",")
        }
    }

    fn timestamp_arithmetic_projection_output_columns(&self) -> String {
        if self.timestamp_arithmetic_projections.is_empty() {
            "not_applicable".to_string()
        } else {
            self.timestamp_arithmetic_projections
                .iter()
                .map(|projection| projection.alias.as_str())
                .collect::<Vec<_>>()
                .join(",")
        }
    }

    fn string_length_projection_source_columns(&self) -> String {
        if self.string_length_projections.is_empty() {
            "not_applicable".to_string()
        } else {
            self.string_length_projections
                .iter()
                .map(|projection| projection.column.as_str())
                .collect::<Vec<_>>()
                .join(",")
        }
    }

    fn string_length_projection_output_columns(&self) -> String {
        if self.string_length_projections.is_empty() {
            "not_applicable".to_string()
        } else {
            self.string_length_projections
                .iter()
                .map(|projection| projection.alias.as_str())
                .collect::<Vec<_>>()
                .join(",")
        }
    }

    fn string_transform_projection_operators(&self) -> String {
        if self.string_transform_projections.is_empty() {
            "not_applicable".to_string()
        } else {
            self.string_transform_projections
                .iter()
                .map(|projection| projection.op.as_str())
                .collect::<Vec<_>>()
                .join(",")
        }
    }

    fn string_transform_projection_source_columns(&self) -> String {
        if self.string_transform_projections.is_empty() {
            "not_applicable".to_string()
        } else {
            self.string_transform_projections
                .iter()
                .map(|projection| projection.column.as_str())
                .collect::<Vec<_>>()
                .join(",")
        }
    }

    fn string_transform_projection_output_columns(&self) -> String {
        if self.string_transform_projections.is_empty() {
            "not_applicable".to_string()
        } else {
            self.string_transform_projections
                .iter()
                .map(|projection| projection.alias.as_str())
                .collect::<Vec<_>>()
                .join(",")
        }
    }

    fn string_function_projection_operators(&self) -> String {
        if self.string_function_projections.is_empty() {
            "not_applicable".to_string()
        } else {
            self.string_function_projections
                .iter()
                .map(|projection| projection.op.as_str())
                .collect::<Vec<_>>()
                .join(",")
        }
    }

    fn string_function_projection_source_columns(&self) -> String {
        if self.string_function_projections.is_empty() {
            "not_applicable".to_string()
        } else {
            self.string_function_projections
                .iter()
                .map(|projection| projection.source_columns.join("+"))
                .collect::<Vec<_>>()
                .join(",")
        }
    }

    fn string_function_projection_output_columns(&self) -> String {
        if self.string_function_projections.is_empty() {
            "not_applicable".to_string()
        } else {
            self.string_function_projections
                .iter()
                .map(|projection| projection.alias.as_str())
                .collect::<Vec<_>>()
                .join(",")
        }
    }

    fn string_function_projection_literal_counts(&self) -> String {
        if self.string_function_projections.is_empty() {
            "not_applicable".to_string()
        } else {
            self.string_function_projections
                .iter()
                .map(|projection| projection.literal_count.to_string())
                .collect::<Vec<_>>()
                .join(",")
        }
    }

    fn date_extract_projection_operators(&self) -> String {
        if self.date_extract_projections.is_empty() {
            "not_applicable".to_string()
        } else {
            self.date_extract_projections
                .iter()
                .map(|projection| projection.op.as_str())
                .collect::<Vec<_>>()
                .join(",")
        }
    }

    fn date_extract_projection_source_columns(&self) -> String {
        if self.date_extract_projections.is_empty() {
            "not_applicable".to_string()
        } else {
            self.date_extract_projections
                .iter()
                .map(|projection| projection.column.as_str())
                .collect::<Vec<_>>()
                .join(",")
        }
    }

    fn date_extract_projection_output_columns(&self) -> String {
        if self.date_extract_projections.is_empty() {
            "not_applicable".to_string()
        } else {
            self.date_extract_projections
                .iter()
                .map(|projection| projection.alias.as_str())
                .collect::<Vec<_>>()
                .join(",")
        }
    }

    fn timestamp_extract_projection_operators(&self) -> String {
        if self.timestamp_extract_projections.is_empty() {
            "not_applicable".to_string()
        } else {
            self.timestamp_extract_projections
                .iter()
                .map(|projection| projection.op.as_str())
                .collect::<Vec<_>>()
                .join(",")
        }
    }

    fn timestamp_extract_projection_source_columns(&self) -> String {
        if self.timestamp_extract_projections.is_empty() {
            "not_applicable".to_string()
        } else {
            self.timestamp_extract_projections
                .iter()
                .map(|projection| projection.column.as_str())
                .collect::<Vec<_>>()
                .join(",")
        }
    }

    fn timestamp_extract_projection_output_columns(&self) -> String {
        if self.timestamp_extract_projections.is_empty() {
            "not_applicable".to_string()
        } else {
            self.timestamp_extract_projections
                .iter()
                .map(|projection| projection.alias.as_str())
                .collect::<Vec<_>>()
                .join(",")
        }
    }
}

impl ParsedAggregate {
    fn output_name(&self) -> String {
        if let Some(alias) = self.alias.as_ref() {
            return alias.clone();
        }
        match (self.function, self.column.as_deref(), self.distinct) {
            (AggregateFunction::Count, None, _) => "count_all".to_string(),
            (function, Some(column), true) => {
                format!("{}_distinct_{}", function.as_str(), column)
            }
            (function, Some(column), false) => format!("{}_{}", function.as_str(), column),
            (function, None, _) => function.as_str().to_string(),
        }
    }

    fn label(&self) -> String {
        match (self.function, self.column.as_deref(), self.distinct) {
            (AggregateFunction::Count, None, _) => "count(*)".to_string(),
            (function, Some(column), true) => {
                format!("{}(DISTINCT {column})", function.as_str())
            }
            (function, Some(column), false) => format!("{}({column})", function.as_str()),
            (function, None, _) => format!("{}()", function.as_str()),
        }
    }

    fn required_column(&self) -> Result<&str, ShardLoomError> {
        self.column.as_deref().ok_or_else(|| {
            unsupported_sql_error("aggregate function requires a column in this scoped smoke")
        })
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

impl ParsedOrderBy {
    fn is_multi_key(&self) -> bool {
        self.keys.len() > 1
    }

    fn columns_label(&self) -> String {
        self.keys
            .iter()
            .map(|key| key.column.as_str())
            .collect::<Vec<_>>()
            .join(",")
    }

    fn directions_label(&self) -> String {
        self.keys
            .iter()
            .map(|key| key.direction.as_str())
            .collect::<Vec<_>>()
            .join(",")
    }

    fn operator_family_label(&self) -> &'static str {
        if self.is_multi_key() {
            "multi_key_scalar_topn"
        } else {
            "single_key_scalar_topn"
        }
    }
}

impl QualifiedColumn {
    fn to_ref(&self) -> String {
        qualified_column_name(&self.alias, &self.column)
    }
}

impl ParsedJoin {
    fn key_arity(&self) -> usize {
        self.key_pairs.len()
    }

    fn is_multi_key(&self) -> bool {
        self.key_arity() > 1
    }

    fn left_key_refs(&self) -> String {
        self.key_pairs
            .iter()
            .map(|pair| pair.left.to_ref())
            .collect::<Vec<_>>()
            .join(",")
    }

    fn right_key_refs(&self) -> String {
        self.key_pairs
            .iter()
            .map(|pair| pair.right.to_ref())
            .collect::<Vec<_>>()
            .join(",")
    }
}

impl SortDirection {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Asc => "asc",
            Self::Desc => "desc",
        }
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

    fn push_columns<'a>(&'a self, columns: &mut Vec<&'a str>) {
        match self {
            Self::All => {}
            Self::Compare { column, .. }
            | Self::CastCompare { column, .. }
            | Self::NumericArithmeticCompare { column, .. }
            | Self::NumericAbsCompare { column, .. }
            | Self::NumericRoundingCompare { column, .. }
            | Self::DateArithmeticCompare { column, .. }
            | Self::TimestampArithmeticCompare { column, .. }
            | Self::DateExtractCompare { column, .. }
            | Self::StringLengthCompare { column, .. }
            | Self::TimestampExtractCompare { column, .. }
            | Self::BooleanPredicate { column, .. }
            | Self::IsNull { column }
            | Self::IsNotNull { column }
            | Self::InList { column, .. }
            | Self::InSubquery { column, .. }
            | Self::StringMatch { column, .. }
            | Self::StringTransformCompare { column, .. } => columns.push(column),
            Self::StringFunctionCompare { source_columns, .. }
            | Self::GenericExpressionCompare { source_columns, .. } => {
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
            Self::StringLengthCompare {
                column,
                comparison,
                value,
            } => string_length_compare_expression(column, *comparison, value),
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
            Self::StringTransformCompare {
                column,
                op,
                comparison,
                value,
            } => string_transform_compare_expression(column, *op, *comparison, value),
            Self::StringFunctionCompare {
                expression,
                comparison,
                value,
                ..
            } => string_function_compare_expression(expression, *comparison, value),
            Self::IsNull { column } => null_predicate_expression(column, true),
            Self::IsNotNull { column } => null_predicate_expression(column, false),
            Self::InList { column, values } => in_list_expression(column, values),
            Self::InSubquery { column, subquery } => in_subquery_expression(column, subquery),
            Self::StringMatch { column, op, value } => string_match_expression(column, *op, value),
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
        }
    }

    fn cast_compare_expression(&self) -> Result<Expression, ShardLoomError> {
        let Self::CastCompare {
            column,
            target_dtype,
            mode,
            op,
            value,
        } = self
        else {
            return Err(ShardLoomError::InvalidOperation(
                "internal error: non-cast predicate cannot lower through cast expression"
                    .to_string(),
            ));
        };
        cast_compare_expression(column, target_dtype, *mode, *op, value)
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

    fn family(&self) -> &'static str {
        match self {
            Self::All => "none",
            Self::Compare { .. } => "comparison",
            Self::CastCompare { .. } => "cast",
            Self::NumericArithmeticCompare { .. } => "numeric_arithmetic",
            Self::NumericAbsCompare { .. } => "numeric_abs",
            Self::NumericRoundingCompare { .. } => "numeric_rounding",
            Self::GenericExpressionCompare { .. } => "generic_expression",
            Self::DateArithmeticCompare { .. } => "date_arithmetic",
            Self::TimestampArithmeticCompare { .. } => "timestamp_arithmetic",
            Self::DateExtractCompare { .. } => "date_extract",
            Self::StringLengthCompare { .. } => "string_length",
            Self::TimestampExtractCompare { .. } => "timestamp_extract",
            Self::BooleanPredicate { .. } => "boolean_predicate",
            Self::StringTransformCompare { .. } => "string_transform",
            Self::StringFunctionCompare { .. } => "string_function",
            Self::IsNull { .. } | Self::IsNotNull { .. } => "null_predicate",
            Self::InList { .. } => "in_predicate",
            Self::InSubquery { .. } => "in_subquery",
            Self::StringMatch { .. } => "string_predicate",
            Self::Logical { .. } | Self::Not { .. } => "logical_predicate",
        }
    }

    fn uses_null_predicate(&self) -> bool {
        match self {
            Self::IsNull { .. } | Self::IsNotNull { .. } => true,
            Self::Logical { left, right, .. } => {
                left.uses_null_predicate() || right.uses_null_predicate()
            }
            Self::Not { inner } => inner.uses_null_predicate(),
            Self::All
            | Self::Compare { .. }
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
            | Self::InList { .. }
            | Self::InSubquery { .. }
            | Self::StringMatch { .. } => false,
        }
    }

    fn null_predicate_operator(&self) -> String {
        let mut operators = Vec::new();
        self.push_null_predicate_operators(&mut operators);
        if operators.is_empty() {
            "not_applicable".to_string()
        } else {
            operators.join(",")
        }
    }

    fn push_null_predicate_operators(&self, operators: &mut Vec<&'static str>) {
        match self {
            Self::IsNull { .. } => operators.push("is_null"),
            Self::IsNotNull { .. } => operators.push("is_not_null"),
            Self::Logical { left, right, .. } => {
                left.push_null_predicate_operators(operators);
                right.push_null_predicate_operators(operators);
            }
            Self::Not { inner } => inner.push_null_predicate_operators(operators),
            Self::All
            | Self::Compare { .. }
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
            | Self::InList { .. }
            | Self::InSubquery { .. }
            | Self::StringMatch { .. } => {}
        }
    }

    fn null_predicate_source_columns(&self) -> String {
        let mut columns = Vec::new();
        self.push_null_predicate_source_columns(&mut columns);
        if columns.is_empty() {
            "not_applicable".to_string()
        } else {
            columns.join(",")
        }
    }

    fn push_null_predicate_source_columns<'a>(&'a self, columns: &mut Vec<&'a str>) {
        match self {
            Self::IsNull { column } | Self::IsNotNull { column } => columns.push(column),
            Self::Logical { left, right, .. } => {
                left.push_null_predicate_source_columns(columns);
                right.push_null_predicate_source_columns(columns);
            }
            Self::Not { inner } => inner.push_null_predicate_source_columns(columns),
            Self::All
            | Self::Compare { .. }
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
            | Self::InList { .. }
            | Self::InSubquery { .. }
            | Self::StringMatch { .. } => {}
        }
    }

    fn uses_boolean_predicate(&self) -> bool {
        match self {
            Self::BooleanPredicate { .. } => true,
            Self::Logical { left, right, .. } => {
                left.uses_boolean_predicate() || right.uses_boolean_predicate()
            }
            Self::Not { inner } => inner.uses_boolean_predicate(),
            Self::All
            | Self::Compare { .. }
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
            | Self::IsNull { .. }
            | Self::IsNotNull { .. }
            | Self::InList { .. }
            | Self::InSubquery { .. }
            | Self::StringMatch { .. } => false,
        }
    }

    fn boolean_predicate_operator(&self) -> String {
        let mut operators = Vec::new();
        self.push_boolean_predicate_operators(&mut operators);
        if operators.is_empty() {
            "not_applicable".to_string()
        } else {
            operators.join(",")
        }
    }

    fn push_boolean_predicate_operators(&self, operators: &mut Vec<&'static str>) {
        match self {
            Self::BooleanPredicate {
                expected: true,
                negated: false,
                ..
            } => operators.push("is_true"),
            Self::BooleanPredicate {
                expected: false,
                negated: false,
                ..
            } => operators.push("is_false"),
            Self::BooleanPredicate {
                expected: true,
                negated: true,
                ..
            } => operators.push("is_not_true"),
            Self::BooleanPredicate {
                expected: false,
                negated: true,
                ..
            } => operators.push("is_not_false"),
            Self::Logical { left, right, .. } => {
                left.push_boolean_predicate_operators(operators);
                right.push_boolean_predicate_operators(operators);
            }
            Self::Not { inner } => inner.push_boolean_predicate_operators(operators),
            Self::All
            | Self::Compare { .. }
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
            | Self::IsNull { .. }
            | Self::IsNotNull { .. }
            | Self::InList { .. }
            | Self::InSubquery { .. }
            | Self::StringMatch { .. } => {}
        }
    }

    fn boolean_predicate_source_columns(&self) -> String {
        let mut columns = Vec::new();
        self.push_boolean_predicate_source_columns(&mut columns);
        if columns.is_empty() {
            "not_applicable".to_string()
        } else {
            columns.join(",")
        }
    }

    fn push_boolean_predicate_source_columns<'a>(&'a self, columns: &mut Vec<&'a str>) {
        match self {
            Self::BooleanPredicate { column, .. } => columns.push(column),
            Self::Logical { left, right, .. } => {
                left.push_boolean_predicate_source_columns(columns);
                right.push_boolean_predicate_source_columns(columns);
            }
            Self::Not { inner } => inner.push_boolean_predicate_source_columns(columns),
            Self::All
            | Self::Compare { .. }
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
            | Self::IsNull { .. }
            | Self::IsNotNull { .. }
            | Self::InList { .. }
            | Self::InSubquery { .. }
            | Self::StringMatch { .. } => {}
        }
    }

    fn boolean_predicate_null_semantics(&self) -> &'static str {
        if !self.uses_boolean_predicate() {
            "not_applicable"
        } else if self.uses_boolean_is_not_truth_match() {
            "sql_boolean_is_not_true_false_null_matches"
        } else {
            "sql_where_true_only_null_filters_out"
        }
    }

    fn projection_null_semantics(&self) -> &'static str {
        if self.uses_null_predicate() {
            "sql_is_null_is_not_null"
        } else if self.uses_boolean_predicate() {
            self.boolean_predicate_null_semantics()
        } else {
            "sql_three_valued_boolean_or_null_projection"
        }
    }

    fn uses_boolean_is_not_truth_match(&self) -> bool {
        match self {
            Self::BooleanPredicate { negated: true, .. } => true,
            Self::Not { inner } => match inner.as_ref() {
                Self::BooleanPredicate {
                    null_is_false: true,
                    ..
                } => true,
                _ => inner.uses_boolean_is_not_truth_match(),
            },
            Self::Logical { left, right, .. } => {
                left.uses_boolean_is_not_truth_match() || right.uses_boolean_is_not_truth_match()
            }
            Self::All
            | Self::Compare { .. }
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
            | Self::InSubquery { .. }
            | Self::StringMatch { .. } => false,
        }
    }

    fn uses_string_predicate(&self) -> bool {
        match self {
            Self::StringMatch { .. } => true,
            Self::Logical { left, right, .. } => {
                left.uses_string_predicate() || right.uses_string_predicate()
            }
            Self::Not { inner } => inner.uses_string_predicate(),
            Self::All
            | Self::Compare { .. }
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
            | Self::InSubquery { .. } => false,
        }
    }

    fn string_operator(&self) -> String {
        let mut operators = Vec::new();
        self.push_string_operators(&mut operators);
        if operators.is_empty() {
            "not_applicable".to_string()
        } else {
            operators.join(",")
        }
    }

    fn push_string_operators(&self, operators: &mut Vec<&'static str>) {
        match self {
            Self::StringMatch { op, .. } => operators.push(op.as_str()),
            Self::Logical { left, right, .. } => {
                left.push_string_operators(operators);
                right.push_string_operators(operators);
            }
            Self::Not { inner } => inner.push_string_operators(operators),
            Self::All
            | Self::Compare { .. }
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
            | Self::InSubquery { .. } => {}
        }
    }

    fn uses_string_transform(&self) -> bool {
        match self {
            Self::StringTransformCompare { .. } => true,
            Self::Logical { left, right, .. } => {
                left.uses_string_transform() || right.uses_string_transform()
            }
            Self::Not { inner } => inner.uses_string_transform(),
            Self::All
            | Self::Compare { .. }
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
            | Self::StringFunctionCompare { .. }
            | Self::BooleanPredicate { .. }
            | Self::IsNull { .. }
            | Self::IsNotNull { .. }
            | Self::InList { .. }
            | Self::InSubquery { .. }
            | Self::StringMatch { .. } => false,
        }
    }

    fn string_transform_operator(&self) -> String {
        let mut operators = Vec::new();
        self.push_string_transform_operators(&mut operators);
        if operators.is_empty() {
            "not_applicable".to_string()
        } else {
            operators.join(",")
        }
    }

    fn push_string_transform_operators(&self, operators: &mut Vec<&'static str>) {
        match self {
            Self::StringTransformCompare { op, .. } => operators.push(op.as_str()),
            Self::Logical { left, right, .. } => {
                left.push_string_transform_operators(operators);
                right.push_string_transform_operators(operators);
            }
            Self::Not { inner } => inner.push_string_transform_operators(operators),
            Self::All
            | Self::Compare { .. }
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
            | Self::StringFunctionCompare { .. }
            | Self::BooleanPredicate { .. }
            | Self::IsNull { .. }
            | Self::IsNotNull { .. }
            | Self::InList { .. }
            | Self::InSubquery { .. }
            | Self::StringMatch { .. } => {}
        }
    }

    fn string_transform_source_columns(&self) -> String {
        let mut columns = Vec::new();
        self.push_string_transform_source_columns(&mut columns);
        if columns.is_empty() {
            "not_applicable".to_string()
        } else {
            columns.join(",")
        }
    }

    fn push_string_transform_source_columns<'a>(&'a self, columns: &mut Vec<&'a str>) {
        match self {
            Self::StringTransformCompare { column, .. } => columns.push(column),
            Self::Logical { left, right, .. } => {
                left.push_string_transform_source_columns(columns);
                right.push_string_transform_source_columns(columns);
            }
            Self::Not { inner } => inner.push_string_transform_source_columns(columns),
            Self::All
            | Self::Compare { .. }
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
            | Self::StringFunctionCompare { .. }
            | Self::BooleanPredicate { .. }
            | Self::IsNull { .. }
            | Self::IsNotNull { .. }
            | Self::InList { .. }
            | Self::InSubquery { .. }
            | Self::StringMatch { .. } => {}
        }
    }

    fn uses_string_length(&self) -> bool {
        match self {
            Self::StringLengthCompare { .. } => true,
            Self::Logical { left, right, .. } => {
                left.uses_string_length() || right.uses_string_length()
            }
            Self::Not { inner } => inner.uses_string_length(),
            Self::All
            | Self::Compare { .. }
            | Self::CastCompare { .. }
            | Self::NumericArithmeticCompare { .. }
            | Self::NumericAbsCompare { .. }
            | Self::NumericRoundingCompare { .. }
            | Self::GenericExpressionCompare { .. }
            | Self::DateArithmeticCompare { .. }
            | Self::TimestampArithmeticCompare { .. }
            | Self::DateExtractCompare { .. }
            | Self::TimestampExtractCompare { .. }
            | Self::StringTransformCompare { .. }
            | Self::StringFunctionCompare { .. }
            | Self::BooleanPredicate { .. }
            | Self::IsNull { .. }
            | Self::IsNotNull { .. }
            | Self::InList { .. }
            | Self::InSubquery { .. }
            | Self::StringMatch { .. } => false,
        }
    }

    fn string_length_source_columns(&self) -> String {
        let mut columns = Vec::new();
        self.push_string_length_source_columns(&mut columns);
        if columns.is_empty() {
            "not_applicable".to_string()
        } else {
            columns.join(",")
        }
    }

    fn push_string_length_source_columns<'a>(&'a self, columns: &mut Vec<&'a str>) {
        match self {
            Self::StringLengthCompare { column, .. } => columns.push(column),
            Self::Logical { left, right, .. } => {
                left.push_string_length_source_columns(columns);
                right.push_string_length_source_columns(columns);
            }
            Self::Not { inner } => inner.push_string_length_source_columns(columns),
            Self::All
            | Self::Compare { .. }
            | Self::CastCompare { .. }
            | Self::NumericArithmeticCompare { .. }
            | Self::NumericAbsCompare { .. }
            | Self::NumericRoundingCompare { .. }
            | Self::GenericExpressionCompare { .. }
            | Self::DateArithmeticCompare { .. }
            | Self::TimestampArithmeticCompare { .. }
            | Self::DateExtractCompare { .. }
            | Self::TimestampExtractCompare { .. }
            | Self::StringTransformCompare { .. }
            | Self::StringFunctionCompare { .. }
            | Self::BooleanPredicate { .. }
            | Self::IsNull { .. }
            | Self::IsNotNull { .. }
            | Self::InList { .. }
            | Self::InSubquery { .. }
            | Self::StringMatch { .. } => {}
        }
    }

    fn string_length_rhs_dtypes(&self) -> String {
        let mut dtypes = Vec::new();
        self.push_string_length_rhs_dtypes(&mut dtypes);
        if dtypes.is_empty() {
            "not_applicable".to_string()
        } else {
            dtypes.join(",")
        }
    }

    fn push_string_length_rhs_dtypes(&self, dtypes: &mut Vec<String>) {
        match self {
            Self::StringLengthCompare { value, .. } => {
                dtypes.push(value.dtype().as_str().to_string());
            }
            Self::Logical { left, right, .. } => {
                left.push_string_length_rhs_dtypes(dtypes);
                right.push_string_length_rhs_dtypes(dtypes);
            }
            Self::Not { inner } => inner.push_string_length_rhs_dtypes(dtypes),
            Self::All
            | Self::Compare { .. }
            | Self::CastCompare { .. }
            | Self::NumericArithmeticCompare { .. }
            | Self::NumericAbsCompare { .. }
            | Self::NumericRoundingCompare { .. }
            | Self::GenericExpressionCompare { .. }
            | Self::DateArithmeticCompare { .. }
            | Self::TimestampArithmeticCompare { .. }
            | Self::DateExtractCompare { .. }
            | Self::TimestampExtractCompare { .. }
            | Self::StringTransformCompare { .. }
            | Self::StringFunctionCompare { .. }
            | Self::BooleanPredicate { .. }
            | Self::IsNull { .. }
            | Self::IsNotNull { .. }
            | Self::InList { .. }
            | Self::InSubquery { .. }
            | Self::StringMatch { .. } => {}
        }
    }

    fn uses_string_function(&self) -> bool {
        match self {
            Self::StringFunctionCompare { .. } => true,
            Self::Logical { left, right, .. } => {
                left.uses_string_function() || right.uses_string_function()
            }
            Self::Not { inner } => inner.uses_string_function(),
            Self::All
            | Self::Compare { .. }
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
            | Self::BooleanPredicate { .. }
            | Self::IsNull { .. }
            | Self::IsNotNull { .. }
            | Self::InList { .. }
            | Self::InSubquery { .. }
            | Self::StringMatch { .. } => false,
        }
    }

    fn string_function_operator(&self) -> String {
        let mut operators = Vec::new();
        self.push_string_function_operators(&mut operators);
        if operators.is_empty() {
            "not_applicable".to_string()
        } else {
            operators.join(",")
        }
    }

    fn push_string_function_operators(&self, operators: &mut Vec<&'static str>) {
        match self {
            Self::StringFunctionCompare { op, .. } => operators.push(op.as_str()),
            Self::Logical { left, right, .. } => {
                left.push_string_function_operators(operators);
                right.push_string_function_operators(operators);
            }
            Self::Not { inner } => inner.push_string_function_operators(operators),
            Self::All
            | Self::Compare { .. }
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
            | Self::BooleanPredicate { .. }
            | Self::IsNull { .. }
            | Self::IsNotNull { .. }
            | Self::InList { .. }
            | Self::InSubquery { .. }
            | Self::StringMatch { .. } => {}
        }
    }

    fn string_function_source_columns(&self) -> String {
        let mut columns = Vec::new();
        self.push_string_function_source_columns(&mut columns);
        if columns.is_empty() {
            "not_applicable".to_string()
        } else {
            columns.join(",")
        }
    }

    fn push_string_function_source_columns(&self, columns: &mut Vec<String>) {
        match self {
            Self::StringFunctionCompare { source_columns, .. } => {
                columns.push(source_columns.join("+"));
            }
            Self::Logical { left, right, .. } => {
                left.push_string_function_source_columns(columns);
                right.push_string_function_source_columns(columns);
            }
            Self::Not { inner } => inner.push_string_function_source_columns(columns),
            Self::All
            | Self::Compare { .. }
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
            | Self::BooleanPredicate { .. }
            | Self::IsNull { .. }
            | Self::IsNotNull { .. }
            | Self::InList { .. }
            | Self::InSubquery { .. }
            | Self::StringMatch { .. } => {}
        }
    }

    fn string_function_literal_counts(&self) -> String {
        let mut counts = Vec::new();
        self.push_string_function_literal_counts(&mut counts);
        if counts.is_empty() {
            "not_applicable".to_string()
        } else {
            counts.join(",")
        }
    }

    fn push_string_function_literal_counts(&self, counts: &mut Vec<String>) {
        match self {
            Self::StringFunctionCompare { literal_count, .. } => {
                counts.push(literal_count.to_string());
            }
            Self::Logical { left, right, .. } => {
                left.push_string_function_literal_counts(counts);
                right.push_string_function_literal_counts(counts);
            }
            Self::Not { inner } => inner.push_string_function_literal_counts(counts),
            Self::All
            | Self::Compare { .. }
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
            | Self::BooleanPredicate { .. }
            | Self::IsNull { .. }
            | Self::IsNotNull { .. }
            | Self::InList { .. }
            | Self::InSubquery { .. }
            | Self::StringMatch { .. } => {}
        }
    }

    fn string_function_rhs_dtypes(&self) -> String {
        let mut dtypes = Vec::new();
        self.push_string_function_rhs_dtypes(&mut dtypes);
        if dtypes.is_empty() {
            "not_applicable".to_string()
        } else {
            dtypes.join(",")
        }
    }

    fn push_string_function_rhs_dtypes(&self, dtypes: &mut Vec<String>) {
        match self {
            Self::StringFunctionCompare { value, .. } => {
                dtypes.push(value.dtype().as_str().to_string());
            }
            Self::Logical { left, right, .. } => {
                left.push_string_function_rhs_dtypes(dtypes);
                right.push_string_function_rhs_dtypes(dtypes);
            }
            Self::Not { inner } => inner.push_string_function_rhs_dtypes(dtypes),
            Self::All
            | Self::Compare { .. }
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
            | Self::BooleanPredicate { .. }
            | Self::IsNull { .. }
            | Self::IsNotNull { .. }
            | Self::InList { .. }
            | Self::InSubquery { .. }
            | Self::StringMatch { .. } => {}
        }
    }

    fn uses_numeric_arithmetic(&self) -> bool {
        match self {
            Self::NumericArithmeticCompare { .. } => true,
            Self::Logical { left, right, .. } => {
                left.uses_numeric_arithmetic() || right.uses_numeric_arithmetic()
            }
            Self::Not { inner } => inner.uses_numeric_arithmetic(),
            Self::All
            | Self::Compare { .. }
            | Self::CastCompare { .. }
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
            | Self::InSubquery { .. }
            | Self::StringMatch { .. } => false,
        }
    }

    fn numeric_arithmetic_operator(&self) -> String {
        let mut operators = Vec::new();
        self.push_numeric_arithmetic_operators(&mut operators);
        if operators.is_empty() {
            "not_applicable".to_string()
        } else {
            operators.join(",")
        }
    }

    fn push_numeric_arithmetic_operators(&self, operators: &mut Vec<&'static str>) {
        match self {
            Self::NumericArithmeticCompare { op, .. } => operators.push(op.as_str()),
            Self::Logical { left, right, .. } => {
                left.push_numeric_arithmetic_operators(operators);
                right.push_numeric_arithmetic_operators(operators);
            }
            Self::Not { inner } => inner.push_numeric_arithmetic_operators(operators),
            Self::All
            | Self::Compare { .. }
            | Self::CastCompare { .. }
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
            | Self::InSubquery { .. }
            | Self::StringMatch { .. } => {}
        }
    }

    fn numeric_arithmetic_source_columns(&self) -> String {
        let mut columns = Vec::new();
        self.push_numeric_arithmetic_source_columns(&mut columns);
        if columns.is_empty() {
            "not_applicable".to_string()
        } else {
            columns.join(",")
        }
    }

    fn push_numeric_arithmetic_source_columns<'a>(&'a self, columns: &mut Vec<&'a str>) {
        match self {
            Self::NumericArithmeticCompare { column, .. } => columns.push(column),
            Self::Logical { left, right, .. } => {
                left.push_numeric_arithmetic_source_columns(columns);
                right.push_numeric_arithmetic_source_columns(columns);
            }
            Self::Not { inner } => inner.push_numeric_arithmetic_source_columns(columns),
            Self::All
            | Self::Compare { .. }
            | Self::CastCompare { .. }
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
            | Self::InSubquery { .. }
            | Self::StringMatch { .. } => {}
        }
    }

    fn numeric_arithmetic_rhs_dtypes(&self) -> String {
        let mut dtypes = Vec::new();
        self.push_numeric_arithmetic_rhs_dtypes(&mut dtypes);
        if dtypes.is_empty() {
            "not_applicable".to_string()
        } else {
            dtypes.join(",")
        }
    }

    fn push_numeric_arithmetic_rhs_dtypes(&self, dtypes: &mut Vec<String>) {
        match self {
            Self::NumericArithmeticCompare { rhs, .. } => {
                dtypes.push(rhs.dtype().as_str().to_string());
            }
            Self::Logical { left, right, .. } => {
                left.push_numeric_arithmetic_rhs_dtypes(dtypes);
                right.push_numeric_arithmetic_rhs_dtypes(dtypes);
            }
            Self::Not { inner } => inner.push_numeric_arithmetic_rhs_dtypes(dtypes),
            Self::All
            | Self::Compare { .. }
            | Self::CastCompare { .. }
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
            | Self::InSubquery { .. }
            | Self::StringMatch { .. } => {}
        }
    }

    fn uses_numeric_abs(&self) -> bool {
        match self {
            Self::NumericAbsCompare { .. } => true,
            Self::Logical { left, right, .. } => {
                left.uses_numeric_abs() || right.uses_numeric_abs()
            }
            Self::Not { inner } => inner.uses_numeric_abs(),
            Self::All
            | Self::Compare { .. }
            | Self::CastCompare { .. }
            | Self::NumericArithmeticCompare { .. }
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
            | Self::InSubquery { .. }
            | Self::StringMatch { .. } => false,
        }
    }

    fn numeric_abs_source_columns(&self) -> String {
        let mut columns = Vec::new();
        self.push_numeric_abs_source_columns(&mut columns);
        if columns.is_empty() {
            "not_applicable".to_string()
        } else {
            columns.join(",")
        }
    }

    fn push_numeric_abs_source_columns<'a>(&'a self, columns: &mut Vec<&'a str>) {
        match self {
            Self::NumericAbsCompare { column, .. } => columns.push(column),
            Self::Logical { left, right, .. } => {
                left.push_numeric_abs_source_columns(columns);
                right.push_numeric_abs_source_columns(columns);
            }
            Self::Not { inner } => inner.push_numeric_abs_source_columns(columns),
            Self::All
            | Self::Compare { .. }
            | Self::CastCompare { .. }
            | Self::NumericArithmeticCompare { .. }
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
            | Self::InSubquery { .. }
            | Self::StringMatch { .. } => {}
        }
    }

    fn numeric_abs_rhs_dtypes(&self) -> String {
        let mut dtypes = Vec::new();
        self.push_numeric_abs_rhs_dtypes(&mut dtypes);
        if dtypes.is_empty() {
            "not_applicable".to_string()
        } else {
            dtypes.join(",")
        }
    }

    fn push_numeric_abs_rhs_dtypes(&self, dtypes: &mut Vec<String>) {
        match self {
            Self::NumericAbsCompare { value, .. } => {
                dtypes.push(value.dtype().as_str().to_string());
            }
            Self::Logical { left, right, .. } => {
                left.push_numeric_abs_rhs_dtypes(dtypes);
                right.push_numeric_abs_rhs_dtypes(dtypes);
            }
            Self::Not { inner } => inner.push_numeric_abs_rhs_dtypes(dtypes),
            Self::All
            | Self::Compare { .. }
            | Self::CastCompare { .. }
            | Self::NumericArithmeticCompare { .. }
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
            | Self::InSubquery { .. }
            | Self::StringMatch { .. } => {}
        }
    }

    fn uses_numeric_rounding(&self) -> bool {
        match self {
            Self::NumericRoundingCompare { .. } => true,
            Self::Logical { left, right, .. } => {
                left.uses_numeric_rounding() || right.uses_numeric_rounding()
            }
            Self::Not { inner } => inner.uses_numeric_rounding(),
            Self::All
            | Self::Compare { .. }
            | Self::CastCompare { .. }
            | Self::NumericArithmeticCompare { .. }
            | Self::NumericAbsCompare { .. }
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
            | Self::InSubquery { .. }
            | Self::StringMatch { .. } => false,
        }
    }

    fn numeric_rounding_operator(&self) -> String {
        let mut operators = Vec::new();
        self.push_numeric_rounding_operators(&mut operators);
        if operators.is_empty() {
            "not_applicable".to_string()
        } else {
            operators.join(",")
        }
    }

    fn push_numeric_rounding_operators(&self, operators: &mut Vec<&'static str>) {
        match self {
            Self::NumericRoundingCompare { op, .. } => operators.push(op.as_str()),
            Self::Logical { left, right, .. } => {
                left.push_numeric_rounding_operators(operators);
                right.push_numeric_rounding_operators(operators);
            }
            Self::Not { inner } => inner.push_numeric_rounding_operators(operators),
            Self::All
            | Self::Compare { .. }
            | Self::CastCompare { .. }
            | Self::NumericArithmeticCompare { .. }
            | Self::NumericAbsCompare { .. }
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
            | Self::InSubquery { .. }
            | Self::StringMatch { .. } => {}
        }
    }

    fn numeric_rounding_source_columns(&self) -> String {
        let mut columns = Vec::new();
        self.push_numeric_rounding_source_columns(&mut columns);
        if columns.is_empty() {
            "not_applicable".to_string()
        } else {
            columns.join(",")
        }
    }

    fn push_numeric_rounding_source_columns<'a>(&'a self, columns: &mut Vec<&'a str>) {
        match self {
            Self::NumericRoundingCompare { column, .. } => columns.push(column),
            Self::Logical { left, right, .. } => {
                left.push_numeric_rounding_source_columns(columns);
                right.push_numeric_rounding_source_columns(columns);
            }
            Self::Not { inner } => inner.push_numeric_rounding_source_columns(columns),
            Self::All
            | Self::Compare { .. }
            | Self::CastCompare { .. }
            | Self::NumericArithmeticCompare { .. }
            | Self::NumericAbsCompare { .. }
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
            | Self::InSubquery { .. }
            | Self::StringMatch { .. } => {}
        }
    }

    fn numeric_rounding_rhs_dtypes(&self) -> String {
        let mut dtypes = Vec::new();
        self.push_numeric_rounding_rhs_dtypes(&mut dtypes);
        if dtypes.is_empty() {
            "not_applicable".to_string()
        } else {
            dtypes.join(",")
        }
    }

    fn push_numeric_rounding_rhs_dtypes(&self, dtypes: &mut Vec<String>) {
        match self {
            Self::NumericRoundingCompare { value, .. } => {
                dtypes.push(value.dtype().as_str().to_string());
            }
            Self::Logical { left, right, .. } => {
                left.push_numeric_rounding_rhs_dtypes(dtypes);
                right.push_numeric_rounding_rhs_dtypes(dtypes);
            }
            Self::Not { inner } => inner.push_numeric_rounding_rhs_dtypes(dtypes),
            Self::All
            | Self::Compare { .. }
            | Self::CastCompare { .. }
            | Self::NumericArithmeticCompare { .. }
            | Self::NumericAbsCompare { .. }
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
            | Self::InSubquery { .. }
            | Self::StringMatch { .. } => {}
        }
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
            | Self::InSubquery { .. }
            | Self::StringMatch { .. } => false,
        }
    }

    fn generic_expression_source_columns(&self) -> String {
        let mut groups = Vec::new();
        self.push_generic_expression_source_columns(&mut groups);
        if groups.is_empty() {
            "not_applicable".to_string()
        } else {
            groups.join(",")
        }
    }

    fn push_generic_expression_source_columns(&self, groups: &mut Vec<String>) {
        match self {
            Self::GenericExpressionCompare { source_columns, .. } => {
                groups.push(source_columns.join("+"));
            }
            Self::Logical { left, right, .. } => {
                left.push_generic_expression_source_columns(groups);
                right.push_generic_expression_source_columns(groups);
            }
            Self::Not { inner } => inner.push_generic_expression_source_columns(groups),
            Self::All
            | Self::Compare { .. }
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
            | Self::InSubquery { .. }
            | Self::StringMatch { .. } => {}
        }
    }

    fn generic_expression_operator_families(&self) -> String {
        let mut groups = Vec::new();
        self.push_generic_expression_operator_families(&mut groups);
        if groups.is_empty() {
            "not_applicable".to_string()
        } else {
            groups.join(",")
        }
    }

    fn push_generic_expression_operator_families(&self, groups: &mut Vec<String>) {
        match self {
            Self::GenericExpressionCompare {
                operator_families, ..
            } => {
                groups.push(operator_families.join("+"));
            }
            Self::Logical { left, right, .. } => {
                left.push_generic_expression_operator_families(groups);
                right.push_generic_expression_operator_families(groups);
            }
            Self::Not { inner } => inner.push_generic_expression_operator_families(groups),
            Self::All
            | Self::Compare { .. }
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
            | Self::InSubquery { .. }
            | Self::StringMatch { .. } => {}
        }
    }

    fn generic_expression_binary_operator_count(&self) -> usize {
        match self {
            Self::GenericExpressionCompare {
                binary_operator_count,
                ..
            } => *binary_operator_count,
            Self::Logical { left, right, .. } => {
                left.generic_expression_binary_operator_count()
                    + right.generic_expression_binary_operator_count()
            }
            Self::Not { inner } => inner.generic_expression_binary_operator_count(),
            Self::All
            | Self::Compare { .. }
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
            | Self::InSubquery { .. }
            | Self::StringMatch { .. } => 0,
        }
    }

    fn generic_expression_comparison_operator(&self) -> String {
        let mut operators = Vec::new();
        self.push_generic_expression_comparison_operators(&mut operators);
        if operators.is_empty() {
            "not_applicable".to_string()
        } else {
            operators.join(",")
        }
    }

    fn push_generic_expression_comparison_operators(&self, operators: &mut Vec<&'static str>) {
        match self {
            Self::GenericExpressionCompare { comparison, .. } => {
                operators.push(comparison_op_label(*comparison));
            }
            Self::Logical { left, right, .. } => {
                left.push_generic_expression_comparison_operators(operators);
                right.push_generic_expression_comparison_operators(operators);
            }
            Self::Not { inner } => inner.push_generic_expression_comparison_operators(operators),
            Self::All
            | Self::Compare { .. }
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
            | Self::InSubquery { .. }
            | Self::StringMatch { .. } => {}
        }
    }

    fn uses_date_literal(&self) -> bool {
        match self {
            Self::Compare {
                value: ScalarValue::Date32(_),
                ..
            }
            | Self::CastCompare {
                value: ScalarValue::Date32(_),
                ..
            }
            | Self::DateArithmeticCompare {
                value: ScalarValue::Date32(_),
                ..
            } => true,
            Self::InList { values, .. } => values
                .iter()
                .any(|value| matches!(value, ScalarValue::Date32(_))),
            Self::InSubquery { subquery, .. } => subquery
                .values
                .iter()
                .any(|value| matches!(value, ScalarValue::Date32(_))),
            Self::Logical { left, right, .. } => {
                left.uses_date_literal() || right.uses_date_literal()
            }
            Self::Not { inner } => inner.uses_date_literal(),
            Self::All
            | Self::Compare { .. }
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
            | Self::StringMatch { .. } => false,
        }
    }

    fn uses_cast(&self) -> bool {
        match self {
            Self::CastCompare { .. } => true,
            Self::Logical { left, right, .. } => left.uses_cast() || right.uses_cast(),
            Self::Not { inner } => inner.uses_cast(),
            Self::All
            | Self::Compare { .. }
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
            | Self::InSubquery { .. }
            | Self::StringMatch { .. } => false,
        }
    }

    fn cast_source_columns(&self) -> String {
        let mut columns = Vec::new();
        self.push_cast_source_columns(&mut columns);
        columns.join(",")
    }

    fn push_cast_source_columns<'a>(&'a self, columns: &mut Vec<&'a str>) {
        match self {
            Self::CastCompare { column, .. } => columns.push(column),
            Self::Logical { left, right, .. } => {
                left.push_cast_source_columns(columns);
                right.push_cast_source_columns(columns);
            }
            Self::Not { inner } => inner.push_cast_source_columns(columns),
            Self::All
            | Self::Compare { .. }
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
            | Self::InSubquery { .. }
            | Self::StringMatch { .. } => {}
        }
    }

    fn cast_target_dtypes(&self) -> String {
        let mut dtypes = Vec::new();
        self.push_cast_target_dtypes(&mut dtypes);
        if dtypes.is_empty() {
            "not_applicable".to_string()
        } else {
            dtypes.join(",")
        }
    }

    fn push_cast_target_dtypes(&self, dtypes: &mut Vec<String>) {
        match self {
            Self::CastCompare { target_dtype, .. } => {
                dtypes.push(target_dtype.as_str().to_string());
            }
            Self::Logical { left, right, .. } => {
                left.push_cast_target_dtypes(dtypes);
                right.push_cast_target_dtypes(dtypes);
            }
            Self::Not { inner } => inner.push_cast_target_dtypes(dtypes),
            Self::All
            | Self::Compare { .. }
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
            | Self::InSubquery { .. }
            | Self::StringMatch { .. } => {}
        }
    }

    fn cast_modes(&self) -> String {
        let mut modes = Vec::new();
        self.push_cast_modes(&mut modes);
        if modes.is_empty() {
            "not_applicable".to_string()
        } else {
            modes.join(",")
        }
    }

    fn push_cast_modes(&self, modes: &mut Vec<&'static str>) {
        match self {
            Self::CastCompare { mode, .. } => modes.push(mode.as_str()),
            Self::Logical { left, right, .. } => {
                left.push_cast_modes(modes);
                right.push_cast_modes(modes);
            }
            Self::Not { inner } => inner.push_cast_modes(modes),
            Self::All
            | Self::Compare { .. }
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
            | Self::InSubquery { .. }
            | Self::StringMatch { .. } => {}
        }
    }

    fn uses_logical_predicate(&self) -> bool {
        matches!(self, Self::Logical { .. } | Self::Not { .. })
    }

    fn logical_operator(&self) -> &'static str {
        match self {
            Self::Logical { op, .. } => op.as_str(),
            Self::Not { .. } => "not",
            Self::All
            | Self::Compare { .. }
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
            | Self::InSubquery { .. }
            | Self::StringMatch { .. } => "not_applicable",
        }
    }

    fn logical_leaf_count(&self) -> usize {
        match self {
            Self::Logical { left, right, .. } => {
                left.logical_leaf_count() + right.logical_leaf_count()
            }
            Self::Not { inner } => inner.logical_leaf_count(),
            Self::All => 0,
            Self::Compare { .. }
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
            | Self::InSubquery { .. }
            | Self::StringMatch { .. } => 1,
        }
    }

    fn uses_in_list(&self) -> bool {
        match self {
            Self::InList { .. } | Self::InSubquery { .. } => true,
            Self::Logical { left, right, .. } => left.uses_in_list() || right.uses_in_list(),
            Self::Not { inner } => inner.uses_in_list(),
            Self::All
            | Self::Compare { .. }
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
            | Self::StringMatch { .. } => false,
        }
    }

    fn in_list_value_count(&self) -> usize {
        match self {
            Self::InList { values, .. } => values.len(),
            Self::InSubquery { subquery, .. } => subquery.values.len(),
            Self::Logical { left, right, .. } => {
                left.in_list_value_count() + right.in_list_value_count()
            }
            Self::Not { inner } => inner.in_list_value_count(),
            Self::All
            | Self::Compare { .. }
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
            | Self::StringMatch { .. } => 0,
        }
    }

    fn in_list_null_value_count(&self) -> usize {
        match self {
            Self::InList { values, .. } => values
                .iter()
                .filter(|value| matches!(value, ScalarValue::Null))
                .count(),
            Self::InSubquery { subquery, .. } => subquery
                .values
                .iter()
                .filter(|value| matches!(value, ScalarValue::Null))
                .count(),
            Self::Logical { left, right, .. } => {
                left.in_list_null_value_count() + right.in_list_null_value_count()
            }
            Self::Not { inner } => inner.in_list_null_value_count(),
            Self::All
            | Self::Compare { .. }
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
            | Self::StringMatch { .. } => 0,
        }
    }

    fn in_subquery_value_count(&self) -> usize {
        match self {
            Self::InSubquery { subquery, .. } => subquery.values.len(),
            Self::Logical { left, right, .. } => {
                left.in_subquery_value_count() + right.in_subquery_value_count()
            }
            Self::Not { inner } => inner.in_subquery_value_count(),
            Self::All
            | Self::Compare { .. }
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
            | Self::StringMatch { .. } => 0,
        }
    }

    fn in_subquery_null_value_count(&self) -> usize {
        match self {
            Self::InSubquery { subquery, .. } => subquery
                .values
                .iter()
                .filter(|value| matches!(value, ScalarValue::Null))
                .count(),
            Self::Logical { left, right, .. } => {
                left.in_subquery_null_value_count() + right.in_subquery_null_value_count()
            }
            Self::Not { inner } => inner.in_subquery_null_value_count(),
            Self::All
            | Self::Compare { .. }
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
            | Self::StringMatch { .. } => 0,
        }
    }

    fn uses_in_subquery(&self) -> bool {
        match self {
            Self::InSubquery { .. } => true,
            Self::Logical { left, right, .. } => {
                left.uses_in_subquery() || right.uses_in_subquery()
            }
            Self::Not { inner } => inner.uses_in_subquery(),
            Self::All
            | Self::Compare { .. }
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
            | Self::StringMatch { .. } => false,
        }
    }

    fn in_subquery_source_columns(&self) -> String {
        let mut columns = Vec::new();
        self.push_in_subquery_source_columns(&mut columns);
        if columns.is_empty() {
            "not_applicable".to_string()
        } else {
            columns.join(",")
        }
    }

    fn push_in_subquery_source_columns<'a>(&'a self, columns: &mut Vec<&'a str>) {
        match self {
            Self::InSubquery { subquery, .. } => columns.push(&subquery.source_column),
            Self::Logical { left, right, .. } => {
                left.push_in_subquery_source_columns(columns);
                right.push_in_subquery_source_columns(columns);
            }
            Self::Not { inner } => inner.push_in_subquery_source_columns(columns),
            Self::All
            | Self::Compare { .. }
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
            | Self::StringMatch { .. } => {}
        }
    }

    fn in_subquery_source_formats(&self) -> String {
        let mut formats = Vec::new();
        self.push_in_subquery_source_formats(&mut formats);
        if formats.is_empty() {
            "not_applicable".to_string()
        } else {
            formats.join(",")
        }
    }

    fn push_in_subquery_source_formats(&self, formats: &mut Vec<&'static str>) {
        match self {
            Self::InSubquery { subquery, .. } => {
                formats.push(
                    subquery
                        .source_format
                        .map_or("not_materialized", LocalSourceFormat::as_str),
                );
            }
            Self::Logical { left, right, .. } => {
                left.push_in_subquery_source_formats(formats);
                right.push_in_subquery_source_formats(formats);
            }
            Self::Not { inner } => inner.push_in_subquery_source_formats(formats),
            Self::All
            | Self::Compare { .. }
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
            | Self::StringMatch { .. } => {}
        }
    }

    fn in_subquery_plan_digest_fragment(&self) -> String {
        let mut fragments = Vec::new();
        self.push_in_subquery_plan_digest_fragments(&mut fragments);
        fragments.join("|")
    }

    fn push_in_subquery_plan_digest_fragments(&self, fragments: &mut Vec<String>) {
        match self {
            Self::InSubquery { subquery, .. } => fragments.push(format!(
                "{}:{}:{}:{}",
                subquery.source_path.display(),
                subquery.source_column,
                subquery
                    .source_digest
                    .as_deref()
                    .unwrap_or("not_materialized"),
                subquery.values.len()
            )),
            Self::Logical { left, right, .. } => {
                left.push_in_subquery_plan_digest_fragments(fragments);
                right.push_in_subquery_plan_digest_fragments(fragments);
            }
            Self::Not { inner } => inner.push_in_subquery_plan_digest_fragments(fragments),
            Self::All
            | Self::Compare { .. }
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
            | Self::StringMatch { .. } => {}
        }
    }

    fn uses_date_extract(&self) -> bool {
        match self {
            Self::DateExtractCompare { .. } => true,
            Self::Logical { left, right, .. } => {
                left.uses_date_extract() || right.uses_date_extract()
            }
            Self::Not { inner } => inner.uses_date_extract(),
            Self::All
            | Self::Compare { .. }
            | Self::CastCompare { .. }
            | Self::NumericArithmeticCompare { .. }
            | Self::NumericAbsCompare { .. }
            | Self::NumericRoundingCompare { .. }
            | Self::GenericExpressionCompare { .. }
            | Self::DateArithmeticCompare { .. }
            | Self::TimestampArithmeticCompare { .. }
            | Self::StringLengthCompare { .. }
            | Self::StringTransformCompare { .. }
            | Self::StringFunctionCompare { .. }
            | Self::TimestampExtractCompare { .. }
            | Self::BooleanPredicate { .. }
            | Self::IsNull { .. }
            | Self::IsNotNull { .. }
            | Self::InList { .. }
            | Self::InSubquery { .. }
            | Self::StringMatch { .. } => false,
        }
    }

    fn date_extract_operator(&self) -> String {
        let mut operators = Vec::new();
        self.push_date_extract_operators(&mut operators);
        if operators.is_empty() {
            "not_applicable".to_string()
        } else {
            operators.join(",")
        }
    }

    fn push_date_extract_operators(&self, operators: &mut Vec<&'static str>) {
        match self {
            Self::DateExtractCompare { op, .. } => operators.push(op.as_str()),
            Self::Logical { left, right, .. } => {
                left.push_date_extract_operators(operators);
                right.push_date_extract_operators(operators);
            }
            Self::Not { inner } => inner.push_date_extract_operators(operators),
            Self::All
            | Self::Compare { .. }
            | Self::CastCompare { .. }
            | Self::NumericArithmeticCompare { .. }
            | Self::NumericAbsCompare { .. }
            | Self::NumericRoundingCompare { .. }
            | Self::GenericExpressionCompare { .. }
            | Self::DateArithmeticCompare { .. }
            | Self::TimestampArithmeticCompare { .. }
            | Self::StringLengthCompare { .. }
            | Self::StringTransformCompare { .. }
            | Self::StringFunctionCompare { .. }
            | Self::TimestampExtractCompare { .. }
            | Self::BooleanPredicate { .. }
            | Self::IsNull { .. }
            | Self::IsNotNull { .. }
            | Self::InList { .. }
            | Self::InSubquery { .. }
            | Self::StringMatch { .. } => {}
        }
    }

    fn date_extract_source_columns(&self) -> String {
        let mut columns = Vec::new();
        self.push_date_extract_source_columns(&mut columns);
        if columns.is_empty() {
            "not_applicable".to_string()
        } else {
            columns.join(",")
        }
    }

    fn push_date_extract_source_columns<'a>(&'a self, columns: &mut Vec<&'a str>) {
        match self {
            Self::DateExtractCompare { column, .. } => columns.push(column),
            Self::Logical { left, right, .. } => {
                left.push_date_extract_source_columns(columns);
                right.push_date_extract_source_columns(columns);
            }
            Self::Not { inner } => inner.push_date_extract_source_columns(columns),
            Self::All
            | Self::Compare { .. }
            | Self::CastCompare { .. }
            | Self::NumericArithmeticCompare { .. }
            | Self::NumericAbsCompare { .. }
            | Self::NumericRoundingCompare { .. }
            | Self::GenericExpressionCompare { .. }
            | Self::DateArithmeticCompare { .. }
            | Self::TimestampArithmeticCompare { .. }
            | Self::StringLengthCompare { .. }
            | Self::StringTransformCompare { .. }
            | Self::StringFunctionCompare { .. }
            | Self::TimestampExtractCompare { .. }
            | Self::BooleanPredicate { .. }
            | Self::IsNull { .. }
            | Self::IsNotNull { .. }
            | Self::InList { .. }
            | Self::InSubquery { .. }
            | Self::StringMatch { .. } => {}
        }
    }

    fn uses_timestamp_literal(&self) -> bool {
        match self {
            Self::Compare {
                value: ScalarValue::TimestampMicros(_),
                ..
            }
            | Self::CastCompare {
                value: ScalarValue::TimestampMicros(_),
                ..
            }
            | Self::TimestampArithmeticCompare {
                value: ScalarValue::TimestampMicros(_),
                ..
            } => true,
            Self::InList { values, .. } => values
                .iter()
                .any(|value| matches!(value, ScalarValue::TimestampMicros(_))),
            Self::InSubquery { subquery, .. } => subquery
                .values
                .iter()
                .any(|value| matches!(value, ScalarValue::TimestampMicros(_))),
            Self::Logical { left, right, .. } => {
                left.uses_timestamp_literal() || right.uses_timestamp_literal()
            }
            Self::Not { inner } => inner.uses_timestamp_literal(),
            Self::TimestampArithmeticCompare { .. }
            | Self::All
            | Self::Compare { .. }
            | Self::CastCompare { .. }
            | Self::NumericArithmeticCompare { .. }
            | Self::NumericAbsCompare { .. }
            | Self::NumericRoundingCompare { .. }
            | Self::GenericExpressionCompare { .. }
            | Self::DateArithmeticCompare { .. }
            | Self::DateExtractCompare { .. }
            | Self::StringLengthCompare { .. }
            | Self::TimestampExtractCompare { .. }
            | Self::StringTransformCompare { .. }
            | Self::StringFunctionCompare { .. }
            | Self::BooleanPredicate { .. }
            | Self::IsNull { .. }
            | Self::IsNotNull { .. }
            | Self::StringMatch { .. } => false,
        }
    }

    fn uses_timestamp_extract(&self) -> bool {
        match self {
            Self::TimestampExtractCompare { .. } => true,
            Self::Logical { left, right, .. } => {
                left.uses_timestamp_extract() || right.uses_timestamp_extract()
            }
            Self::Not { inner } => inner.uses_timestamp_extract(),
            Self::All
            | Self::Compare { .. }
            | Self::CastCompare { .. }
            | Self::NumericArithmeticCompare { .. }
            | Self::NumericAbsCompare { .. }
            | Self::NumericRoundingCompare { .. }
            | Self::GenericExpressionCompare { .. }
            | Self::DateArithmeticCompare { .. }
            | Self::TimestampArithmeticCompare { .. }
            | Self::DateExtractCompare { .. }
            | Self::StringLengthCompare { .. }
            | Self::StringTransformCompare { .. }
            | Self::StringFunctionCompare { .. }
            | Self::BooleanPredicate { .. }
            | Self::IsNull { .. }
            | Self::IsNotNull { .. }
            | Self::InList { .. }
            | Self::InSubquery { .. }
            | Self::StringMatch { .. } => false,
        }
    }

    fn timestamp_extract_operator(&self) -> String {
        let mut operators = Vec::new();
        self.push_timestamp_extract_operators(&mut operators);
        if operators.is_empty() {
            "not_applicable".to_string()
        } else {
            operators.join(",")
        }
    }

    fn push_timestamp_extract_operators(&self, operators: &mut Vec<&'static str>) {
        match self {
            Self::TimestampExtractCompare { op, .. } => operators.push(op.as_str()),
            Self::Logical { left, right, .. } => {
                left.push_timestamp_extract_operators(operators);
                right.push_timestamp_extract_operators(operators);
            }
            Self::Not { inner } => inner.push_timestamp_extract_operators(operators),
            Self::All
            | Self::Compare { .. }
            | Self::CastCompare { .. }
            | Self::NumericArithmeticCompare { .. }
            | Self::NumericAbsCompare { .. }
            | Self::NumericRoundingCompare { .. }
            | Self::GenericExpressionCompare { .. }
            | Self::DateArithmeticCompare { .. }
            | Self::TimestampArithmeticCompare { .. }
            | Self::DateExtractCompare { .. }
            | Self::StringLengthCompare { .. }
            | Self::StringTransformCompare { .. }
            | Self::StringFunctionCompare { .. }
            | Self::BooleanPredicate { .. }
            | Self::IsNull { .. }
            | Self::IsNotNull { .. }
            | Self::InList { .. }
            | Self::InSubquery { .. }
            | Self::StringMatch { .. } => {}
        }
    }

    fn timestamp_extract_source_columns(&self) -> String {
        let mut columns = Vec::new();
        self.push_timestamp_extract_source_columns(&mut columns);
        if columns.is_empty() {
            "not_applicable".to_string()
        } else {
            columns.join(",")
        }
    }

    fn push_timestamp_extract_source_columns<'a>(&'a self, columns: &mut Vec<&'a str>) {
        match self {
            Self::TimestampExtractCompare { column, .. } => columns.push(column),
            Self::Logical { left, right, .. } => {
                left.push_timestamp_extract_source_columns(columns);
                right.push_timestamp_extract_source_columns(columns);
            }
            Self::Not { inner } => inner.push_timestamp_extract_source_columns(columns),
            Self::All
            | Self::Compare { .. }
            | Self::CastCompare { .. }
            | Self::NumericArithmeticCompare { .. }
            | Self::NumericAbsCompare { .. }
            | Self::NumericRoundingCompare { .. }
            | Self::GenericExpressionCompare { .. }
            | Self::DateArithmeticCompare { .. }
            | Self::TimestampArithmeticCompare { .. }
            | Self::DateExtractCompare { .. }
            | Self::StringLengthCompare { .. }
            | Self::StringTransformCompare { .. }
            | Self::StringFunctionCompare { .. }
            | Self::BooleanPredicate { .. }
            | Self::IsNull { .. }
            | Self::IsNotNull { .. }
            | Self::InList { .. }
            | Self::InSubquery { .. }
            | Self::StringMatch { .. } => {}
        }
    }

    fn uses_timestamp_arithmetic(&self) -> bool {
        match self {
            Self::TimestampArithmeticCompare { .. } => true,
            Self::Logical { left, right, .. } => {
                left.uses_timestamp_arithmetic() || right.uses_timestamp_arithmetic()
            }
            Self::Not { inner } => inner.uses_timestamp_arithmetic(),
            Self::All
            | Self::Compare { .. }
            | Self::CastCompare { .. }
            | Self::NumericArithmeticCompare { .. }
            | Self::NumericAbsCompare { .. }
            | Self::NumericRoundingCompare { .. }
            | Self::GenericExpressionCompare { .. }
            | Self::DateArithmeticCompare { .. }
            | Self::DateExtractCompare { .. }
            | Self::StringLengthCompare { .. }
            | Self::TimestampExtractCompare { .. }
            | Self::StringTransformCompare { .. }
            | Self::StringFunctionCompare { .. }
            | Self::BooleanPredicate { .. }
            | Self::IsNull { .. }
            | Self::IsNotNull { .. }
            | Self::InList { .. }
            | Self::InSubquery { .. }
            | Self::StringMatch { .. } => false,
        }
    }

    fn timestamp_arithmetic_operator(&self) -> String {
        let mut operators = Vec::new();
        self.push_timestamp_arithmetic_operators(&mut operators);
        if operators.is_empty() {
            "not_applicable".to_string()
        } else {
            operators.join(",")
        }
    }

    fn push_timestamp_arithmetic_operators(&self, operators: &mut Vec<&'static str>) {
        match self {
            Self::TimestampArithmeticCompare { op, .. } => operators.push(op.as_str()),
            Self::Logical { left, right, .. } => {
                left.push_timestamp_arithmetic_operators(operators);
                right.push_timestamp_arithmetic_operators(operators);
            }
            Self::Not { inner } => inner.push_timestamp_arithmetic_operators(operators),
            Self::All
            | Self::Compare { .. }
            | Self::CastCompare { .. }
            | Self::NumericArithmeticCompare { .. }
            | Self::NumericAbsCompare { .. }
            | Self::NumericRoundingCompare { .. }
            | Self::GenericExpressionCompare { .. }
            | Self::DateArithmeticCompare { .. }
            | Self::DateExtractCompare { .. }
            | Self::StringLengthCompare { .. }
            | Self::TimestampExtractCompare { .. }
            | Self::StringTransformCompare { .. }
            | Self::StringFunctionCompare { .. }
            | Self::BooleanPredicate { .. }
            | Self::IsNull { .. }
            | Self::IsNotNull { .. }
            | Self::InList { .. }
            | Self::InSubquery { .. }
            | Self::StringMatch { .. } => {}
        }
    }

    fn timestamp_arithmetic_seconds(&self) -> String {
        let mut values = Vec::new();
        self.push_timestamp_arithmetic_seconds(&mut values);
        if values.is_empty() {
            "not_applicable".to_string()
        } else {
            values.join(",")
        }
    }

    fn push_timestamp_arithmetic_seconds(&self, values: &mut Vec<String>) {
        match self {
            Self::TimestampArithmeticCompare { second_count, .. } => {
                values.push(second_count.to_string());
            }
            Self::Logical { left, right, .. } => {
                left.push_timestamp_arithmetic_seconds(values);
                right.push_timestamp_arithmetic_seconds(values);
            }
            Self::Not { inner } => inner.push_timestamp_arithmetic_seconds(values),
            Self::All
            | Self::Compare { .. }
            | Self::CastCompare { .. }
            | Self::NumericArithmeticCompare { .. }
            | Self::NumericAbsCompare { .. }
            | Self::NumericRoundingCompare { .. }
            | Self::GenericExpressionCompare { .. }
            | Self::DateArithmeticCompare { .. }
            | Self::DateExtractCompare { .. }
            | Self::StringLengthCompare { .. }
            | Self::TimestampExtractCompare { .. }
            | Self::StringTransformCompare { .. }
            | Self::StringFunctionCompare { .. }
            | Self::BooleanPredicate { .. }
            | Self::IsNull { .. }
            | Self::IsNotNull { .. }
            | Self::InList { .. }
            | Self::InSubquery { .. }
            | Self::StringMatch { .. } => {}
        }
    }

    fn timestamp_arithmetic_source_columns(&self) -> String {
        let mut columns = Vec::new();
        self.push_timestamp_arithmetic_source_columns(&mut columns);
        if columns.is_empty() {
            "not_applicable".to_string()
        } else {
            columns.join(",")
        }
    }

    fn push_timestamp_arithmetic_source_columns<'a>(&'a self, columns: &mut Vec<&'a str>) {
        match self {
            Self::TimestampArithmeticCompare { column, .. } => columns.push(column),
            Self::Logical { left, right, .. } => {
                left.push_timestamp_arithmetic_source_columns(columns);
                right.push_timestamp_arithmetic_source_columns(columns);
            }
            Self::Not { inner } => inner.push_timestamp_arithmetic_source_columns(columns),
            Self::All
            | Self::Compare { .. }
            | Self::CastCompare { .. }
            | Self::NumericArithmeticCompare { .. }
            | Self::NumericAbsCompare { .. }
            | Self::NumericRoundingCompare { .. }
            | Self::GenericExpressionCompare { .. }
            | Self::DateArithmeticCompare { .. }
            | Self::DateExtractCompare { .. }
            | Self::StringLengthCompare { .. }
            | Self::TimestampExtractCompare { .. }
            | Self::StringTransformCompare { .. }
            | Self::StringFunctionCompare { .. }
            | Self::BooleanPredicate { .. }
            | Self::IsNull { .. }
            | Self::IsNotNull { .. }
            | Self::InList { .. }
            | Self::InSubquery { .. }
            | Self::StringMatch { .. } => {}
        }
    }

    fn uses_date_arithmetic(&self) -> bool {
        match self {
            Self::DateArithmeticCompare { .. } => true,
            Self::Logical { left, right, .. } => {
                left.uses_date_arithmetic() || right.uses_date_arithmetic()
            }
            Self::Not { inner } => inner.uses_date_arithmetic(),
            Self::All
            | Self::Compare { .. }
            | Self::CastCompare { .. }
            | Self::NumericArithmeticCompare { .. }
            | Self::NumericAbsCompare { .. }
            | Self::NumericRoundingCompare { .. }
            | Self::GenericExpressionCompare { .. }
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
            | Self::InSubquery { .. }
            | Self::StringMatch { .. } => false,
        }
    }

    fn date_arithmetic_operator(&self) -> String {
        let mut operators = Vec::new();
        self.push_date_arithmetic_operators(&mut operators);
        if operators.is_empty() {
            "not_applicable".to_string()
        } else {
            operators.join(",")
        }
    }

    fn push_date_arithmetic_operators(&self, operators: &mut Vec<&'static str>) {
        match self {
            Self::DateArithmeticCompare { op, .. } => operators.push(op.as_str()),
            Self::Logical { left, right, .. } => {
                left.push_date_arithmetic_operators(operators);
                right.push_date_arithmetic_operators(operators);
            }
            Self::Not { inner } => inner.push_date_arithmetic_operators(operators),
            Self::All
            | Self::Compare { .. }
            | Self::CastCompare { .. }
            | Self::NumericArithmeticCompare { .. }
            | Self::NumericAbsCompare { .. }
            | Self::NumericRoundingCompare { .. }
            | Self::GenericExpressionCompare { .. }
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
            | Self::InSubquery { .. }
            | Self::StringMatch { .. } => {}
        }
    }

    fn date_arithmetic_days(&self) -> String {
        let mut values = Vec::new();
        self.push_date_arithmetic_days(&mut values);
        if values.is_empty() {
            "not_applicable".to_string()
        } else {
            values.join(",")
        }
    }

    fn push_date_arithmetic_days(&self, values: &mut Vec<String>) {
        match self {
            Self::DateArithmeticCompare { day_count, .. } => values.push(day_count.to_string()),
            Self::Logical { left, right, .. } => {
                left.push_date_arithmetic_days(values);
                right.push_date_arithmetic_days(values);
            }
            Self::Not { inner } => inner.push_date_arithmetic_days(values),
            Self::All
            | Self::Compare { .. }
            | Self::CastCompare { .. }
            | Self::NumericArithmeticCompare { .. }
            | Self::NumericAbsCompare { .. }
            | Self::NumericRoundingCompare { .. }
            | Self::GenericExpressionCompare { .. }
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
            | Self::InSubquery { .. }
            | Self::StringMatch { .. } => {}
        }
    }

    fn date_arithmetic_source_columns(&self) -> String {
        let mut columns = Vec::new();
        self.push_date_arithmetic_source_columns(&mut columns);
        if columns.is_empty() {
            "not_applicable".to_string()
        } else {
            columns.join(",")
        }
    }

    fn push_date_arithmetic_source_columns<'a>(&'a self, columns: &mut Vec<&'a str>) {
        match self {
            Self::DateArithmeticCompare { column, .. } => columns.push(column),
            Self::Logical { left, right, .. } => {
                left.push_date_arithmetic_source_columns(columns);
                right.push_date_arithmetic_source_columns(columns);
            }
            Self::Not { inner } => inner.push_date_arithmetic_source_columns(columns),
            Self::All
            | Self::Compare { .. }
            | Self::CastCompare { .. }
            | Self::NumericArithmeticCompare { .. }
            | Self::NumericAbsCompare { .. }
            | Self::NumericRoundingCompare { .. }
            | Self::GenericExpressionCompare { .. }
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
            | Self::InSubquery { .. }
            | Self::StringMatch { .. } => {}
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

fn cast_compare_expression(
    column: &str,
    target_dtype: &LogicalDType,
    mode: CastMode,
    op: ComparisonOp,
    value: &ScalarValue,
) -> Result<Expression, ShardLoomError> {
    Ok(Expression::new(
        ExprId::new("where.cast_compare")?,
        ExpressionKind::Compare {
            left: Box::new(mode.build_expression(
                ExprId::new(format!("where.cast.{column}"))?,
                Expression::column(
                    ExprId::new(format!("where.{column}"))?,
                    ColumnRef::new(column.to_string())?,
                ),
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
    column: &str,
    op: StringTransformOp,
    comparison: ComparisonOp,
    value: &ScalarValue,
) -> Result<Expression, ShardLoomError> {
    Ok(Expression::new(
        ExprId::new("where.string_transform_compare")?,
        ExpressionKind::Compare {
            left: Box::new(Expression::new(
                ExprId::new(format!("where.string_transform.{column}"))?,
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
                ExprId::new("where.string_transform.literal")?,
                value.clone(),
            )),
        },
    ))
}

fn string_length_compare_expression(
    column: &str,
    comparison: ComparisonOp,
    value: &ScalarValue,
) -> Result<Expression, ShardLoomError> {
    Ok(Expression::new(
        ExprId::new("where.string_length_compare")?,
        ExpressionKind::Compare {
            left: Box::new(Expression::new(
                ExprId::new(format!("where.string_length.{column}"))?,
                ExpressionKind::FunctionCall {
                    name: "length".to_string(),
                    args: vec![Expression::column(
                        ExprId::new(format!("where.{column}"))?,
                        ColumnRef::new(column.to_string())?,
                    )],
                },
            )),
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

#[allow(clippy::too_many_lines)]
impl SqlLocalSourceReport {
    fn fields(&self) -> Vec<(String, String)> {
        let mut fields = vec![
            ("schema_version".to_string(), SCHEMA_VERSION.to_string()),
            (
                "execution_mode".to_string(),
                "direct_compatibility_transient".to_string(),
            ),
            ("engine_mode".to_string(), "batch".to_string()),
            ("runtime_execution".to_string(), "true".to_string()),
            (
                "support_status".to_string(),
                "fixture_smoke_supported".to_string(),
            ),
            (
                "sql_statement_kind".to_string(),
                self.parsed.statement_kind().to_string(),
            ),
            ("sql_statement".to_string(), self.request.statement.clone()),
            ("sql_parser_executed".to_string(), "true".to_string()),
            ("sql_binder_executed".to_string(), "true".to_string()),
            ("sql_planner_executed".to_string(), "true".to_string()),
            ("sql_runtime_execution".to_string(), "true".to_string()),
            (
                "user_surface_runtime_scope".to_string(),
                "format_neutral_sql_python_runtime".to_string(),
            ),
            (
                "format_specific_boundary_scope".to_string(),
                "read_ingest_and_write_only".to_string(),
            ),
            (
                "format_specific_compute_path".to_string(),
                "false".to_string(),
            ),
            ("source_io_performed".to_string(), "true".to_string()),
            (
                "source_format".to_string(),
                self.source.source_format.as_str().to_string(),
            ),
            (
                "source_kind".to_string(),
                "local_non_vortex_file".to_string(),
            ),
            (
                "source_adapter_id".to_string(),
                self.source_adapter_id().to_string(),
            ),
            (
                "source_adapter_status".to_string(),
                "smoke_supported".to_string(),
            ),
            (
                "source_adapter_blocker_id".to_string(),
                "none_scoped_sql_local_source_smoke_only".to_string(),
            ),
            ("ingress_route".to_string(), "direct_transient".to_string()),
            (
                "ingress_route_label".to_string(),
                "Direct one-shot route".to_string(),
            ),
            ("ingress_status".to_string(), "smoke_supported".to_string()),
            (
                "ingress_certification_level".to_string(),
                "fixture_smoke".to_string(),
            ),
            ("vortex_ingest_performed".to_string(), "false".to_string()),
            (
                "vortex_ingest_status".to_string(),
                "not_performed_direct_transient".to_string(),
            ),
            (
                "vortex_ingest_blocker_id".to_string(),
                "not_applicable_direct_transient".to_string(),
            ),
            ("prepared_state_id".to_string(), String::new()),
            ("prepared_state_digest".to_string(), String::new()),
            ("prepared_state_created".to_string(), "false".to_string()),
            ("prepared_state_reused".to_string(), "false".to_string()),
            ("prepared_state_reuse_hit".to_string(), "false".to_string()),
            (
                "selected_execution_mode".to_string(),
                "direct_compatibility_transient".to_string(),
            ),
            (
                "execution_route_label".to_string(),
                "Direct one-shot route".to_string(),
            ),
            ("timing_scope".to_string(), "direct_one_shot".to_string()),
            (
                "certification_policy".to_string(),
                "scoped_compatibility_source_execution".to_string(),
            ),
            (
                "certification_status".to_string(),
                "fixture_smoke_certified".to_string(),
            ),
            (
                "certification_blocker_id".to_string(),
                "not_claim_grade_fixture_smoke".to_string(),
            ),
            (
                "source_path".to_string(),
                self.parsed.source_path.display().to_string(),
            ),
            (
                "source_alias".to_string(),
                self.parsed.source_alias.clone().unwrap_or_default(),
            ),
            (
                "source_bytes".to_string(),
                self.source.source_bytes.to_string(),
            ),
            (
                "source_digest".to_string(),
                self.source.source_digest.clone(),
            ),
            (
                "source_fingerprint_kind".to_string(),
                "local_file_content_digest".to_string(),
            ),
            (
                "source_state_id".to_string(),
                Self::source_state_id(&self.source),
            ),
            (
                "source_state_digest".to_string(),
                self.source_state_digest(&self.source),
            ),
            (
                "source_state_contract_schema_version".to_string(),
                LOCAL_SOURCE_STATE_SCHEMA_VERSION.to_string(),
            ),
            (
                "local_input_adapter_registry_version".to_string(),
                LOCAL_INPUT_ADAPTER_REGISTRY_VERSION.to_string(),
            ),
            (
                "source_state_read_plan".to_string(),
                self.source.read_plan.status().to_string(),
            ),
            (
                "source_state_read_plan_reason".to_string(),
                self.source.read_plan.reason.to_string(),
            ),
            (
                "source_state_requested_columns".to_string(),
                self.source.read_plan.requested_columns(),
            ),
            (
                "source_state_projection_pushdown_status".to_string(),
                self.source.projection_pushdown_status.as_str().to_string(),
            ),
            (
                "source_state_materialization_layout".to_string(),
                self.source.materialization_layout().to_string(),
            ),
            (
                "source_state_parse_normalization".to_string(),
                self.source.parse_normalization().to_string(),
            ),
            (
                "source_state_columnar_preserved".to_string(),
                self.source.columnar_source_preserved().to_string(),
            ),
            (
                "source_state_record_batch_count".to_string(),
                self.source.record_batch_count.to_string(),
            ),
            (
                "source_to_columnar_millis".to_string(),
                self.source.source_to_columnar_millis.to_string(),
            ),
            (
                "source_state_runtime_consumption_layout".to_string(),
                "scalar_row_map_expression_runtime".to_string(),
            ),
            (
                "source_state_scalar_runtime_materialization_required".to_string(),
                "true".to_string(),
            ),
            (
                "source_state_materialized_column_count".to_string(),
                self.source.materialized_columns.len().to_string(),
            ),
            (
                "source_state_materialized_columns".to_string(),
                self.source.materialized_columns_field(),
            ),
            (
                "source_state_reader_projection_column_count".to_string(),
                self.source.reader_projection_columns.len().to_string(),
            ),
            (
                "source_state_reader_projection_columns".to_string(),
                self.source.reader_projection_columns_field(),
            ),
            (
                "source_state_pruned_column_count".to_string(),
                self.source.pruned_column_count().to_string(),
            ),
            (
                "source_state_column_pruning_applied".to_string(),
                self.source.column_pruning_applied().to_string(),
            ),
            (
                "source_state_reuse_allowed".to_string(),
                "false".to_string(),
            ),
            ("source_state_reuse_hit".to_string(), "false".to_string()),
            (
                "source_state_reuse_reason".to_string(),
                "not_cached_sql_local_source_smoke".to_string(),
            ),
            (
                "source_schema_digest".to_string(),
                self.source_schema_digest.clone(),
            ),
            (
                "source_column_count".to_string(),
                self.source.header.len().to_string(),
            ),
            ("source_columns".to_string(), self.source.header.join(",")),
            (
                "input_row_count".to_string(),
                self.source.rows.len().to_string(),
            ),
            (
                "left_input_row_count".to_string(),
                self.source.rows.len().to_string(),
            ),
            (
                "right_source_path".to_string(),
                self.parsed.join.as_ref().map_or_else(String::new, |join| {
                    join.right_source_path.display().to_string()
                }),
            ),
            (
                "right_source_alias".to_string(),
                self.parsed
                    .join
                    .as_ref()
                    .map_or_else(String::new, |join| join.right_alias.clone()),
            ),
            (
                "right_source_format".to_string(),
                self.right_source
                    .as_ref()
                    .map_or_else(String::new, |source| {
                        source.source_format.as_str().to_string()
                    }),
            ),
            (
                "join_source_formats".to_string(),
                self.right_source
                    .as_ref()
                    .map_or_else(String::new, |source| {
                        format!(
                            "{},{}",
                            self.source.source_format.as_str(),
                            source.source_format.as_str()
                        )
                    }),
            ),
            (
                "right_input_row_count".to_string(),
                self.right_source
                    .as_ref()
                    .map_or(0, |source| source.rows.len())
                    .to_string(),
            ),
            (
                "right_source_digest".to_string(),
                self.right_source
                    .as_ref()
                    .map_or_else(String::new, |source| source.source_digest.clone()),
            ),
            (
                "right_source_columns".to_string(),
                self.right_source
                    .as_ref()
                    .map_or_else(String::new, |source| source.header.join(",")),
            ),
            (
                "join_runtime_execution".to_string(),
                self.parsed.is_join().to_string(),
            ),
            (
                "join_type".to_string(),
                if self.parsed.is_join() {
                    "inner_equi"
                } else {
                    "not_applicable"
                }
                .to_string(),
            ),
            (
                "join_left_key".to_string(),
                self.parsed
                    .join
                    .as_ref()
                    .map_or_else(String::new, ParsedJoin::left_key_refs),
            ),
            (
                "join_right_key".to_string(),
                self.parsed
                    .join
                    .as_ref()
                    .map_or_else(String::new, ParsedJoin::right_key_refs),
            ),
            (
                "join_left_keys".to_string(),
                self.parsed
                    .join
                    .as_ref()
                    .map_or_else(String::new, ParsedJoin::left_key_refs),
            ),
            (
                "join_right_keys".to_string(),
                self.parsed
                    .join
                    .as_ref()
                    .map_or_else(String::new, ParsedJoin::right_key_refs),
            ),
            (
                "join_key_arity".to_string(),
                self.parsed
                    .join
                    .as_ref()
                    .map_or(0, ParsedJoin::key_arity)
                    .to_string(),
            ),
            (
                "join_multi_key_runtime_execution".to_string(),
                self.parsed
                    .join
                    .as_ref()
                    .is_some_and(ParsedJoin::is_multi_key)
                    .to_string(),
            ),
            (
                "join_matched_row_count".to_string(),
                self.joined_row_count.to_string(),
            ),
            (
                "join_left_rows_scanned".to_string(),
                if self.parsed.is_join() {
                    self.source.rows.len().to_string()
                } else {
                    "0".to_string()
                },
            ),
            (
                "join_right_rows_scanned".to_string(),
                self.right_source
                    .as_ref()
                    .map_or(0, |source| source.rows.len())
                    .to_string(),
            ),
            (
                "join_rows_output".to_string(),
                if self.parsed.is_join() {
                    self.output_rows.len().to_string()
                } else {
                    "0".to_string()
                },
            ),
            (
                "join_memory_estimate_bytes".to_string(),
                self.right_source
                    .as_ref()
                    .map_or(0, |right_source| {
                        join_memory_estimate_bytes(&self.source, right_source)
                    })
                    .to_string(),
            ),
            (
                "join_computed_projection_runtime_execution".to_string(),
                (self.parsed.is_join() && self.parsed.has_computed_projection()).to_string(),
            ),
            (
                "join_order_by_top_n_runtime_execution".to_string(),
                (self.parsed.is_join() && self.parsed.order_by.is_some()).to_string(),
            ),
            (
                "join_projection_operator_family".to_string(),
                if self.parsed.is_join()
                    && self.parsed.has_computed_projection()
                    && self.parsed.order_by.is_some()
                {
                    "computed_projection_topn"
                } else if self.parsed.is_join() && self.parsed.has_computed_projection() {
                    "computed_projection"
                } else if self.parsed.is_join() && self.parsed.order_by.is_some() {
                    "raw_projection_topn"
                } else if self.parsed.is_join() {
                    "raw_projection"
                } else {
                    "not_applicable"
                }
                .to_string(),
            ),
            (
                "join_aggregate_runtime_execution".to_string(),
                (self.parsed.is_join() && self.parsed.is_aggregate()).to_string(),
            ),
            (
                "join_aggregate_operator_family".to_string(),
                if self.parsed.is_join() && self.parsed.is_grouped_aggregate() {
                    "grouped_join_aggregate"
                } else if self.parsed.is_join() && self.parsed.is_aggregate() {
                    "scalar_join_aggregate"
                } else {
                    "not_applicable"
                }
                .to_string(),
            ),
            (
                "join_aggregate_group_count".to_string(),
                if self.parsed.is_join() && self.parsed.is_grouped_aggregate() {
                    self.output_rows.len().to_string()
                } else {
                    "0".to_string()
                },
            ),
            (
                "selected_row_count".to_string(),
                self.selected_row_count.to_string(),
            ),
            ("limit".to_string(), self.parsed.limit.to_string()),
            (
                "output_row_count".to_string(),
                self.output_rows.len().to_string(),
            ),
            (
                "projected_columns".to_string(),
                self.parsed.output_columns(&self.source.header).join(","),
            ),
            (
                "aggregate_runtime_execution".to_string(),
                self.parsed.is_aggregate().to_string(),
            ),
            (
                "aggregate_operator_family".to_string(),
                if self.parsed.is_grouped_aggregate() {
                    "grouped_aggregate"
                } else if self.parsed.is_aggregate() {
                    "scalar_aggregate"
                } else {
                    "not_applicable"
                }
                .to_string(),
            ),
            (
                "aggregate_functions".to_string(),
                self.parsed
                    .aggregates
                    .iter()
                    .map(ParsedAggregate::label)
                    .collect::<Vec<_>>()
                    .join(","),
            ),
            (
                "aggregate_output_columns".to_string(),
                self.parsed.aggregate_output_columns(),
            ),
            (
                "aggregate_alias_runtime_execution".to_string(),
                (self.parsed.is_aggregate() && self.parsed.has_aggregate_aliases()).to_string(),
            ),
            (
                "aggregate_aliases".to_string(),
                self.parsed.aggregate_aliases(),
            ),
            (
                "distinct_aggregate_runtime_execution".to_string(),
                (self.parsed.is_aggregate() && self.parsed.has_distinct_aggregate()).to_string(),
            ),
            (
                "distinct_aggregate_function".to_string(),
                self.parsed.distinct_aggregate_functions(),
            ),
            (
                "distinct_aggregate_column".to_string(),
                self.parsed.distinct_aggregate_columns(),
            ),
            (
                "distinct_aggregate_null_semantics".to_string(),
                self.parsed.distinct_aggregate_null_semantics(),
            ),
            (
                "group_by_runtime_execution".to_string(),
                self.parsed.is_grouped_aggregate().to_string(),
            ),
            (
                "group_by_columns".to_string(),
                self.parsed.group_by.join(","),
            ),
            (
                "group_by_key_arity".to_string(),
                if self.parsed.is_grouped_aggregate() {
                    self.parsed.group_by.len().to_string()
                } else {
                    "0".to_string()
                },
            ),
            (
                "group_by_multi_key_runtime_execution".to_string(),
                (self.parsed.is_grouped_aggregate() && self.parsed.group_by.len() > 1).to_string(),
            ),
            (
                "group_by_group_count".to_string(),
                if self.parsed.is_grouped_aggregate() {
                    self.output_rows.len().to_string()
                } else {
                    "0".to_string()
                },
            ),
            (
                "order_by_runtime_execution".to_string(),
                self.parsed.order_by.is_some().to_string(),
            ),
            (
                "top_n_runtime_execution".to_string(),
                self.parsed.order_by.is_some().to_string(),
            ),
            (
                "sort_operator_family".to_string(),
                self.parsed.order_by.as_ref().map_or_else(
                    || "not_applicable".to_string(),
                    |order_by| order_by.operator_family_label().to_string(),
                ),
            ),
            (
                "sort_keys".to_string(),
                self.parsed
                    .order_by
                    .as_ref()
                    .map_or_else(String::new, ParsedOrderBy::columns_label),
            ),
            (
                "sort_direction".to_string(),
                self.parsed
                    .order_by
                    .as_ref()
                    .map_or_else(String::new, ParsedOrderBy::directions_label),
            ),
            (
                "sort_null_ordering".to_string(),
                if self.parsed.order_by.is_some() {
                    "nulls_blocked_for_fixture_smoke"
                } else {
                    "not_applicable"
                }
                .to_string(),
            ),
            (
                "top_n_limit".to_string(),
                if self.parsed.order_by.is_some() {
                    self.parsed.limit.to_string()
                } else {
                    "0".to_string()
                },
            ),
            (
                "predicate_operator_family".to_string(),
                self.parsed.predicate.family().to_string(),
            ),
            (
                "filter_runtime_execution".to_string(),
                self.parsed.has_filter().to_string(),
            ),
            (
                "null_predicate_runtime_execution".to_string(),
                self.parsed.predicate.uses_null_predicate().to_string(),
            ),
            (
                "null_predicate_operator".to_string(),
                self.parsed.predicate.null_predicate_operator(),
            ),
            (
                "null_predicate_source_column".to_string(),
                self.parsed.predicate.null_predicate_source_columns(),
            ),
            (
                "null_predicate_null_semantics".to_string(),
                if self.parsed.predicate.uses_null_predicate() {
                    "sql_is_null_is_not_null"
                } else {
                    "not_applicable"
                }
                .to_string(),
            ),
            (
                "boolean_predicate_runtime_execution".to_string(),
                self.parsed.predicate.uses_boolean_predicate().to_string(),
            ),
            (
                "boolean_predicate_operator".to_string(),
                self.parsed.predicate.boolean_predicate_operator(),
            ),
            (
                "boolean_predicate_source_column".to_string(),
                self.parsed.predicate.boolean_predicate_source_columns(),
            ),
            (
                "boolean_predicate_null_semantics".to_string(),
                self.parsed
                    .predicate
                    .boolean_predicate_null_semantics()
                    .to_string(),
            ),
            (
                "string_predicate_runtime_execution".to_string(),
                self.parsed.predicate.uses_string_predicate().to_string(),
            ),
            (
                "string_predicate_operator".to_string(),
                self.parsed.predicate.string_operator(),
            ),
            (
                "string_transform_runtime_execution".to_string(),
                self.parsed.predicate.uses_string_transform().to_string(),
            ),
            (
                "string_transform_operator".to_string(),
                self.parsed.predicate.string_transform_operator(),
            ),
            (
                "string_transform_source_column".to_string(),
                self.parsed.predicate.string_transform_source_columns(),
            ),
            (
                "string_length_runtime_execution".to_string(),
                self.parsed.predicate.uses_string_length().to_string(),
            ),
            (
                "string_length_source_column".to_string(),
                self.parsed.predicate.string_length_source_columns(),
            ),
            (
                "string_length_rhs_dtype".to_string(),
                self.parsed.predicate.string_length_rhs_dtypes(),
            ),
            (
                "string_function_runtime_execution".to_string(),
                self.parsed.predicate.uses_string_function().to_string(),
            ),
            (
                "string_function_operator".to_string(),
                self.parsed.predicate.string_function_operator(),
            ),
            (
                "string_function_source_column".to_string(),
                self.parsed.predicate.string_function_source_columns(),
            ),
            (
                "string_function_literal_count".to_string(),
                self.parsed.predicate.string_function_literal_counts(),
            ),
            (
                "string_function_rhs_dtype".to_string(),
                self.parsed.predicate.string_function_rhs_dtypes(),
            ),
            (
                "numeric_arithmetic_runtime_execution".to_string(),
                self.parsed.predicate.uses_numeric_arithmetic().to_string(),
            ),
            (
                "numeric_arithmetic_operator".to_string(),
                self.parsed.predicate.numeric_arithmetic_operator(),
            ),
            (
                "numeric_arithmetic_source_column".to_string(),
                self.parsed.predicate.numeric_arithmetic_source_columns(),
            ),
            (
                "numeric_arithmetic_rhs_dtype".to_string(),
                self.parsed.predicate.numeric_arithmetic_rhs_dtypes(),
            ),
            (
                "numeric_abs_runtime_execution".to_string(),
                self.parsed.predicate.uses_numeric_abs().to_string(),
            ),
            (
                "numeric_abs_source_column".to_string(),
                self.parsed.predicate.numeric_abs_source_columns(),
            ),
            (
                "numeric_abs_rhs_dtype".to_string(),
                self.parsed.predicate.numeric_abs_rhs_dtypes(),
            ),
            (
                "numeric_rounding_runtime_execution".to_string(),
                self.parsed.predicate.uses_numeric_rounding().to_string(),
            ),
            (
                "numeric_rounding_operator".to_string(),
                self.parsed.predicate.numeric_rounding_operator(),
            ),
            (
                "numeric_rounding_source_column".to_string(),
                self.parsed.predicate.numeric_rounding_source_columns(),
            ),
            (
                "numeric_rounding_rhs_dtype".to_string(),
                self.parsed.predicate.numeric_rounding_rhs_dtypes(),
            ),
            (
                "generic_expression_predicate_runtime_execution".to_string(),
                self.parsed.predicate.uses_generic_expression().to_string(),
            ),
            (
                "generic_expression_predicate_source_column".to_string(),
                self.parsed.predicate.generic_expression_source_columns(),
            ),
            (
                "generic_expression_predicate_operator_family".to_string(),
                self.parsed.predicate.generic_expression_operator_families(),
            ),
            (
                "generic_expression_predicate_binary_operator_count".to_string(),
                self.parsed
                    .predicate
                    .generic_expression_binary_operator_count()
                    .to_string(),
            ),
            (
                "generic_expression_predicate_comparison_operator".to_string(),
                self.parsed
                    .predicate
                    .generic_expression_comparison_operator(),
            ),
            (
                "logical_predicate_runtime_execution".to_string(),
                self.parsed.predicate.uses_logical_predicate().to_string(),
            ),
            (
                "logical_predicate_operator".to_string(),
                self.parsed.predicate.logical_operator().to_string(),
            ),
            (
                "logical_predicate_leaf_count".to_string(),
                self.parsed.predicate.logical_leaf_count().to_string(),
            ),
            (
                "in_predicate_runtime_execution".to_string(),
                self.parsed.predicate.uses_in_list().to_string(),
            ),
            (
                "in_list_value_count".to_string(),
                self.parsed.predicate.in_list_value_count().to_string(),
            ),
            (
                "in_list_null_value_count".to_string(),
                self.parsed.predicate.in_list_null_value_count().to_string(),
            ),
            (
                "in_subquery_runtime_execution".to_string(),
                self.parsed.predicate.uses_in_subquery().to_string(),
            ),
            (
                "in_subquery_source_column".to_string(),
                self.parsed.predicate.in_subquery_source_columns(),
            ),
            (
                "in_subquery_source_format".to_string(),
                self.parsed.predicate.in_subquery_source_formats(),
            ),
            (
                "in_subquery_materialized_value_count".to_string(),
                if self.parsed.predicate.uses_in_subquery() {
                    self.parsed.predicate.in_subquery_value_count().to_string()
                } else {
                    "0".to_string()
                },
            ),
            (
                "in_subquery_materialized_null_value_count".to_string(),
                if self.parsed.predicate.uses_in_subquery() {
                    self.parsed
                        .predicate
                        .in_subquery_null_value_count()
                        .to_string()
                } else {
                    "0".to_string()
                },
            ),
            (
                "in_predicate_null_semantics".to_string(),
                if self.parsed.predicate.in_list_null_value_count() > 0 {
                    "sql_three_valued_where_filter"
                } else {
                    "not_applicable"
                }
                .to_string(),
            ),
            (
                "date_literal_runtime_execution".to_string(),
                self.parsed.predicate.uses_date_literal().to_string(),
            ),
            (
                "timestamp_literal_runtime_execution".to_string(),
                self.parsed.predicate.uses_timestamp_literal().to_string(),
            ),
            (
                "date_extract_runtime_execution".to_string(),
                self.parsed.predicate.uses_date_extract().to_string(),
            ),
            (
                "date_extract_operator".to_string(),
                self.parsed.predicate.date_extract_operator(),
            ),
            (
                "date_extract_source_column".to_string(),
                self.parsed.predicate.date_extract_source_columns(),
            ),
            (
                "timestamp_extract_runtime_execution".to_string(),
                self.parsed.predicate.uses_timestamp_extract().to_string(),
            ),
            (
                "timestamp_extract_operator".to_string(),
                self.parsed.predicate.timestamp_extract_operator(),
            ),
            (
                "timestamp_extract_source_column".to_string(),
                self.parsed.predicate.timestamp_extract_source_columns(),
            ),
            (
                "date_arithmetic_runtime_execution".to_string(),
                self.parsed.predicate.uses_date_arithmetic().to_string(),
            ),
            (
                "date_arithmetic_operator".to_string(),
                self.parsed.predicate.date_arithmetic_operator(),
            ),
            (
                "date_arithmetic_days".to_string(),
                self.parsed.predicate.date_arithmetic_days(),
            ),
            (
                "date_arithmetic_source_column".to_string(),
                self.parsed.predicate.date_arithmetic_source_columns(),
            ),
            (
                "timestamp_arithmetic_runtime_execution".to_string(),
                self.parsed
                    .predicate
                    .uses_timestamp_arithmetic()
                    .to_string(),
            ),
            (
                "timestamp_arithmetic_operator".to_string(),
                self.parsed.predicate.timestamp_arithmetic_operator(),
            ),
            (
                "timestamp_arithmetic_seconds".to_string(),
                self.parsed.predicate.timestamp_arithmetic_seconds(),
            ),
            (
                "timestamp_arithmetic_source_column".to_string(),
                self.parsed.predicate.timestamp_arithmetic_source_columns(),
            ),
            (
                "cast_runtime_execution".to_string(),
                self.parsed.predicate.uses_cast().to_string(),
            ),
            (
                "cast_source_column".to_string(),
                self.parsed.predicate.cast_source_columns(),
            ),
            (
                "cast_target_dtype".to_string(),
                self.parsed.predicate.cast_target_dtypes(),
            ),
            ("cast_mode".to_string(), self.parsed.predicate.cast_modes()),
            (
                "literal_projection_runtime_execution".to_string(),
                self.parsed.has_literal_projection().to_string(),
            ),
            (
                "literal_projection_columns".to_string(),
                self.parsed
                    .literal_projections
                    .iter()
                    .map(|projection| projection.alias.as_str())
                    .collect::<Vec<_>>()
                    .join(","),
            ),
            (
                "literal_projection_count".to_string(),
                self.parsed.literal_projections.len().to_string(),
            ),
            (
                "cast_projection_runtime_execution".to_string(),
                self.parsed.has_cast_projection().to_string(),
            ),
            (
                "cast_projection_source_column".to_string(),
                self.parsed.cast_projection_source_columns(),
            ),
            (
                "cast_projection_output_column".to_string(),
                self.parsed.cast_projection_output_columns(),
            ),
            (
                "cast_projection_target_dtype".to_string(),
                self.parsed.cast_projection_target_dtypes(),
            ),
            (
                "cast_projection_mode".to_string(),
                self.parsed.cast_projection_modes(),
            ),
            (
                "null_coalesce_projection_runtime_execution".to_string(),
                self.parsed.has_null_coalesce_projection().to_string(),
            ),
            (
                "null_coalesce_projection_source_column".to_string(),
                self.parsed.null_coalesce_projection_source_columns(),
            ),
            (
                "null_coalesce_projection_output_column".to_string(),
                self.parsed.null_coalesce_projection_output_columns(),
            ),
            (
                "null_coalesce_projection_fallback_dtype".to_string(),
                self.parsed.null_coalesce_projection_fallback_dtypes(),
            ),
            (
                "nullif_projection_runtime_execution".to_string(),
                self.parsed.has_nullif_projection().to_string(),
            ),
            (
                "nullif_projection_source_column".to_string(),
                self.parsed.nullif_projection_source_columns(),
            ),
            (
                "nullif_projection_output_column".to_string(),
                self.parsed.nullif_projection_output_columns(),
            ),
            (
                "nullif_projection_sentinel_dtype".to_string(),
                self.parsed.nullif_projection_sentinel_dtypes(),
            ),
            (
                "conditional_projection_runtime_execution".to_string(),
                self.parsed.has_conditional_projection().to_string(),
            ),
            (
                "conditional_projection_predicate_family".to_string(),
                self.parsed.conditional_projection_predicate_families(),
            ),
            (
                "conditional_projection_source_column".to_string(),
                self.parsed.conditional_projection_source_columns(),
            ),
            (
                "conditional_projection_output_column".to_string(),
                self.parsed.conditional_projection_output_columns(),
            ),
            (
                "conditional_projection_then_dtype".to_string(),
                self.parsed.conditional_projection_then_dtypes(),
            ),
            (
                "conditional_projection_else_dtype".to_string(),
                self.parsed.conditional_projection_else_dtypes(),
            ),
            (
                "predicate_projection_runtime_execution".to_string(),
                self.parsed.has_predicate_projection().to_string(),
            ),
            (
                "predicate_projection_predicate_family".to_string(),
                self.parsed.predicate_projection_predicate_families(),
            ),
            (
                "predicate_projection_source_column".to_string(),
                self.parsed.predicate_projection_source_columns(),
            ),
            (
                "predicate_projection_output_column".to_string(),
                self.parsed.predicate_projection_output_columns(),
            ),
            (
                "predicate_projection_null_semantics".to_string(),
                self.parsed.predicate_projection_null_semantics(),
            ),
            (
                "numeric_arithmetic_projection_runtime_execution".to_string(),
                self.parsed.has_numeric_arithmetic_projection().to_string(),
            ),
            (
                "numeric_arithmetic_projection_operator".to_string(),
                self.parsed.numeric_arithmetic_projection_operators(),
            ),
            (
                "numeric_arithmetic_projection_source_column".to_string(),
                self.parsed.numeric_arithmetic_projection_source_columns(),
            ),
            (
                "numeric_arithmetic_projection_output_column".to_string(),
                self.parsed.numeric_arithmetic_projection_output_columns(),
            ),
            (
                "numeric_arithmetic_projection_rhs_dtype".to_string(),
                self.parsed.numeric_arithmetic_projection_rhs_dtypes(),
            ),
            (
                "numeric_abs_projection_runtime_execution".to_string(),
                self.parsed.has_numeric_abs_projection().to_string(),
            ),
            (
                "numeric_abs_projection_source_column".to_string(),
                self.parsed.numeric_abs_projection_source_columns(),
            ),
            (
                "numeric_abs_projection_output_column".to_string(),
                self.parsed.numeric_abs_projection_output_columns(),
            ),
            (
                "numeric_rounding_projection_runtime_execution".to_string(),
                self.parsed.has_numeric_rounding_projection().to_string(),
            ),
            (
                "numeric_rounding_projection_operator".to_string(),
                self.parsed.numeric_rounding_projection_operators(),
            ),
            (
                "numeric_rounding_projection_source_column".to_string(),
                self.parsed.numeric_rounding_projection_source_columns(),
            ),
            (
                "numeric_rounding_projection_output_column".to_string(),
                self.parsed.numeric_rounding_projection_output_columns(),
            ),
            (
                "generic_expression_projection_runtime_execution".to_string(),
                self.parsed.has_generic_expression_projection().to_string(),
            ),
            (
                "generic_expression_projection_source_column".to_string(),
                self.parsed.generic_expression_projection_source_columns(),
            ),
            (
                "generic_expression_projection_output_column".to_string(),
                self.parsed.generic_expression_projection_output_columns(),
            ),
            (
                "generic_expression_projection_operator_family".to_string(),
                self.parsed
                    .generic_expression_projection_operator_families(),
            ),
            (
                "generic_expression_projection_binary_operator_count".to_string(),
                self.parsed
                    .generic_expression_projection_binary_operator_count()
                    .to_string(),
            ),
            (
                "date_arithmetic_projection_runtime_execution".to_string(),
                self.parsed.has_date_arithmetic_projection().to_string(),
            ),
            (
                "date_arithmetic_projection_operator".to_string(),
                self.parsed.date_arithmetic_projection_operators(),
            ),
            (
                "date_arithmetic_projection_days".to_string(),
                self.parsed.date_arithmetic_projection_days(),
            ),
            (
                "date_arithmetic_projection_source_column".to_string(),
                self.parsed.date_arithmetic_projection_source_columns(),
            ),
            (
                "date_arithmetic_projection_output_column".to_string(),
                self.parsed.date_arithmetic_projection_output_columns(),
            ),
            (
                "timestamp_arithmetic_projection_runtime_execution".to_string(),
                self.parsed
                    .has_timestamp_arithmetic_projection()
                    .to_string(),
            ),
            (
                "timestamp_arithmetic_projection_operator".to_string(),
                self.parsed.timestamp_arithmetic_projection_operators(),
            ),
            (
                "timestamp_arithmetic_projection_seconds".to_string(),
                self.parsed.timestamp_arithmetic_projection_seconds(),
            ),
            (
                "timestamp_arithmetic_projection_source_column".to_string(),
                self.parsed.timestamp_arithmetic_projection_source_columns(),
            ),
            (
                "timestamp_arithmetic_projection_output_column".to_string(),
                self.parsed.timestamp_arithmetic_projection_output_columns(),
            ),
            (
                "string_length_projection_runtime_execution".to_string(),
                self.parsed.has_string_length_projection().to_string(),
            ),
            (
                "string_length_projection_source_column".to_string(),
                self.parsed.string_length_projection_source_columns(),
            ),
            (
                "string_length_projection_output_column".to_string(),
                self.parsed.string_length_projection_output_columns(),
            ),
            (
                "string_transform_projection_runtime_execution".to_string(),
                self.parsed.has_string_transform_projection().to_string(),
            ),
            (
                "string_transform_projection_operator".to_string(),
                self.parsed.string_transform_projection_operators(),
            ),
            (
                "string_transform_projection_source_column".to_string(),
                self.parsed.string_transform_projection_source_columns(),
            ),
            (
                "string_transform_projection_output_column".to_string(),
                self.parsed.string_transform_projection_output_columns(),
            ),
            (
                "string_function_projection_runtime_execution".to_string(),
                self.parsed.has_string_function_projection().to_string(),
            ),
            (
                "string_function_projection_operator".to_string(),
                self.parsed.string_function_projection_operators(),
            ),
            (
                "string_function_projection_source_column".to_string(),
                self.parsed.string_function_projection_source_columns(),
            ),
            (
                "string_function_projection_output_column".to_string(),
                self.parsed.string_function_projection_output_columns(),
            ),
            (
                "string_function_projection_literal_count".to_string(),
                self.parsed.string_function_projection_literal_counts(),
            ),
            (
                "date_extract_projection_runtime_execution".to_string(),
                self.parsed.has_date_extract_projection().to_string(),
            ),
            (
                "date_extract_projection_operator".to_string(),
                self.parsed.date_extract_projection_operators(),
            ),
            (
                "date_extract_projection_source_column".to_string(),
                self.parsed.date_extract_projection_source_columns(),
            ),
            (
                "date_extract_projection_output_column".to_string(),
                self.parsed.date_extract_projection_output_columns(),
            ),
            (
                "timestamp_extract_projection_runtime_execution".to_string(),
                self.parsed.has_timestamp_extract_projection().to_string(),
            ),
            (
                "timestamp_extract_projection_operator".to_string(),
                self.parsed.timestamp_extract_projection_operators(),
            ),
            (
                "timestamp_extract_projection_source_column".to_string(),
                self.parsed.timestamp_extract_projection_source_columns(),
            ),
            (
                "timestamp_extract_projection_output_column".to_string(),
                self.parsed.timestamp_extract_projection_output_columns(),
            ),
            ("projection_pushed_down".to_string(), "false".to_string()),
            ("filter_pushed_down".to_string(), "false".to_string()),
            ("limit_pushed_down".to_string(), "false".to_string()),
            ("pushdown_status".to_string(), self.pushdown_status()),
            ("plan_digest".to_string(), self.plan_digest.clone()),
            ("correctness_digest".to_string(), self.result_digest.clone()),
            ("result_digest".to_string(), self.result_digest.clone()),
            ("result_format".to_string(), "inline_jsonl".to_string()),
            ("result_jsonl".to_string(), self.result_jsonl.clone()),
            (
                "output_path".to_string(),
                self.request
                    .output_path
                    .as_ref()
                    .map_or_else(String::new, |path| path.display().to_string()),
            ),
            (
                "output_format".to_string(),
                self.request.output_format.sink_format().to_string(),
            ),
            (
                "output_route".to_string(),
                self.output_route_label().to_string(),
            ),
            ("output_plan_id".to_string(), self.output_plan_id()),
            ("output_plan_digest".to_string(), self.output_plan_digest()),
            (
                "output_plan_status".to_string(),
                if self.output_io_performed() {
                    "smoke_supported"
                } else {
                    "not_applicable_inline_result"
                }
                .to_string(),
            ),
            (
                "output_plan_reuse_allowed".to_string(),
                self.output_fanout_performed().to_string(),
            ),
            ("output_plan_reuse_hit".to_string(), "false".to_string()),
            (
                "result_reuse_for_fanout".to_string(),
                self.output_fanout_performed().to_string(),
            ),
            (
                "fanout_result_reuse_hit".to_string(),
                self.output_fanout_performed().to_string(),
            ),
            (
                "result_replay_verified".to_string(),
                self.result_replay_verified().to_string(),
            ),
            (
                "output_replay_status".to_string(),
                self.output_replay_status(),
            ),
            (
                "output_replay_millis".to_string(),
                self.output_replay_millis().to_string(),
            ),
            (
                "output_fidelity_report_status".to_string(),
                self.output_fidelity_report_status(),
            ),
            (
                "output_fidelity_loss".to_string(),
                self.output_fidelity_loss(),
            ),
            ("output_bytes".to_string(), self.output_bytes.to_string()),
            ("output_digest".to_string(), self.output_digest.clone()),
            (
                "output_fanout_performed".to_string(),
                self.output_fanout_performed().to_string(),
            ),
            (
                "fanout_output_count".to_string(),
                self.fanout_output_count().to_string(),
            ),
            (
                "fanout_output_formats".to_string(),
                self.fanout_output_formats(),
            ),
            (
                "fanout_output_paths".to_string(),
                self.fanout_output_paths(),
            ),
            (
                "fanout_output_bytes".to_string(),
                self.fanout_output_bytes(),
            ),
            (
                "fanout_output_digests".to_string(),
                self.fanout_output_digests(),
            ),
            (
                "fanout_output_certificate_refs".to_string(),
                self.fanout_output_certificate_refs(),
            ),
            (
                "fanout_output_native_io_certificate_statuses".to_string(),
                self.fanout_output_certificate_statuses(),
            ),
            (
                "fanout_output_write_millis".to_string(),
                self.fanout_output_write_millis().to_string(),
            ),
            (
                "fanout_output_replay_statuses".to_string(),
                self.fanout_output_replay_statuses(),
            ),
            (
                "fanout_output_fidelity_statuses".to_string(),
                self.fanout_output_fidelity_statuses(),
            ),
            (
                "fanout_output_fidelity_loss".to_string(),
                self.fanout_output_fidelity_loss(),
            ),
            (
                "vortex_output_runtime_execution".to_string(),
                self.vortex_output_runtime_execution().to_string(),
            ),
            (
                "vortex_output_count".to_string(),
                self.vortex_output_count().to_string(),
            ),
            (
                "vortex_output_paths".to_string(),
                self.vortex_output_paths(),
            ),
            (
                "vortex_output_digests".to_string(),
                self.vortex_output_digests(),
            ),
            (
                "vortex_artifact_digest".to_string(),
                self.primary_vortex_artifact_digest(),
            ),
            (
                "vortex_output_reopen_verified".to_string(),
                self.vortex_output_reopen_verified().to_string(),
            ),
            (
                "vortex_output_row_count".to_string(),
                self.primary_vortex_row_count().to_string(),
            ),
            (
                "vortex_output_row_counts".to_string(),
                self.vortex_output_row_counts(),
            ),
            (
                "vortex_output_column_count".to_string(),
                self.primary_vortex_column_count().to_string(),
            ),
            (
                "vortex_output_column_counts".to_string(),
                self.vortex_output_column_counts(),
            ),
            (
                "vortex_output_column_families".to_string(),
                self.vortex_output_column_families(),
            ),
            (
                "vortex_artifact_bytes".to_string(),
                self.vortex_output_artifact_bytes(),
            ),
            (
                "vortex_write_millis".to_string(),
                self.vortex_write_millis().to_string(),
            ),
            (
                "vortex_digest_millis".to_string(),
                self.vortex_digest_millis().to_string(),
            ),
            (
                "vortex_reopen_verify_millis".to_string(),
                self.vortex_reopen_verify_millis().to_string(),
            ),
            (
                "vortex_output_timing_scope".to_string(),
                if self.vortex_output_runtime_execution() {
                    "local_sql_output_write"
                } else {
                    "not_applicable"
                }
                .to_string(),
            ),
            (
                "vortex_output_certification_level".to_string(),
                if self.vortex_output_runtime_execution() {
                    "local_reopen_row_count_proof"
                } else {
                    "not_applicable"
                }
                .to_string(),
            ),
            (
                "upstream_vortex_write_called".to_string(),
                self.vortex_output_runtime_execution().to_string(),
            ),
            (
                "upstream_vortex_scan_called".to_string(),
                self.vortex_output_reopen_verified().to_string(),
            ),
            (
                "source_read_millis".to_string(),
                self.source.read_millis.to_string(),
            ),
            (
                "compatibility_parse_millis".to_string(),
                self.source.parse_millis.to_string(),
            ),
            (
                "operator_compute_millis".to_string(),
                self.operator_compute_millis.to_string(),
            ),
            (
                "result_sink_write_millis".to_string(),
                self.output_write_millis.to_string(),
            ),
            (
                "output_write_millis".to_string(),
                self.output_write_millis.to_string(),
            ),
            (
                "evidence_render_millis".to_string(),
                self.evidence_render_millis.to_string(),
            ),
            (
                "total_runtime_millis".to_string(),
                self.total_runtime_millis.to_string(),
            ),
            (
                "source_native_io_certificate_status".to_string(),
                "scoped_compatibility_import_certificate".to_string(),
            ),
            (
                "native_io_certificate_status".to_string(),
                "scoped_compatibility_import_certificate".to_string(),
            ),
            (
                "source_certificate_ref".to_string(),
                self.source_certificate_ref(),
            ),
            (
                "execution_certificate_status".to_string(),
                "certified".to_string(),
            ),
            (
                "execution_certificate_ref".to_string(),
                self.execution_certificate_ref(),
            ),
            (
                "materialization_boundary".to_string(),
                self.materialization_boundary(),
            ),
            ("data_decoded".to_string(), "true".to_string()),
            ("data_materialized".to_string(), "true".to_string()),
            (
                "output_io_performed".to_string(),
                self.output_io_performed().to_string(),
            ),
            (
                "write_io".to_string(),
                self.output_io_performed().to_string(),
            ),
            (
                "output_native_io_certificate_status".to_string(),
                self.output_certificate_status(),
            ),
            (
                "output_certificate_ref".to_string(),
                self.output_certificate_ref(),
            ),
            ("object_store_io".to_string(), "false".to_string()),
            ("network_probe".to_string(), "false".to_string()),
            ("catalog_probe".to_string(), "false".to_string()),
            ("foundry_runtime_invoked".to_string(), "false".to_string()),
            ("foundry_spark_invoked".to_string(), "false".to_string()),
            ("fallback_attempted".to_string(), "false".to_string()),
            (
                "fallback_execution_allowed".to_string(),
                "false".to_string(),
            ),
            ("external_engine_invoked".to_string(), "false".to_string()),
            (
                "claim_gate_status".to_string(),
                "fixture_smoke_only".to_string(),
            ),
            ("claim_gate_reason".to_string(), self.claim_gate_reason()),
            ("performance_claim_allowed".to_string(), "false".to_string()),
            ("production_claim_allowed".to_string(), "false".to_string()),
            (
                "sql_dataframe_runtime_claim_allowed".to_string(),
                "false".to_string(),
            ),
            (
                "object_store_lakehouse_claim_allowed".to_string(),
                "false".to_string(),
            ),
            (
                "spark_replacement_claim_allowed".to_string(),
                "false".to_string(),
            ),
        ];
        if let Some(output) = self.primary_written_output() {
            fields.extend(output.workspace_write_report.evidence_fields("output"));
        } else {
            fields.push((
                "output_workspace_path_safety_status".to_string(),
                "not_applicable_inline_result".to_string(),
            ));
        }
        fields.extend([
            (
                "fanout_output_workspace_path_safety_statuses".to_string(),
                self.fanout_output_workspace_safety_statuses(),
            ),
            (
                "fanout_output_commit_modes".to_string(),
                self.fanout_output_commit_modes(),
            ),
        ]);
        fields
    }

    fn to_text(&self) -> String {
        let output = self.request.output_path.as_ref().map_or_else(
            || "not requested".to_string(),
            |path| path.display().to_string(),
        );
        let fanout = if self.output_fanout_performed() {
            format!(
                "\nfanout outputs: {} ({})",
                self.fanout_output_count(),
                self.fanout_output_formats()
            )
        } else {
            String::new()
        };
        format!(
            "SQL local-source smoke\nschema_version: {SCHEMA_VERSION}\nsource: {}\nrows read: {}\nrows selected: {}\nrows output: {}\noutput: {output}{fanout}\nresult:\n{}fallback_attempted: false\nexternal_engine_invoked: false\nclaim_gate_status: fixture_smoke_only",
            self.parsed.source_path.display(),
            self.source.rows.len(),
            self.selected_row_count,
            self.output_rows.len(),
            self.result_jsonl,
        )
    }

    fn source_state_id(source: &CsvSourceData) -> String {
        source.source_state_id()
    }

    fn source_state_digest(&self, source: &CsvSourceData) -> String {
        source.source_state_digest(&self.source_schema_digest)
    }

    fn source_format_label(&self) -> &'static str {
        self.source.source_format.as_str()
    }

    fn source_adapter_id(&self) -> &'static str {
        match self.source.source_format {
            LocalSourceFormat::Csv => "local_csv_input_adapter",
            LocalSourceFormat::Json => "local_json_input_adapter",
            LocalSourceFormat::JsonLines => "local_jsonl_input_adapter",
            LocalSourceFormat::Parquet => "local_parquet_input_adapter",
            LocalSourceFormat::ArrowIpc => "local_arrow_ipc_input_adapter",
            LocalSourceFormat::Avro => "local_avro_input_adapter",
            LocalSourceFormat::Orc => "local_orc_input_adapter",
        }
    }

    fn source_certificate_ref(&self) -> String {
        format!(
            "sql-local-source.{}.compatibility-source.v1",
            self.source_format_label()
        )
    }

    fn execution_certificate_ref(&self) -> String {
        format!(
            "sql-local-source.{}.{}.execution.v1",
            self.source_format_label(),
            self.parsed.execution_certificate_suffix()
        )
    }

    fn materialization_boundary(&self) -> String {
        format!(
            "local_{}_row_materialization_to_expression_semantics",
            self.source_format_label()
        )
    }

    fn output_io_performed(&self) -> bool {
        !self.written_outputs.is_empty()
    }

    fn result_replay_verified(&self) -> bool {
        self.output_io_performed()
            && self
                .written_outputs
                .iter()
                .all(|output| output.replay.verified)
    }

    fn output_replay_status(&self) -> String {
        if !self.output_io_performed() {
            return "not_applicable_inline_result".to_string();
        }
        if self.result_replay_verified() {
            "verified_local_sink_artifacts".to_string()
        } else {
            "blocked_unverified_local_sink_artifact".to_string()
        }
    }

    fn output_replay_millis(&self) -> u128 {
        self.written_outputs
            .iter()
            .map(|output| output.replay.replay_millis)
            .sum()
    }

    fn output_fidelity_report_status(&self) -> String {
        if !self.output_io_performed() {
            return "not_applicable_inline_result".to_string();
        }
        if self
            .written_outputs
            .iter()
            .all(|output| output.replay.verified)
        {
            "scoped_local_output_fidelity_reported".to_string()
        } else {
            "blocked_unverified_output_replay".to_string()
        }
    }

    fn output_fidelity_loss(&self) -> String {
        csv_or_not_applicable(self.written_outputs.iter().map(|output| {
            format!(
                "{}:{}",
                output.format.sink_format(),
                output.replay.fidelity_loss
            )
        }))
    }

    fn output_fanout_performed(&self) -> bool {
        !self.request.fanout_outputs.is_empty()
    }

    fn primary_written_output(&self) -> Option<&SqlWrittenOutput> {
        let primary_path = self.request.output_path.as_ref()?;
        self.written_outputs
            .iter()
            .find(|output| &output.path == primary_path)
    }

    fn fanout_written_outputs(&self) -> impl Iterator<Item = &SqlWrittenOutput> {
        let primary_path = self.request.output_path.as_ref();
        self.written_outputs
            .iter()
            .filter(move |output| Some(&output.path) != primary_path)
    }

    fn fanout_output_count(&self) -> usize {
        self.fanout_written_outputs().count()
    }

    fn fanout_output_formats(&self) -> String {
        self.fanout_written_outputs()
            .map(|output| output.format.sink_format())
            .collect::<Vec<_>>()
            .join(",")
    }

    fn fanout_output_paths(&self) -> String {
        self.fanout_written_outputs()
            .map(|output| output.path.display().to_string())
            .collect::<Vec<_>>()
            .join(",")
    }

    fn fanout_output_bytes(&self) -> String {
        self.fanout_written_outputs()
            .map(|output| format!("{}:{}", output.format.sink_format(), output.bytes))
            .collect::<Vec<_>>()
            .join(",")
    }

    fn fanout_output_digests(&self) -> String {
        self.fanout_written_outputs()
            .map(|output| format!("{}:{}", output.format.sink_format(), output.digest))
            .collect::<Vec<_>>()
            .join(",")
    }

    fn fanout_output_certificate_refs(&self) -> String {
        self.fanout_written_outputs()
            .map(|output| {
                format!(
                    "{}:{}",
                    output.format.sink_format(),
                    output.format.certificate_ref()
                )
            })
            .collect::<Vec<_>>()
            .join(",")
    }

    fn fanout_output_certificate_statuses(&self) -> String {
        self.fanout_written_outputs()
            .map(|output| {
                format!(
                    "{}:{}",
                    output.format.sink_format(),
                    output.format.certificate_status()
                )
            })
            .collect::<Vec<_>>()
            .join(",")
    }

    fn fanout_output_write_millis(&self) -> u128 {
        self.fanout_written_outputs()
            .map(|output| output.write_millis)
            .sum()
    }

    fn fanout_output_replay_statuses(&self) -> String {
        csv_or_not_applicable(
            self.fanout_written_outputs()
                .map(|output| format!("{}:{}", output.format.sink_format(), output.replay.status)),
        )
    }

    fn fanout_output_fidelity_statuses(&self) -> String {
        csv_or_not_applicable(self.fanout_written_outputs().map(|output| {
            format!(
                "{}:{}",
                output.format.sink_format(),
                output.replay.fidelity_status
            )
        }))
    }

    fn fanout_output_fidelity_loss(&self) -> String {
        csv_or_not_applicable(self.fanout_written_outputs().map(|output| {
            format!(
                "{}:{}",
                output.format.sink_format(),
                output.replay.fidelity_loss
            )
        }))
    }

    fn fanout_output_workspace_safety_statuses(&self) -> String {
        csv_or_not_applicable(self.fanout_written_outputs().map(|output| {
            format!(
                "{}:{}",
                output.format.sink_format(),
                output.workspace_write_report.path_safety_report.accepted()
            )
        }))
    }

    fn fanout_output_commit_modes(&self) -> String {
        csv_or_not_applicable(self.fanout_written_outputs().map(|output| {
            format!(
                "{}:{}",
                output.format.sink_format(),
                output.workspace_write_report.commit_mode.as_str()
            )
        }))
    }

    fn vortex_written_outputs(&self) -> impl Iterator<Item = &SqlWrittenOutput> {
        self.written_outputs
            .iter()
            .filter(|output| matches!(output.format, SqlLocalSourceOutputFormat::Vortex))
    }

    fn vortex_output_runtime_execution(&self) -> bool {
        self.vortex_written_outputs().next().is_some()
    }

    fn vortex_output_count(&self) -> usize {
        self.vortex_written_outputs().count()
    }

    fn vortex_output_paths(&self) -> String {
        csv_or_not_applicable(
            self.vortex_written_outputs()
                .map(|output| output.path.display().to_string()),
        )
    }

    fn vortex_output_digests(&self) -> String {
        csv_or_not_applicable(
            self.vortex_written_outputs()
                .map(|output| output.digest.clone()),
        )
    }

    fn primary_vortex_artifact_digest(&self) -> String {
        self.vortex_written_outputs().next().map_or_else(
            || "not_applicable".to_string(),
            |output| output.digest.clone(),
        )
    }

    fn vortex_output_reopen_verified(&self) -> bool {
        self.vortex_written_outputs().any(|output| {
            output
                .vortex_report
                .as_ref()
                .is_some_and(|report| report.upstream_vortex_scan_called)
        })
    }

    fn vortex_output_row_counts(&self) -> String {
        csv_or_not_applicable(self.vortex_written_outputs().filter_map(|output| {
            output
                .vortex_report
                .as_ref()
                .map(|report| report.row_count.to_string())
        }))
    }

    fn primary_vortex_row_count(&self) -> u64 {
        self.vortex_written_outputs()
            .next()
            .and_then(|output| output.vortex_report.as_ref())
            .map_or(0, |report| report.row_count)
    }

    fn vortex_output_column_counts(&self) -> String {
        csv_or_not_applicable(self.vortex_written_outputs().filter_map(|output| {
            output
                .vortex_report
                .as_ref()
                .map(|report| report.column_count.to_string())
        }))
    }

    fn primary_vortex_column_count(&self) -> usize {
        self.vortex_written_outputs()
            .next()
            .and_then(|output| output.vortex_report.as_ref())
            .map_or(0, |report| report.column_count)
    }

    fn vortex_output_column_families(&self) -> String {
        csv_or_not_applicable(self.vortex_written_outputs().filter_map(|output| {
            output
                .vortex_report
                .as_ref()
                .map(shardloom_vortex::VortexPreparedStateWriteReport::column_family_summary)
        }))
    }

    fn vortex_output_artifact_bytes(&self) -> String {
        csv_or_not_applicable(self.vortex_written_outputs().filter_map(|output| {
            output
                .vortex_report
                .as_ref()
                .map(|report| report.bytes_written.to_string())
        }))
    }

    fn vortex_write_millis(&self) -> u128 {
        self.vortex_written_outputs()
            .filter_map(|output| output.vortex_report.as_ref())
            .map(|report| report.write_micros / 1_000)
            .sum()
    }

    fn vortex_digest_millis(&self) -> u128 {
        self.vortex_written_outputs()
            .filter_map(|output| output.vortex_report.as_ref())
            .map(|report| report.digest_micros / 1_000)
            .sum()
    }

    fn vortex_reopen_verify_millis(&self) -> u128 {
        self.vortex_written_outputs()
            .filter_map(|output| output.vortex_report.as_ref())
            .map(|report| report.reopen_scan_micros / 1_000)
            .sum()
    }

    fn output_route_label(&self) -> &'static str {
        match (
            self.request.output_path.is_some(),
            self.output_fanout_performed(),
        ) {
            (true, true) => "local_sink_and_fanout",
            (true, false) => "local_sink",
            (false, true) => "local_fanout",
            (false, false) => "inline_result",
        }
    }

    fn output_plan_id(&self) -> String {
        if self.output_fanout_performed() {
            format!(
                "sql-local-source.{}.fanout.output-plan.v1",
                self.source_format_label()
            )
        } else {
            format!(
                "sql-local-source.{}.{}.output-plan.v1",
                self.source_format_label(),
                self.request.output_format.sink_format()
            )
        }
    }

    fn output_plan_digest(&self) -> String {
        let mut fragments = Vec::new();
        if let Some(path) = &self.request.output_path {
            fragments.push(format!(
                "primary:{}={}",
                self.request.output_format.sink_format(),
                path.display()
            ));
        }
        fragments.extend(self.request.fanout_outputs.iter().map(|target| {
            format!(
                "fanout:{}={}",
                target.format.sink_format(),
                target.path.display()
            )
        }));
        fnv64_digest(&format!(
            "{}|{}|{}|{}",
            self.output_plan_id(),
            self.source_schema_digest,
            self.plan_digest,
            fragments.join(";")
        ))
    }

    fn output_certificate_status(&self) -> String {
        match (
            self.request.output_path.is_some(),
            self.output_fanout_performed(),
        ) {
            (true, true) => format!(
                "{},certified_local_fanout_sinks",
                self.request.output_format.certificate_status()
            ),
            (true, false) => self.request.output_format.certificate_status().to_string(),
            (false, true) => "certified_local_fanout_sinks".to_string(),
            (false, false) => "not_requested".to_string(),
        }
    }

    fn output_certificate_ref(&self) -> String {
        if !self.output_fanout_performed() {
            return if self.request.output_path.is_some() {
                self.request.output_format.certificate_ref().to_string()
            } else {
                "not_requested".to_string()
            };
        }
        let mut refs = Vec::new();
        if self.request.output_path.is_some() {
            refs.push(format!(
                "{}:{}",
                self.request.output_format.sink_format(),
                self.request.output_format.certificate_ref()
            ));
        }
        refs.extend(self.fanout_written_outputs().map(|output| {
            format!(
                "{}:{}",
                output.format.sink_format(),
                output.format.certificate_ref()
            )
        }));
        refs.join(",")
    }

    fn pushdown_status(&self) -> String {
        format!(
            "not_applicable_local_{}_transient",
            self.source_format_label()
        )
    }

    fn claim_gate_reason(&self) -> String {
        format!(
            "one_scoped_local_{}_sql_{}_smoke",
            self.source_format_label(),
            self.parsed.claim_gate_reason_suffix()
        )
    }
}

fn read_local_source(path: &Path) -> Result<CsvSourceData, ShardLoomError> {
    read_local_source_with_plan(
        path,
        &LocalSourceReadPlan::full("full_source_state_default"),
    )
}

fn read_local_source_with_plan(
    path: &Path,
    read_plan: &LocalSourceReadPlan,
) -> Result<CsvSourceData, ShardLoomError> {
    reject_remote_source_path(path)?;
    let source_format = LocalSourceFormat::from_path(path)?;
    let read_start = Instant::now();
    let bytes = fs::read(path).map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "failed to read local {} source {}: {error}",
            source_format.row_label(),
            path.display(),
        ))
    })?;
    let read_millis = read_start.elapsed().as_millis();
    let source_bytes = u64::try_from(bytes.len()).map_err(|_| {
        ShardLoomError::InvalidOperation(format!(
            "{} source length does not fit in u64",
            source_format.row_label()
        ))
    })?;
    let source_digest = fnv64_digest_bytes(&bytes);
    let parse_start = Instant::now();
    let content = match source_format {
        LocalSourceFormat::Csv => {
            let content = decode_local_text_source(path, source_format, bytes)?;
            let (header, rows) = parse_csv_source_content_with_plan(&content, read_plan)?;
            LocalSourceReadContent::text(source_format, header, rows)
        }
        LocalSourceFormat::Json => {
            let content = decode_local_text_source(path, source_format, bytes)?;
            let (header, rows) = parse_json_source_content_with_plan(&content, read_plan)?;
            LocalSourceReadContent::text(source_format, header, rows)
        }
        LocalSourceFormat::JsonLines => {
            let content = decode_local_text_source(path, source_format, bytes)?;
            let (header, rows) = parse_jsonl_source_content_with_plan(&content, read_plan)?;
            LocalSourceReadContent::text(source_format, header, rows)
        }
        LocalSourceFormat::Parquet => read_parquet_source_content(path, read_plan)?,
        LocalSourceFormat::ArrowIpc => read_arrow_ipc_source_content(path, read_plan)?,
        LocalSourceFormat::Avro => read_avro_source_content(path, read_plan)?,
        LocalSourceFormat::Orc => read_orc_source_content(path, read_plan)?,
    };
    let LocalSourceReadContent {
        header,
        mut rows,
        reader_projection_columns,
        source_to_columnar_millis,
        record_batch_count,
        materialization_layout,
        parse_normalization,
        columnar_source_preserved,
    } = content;
    prune_rows_to_read_plan(&mut rows, read_plan);
    let materialized_columns = read_plan.materialized_columns(&header);
    let reader_projection_columns =
        reader_projection_columns.unwrap_or_else(|| materialized_columns.clone());
    let projection_pushdown_status = source_format.projection_pushdown_status(read_plan);
    Ok(CsvSourceData {
        source_format,
        header,
        rows,
        read_plan: read_plan.clone(),
        materialized_columns,
        reader_projection_columns,
        projection_pushdown_status,
        source_bytes,
        source_digest,
        read_millis,
        parse_millis: parse_start.elapsed().as_millis(),
        source_to_columnar_millis,
        record_batch_count,
        materialization_layout,
        parse_normalization,
        columnar_source_preserved,
    })
}

fn decode_local_text_source(
    path: &Path,
    source_format: LocalSourceFormat,
    bytes: Vec<u8>,
) -> Result<String, ShardLoomError> {
    String::from_utf8(bytes).map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "local {} source {} is not valid UTF-8: {error}",
            source_format.row_label(),
            path.display()
        ))
    })
}

fn prune_rows_to_read_plan(rows: &mut [ExpressionInputRow], read_plan: &LocalSourceReadPlan) {
    let Some(required_columns) = read_plan.required_columns.as_ref() else {
        return;
    };
    for row in rows {
        row.retain(|column, _value| required_columns.contains(column));
    }
}

#[cfg(feature = "universal-format-io")]
fn read_structured_columnar_source_content<ReadFull, ReadProjected>(
    path: &Path,
    read_plan: &LocalSourceReadPlan,
    source_format: LocalSourceFormat,
    read_full: ReadFull,
    read_projected: ReadProjected,
) -> Result<LocalSourceReadContent, ShardLoomError>
where
    ReadFull:
        FnOnce(&Path, usize) -> Result<shardloom_vortex::FlatLocalColumnarSource, ShardLoomError>,
    ReadProjected: FnOnce(
        &Path,
        usize,
        &[String],
    ) -> Result<shardloom_vortex::FlatLocalColumnarSource, ShardLoomError>,
{
    let source_to_columnar_start = Instant::now();
    let columnar_source = if let Some(required_columns) = read_plan.required_columns_vec() {
        read_projected(path, MAX_INPUT_ROWS, &required_columns)?
    } else {
        read_full(path, MAX_INPUT_ROWS)?
    };
    let source_to_columnar_millis = source_to_columnar_start.elapsed().as_millis();
    for column in &columnar_source.header {
        validate_sql_identifier(column)?;
    }
    let record_batch_count = columnar_source.batches.len();
    let table = shardloom_vortex::materialize_flat_columnar_source_to_scalar_table(
        &columnar_source,
        path,
        source_format.row_label(),
    )?;
    Ok(LocalSourceReadContent::columnar_then_scalar(
        table.header,
        table.rows,
        table.reader_projection_columns,
        source_to_columnar_millis,
        record_batch_count,
    ))
}

#[cfg(feature = "universal-format-io")]
fn read_parquet_source_content(
    path: &Path,
    read_plan: &LocalSourceReadPlan,
) -> Result<LocalSourceReadContent, ShardLoomError> {
    read_structured_columnar_source_content(
        path,
        read_plan,
        LocalSourceFormat::Parquet,
        shardloom_vortex::read_flat_parquet_columnar_source,
        shardloom_vortex::read_flat_parquet_columnar_source_with_projection,
    )
}

#[cfg(not(feature = "universal-format-io"))]
fn read_parquet_source_content(
    _path: &Path,
    _read_plan: &LocalSourceReadPlan,
) -> Result<LocalSourceReadContent, ShardLoomError> {
    Err(unsupported_sql_error(
        "local Parquet source runtime requires building shardloom-cli with --features universal-format-io; default builds expose Parquet as a deterministic blocked adapter",
    ))
}

#[cfg(feature = "universal-format-io")]
fn read_arrow_ipc_source_content(
    path: &Path,
    read_plan: &LocalSourceReadPlan,
) -> Result<LocalSourceReadContent, ShardLoomError> {
    read_structured_columnar_source_content(
        path,
        read_plan,
        LocalSourceFormat::ArrowIpc,
        shardloom_vortex::read_flat_arrow_ipc_columnar_source,
        shardloom_vortex::read_flat_arrow_ipc_columnar_source_with_projection,
    )
}

#[cfg(not(feature = "universal-format-io"))]
fn read_arrow_ipc_source_content(
    _path: &Path,
    _read_plan: &LocalSourceReadPlan,
) -> Result<LocalSourceReadContent, ShardLoomError> {
    Err(unsupported_sql_error(
        "local Arrow IPC source runtime requires building shardloom-cli with --features universal-format-io; default builds expose Arrow IPC as a deterministic blocked adapter",
    ))
}

#[cfg(feature = "universal-format-io")]
fn read_avro_source_content(
    path: &Path,
    read_plan: &LocalSourceReadPlan,
) -> Result<LocalSourceReadContent, ShardLoomError> {
    read_structured_columnar_source_content(
        path,
        read_plan,
        LocalSourceFormat::Avro,
        shardloom_vortex::read_flat_avro_columnar_source,
        shardloom_vortex::read_flat_avro_columnar_source_with_projection,
    )
}

#[cfg(not(feature = "universal-format-io"))]
fn read_avro_source_content(
    _path: &Path,
    _read_plan: &LocalSourceReadPlan,
) -> Result<LocalSourceReadContent, ShardLoomError> {
    Err(unsupported_sql_error(
        "local Avro source runtime requires building shardloom-cli with --features universal-format-io; default builds expose Avro as a deterministic blocked adapter",
    ))
}

#[cfg(feature = "universal-format-io")]
fn read_orc_source_content(
    path: &Path,
    read_plan: &LocalSourceReadPlan,
) -> Result<LocalSourceReadContent, ShardLoomError> {
    read_structured_columnar_source_content(
        path,
        read_plan,
        LocalSourceFormat::Orc,
        shardloom_vortex::read_flat_orc_columnar_source,
        shardloom_vortex::read_flat_orc_columnar_source_with_projection,
    )
}

#[cfg(not(feature = "universal-format-io"))]
fn read_orc_source_content(
    _path: &Path,
    _read_plan: &LocalSourceReadPlan,
) -> Result<LocalSourceReadContent, ShardLoomError> {
    Err(unsupported_sql_error(
        "local ORC source runtime requires building shardloom-cli with --features universal-format-io; default builds expose ORC as a deterministic blocked adapter",
    ))
}

fn parse_csv_source_content_with_plan(
    content: &str,
    read_plan: &LocalSourceReadPlan,
) -> Result<(Vec<String>, Vec<ExpressionInputRow>), ShardLoomError> {
    let mut records = content
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(split_csv_record)
        .collect::<Result<Vec<_>, _>>()?;
    if records.is_empty() {
        return Err(unsupported_sql_error(
            "CSV source must include a header row",
        ));
    }
    let mut header = records.remove(0);
    strip_utf8_bom_from_first_header_cell(&mut header);
    if header.is_empty() {
        return Err(unsupported_sql_error("CSV source header must not be empty"));
    }
    for column in &header {
        validate_sql_identifier(column)?;
    }
    if records.len() > MAX_INPUT_ROWS {
        return Err(unsupported_sql_error(&format!(
            "scoped SQL local-source smoke supports at most {MAX_INPUT_ROWS} CSV data rows"
        )));
    }
    let mut rows = Vec::with_capacity(records.len());
    for record in records {
        if record.len() != header.len() {
            return Err(unsupported_sql_error(
                "CSV row width must match the header width for this scoped SQL smoke",
            ));
        }
        let mut row = ExpressionInputRow::new();
        for (column, value) in header.iter().zip(record) {
            if read_plan.should_materialize(column) {
                row.insert(column.clone(), parse_csv_scalar(&value));
            }
        }
        rows.push(row);
    }
    Ok((header, rows))
}

#[cfg(test)]
fn parse_jsonl_source_content(
    content: &str,
) -> Result<(Vec<String>, Vec<ExpressionInputRow>), ShardLoomError> {
    parse_jsonl_source_content_with_plan(
        content,
        &LocalSourceReadPlan::full("full_source_state_parse_test"),
    )
}

fn parse_jsonl_source_content_with_plan(
    content: &str,
    read_plan: &LocalSourceReadPlan,
) -> Result<(Vec<String>, Vec<ExpressionInputRow>), ShardLoomError> {
    let mut raw_rows = Vec::new();
    for (line_index, line) in content
        .lines()
        .filter(|line| !line.trim().is_empty())
        .enumerate()
    {
        let line = if line_index == 0 {
            line.strip_prefix('\u{feff}').unwrap_or(line)
        } else {
            line
        };
        let fields = parse_flat_json_object(line).map_err(|error| {
            ShardLoomError::InvalidOperation(format!(
                "JSONL row {} is not admitted by this scoped source runtime: {error}",
                line_index + 1
            ))
        })?;
        raw_rows.push(fields);
    }
    materialize_flat_json_rows("JSONL", raw_rows, read_plan)
}

#[cfg(test)]
fn parse_json_source_content(
    content: &str,
) -> Result<(Vec<String>, Vec<ExpressionInputRow>), ShardLoomError> {
    parse_json_source_content_with_plan(
        content,
        &LocalSourceReadPlan::full("full_source_state_parse_test"),
    )
}

fn parse_json_source_content_with_plan(
    content: &str,
    read_plan: &LocalSourceReadPlan,
) -> Result<(Vec<String>, Vec<ExpressionInputRow>), ShardLoomError> {
    let trimmed = content.trim_start_matches('\u{feff}').trim();
    if trimmed.is_empty() {
        return Err(unsupported_sql_error(
            "JSON source must include one flat object or an array of flat object rows",
        ));
    }
    let chars = trimmed.chars().collect::<Vec<_>>();
    let mut index = skip_json_ws(&chars, 0);
    let mut raw_rows = Vec::new();
    match chars.get(index) {
        Some('{') => {
            let (fields, next_index) = parse_flat_json_object_at(&chars, index, "JSON")?;
            index = skip_json_ws(&chars, next_index);
            if index != chars.len() {
                return Err(unsupported_sql_error(
                    "JSON source must contain exactly one flat object or one array of flat objects",
                ));
            }
            raw_rows.push(fields);
        }
        Some('[') => {
            index += 1;
            loop {
                index = skip_json_ws(&chars, index);
                if chars.get(index) == Some(&']') {
                    index += 1;
                    break;
                }
                let (fields, next_index) = parse_flat_json_object_at(&chars, index, "JSON")?;
                raw_rows.push(fields);
                index = skip_json_ws(&chars, next_index);
                match chars.get(index) {
                    Some(',') => index += 1,
                    Some(']') => {
                        index += 1;
                        break;
                    }
                    _ => {
                        return Err(unsupported_sql_error(
                            "JSON array rows must be separated by ','",
                        ));
                    }
                }
            }
            if skip_json_ws(&chars, index) != chars.len() {
                return Err(unsupported_sql_error(
                    "JSON source array must be the only top-level value",
                ));
            }
        }
        _ => {
            return Err(unsupported_sql_error(
                "JSON source must be a flat object or an array of flat object rows",
            ));
        }
    }
    materialize_flat_json_rows("JSON", raw_rows, read_plan)
}

fn materialize_flat_json_rows(
    source_label: &str,
    raw_rows: Vec<Vec<(String, ScalarValue)>>,
    read_plan: &LocalSourceReadPlan,
) -> Result<(Vec<String>, Vec<ExpressionInputRow>), ShardLoomError> {
    let mut header = Vec::new();
    for fields in &raw_rows {
        for (name, _value) in fields {
            if !header.contains(name) {
                validate_sql_identifier(name)?;
                header.push(name.clone());
            }
        }
    }
    if raw_rows.is_empty() {
        return Err(unsupported_sql_error(&format!(
            "{source_label} source must include at least one object row"
        )));
    }
    if raw_rows.len() > MAX_INPUT_ROWS {
        return Err(unsupported_sql_error(&format!(
            "scoped SQL local-source smoke supports at most {MAX_INPUT_ROWS} {source_label} data rows"
        )));
    }
    let mut rows = Vec::with_capacity(raw_rows.len());
    for fields in raw_rows {
        let mut row = ExpressionInputRow::new();
        for column in &header {
            if read_plan.should_materialize(column) {
                row.insert(column.clone(), ScalarValue::Null);
            }
        }
        for (column, value) in fields {
            if read_plan.should_materialize(&column) {
                row.insert(column, value);
            }
        }
        rows.push(row);
    }
    Ok((header, rows))
}

fn parse_flat_json_object(raw: &str) -> Result<Vec<(String, ScalarValue)>, ShardLoomError> {
    let chars = raw.trim().chars().collect::<Vec<_>>();
    let (fields, index) = parse_flat_json_object_at(&chars, skip_json_ws(&chars, 0), "JSONL")?;
    if skip_json_ws(&chars, index) != chars.len() {
        return Err(unsupported_sql_error(
            "JSONL rows must contain exactly one JSON object per line",
        ));
    }
    Ok(fields)
}

fn parse_flat_json_object_at(
    chars: &[char],
    mut index: usize,
    source_label: &str,
) -> Result<(Vec<(String, ScalarValue)>, usize), ShardLoomError> {
    if chars.get(index) != Some(&'{') {
        return Err(unsupported_sql_error(&format!(
            "{source_label} rows must be flat JSON objects"
        )));
    }
    index += 1;
    let mut fields = Vec::new();
    loop {
        index = skip_json_ws(chars, index);
        if chars.get(index) == Some(&'}') {
            index += 1;
            break;
        }
        let (key, next_index) = parse_json_string(chars, index)?;
        validate_sql_identifier(&key)?;
        index = skip_json_ws(chars, next_index);
        if chars.get(index) != Some(&':') {
            return Err(unsupported_sql_error(
                "JSON object fields must use ':' between key and value",
            ));
        }
        index += 1;
        let (value, next_index) = parse_json_value(chars, index)?;
        fields.push((key, value));
        index = skip_json_ws(chars, next_index);
        match chars.get(index) {
            Some(',') => index += 1,
            Some('}') => {
                index += 1;
                break;
            }
            _ => {
                return Err(unsupported_sql_error(
                    "JSON object fields must be separated by ','",
                ));
            }
        }
    }
    if fields.is_empty() {
        return Err(unsupported_sql_error(&format!(
            "{source_label} object rows must include at least one field"
        )));
    }
    Ok((fields, index))
}

fn parse_json_value(
    chars: &[char],
    mut index: usize,
) -> Result<(ScalarValue, usize), ShardLoomError> {
    index = skip_json_ws(chars, index);
    match chars.get(index) {
        Some('"') => {
            let (value, next_index) = parse_json_string(chars, index)?;
            Ok((ScalarValue::Utf8(value), next_index))
        }
        Some('{' | '[') => Err(unsupported_sql_error(
            "JSON source runtime admits scalar values only; nested objects and arrays remain blocked",
        )),
        Some(_) => {
            let start = index;
            while let Some(ch) = chars.get(index) {
                if *ch == ',' || *ch == '}' {
                    break;
                }
                index += 1;
            }
            let token = chars[start..index]
                .iter()
                .collect::<String>()
                .trim()
                .to_string();
            if token.is_empty() {
                return Err(unsupported_sql_error(
                    "JSON scalar values must not be empty",
                ));
            }
            let value = parse_json_bare_scalar(&token)?;
            Ok((value, index))
        }
        None => Err(unsupported_sql_error("JSONL object value is missing")),
    }
}

fn parse_json_bare_scalar(token: &str) -> Result<ScalarValue, ShardLoomError> {
    if token == "null" {
        Ok(ScalarValue::Null)
    } else if token == "true" {
        Ok(ScalarValue::Boolean(true))
    } else if token == "false" {
        Ok(ScalarValue::Boolean(false))
    } else if let Ok(parsed) = token.parse::<i64>() {
        Ok(ScalarValue::Int64(parsed))
    } else if let Ok(parsed) = token.parse::<f64>() {
        if parsed.is_finite() {
            Ok(ScalarValue::Float64(parsed))
        } else {
            Err(unsupported_sql_error(
                "JSON numeric values must be finite int64 or float64 scalars",
            ))
        }
    } else {
        Err(unsupported_sql_error(
            "JSON bare values are limited to null, booleans, finite numbers, and quoted strings",
        ))
    }
}

fn parse_json_string(chars: &[char], mut index: usize) -> Result<(String, usize), ShardLoomError> {
    if chars.get(index) != Some(&'"') {
        return Err(unsupported_sql_error(
            "JSON object keys and string values must be quoted strings",
        ));
    }
    index += 1;
    let mut value = String::new();
    while let Some(ch) = chars.get(index).copied() {
        index += 1;
        match ch {
            '"' => return Ok((value, index)),
            '\\' => {
                let Some(escaped) = chars.get(index).copied() else {
                    return Err(unsupported_sql_error("JSONL string escape is incomplete"));
                };
                index += 1;
                match escaped {
                    '"' => value.push('"'),
                    '\\' => value.push('\\'),
                    '/' => value.push('/'),
                    'b' => value.push('\u{0008}'),
                    'f' => value.push('\u{000c}'),
                    'n' => value.push('\n'),
                    'r' => value.push('\r'),
                    't' => value.push('\t'),
                    'u' => {
                        return Err(unsupported_sql_error(
                            "JSON unicode escape decoding is not admitted in this scoped runtime slice",
                        ));
                    }
                    _ => {
                        return Err(unsupported_sql_error(
                            "JSON string contains an unsupported escape sequence",
                        ));
                    }
                }
            }
            value_char => value.push(value_char),
        }
    }
    Err(unsupported_sql_error("JSON string is not closed"))
}

fn skip_json_ws(chars: &[char], mut index: usize) -> usize {
    while chars.get(index).is_some_and(|ch| ch.is_whitespace()) {
        index += 1;
    }
    index
}

fn normalize_local_output_path(value: &str) -> Result<PathBuf, ShardLoomError> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(ShardLoomError::InvalidOperation(
            "SQL local-source output path must not be empty".to_string(),
        ));
    }
    if trimmed.contains("://") && !trimmed.starts_with("file://") {
        return Err(ShardLoomError::InvalidOperation(
            "scoped SQL local-source smokes support local file output only; object-store and remote URI writes remain blocked".to_string(),
        ));
    }
    let local = if let Some(rest) = trimmed.strip_prefix("file://") {
        local_path_from_file_uri(rest)?
    } else {
        trimmed.to_string()
    };
    if local.trim().is_empty() {
        return Err(ShardLoomError::InvalidOperation(
            "file:// SQL local-source output path must include a local path".to_string(),
        ));
    }
    Ok(Path::new(&local).to_path_buf())
}

fn normalize_local_vortex_ingest_target_path(value: &str) -> Result<PathBuf, ShardLoomError> {
    let path = normalize_local_output_path(value)?;
    let extension = path
        .extension()
        .and_then(|value| value.to_str())
        .unwrap_or_default();
    if !extension.eq_ignore_ascii_case("vortex") {
        return Err(ShardLoomError::InvalidOperation(
            "vortex_ingest smoke writes local .vortex targets only; object-store, table, and non-Vortex sinks remain blocked"
                .to_string(),
        ));
    }
    Ok(path)
}

fn validate_sql_vortex_output_plan(path: &Path) -> Result<(), ShardLoomError> {
    if !shardloom_vortex::vortex_ingest_write_feature_enabled() {
        return Err(unsupported_sql_error(
            "local Vortex SQL output runtime requires building shardloom-cli with --features vortex-write; default builds expose Vortex as a deterministic blocked sink",
        ));
    }
    let extension = path
        .extension()
        .and_then(|value| value.to_str())
        .unwrap_or_default();
    if !extension.eq_ignore_ascii_case("vortex") {
        return Err(ShardLoomError::InvalidOperation(
            "local Vortex SQL output writes local .vortex targets only; object-store, table, and non-Vortex sinks remain blocked"
                .to_string(),
        ));
    }
    Ok(())
}

fn strip_utf8_bom_from_first_header_cell(header: &mut [String]) {
    if let Some(first) = header.first_mut() {
        if let Some(stripped) = first.strip_prefix('\u{feff}') {
            *first = stripped.to_string();
        }
    }
}

fn local_path_from_file_uri(rest: &str) -> Result<String, ShardLoomError> {
    if rest.is_empty() {
        return Err(ShardLoomError::InvalidOperation(
            "file:// SQL local-source output path must include a local path".to_string(),
        ));
    }
    let local = if rest.starts_with('/') {
        rest.to_string()
    } else {
        let Some((authority, path)) = rest.split_once('/') else {
            return Err(ShardLoomError::InvalidOperation(
                "file:// SQL local-source output path must include a local path".to_string(),
            ));
        };
        if !authority.is_empty() && !authority.eq_ignore_ascii_case("localhost") {
            return Err(ShardLoomError::InvalidOperation(format!(
                "file:// SQL local-source output URI authority {authority:?} is not local; only empty authority or localhost is allowed"
            )));
        }
        format!("/{path}")
    };
    if cfg!(windows)
        && local.len() >= 3
        && local.as_bytes()[0] == b'/'
        && local.as_bytes()[2] == b':'
        && local.as_bytes()[1].is_ascii_alphabetic()
    {
        Ok(local[1..].to_string())
    } else {
        Ok(local)
    }
}

fn reject_remote_source_path(path: &Path) -> Result<(), ShardLoomError> {
    let value = path.to_string_lossy();
    if value.contains("://") || value.starts_with("s3:") || value.starts_with("gs:") {
        return Err(unsupported_sql_error(
            "SQL local-source smoke supports local CSV, JSONL/NDJSON, flat JSON, and feature-gated Parquet/Arrow IPC/Avro/ORC file paths only; object-store and remote URI reads remain blocked",
        ));
    }
    Ok(())
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

fn parse_sql_local_source_statement(raw: &str) -> Result<ParsedSqlLocalSource, ShardLoomError> {
    let statement = normalize_sql_statement(raw)?;
    if !statement
        .get(..6)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("select"))
    {
        return Err(unsupported_sql_error(
            "SQL local-source smoke admits SELECT statements only",
        ));
    }
    let from_index = find_keyword_outside_quotes(&statement, "from").ok_or_else(|| {
        unsupported_sql_error("SQL local-source smoke requires a FROM <local.csv> clause")
    })?;
    let limit_index = find_keyword_outside_quotes(&statement, "limit").ok_or_else(|| {
        unsupported_sql_error("SQL local-source smoke requires a LIMIT <n> clause")
    })?;
    let where_index = find_keyword_outside_quotes(&statement, "where");
    let group_by_index = find_keyword_outside_quotes(&statement, "group by");
    let order_by_index = find_keyword_outside_quotes(&statement, "order by");
    if !(from_index > 6 && limit_index > from_index)
        || where_index.is_some_and(|index| !(index > from_index && index < limit_index))
        || group_by_index.is_some_and(|index| !(index > from_index && index < limit_index))
        || order_by_index.is_some_and(|index| !(index > from_index && index < limit_index))
        || where_index
            .zip(group_by_index)
            .is_some_and(|(where_index, group_by)| where_index > group_by)
        || where_index
            .zip(order_by_index)
            .is_some_and(|(where_index, order_by)| where_index > order_by)
        || group_by_index
            .zip(order_by_index)
            .is_some_and(|(group_by, order_by)| group_by > order_by)
    {
        return Err(unsupported_sql_error(
            "SQL local-source smoke requires SELECT ... FROM ... [WHERE ...] [GROUP BY ...] [ORDER BY ...] LIMIT ... order",
        ));
    }

    let select_list = statement[6..from_index].trim();
    let source_end = earliest_clause_index_after(
        from_index,
        &[
            where_index,
            group_by_index,
            order_by_index,
            Some(limit_index),
        ],
    );
    let source_raw = statement[from_index + 4..source_end].trim();
    let predicate_raw = where_index.map(|index| {
        let end = earliest_clause_index_after(
            index,
            &[group_by_index, order_by_index, Some(limit_index)],
        );
        statement[index + 5..end].trim()
    });
    let group_by_raw = group_by_index.map(|index| {
        let end = order_by_index.unwrap_or(limit_index);
        statement[index + "group by".len()..end].trim()
    });
    let order_by_raw =
        order_by_index.map(|index| statement[index + "order by".len()..limit_index].trim());
    let limit_raw = statement[limit_index + 5..].trim();
    if select_list.is_empty()
        || source_raw.is_empty()
        || predicate_raw.is_some_and(str::is_empty)
        || limit_raw.is_empty()
    {
        return Err(unsupported_sql_error(
            "SQL local-source SELECT list, source, optional predicate, and limit must not be empty",
        ));
    }
    if contains_keyword_outside_quotes(limit_raw, "where")
        || contains_keyword_outside_quotes(limit_raw, "from")
        || contains_keyword_outside_quotes(limit_raw, "select")
        || contains_keyword_outside_quotes(limit_raw, "order by")
    {
        return Err(unsupported_sql_error(
            "SQL local-source smoke admits one flat SELECT without subqueries",
        ));
    }

    let projection_list = parse_projection_list(select_list)?;
    let group_by = parse_group_by_list(group_by_raw)?;
    let order_by = parse_order_by(order_by_raw)?;
    let source_clause = parse_source_clause(source_raw)?;
    let predicate = predicate_raw.map_or(Ok(ParsedPredicate::All), parse_predicate)?;
    let limit = parse_limit(limit_raw)?;

    Ok(parsed_sql_local_source_from_parts(
        statement,
        projection_list,
        group_by,
        order_by,
        source_clause,
        predicate,
        limit,
    ))
}

fn parsed_sql_local_source_from_parts(
    statement: String,
    projection_list: ParsedProjectionList,
    group_by: Vec<String>,
    order_by: Option<ParsedOrderBy>,
    source_clause: ParsedSourceClause,
    predicate: ParsedPredicate,
    limit: usize,
) -> ParsedSqlLocalSource {
    ParsedSqlLocalSource {
        projection_order: projection_list.projection_order,
        projections: projection_list.projections,
        literal_projections: projection_list.literal_projections,
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
        date_extract_projections: projection_list.date_extract_projections,
        timestamp_extract_projections: projection_list.timestamp_extract_projections,
        aggregates: projection_list.aggregates,
        group_by,
        order_by,
        source_path: source_clause.source_path,
        source_alias: source_clause.source_alias,
        join: source_clause.join,
        predicate,
        limit,
        normalized_statement: statement,
    }
}

fn normalize_sql_statement(raw: &str) -> Result<String, ShardLoomError> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(unsupported_sql_error("SQL statement must not be empty"));
    }
    if trimmed.matches(';').count() > 1 {
        return Err(unsupported_sql_error(
            "SQL local-source smoke admits a single statement only",
        ));
    }
    let statement = trimmed.strip_suffix(';').unwrap_or(trimmed).trim();
    if statement.is_empty() {
        return Err(unsupported_sql_error("SQL statement must not be empty"));
    }
    Ok(statement.to_string())
}

#[allow(clippy::too_many_lines)]
fn parse_projection_list(raw: &str) -> Result<ParsedProjectionList, ShardLoomError> {
    let entries = split_sql_csv(raw)?;
    if entries.is_empty() {
        return Err(unsupported_sql_error("SELECT list must not be empty"));
    }
    let mut projection_order = Vec::with_capacity(entries.len());
    let mut projections = Vec::with_capacity(entries.len());
    let mut literal_projections = Vec::new();
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
    let mut date_extract_projections = Vec::new();
    let mut timestamp_extract_projections = Vec::new();
    let mut aggregates = Vec::new();
    for projection in entries {
        let projection = projection.trim();
        if projection == "*" {
            projection_order.push(ParsedProjectionOutput::Raw("*".to_string()));
            projections.push("*".to_string());
        } else if let Some(aggregate) = parse_aggregate_projection(projection)? {
            aggregates.push(aggregate);
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
                "SELECT * can be mixed only with computed or literal projections in this scoped smoke",
            ));
        }
    }
    Ok(ParsedProjectionList {
        projection_order,
        projections,
        literal_projections,
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
        date_extract_projections,
        timestamp_extract_projections,
        aggregates,
    })
}

fn parse_literal_projection(raw: &str) -> Result<Option<ParsedLiteralProjection>, ShardLoomError> {
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
    let value = parse_projection_literal_value(literal_raw)?;
    if matches!(value, ScalarValue::Null) {
        return Err(unsupported_sql_error(
            "literal projections do not admit NULL values in this scoped runtime slice",
        ));
    }
    Ok(Some(ParsedLiteralProjection {
        alias: alias.to_string(),
        value,
    }))
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
    if matches!(
        (op, &rhs),
        (
            NumericArithmeticOp::Divide,
            ScalarValue::Int64(0) | ScalarValue::Float64(0.0)
        )
    ) {
        return Err(unsupported_sql_error(
            "numeric arithmetic projection division by zero is not admitted",
        ));
    }
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
    validate_sql_column_ref(column)?;
    Ok(Some(ParsedCastProjection {
        alias: alias.to_string(),
        column: column.to_string(),
        target_dtype: parse_cast_target_dtype(target_raw)?,
        mode,
    }))
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
    let then_branch = parse_conditional_projection_branch(then_raw, "THEN")?;
    let else_branch = parse_conditional_projection_branch(else_raw, "ELSE")?;
    let then_dtype = then_branch.literal_dtype();
    let else_dtype = else_branch.literal_dtype();
    if let (Some(then_dtype), Some(else_dtype)) = (&then_dtype, &else_dtype) {
        if then_dtype != else_dtype {
            return Err(unsupported_sql_error(&format!(
                "CASE projection THEN/ELSE branches must have matching dtypes; got {} and {}",
                then_dtype.as_str(),
                else_dtype.as_str()
            )));
        }
    }
    Ok(Some(ParsedConditionalProjection {
        alias: alias.to_string(),
        predicate: parse_predicate(predicate_raw)?,
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
        validate_sql_column_ref(raw)?;
        Ok(ParsedConditionalBranch::Column(raw.to_string()))
    }
}

fn resolve_conditional_projection_branch_dtypes(
    parsed: &mut ParsedSqlLocalSource,
    source: &CsvSourceData,
) -> Result<(), ShardLoomError> {
    let has_source_column_branch = parsed.conditional_projections.iter().any(|projection| {
        projection.then_branch.source_column().is_some()
            || projection.else_branch.source_column().is_some()
    });
    if parsed.is_join() && has_source_column_branch {
        return Err(unsupported_sql_error(
            "CASE projection source-column branches are not admitted for JOIN projections in this scoped runtime slice",
        ));
    }
    for projection in &mut parsed.conditional_projections {
        let then_dtype = resolve_conditional_projection_branch_dtype(
            &projection.then_branch,
            source,
            "THEN",
            &projection.alias,
        )?;
        let else_dtype = resolve_conditional_projection_branch_dtype(
            &projection.else_branch,
            source,
            "ELSE",
            &projection.alias,
        )?;
        if then_dtype != else_dtype {
            return Err(unsupported_sql_error(&format!(
                "CASE projection {:?} THEN/ELSE branches must have matching dtypes after source-column binding; got {} and {}",
                projection.alias,
                then_dtype.as_str(),
                else_dtype.as_str()
            )));
        }
        projection.then_dtype = Some(then_dtype);
        projection.else_dtype = Some(else_dtype);
    }
    Ok(())
}

fn resolve_conditional_projection_branch_dtype(
    branch: &ParsedConditionalBranch,
    source: &CsvSourceData,
    branch_label: &str,
    alias: &str,
) -> Result<LogicalDType, ShardLoomError> {
    match branch {
        ParsedConditionalBranch::Literal(value) => Ok(value.dtype()),
        ParsedConditionalBranch::Column(column) => {
            infer_source_column_dtype(source, column, branch_label, alias)
        }
    }
}

fn infer_source_column_dtype(
    source: &CsvSourceData,
    column: &str,
    branch_label: &str,
    alias: &str,
) -> Result<LogicalDType, ShardLoomError> {
    let mut dtype: Option<LogicalDType> = None;
    for row in &source.rows {
        let Some(value) = row.get(column) else {
            continue;
        };
        if matches!(value, ScalarValue::Null) {
            continue;
        }
        let value_dtype = value.dtype();
        match dtype.as_ref() {
            Some(existing) if existing != &value_dtype => {
                return Err(unsupported_sql_error(&format!(
                    "CASE projection {alias:?} {branch_label} branch column {column:?} has mixed non-NULL dtypes {} and {}",
                    existing.as_str(),
                    value_dtype.as_str()
                )));
            }
            Some(_) => {}
            None => dtype = Some(value_dtype),
        }
    }
    dtype.ok_or_else(|| {
        unsupported_sql_error(&format!(
            "CASE projection {alias:?} {branch_label} branch column {column:?} has no non-NULL values to infer a stable dtype"
        ))
    })
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
    Ok(Some(ParsedPredicateProjection {
        alias: alias.to_string(),
        predicate: parse_predicate(expression_raw)?,
    }))
}

fn is_explicit_predicate_projection_shape(raw: &str) -> Result<bool, ShardLoomError> {
    let tokens = split_whitespace_outside_quotes(raw)?;
    if tokens.len() <= 1 {
        return Ok(false);
    }
    Ok(tokens.iter().any(|token| {
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
                | "between"
                | "and"
                | "or"
        )
    }))
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
    let contains_temporal_difference =
        expression_contains_temporal_difference_call(expression_raw)?;
    if is_simple_numeric_arithmetic_projection_shape(expression_raw)?
        || (!expression_contains_numeric_operator(expression_raw)? && !contains_temporal_difference)
    {
        return Ok(None);
    }
    validate_sql_identifier(alias)?;
    let expression =
        parse_numeric_scalar_expression(expression_raw, &format!("project.generic.{alias}"))?;
    let source_columns = expression_source_columns(&expression);
    if source_columns.is_empty() {
        return Err(unsupported_sql_error(
            "generic expression projections require at least one source column",
        ));
    }
    let operator_families = expression_operator_families(&expression);
    let binary_operator_count = expression_binary_operator_count(&expression);
    if binary_operator_count == 0 && !expression_has_temporal_difference(&expression) {
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

fn parse_numeric_scalar_expression(
    raw: &str,
    id_prefix: &str,
) -> Result<Expression, ShardLoomError> {
    let trimmed = trim_enclosing_scalar_expression_parentheses(raw)?;
    if let Some((index, op)) = find_top_level_numeric_operator(trimmed, &['+', '-'])? {
        return numeric_binary_expression(trimmed, id_prefix, index, op);
    }
    if let Some((index, op)) = find_top_level_numeric_operator(trimmed, &['*', '/'])? {
        return numeric_binary_expression(trimmed, id_prefix, index, op);
    }
    if let Some(expression) = parse_numeric_cast_expression(trimmed, id_prefix)? {
        return Ok(expression);
    }
    if let Some(expression) = parse_temporal_difference_function_expression(trimmed, id_prefix)? {
        return Ok(expression);
    }
    if let Some(expression) = parse_numeric_function_expression(trimmed, id_prefix)? {
        return Ok(expression);
    }
    if let Ok(value) = parse_numeric_arithmetic_literal(trimmed) {
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

fn numeric_binary_expression(
    raw: &str,
    id_prefix: &str,
    op_index: usize,
    op_char: char,
) -> Result<Expression, ShardLoomError> {
    let left_raw = raw[..op_index].trim();
    let right_raw = raw[op_index + op_char.len_utf8()..].trim();
    if left_raw.is_empty() || right_raw.is_empty() {
        return Err(unsupported_sql_error(
            "generic numeric expressions require operands on both sides of an arithmetic operator",
        ));
    }
    if op_char == '/'
        && matches!(
            parse_numeric_arithmetic_literal(right_raw),
            Ok(ScalarValue::Int64(0) | ScalarValue::Float64(0.0))
        )
    {
        return Err(unsupported_sql_error(
            "generic numeric expression division by zero is not admitted",
        ));
    }
    Ok(Expression::new(
        ExprId::new(format!("{id_prefix}.binary"))?,
        ExpressionKind::Binary {
            left: Box::new(parse_numeric_scalar_expression(
                left_raw,
                &format!("{id_prefix}.left"),
            )?),
            op: numeric_binary_op_from_char(op_char)?,
            right: Box::new(parse_numeric_scalar_expression(
                right_raw,
                &format!("{id_prefix}.right"),
            )?),
        },
    ))
}

fn numeric_binary_op_from_char(op: char) -> Result<BinaryOp, ShardLoomError> {
    match op {
        '+' => Ok(BinaryOp::Add),
        '-' => Ok(BinaryOp::Subtract),
        '*' => Ok(BinaryOp::Multiply),
        '/' => Ok(BinaryOp::Divide),
        _ => Err(unsupported_sql_error(
            "generic numeric expressions admit +, -, *, and / operators only",
        )),
    }
}

fn parse_numeric_cast_expression(
    raw: &str,
    id_prefix: &str,
) -> Result<Option<Expression>, ShardLoomError> {
    if !raw
        .get(..5)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("cast("))
    {
        return Ok(None);
    }
    let close_index = matching_closing_parenthesis(raw, 4)?.ok_or_else(|| {
        unsupported_sql_error(
            "generic numeric CAST expressions must use CAST(<expr> AS int64|float64)",
        )
    })?;
    if !raw[close_index + 1..].trim().is_empty() {
        return Err(unsupported_sql_error(
            "generic numeric CAST projections must be a single CAST expression",
        ));
    }
    let inner = raw[5..close_index].trim();
    let as_index = find_keyword_outside_quotes_and_parentheses(inner, "as")?.ok_or_else(|| {
        unsupported_sql_error("generic numeric CAST expressions must use CAST(<expr> AS <dtype>)")
    })?;
    let source_raw = inner[..as_index].trim();
    let target_raw = inner[as_index + "as".len()..].trim();
    let target_dtype = parse_cast_target_dtype(target_raw)?;
    if !matches!(target_dtype, LogicalDType::Int64 | LogicalDType::Float64) {
        return Err(unsupported_sql_error(
            "generic numeric CAST projections currently admit int64 and float64 targets only",
        ));
    }
    Ok(Some(Expression::cast(
        ExprId::new(format!("{id_prefix}.cast"))?,
        parse_numeric_scalar_expression(source_raw, &format!("{id_prefix}.cast.source"))?,
        target_dtype,
    )))
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

fn parse_temporal_difference_function_expression(
    raw: &str,
    id_prefix: &str,
) -> Result<Option<Expression>, ShardLoomError> {
    let Some((function_name, target_dtype)) = temporal_difference_function_prefix(raw) else {
        return Ok(None);
    };
    let open_index = function_name.len();
    let close_index = matching_closing_parenthesis(raw, open_index)?.ok_or_else(|| {
        unsupported_sql_error(
            "temporal difference expressions must use DATE_DIFF_DAYS(left, right) or TIMESTAMP_DIFF_SECONDS(left, right)",
        )
    })?;
    if !raw[close_index + 1..].trim().is_empty() {
        return Err(unsupported_sql_error(
            "temporal difference expressions must be a single function expression",
        ));
    }
    let args = split_sql_csv(raw[open_index + 1..close_index].trim())?;
    let [left_raw, right_raw] = args.as_slice() else {
        return Err(unsupported_sql_error(
            "temporal difference expressions require exactly two arguments",
        ));
    };
    let left = parse_temporal_difference_arg_expression(
        left_raw,
        &format!("{id_prefix}.{function_name}.left"),
        &target_dtype,
    )?;
    let right = parse_temporal_difference_arg_expression(
        right_raw,
        &format!("{id_prefix}.{function_name}.right"),
        &target_dtype,
    )?;
    Ok(Some(Expression::new(
        ExprId::new(format!("{id_prefix}.{function_name}"))?,
        ExpressionKind::FunctionCall {
            name: function_name.to_string(),
            args: vec![left, right],
        },
    )))
}

fn parse_temporal_difference_arg_expression(
    raw: &str,
    id_prefix: &str,
    target_dtype: &LogicalDType,
) -> Result<Expression, ShardLoomError> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(unsupported_sql_error(
            "temporal difference arguments must not be empty",
        ));
    }
    if let Some(expression) = parse_temporal_difference_cast_arg(trimmed, id_prefix, target_dtype)?
    {
        return Ok(expression);
    }
    if let Ok(value) = parse_projection_literal_value(trimmed) {
        if matches!(value, ScalarValue::Null) || &value.dtype() == target_dtype {
            return Ok(Expression::literal(
                ExprId::new(format!("{id_prefix}.literal"))?,
                value,
            ));
        }
        return Err(unsupported_sql_error(&format!(
            "temporal difference literals for {} arguments must match the function dtype, got {}",
            target_dtype.as_str(),
            value.dtype().as_str()
        )));
    }
    validate_sql_column_ref(trimmed)?;
    Ok(Expression::column(
        ExprId::new(format!("{id_prefix}.{trimmed}"))?,
        ColumnRef::new(trimmed.to_string())?,
    ))
}

fn parse_temporal_difference_cast_arg(
    raw: &str,
    id_prefix: &str,
    target_dtype: &LogicalDType,
) -> Result<Option<Expression>, ShardLoomError> {
    if !raw
        .get(..5)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("cast("))
    {
        return Ok(None);
    }
    let close_index = matching_closing_parenthesis(raw, 4)?.ok_or_else(|| {
        unsupported_sql_error(
            "temporal difference CAST arguments must use CAST(<column> AS date32|timestamp_micros)",
        )
    })?;
    if !raw[close_index + 1..].trim().is_empty() {
        return Err(unsupported_sql_error(
            "temporal difference CAST arguments must be a single CAST expression",
        ));
    }
    let inner = raw[5..close_index].trim();
    let as_index = find_keyword_outside_quotes(inner, "as").ok_or_else(|| {
        unsupported_sql_error("temporal difference CAST arguments must use CAST(<column> AS dtype)")
    })?;
    let column = inner[..as_index].trim();
    let target_raw = inner[as_index + "as".len()..].trim();
    validate_sql_column_ref(column)?;
    let parsed_target = parse_cast_target_dtype(target_raw)?;
    if parsed_target != *target_dtype {
        return Err(unsupported_sql_error(&format!(
            "temporal difference CAST arguments must target {}, got {}",
            target_dtype.as_str(),
            parsed_target.as_str()
        )));
    }
    Ok(Some(Expression::cast(
        ExprId::new(format!("{id_prefix}.cast"))?,
        Expression::column(
            ExprId::new(format!("{id_prefix}.{column}"))?,
            ColumnRef::new(column.to_string())?,
        ),
        parsed_target,
    )))
}

fn parse_numeric_function_expression(
    raw: &str,
    id_prefix: &str,
) -> Result<Option<Expression>, ShardLoomError> {
    let Some(open_index) = raw.find('(') else {
        return Ok(None);
    };
    let function_raw = raw[..open_index].trim();
    let function_name = match function_raw.to_ascii_lowercase().as_str() {
        "abs" => "abs",
        "floor" => "floor",
        "ceil" | "ceiling" => "ceil",
        "round" => "round",
        _ => return Ok(None),
    };
    let close_index = matching_closing_parenthesis(raw, open_index)?.ok_or_else(|| {
        unsupported_sql_error(
            "generic numeric function projections must use ABS/FLOOR/CEIL/ROUND(<expr>)",
        )
    })?;
    if !raw[close_index + 1..].trim().is_empty() {
        return Err(unsupported_sql_error(
            "generic numeric function projections must be a single function expression",
        ));
    }
    let inner = raw[open_index + 1..close_index].trim();
    let args = split_sql_csv(inner)?;
    let [arg] = args.as_slice() else {
        return Err(unsupported_sql_error(
            "generic numeric function projections require exactly one expression argument",
        ));
    };
    Ok(Some(Expression::new(
        ExprId::new(format!("{id_prefix}.{function_name}"))?,
        ExpressionKind::FunctionCall {
            name: function_name.to_string(),
            args: vec![parse_numeric_scalar_expression(
                arg,
                &format!("{id_prefix}.{function_name}.arg"),
            )?],
        },
    )))
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
            '+' | '-' | '*' | '/' if !is_unary_numeric_sign(raw, index, ch) => return Ok(true),
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
        before.is_none_or(|candidate| matches!(candidate, '(' | ',' | '+' | '-' | '*' | '/'));
    sign_position && after.is_some_and(|candidate| candidate.is_ascii_digit() || candidate == '.')
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

fn expression_temporal_difference_date_source_columns(expression: &Expression) -> Vec<String> {
    expression_temporal_difference_source_columns(expression, "date_diff_days")
}

fn expression_temporal_difference_timestamp_source_columns(expression: &Expression) -> Vec<String> {
    expression_temporal_difference_source_columns(expression, "timestamp_diff_seconds")
}

fn expression_temporal_difference_source_columns(
    expression: &Expression,
    function_name: &str,
) -> Vec<String> {
    let mut columns = BTreeSet::new();
    collect_expression_temporal_difference_source_columns(expression, function_name, &mut columns);
    columns.into_iter().collect()
}

fn collect_expression_temporal_difference_source_columns(
    expression: &Expression,
    function_name: &str,
    columns: &mut BTreeSet<String>,
) {
    match &expression.kind {
        ExpressionKind::FunctionCall { name, args } if name.eq_ignore_ascii_case(function_name) => {
            for arg in args {
                collect_expression_source_columns(arg, columns);
            }
        }
        ExpressionKind::FunctionCall { args, .. } => {
            for arg in args {
                collect_expression_temporal_difference_source_columns(arg, function_name, columns);
            }
        }
        ExpressionKind::Alias { expr, .. }
        | ExpressionKind::Cast { expr, .. }
        | ExpressionKind::TryCast { expr, .. }
        | ExpressionKind::Unary { expr, .. } => {
            collect_expression_temporal_difference_source_columns(expr, function_name, columns);
        }
        ExpressionKind::Binary { left, right, .. }
        | ExpressionKind::Compare { left, right, .. } => {
            collect_expression_temporal_difference_source_columns(left, function_name, columns);
            collect_expression_temporal_difference_source_columns(right, function_name, columns);
        }
        ExpressionKind::Literal(_)
        | ExpressionKind::Column(_)
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
                    BinaryOp::Add | BinaryOp::Subtract | BinaryOp::Multiply | BinaryOp::Divide => {
                        "numeric_binary"
                    }
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

fn parse_string_transform_projection(
    raw: &str,
) -> Result<Option<ParsedStringTransformProjection>, ShardLoomError> {
    let Some(as_index) = find_keyword_outside_quotes(raw, "as") else {
        return Ok(None);
    };
    let expression_raw = raw[..as_index].trim();
    let alias = raw[as_index + "as".len()..].trim();
    let Some(open_index) = expression_raw.find('(') else {
        return Ok(None);
    };
    let function_raw = expression_raw[..open_index].trim();
    let op = match function_raw.to_ascii_lowercase().as_str() {
        "lower" => StringTransformOp::Lower,
        "upper" => StringTransformOp::Upper,
        "trim" => StringTransformOp::Trim,
        _ => return Ok(None),
    };
    if expression_raw.is_empty() || alias.is_empty() || !expression_raw.ends_with(')') {
        return Err(unsupported_sql_error(
            "string transform projections must be written as LOWER|UPPER|TRIM(<column>) AS <column>",
        ));
    }
    validate_sql_identifier(alias)?;
    let argument = expression_raw[open_index + 1..expression_raw.len() - 1].trim();
    if argument.is_empty() {
        return Err(unsupported_sql_error(
            "string transform projections require one source column argument",
        ));
    }
    validate_sql_column_ref(argument)?;
    Ok(Some(ParsedStringTransformProjection {
        alias: alias.to_string(),
        column: argument.to_string(),
        op,
    }))
}

fn parse_string_length_projection(
    raw: &str,
) -> Result<Option<ParsedStringLengthProjection>, ShardLoomError> {
    let Some(as_index) = find_keyword_outside_quotes(raw, "as") else {
        return Ok(None);
    };
    let expression_raw = raw[..as_index].trim();
    let alias = raw[as_index + "as".len()..].trim();
    let Some(open_index) = expression_raw.find('(') else {
        return Ok(None);
    };
    let function_raw = expression_raw[..open_index].trim();
    if !function_raw.eq_ignore_ascii_case("length") {
        return Ok(None);
    }
    if expression_raw.is_empty() || alias.is_empty() || !expression_raw.ends_with(')') {
        return Err(unsupported_sql_error(
            "string length projections must be written as LENGTH(<column>) AS <column>",
        ));
    }
    validate_sql_identifier(alias)?;
    let argument = expression_raw[open_index + 1..expression_raw.len() - 1].trim();
    if argument.is_empty() {
        return Err(unsupported_sql_error(
            "string length projections require one source column argument",
        ));
    }
    validate_sql_column_ref(argument)?;
    Ok(Some(ParsedStringLengthProjection {
        alias: alias.to_string(),
        column: argument.to_string(),
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
    if parsed_args.source_columns.is_empty() {
        return Err(unsupported_sql_error(
            "string function expressions require at least one source column argument",
        ));
    }
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
        let (expression, source_column, arg_literal_count) =
            parse_string_function_text_arg(arg, id_prefix, index)?;
        push_unique_string_function_source_column(&mut source_columns, source_column);
        literal_count += arg_literal_count;
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
    let (value_expression, source_column, arg_literal_count) =
        parse_string_function_text_arg(value_raw, id_prefix, 0)?;
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
    push_unique_string_function_source_column(&mut source_columns, source_column);
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
        literal_count: arg_literal_count + 2,
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
    let (value_expression, source_column, arg_literal_count) =
        parse_string_function_text_arg(value_raw, id_prefix, 0)?;
    let count = parse_string_function_int_literal(count_raw, "left/right count")?;
    if count < 0 {
        return Err(unsupported_sql_error(
            "LEFT/RIGHT string function expressions require a non-negative count",
        ));
    }
    let mut source_columns = Vec::new();
    push_unique_string_function_source_column(&mut source_columns, source_column);
    Ok(ParsedStringFunctionArgs {
        expression_args: vec![
            value_expression,
            Expression::literal(
                ExprId::new(format!("{id_prefix}.count"))?,
                ScalarValue::Int64(count),
            ),
        ],
        source_columns,
        literal_count: arg_literal_count + 1,
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
    let (value_expression, source_column, arg_literal_count) =
        parse_string_function_text_arg(value_raw, id_prefix, 0)?;
    let needle = parse_sql_string_literal(needle_raw)?;
    if needle.is_empty() {
        return Err(unsupported_sql_error(
            "REPLACE string function expressions require a non-empty search literal",
        ));
    }
    let replacement = parse_sql_string_literal(replacement_raw)?;
    let mut source_columns = Vec::new();
    push_unique_string_function_source_column(&mut source_columns, source_column);
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
        literal_count: arg_literal_count + 2,
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

fn parse_string_function_text_arg(
    raw: &str,
    id_prefix: &str,
    index: usize,
) -> Result<(Expression, Option<String>, usize), ShardLoomError> {
    let trimmed = raw.trim();
    if trimmed.starts_with('\'') {
        return Ok((
            Expression::literal(
                ExprId::new(format!("{id_prefix}.arg{index}"))?,
                ScalarValue::Utf8(parse_sql_string_literal(trimmed)?),
            ),
            None,
            1,
        ));
    }
    validate_sql_column_ref(trimmed)?;
    Ok((
        Expression::column(
            ExprId::new(format!("{id_prefix}.arg{index}.{trimmed}"))?,
            ColumnRef::new(trimmed.to_string())?,
        ),
        Some(trimmed.to_string()),
        0,
    ))
}

fn parse_string_function_int_literal(raw: &str, label: &str) -> Result<i64, ShardLoomError> {
    match parse_sql_literal(raw)? {
        ScalarValue::Int64(value) => Ok(value),
        _ => Err(unsupported_sql_error(&format!(
            "{label} must be an int64 literal"
        ))),
    }
}

fn push_unique_string_function_source_column(columns: &mut Vec<String>, column: Option<String>) {
    if let Some(column) = column {
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
            "aggregate expressions must be written as function(column) or function(column) AS alias in this scoped smoke",
        ));
    }
    let argument = expression_raw[open_index + 1..expression_raw.len() - 1].trim();
    if argument.is_empty() {
        return Err(unsupported_sql_error(
            "aggregate expressions require a column or COUNT(*) argument",
        ));
    }
    let (distinct, argument) = if let Some(argument) = strip_leading_keyword(argument, "distinct")?
    {
        if function != AggregateFunction::Count {
            return Err(unsupported_sql_error(
                "DISTINCT aggregate runtime currently admits COUNT(DISTINCT <column>) only",
            ));
        }
        let argument = argument.trim();
        if argument.is_empty() {
            return Err(unsupported_sql_error(
                "COUNT(DISTINCT ...) requires one source column",
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
            column: None,
            alias,
            distinct,
        }));
    }
    validate_sql_column_ref(argument)?;
    Ok(Some(ParsedAggregate {
        function,
        column: Some(argument.to_string()),
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
        let (column, direction) = match tokens.as_slice() {
            [column] => (column, SortDirection::Asc),
            [column, direction] if direction.eq_ignore_ascii_case("asc") => {
                (column, SortDirection::Asc)
            }
            [column, direction] if direction.eq_ignore_ascii_case("desc") => {
                (column, SortDirection::Desc)
            }
            [_, direction] if direction.eq_ignore_ascii_case("nulls") => {
                return Err(unsupported_sql_error(
                    "ORDER BY NULLS FIRST/LAST is not admitted in this scoped top-N smoke",
                ));
            }
            _ => {
                return Err(unsupported_sql_error(
                    "ORDER BY top-N smoke admits <column> [ASC|DESC] keys only",
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
        });
    }
    Ok(Some(ParsedOrderBy { keys }))
}

fn parse_source_clause(raw: &str) -> Result<ParsedSourceClause, ShardLoomError> {
    let Some((join_index, join_keyword_len)) = find_keyword_outside_quotes(raw, "inner join")
        .map(|index| (index, "inner join".len()))
        .or_else(|| find_keyword_outside_quotes(raw, "join").map(|index| (index, "join".len())))
    else {
        return Ok(ParsedSourceClause {
            source_path: parse_source_path(raw)?,
            source_alias: None,
            join: None,
        });
    };
    if contains_keyword_outside_quotes(raw, "left join")
        || contains_keyword_outside_quotes(raw, "right join")
        || contains_keyword_outside_quotes(raw, "full join")
        || contains_keyword_outside_quotes(raw, "cross join")
        || contains_keyword_outside_quotes(raw, "outer join")
        || contains_keyword_outside_quotes(raw, "semi join")
        || contains_keyword_outside_quotes(raw, "anti join")
    {
        return Err(unsupported_sql_error(
            "JOIN smoke admits explicit INNER-style equi-join only; outer/cross/semi/anti joins remain blocked",
        ));
    }
    let left_raw = raw[..join_index].trim();
    let join_tail = raw[join_index + join_keyword_len..].trim();
    let on_index = find_keyword_outside_quotes(join_tail, "on").ok_or_else(|| {
        unsupported_sql_error("JOIN smoke requires an ON <left> = <right> clause")
    })?;
    let right_raw = join_tail[..on_index].trim();
    let on_raw = join_tail[on_index + "on".len()..].trim();
    let (source_path, left_alias) = parse_aliased_source(left_raw, "left")?;
    let (right_source_path, right_alias) = parse_aliased_source(right_raw, "right")?;
    let _left_format = LocalSourceFormat::from_path(&source_path)?;
    let _right_format = LocalSourceFormat::from_path(&right_source_path)?;
    if left_alias == right_alias {
        return Err(unsupported_sql_error(
            "JOIN smoke requires distinct left and right aliases",
        ));
    }
    let key_pairs = parse_join_on(on_raw)?;
    if key_pairs
        .iter()
        .any(|pair| pair.left.alias != left_alias || pair.right.alias != right_alias)
    {
        return Err(unsupported_sql_error(
            "JOIN ON predicates must be ordered as <left_alias>.<column> = <right_alias>.<column>",
        ));
    }
    Ok(ParsedSourceClause {
        source_path,
        source_alias: Some(left_alias),
        join: Some(ParsedJoin {
            right_source_path,
            right_alias,
            key_pairs,
        }),
    })
}

fn parse_aliased_source(raw: &str, side: &str) -> Result<(PathBuf, String), ShardLoomError> {
    let tokens = split_whitespace_outside_quotes(raw)?;
    let [path_raw, as_keyword, alias] = tokens.as_slice() else {
        return Err(unsupported_sql_error(&format!(
            "JOIN smoke requires {side} source syntax <local-source> AS <alias>"
        )));
    };
    if !as_keyword.eq_ignore_ascii_case("as") {
        return Err(unsupported_sql_error(&format!(
            "JOIN smoke requires {side} source alias with AS"
        )));
    }
    validate_sql_identifier(alias)?;
    Ok((parse_source_path(path_raw)?, alias.clone()))
}

fn parse_join_on(raw: &str) -> Result<Vec<ParsedJoinKeyPair>, ShardLoomError> {
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
    let _format = LocalSourceFormat::from_path(&path)?;
    Ok(path)
}

fn parse_predicate(raw: &str) -> Result<ParsedPredicate, ShardLoomError> {
    let raw = trim_enclosing_predicate_parentheses(raw)?;
    if let Some(predicate) = parse_logical_predicate(raw)? {
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
    if let Some(predicate) = parse_in_list_predicate(raw)? {
        return Ok(predicate);
    }
    parse_token_predicate(raw)
}

fn parse_token_predicate(raw: &str) -> Result<ParsedPredicate, ShardLoomError> {
    let tokens = split_whitespace_outside_quotes(raw)?;
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
            validate_sql_column_ref(column)?;
            if op_raw.eq_ignore_ascii_case("like") {
                let pattern = parse_sql_string_literal(literal_raw)?;
                let (op, value) = parse_like_string_predicate(&pattern)?;
                Ok(ParsedPredicate::StringMatch {
                    column: (*column).clone(),
                    op,
                    value,
                })
            } else {
                let op = parse_comparison_op(op_raw)?;
                let value = parse_sql_literal(literal_raw)?;
                Ok(ParsedPredicate::Compare {
                    column: (*column).clone(),
                    op,
                    value,
                })
            }
        }
        [column, not_keyword, like_keyword, literal_raw]
            if not_keyword.eq_ignore_ascii_case("not")
                && like_keyword.eq_ignore_ascii_case("like") =>
        {
            validate_sql_column_ref(column)?;
            let pattern = parse_sql_string_literal(literal_raw)?;
            let (op, value) = parse_like_string_predicate(&pattern)?;
            Ok(ParsedPredicate::Not {
                inner: Box::new(ParsedPredicate::StringMatch {
                    column: (*column).clone(),
                    op,
                    value,
                }),
            })
        }
        _ => Err(unsupported_sql_error(
            "WHERE admits only <column>, <column> IS [NOT] TRUE/FALSE, <column> <op> <literal>, <column> <op> DATE <date-literal>, <column> <op> TIMESTAMP <timestamp-literal>, <column> [NOT] BETWEEN <literal> AND <literal>, <column> (+|-|*|/) <numeric-literal> <op> <numeric-literal>, generalized numeric expression trees or temporal differences <op> numeric expression/literal, ABS/FLOOR/CEIL/ROUND(<column>) <op> <numeric-literal>, LENGTH(<column>) <op> <int-literal>, CONCAT/SUBSTR/SUBSTRING/LEFT/RIGHT/REPLACE string function expressions <op> <string-literal>, DATE_YEAR/MONTH/DAY(<column>) <op> <int-literal>, TIMESTAMP_YEAR/MONTH/DAY/HOUR/MINUTE/SECOND(<column>) <op> <int-literal>, DATE_ADD_DAYS(<column>, <days>) <op> DATE <date-literal>, DATE_SUB_DAYS(<column>, <days>) <op> DATE <date-literal>, TIMESTAMP_ADD_SECONDS(<column>, <seconds>) <op> TIMESTAMP <timestamp-literal>, TIMESTAMP_SUB_SECONDS(<column>, <seconds>) <op> TIMESTAMP <timestamp-literal>, LOWER/UPPER/TRIM(<column>) <op> <string-literal>, <column> [NOT] IN (<literal>,...), <column> [NOT] LIKE <string-pattern>, <column> IS NULL, <column> IS NOT NULL, admitted predicates joined by AND/OR/NOT, or balanced grouping parentheses around admitted predicates",
        )),
    }
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
            "BETWEEN bounds admit scalar, DATE 'YYYY-MM-DD', or TIMESTAMP 'YYYY-MM-DDTHH:MM:SS(.ffffff)Z' literals only",
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
    let trimmed = raw.trim();
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
    validate_sql_column_ref(column)?;
    let target_dtype = parse_cast_target_dtype(target_raw)?;

    let tokens = split_whitespace_outside_quotes(tail)?;
    let (op, value) = match tokens.as_slice() {
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
    Ok(Some(ParsedPredicate::CastCompare {
        column: column.to_string(),
        target_dtype,
        mode,
        op,
        value,
    }))
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
            "timestamp arithmetic predicates compare against TIMESTAMP 'YYYY-MM-DDTHH:MM:SS(.ffffff)Z' literals only",
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

fn parse_cast_target_dtype(raw: &str) -> Result<LogicalDType, ShardLoomError> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "int64" | "bigint" | "integer" | "int" => Ok(LogicalDType::Int64),
        "float64" | "double" | "float" => Ok(LogicalDType::Float64),
        "utf8" | "string" | "text" => Ok(LogicalDType::Utf8),
        "boolean" | "bool" => Ok(LogicalDType::Boolean),
        "date32" | "date" => Ok(LogicalDType::Date32),
        "timestamp_micros" | "timestamp" => Ok(LogicalDType::TimestampMicros),
        _ => Err(unsupported_sql_error(
            "CAST target dtype must be one of int64, float64, utf8, boolean, date32, or timestamp_micros",
        )),
    }
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
    if matches!(
        (op, &rhs),
        (
            NumericArithmeticOp::Divide,
            ScalarValue::Int64(0) | ScalarValue::Float64(0.0)
        )
    ) {
        return Err(unsupported_sql_error(
            "numeric arithmetic division by zero is not admitted",
        ));
    }
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
    if !expression_contains_numeric_operator(left_raw)?
        && !expression_contains_numeric_operator(right_raw)?
        && !contains_temporal_difference
    {
        return Ok(None);
    }
    let left = parse_numeric_scalar_expression(left_raw, "where.generic.left")?;
    let right = parse_numeric_scalar_expression(right_raw, "where.generic.right")?;
    let source_columns = expression_pair_source_columns(&left, &right);
    if source_columns.is_empty() {
        return Err(unsupported_sql_error(
            "generic numeric expression predicates require at least one source column",
        ));
    }
    let operator_families = expression_pair_operator_families(&left, &right);
    let binary_operator_count =
        expression_binary_operator_count(&left) + expression_binary_operator_count(&right);
    if binary_operator_count == 0 && !expression_pair_has_temporal_difference(&left, &right) {
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
        .get(..6)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("length"))
        || trimmed.as_bytes().get(6) != Some(&b'(')
    {
        return Ok(None);
    }
    let open_index = "length".len();
    let close_index = matching_closing_parenthesis(trimmed, open_index)?.ok_or_else(|| {
        unsupported_sql_error("string length predicates must use LENGTH(<column>)")
    })?;
    let inner = trimmed[open_index + 1..close_index].trim();
    let tail = trimmed[close_index + 1..].trim();
    if inner.is_empty() || tail.is_empty() {
        return Err(unsupported_sql_error(
            "string length predicates require a source column, comparison operator, and int64 literal",
        ));
    }
    validate_sql_column_ref(inner)?;
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
        column: inner.to_string(),
        comparison: parse_comparison_op(op_raw)?,
        value,
    }))
}

fn parse_string_transform_predicate(raw: &str) -> Result<Option<ParsedPredicate>, ShardLoomError> {
    let trimmed = raw.trim();
    let Some((function_name, op)) = [
        ("lower", StringTransformOp::Lower),
        ("upper", StringTransformOp::Upper),
        ("trim", StringTransformOp::Trim),
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
            "string transform predicates must use LOWER(<column>), UPPER(<column>), or TRIM(<column>)",
        )
    })?;
    let inner = trimmed[open_index + 1..close_index].trim();
    let tail = trimmed[close_index + 1..].trim();
    if inner.is_empty() || tail.is_empty() {
        return Err(unsupported_sql_error(
            "string transform predicates require a source column, comparison operator, and string literal",
        ));
    }
    validate_sql_column_ref(inner)?;
    let tokens = split_whitespace_outside_quotes(tail)?;
    let [op_raw, literal_raw] = tokens.as_slice() else {
        return Err(unsupported_sql_error(
            "string transform predicates admit LOWER/UPPER/TRIM(<column>) <op> <string-literal> only",
        ));
    };
    let literal = parse_sql_string_literal(literal_raw)?;
    Ok(Some(ParsedPredicate::StringTransformCompare {
        column: inner.to_string(),
        op,
        comparison: parse_comparison_op(op_raw)?,
        value: ScalarValue::Utf8(literal),
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
                "TIMESTAMP literals must use TIMESTAMP 'YYYY-MM-DDTHH:MM:SS(.ffffff)Z'",
            )
        })
}

fn parse_in_list_predicate(raw: &str) -> Result<Option<ParsedPredicate>, ShardLoomError> {
    let Some(in_index) = find_keyword_outside_quotes(raw, "in") else {
        return Ok(None);
    };
    let column_raw = raw[..in_index].trim();
    let tail = raw[in_index + "in".len()..].trim();
    let column_tokens = split_whitespace_outside_quotes(column_raw)?;
    let (column, negated) = match column_tokens.as_slice() {
        [column] => (column.as_str(), false),
        [column, not_keyword] if not_keyword.eq_ignore_ascii_case("not") => (column.as_str(), true),
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

fn parse_in_subquery_predicate(column: &str, raw: &str) -> Result<ParsedPredicate, ShardLoomError> {
    let from_index = find_keyword_outside_quotes(raw, "from").ok_or_else(|| {
        unsupported_sql_error(
            "IN subquery predicates admit SELECT <column> FROM <local-source> only",
        )
    })?;
    if from_index <= "select".len() {
        return Err(unsupported_sql_error(
            "IN subquery predicates require a selected source column",
        ));
    }
    let select_column = raw["select".len()..from_index].trim();
    validate_sql_identifier(select_column)?;
    let source_raw = raw[from_index + "from".len()..].trim();
    if source_raw.is_empty()
        || contains_keyword_outside_quotes(source_raw, "where")
        || contains_keyword_outside_quotes(source_raw, "group by")
        || contains_keyword_outside_quotes(source_raw, "order by")
        || contains_keyword_outside_quotes(source_raw, "limit")
        || contains_keyword_outside_quotes(source_raw, "join")
        || contains_keyword_outside_quotes(source_raw, "select")
    {
        return Err(unsupported_sql_error(
            "IN subquery predicates admit SELECT <column> FROM <local-source> only",
        ));
    }
    let source_path = parse_source_path(source_raw)?;
    let _source_format = LocalSourceFormat::from_path(&source_path)?;
    Ok(ParsedPredicate::InSubquery {
        column: column.to_string(),
        subquery: Box::new(ParsedInSubquery {
            source_column: select_column.to_string(),
            source_path,
            source_format: None,
            source_digest: None,
            values: Vec::new(),
        }),
    })
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

fn parse_like_string_predicate(
    pattern: &str,
) -> Result<(StringPredicateOp, String), ShardLoomError> {
    if pattern.contains('_') {
        return Err(unsupported_sql_error(
            "LIKE '_' wildcards are not admitted in this scoped string-predicate smoke",
        ));
    }
    let percent_count = pattern.chars().filter(|ch| *ch == '%').count();
    match (
        pattern.strip_prefix('%'),
        pattern.strip_suffix('%'),
        percent_count,
    ) {
        (Some(inner), Some(_), 2) if pattern.len() >= 3 => {
            let needle = inner.strip_suffix('%').unwrap_or(inner);
            if needle.is_empty() || needle.contains('%') {
                Err(unsupported_sql_error(
                    "LIKE contains predicates admit exactly one non-empty %needle% pattern",
                ))
            } else {
                Ok((StringPredicateOp::Contains, needle.to_string()))
            }
        }
        (None, Some(prefix), 1) if !prefix.is_empty() => {
            Ok((StringPredicateOp::StartsWith, prefix.to_string()))
        }
        (Some(suffix), None, 1) if !suffix.is_empty() => {
            Ok((StringPredicateOp::EndsWith, suffix.to_string()))
        }
        _ => Err(unsupported_sql_error(
            "LIKE admits only prefix 'text%', suffix '%text', and contains '%text%' patterns in this scoped runtime slice; use = for exact string equality",
        )),
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

fn comparison_op_label(op: ComparisonOp) -> &'static str {
    match op {
        ComparisonOp::Eq => "eq",
        ComparisonOp::NotEq => "not_eq",
        ComparisonOp::Gt => "gt",
        ComparisonOp::GtEq => "gte",
        ComparisonOp::Lt => "lt",
        ComparisonOp::LtEq => "lte",
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
    if value > MAX_LIMIT_ROWS {
        return Err(unsupported_sql_error(&format!(
            "scoped SQL local-source smoke supports LIMIT <= {MAX_LIMIT_ROWS}"
        )));
    }
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
    if let Ok(parsed) = value.parse::<f64>() {
        if parsed.is_finite() {
            return Ok(ScalarValue::Float64(parsed));
        }
    }
    Err(unsupported_sql_error(
        "SQL local-source literals are limited to int64, finite float64, boolean, null, and single-quoted UTF-8 strings",
    ))
}

fn parse_csv_scalar(raw: &str) -> ScalarValue {
    let value = raw.trim();
    if value.is_empty() || value.eq_ignore_ascii_case("null") {
        ScalarValue::Null
    } else if value.eq_ignore_ascii_case("true") {
        ScalarValue::Boolean(true)
    } else if value.eq_ignore_ascii_case("false") {
        ScalarValue::Boolean(false)
    } else if let Ok(parsed) = value.parse::<i64>() {
        ScalarValue::Int64(parsed)
    } else if let Ok(parsed) = value.parse::<f64>() {
        if parsed.is_finite() {
            ScalarValue::Float64(parsed)
        } else {
            ScalarValue::Utf8(raw.to_string())
        }
    } else {
        ScalarValue::Utf8(raw.to_string())
    }
}

fn split_csv_record(raw: &str) -> Result<Vec<String>, ShardLoomError> {
    let mut values = Vec::new();
    let mut current = String::new();
    let mut chars = raw.chars().peekable();
    let mut in_quote = false;
    while let Some(ch) = chars.next() {
        match ch {
            '"' if in_quote && chars.peek() == Some(&'"') => {
                current.push('"');
                let _ = chars.next();
            }
            '"' => in_quote = !in_quote,
            ',' if !in_quote => {
                values.push(current);
                current = String::new();
            }
            _ => current.push(ch),
        }
    }
    if in_quote {
        return Err(unsupported_sql_error("CSV quoted field is not closed"));
    }
    values.push(current);
    Ok(values)
}

fn split_sql_csv(raw: &str) -> Result<Vec<String>, ShardLoomError> {
    let mut values = Vec::new();
    let mut current = String::new();
    let mut chars = raw.chars().peekable();
    let mut in_quote = false;
    let mut depth = 0_u32;
    while let Some(ch) = chars.next() {
        match ch {
            '\'' if in_quote && chars.peek() == Some(&'\'') => {
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
            ',' if !in_quote && depth == 0 => {
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
    if trimmed.len() < keyword.len() {
        return Ok(None);
    }
    if !trimmed[..keyword.len()].eq_ignore_ascii_case(keyword) {
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
        if remaining.len() >= lower_keyword.len()
            && remaining[..lower_keyword.len()].eq_ignore_ascii_case(&lower_keyword)
            && keyword_boundary(raw, index, lower_keyword.len())
        {
            return Some(index);
        }
    }
    None
}

fn find_keyword_outside_quotes_and_parentheses(
    raw: &str,
    keyword: &str,
) -> Result<Option<usize>, ShardLoomError> {
    let lower_keyword = keyword.to_ascii_lowercase();
    let mut chars = raw.char_indices().peekable();
    let mut in_quote = false;
    let mut depth = 0_u32;
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
            _ => {}
        }
        if depth == 0 {
            let remaining = &raw[index..];
            if lower_keyword == "and"
                && remaining.len() >= "between".len()
                && remaining[.."between".len()].eq_ignore_ascii_case("between")
                && keyword_boundary(raw, index, "between".len())
            {
                skip_next_and_for_between = true;
            }
            if remaining.len() >= lower_keyword.len()
                && remaining[..lower_keyword.len()].eq_ignore_ascii_case(&lower_keyword)
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

fn validate_sql_identifier(value: &str) -> Result<(), ShardLoomError> {
    let mut chars = value.chars();
    let Some(first) = chars.next() else {
        return Err(unsupported_sql_error("SQL identifiers must not be empty"));
    };
    if !(first == '_' || first.is_ascii_alphabetic()) {
        return Err(unsupported_sql_error(
            "SQL identifiers must start with an ASCII letter or underscore",
        ));
    }
    if !chars.all(is_identifier_char) {
        return Err(unsupported_sql_error(
            "SQL identifiers may contain only ASCII letters, numbers, and underscores",
        ));
    }
    Ok(())
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

fn is_identifier_char(ch: char) -> bool {
    ch == '_' || ch.is_ascii_alphanumeric()
}

fn rows_to_jsonl(rows: &[Vec<(String, ScalarValue)>]) -> String {
    let mut output = String::new();
    for row in rows {
        let mut line = String::new();
        line.push('{');
        for (index, (name, value)) in row.iter().enumerate() {
            if index > 0 {
                line.push(',');
            }
            write!(line, "\"{}\":{}", json_escape(name), scalar_to_json(value))
                .expect("write to string");
        }
        line.push('}');
        output.push_str(&line);
        output.push('\n');
    }
    output
}

fn rows_to_csv(columns: &[String], rows: &[Vec<(String, ScalarValue)>]) -> String {
    let mut output = String::new();
    if columns.is_empty() {
        return output;
    }
    for (index, name) in columns.iter().enumerate() {
        if index > 0 {
            output.push(',');
        }
        output.push_str(&csv_escape(name));
    }
    output.push('\n');
    for row in rows {
        for (index, (_name, value)) in row.iter().enumerate() {
            if index > 0 {
                output.push(',');
            }
            output.push_str(&csv_escape(&scalar_to_csv_value(value)));
        }
        output.push('\n');
    }
    output
}

#[cfg(feature = "universal-format-io")]
fn encode_parquet_output_rows(
    columns: &[String],
    rows: &[Vec<(String, ScalarValue)>],
) -> Result<Vec<u8>, ShardLoomError> {
    shardloom_vortex::encode_flat_parquet_rows(columns, rows)
}

#[cfg(not(feature = "universal-format-io"))]
fn encode_parquet_output_rows(
    _columns: &[String],
    _rows: &[Vec<(String, ScalarValue)>],
) -> Result<Vec<u8>, ShardLoomError> {
    Err(unsupported_sql_error(
        "local Parquet output runtime requires building shardloom-cli with --features universal-format-io; default builds expose Parquet as a deterministic blocked sink",
    ))
}

#[cfg(feature = "universal-format-io")]
fn encode_arrow_ipc_output_rows(
    columns: &[String],
    rows: &[Vec<(String, ScalarValue)>],
) -> Result<Vec<u8>, ShardLoomError> {
    shardloom_vortex::encode_flat_arrow_ipc_rows(columns, rows)
}

#[cfg(not(feature = "universal-format-io"))]
fn encode_arrow_ipc_output_rows(
    _columns: &[String],
    _rows: &[Vec<(String, ScalarValue)>],
) -> Result<Vec<u8>, ShardLoomError> {
    Err(unsupported_sql_error(
        "local Arrow IPC output runtime requires building shardloom-cli with --features universal-format-io; default builds expose Arrow IPC as a deterministic blocked sink",
    ))
}

#[cfg(feature = "universal-format-io")]
fn encode_avro_output_rows(
    columns: &[String],
    rows: &[Vec<(String, ScalarValue)>],
) -> Result<Vec<u8>, ShardLoomError> {
    shardloom_vortex::encode_flat_avro_rows(columns, rows)
}

#[cfg(not(feature = "universal-format-io"))]
fn encode_avro_output_rows(
    _columns: &[String],
    _rows: &[Vec<(String, ScalarValue)>],
) -> Result<Vec<u8>, ShardLoomError> {
    Err(unsupported_sql_error(
        "local Avro output runtime requires building shardloom-cli with --features universal-format-io; default builds expose Avro as a deterministic blocked sink",
    ))
}

#[cfg(feature = "universal-format-io")]
fn encode_orc_output_rows(
    columns: &[String],
    rows: &[Vec<(String, ScalarValue)>],
) -> Result<Vec<u8>, ShardLoomError> {
    shardloom_vortex::encode_flat_orc_rows(columns, rows)
}

#[cfg(not(feature = "universal-format-io"))]
fn encode_orc_output_rows(
    _columns: &[String],
    _rows: &[Vec<(String, ScalarValue)>],
) -> Result<Vec<u8>, ShardLoomError> {
    Err(unsupported_sql_error(
        "local ORC output runtime requires building shardloom-cli with --features universal-format-io; default builds expose ORC as a deterministic blocked sink",
    ))
}

fn scalar_to_csv_value(value: &ScalarValue) -> String {
    match value {
        ScalarValue::Boolean(value) => value.to_string(),
        ScalarValue::Int64(value) => value.to_string(),
        ScalarValue::UInt64(value) => value.to_string(),
        ScalarValue::Float64(value) if value.is_finite() => value.to_string(),
        ScalarValue::Null | ScalarValue::Float64(_) => String::new(),
        ScalarValue::Utf8(value) => value.clone(),
        ScalarValue::Binary(value) => format!("binary[len={}]", value.len()),
        ScalarValue::Date32(value) => format_iso_date32(*value),
        ScalarValue::TimestampMicros(value) => format_iso_timestamp_micros(*value),
    }
}

fn scalar_to_json(value: &ScalarValue) -> String {
    match value {
        ScalarValue::Boolean(value) => value.to_string(),
        ScalarValue::Int64(value) => value.to_string(),
        ScalarValue::UInt64(value) => value.to_string(),
        ScalarValue::Float64(value) if value.is_finite() => {
            let text = value.to_string();
            if text.contains('.') {
                text
            } else {
                format!("{text}.0")
            }
        }
        ScalarValue::Null | ScalarValue::Float64(_) => "null".to_string(),
        ScalarValue::Utf8(value) => format!("\"{}\"", json_escape(value)),
        ScalarValue::Binary(value) => format!("\"binary[len={}]\"", value.len()),
        ScalarValue::Date32(value) => format!("\"{}\"", format_iso_date32(*value)),
        ScalarValue::TimestampMicros(value) => {
            format!("\"{}\"", format_iso_timestamp_micros(*value))
        }
    }
}

fn json_escape(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            ch if ch.is_control() => {
                write!(out, "\\u{:04x}", u32::from(ch)).expect("write to string");
            }
            ch => out.push(ch),
        }
    }
    out
}

fn csv_escape(value: &str) -> String {
    if value.contains([',', '"', '\n', '\r']) {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_string()
    }
}

fn csv_or_not_applicable(values: impl Iterator<Item = String>) -> String {
    let joined = values.collect::<Vec<_>>().join(",");
    if joined.is_empty() {
        "not_applicable".to_string()
    } else {
        joined
    }
}

fn fnv64_digest(value: &str) -> String {
    fnv64_digest_bytes(value.as_bytes())
}

fn fnv64_digest_bytes(value: &[u8]) -> String {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in value {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    format!("fnv64:{hash:016x}")
}

fn unsupported_sql_error(reason: &str) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "{reason}; no fallback execution was attempted and external_engine_invoked=false"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sql_local_source_test_path(extension: &str) -> PathBuf {
        let mut path = std::env::temp_dir();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock")
            .as_nanos();
        path.push(format!(
            "shardloom-sql-local-source-{}-{nanos}.{extension}",
            std::process::id()
        ));
        path
    }

    fn field_map(fields: Vec<(String, String)>) -> BTreeMap<String, String> {
        fields.into_iter().collect()
    }

    fn assert_field_eq(fields: &BTreeMap<String, String>, key: &str, expected: &str) {
        assert_eq!(fields.get(key).map(String::as_str), Some(expected));
    }

    #[cfg(feature = "universal-format-io")]
    #[test]
    fn direct_transient_arrow_ipc_reports_columnar_source_state_boundary() {
        let path = sql_local_source_test_path("arrow");
        let columns = vec!["id".to_string(), "amount".to_string(), "label".to_string()];
        let rows = vec![
            vec![
                ("id".to_string(), ScalarValue::Int64(1)),
                ("amount".to_string(), ScalarValue::Int64(5)),
                ("label".to_string(), ScalarValue::Utf8("low".to_string())),
            ],
            vec![
                ("id".to_string(), ScalarValue::Int64(2)),
                ("amount".to_string(), ScalarValue::Int64(15)),
                ("label".to_string(), ScalarValue::Utf8("high".to_string())),
            ],
        ];
        let bytes = shardloom_vortex::encode_flat_arrow_ipc_rows(&columns, &rows)
            .expect("encode arrow ipc");
        fs::write(&path, bytes).expect("write arrow ipc source");

        let request = SqlLocalSourceRequest {
            statement: format!(
                "SELECT id FROM '{}' WHERE amount >= 10 LIMIT 1",
                path.display()
            ),
            output_format: SqlLocalSourceOutputFormat::InlineJsonl,
            output_path: None,
            fanout_outputs: Vec::new(),
            allow_overwrite: false,
        };
        let report = run_sql_local_source_smoke(&request).expect("run sql smoke");
        let fields = field_map(report.fields());

        assert_field_eq(&fields, "source_format", "arrow_ipc");
        assert_field_eq(
            &fields,
            "user_surface_runtime_scope",
            "format_neutral_sql_python_runtime",
        );
        assert_field_eq(
            &fields,
            "format_specific_boundary_scope",
            "read_ingest_and_write_only",
        );
        assert_field_eq(&fields, "format_specific_compute_path", "false");
        assert_field_eq(
            &fields,
            "source_state_materialization_layout",
            "arrow_record_batch_columnar_source_state_then_scalar_row_map",
        );
        assert_field_eq(
            &fields,
            "source_state_parse_normalization",
            "structured_reader_to_arrow_record_batches_then_scalar_rows",
        );
        assert_field_eq(&fields, "source_state_columnar_preserved", "true");
        assert_field_eq(&fields, "source_state_record_batch_count", "1");
        assert_field_eq(
            &fields,
            "source_state_runtime_consumption_layout",
            "scalar_row_map_expression_runtime",
        );
        assert_field_eq(
            &fields,
            "source_state_scalar_runtime_materialization_required",
            "true",
        );
        assert_field_eq(
            &fields,
            "source_state_projection_pushdown_status",
            "reader_level_projection",
        );
        assert_field_eq(&fields, "source_state_materialized_columns", "id,amount");
        assert_field_eq(
            &fields,
            "source_state_reader_projection_columns",
            "id,amount",
        );
        let source_to_columnar_millis = fields
            .get("source_to_columnar_millis")
            .expect("source_to_columnar_millis")
            .parse::<u128>()
            .expect("source_to_columnar_millis numeric");
        assert!(source_to_columnar_millis <= report.source.parse_millis);
        assert_eq!(report.output_rows.len(), 1);
        assert!(report.output_rows[0].contains(&("id".to_string(), ScalarValue::Int64(2))));
        fs::remove_file(&path).expect("remove arrow ipc source");
    }

    #[test]
    fn parses_scoped_sql_local_source_statement() {
        let parsed = parse_sql_local_source_statement(
            "SELECT id,label FROM 'target/input.csv' WHERE amount >= 10 LIMIT 5",
        )
        .expect("statement parses");

        assert_eq!(parsed.projections, vec!["id", "label"]);
        assert!(parsed.aggregates.is_empty());
        assert!(parsed.group_by.is_empty());
        assert!(parsed.order_by.is_none());
        assert_eq!(parsed.source_path, PathBuf::from("target/input.csv"));
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
        assert!(parsed.aggregates.is_empty());
        assert!(parsed.group_by.is_empty());
        assert!(parsed.order_by.is_none());
        assert_eq!(parsed.source_path, PathBuf::from("target/input.csv"));
        assert_eq!(parsed.limit, 5);
        assert!(parsed.predicate.is_all());
        assert_eq!(parsed.statement_kind(), "local_source_projection_limit");
        assert_eq!(parsed.execution_certificate_suffix(), "projection-limit");
    }

    #[test]
    fn parses_scoped_literal_projection_statement() {
        let parsed = parse_sql_local_source_statement(
            "SELECT id,label,'north' AS segment,DATE '2026-05-19' AS batch_date FROM 'target/input.csv' WHERE amount >= 10 LIMIT 5",
        )
        .expect("literal projection statement parses");

        assert_eq!(parsed.projections, vec!["id", "label"]);
        assert_eq!(parsed.literal_projections.len(), 2);
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
        assert_eq!(
            parsed.statement_kind(),
            "local_source_literal_projection_filter_limit"
        );
        assert_eq!(
            parsed.execution_certificate_suffix(),
            "literal-projection-filter-limit"
        );
    }

    #[test]
    fn parses_scoped_numeric_arithmetic_projection_statement() {
        let parsed = parse_sql_local_source_statement(
            "SELECT id,amount + 5 AS adjusted,ratio * 2.0 AS doubled FROM 'target/input.csv' WHERE amount >= 10 LIMIT 5",
        )
        .expect("numeric arithmetic projection statement parses");

        assert_eq!(parsed.projections, vec!["id"]);
        assert!(parsed.literal_projections.is_empty());
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
        assert_eq!(
            parsed.numeric_arithmetic_projection_output_columns(),
            "adjusted,doubled"
        );
        assert_eq!(
            parsed.numeric_arithmetic_projection_source_columns(),
            "amount,ratio"
        );
        assert_eq!(
            parsed.numeric_arithmetic_projection_operators(),
            "add,multiply"
        );
        assert_eq!(
            parsed.statement_kind(),
            "local_source_computed_projection_filter_limit"
        );
        assert_eq!(
            parsed.execution_certificate_suffix(),
            "computed-projection-filter-limit"
        );
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
        assert_eq!(
            parsed.statement_kind(),
            "local_source_computed_projection_filter_limit"
        );
    }

    #[test]
    fn parser_blocks_star_plus_raw_projection_without_fallback() {
        let error = parse_sql_local_source_statement(
            "SELECT *,id FROM 'target/input.csv' WHERE amount >= 10 LIMIT 5",
        )
        .expect_err("star plus raw projection is not admitted");

        assert!(error.to_string().contains(
            "SELECT * can be mixed only with computed or literal projections in this scoped smoke"
        ));
    }

    #[test]
    fn parses_generic_expression_projection_statement() {
        let parsed = parse_sql_local_source_statement(
            "SELECT id,(amount + tax) * 2 AS gross,ABS(amount - tax) AS spread FROM 'target/input.csv' WHERE amount >= 10 LIMIT 5",
        )
        .expect("generic expression projection statement parses");

        assert_eq!(parsed.projections, vec!["id"]);
        assert!(parsed.numeric_arithmetic_projections.is_empty());
        assert_eq!(parsed.generic_expression_projections.len(), 2);
        assert_eq!(parsed.generic_expression_projections[0].alias, "gross");
        assert_eq!(
            parsed.generic_expression_projections[0].source_columns,
            vec!["amount".to_string(), "tax".to_string()]
        );
        assert_eq!(
            parsed.generic_expression_projection_output_columns(),
            "gross,spread"
        );
        assert_eq!(
            parsed.generic_expression_projection_source_columns(),
            "amount+tax,amount+tax"
        );
        assert_eq!(
            parsed.generic_expression_projection_operator_families(),
            "numeric_binary,numeric_abs+numeric_binary"
        );
        assert_eq!(
            parsed.generic_expression_projection_binary_operator_count(),
            3
        );
        assert_eq!(
            parsed.statement_kind(),
            "local_source_computed_projection_filter_limit"
        );
        assert_eq!(
            parsed.execution_certificate_suffix(),
            "computed-projection-filter-limit"
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
        assert_eq!(
            parsed.generic_expression_projection_output_columns(),
            "age_days,elapsed_seconds"
        );
        assert_eq!(
            parsed.generic_expression_projection_source_columns(),
            "end_date+start_date,end_ts+start_ts"
        );
        assert_eq!(
            parsed.generic_expression_projection_operator_families(),
            "cast+temporal_difference,cast+temporal_difference"
        );
        assert_eq!(
            parsed.generic_expression_projection_binary_operator_count(),
            0
        );
        assert_eq!(parsed.predicate.family(), "generic_expression");
        assert!(parsed.predicate.uses_generic_expression());
        assert_eq!(
            parsed.predicate.generic_expression_source_columns(),
            "end_date"
        );
        assert_eq!(
            parsed.predicate.generic_expression_operator_families(),
            "temporal_difference"
        );
        assert_eq!(
            parsed.predicate.generic_expression_binary_operator_count(),
            0
        );
        assert_eq!(
            parsed.predicate.generic_expression_comparison_operator(),
            "gte"
        );
        assert_eq!(parsed.predicate.columns(), vec!["end_date"]);
    }

    #[test]
    fn parses_scoped_numeric_abs_projection_statement() {
        let parsed = parse_sql_local_source_statement(
            "SELECT id,ABS(amount) AS magnitude FROM 'target/input.csv' WHERE ABS(amount) >= 4 LIMIT 5",
        )
        .expect("numeric abs projection statement parses");

        assert_eq!(parsed.projections, vec!["id"]);
        assert!(parsed.literal_projections.is_empty());
        assert_eq!(parsed.numeric_abs_projections.len(), 1);
        assert_eq!(parsed.numeric_abs_projections[0].alias, "magnitude");
        assert_eq!(parsed.numeric_abs_projections[0].column, "amount");
        assert_eq!(parsed.numeric_abs_projection_output_columns(), "magnitude");
        assert_eq!(parsed.numeric_abs_projection_source_columns(), "amount");
        assert_eq!(
            parsed.statement_kind(),
            "local_source_computed_projection_filter_limit"
        );
        assert_eq!(
            parsed.execution_certificate_suffix(),
            "computed-projection-filter-limit"
        );
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
        assert_eq!(parsed.numeric_rounding_projection_operators(), "floor,ceil");
        assert_eq!(
            parsed.numeric_rounding_projection_output_columns(),
            "bucket,upper"
        );
        assert_eq!(
            parsed.statement_kind(),
            "local_source_computed_projection_filter_limit"
        );
        assert_eq!(
            parsed.execution_certificate_suffix(),
            "computed-projection-filter-limit"
        );
    }

    #[test]
    fn parses_scoped_cast_projection_statement() {
        let parsed = parse_sql_local_source_statement(
            "SELECT id,CAST(amount AS float64) AS amount_float,CAST(event_date AS date32) AS event_day FROM 'target/input.csv' WHERE id >= 1 LIMIT 5",
        )
        .expect("cast projection statement parses");

        assert_eq!(parsed.projections, vec!["id"]);
        assert!(parsed.literal_projections.is_empty());
        assert_eq!(parsed.cast_projections.len(), 2);
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
        assert_eq!(parsed.cast_projection_source_columns(), "amount,event_date");
        assert_eq!(
            parsed.cast_projection_output_columns(),
            "amount_float,event_day"
        );
        assert_eq!(parsed.cast_projection_target_dtypes(), "float64,date32");
        assert_eq!(parsed.cast_projection_modes(), "strict,strict");
        assert_eq!(
            parsed.statement_kind(),
            "local_source_computed_projection_filter_limit"
        );
        assert_eq!(
            parsed.execution_certificate_suffix(),
            "computed-projection-filter-limit"
        );
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
        assert_eq!(parsed.cast_projection_source_columns(), "raw_amount");
        assert_eq!(parsed.cast_projection_output_columns(), "amount_i64");
        assert_eq!(parsed.cast_projection_target_dtypes(), "int64");
        assert_eq!(parsed.cast_projection_modes(), "try");
        assert_eq!(
            parsed.statement_kind(),
            "local_source_computed_projection_filter_limit"
        );
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
        assert_eq!(
            parsed.date_arithmetic_projection_operators(),
            "date_add_days,date_sub_days"
        );
        assert_eq!(
            parsed.date_arithmetic_projection_source_columns(),
            "event_date,event_date"
        );
        assert_eq!(parsed.date_arithmetic_projection_days(), "7,1");
        assert_eq!(
            parsed.date_arithmetic_projection_output_columns(),
            "next_week,prior_day"
        );
        assert_eq!(
            parsed.statement_kind(),
            "local_source_computed_projection_filter_limit"
        );
        assert_eq!(
            parsed.execution_certificate_suffix(),
            "computed-projection-filter-limit"
        );
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
        assert_eq!(
            parsed.timestamp_arithmetic_projection_operators(),
            "timestamp_add_seconds,timestamp_sub_seconds"
        );
        assert_eq!(
            parsed.timestamp_arithmetic_projection_source_columns(),
            "event_ts,event_ts"
        );
        assert_eq!(parsed.timestamp_arithmetic_projection_seconds(), "90,45");
        assert_eq!(
            parsed.timestamp_arithmetic_projection_output_columns(),
            "shifted_ts,prior_ts"
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
        assert_eq!(parsed.predicate.family(), "timestamp_arithmetic");
        assert!(parsed.predicate.uses_timestamp_literal());
        assert!(parsed.predicate.uses_timestamp_arithmetic());
        assert_eq!(
            parsed.predicate.timestamp_arithmetic_operator(),
            "timestamp_add_seconds"
        );
        assert_eq!(parsed.predicate.timestamp_arithmetic_seconds(), "60");
        assert_eq!(
            parsed.predicate.timestamp_arithmetic_source_columns(),
            "event_ts"
        );
        assert_eq!(
            parsed.statement_kind(),
            "local_source_computed_projection_filter_limit"
        );
        assert_eq!(
            parsed.execution_certificate_suffix(),
            "computed-projection-filter-limit"
        );
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
        assert_eq!(
            parsed.null_coalesce_projection_source_columns(),
            "label,event_date"
        );
        assert_eq!(
            parsed.null_coalesce_projection_output_columns(),
            "label_clean,event_day"
        );
        assert_eq!(
            parsed.null_coalesce_projection_fallback_dtypes(),
            "utf8,date32"
        );
        assert_eq!(
            parsed.statement_kind(),
            "local_source_computed_projection_filter_limit"
        );
        assert_eq!(
            parsed.execution_certificate_suffix(),
            "computed-projection-filter-limit"
        );
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
        assert_eq!(
            parsed.nullif_projection_source_columns(),
            "label,event_date"
        );
        assert_eq!(
            parsed.nullif_projection_output_columns(),
            "label_clean,event_day"
        );
        assert_eq!(parsed.nullif_projection_sentinel_dtypes(), "utf8,date32");
        assert_eq!(
            parsed.statement_kind(),
            "local_source_computed_projection_filter_limit"
        );
        assert_eq!(
            parsed.execution_certificate_suffix(),
            "computed-projection-filter-limit"
        );
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
            parsed.conditional_projections[0].predicate.family(),
            "comparison"
        );
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
        assert_eq!(
            parsed.conditional_projection_source_columns(),
            "amount,event_date"
        );
        assert_eq!(
            parsed.conditional_projection_output_columns(),
            "size_band,cutoff_day"
        );
        assert_eq!(
            parsed.conditional_projection_predicate_families(),
            "comparison,comparison"
        );
        assert_eq!(parsed.conditional_projection_then_dtypes(), "utf8,date32");
        assert_eq!(parsed.conditional_projection_else_dtypes(), "utf8,date32");
        assert_eq!(
            parsed.statement_kind(),
            "local_source_computed_projection_filter_limit"
        );
        assert_eq!(
            parsed.execution_certificate_suffix(),
            "computed-projection-filter-limit"
        );
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
        assert_eq!(
            parsed.conditional_projection_source_columns(),
            "amount+fallback_label+preferred_label"
        );
        assert_eq!(parsed.conditional_projection_then_dtypes(), "source_column");
        assert_eq!(parsed.conditional_projection_else_dtypes(), "source_column");
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
        assert_eq!(
            parsed.predicate_projections[0].predicate.family(),
            "comparison"
        );
        assert_eq!(
            parsed.predicate_projections[1].predicate.family(),
            "null_predicate"
        );
        assert_eq!(
            parsed.predicate_projections[2].predicate.family(),
            "boolean_predicate"
        );
        assert_eq!(
            parsed.predicate_projection_predicate_families(),
            "comparison,null_predicate,boolean_predicate"
        );
        assert_eq!(
            parsed.predicate_projection_source_columns(),
            "amount,label,active"
        );
        assert_eq!(
            parsed.predicate_projection_output_columns(),
            "is_large,missing_label,inactive_or_unknown"
        );
        assert_eq!(
            parsed.predicate_projection_null_semantics(),
            "sql_three_valued_boolean_or_null_projection,sql_is_null_is_not_null,sql_boolean_is_not_true_false_null_matches"
        );
        assert_eq!(
            parsed.statement_kind(),
            "local_source_computed_projection_filter_limit"
        );
        assert_eq!(
            parsed.execution_certificate_suffix(),
            "computed-projection-filter-limit"
        );
    }

    #[test]
    fn parses_scoped_string_transform_projection_statement() {
        let parsed = parse_sql_local_source_statement(
            "SELECT id,LOWER(label) AS lowered,UPPER(label) AS raised,TRIM(label) AS trimmed FROM 'target/input.csv' WHERE id >= 1 LIMIT 5",
        )
        .expect("string transform projection statement parses");

        assert_eq!(parsed.projections, vec!["id"]);
        assert!(parsed.literal_projections.is_empty());
        assert!(parsed.numeric_arithmetic_projections.is_empty());
        assert_eq!(parsed.string_transform_projections.len(), 3);
        assert_eq!(parsed.string_transform_projections[0].alias, "lowered");
        assert_eq!(parsed.string_transform_projections[0].column, "label");
        assert_eq!(
            parsed.string_transform_projections[0].op,
            StringTransformOp::Lower
        );
        assert_eq!(
            parsed.string_transform_projection_output_columns(),
            "lowered,raised,trimmed"
        );
        assert_eq!(
            parsed.string_transform_projection_source_columns(),
            "label,label,label"
        );
        assert_eq!(
            parsed.string_transform_projection_operators(),
            "lower,upper,trim"
        );
        assert_eq!(
            parsed.statement_kind(),
            "local_source_computed_projection_filter_limit"
        );
        assert_eq!(
            parsed.execution_certificate_suffix(),
            "computed-projection-filter-limit"
        );
    }

    #[test]
    fn parses_scoped_string_length_projection_statement() {
        let parsed = parse_sql_local_source_statement(
            "SELECT id,LENGTH(label) AS label_len FROM 'target/input.csv' WHERE id >= 1 LIMIT 5",
        )
        .expect("string length projection statement parses");

        assert_eq!(parsed.projections, vec!["id"]);
        assert!(parsed.literal_projections.is_empty());
        assert!(parsed.string_transform_projections.is_empty());
        assert_eq!(parsed.string_length_projections.len(), 1);
        assert_eq!(parsed.string_length_projections[0].alias, "label_len");
        assert_eq!(parsed.string_length_projections[0].column, "label");
        assert_eq!(
            parsed.string_length_projection_output_columns(),
            "label_len"
        );
        assert_eq!(parsed.string_length_projection_source_columns(), "label");
        assert_eq!(
            parsed.statement_kind(),
            "local_source_computed_projection_filter_limit"
        );
        assert_eq!(
            parsed.execution_certificate_suffix(),
            "computed-projection-filter-limit"
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
        assert_eq!(
            parsed.string_function_projection_operators(),
            "concat,substr,left,right,replace"
        );
        assert_eq!(
            parsed.string_function_projection_source_columns(),
            "label+segment,label,label,label,label"
        );
        assert_eq!(
            parsed.string_function_projection_output_columns(),
            "label_key,middle,prefix,suffix,scrubbed"
        );
        assert_eq!(
            parsed.string_function_projection_literal_counts(),
            "1,2,1,1,2"
        );
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
        assert_eq!(parsed.predicate.family(), "string_function");
        assert_eq!(parsed.predicate.string_function_operator(), "concat");
        assert_eq!(
            parsed.predicate.string_function_source_columns(),
            "label+segment"
        );
        assert_eq!(parsed.predicate.string_function_literal_counts(), "2");
        assert_eq!(parsed.predicate.string_function_rhs_dtypes(), "utf8");
        assert_eq!(
            parsed.statement_kind(),
            "local_source_computed_projection_filter_limit"
        );
        assert_eq!(
            parsed.execution_certificate_suffix(),
            "computed-projection-filter-limit"
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
        assert_eq!(parsed.date_extract_projection_operators(), "date_year");
        assert_eq!(
            parsed.timestamp_extract_projection_operators(),
            "timestamp_hour"
        );
        assert_eq!(
            parsed.statement_kind(),
            "local_source_computed_projection_filter_limit"
        );
        assert_eq!(
            parsed.execution_certificate_suffix(),
            "computed-projection-filter-limit"
        );
    }

    #[test]
    fn literal_projection_duplicate_output_names_are_blocked() {
        let parsed = parse_sql_local_source_statement(
            "SELECT id,'north' AS id FROM 'target/input.csv' WHERE amount >= 10 LIMIT 5",
        )
        .expect("literal projection statement parses before binding");
        let header = vec!["id".to_string(), "amount".to_string()];

        let error = bind_sql_local_source(&parsed, &header, None)
            .expect_err("duplicate literal projection output name is blocked");

        assert!(
            error
                .to_string()
                .contains("computed projection smoke requires unique output column names"),
            "{error}"
        );
    }

    #[test]
    fn numeric_arithmetic_projection_duplicate_output_names_are_blocked() {
        let parsed = parse_sql_local_source_statement(
            "SELECT id,amount + 1 AS id FROM 'target/input.csv' WHERE amount >= 10 LIMIT 5",
        )
        .expect("numeric arithmetic projection statement parses before binding");
        let header = vec!["id".to_string(), "amount".to_string()];

        let error = bind_sql_local_source(&parsed, &header, None)
            .expect_err("duplicate arithmetic projection output name is blocked");

        assert!(
            error
                .to_string()
                .contains("computed projection smoke requires unique output column names"),
            "{error}"
        );
    }

    #[test]
    fn string_transform_projection_duplicate_output_names_are_blocked() {
        let parsed = parse_sql_local_source_statement(
            "SELECT id,LOWER(label) AS id FROM 'target/input.csv' WHERE amount >= 10 LIMIT 5",
        )
        .expect("string transform projection statement parses before binding");
        let header = vec!["id".to_string(), "label".to_string(), "amount".to_string()];

        let error = bind_sql_local_source(&parsed, &header, None)
            .expect_err("duplicate transform projection output name is blocked");

        assert!(
            error
                .to_string()
                .contains("computed projection smoke requires unique output column names"),
            "{error}"
        );
    }

    #[test]
    fn cast_projection_missing_source_column_is_blocked() {
        let parsed = parse_sql_local_source_statement(
            "SELECT id,CAST(missing_amount AS float64) AS amount_float FROM 'target/input.csv' LIMIT 5",
        )
        .expect("cast projection statement parses before binding");
        let header = vec!["id".to_string(), "amount".to_string()];

        let error = bind_sql_local_source(&parsed, &header, None)
            .expect_err("missing cast projection source column is blocked");

        assert!(
            error
                .to_string()
                .contains("cast projection source column \"missing_amount\" is not present"),
            "{error}"
        );
    }

    #[test]
    fn date_arithmetic_projection_missing_source_column_is_blocked() {
        let parsed = parse_sql_local_source_statement(
            "SELECT id,DATE_ADD_DAYS(missing_date, 7) AS next_week FROM 'target/input.csv' LIMIT 5",
        )
        .expect("date arithmetic projection statement parses before binding");
        let header = vec!["id".to_string(), "event_date".to_string()];

        let error = bind_sql_local_source(&parsed, &header, None)
            .expect_err("missing date arithmetic projection source column is blocked");

        assert!(
            error.to_string().contains(
                "date arithmetic projection source column \"missing_date\" is not present"
            ),
            "{error}"
        );
    }

    #[test]
    fn null_coalesce_projection_missing_source_column_is_blocked() {
        let parsed = parse_sql_local_source_statement(
            "SELECT id,COALESCE(missing_label, 'unknown') AS label_clean FROM 'target/input.csv' LIMIT 5",
        )
        .expect("null coalesce projection statement parses before binding");
        let header = vec!["id".to_string(), "label".to_string()];

        let error = bind_sql_local_source(&parsed, &header, None)
            .expect_err("missing null coalesce source column is blocked");

        assert!(
            error
                .to_string()
                .contains("COALESCE projection source column \"missing_label\" is not present"),
            "{error}"
        );
    }

    #[test]
    fn null_coalesce_projection_null_fallback_is_blocked() {
        let error = parse_sql_local_source_statement(
            "SELECT id,COALESCE(label, NULL) AS label_clean FROM 'target/input.csv' LIMIT 5",
        )
        .expect_err("null fallback is blocked during parsing");

        assert!(
            error
                .to_string()
                .contains("COALESCE projections require a non-NULL fallback literal"),
            "{error}"
        );
    }

    #[test]
    fn nullif_projection_missing_source_column_is_blocked() {
        let parsed = parse_sql_local_source_statement(
            "SELECT id,NULLIF(missing_label, 'unknown') AS label_clean FROM 'target/input.csv' LIMIT 5",
        )
        .expect("nullif projection statement parses before binding");
        let header = vec!["id".to_string(), "label".to_string()];

        let error = bind_sql_local_source(&parsed, &header, None)
            .expect_err("missing nullif source column is blocked");

        assert!(
            error
                .to_string()
                .contains("NULLIF projection source column \"missing_label\" is not present"),
            "{error}"
        );
    }

    #[test]
    fn nullif_projection_null_sentinel_is_blocked() {
        let error = parse_sql_local_source_statement(
            "SELECT id,NULLIF(label, NULL) AS label_clean FROM 'target/input.csv' LIMIT 5",
        )
        .expect_err("null sentinel is blocked during parsing");

        assert!(
            error
                .to_string()
                .contains("NULLIF projections require a non-NULL sentinel literal"),
            "{error}"
        );
    }

    #[test]
    fn conditional_projection_missing_source_column_is_blocked() {
        let parsed = parse_sql_local_source_statement(
            "SELECT id,CASE WHEN missing_amount >= 10 THEN 'large' ELSE 'small' END AS size_band FROM 'target/input.csv' LIMIT 5",
        )
        .expect("conditional projection statement parses before binding");
        let header = vec!["id".to_string(), "amount".to_string()];

        let error = bind_sql_local_source(&parsed, &header, None)
            .expect_err("missing conditional predicate column is blocked");

        assert!(
            error.to_string().contains(
                "conditional projection predicate column \"missing_amount\" is not present"
            ),
            "{error}"
        );
    }

    #[test]
    fn conditional_projection_null_branch_is_blocked() {
        let error = parse_sql_local_source_statement(
            "SELECT id,CASE WHEN amount >= 10 THEN NULL ELSE 'small' END AS size_band FROM 'target/input.csv' LIMIT 5",
        )
        .expect_err("null CASE branch is blocked during parsing");

        assert!(
            error
                .to_string()
                .contains("CASE projections require a non-NULL THEN branch literal"),
            "{error}"
        );
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
    fn temporal_extract_projection_missing_source_column_is_blocked() {
        let parsed = parse_sql_local_source_statement(
            "SELECT id,DATE_YEAR(missing_date) AS event_year FROM 'target/input.csv' LIMIT 5",
        )
        .expect("date extract projection statement parses before binding");
        let header = vec!["id".to_string(), "event_date".to_string()];

        let error = bind_sql_local_source(&parsed, &header, None)
            .expect_err("missing temporal projection source column is blocked");

        assert!(
            error
                .to_string()
                .contains("date extract projection source column \"missing_date\" is not present"),
            "{error}"
        );
    }

    #[test]
    fn numeric_arithmetic_projection_divide_by_zero_is_blocked() {
        let error = parse_sql_local_source_statement(
            "SELECT id,amount / 0 AS ratio FROM 'target/input.csv' LIMIT 5",
        )
        .expect_err("division by zero projection is blocked during parsing");

        assert!(
            error
                .to_string()
                .contains("numeric arithmetic projection division by zero is not admitted"),
            "{error}"
        );
    }

    #[test]
    fn parses_scoped_scalar_aggregate_statement() {
        let parsed = parse_sql_local_source_statement(
            "SELECT count(*),sum(amount),avg(amount),min(amount),max(amount) FROM 'target/input.csv' WHERE amount >= 10 LIMIT 1",
        )
        .expect("aggregate statement parses");

        assert!(parsed.projections.is_empty());
        assert_eq!(parsed.aggregates.len(), 5);
        assert_eq!(parsed.aggregates[0].label(), "count(*)");
        assert_eq!(parsed.aggregates[1].label(), "sum(amount)");
        assert_eq!(parsed.aggregates[2].output_name(), "avg_amount");
        assert!(parsed.group_by.is_empty());
        assert!(parsed.order_by.is_none());
        assert_eq!(parsed.source_path, PathBuf::from("target/input.csv"));
        assert_eq!(
            parsed.statement_kind(),
            "local_source_aggregate_filter_limit"
        );
    }

    #[test]
    fn parses_scoped_aggregate_alias_statement() {
        let parsed = parse_sql_local_source_statement(
            "SELECT count(*) AS rows,sum(amount) AS total_amount FROM 'target/input.csv' WHERE amount >= 10 LIMIT 1",
        )
        .expect("aggregate alias statement parses");

        assert!(parsed.projections.is_empty());
        assert_eq!(parsed.aggregates.len(), 2);
        assert_eq!(parsed.aggregates[0].label(), "count(*)");
        assert_eq!(parsed.aggregates[0].output_name(), "rows");
        assert_eq!(parsed.aggregates[0].alias.as_deref(), Some("rows"));
        assert_eq!(parsed.aggregates[1].label(), "sum(amount)");
        assert_eq!(parsed.aggregates[1].output_name(), "total_amount");
        assert!(parsed.group_by.is_empty());
        assert_eq!(
            parsed.statement_kind(),
            "local_source_aggregate_filter_limit"
        );
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
        assert_eq!(parsed.aggregates[0].label(), "count(DISTINCT customer_id)");
        assert_eq!(parsed.aggregates[0].output_name(), "unique_customers");
        assert!(parsed.aggregates[0].distinct);
        assert_eq!(
            parsed.distinct_aggregate_functions(),
            "count(DISTINCT customer_id)"
        );
        assert_eq!(parsed.distinct_aggregate_columns(), "customer_id");
        assert_eq!(
            parsed.distinct_aggregate_null_semantics(),
            "sql_count_distinct_ignores_nulls"
        );
        assert_eq!(
            parsed.statement_kind(),
            "local_source_group_by_aggregate_filter_limit"
        );
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
                .contains("COUNT(DISTINCT <column>) only"),
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
        assert_eq!(parsed.aggregates[0].label(), "count(*)");
        assert_eq!(parsed.aggregates[1].label(), "sum(amount)");
        assert_eq!(
            parsed.statement_kind(),
            "local_source_group_by_aggregate_filter_limit"
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
        assert_eq!(parsed.aggregates[0].label(), "count(*)");
        assert_eq!(parsed.aggregates[1].label(), "sum(amount)");
        assert_eq!(
            parsed.statement_kind(),
            "local_source_group_by_aggregate_filter_limit"
        );
    }

    #[test]
    fn parses_scoped_order_by_topn_statement() {
        let parsed = parse_sql_local_source_statement(
            "SELECT id,label FROM 'target/input.csv' WHERE amount >= 0 ORDER BY amount DESC LIMIT 3",
        )
        .expect("order-by statement parses");

        assert_eq!(parsed.projections, vec!["id", "label"]);
        assert!(parsed.aggregates.is_empty());
        assert!(parsed.group_by.is_empty());
        let order_by = parsed.order_by.as_ref().expect("order by parsed");
        assert_eq!(order_by.columns_label(), "amount");
        assert_eq!(order_by.directions_label(), "desc");
        assert_eq!(order_by.operator_family_label(), "single_key_scalar_topn");
        assert_eq!(order_by.keys[0].column, "amount");
        assert_eq!(order_by.keys[0].direction, SortDirection::Desc);
        assert_eq!(parsed.limit, 3);
        assert_eq!(
            parsed.statement_kind(),
            "local_source_order_by_topn_filter_limit"
        );
    }

    #[test]
    fn parses_scoped_multi_key_order_by_topn_statement() {
        let parsed = parse_sql_local_source_statement(
            "SELECT id,label FROM 'target/input.csv' WHERE amount >= 0 ORDER BY amount DESC,id ASC LIMIT 3",
        )
        .expect("multi-key order-by statement parses");

        let order_by = parsed.order_by.as_ref().expect("order by parsed");
        assert!(order_by.is_multi_key());
        assert_eq!(order_by.columns_label(), "amount,id");
        assert_eq!(order_by.directions_label(), "desc,asc");
        assert_eq!(order_by.operator_family_label(), "multi_key_scalar_topn");
        assert_eq!(parsed.limit, 3);
        assert_eq!(
            parsed.statement_kind(),
            "local_source_order_by_topn_filter_limit"
        );
    }

    #[test]
    fn runs_scoped_multi_key_order_by_topn_csv_statement() {
        let path = sql_local_source_test_path("csv");
        fs::write(
            &path,
            "id,label,amount\n1,alpha,10\n2,beta,10\n3,gamma,20\n4,delta,20\n5,epsilon,5\n",
        )
        .expect("write csv source");

        let request = SqlLocalSourceRequest {
            statement: format!(
                "SELECT id,label FROM '{}' WHERE amount >= 10 ORDER BY amount DESC,id ASC LIMIT 3",
                path.display()
            ),
            output_format: SqlLocalSourceOutputFormat::InlineJsonl,
            output_path: None,
            fanout_outputs: Vec::new(),
            allow_overwrite: false,
        };
        let report = run_sql_local_source_smoke(&request).expect("run multi-key top-N smoke");
        let fields = field_map(report.fields());

        assert_eq!(
            report.result_jsonl,
            "{\"id\":3,\"label\":\"gamma\"}\n{\"id\":4,\"label\":\"delta\"}\n{\"id\":1,\"label\":\"alpha\"}\n"
        );
        assert_field_eq(&fields, "order_by_runtime_execution", "true");
        assert_field_eq(&fields, "top_n_runtime_execution", "true");
        assert_field_eq(&fields, "sort_operator_family", "multi_key_scalar_topn");
        assert_field_eq(&fields, "sort_keys", "amount,id");
        assert_field_eq(&fields, "sort_direction", "desc,asc");
        assert_field_eq(&fields, "top_n_limit", "3");
        assert_field_eq(&fields, "fallback_attempted", "false");
        assert_field_eq(&fields, "external_engine_invoked", "false");

        fs::remove_file(&path).expect("remove csv source");
    }

    #[test]
    fn parses_scoped_cast_predicate_statement() {
        let parsed = parse_sql_local_source_statement(
            "SELECT id,amount FROM 'target/input.jsonl' WHERE CAST(amount AS int64) >= 10 LIMIT 5",
        )
        .expect("cast predicate statement parses");

        assert_eq!(parsed.projections, vec!["id", "amount"]);
        assert_eq!(parsed.source_path, PathBuf::from("target/input.jsonl"));
        assert!(matches!(
            parsed.predicate,
            ParsedPredicate::CastCompare {
                ref column,
                target_dtype: LogicalDType::Int64,
                mode: CastMode::Strict,
                op: ComparisonOp::GtEq,
                value: ScalarValue::Int64(10)
            } if column == "amount"
        ));
        assert_eq!(parsed.predicate.family(), "cast");
        assert_eq!(parsed.predicate.cast_source_columns(), "amount");
        assert_eq!(parsed.predicate.cast_target_dtypes(), "int64");
        assert_eq!(parsed.predicate.cast_modes(), "strict");
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
                value: ScalarValue::Int64(10)
            } if column == "raw_amount"
        ));
        assert_eq!(parsed.predicate.family(), "cast");
        assert_eq!(parsed.predicate.cast_source_columns(), "raw_amount");
        assert_eq!(parsed.predicate.cast_target_dtypes(), "int64");
        assert_eq!(parsed.predicate.cast_modes(), "try");
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
                value: ScalarValue::Date32(_)
            } if column == "event_date"
        ));
        assert!(parsed.predicate.uses_date_literal());
        assert_eq!(parsed.predicate.family(), "cast");
        assert_eq!(parsed.predicate.cast_target_dtypes(), "date32");
        assert_eq!(parsed.predicate.cast_modes(), "strict");
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
                ref column,
                comparison: ComparisonOp::GtEq,
                value: ScalarValue::Int64(4)
            } if column == "label"
        ));
        assert_eq!(parsed.predicate.family(), "string_length");
        assert!(parsed.predicate.uses_string_length());
        assert_eq!(parsed.predicate.string_length_source_columns(), "label");
        assert_eq!(parsed.predicate.string_length_rhs_dtypes(), "int64");
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
        assert_eq!(parsed.predicate.family(), "numeric_abs");
        assert!(parsed.predicate.uses_numeric_abs());
        assert_eq!(parsed.predicate.numeric_abs_source_columns(), "amount");
        assert_eq!(parsed.predicate.numeric_abs_rhs_dtypes(), "int64");
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
        assert_eq!(parsed.predicate.family(), "numeric_rounding");
        assert!(parsed.predicate.uses_numeric_rounding());
        assert_eq!(parsed.predicate.numeric_rounding_operator(), "floor");
        assert_eq!(parsed.predicate.numeric_rounding_source_columns(), "amount");
        assert_eq!(parsed.predicate.numeric_rounding_rhs_dtypes(), "int64");
    }

    #[test]
    fn parses_generic_expression_predicate_statement() {
        let parsed = parse_sql_local_source_statement(
            "SELECT id,amount FROM 'target/input.csv' WHERE (amount + tax) * 2 >= 40 AND ABS(amount - tax) > 8 LIMIT 5",
        )
        .expect("generic expression predicate statement parses");

        assert_eq!(parsed.projections, vec!["id", "amount"]);
        assert_eq!(parsed.predicate.family(), "logical_predicate");
        assert!(parsed.predicate.uses_logical_predicate());
        assert!(parsed.predicate.uses_generic_expression());
        assert_eq!(parsed.predicate.logical_operator(), "and");
        assert_eq!(parsed.predicate.logical_leaf_count(), 2);
        assert_eq!(
            parsed.predicate.generic_expression_source_columns(),
            "amount+tax,amount+tax"
        );
        assert_eq!(
            parsed.predicate.generic_expression_operator_families(),
            "numeric_binary,numeric_abs+numeric_binary"
        );
        assert_eq!(
            parsed.predicate.generic_expression_binary_operator_count(),
            3
        );
        assert_eq!(
            parsed.predicate.generic_expression_comparison_operator(),
            "gte,gt"
        );
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
        assert_eq!(parsed.source_path, PathBuf::from("target/input.csv"));
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
        assert_eq!(parsed.predicate.family(), "in_predicate");
        assert!(parsed.predicate.uses_in_list());
        assert_eq!(parsed.predicate.in_list_value_count(), 2);
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
        assert_eq!(parsed.predicate.family(), "logical_predicate");
        assert!(parsed.predicate.uses_in_list());
        assert_eq!(parsed.predicate.in_list_value_count(), 2);
        assert_eq!(parsed.predicate.columns(), vec!["label"]);
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
        assert_eq!(parsed.predicate.family(), "in_predicate");
        assert!(parsed.predicate.uses_in_list());
        assert!(parsed.predicate.uses_date_literal());
        assert_eq!(parsed.predicate.in_list_value_count(), 2);
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
                && subquery.source_path == Path::new("target/allowed.csv")
                && subquery.values.is_empty()
        ));
        assert_eq!(parsed.predicate.family(), "in_subquery");
        assert!(parsed.predicate.uses_in_list());
        assert!(parsed.predicate.uses_in_subquery());
        assert_eq!(parsed.predicate.in_subquery_source_columns(), "id");
        assert_eq!(
            parsed.predicate.in_subquery_source_formats(),
            "not_materialized"
        );
    }

    #[test]
    fn in_subquery_counts_stay_separate_from_literal_in_counts() {
        let predicate = ParsedPredicate::Logical {
            op: LogicalPredicateOp::And,
            left: Box::new(ParsedPredicate::InList {
                column: "label".to_string(),
                values: vec![ScalarValue::Utf8("alpha".to_string())],
            }),
            right: Box::new(ParsedPredicate::InSubquery {
                column: "id".to_string(),
                subquery: Box::new(ParsedInSubquery {
                    source_column: "id".to_string(),
                    source_path: PathBuf::from("target/allowed.csv"),
                    source_format: Some(LocalSourceFormat::Csv),
                    source_digest: Some("digest".to_string()),
                    values: vec![ScalarValue::Int64(1), ScalarValue::Null],
                }),
            }),
        };

        assert_eq!(predicate.in_list_value_count(), 3);
        assert_eq!(predicate.in_list_null_value_count(), 1);
        assert_eq!(predicate.in_subquery_value_count(), 2);
        assert_eq!(predicate.in_subquery_null_value_count(), 1);
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
        assert_eq!(null_admitted.predicate.in_list_value_count(), 2);
        assert_eq!(null_admitted.predicate.in_list_null_value_count(), 1);
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
    fn parses_scoped_logical_and_predicate_statement() {
        let parsed = parse_sql_local_source_statement(
            "SELECT id,label FROM 'target/input.csv' WHERE amount >= 10 AND label LIKE '%ta' LIMIT 5",
        )
        .expect("logical AND statement parses");

        assert_eq!(parsed.predicate.family(), "logical_predicate");
        assert!(parsed.predicate.uses_logical_predicate());
        assert_eq!(parsed.predicate.logical_operator(), "and");
        assert_eq!(parsed.predicate.logical_leaf_count(), 2);
        assert!(parsed.predicate.uses_string_predicate());
        assert_eq!(parsed.predicate.string_operator(), "ends_with");
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

        assert_eq!(parsed.predicate.family(), "logical_predicate");
        assert!(parsed.predicate.uses_logical_predicate());
        assert_eq!(parsed.predicate.logical_operator(), "or");
        assert_eq!(parsed.predicate.logical_leaf_count(), 2);
        assert!(parsed.predicate.uses_string_predicate());
        assert_eq!(parsed.predicate.string_operator(), "ends_with");
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

        assert_eq!(parsed.predicate.family(), "logical_predicate");
        assert_eq!(parsed.predicate.logical_operator(), "or");
        assert_eq!(parsed.predicate.logical_leaf_count(), 3);
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

        assert_eq!(parsed.predicate.family(), "logical_predicate");
        assert!(parsed.predicate.uses_logical_predicate());
        assert_eq!(parsed.predicate.logical_operator(), "not");
        assert_eq!(parsed.predicate.logical_leaf_count(), 1);
        assert!(parsed.predicate.uses_string_predicate());
        assert_eq!(parsed.predicate.string_operator(), "ends_with");
        assert_eq!(parsed.predicate.columns(), vec!["label"]);
        assert!(matches!(parsed.predicate, ParsedPredicate::Not { .. }));
    }

    #[test]
    fn logical_not_preserves_or_precedence_without_fallback() {
        let parsed = parse_sql_local_source_statement(
            "SELECT id,label FROM 'target/input.csv' WHERE NOT id >= 1 OR amount >= 10 LIMIT 5",
        )
        .expect("logical NOT/OR statement parses");

        assert_eq!(parsed.predicate.family(), "logical_predicate");
        assert_eq!(parsed.predicate.logical_operator(), "or");
        assert_eq!(parsed.predicate.logical_leaf_count(), 2);
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

        assert_eq!(parsed.predicate.family(), "logical_predicate");
        assert!(parsed.predicate.uses_logical_predicate());
        assert_eq!(parsed.predicate.logical_operator(), "and");
        assert_eq!(parsed.predicate.logical_leaf_count(), 3);
        assert_eq!(parsed.predicate.string_operator(), "ends_with,starts_with");
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

        assert_eq!(parsed.predicate.family(), "logical_predicate");
        assert_eq!(parsed.predicate.logical_operator(), "and");
        assert_eq!(parsed.predicate.logical_leaf_count(), 3);
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
            "SELECT id FROM 'target/input.jsonl' WHERE CAST(amount AS decimal) >= 10 LIMIT 5",
        )
        .expect_err("decimal cast target remains blocked");

        assert!(error.to_string().contains("CAST target dtype"));
        assert!(error.to_string().contains("external_engine_invoked=false"));
    }

    #[test]
    fn parses_scoped_inner_equi_join_statement() {
        let parsed = parse_sql_local_source_statement(
            "SELECT f.id,d.segment FROM 'target/fact.csv' AS f JOIN 'target/dim.csv' AS d ON f.customer_id = d.customer_id WHERE f.amount >= 10 LIMIT 3",
        )
        .expect("join statement parses");

        assert_eq!(parsed.projections, vec!["f.id", "d.segment"]);
        assert!(parsed.aggregates.is_empty());
        assert!(parsed.group_by.is_empty());
        assert!(parsed.order_by.is_none());
        assert_eq!(parsed.source_path, PathBuf::from("target/fact.csv"));
        assert_eq!(parsed.source_alias.as_deref(), Some("f"));
        let join = parsed.join.as_ref().expect("join parsed");
        assert_eq!(join.right_source_path, PathBuf::from("target/dim.csv"));
        assert_eq!(join.right_alias, "d");
        assert_eq!(join.left_key_refs(), "f.customer_id");
        assert_eq!(join.right_key_refs(), "d.customer_id");
        assert_eq!(join.key_arity(), 1);
        assert!(!join.is_multi_key());
        assert_eq!(
            parsed.statement_kind(),
            "local_source_inner_equi_join_filter_limit"
        );
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
        assert_eq!(join.left_key_refs(), "f.customer_id,f.region");
        assert_eq!(join.right_key_refs(), "d.customer_id,d.region");
        assert_eq!(join.key_arity(), 2);
        assert!(join.is_multi_key());
        assert_eq!(
            parsed.statement_kind(),
            "local_source_inner_equi_join_filter_limit"
        );
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
        assert_eq!(
            parsed.generic_expression_projection_output_columns(),
            "adjusted"
        );
        assert_eq!(parsed.string_function_projections.len(), 1);
        assert_eq!(
            parsed.string_function_projection_source_columns(),
            "d.segment+f.region"
        );
        assert_eq!(
            parsed.string_function_projection_output_columns(),
            "segment_region"
        );
        let order_by = parsed.order_by.as_ref().expect("order by parsed");
        assert_eq!(order_by.columns_label(), "f.amount");
        assert_eq!(order_by.directions_label(), "desc");
        assert_eq!(order_by.operator_family_label(), "single_key_scalar_topn");
        assert_eq!(order_by.keys[0].column, "f.amount");
        assert_eq!(order_by.keys[0].direction, SortDirection::Desc);
        assert_eq!(
            parsed.statement_kind(),
            "local_source_inner_equi_join_computed_projection_order_by_topn_filter_limit"
        );
        assert_eq!(
            parsed.execution_certificate_suffix(),
            "inner-equi-join-computed-projection-order-by-topn-filter-limit"
        );
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
        assert_eq!(parsed.source_path, PathBuf::from("target/fact.csv"));
        assert_eq!(parsed.source_alias.as_deref(), Some("f"));
        let join = parsed.join.as_ref().expect("join parsed");
        assert_eq!(join.right_source_path, PathBuf::from("target/dim.csv"));
        assert_eq!(join.right_alias, "d");
        assert_eq!(join.left_key_refs(), "f.customer_id,f.region");
        assert_eq!(join.right_key_refs(), "d.customer_id,d.region");
        assert_eq!(join.key_arity(), 2);
        assert!(join.is_multi_key());
        assert_eq!(parsed.aggregates.len(), 2);
        assert_eq!(parsed.aggregates[0].label(), "sum(f.amount)");
        assert_eq!(parsed.aggregates[0].output_name(), "total_amount");
        assert_eq!(parsed.aggregates[0].column.as_deref(), Some("f.amount"));
        assert_eq!(parsed.aggregates[1].label(), "count(*)");
        assert_eq!(parsed.aggregates[1].output_name(), "rows");
        assert_eq!(
            parsed.statement_kind(),
            "local_source_inner_equi_join_group_by_aggregate_filter_limit"
        );
        assert_eq!(
            parsed.execution_certificate_suffix(),
            "inner-equi-join-group-by-aggregate-filter-limit"
        );
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
    fn source_read_plan_collects_sql_required_columns() {
        let parsed = parse_sql_local_source_statement(
            "SELECT id,amount * 2 AS doubled FROM 'target/input.csv' WHERE label = 'alpha' LIMIT 5",
        )
        .expect("statement parses");

        let plan = source_read_plan_for_sql(&parsed);

        assert_eq!(plan.status(), "required_columns");
        assert_eq!(plan.reason, "sql_required_source_columns");
        assert_eq!(plan.requested_columns(), "amount,id,label");
        assert_eq!(
            plan.materialized_columns(&["id".into(), "label".into(), "amount".into()]),
            vec!["id", "label", "amount"]
        );
    }

    #[test]
    fn csv_source_read_plan_materializes_required_columns_only() {
        let plan = LocalSourceReadPlan::required(
            BTreeSet::from(["id".to_string(), "amount".to_string()]),
            "test_required_columns",
        );

        let (header, rows) =
            parse_csv_source_content_with_plan("id,label,amount\n1,alpha,8\n", &plan)
                .expect("CSV parses with read plan");

        assert_eq!(header, vec!["id", "label", "amount"]);
        assert_eq!(rows[0].get("id"), Some(&ScalarValue::Int64(1)));
        assert_eq!(rows[0].get("amount"), Some(&ScalarValue::Int64(8)));
        assert!(!rows[0].contains_key("label"));
    }

    #[test]
    fn jsonl_parser_handles_flat_scalar_rows() {
        let (header, rows) = parse_jsonl_source_content(
            "{\"id\":1,\"label\":\"alpha\",\"active\":true,\"event_date\":\"2026-05-19\"}\n\
             {\"id\":2,\"label\":\"beta\",\"active\":false,\"score\":2.5}\n",
        )
        .expect("jsonl parses");

        assert_eq!(header, vec!["id", "label", "active", "event_date", "score"]);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].get("id"), Some(&ScalarValue::Int64(1)));
        assert_eq!(
            rows[0].get("label"),
            Some(&ScalarValue::Utf8("alpha".into()))
        );
        assert_eq!(rows[0].get("active"), Some(&ScalarValue::Boolean(true)));
        assert_eq!(
            rows[0].get("event_date"),
            Some(&ScalarValue::Utf8("2026-05-19".into()))
        );
        assert_eq!(rows[0].get("score"), Some(&ScalarValue::Null));
        assert_eq!(rows[1].get("score"), Some(&ScalarValue::Float64(2.5)));
    }

    #[test]
    fn jsonl_parser_handles_leading_utf8_bom() {
        let (header, rows) = parse_jsonl_source_content("\u{feff}{\"id\":1,\"label\":\"alpha\"}\n")
            .expect("jsonl with leading BOM parses");

        assert_eq!(header, vec!["id", "label"]);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get("id"), Some(&ScalarValue::Int64(1)));
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
    fn jsonl_parser_blocks_nested_values() {
        let error = parse_jsonl_source_content("{\"id\":1,\"payload\":{\"x\":1}}\n")
            .expect_err("nested object is blocked");
        assert!(error.to_string().contains("scalar values only"));
    }

    #[test]
    fn json_parser_blocks_nested_values() {
        let error = parse_json_source_content("[{\"id\":1,\"payload\":{\"x\":1}}]")
            .expect_err("nested object is blocked");
        assert!(error.to_string().contains("scalar values only"));
    }
}
