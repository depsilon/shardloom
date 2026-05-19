//! Scoped local-source SQL runtime smoke.
//!
//! This module intentionally admits one small SQL shape over local CSV/JSONL:
//! `SELECT <columns> FROM <local.csv|local.jsonl> [WHERE <scoped predicate>] [ORDER BY <column> ASC|DESC] LIMIT <n>`
//! plus one explicit local inner equi-join shape.
//! It uses ShardLoom-owned parsing/binding plus the core expression semantics
//! baseline. It does not invoke `DataFusion`, `DuckDB`, `SQLite`, `Spark`,
//! `Polars`, `pandas`, object stores, catalogs, or Vortex query-engine
//! integrations.

use std::{
    cmp::Ordering,
    collections::BTreeMap,
    fmt::Write as _,
    fs,
    path::{Path, PathBuf},
    process::ExitCode,
    time::Instant,
};

use shardloom_core::{
    BinaryOp, ColumnRef, CommandStatus, ComparisonOp, ExprId, Expression, ExpressionInputRow,
    ExpressionKind, LogicalDType, OutputFormat, ScalarValue, ShardLoomError, UnaryOp,
    evaluate_filter, evaluate_projection, format_iso_date32, parse_iso_date32,
};

use crate::{
    cli_output::{emit, emit_error},
    cli_unknown_arg_error,
};

const COMMAND: &str = "sql-local-source-smoke";
const SCHEMA_VERSION: &str = "shardloom.sql_local_source_smoke.v1";
const JSONL_OUTPUT_CERTIFICATE_ID: &str = "sql-local-source.csv.local-jsonl-output.native-io.v1";
const CSV_OUTPUT_CERTIFICATE_ID: &str = "sql-local-source.csv.local-csv-output.native-io.v1";
const MAX_INPUT_ROWS: usize = 50_000;
const MAX_LIMIT_ROWS: usize = 10_000;
const MAX_JOIN_CANDIDATE_ROWS: usize = MAX_INPUT_ROWS;
const MAX_IN_LIST_VALUES: usize = 32;

#[derive(Debug, Clone, PartialEq, Eq)]
struct SqlLocalSourceRequest {
    statement: String,
    output_format: SqlLocalSourceOutputFormat,
    output_path: Option<PathBuf>,
    allow_overwrite: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SqlLocalSourceOutputFormat {
    InlineJsonl,
    Csv,
}

impl SqlLocalSourceOutputFormat {
    fn parse(value: &str) -> Result<Self, ShardLoomError> {
        match value.trim().to_ascii_lowercase().as_str() {
            "inline-jsonl" | "jsonl" | "json-lines" | "ndjson" => Ok(Self::InlineJsonl),
            "csv" => Ok(Self::Csv),
            other => Err(ShardLoomError::InvalidOperation(format!(
                "unsupported SQL local-source output format {other:?}; scoped local SQL supports local JSONL or CSV only"
            ))),
        }
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::InlineJsonl => "inline_jsonl",
            Self::Csv => "csv",
        }
    }

    const fn sink_format(self) -> &'static str {
        match self {
            Self::InlineJsonl => "jsonl",
            Self::Csv => "csv",
        }
    }

    const fn certificate_status(self) -> &'static str {
        match self {
            Self::InlineJsonl => "certified_local_jsonl_sink",
            Self::Csv => "certified_local_csv_sink",
        }
    }

    const fn certificate_ref(self) -> &'static str {
        match self {
            Self::InlineJsonl => JSONL_OUTPUT_CERTIFICATE_ID,
            Self::Csv => CSV_OUTPUT_CERTIFICATE_ID,
        }
    }

    fn render_rows(self, columns: &[String], rows: &[Vec<(String, ScalarValue)>]) -> String {
        match self {
            Self::InlineJsonl => rows_to_jsonl(rows),
            Self::Csv => rows_to_csv(columns, rows),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
struct ParsedSqlLocalSource {
    projections: Vec<String>,
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
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ParsedOrderBy {
    column: String,
    direction: SortDirection,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ParsedJoin {
    right_source_path: PathBuf,
    right_alias: String,
    left_key: QualifiedColumn,
    right_key: QualifiedColumn,
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

#[derive(Debug, Clone, Copy, PartialEq)]
enum SortValue {
    Int(i64),
    Float(f64),
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
            ScalarValue::Null => Err(unsupported_sql_error(
                "ORDER BY NULL ordering is not admitted in this scoped top-N smoke",
            )),
            _ => Err(unsupported_sql_error(
                "ORDER BY top-N smoke admits numeric sort columns only",
            )),
        }
    }
}

impl Eq for SortValue {}

impl Ord for SortValue {
    fn cmp(&self, other: &Self) -> Ordering {
        self.as_f64().total_cmp(&other.as_f64())
    }
}

impl PartialOrd for SortValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl SortValue {
    fn as_f64(self) -> f64 {
        match self {
            Self::Int(value) => i64_to_f64(value),
            Self::Float(value) => value,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ParsedProjectionList {
    projections: Vec<String>,
    aggregates: Vec<ParsedAggregate>,
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
        op: ComparisonOp,
        value: ScalarValue,
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
    StringMatch {
        column: String,
        op: StringPredicateOp,
        value: String,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LocalSourceFormat {
    Csv,
    JsonLines,
}

impl LocalSourceFormat {
    fn from_path(path: &Path) -> Result<Self, ShardLoomError> {
        let Some(extension) = path.extension().and_then(|value| value.to_str()) else {
            return Err(unsupported_sql_error(
                "GAR-RUNTIME-IMPL-4F admits local CSV and JSONL/NDJSON sources only in this slice",
            ));
        };
        match extension.to_ascii_lowercase().as_str() {
            "csv" => Ok(Self::Csv),
            "jsonl" | "ndjson" => Ok(Self::JsonLines),
            _ => Err(unsupported_sql_error(
                "GAR-RUNTIME-IMPL-4F admits local CSV and JSONL/NDJSON sources only in this slice",
            )),
        }
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::Csv => "csv",
            Self::JsonLines => "jsonl",
        }
    }

    const fn row_label(self) -> &'static str {
        match self {
            Self::Csv => "CSV",
            Self::JsonLines => "JSONL",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
struct CsvSourceData {
    source_format: LocalSourceFormat,
    header: Vec<String>,
    rows: Vec<ExpressionInputRow>,
    source_bytes: u64,
    source_digest: String,
    read_millis: u128,
    parse_millis: u128,
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
    operator_compute_millis: u128,
    evidence_render_millis: u128,
    total_runtime_millis: u128,
}

pub(crate) fn handle_sql_local_source_smoke(
    mut args: impl Iterator<Item = String>,
    format: OutputFormat,
) -> ExitCode {
    let Some(statement_raw) = args.next() else {
        eprintln!(
            "usage: shardloom {COMMAND} <sql-statement> [--output-format inline-jsonl|csv] [--output local.jsonl|local.csv] [--allow-overwrite] [--format text|json]"
        );
        return ExitCode::from(2);
    };

    let mut output_format = SqlLocalSourceOutputFormat::InlineJsonl;
    let mut output_path = None;
    let mut allow_overwrite = false;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--output-format" => {
                let Some(value) = args.next() else {
                    return emit_error(
                        COMMAND,
                        format,
                        "SQL local-source smoke failed",
                        &ShardLoomError::InvalidOperation(
                            "--output-format requires a value".to_string(),
                        ),
                    );
                };
                output_format = match SqlLocalSourceOutputFormat::parse(&value) {
                    Ok(parsed) => parsed,
                    Err(error) => {
                        return emit_error(
                            COMMAND,
                            format,
                            "SQL local-source smoke failed",
                            &error,
                        );
                    }
                };
            }
            "--output" => {
                let Some(value) = args.next() else {
                    return emit_error(
                        COMMAND,
                        format,
                        "SQL local-source smoke failed",
                        &ShardLoomError::InvalidOperation("--output requires a value".to_string()),
                    );
                };
                output_path = match normalize_local_output_path(&value) {
                    Ok(parsed) => Some(parsed),
                    Err(error) => {
                        return emit_error(
                            COMMAND,
                            format,
                            "SQL local-source smoke failed",
                            &error,
                        );
                    }
                };
            }
            "--allow-overwrite" => allow_overwrite = true,
            extra => {
                return emit_error(
                    COMMAND,
                    format,
                    "SQL local-source smoke failed",
                    &cli_unknown_arg_error(COMMAND, extra),
                );
            }
        }
    }

    if let Err(error) =
        validate_sql_local_source_output_request(output_format, output_path.as_deref())
    {
        return emit_error(COMMAND, format, "SQL local-source smoke failed", &error);
    }

    let request = SqlLocalSourceRequest {
        statement: statement_raw,
        output_format,
        output_path,
        allow_overwrite,
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

fn validate_sql_local_source_output_request(
    output_format: SqlLocalSourceOutputFormat,
    output_path: Option<&Path>,
) -> Result<(), ShardLoomError> {
    if output_path.is_none() && matches!(output_format, SqlLocalSourceOutputFormat::Csv) {
        return Err(ShardLoomError::InvalidOperation(
            "SQL local-source CSV output requires --output <local.csv>".to_string(),
        ));
    }
    Ok(())
}

fn output_column_names(parsed: &ParsedSqlLocalSource, source: &CsvSourceData) -> Vec<String> {
    if parsed.is_grouped_aggregate() {
        let mut columns = parsed.group_by.clone();
        columns.extend(parsed.aggregates.iter().map(ParsedAggregate::output_name));
        return columns;
    }
    if parsed.is_aggregate() {
        return parsed
            .aggregates
            .iter()
            .map(ParsedAggregate::output_name)
            .collect();
    }
    if parsed.join.is_some() {
        return parsed.projections.clone();
    }
    parsed.projection_columns(&source.header)
}

fn run_sql_local_source_smoke(
    request: &SqlLocalSourceRequest,
) -> Result<SqlLocalSourceReport, ShardLoomError> {
    let total_start = Instant::now();
    let parsed = parse_sql_local_source_statement(&request.statement)?;
    let mut source = read_local_source(&parsed.source_path)?;
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
    apply_date_literal_column_coercions(&parsed, &mut source, right_source.as_mut())?;

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
                evaluate_grouped_aggregate_output(&parsed, &source, &selected_row_indexes)?
            } else if parsed.is_aggregate() {
                evaluate_scalar_aggregate_output(&parsed, &source, &selected_row_indexes)?
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
    let output_content = request
        .output_format
        .render_rows(&output_columns, &output_rows);
    let output_digest = fnv64_digest(&output_content);
    let source_schema_digest = fnv64_digest(&source.header.join(","));
    let plan_digest = fnv64_digest(&format!(
        "{}|{}|{}|{}|{}",
        parsed.normalized_statement,
        source_schema_digest,
        source.source_digest,
        right_source
            .as_ref()
            .map_or_else(String::new, |source| source.source_digest.clone()),
        request.output_format.as_str()
    ));
    let evidence_render_millis = evidence_start.elapsed().as_millis();
    let output_bytes = u64::try_from(output_content.len()).unwrap_or(u64::MAX);
    let output_write_millis = write_optional_sql_output(request, &output_content)?;

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
        operator_compute_millis,
        evidence_render_millis,
        total_runtime_millis: total_start.elapsed().as_millis(),
    })
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
                    .message
                    .as_str())
        )));
    }
    Ok(filter.selected_row_indexes)
}

fn evaluate_projection_output(
    parsed: &ParsedSqlLocalSource,
    source: &CsvSourceData,
    selected_row_indexes: &[usize],
) -> Result<Vec<Vec<(String, ScalarValue)>>, ShardLoomError> {
    let projection_expressions = parsed
        .projection_columns(&source.header)
        .iter()
        .map(|column| {
            Ok(Expression::column(
                ExprId::new(format!("project.{column}"))?,
                ColumnRef::new(column.clone())?,
            ))
        })
        .collect::<Result<Vec<_>, ShardLoomError>>()?;
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
                        .message
                        .as_str())
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
        let value = row.get(&order_by.column).ok_or_else(|| {
            unsupported_sql_error(&format!(
                "ORDER BY column {:?} is not present in the CSV row",
                order_by.column
            ))
        })?;
        sort_values.push((*row_index, SortValue::try_from_scalar(value)?));
    }
    sort_values.sort_by(|(left_index, left_value), (right_index, right_value)| {
        let ordering = left_value.cmp(right_value);
        let ordering = match order_by.direction {
            SortDirection::Asc => ordering,
            SortDirection::Desc => ordering.reverse(),
        };
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
    let projection_expressions = parsed
        .projections
        .iter()
        .map(|column| {
            Ok(Expression::column(
                ExprId::new(format!("join.project.{column}"))?,
                ColumnRef::new(column.clone())?,
            ))
        })
        .collect::<Result<Vec<_>, ShardLoomError>>()?;
    let mut joined_row_count = 0usize;
    let mut selected_row_count = 0usize;
    let mut output_rows = Vec::new();
    for left_row in &left_source.rows {
        let Some(key_value) = left_row.get(&join.left_key.column) else {
            return Err(unsupported_sql_error(&format!(
                "JOIN left key column {:?} is not present in the left CSV row",
                join.left_key.column
            )));
        };
        if matches!(key_value, ScalarValue::Null) {
            continue;
        }
        if let Some(right_matches) = right_rows_by_key.get(&key_value.summary()) {
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
                if evaluate_join_candidate(
                    predicate_expression.as_ref(),
                    &projection_expressions,
                    &joined_row,
                    parsed.limit,
                    &mut output_rows,
                )? {
                    selected_row_count += 1;
                }
            }
        }
    }

    Ok(JoinEvaluationOutput {
        joined_row_count,
        selected_row_count,
        output_rows,
    })
}

fn build_join_right_rows_by_key<'a>(
    join: &ParsedJoin,
    right_source: &'a CsvSourceData,
) -> Result<BTreeMap<String, Vec<&'a ExpressionInputRow>>, ShardLoomError> {
    let mut right_rows_by_key: BTreeMap<String, Vec<&ExpressionInputRow>> = BTreeMap::new();
    for right_row in &right_source.rows {
        let Some(key_value) = right_row.get(&join.right_key.column) else {
            return Err(unsupported_sql_error(&format!(
                "JOIN right key column {:?} is not present in the right CSV row",
                join.right_key.column
            )));
        };
        if matches!(key_value, ScalarValue::Null) {
            continue;
        }
        right_rows_by_key
            .entry(key_value.summary())
            .or_default()
            .push(right_row);
    }
    Ok(right_rows_by_key)
}

fn evaluate_join_candidate(
    predicate_expression: Option<&Expression>,
    projection_expressions: &[Expression],
    joined_row: &ExpressionInputRow,
    output_limit: usize,
    output_rows: &mut Vec<Vec<(String, ScalarValue)>>,
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
    if output_rows.len() < output_limit {
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
        output_rows.push(
            projection
                .projected_columns
                .into_iter()
                .map(|column| (column.name, column.value))
                .collect(),
        );
    }
    Ok(true)
}

fn apply_date_literal_column_coercions(
    parsed: &ParsedSqlLocalSource,
    source: &mut CsvSourceData,
    right_source: Option<&mut CsvSourceData>,
) -> Result<(), ShardLoomError> {
    apply_date_literal_predicate_coercions(&parsed.predicate, parsed, source, right_source)
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
        } => coerce_date_literal_column(column, parsed, source, right_source),
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
        | ParsedPredicate::IsNull { .. }
        | ParsedPredicate::IsNotNull { .. }
        | ParsedPredicate::InList { .. }
        | ParsedPredicate::StringMatch { .. } => Ok(()),
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
            "DATE literal predicate column {column:?} is not present in the local source"
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
                        "DATE literal predicate column {column:?} requires ISO YYYY-MM-DD strings or nulls"
                    ))
                })?;
                *value = ScalarValue::Date32(parsed);
            }
            other => {
                return Err(unsupported_sql_error(&format!(
                    "DATE literal predicate column {column:?} requires ISO date strings or nulls, got {}",
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
    source: &CsvSourceData,
    selected_row_indexes: &[usize],
) -> Result<Vec<Vec<(String, ScalarValue)>>, ShardLoomError> {
    if parsed.limit == 0 {
        return Ok(Vec::new());
    }
    let mut row = Vec::with_capacity(parsed.aggregates.len());
    for aggregate in &parsed.aggregates {
        row.push((
            aggregate.output_name(),
            evaluate_scalar_aggregate(aggregate, source, selected_row_indexes)?,
        ));
    }
    Ok(vec![row])
}

fn evaluate_grouped_aggregate_output(
    parsed: &ParsedSqlLocalSource,
    source: &CsvSourceData,
    selected_row_indexes: &[usize],
) -> Result<Vec<Vec<(String, ScalarValue)>>, ShardLoomError> {
    if parsed.limit == 0 {
        return Ok(Vec::new());
    }
    let mut groups: BTreeMap<String, GroupedAggregateBucket> = BTreeMap::new();
    for row_index in selected_row_indexes {
        let row = source.rows.get(*row_index).ok_or_else(|| {
            ShardLoomError::InvalidOperation("selected row index is out of bounds".to_string())
        })?;
        let group_values = parsed
            .group_by
            .iter()
            .map(|column| {
                let value = row.get(column).cloned().ok_or_else(|| {
                    unsupported_sql_error(&format!(
                        "GROUP BY column {column:?} is not present in the CSV row"
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
        entry.row_indexes.push(*row_index);
    }

    let mut output_rows = Vec::new();
    for (_key, bucket) in groups.into_iter().take(parsed.limit) {
        let mut row = bucket.values;
        for aggregate in &parsed.aggregates {
            row.push((
                aggregate.output_name(),
                evaluate_scalar_aggregate(aggregate, source, &bucket.row_indexes)?,
            ));
        }
        output_rows.push(row);
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
    source: &CsvSourceData,
    selected_row_indexes: &[usize],
) -> Result<ScalarValue, ShardLoomError> {
    match aggregate.function {
        AggregateFunction::Count => {
            let count = if let Some(column) = aggregate.column.as_deref() {
                selected_row_indexes
                    .iter()
                    .filter_map(|row_index| source.rows.get(*row_index))
                    .filter(|row| !matches!(row.get(column), None | Some(ScalarValue::Null)))
                    .count()
            } else {
                selected_row_indexes.len()
            };
            i64::try_from(count).map(ScalarValue::Int64).map_err(|_| {
                unsupported_sql_error("COUNT result does not fit in int64 for this scoped smoke")
            })
        }
        AggregateFunction::Sum => aggregate_numeric_sum(aggregate, source, selected_row_indexes),
        AggregateFunction::Avg => aggregate_numeric_avg(aggregate, source, selected_row_indexes),
        AggregateFunction::Min => {
            aggregate_numeric_min_max(aggregate, source, selected_row_indexes, MinMaxMode::Min)
        }
        AggregateFunction::Max => {
            aggregate_numeric_min_max(aggregate, source, selected_row_indexes, MinMaxMode::Max)
        }
    }
}

fn aggregate_numeric_sum(
    aggregate: &ParsedAggregate,
    source: &CsvSourceData,
    selected_row_indexes: &[usize],
) -> Result<ScalarValue, ShardLoomError> {
    let column = aggregate.required_column()?;
    let mut int_sum = 0_i64;
    let mut float_sum = 0.0_f64;
    let mut saw_float = false;
    let mut count = 0_usize;
    for value in aggregate_numeric_values(column, source, selected_row_indexes)? {
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
    source: &CsvSourceData,
    selected_row_indexes: &[usize],
) -> Result<ScalarValue, ShardLoomError> {
    let column = aggregate.required_column()?;
    let mut sum = 0.0_f64;
    let mut count = 0_usize;
    for value in aggregate_numeric_values(column, source, selected_row_indexes)? {
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
    source: &CsvSourceData,
    selected_row_indexes: &[usize],
    mode: MinMaxMode,
) -> Result<ScalarValue, ShardLoomError> {
    let column = aggregate.required_column()?;
    let mut selected: Option<NumericAggregateValue> = None;
    for value in aggregate_numeric_values(column, source, selected_row_indexes)? {
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
    source: &CsvSourceData,
    selected_row_indexes: &[usize],
) -> Result<Vec<NumericAggregateValue>, ShardLoomError> {
    let mut values = Vec::new();
    for row_index in selected_row_indexes {
        let row = source.rows.get(*row_index).ok_or_else(|| {
            ShardLoomError::InvalidOperation("selected row index is out of bounds".to_string())
        })?;
        let Some(value) = row.get(column) else {
            return Err(unsupported_sql_error(&format!(
                "aggregate column {column:?} is not present in the CSV row"
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

fn write_optional_sql_output(
    request: &SqlLocalSourceRequest,
    output_content: &str,
) -> Result<u128, ShardLoomError> {
    let Some(output_path) = request.output_path.as_ref() else {
        return Ok(0);
    };
    if output_path.exists() && !request.allow_overwrite {
        return Err(ShardLoomError::InvalidOperation(format!(
            "SQL local-source output path already exists: {}; pass --allow-overwrite to replace it",
            output_path.display()
        )));
    }
    if let Some(parent) = output_path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent).map_err(|error| {
                ShardLoomError::Message(format!(
                    "failed to create local SQL output directory {}: {error}",
                    parent.display()
                ))
            })?;
        }
    }
    let write_start = Instant::now();
    fs::write(output_path, output_content.as_bytes()).map_err(|error| {
        ShardLoomError::Message(format!(
            "failed to write local SQL output {}: {error}",
            output_path.display()
        ))
    })?;
    Ok(write_start.elapsed().as_millis())
}

fn bind_sql_local_source(
    parsed: &ParsedSqlLocalSource,
    header: &[String],
    right_header: Option<&[String]>,
) -> Result<(), ShardLoomError> {
    if parsed.is_join() {
        return bind_join_sql_local_source(parsed, header, right_header);
    }
    if let Some(order_by) = parsed.order_by.as_ref() {
        if parsed.is_aggregate() || parsed.is_grouped_aggregate() {
            return Err(unsupported_sql_error(
                "ORDER BY top-N smoke currently admits projection rows only; aggregate and grouped top-N remain blocked",
            ));
        }
        if !header.iter().any(|candidate| candidate == &order_by.column) {
            return Err(unsupported_sql_error(&format!(
                "ORDER BY column {:?} is not present in the CSV header",
                order_by.column
            )));
        }
    }
    if parsed.is_grouped_aggregate() {
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
        for aggregate in &parsed.aggregates {
            if let Some(column) = aggregate.column.as_deref() {
                if !header.iter().any(|candidate| candidate == column) {
                    return Err(unsupported_sql_error(&format!(
                        "aggregate column {column:?} is not present in the CSV header"
                    )));
                }
            }
        }
    } else if parsed.is_aggregate() {
        if !parsed.projections.is_empty() {
            return Err(unsupported_sql_error(
                "scalar aggregate SELECT list cannot mix aggregate functions with raw columns in this scoped smoke",
            ));
        }
        for aggregate in &parsed.aggregates {
            if let Some(column) = aggregate.column.as_deref() {
                if !header.iter().any(|candidate| candidate == column) {
                    return Err(unsupported_sql_error(&format!(
                        "aggregate column {column:?} is not present in the CSV header"
                    )));
                }
            }
        }
    } else {
        for column in parsed.projection_columns(header) {
            if !header.iter().any(|candidate| candidate == &column) {
                return Err(unsupported_sql_error(&format!(
                    "projection column {column:?} is not present in the CSV header"
                )));
            }
        }
    }
    if !parsed.group_by.is_empty() && !parsed.is_grouped_aggregate() {
        return Err(unsupported_sql_error(
            "GROUP BY requires at least one aggregate function in this scoped smoke",
        ));
    }
    for predicate_column in parsed.predicate.columns() {
        if !header.iter().any(|candidate| candidate == predicate_column) {
            return Err(unsupported_sql_error(&format!(
                "predicate column {predicate_column:?} is not present in the CSV header"
            )));
        }
    }
    Ok(())
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
            "JOIN smoke requires a readable local right CSV source",
        ));
    };
    if parsed.is_aggregate()
        || !parsed.group_by.is_empty()
        || parsed.order_by.is_some()
        || parsed.projections.is_empty()
    {
        return Err(unsupported_sql_error(
            "JOIN smoke currently admits projection/filter/limit only; aggregate, group-by, and order-by joins remain blocked",
        ));
    }
    if join.left_key.alias != *left_alias || join.right_key.alias != join.right_alias {
        return Err(unsupported_sql_error(
            "JOIN ON must compare the left alias to the right alias in this scoped smoke",
        ));
    }
    if !left_header
        .iter()
        .any(|column| column == &join.left_key.column)
    {
        return Err(unsupported_sql_error(&format!(
            "JOIN left key column {:?} is not present in the left CSV header",
            join.left_key.column
        )));
    }
    if !right_header
        .iter()
        .any(|column| column == &join.right_key.column)
    {
        return Err(unsupported_sql_error(&format!(
            "JOIN right key column {:?} is not present in the right CSV header",
            join.right_key.column
        )));
    }
    for projection in &parsed.projections {
        bind_qualified_column(
            projection,
            left_alias,
            left_header,
            &join.right_alias,
            right_header,
        )?;
    }
    bind_qualified_predicate(
        &parsed.predicate,
        left_alias,
        left_header,
        &join.right_alias,
        right_header,
    )
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
                "qualified left column {column_ref:?} is not present in the left CSV header"
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
                "qualified right column {column_ref:?} is not present in the right CSV header"
            )))
        }
    } else {
        Err(unsupported_sql_error(&format!(
            "qualified column {column_ref:?} does not use an admitted JOIN alias"
        )))
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

    fn statement_kind(&self) -> &'static str {
        if self.is_join() && self.has_filter() {
            "local_source_inner_equi_join_filter_limit"
        } else if self.is_join() {
            "local_source_inner_equi_join_limit"
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
        } else if self.has_filter() {
            "local_source_projection_filter_limit"
        } else {
            "local_source_projection_limit"
        }
    }

    fn execution_certificate_suffix(&self) -> &'static str {
        if self.is_join() && self.has_filter() {
            "inner-equi-join-filter-limit"
        } else if self.is_join() {
            "inner-equi-join-limit"
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
        } else if self.has_filter() {
            "projection-filter-limit"
        } else {
            "projection-limit"
        }
    }

    fn claim_gate_reason_suffix(&self) -> &'static str {
        if self.is_join() && self.has_filter() {
            "inner_equi_join_filter_limit"
        } else if self.is_join() {
            "inner_equi_join_limit"
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
        if self.is_join() {
            self.projections.clone()
        } else if self.is_grouped_aggregate() {
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
            self.projection_columns(header)
        }
    }
}

impl ParsedAggregate {
    fn output_name(&self) -> String {
        match (self.function, self.column.as_deref()) {
            (AggregateFunction::Count, None) => "count_all".to_string(),
            (function, Some(column)) => format!("{}_{}", function.as_str(), column),
            (function, None) => function.as_str().to_string(),
        }
    }

    fn label(&self) -> String {
        match (self.function, self.column.as_deref()) {
            (AggregateFunction::Count, None) => "count(*)".to_string(),
            (function, Some(column)) => format!("{}({column})", function.as_str()),
            (function, None) => format!("{}()", function.as_str()),
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
    fn direction_label(&self) -> &'static str {
        self.direction.as_str()
    }
}

impl QualifiedColumn {
    fn to_ref(&self) -> String {
        qualified_column_name(&self.alias, &self.column)
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
            | Self::IsNull { column }
            | Self::IsNotNull { column }
            | Self::InList { column, .. }
            | Self::StringMatch { column, .. } => columns.push(column),
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
            Self::Compare { column, op, value } => Ok(Expression::new(
                ExprId::new("where.compare")?,
                ExpressionKind::Compare {
                    left: Box::new(Expression::column(
                        ExprId::new(format!("where.{column}"))?,
                        ColumnRef::new(column.clone())?,
                    )),
                    op: *op,
                    right: Box::new(Expression::literal(
                        ExprId::new("where.literal")?,
                        value.clone(),
                    )),
                },
            )),
            Self::CastCompare {
                column,
                target_dtype,
                op,
                value,
            } => Ok(Expression::new(
                ExprId::new("where.cast_compare")?,
                ExpressionKind::Compare {
                    left: Box::new(Expression::cast(
                        ExprId::new(format!("where.cast.{column}"))?,
                        Expression::column(
                            ExprId::new(format!("where.{column}"))?,
                            ColumnRef::new(column.clone())?,
                        ),
                        target_dtype.clone(),
                    )),
                    op: *op,
                    right: Box::new(Expression::literal(
                        ExprId::new("where.cast.literal")?,
                        value.clone(),
                    )),
                },
            )),
            Self::IsNull { column } => Ok(Expression::new(
                ExprId::new("where.is_null")?,
                ExpressionKind::Unary {
                    op: UnaryOp::IsNull,
                    expr: Box::new(Expression::column(
                        ExprId::new(format!("where.{column}"))?,
                        ColumnRef::new(column.clone())?,
                    )),
                },
            )),
            Self::IsNotNull { column } => Ok(Expression::new(
                ExprId::new("where.is_not_null")?,
                ExpressionKind::Unary {
                    op: UnaryOp::IsNotNull,
                    expr: Box::new(Expression::column(
                        ExprId::new(format!("where.{column}"))?,
                        ColumnRef::new(column.clone())?,
                    )),
                },
            )),
            Self::InList { column, values } => in_list_expression(column, values),
            Self::StringMatch { column, op, value } => Ok(Expression::new(
                ExprId::new(format!("where.string.{}", op.as_str()))?,
                ExpressionKind::FunctionCall {
                    name: op.function_name().to_string(),
                    args: vec![
                        Expression::column(
                            ExprId::new(format!("where.{column}"))?,
                            ColumnRef::new(column.clone())?,
                        ),
                        Expression::literal(
                            ExprId::new("where.string.literal")?,
                            ScalarValue::Utf8(value.clone()),
                        ),
                    ],
                },
            )),
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

    fn family(&self) -> &'static str {
        match self {
            Self::All => "none",
            Self::Compare { .. } => "comparison",
            Self::CastCompare { .. } => "cast",
            Self::IsNull { .. } | Self::IsNotNull { .. } => "null_predicate",
            Self::InList { .. } => "in_predicate",
            Self::StringMatch { .. } => "string_predicate",
            Self::Logical { .. } | Self::Not { .. } => "logical_predicate",
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
            | Self::IsNull { .. }
            | Self::IsNotNull { .. }
            | Self::InList { .. } => false,
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
            | Self::IsNull { .. }
            | Self::IsNotNull { .. }
            | Self::InList { .. } => {}
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
            } => true,
            Self::InList { values, .. } => values
                .iter()
                .any(|value| matches!(value, ScalarValue::Date32(_))),
            Self::Logical { left, right, .. } => {
                left.uses_date_literal() || right.uses_date_literal()
            }
            Self::Not { inner } => inner.uses_date_literal(),
            Self::All
            | Self::Compare { .. }
            | Self::CastCompare { .. }
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
            | Self::IsNull { .. }
            | Self::IsNotNull { .. }
            | Self::InList { .. }
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
            | Self::IsNull { .. }
            | Self::IsNotNull { .. }
            | Self::InList { .. }
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
            | Self::IsNull { .. }
            | Self::IsNotNull { .. }
            | Self::InList { .. }
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
            | Self::IsNull { .. }
            | Self::IsNotNull { .. }
            | Self::InList { .. }
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
            | Self::IsNull { .. }
            | Self::IsNotNull { .. }
            | Self::InList { .. }
            | Self::StringMatch { .. } => 1,
        }
    }

    fn uses_in_list(&self) -> bool {
        match self {
            Self::InList { .. } => true,
            Self::Logical { left, right, .. } => left.uses_in_list() || right.uses_in_list(),
            Self::Not { inner } => inner.uses_in_list(),
            Self::All
            | Self::Compare { .. }
            | Self::CastCompare { .. }
            | Self::IsNull { .. }
            | Self::IsNotNull { .. }
            | Self::StringMatch { .. } => false,
        }
    }

    fn in_list_value_count(&self) -> usize {
        match self {
            Self::InList { values, .. } => values.len(),
            Self::Logical { left, right, .. } => {
                left.in_list_value_count() + right.in_list_value_count()
            }
            Self::Not { inner } => inner.in_list_value_count(),
            Self::All
            | Self::Compare { .. }
            | Self::CastCompare { .. }
            | Self::IsNull { .. }
            | Self::IsNotNull { .. }
            | Self::StringMatch { .. } => 0,
        }
    }
}

fn in_list_expression(column: &str, values: &[ScalarValue]) -> Result<Expression, ShardLoomError> {
    let mut values = values.iter().enumerate();
    let Some((first_index, first_value)) = values.next() else {
        return Err(unsupported_sql_error(
            "IN predicates require at least one non-null literal value",
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
        vec![
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
            ("source_io_performed".to_string(), "true".to_string()),
            (
                "source_format".to_string(),
                self.source.source_format.as_str().to_string(),
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
                    .map_or_else(String::new, |join| join.left_key.to_ref()),
            ),
            (
                "join_right_key".to_string(),
                self.parsed
                    .join
                    .as_ref()
                    .map_or_else(String::new, |join| join.right_key.to_ref()),
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
                "group_by_runtime_execution".to_string(),
                self.parsed.is_grouped_aggregate().to_string(),
            ),
            (
                "group_by_columns".to_string(),
                self.parsed.group_by.join(","),
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
                if self.parsed.order_by.is_some() {
                    "single_key_numeric_topn"
                } else {
                    "not_applicable"
                }
                .to_string(),
            ),
            (
                "sort_keys".to_string(),
                self.parsed
                    .order_by
                    .as_ref()
                    .map_or_else(String::new, |order_by| order_by.column.clone()),
            ),
            (
                "sort_direction".to_string(),
                self.parsed
                    .order_by
                    .as_ref()
                    .map_or_else(String::new, |order_by| {
                        order_by.direction_label().to_string()
                    }),
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
                "string_predicate_runtime_execution".to_string(),
                self.parsed.predicate.uses_string_predicate().to_string(),
            ),
            (
                "string_predicate_operator".to_string(),
                self.parsed.predicate.string_operator(),
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
                "date_literal_runtime_execution".to_string(),
                self.parsed.predicate.uses_date_literal().to_string(),
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
            ("output_bytes".to_string(), self.output_bytes.to_string()),
            ("output_digest".to_string(), self.output_digest.clone()),
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
                self.request.output_path.is_some().to_string(),
            ),
            (
                "write_io".to_string(),
                self.request.output_path.is_some().to_string(),
            ),
            (
                "output_native_io_certificate_status".to_string(),
                if self.request.output_path.is_some() {
                    self.request.output_format.certificate_status()
                } else {
                    "not_requested"
                }
                .to_string(),
            ),
            (
                "output_certificate_ref".to_string(),
                if self.request.output_path.is_some() {
                    self.request.output_format.certificate_ref()
                } else {
                    ""
                }
                .to_string(),
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
        ]
    }

    fn to_text(&self) -> String {
        let output = self.request.output_path.as_ref().map_or_else(
            || "not requested".to_string(),
            |path| path.display().to_string(),
        );
        format!(
            "SQL local-source smoke\nschema_version: {SCHEMA_VERSION}\nsource: {}\nrows read: {}\nrows selected: {}\nrows output: {}\noutput: {output}\nresult:\n{}fallback_attempted: false\nexternal_engine_invoked: false\nclaim_gate_status: fixture_smoke_only",
            self.parsed.source_path.display(),
            self.source.rows.len(),
            self.selected_row_count,
            self.output_rows.len(),
            self.result_jsonl,
        )
    }

    fn source_state_id(source: &CsvSourceData) -> String {
        format!(
            "local-{}-{}",
            source.source_format.as_str(),
            source.source_digest.replace(':', "-")
        )
    }

    fn source_state_digest(&self, source: &CsvSourceData) -> String {
        fnv64_digest(&format!(
            "{}|{}|{}|{}|{}",
            source.source_format.as_str(),
            source.source_digest,
            self.source_schema_digest,
            source.rows.len(),
            source.source_bytes
        ))
    }

    fn source_format_label(&self) -> &'static str {
        self.source.source_format.as_str()
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
    reject_remote_source_path(path)?;
    let source_format = LocalSourceFormat::from_path(path)?;
    let read_start = Instant::now();
    let content = fs::read_to_string(path).map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "failed to read local {} source {}: {error}",
            source_format.row_label(),
            path.display(),
        ))
    })?;
    let read_millis = read_start.elapsed().as_millis();
    let source_bytes = u64::try_from(content.len()).map_err(|_| {
        ShardLoomError::InvalidOperation(format!(
            "{} source length does not fit in u64",
            source_format.row_label()
        ))
    })?;
    let source_digest = fnv64_digest(&content);
    let parse_start = Instant::now();
    let (header, rows) = match source_format {
        LocalSourceFormat::Csv => parse_csv_source_content(&content)?,
        LocalSourceFormat::JsonLines => parse_jsonl_source_content(&content)?,
    };
    Ok(CsvSourceData {
        source_format,
        header,
        rows,
        source_bytes,
        source_digest,
        read_millis,
        parse_millis: parse_start.elapsed().as_millis(),
    })
}

fn parse_csv_source_content(
    content: &str,
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
            row.insert(column.clone(), parse_csv_scalar(&value));
        }
        rows.push(row);
    }
    Ok((header, rows))
}

fn parse_jsonl_source_content(
    content: &str,
) -> Result<(Vec<String>, Vec<ExpressionInputRow>), ShardLoomError> {
    let mut header = Vec::new();
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
        for (name, _value) in &fields {
            if !header.contains(name) {
                validate_sql_identifier(name)?;
                header.push(name.clone());
            }
        }
        raw_rows.push(fields);
    }
    if raw_rows.is_empty() {
        return Err(unsupported_sql_error(
            "JSONL source must include at least one object row",
        ));
    }
    if raw_rows.len() > MAX_INPUT_ROWS {
        return Err(unsupported_sql_error(&format!(
            "scoped SQL local-source smoke supports at most {MAX_INPUT_ROWS} JSONL data rows"
        )));
    }
    let mut rows = Vec::with_capacity(raw_rows.len());
    for fields in raw_rows {
        let mut row = ExpressionInputRow::new();
        for column in &header {
            row.insert(column.clone(), ScalarValue::Null);
        }
        for (column, value) in fields {
            row.insert(column, value);
        }
        rows.push(row);
    }
    Ok((header, rows))
}

fn parse_flat_json_object(raw: &str) -> Result<Vec<(String, ScalarValue)>, ShardLoomError> {
    let chars = raw.trim().chars().collect::<Vec<_>>();
    let mut index = skip_json_ws(&chars, 0);
    if chars.get(index) != Some(&'{') {
        return Err(unsupported_sql_error(
            "JSONL rows must be flat JSON objects",
        ));
    }
    index += 1;
    let mut fields = Vec::new();
    loop {
        index = skip_json_ws(&chars, index);
        if chars.get(index) == Some(&'}') {
            index += 1;
            break;
        }
        let (key, next_index) = parse_json_string(&chars, index)?;
        validate_sql_identifier(&key)?;
        index = skip_json_ws(&chars, next_index);
        if chars.get(index) != Some(&':') {
            return Err(unsupported_sql_error(
                "JSONL object fields must use ':' between key and value",
            ));
        }
        index += 1;
        let (value, next_index) = parse_json_value(&chars, index)?;
        fields.push((key, value));
        index = skip_json_ws(&chars, next_index);
        match chars.get(index) {
            Some(',') => index += 1,
            Some('}') => {
                index += 1;
                break;
            }
            _ => {
                return Err(unsupported_sql_error(
                    "JSONL object fields must be separated by ','",
                ));
            }
        }
    }
    if fields.is_empty() {
        return Err(unsupported_sql_error(
            "JSONL object rows must include at least one field",
        ));
    }
    if skip_json_ws(&chars, index) != chars.len() {
        return Err(unsupported_sql_error(
            "JSONL rows must contain exactly one JSON object per line",
        ));
    }
    Ok(fields)
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
            "JSONL source runtime admits scalar values only; nested objects and arrays remain blocked",
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
                    "JSONL scalar values must not be empty",
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
                "JSONL numeric values must be finite int64 or float64 scalars",
            ))
        }
    } else {
        Err(unsupported_sql_error(
            "JSONL bare values are limited to null, booleans, finite numbers, and quoted strings",
        ))
    }
}

fn parse_json_string(chars: &[char], mut index: usize) -> Result<(String, usize), ShardLoomError> {
    if chars.get(index) != Some(&'"') {
        return Err(unsupported_sql_error(
            "JSONL object keys and string values must be quoted strings",
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
                            "JSONL unicode escape decoding is not admitted in this scoped runtime slice",
                        ));
                    }
                    _ => {
                        return Err(unsupported_sql_error(
                            "JSONL string contains an unsupported escape sequence",
                        ));
                    }
                }
            }
            value_char => value.push(value_char),
        }
    }
    Err(unsupported_sql_error("JSONL string is not closed"))
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
            "SQL local-source smoke supports local CSV file paths only; object-store and remote URI reads remain blocked",
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

    Ok(ParsedSqlLocalSource {
        projections: projection_list.projections,
        aggregates: projection_list.aggregates,
        group_by,
        order_by,
        source_path: source_clause.source_path,
        source_alias: source_clause.source_alias,
        join: source_clause.join,
        predicate,
        limit,
        normalized_statement: statement,
    })
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

fn parse_projection_list(raw: &str) -> Result<ParsedProjectionList, ShardLoomError> {
    let entries = split_sql_csv(raw)?;
    if entries.is_empty() {
        return Err(unsupported_sql_error("SELECT list must not be empty"));
    }
    let mut projections = Vec::with_capacity(entries.len());
    let mut aggregates = Vec::new();
    let projection_count = entries.len();
    for projection in entries {
        let projection = projection.trim();
        if projection == "*" {
            if projection_count > 1 {
                return Err(unsupported_sql_error(
                    "SELECT * cannot be mixed with explicit columns in this scoped smoke",
                ));
            }
            projections.push("*".to_string());
        } else if let Some(aggregate) = parse_aggregate_projection(projection)? {
            aggregates.push(aggregate);
        } else {
            validate_sql_column_ref(projection)?;
            projections.push(projection.to_string());
        }
    }
    Ok(ParsedProjectionList {
        projections,
        aggregates,
    })
}

fn parse_aggregate_projection(raw: &str) -> Result<Option<ParsedAggregate>, ShardLoomError> {
    let Some(open_index) = raw.find('(') else {
        return Ok(None);
    };
    if !raw.ends_with(')') {
        return Err(unsupported_sql_error(
            "aggregate expressions must be written as function(column) in this scoped smoke",
        ));
    }
    let function_raw = raw[..open_index].trim();
    let function = match function_raw.to_ascii_lowercase().as_str() {
        "count" => AggregateFunction::Count,
        "sum" => AggregateFunction::Sum,
        "avg" => AggregateFunction::Avg,
        "min" => AggregateFunction::Min,
        "max" => AggregateFunction::Max,
        _ => return Ok(None),
    };
    let argument = raw[open_index + 1..raw.len() - 1].trim();
    if argument.is_empty() {
        return Err(unsupported_sql_error(
            "aggregate expressions require a column or COUNT(*) argument",
        ));
    }
    if argument == "*" {
        if function != AggregateFunction::Count {
            return Err(unsupported_sql_error(
                "only COUNT(*) is admitted in this scoped aggregate smoke",
            ));
        }
        return Ok(Some(ParsedAggregate {
            function,
            column: None,
        }));
    }
    validate_sql_identifier(argument)?;
    Ok(Some(ParsedAggregate {
        function,
        column: Some(argument.to_string()),
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
        validate_sql_identifier(&column)?;
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
    if entries.len() != 1 {
        return Err(unsupported_sql_error(
            "ORDER BY top-N smoke admits exactly one sort key",
        ));
    }
    let tokens = split_whitespace_outside_quotes(&entries[0])?;
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
                "ORDER BY top-N smoke admits <column> [ASC|DESC] only",
            ));
        }
    };
    validate_sql_identifier(column)?;
    Ok(Some(ParsedOrderBy {
        column: column.clone(),
        direction,
    }))
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
    if left_alias == right_alias {
        return Err(unsupported_sql_error(
            "JOIN smoke requires distinct left and right aliases",
        ));
    }
    let (left_key, right_key) = parse_join_on(on_raw)?;
    if left_key.alias != left_alias || right_key.alias != right_alias {
        return Err(unsupported_sql_error(
            "JOIN ON must be ordered as <left_alias>.<column> = <right_alias>.<column>",
        ));
    }
    Ok(ParsedSourceClause {
        source_path,
        source_alias: Some(left_alias),
        join: Some(ParsedJoin {
            right_source_path,
            right_alias,
            left_key,
            right_key,
        }),
    })
}

fn parse_aliased_source(raw: &str, side: &str) -> Result<(PathBuf, String), ShardLoomError> {
    let tokens = split_whitespace_outside_quotes(raw)?;
    let [path_raw, as_keyword, alias] = tokens.as_slice() else {
        return Err(unsupported_sql_error(&format!(
            "JOIN smoke requires {side} source syntax <local.csv|local.jsonl|local.ndjson> AS <alias>"
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

fn parse_join_on(raw: &str) -> Result<(QualifiedColumn, QualifiedColumn), ShardLoomError> {
    let tokens = split_whitespace_outside_quotes(raw)?;
    match tokens.as_slice() {
        [left, op, right] if op == "=" => Ok((
            parse_qualified_column_ref(left)?,
            parse_qualified_column_ref(right)?,
        )),
        [_, op, _] if op != "=" => Err(unsupported_sql_error(
            "JOIN smoke admits equi-join ON predicates only",
        )),
        _ => Err(unsupported_sql_error(
            "JOIN smoke ON clause must be <left_alias>.<column> = <right_alias>.<column>",
        )),
    }
}

fn parse_source_path(raw: &str) -> Result<PathBuf, ShardLoomError> {
    let path = if raw.starts_with('\'') {
        parse_sql_string_literal(raw)?
    } else {
        if raw.split_whitespace().count() != 1 {
            return Err(unsupported_sql_error(
                "FROM source must be a single local CSV/JSONL path or single-quoted path",
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
    if let Some(predicate) = parse_cast_predicate(raw)? {
        return Ok(predicate);
    }
    if let Some(predicate) = parse_in_list_predicate(raw)? {
        return Ok(predicate);
    }
    let tokens = split_whitespace_outside_quotes(raw)?;
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
        _ => Err(unsupported_sql_error(
            "WHERE admits only <column> <op> <literal>, <column> <op> DATE <date-literal>, <column> IN (<literal>,...), <column> LIKE <string-pattern>, <column> IS NULL, <column> IS NOT NULL, admitted predicates joined by AND/OR/NOT, or balanced grouping parentheses around admitted predicates",
        )),
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
    if !trimmed
        .get(..5)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("cast("))
    {
        return Ok(None);
    }
    let close_index = trimmed.find(')').ok_or_else(|| {
        unsupported_sql_error(
            "CAST predicates must be written as CAST(<column> AS <dtype>) <op> <literal>",
        )
    })?;
    let inner = trimmed[5..close_index].trim();
    let tail = trimmed[close_index + 1..].trim();
    if inner.is_empty() || tail.is_empty() {
        return Err(unsupported_sql_error(
            "CAST predicates require a source column, target dtype, comparison operator, and literal",
        ));
    }
    let as_index = find_keyword_outside_quotes(inner, "as").ok_or_else(|| {
        unsupported_sql_error("CAST predicates must use CAST(<column> AS <dtype>) syntax")
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
        [op_raw, literal_raw] => (
            parse_comparison_op(op_raw)?,
            parse_sql_literal(literal_raw)?,
        ),
        _ => {
            return Err(unsupported_sql_error(
                "CAST predicates admit CAST(<column> AS <dtype>) <op> <literal> only",
            ));
        }
    };
    Ok(Some(ParsedPredicate::CastCompare {
        column: column.to_string(),
        target_dtype,
        op,
        value,
    }))
}

fn parse_cast_target_dtype(raw: &str) -> Result<LogicalDType, ShardLoomError> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "int64" | "bigint" | "integer" | "int" => Ok(LogicalDType::Int64),
        "float64" | "double" | "float" => Ok(LogicalDType::Float64),
        "utf8" | "string" | "text" => Ok(LogicalDType::Utf8),
        "boolean" | "bool" => Ok(LogicalDType::Boolean),
        "date32" | "date" => Ok(LogicalDType::Date32),
        _ => Err(unsupported_sql_error(
            "CAST target dtype must be one of int64, float64, utf8, boolean, or date32",
        )),
    }
}

fn parse_sql_date_literal(raw: &str) -> Result<ScalarValue, ShardLoomError> {
    let value = parse_sql_string_literal(raw)?;
    parse_iso_date32(&value)
        .map(ScalarValue::Date32)
        .map_err(|_| unsupported_sql_error("DATE literals must use DATE 'YYYY-MM-DD'"))
}

fn parse_in_list_predicate(raw: &str) -> Result<Option<ParsedPredicate>, ShardLoomError> {
    let Some(in_index) = find_keyword_outside_quotes(raw, "in") else {
        return Ok(None);
    };
    let column = raw[..in_index].trim();
    let tail = raw[in_index + "in".len()..].trim();
    validate_sql_column_ref(column)?;
    if !tail.starts_with('(') || !tail.ends_with(')') {
        return Err(unsupported_sql_error(
            "IN predicates must use <column> IN (<literal>,...) syntax",
        ));
    }
    let values_raw = tail[1..tail.len() - 1].trim();
    if values_raw.is_empty() {
        return Err(unsupported_sql_error(
            "IN predicates require at least one non-null literal value",
        ));
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
    if values
        .iter()
        .any(|value| matches!(value, ScalarValue::Null))
    {
        return Err(unsupported_sql_error(
            "IN predicates do not admit NULL list values in this scoped runtime slice",
        ));
    }
    let has_date = values
        .iter()
        .any(|value| matches!(value, ScalarValue::Date32(_)));
    let has_non_date = values
        .iter()
        .any(|value| !matches!(value, ScalarValue::Date32(_)));
    if has_date && has_non_date {
        return Err(unsupported_sql_error(
            "IN predicates do not admit mixed DATE and non-DATE literal lists in this scoped runtime slice",
        ));
    }
    Ok(Some(ParsedPredicate::InList {
        column: column.to_string(),
        values,
    }))
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
            ',' if !in_quote => {
                values.push(current.trim().to_string());
                current = String::new();
            }
            _ => current.push(ch),
        }
    }
    if in_quote {
        return Err(unsupported_sql_error("SQL string literal is not closed"));
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
            if remaining.len() >= lower_keyword.len()
                && remaining[..lower_keyword.len()].eq_ignore_ascii_case(&lower_keyword)
                && keyword_boundary(raw, index, lower_keyword.len())
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

fn scalar_to_csv_value(value: &ScalarValue) -> String {
    match value {
        ScalarValue::Boolean(value) => value.to_string(),
        ScalarValue::Int64(value) | ScalarValue::TimestampMicros(value) => value.to_string(),
        ScalarValue::UInt64(value) => value.to_string(),
        ScalarValue::Float64(value) if value.is_finite() => value.to_string(),
        ScalarValue::Null | ScalarValue::Float64(_) => String::new(),
        ScalarValue::Utf8(value) => value.clone(),
        ScalarValue::Binary(value) => format!("binary[len={}]", value.len()),
        ScalarValue::Date32(value) => format_iso_date32(*value),
    }
}

fn scalar_to_json(value: &ScalarValue) -> String {
    match value {
        ScalarValue::Boolean(value) => value.to_string(),
        ScalarValue::Int64(value) | ScalarValue::TimestampMicros(value) => value.to_string(),
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

fn fnv64_digest(value: &str) -> String {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in value.as_bytes() {
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
    fn parses_scoped_order_by_topn_statement() {
        let parsed = parse_sql_local_source_statement(
            "SELECT id,label FROM 'target/input.csv' WHERE amount >= 0 ORDER BY amount DESC LIMIT 3",
        )
        .expect("order-by statement parses");

        assert_eq!(parsed.projections, vec!["id", "label"]);
        assert!(parsed.aggregates.is_empty());
        assert!(parsed.group_by.is_empty());
        let order_by = parsed.order_by.as_ref().expect("order by parsed");
        assert_eq!(order_by.column, "amount");
        assert_eq!(order_by.direction, SortDirection::Desc);
        assert_eq!(parsed.limit, 3);
        assert_eq!(
            parsed.statement_kind(),
            "local_source_order_by_topn_filter_limit"
        );
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
                op: ComparisonOp::GtEq,
                value: ScalarValue::Int64(10)
            } if column == "amount"
        ));
        assert_eq!(parsed.predicate.family(), "cast");
        assert_eq!(parsed.predicate.cast_source_columns(), "amount");
        assert_eq!(parsed.predicate.cast_target_dtypes(), "int64");
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
                op: ComparisonOp::GtEq,
                value: ScalarValue::Date32(_)
            } if column == "event_date"
        ));
        assert!(parsed.predicate.uses_date_literal());
        assert_eq!(parsed.predicate.family(), "cast");
        assert_eq!(parsed.predicate.cast_target_dtypes(), "date32");
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
    fn in_predicate_blocks_unadmitted_literal_lists_without_fallback() {
        let empty_error = parse_sql_local_source_statement(
            "SELECT id FROM 'target/input.csv' WHERE label IN () LIMIT 5",
        )
        .expect_err("empty IN list remains blocked");
        assert!(
            empty_error
                .to_string()
                .contains("IN predicates require at least one non-null literal value")
        );
        assert!(
            empty_error
                .to_string()
                .contains("external_engine_invoked=false")
        );

        let null_error = parse_sql_local_source_statement(
            "SELECT id FROM 'target/input.csv' WHERE label IN ('alpha', NULL) LIMIT 5",
        )
        .expect_err("NULL IN list values remain blocked");
        assert!(
            null_error
                .to_string()
                .contains("IN predicates do not admit NULL list values")
        );
        assert!(
            null_error
                .to_string()
                .contains("external_engine_invoked=false")
        );

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
        assert_eq!(join.left_key.to_ref(), "f.customer_id");
        assert_eq!(join.right_key.to_ref(), "d.customer_id");
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
    fn jsonl_parser_blocks_nested_values() {
        let error = parse_jsonl_source_content("{\"id\":1,\"payload\":{\"x\":1}}\n")
            .expect_err("nested object is blocked");
        assert!(error.to_string().contains("scalar values only"));
    }
}
