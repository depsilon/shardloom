//! Scoped local-source SQL runtime smoke.
//!
//! This module intentionally admits one small SQL shape over local CSV:
//! `SELECT <columns> FROM <local.csv> WHERE <simple predicate> LIMIT <n>`.
//! It uses ShardLoom-owned parsing/binding plus the core expression semantics
//! baseline. It does not invoke `DataFusion`, `DuckDB`, `SQLite`, `Spark`,
//! `Polars`, `pandas`, object stores, catalogs, or Vortex query-engine
//! integrations.

use std::{
    fmt::Write as _,
    fs,
    path::{Path, PathBuf},
    process::ExitCode,
    time::Instant,
};

use shardloom_core::{
    ColumnRef, CommandStatus, ComparisonOp, ExprId, Expression, ExpressionInputRow, ExpressionKind,
    OutputFormat, ScalarValue, ShardLoomError, UnaryOp, evaluate_filter, evaluate_projection,
};

use crate::{
    cli_output::{emit, emit_error},
    cli_unknown_arg_error,
};

const COMMAND: &str = "sql-local-source-smoke";
const SCHEMA_VERSION: &str = "shardloom.sql_local_source_smoke.v1";
const EXECUTION_CERTIFICATE_ID: &str = "sql-local-source.csv.projection-filter-limit.execution.v1";
const SOURCE_CERTIFICATE_ID: &str = "sql-local-source.csv.compatibility-source.v1";
const OUTPUT_CERTIFICATE_ID: &str = "sql-local-source.csv.local-jsonl-output.native-io.v1";
const MAX_INPUT_ROWS: usize = 50_000;
const MAX_LIMIT_ROWS: usize = 10_000;

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
}

impl SqlLocalSourceOutputFormat {
    fn parse(value: &str) -> Result<Self, ShardLoomError> {
        match value.trim().to_ascii_lowercase().as_str() {
            "inline-jsonl" | "jsonl" | "json-lines" | "ndjson" => Ok(Self::InlineJsonl),
            other => Err(ShardLoomError::InvalidOperation(format!(
                "unsupported SQL local-source output format {other:?}; scoped local SQL supports inline JSONL only"
            ))),
        }
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::InlineJsonl => "inline_jsonl",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
struct ParsedSqlLocalSource {
    projections: Vec<String>,
    source_path: PathBuf,
    predicate: ParsedPredicate,
    limit: usize,
    normalized_statement: String,
}

#[derive(Debug, Clone, PartialEq)]
enum ParsedPredicate {
    Compare {
        column: String,
        op: ComparisonOp,
        value: ScalarValue,
    },
    IsNull {
        column: String,
    },
    IsNotNull {
        column: String,
    },
}

#[derive(Debug, Clone, PartialEq)]
struct CsvSourceData {
    header: Vec<String>,
    rows: Vec<ExpressionInputRow>,
    source_bytes: u64,
    source_digest: String,
    read_millis: u128,
    parse_millis: u128,
}

#[derive(Debug, Clone, PartialEq)]
struct SqlLocalSourceReport {
    request: SqlLocalSourceRequest,
    parsed: ParsedSqlLocalSource,
    source: CsvSourceData,
    selected_row_count: usize,
    output_rows: Vec<Vec<(String, ScalarValue)>>,
    result_jsonl: String,
    plan_digest: String,
    source_schema_digest: String,
    result_digest: String,
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
            "usage: shardloom {COMMAND} <sql-statement> [--output-format inline-jsonl] [--output local.jsonl] [--allow-overwrite] [--format text|json]"
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

fn run_sql_local_source_smoke(
    request: &SqlLocalSourceRequest,
) -> Result<SqlLocalSourceReport, ShardLoomError> {
    let total_start = Instant::now();
    let parsed = parse_sql_local_source_statement(&request.statement)?;
    let source = read_local_csv_source(&parsed.source_path)?;
    bind_sql_local_source(&parsed, &source.header)?;

    let compute_start = Instant::now();
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
    for row_index in filter.selected_row_indexes.iter().take(parsed.limit) {
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
    let operator_compute_millis = compute_start.elapsed().as_millis();

    let evidence_start = Instant::now();
    let result_jsonl = rows_to_jsonl(&output_rows);
    let result_digest = fnv64_digest(&result_jsonl);
    let source_schema_digest = fnv64_digest(&source.header.join(","));
    let plan_digest = fnv64_digest(&format!(
        "{}|{}|{}|{}",
        parsed.normalized_statement,
        source_schema_digest,
        source.source_digest,
        request.output_format.as_str()
    ));
    let evidence_render_millis = evidence_start.elapsed().as_millis();
    let output_bytes = u64::try_from(result_jsonl.len()).unwrap_or(u64::MAX);
    let output_write_millis = write_optional_sql_output(request, &result_jsonl)?;

    Ok(SqlLocalSourceReport {
        request: request.clone(),
        parsed,
        source,
        selected_row_count: filter.selected_row_count(),
        output_rows,
        result_jsonl,
        plan_digest,
        source_schema_digest,
        result_digest,
        output_write_millis,
        output_bytes,
        operator_compute_millis,
        evidence_render_millis,
        total_runtime_millis: total_start.elapsed().as_millis(),
    })
}

fn write_optional_sql_output(
    request: &SqlLocalSourceRequest,
    result_jsonl: &str,
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
    fs::write(output_path, result_jsonl.as_bytes()).map_err(|error| {
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
) -> Result<(), ShardLoomError> {
    for column in parsed.projection_columns(header) {
        if !header.iter().any(|candidate| candidate == &column) {
            return Err(unsupported_sql_error(&format!(
                "projection column {column:?} is not present in the CSV header"
            )));
        }
    }
    let predicate_column = parsed.predicate.column();
    if !header.iter().any(|candidate| candidate == predicate_column) {
        return Err(unsupported_sql_error(&format!(
            "predicate column {predicate_column:?} is not present in the CSV header"
        )));
    }
    Ok(())
}

impl ParsedSqlLocalSource {
    fn projection_columns(&self, header: &[String]) -> Vec<String> {
        if self.projections.len() == 1 && self.projections[0] == "*" {
            header.to_vec()
        } else {
            self.projections.clone()
        }
    }
}

impl ParsedPredicate {
    fn column(&self) -> &str {
        match self {
            Self::Compare { column, .. } | Self::IsNull { column } | Self::IsNotNull { column } => {
                column
            }
        }
    }

    fn to_expression(&self) -> Result<Expression, ShardLoomError> {
        match self {
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
        }
    }

    fn family(&self) -> &'static str {
        match self {
            Self::Compare { .. } => "comparison",
            Self::IsNull { .. } | Self::IsNotNull { .. } => "null_predicate",
        }
    }
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
                "local_source_projection_filter_limit".to_string(),
            ),
            ("sql_statement".to_string(), self.request.statement.clone()),
            ("sql_parser_executed".to_string(), "true".to_string()),
            ("sql_binder_executed".to_string(), "true".to_string()),
            ("sql_planner_executed".to_string(), "true".to_string()),
            ("sql_runtime_execution".to_string(), "true".to_string()),
            ("source_io_performed".to_string(), "true".to_string()),
            ("source_format".to_string(), "csv".to_string()),
            (
                "source_path".to_string(),
                self.parsed.source_path.display().to_string(),
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
                self.parsed
                    .projection_columns(&self.source.header)
                    .join(","),
            ),
            (
                "predicate_operator_family".to_string(),
                self.parsed.predicate.family().to_string(),
            ),
            ("projection_pushed_down".to_string(), "false".to_string()),
            ("filter_pushed_down".to_string(), "false".to_string()),
            ("limit_pushed_down".to_string(), "false".to_string()),
            (
                "pushdown_status".to_string(),
                "not_applicable_local_csv_transient".to_string(),
            ),
            ("plan_digest".to_string(), self.plan_digest.clone()),
            ("correctness_digest".to_string(), self.result_digest.clone()),
            ("result_digest".to_string(), self.result_digest.clone()),
            (
                "result_format".to_string(),
                self.request.output_format.as_str().to_string(),
            ),
            ("result_jsonl".to_string(), self.result_jsonl.clone()),
            (
                "output_path".to_string(),
                self.request
                    .output_path
                    .as_ref()
                    .map_or_else(String::new, |path| path.display().to_string()),
            ),
            ("output_format".to_string(), "jsonl".to_string()),
            ("output_bytes".to_string(), self.output_bytes.to_string()),
            ("output_digest".to_string(), self.result_digest.clone()),
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
                SOURCE_CERTIFICATE_ID.to_string(),
            ),
            (
                "execution_certificate_status".to_string(),
                "certified".to_string(),
            ),
            (
                "execution_certificate_ref".to_string(),
                EXECUTION_CERTIFICATE_ID.to_string(),
            ),
            (
                "materialization_boundary".to_string(),
                "local_csv_row_materialization_to_expression_semantics".to_string(),
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
                    "certified_local_jsonl_sink"
                } else {
                    "not_requested"
                }
                .to_string(),
            ),
            (
                "output_certificate_ref".to_string(),
                if self.request.output_path.is_some() {
                    OUTPUT_CERTIFICATE_ID
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
            (
                "claim_gate_reason".to_string(),
                "one_scoped_local_csv_sql_projection_filter_limit_smoke".to_string(),
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
}

fn read_local_csv_source(path: &Path) -> Result<CsvSourceData, ShardLoomError> {
    reject_remote_source_path(path)?;
    let read_start = Instant::now();
    let content = fs::read_to_string(path).map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "failed to read local CSV source {}: {error}",
            path.display()
        ))
    })?;
    let read_millis = read_start.elapsed().as_millis();
    let source_bytes = u64::try_from(content.len()).map_err(|_| {
        ShardLoomError::InvalidOperation("CSV source length does not fit in u64".to_string())
    })?;
    let source_digest = fnv64_digest(&content);
    let parse_start = Instant::now();
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
    Ok(CsvSourceData {
        header,
        rows,
        source_bytes,
        source_digest,
        read_millis,
        parse_millis: parse_start.elapsed().as_millis(),
    })
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
    let where_index = find_keyword_outside_quotes(&statement, "where").ok_or_else(|| {
        unsupported_sql_error("SQL local-source smoke requires a WHERE <simple predicate> clause")
    })?;
    let limit_index = find_keyword_outside_quotes(&statement, "limit").ok_or_else(|| {
        unsupported_sql_error("SQL local-source smoke requires a LIMIT <n> clause")
    })?;
    if !(from_index > 6 && where_index > from_index && limit_index > where_index) {
        return Err(unsupported_sql_error(
            "SQL local-source smoke requires SELECT ... FROM ... WHERE ... LIMIT ... order",
        ));
    }

    let select_list = statement[6..from_index].trim();
    let source_raw = statement[from_index + 4..where_index].trim();
    let predicate_raw = statement[where_index + 5..limit_index].trim();
    let limit_raw = statement[limit_index + 5..].trim();
    if select_list.is_empty()
        || source_raw.is_empty()
        || predicate_raw.is_empty()
        || limit_raw.is_empty()
    {
        return Err(unsupported_sql_error(
            "SQL local-source SELECT list, source, predicate, and limit must not be empty",
        ));
    }
    if contains_keyword_outside_quotes(limit_raw, "where")
        || contains_keyword_outside_quotes(limit_raw, "from")
        || contains_keyword_outside_quotes(limit_raw, "select")
    {
        return Err(unsupported_sql_error(
            "SQL local-source smoke admits one flat SELECT without subqueries",
        ));
    }

    let projections = parse_projection_list(select_list)?;
    let source_path = parse_source_path(source_raw)?;
    let predicate = parse_predicate(predicate_raw)?;
    let limit = parse_limit(limit_raw)?;

    Ok(ParsedSqlLocalSource {
        projections,
        source_path,
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

fn parse_projection_list(raw: &str) -> Result<Vec<String>, ShardLoomError> {
    let projections = split_sql_csv(raw)?;
    if projections.is_empty() {
        return Err(unsupported_sql_error("SELECT list must not be empty"));
    }
    let mut parsed = Vec::with_capacity(projections.len());
    let projection_count = projections.len();
    for projection in projections {
        let projection = projection.trim();
        if projection == "*" {
            if projection_count > 1 {
                return Err(unsupported_sql_error(
                    "SELECT * cannot be mixed with explicit columns in this scoped smoke",
                ));
            }
            parsed.push("*".to_string());
        } else {
            validate_sql_identifier(projection)?;
            parsed.push(projection.to_string());
        }
    }
    Ok(parsed)
}

fn parse_source_path(raw: &str) -> Result<PathBuf, ShardLoomError> {
    let path = if raw.starts_with('\'') {
        parse_sql_string_literal(raw)?
    } else {
        if raw.split_whitespace().count() != 1 {
            return Err(unsupported_sql_error(
                "FROM source must be a single local CSV path or single-quoted path",
            ));
        }
        raw.to_string()
    };
    if !path.to_ascii_lowercase().ends_with(".csv") {
        return Err(unsupported_sql_error(
            "GAR-RUNTIME-IMPL-1B admits local CSV sources only in this slice",
        ));
    }
    Ok(PathBuf::from(path))
}

fn parse_predicate(raw: &str) -> Result<ParsedPredicate, ShardLoomError> {
    let tokens = split_whitespace_outside_quotes(raw)?;
    match tokens.as_slice() {
        [column, is_keyword, null_keyword]
            if is_keyword.eq_ignore_ascii_case("is")
                && null_keyword.eq_ignore_ascii_case("null") =>
        {
            validate_sql_identifier(column)?;
            Ok(ParsedPredicate::IsNull {
                column: (*column).clone(),
            })
        }
        [column, is_keyword, not_keyword, null_keyword]
            if is_keyword.eq_ignore_ascii_case("is")
                && not_keyword.eq_ignore_ascii_case("not")
                && null_keyword.eq_ignore_ascii_case("null") =>
        {
            validate_sql_identifier(column)?;
            Ok(ParsedPredicate::IsNotNull {
                column: (*column).clone(),
            })
        }
        [column, op_raw, literal_raw] => {
            validate_sql_identifier(column)?;
            let op = parse_comparison_op(op_raw)?;
            let value = parse_sql_literal(literal_raw)?;
            Ok(ParsedPredicate::Compare {
                column: (*column).clone(),
                op,
                value,
            })
        }
        _ => Err(unsupported_sql_error(
            "WHERE admits only <column> <op> <literal>, <column> IS NULL, or <column> IS NOT NULL",
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

fn contains_keyword_outside_quotes(raw: &str, keyword: &str) -> bool {
    find_keyword_outside_quotes(raw, keyword).is_some()
}

fn keyword_boundary(raw: &str, index: usize, len: usize) -> bool {
    let before = raw[..index].chars().next_back();
    let after = raw[index + len..].chars().next();
    !before.is_some_and(is_identifier_char) && !after.is_some_and(is_identifier_char)
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
        ScalarValue::Date32(value) => value.to_string(),
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
    fn parser_blocks_unbounded_or_remote_sql() {
        assert!(
            parse_sql_local_source_statement("SELECT id FROM 'target/input.csv' LIMIT 5").is_err()
        );
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
}
