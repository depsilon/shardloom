//! Local `SQLite` adapter smoke.
//!
//! This command admits only a local `SQLite` file fixture path: table scan to a
//! workspace-safe JSONL artifact plus a roundtrip `SQLite` import artifact. It
//! does not accept arbitrary SQL, connect to network databases, resolve
//! credentials, load extensions, or use `SQLite` as an external compute fallback.

use std::{
    cmp::Ordering,
    fmt::Write as _,
    fs,
    path::{Path, PathBuf},
    process::ExitCode,
};

use rusqlite::{
    Connection, OpenFlags, params_from_iter,
    types::{ToSql, ToSqlOutput, ValueRef},
};
use shardloom_core::{
    CommandStatus, ExecutionResources, OutputFormat, ShardLoomError, WorkspaceSafeLocalWritePlan,
    WorkspaceSafeLocalWriteReport,
};
use shardloom_exec::live_memory::{Budgeted, LiveMemoryPool};

use crate::{
    cli_output::{emit, emit_error},
    extension_planning::append_effectful_operation_admission_matrix_fields,
};

const MAX_SQLITE_FIXTURE_BYTES: u64 = 128 * 1024 * 1024;
const MAX_SQLITE_FIXTURE_ROWS: usize = 50_000;
const MAX_SQLITE_EXPORT_JSONL_BYTES: usize = 128 * 1024 * 1024;

#[derive(Debug, Clone)]
struct SqliteSmokeOptions {
    resources: ExecutionResources,
    source_db: PathBuf,
    table: String,
    export_jsonl: PathBuf,
    roundtrip_db: PathBuf,
    order_by: Option<String>,
    allow_overwrite: bool,
}

#[derive(Debug, Clone)]
struct SqliteColumn {
    name: String,
    declared_type: String,
    not_null: bool,
    primary_key_position: i64,
}

#[derive(Debug, Clone, PartialEq)]
enum SqliteCell {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
}

impl ToSql for SqliteCell {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::Borrowed(match self {
            Self::Null => ValueRef::Null,
            Self::Integer(value) => ValueRef::Integer(*value),
            Self::Real(value) => ValueRef::Real(*value),
            Self::Text(value) => ValueRef::Text(value.as_bytes()),
        }))
    }
}

#[derive(Debug, Clone, PartialEq)]
struct SqliteRow {
    cells: Vec<SqliteCell>,
}

#[derive(Debug, Clone)]
#[allow(clippy::struct_excessive_bools)]
struct LocalSqliteImportExportReport {
    schema_version: &'static str,
    adapter_id: &'static str,
    source_adapter_id: &'static str,
    source_db: PathBuf,
    canonical_source_db: PathBuf,
    table: String,
    column_order: Vec<String>,
    column_declared_types: Vec<String>,
    not_null_columns: Vec<String>,
    primary_key_columns: Vec<String>,
    source_row_count: usize,
    exported_row_count: usize,
    roundtrip_row_count: usize,
    source_database_digest: String,
    export_jsonl_digest: String,
    roundtrip_database_digest: String,
    source_roundtrip_content_digest: String,
    roundtrip_content_digest: String,
    roundtrip_replay_verification_method: &'static str,
    roundtrip_replay_verified: bool,
    export_write_report: WorkspaceSafeLocalWriteReport,
    roundtrip_write_plan: WorkspaceSafeLocalWritePlan,
    order_by: Option<String>,
    allow_overwrite: bool,
    sqlite_sql_execution_scope: &'static str,
    sqlite_query_pushdown_allowed: bool,
    credential_policy_status: &'static str,
    network_policy: &'static str,
    dynamic_loading_performed: bool,
    extension_code_executed: bool,
    external_effect_executed: bool,
    sqlite_ordering_execution_scope: &'static str,
    fallback_attempted: bool,
    external_engine_invoked: bool,
    claim_gate_status: &'static str,
    claim_boundary: &'static str,
}

struct SqliteRoundtripEvidence {
    roundtrip_database_digest: String,
    source_content_digest: String,
    roundtrip_content_digest: String,
    replay_verified: bool,
}

impl LocalSqliteImportExportReport {
    fn to_human_text(&self) -> String {
        format!(
            "local SQLite import/export smoke\nadapter: {}\ntable: {}\nrows: {}\ncolumns: {}\nexport: {}\nroundtrip: {}\nfallback execution: disabled",
            self.adapter_id,
            self.table,
            self.source_row_count,
            self.column_order.join(","),
            self.export_write_report.target_path.display(),
            self.roundtrip_write_plan.target_path.display()
        )
    }
}

pub(crate) fn handle_sqlite_local_import_export_smoke(
    args: std::vec::IntoIter<String>,
    format: OutputFormat,
) -> ExitCode {
    let options = match parse_sqlite_smoke_options(args) {
        Ok(options) => options,
        Err(error) => {
            return emit_error(
                "sqlite-local-import-export-smoke",
                format,
                "SQLite local import/export smoke failed",
                &error,
            );
        }
    };
    let pool = match crate::fixture_io::owner_for_command(
        options.resources,
        "sqlite-local-import-export-smoke",
        format,
    ) {
        Ok(pool) => pool,
        Err(code) => return code,
    };
    let report = match run_sqlite_local_import_export_smoke(&options, &pool) {
        Ok(report) => report,
        Err(error) => {
            return crate::cli_output::emit_error_with_fields(
                "sqlite-local-import-export-smoke",
                format,
                "SQLite local import/export smoke failed",
                &error,
                crate::fixture_io::with_observation_fields(Vec::new(), options.resources, &pool),
            );
        }
    };
    emit(
        "sqlite-local-import-export-smoke",
        format,
        CommandStatus::Success,
        "SQLite local import/export fixture smoke".to_string(),
        report.to_human_text(),
        vec![],
        crate::fixture_io::with_observation_fields(
            sqlite_local_import_export_fields(&report),
            options.resources,
            &pool,
        ),
    );
    ExitCode::SUCCESS
}

fn parse_sqlite_smoke_options(
    args: std::vec::IntoIter<String>,
) -> Result<SqliteSmokeOptions, ShardLoomError> {
    let (mut args, resources) = crate::execution_resources::ResourceArguments::take_required(
        args,
        &[
            "--table",
            "--export-jsonl",
            "--roundtrip-db",
            "--order-by",
            "--format",
        ],
    )?;
    let Some(source_db) = args.next() else {
        return Err(ShardLoomError::InvalidOperation(
            "usage: sqlite-local-import-export-smoke <db.sqlite> --table <table> --export-jsonl <path> --roundtrip-db <path> [--order-by <column>] [--allow-overwrite]".to_string(),
        ));
    };
    let mut table = None;
    let mut export_jsonl = None;
    let mut roundtrip_db = None;
    let mut order_by = None;
    let mut allow_overwrite = false;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--table" => table = args.next(),
            "--export-jsonl" => export_jsonl = args.next().map(PathBuf::from),
            "--roundtrip-db" => roundtrip_db = args.next().map(PathBuf::from),
            "--order-by" => order_by = args.next(),
            "--allow-overwrite" => allow_overwrite = true,
            "--format" => {
                let _ = args.next();
            }
            other => {
                return Err(ShardLoomError::InvalidOperation(format!(
                    "unknown sqlite-local-import-export-smoke argument {other:?}"
                )));
            }
        }
    }
    let table = table.ok_or_else(|| {
        ShardLoomError::InvalidOperation("missing --table for local SQLite smoke".to_string())
    })?;
    validate_identifier("SQLite table", &table)?;
    if let Some(order_by) = &order_by {
        validate_identifier("SQLite order-by column", order_by)?;
    }
    Ok(SqliteSmokeOptions {
        resources,
        source_db: PathBuf::from(source_db),
        table,
        export_jsonl: export_jsonl.ok_or_else(|| {
            ShardLoomError::InvalidOperation(
                "missing --export-jsonl for local SQLite smoke".to_string(),
            )
        })?,
        roundtrip_db: roundtrip_db.ok_or_else(|| {
            ShardLoomError::InvalidOperation(
                "missing --roundtrip-db for local SQLite smoke".to_string(),
            )
        })?,
        order_by,
        allow_overwrite,
    })
}

fn validate_identifier(label: &str, value: &str) -> Result<(), ShardLoomError> {
    if value.trim().is_empty() {
        return Err(ShardLoomError::InvalidOperation(format!(
            "{label} must not be empty"
        )));
    }
    if !value.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        return Err(ShardLoomError::InvalidOperation(format!(
            "{label} {value:?} must contain only ASCII letters, digits, or underscores"
        )));
    }
    Ok(())
}

#[allow(clippy::too_many_lines)] // Keep source/export/replay owners visible for the whole fixture.
fn run_sqlite_local_import_export_smoke(
    options: &SqliteSmokeOptions,
    pool: &LiveMemoryPool,
) -> Result<LocalSqliteImportExportReport, ShardLoomError> {
    let source_db = canonical_existing_file(&options.source_db)?;
    let source_database_digest = read_file_digest(&source_db, "local SQLite fixture", pool)?;
    let source = Connection::open_with_flags(&source_db, OpenFlags::SQLITE_OPEN_READ_ONLY)
        .map_err(sqlite_error)?;
    require_table(&source, &options.table)?;
    let column_owner = load_table_columns(&source, &options.table, pool)?;
    let columns = column_owner.value();
    validate_sqlite_fixture_shape(columns, &options.table, options.order_by.as_deref())?;
    validate_sqlite_row_budget(
        count_table_rows(&source, &options.table)?,
        "source SQLite table",
    )?;
    let row_owner = read_rows(
        &source,
        &options.table,
        columns,
        options.order_by.as_deref(),
        pool,
    )?;
    let rows = row_owner.value();
    let jsonl = render_jsonl(columns, rows, pool)?;
    let export_workspace_root =
        shardloom_core::infer_local_output_workspace_root(&options.export_jsonl)?;
    let export_write_report = crate::fixture_io::write_bytes(
        &export_workspace_root,
        &options.export_jsonl,
        options.allow_overwrite,
        "local SQLite table export JSONL",
        jsonl.value().as_bytes(),
        pool,
    )
    .map_err(fixture_io_error)?;
    drop(jsonl);
    let roundtrip_workspace_root =
        shardloom_core::infer_local_output_workspace_root(&options.roundtrip_db)?;
    let roundtrip_write_plan = shardloom_core::plan_workspace_safe_local_output(
        roundtrip_workspace_root,
        &options.roundtrip_db,
        options.allow_overwrite,
    )?;
    write_roundtrip_database(&roundtrip_write_plan, &options.table, columns, rows, pool)?;
    let roundtrip_row_count =
        count_roundtrip_rows(&roundtrip_write_plan.target_path, &options.table)?;
    let roundtrip_rows = read_roundtrip_rows(
        &roundtrip_write_plan.target_path,
        &options.table,
        columns,
        options.order_by.as_deref(),
        pool,
    )?;
    let roundtrip_evidence = sqlite_roundtrip_evidence(
        columns,
        rows,
        roundtrip_rows.value(),
        roundtrip_row_count,
        &roundtrip_write_plan.target_path,
        pool,
    )?;
    Ok(LocalSqliteImportExportReport {
        schema_version: "shardloom.local_sqlite_import_export_smoke.v1",
        adapter_id: "local_sqlite_file_adapter",
        source_adapter_id: "sqlite_input_adapter",
        source_db: options.source_db.clone(),
        canonical_source_db: source_db,
        table: options.table.clone(),
        column_order: columns.iter().map(|column| column.name.clone()).collect(),
        column_declared_types: columns
            .iter()
            .map(|column| column.declared_type.clone())
            .collect(),
        not_null_columns: columns
            .iter()
            .filter(|column| column.not_null)
            .map(|column| column.name.clone())
            .collect(),
        primary_key_columns: columns
            .iter()
            .filter(|column| column.primary_key_position > 0)
            .map(|column| column.name.clone())
            .collect(),
        source_row_count: rows.len(),
        exported_row_count: rows.len(),
        roundtrip_row_count,
        source_database_digest,
        export_jsonl_digest: export_write_report.output_digest.clone(),
        roundtrip_database_digest: roundtrip_evidence.roundtrip_database_digest,
        source_roundtrip_content_digest: roundtrip_evidence.source_content_digest,
        roundtrip_content_digest: roundtrip_evidence.roundtrip_content_digest,
        roundtrip_replay_verification_method: "canonical_typed_row_digest",
        roundtrip_replay_verified: roundtrip_evidence.replay_verified,
        export_write_report,
        roundtrip_write_plan,
        order_by: options.order_by.clone(),
        allow_overwrite: options.allow_overwrite,
        sqlite_sql_execution_scope: "single_table_scan_only",
        sqlite_query_pushdown_allowed: false,
        credential_policy_status: "not_required_local_file_only",
        network_policy: "disabled_no_network_probe",
        dynamic_loading_performed: false,
        extension_code_executed: false,
        external_effect_executed: false,
        sqlite_ordering_execution_scope: if options.order_by.is_some() {
            "shardloom_fixture_post_scan"
        } else {
            "not_requested"
        },
        fallback_attempted: false,
        external_engine_invoked: false,
        claim_gate_status: "fixture_smoke_only",
        claim_boundary: "Local SQLite import/export fixture smoke only; no arbitrary SQL, query pushdown, network database connector, credentials, extension loading, production connector, fallback, performance, or warehouse claim is added.",
    })
}

fn sqlite_roundtrip_evidence(
    columns: &[SqliteColumn],
    source_rows: &[SqliteRow],
    roundtrip_rows: &[SqliteRow],
    roundtrip_row_count: usize,
    roundtrip_path: &Path,
    pool: &LiveMemoryPool,
) -> Result<SqliteRoundtripEvidence, ShardLoomError> {
    let source_content_digest = sqlite_typed_content_digest(columns, source_rows)?;
    let roundtrip_content_digest = sqlite_typed_content_digest(columns, roundtrip_rows)?;
    Ok(SqliteRoundtripEvidence {
        roundtrip_database_digest: read_file_digest(
            roundtrip_path,
            "roundtrip SQLite fixture",
            pool,
        )?,
        replay_verified: source_rows.len() == roundtrip_rows.len()
            && source_rows.len() == roundtrip_row_count
            && source_content_digest == roundtrip_content_digest,
        source_content_digest,
        roundtrip_content_digest,
    })
}

fn read_file_digest(
    path: &Path,
    label: &str,
    pool: &LiveMemoryPool,
) -> Result<String, ShardLoomError> {
    let metadata = fs::metadata(path).map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "failed to inspect {label} '{}': {error}; no fallback execution was attempted",
            path.display()
        ))
    })?;
    if metadata.len() > MAX_SQLITE_FIXTURE_BYTES {
        return Err(ShardLoomError::InvalidOperation(format!(
            "{label} '{}' is {} bytes; scoped SQLite fixture reads admit at most {MAX_SQLITE_FIXTURE_BYTES} bytes; no fallback execution was attempted",
            path.display(),
            metadata.len()
        )));
    }
    let bytes =
        crate::fixture_io::read_bytes(path, None, metadata.len(), pool).map_err(|error| {
            ShardLoomError::InvalidOperation(format!(
                "failed to read {label} '{}': {error}; no fallback execution was attempted",
                path.display()
            ))
        })?;
    Ok(fnv64_digest_bytes(bytes.value()))
}

fn validate_sqlite_fixture_shape(
    columns: &[SqliteColumn],
    table: &str,
    order_by: Option<&str>,
) -> Result<(), ShardLoomError> {
    if columns.is_empty() {
        return Err(ShardLoomError::InvalidOperation(format!(
            "SQLite table {table:?} has no visible columns; no fallback execution was attempted"
        )));
    }
    if let Some(column) = columns
        .iter()
        .find(|column| safe_sqlite_declared_type(&column.declared_type) == "BLOB")
    {
        return Err(ShardLoomError::InvalidOperation(format!(
            "SQLite column {:?} declares BLOB storage, which is not admitted by the local scalar fixture; no fallback execution was attempted",
            column.name
        )));
    }
    if let Some(order_by) = order_by
        && !columns.iter().any(|column| column.name == order_by)
    {
        return Err(ShardLoomError::InvalidOperation(format!(
            "SQLite order-by column {order_by:?} is not present in table {table:?}; no fallback execution was attempted"
        )));
    }
    Ok(())
}

fn canonical_existing_file(path: &Path) -> Result<PathBuf, ShardLoomError> {
    let canonical = fs::canonicalize(path).map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "local SQLite fixture '{}' must exist and be canonicalizable: {error}; no fallback execution was attempted",
            path.display()
        ))
    })?;
    if !canonical.is_file() {
        return Err(ShardLoomError::InvalidOperation(format!(
            "local SQLite fixture '{}' is not a file; no fallback execution was attempted",
            canonical.display()
        )));
    }
    let metadata = fs::metadata(&canonical).map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "local SQLite fixture '{}' could not be statted: {error}; no fallback execution was attempted",
            canonical.display()
        ))
    })?;
    if metadata.len() > MAX_SQLITE_FIXTURE_BYTES {
        return Err(ShardLoomError::InvalidOperation(format!(
            "local SQLite fixture '{}' is {} bytes; scoped SQLite fixture reads admit at most {MAX_SQLITE_FIXTURE_BYTES} bytes; no fallback execution was attempted",
            canonical.display(),
            metadata.len()
        )));
    }
    Ok(canonical)
}

fn require_table(conn: &Connection, table: &str) -> Result<(), ShardLoomError> {
    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM sqlite_schema WHERE type = 'table' AND name = ?1",
            [table],
            |row| row.get(0),
        )
        .map_err(sqlite_error)?;
    if count == 0 {
        return Err(ShardLoomError::InvalidOperation(format!(
            "SQLite table {table:?} was not found; no fallback execution was attempted"
        )));
    }
    Ok(())
}

fn load_table_columns(
    conn: &Connection,
    table: &str,
    pool: &LiveMemoryPool,
) -> Result<Budgeted<Vec<SqliteColumn>>, ShardLoomError> {
    let mut statement = conn
        .prepare(&format!("PRAGMA table_info({})", quote_identifier(table)))
        .map_err(sqlite_error)?;
    let mut rows = statement.query([]).map_err(sqlite_error)?;
    let mut lease = pool.reserve(0)?;
    let mut columns = Vec::new();
    while let Some(row) = rows.next().map_err(sqlite_error)? {
        crate::fixture_io::reserve_additional(&mut columns, 1, &mut lease)?;
        columns.push(SqliteColumn {
            name: crate::fixture_io::copy_text(
                row.get_ref(1)
                    .map_err(sqlite_error)?
                    .as_str()
                    .map_err(|error| {
                        ShardLoomError::InvalidOperation(format!(
                            "SQLite column name is not UTF-8 text: {error}"
                        ))
                    })?,
                &mut lease,
            )?,
            declared_type: crate::fixture_io::copy_text(
                row.get_ref(2)
                    .ok()
                    .and_then(|value| value.as_str().ok())
                    .unwrap_or_default(),
                &mut lease,
            )?,
            not_null: row.get::<_, i64>(3).map_err(sqlite_error)? != 0,
            primary_key_position: row.get::<_, i64>(5).map_err(sqlite_error)?,
        });
    }
    Ok(Budgeted::new(columns, lease))
}

fn read_rows(
    conn: &Connection,
    table: &str,
    columns: &[SqliteColumn],
    order_by: Option<&str>,
    pool: &LiveMemoryPool,
) -> Result<Budgeted<Vec<SqliteRow>>, ShardLoomError> {
    let mut sql = crate::fixture_io::Text::new(pool)?;
    let _ = sql.write_str("SELECT ");
    for (index, column) in columns.iter().enumerate() {
        if index > 0 {
            let _ = sql.write_str(", ");
        }
        let _ = write!(sql, "{}", QuotedIdentifier(&column.name));
    }
    let _ = write!(sql, " FROM {}", QuotedIdentifier(table));
    let sql = sql.finish()?;
    let mut statement = conn.prepare(sql.value()).map_err(sqlite_error)?;
    let mut sqlite_rows = statement.query([]).map_err(sqlite_error)?;
    let mut lease = pool.reserve(0)?;
    let mut rows = Vec::new();
    while let Some(row) = sqlite_rows.next().map_err(sqlite_error)? {
        validate_sqlite_row_budget(rows.len() + 1, "source SQLite table")?;
        crate::fixture_io::reserve_additional(&mut rows, 1, &mut lease)?;
        let mut cells = Vec::new();
        crate::fixture_io::reserve_additional(&mut cells, columns.len(), &mut lease)?;
        for index in 0..columns.len() {
            let cell = match row.get_ref(index).map_err(sqlite_error)? {
                ValueRef::Null => SqliteCell::Null,
                ValueRef::Integer(value) => SqliteCell::Integer(value),
                ValueRef::Real(value) if value.is_finite() => SqliteCell::Real(value),
                ValueRef::Real(_) => {
                    return Err(ShardLoomError::InvalidOperation(
                        "SQLite REAL NaN/Infinity values are not admitted by the local scalar fixture; no fallback execution was attempted"
                            .to_string(),
                    ));
                }
                ValueRef::Text(value) => SqliteCell::Text(crate::fixture_io::copy_text(
                    std::str::from_utf8(value)
                        .map_err(|error| {
                            ShardLoomError::InvalidOperation(format!(
                                "SQLite TEXT value is not UTF-8: {error}; no fallback execution was attempted"
                            ))
                        })?,
                    &mut lease,
                )?),
                ValueRef::Blob(_) => {
                    return Err(ShardLoomError::InvalidOperation(
                        "SQLite BLOB values are not admitted by this scalar fixture smoke; no fallback execution was attempted"
                            .to_string(),
                    ));
                }
            };
            cells.push(cell);
        }
        rows.push(SqliteRow { cells });
    }
    apply_fixture_order(&mut rows, columns, order_by, pool)?;
    Ok(Budgeted::new(rows, lease))
}

fn apply_fixture_order(
    rows: &mut [SqliteRow],
    columns: &[SqliteColumn],
    order_by: Option<&str>,
    pool: &LiveMemoryPool,
) -> Result<(), ShardLoomError> {
    let Some(order_by) = order_by else {
        return Ok(());
    };
    let index = columns
        .iter()
        .position(|column| column.name == order_by)
        .ok_or_else(|| {
            ShardLoomError::InvalidOperation(format!(
                "SQLite order-by column {order_by:?} is not present in scanned fixture columns; no fallback execution was attempted"
            ))
        })?;
    let scratch_bytes = rows
        .len()
        .checked_mul(std::mem::size_of::<SqliteRow>())
        .and_then(|value| u64::try_from(value).ok())
        .ok_or_else(|| {
            ShardLoomError::InvalidOperation("SQLite sort scratch size overflow".into())
        })?;
    let _scratch = pool.reserve(scratch_bytes)?;
    rows.sort_by(|left, right| compare_sqlite_cells(&left.cells[index], &right.cells[index]));
    Ok(())
}

fn compare_sqlite_cells(left: &SqliteCell, right: &SqliteCell) -> Ordering {
    match (left, right) {
        (SqliteCell::Null, SqliteCell::Null) => Ordering::Equal,
        (SqliteCell::Null, _) => Ordering::Less,
        (_, SqliteCell::Null) => Ordering::Greater,
        (SqliteCell::Integer(left), SqliteCell::Integer(right)) => left.cmp(right),
        (SqliteCell::Real(left), SqliteCell::Real(right)) => {
            left.partial_cmp(right).unwrap_or(Ordering::Equal)
        }
        (SqliteCell::Integer(left), SqliteCell::Real(right)) => i64_to_f64_for_fixture_order(*left)
            .partial_cmp(right)
            .unwrap_or(Ordering::Equal),
        (SqliteCell::Real(left), SqliteCell::Integer(right)) => left
            .partial_cmp(&i64_to_f64_for_fixture_order(*right))
            .unwrap_or(Ordering::Equal),
        (SqliteCell::Text(left), SqliteCell::Text(right)) => left.cmp(right),
        (left, right) => sqlite_cell_kind(left).cmp(sqlite_cell_kind(right)),
    }
}

fn i64_to_f64_for_fixture_order(value: i64) -> f64 {
    value.to_string().parse::<f64>().unwrap_or_else(|_| {
        if value.is_negative() {
            f64::NEG_INFINITY
        } else {
            f64::INFINITY
        }
    })
}

fn sqlite_cell_kind(cell: &SqliteCell) -> &'static str {
    match cell {
        SqliteCell::Null => "0_null",
        SqliteCell::Integer(_) | SqliteCell::Real(_) => "1_numeric",
        SqliteCell::Text(_) => "2_text",
    }
}

fn render_jsonl(
    columns: &[SqliteColumn],
    rows: &[SqliteRow],
    pool: &LiveMemoryPool,
) -> Result<Budgeted<String>, ShardLoomError> {
    let mut out = crate::fixture_io::Text::new(pool)?;
    for row in rows {
        let _ = out.write_char('{');
        for (index, column) in columns.iter().enumerate() {
            if index > 0 {
                let _ = out.write_char(',');
            }
            let value = row.cells.get(index).ok_or_else(|| {
                ShardLoomError::InvalidOperation(
                    "SQLite row width did not match column metadata; no fallback execution was attempted"
                        .to_string(),
                )
            })?;
            let _ = write!(out, "{}:", JsonString(&column.name));
            match value {
                SqliteCell::Null => { let _ = out.write_str("null"); }
                SqliteCell::Integer(value) => { let _ = write!(out, "{value}"); }
                SqliteCell::Real(value) if value.is_finite() => { let _ = write!(out, "{value}"); }
                SqliteCell::Real(_) => return Err(ShardLoomError::InvalidOperation(
                    "SQLite REAL NaN/Infinity values are not admitted by the JSONL fixture export; no fallback execution was attempted".into(),
                )),
                SqliteCell::Text(value) => { let _ = write!(out, "{}", JsonString(value)); }
            }
        }
        let _ = out.write_str("}\n");
        if out.len() > MAX_SQLITE_EXPORT_JSONL_BYTES {
            return Err(ShardLoomError::InvalidOperation(format!(
                "SQLite JSONL export exceeded scoped render budget {MAX_SQLITE_EXPORT_JSONL_BYTES} bytes; no fallback execution was attempted"
            )));
        }
    }
    out.finish()
}

fn sqlite_typed_content_digest(
    columns: &[SqliteColumn],
    rows: &[SqliteRow],
) -> Result<String, ShardLoomError> {
    let mut canonical = FnvDigest::new();
    let _ = write!(canonical, "columns:{}=", columns.len());
    for (index, column) in columns.iter().enumerate() {
        if index > 0 {
            let _ = canonical.write_char('|');
        }
        let safe_declared_type = safe_sqlite_declared_type(&column.declared_type);
        let _ = write!(
            canonical,
            "name:{}:{}:type:{}:{}:not_null:{}:primary_key_position:{}",
            column.name.len(),
            column.name,
            safe_declared_type.len(),
            safe_declared_type,
            column.not_null,
            column.primary_key_position
        );
    }
    let _ = write!(canonical, "\nrows:{}=", rows.len());
    for row in rows {
        let _ = canonical.write_char('\n');
        let _ = write!(canonical, "cells:{}:", row.cells.len());
        for (index, cell) in row.cells.iter().enumerate() {
            if index > 0 {
                let _ = canonical.write_char('|');
            }
            sqlite_typed_cell_digest_fragment(&mut canonical, cell)?;
        }
    }
    Ok(canonical.finish())
}

fn sqlite_typed_cell_digest_fragment(
    out: &mut FnvDigest,
    cell: &SqliteCell,
) -> Result<(), ShardLoomError> {
    match cell {
        SqliteCell::Null => {
            let _ = out.write_str("null:");
        }
        SqliteCell::Integer(value) => {
            let _ = write!(out, "integer:{value}");
        }
        SqliteCell::Real(value) if value.is_finite() => {
            let _ = write!(out, "real:{value:?}");
        }
        SqliteCell::Real(_) => {
            return Err(ShardLoomError::InvalidOperation(
                "SQLite REAL NaN/Infinity values are not admitted by the typed roundtrip replay digest; no fallback execution was attempted"
                    .to_string(),
            ));
        }
        SqliteCell::Text(value) => {
            let _ = write!(out, "text:{}:", value.len());
            let _ = out.write_str(value);
        }
    }
    Ok(())
}

fn write_roundtrip_database(
    plan: &WorkspaceSafeLocalWritePlan,
    table: &str,
    columns: &[SqliteColumn],
    rows: &[SqliteRow],
    pool: &LiveMemoryPool,
) -> Result<(), ShardLoomError> {
    // Admit all caller-sized SQL text before creating or replacing the target.
    // Bound cell values borrow the already-accounted rows instead of cloning.
    let mut create_sql = crate::fixture_io::Text::new(pool)?;
    let _ = write!(create_sql, "CREATE TABLE {} (", QuotedIdentifier(table));
    let mut insert_sql = crate::fixture_io::Text::new(pool)?;
    let _ = write!(insert_sql, "INSERT INTO {} (", QuotedIdentifier(table));
    for (index, column) in columns.iter().enumerate() {
        if index > 0 {
            let _ = create_sql.write_str(", ");
            let _ = insert_sql.write_str(", ");
        }
        let _ = write!(
            create_sql,
            "{} {}",
            QuotedIdentifier(&column.name),
            safe_sqlite_declared_type(&column.declared_type)
        );
        let _ = write!(insert_sql, "{}", QuotedIdentifier(&column.name));
    }
    let _ = create_sql.write_char(')');
    let _ = insert_sql.write_str(") VALUES (");
    for index in 0..columns.len() {
        if index > 0 {
            let _ = insert_sql.write_str(", ");
        }
        let _ = insert_sql.write_char('?');
    }
    let _ = insert_sql.write_char(')');
    let create_sql = create_sql.finish()?;
    let insert_sql = insert_sql.finish()?;
    fs::create_dir_all(&plan.parent_path).map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "failed to create roundtrip SQLite directory '{}': {error}; no fallback execution was attempted",
            plan.parent_path.display()
        ))
    })?;
    if plan.target_existed_before {
        if plan.overwrite_allowed {
            fs::remove_file(&plan.target_path).map_err(|error| {
                ShardLoomError::InvalidOperation(format!(
                    "failed to replace existing roundtrip SQLite file '{}': {error}; no fallback execution was attempted",
                    plan.target_path.display()
                ))
            })?;
        } else {
            return Err(ShardLoomError::InvalidOperation(format!(
                "roundtrip SQLite target '{}' already exists and overwrite is disabled; no fallback execution was attempted",
                plan.target_path.display()
            )));
        }
    }
    let mut conn = Connection::open(&plan.target_path).map_err(sqlite_error)?;
    conn.execute(create_sql.value(), []).map_err(sqlite_error)?;
    let tx = conn.transaction().map_err(sqlite_error)?;
    {
        let mut statement = tx.prepare(insert_sql.value()).map_err(sqlite_error)?;
        for row in rows {
            statement
                .execute(params_from_iter(row.cells.iter()))
                .map_err(sqlite_error)?;
        }
    }
    tx.commit().map_err(sqlite_error)
}

fn count_table_rows(conn: &Connection, table: &str) -> Result<usize, ShardLoomError> {
    let count: i64 = conn
        .query_row(
            &format!("SELECT COUNT(*) FROM {}", quote_identifier(table)),
            [],
            |row| row.get(0),
        )
        .map_err(sqlite_error)?;
    usize::try_from(count).map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "SQLite row count was invalid: {error}; no fallback execution was attempted"
        ))
    })
}

fn validate_sqlite_row_budget(row_count: usize, label: &str) -> Result<(), ShardLoomError> {
    if row_count > MAX_SQLITE_FIXTURE_ROWS {
        return Err(ShardLoomError::InvalidOperation(format!(
            "{label} has {row_count} rows; scoped SQLite import/export smoke admits at most {MAX_SQLITE_FIXTURE_ROWS} rows; no fallback execution was attempted"
        )));
    }
    Ok(())
}

fn count_roundtrip_rows(path: &Path, table: &str) -> Result<usize, ShardLoomError> {
    let conn = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)
        .map_err(sqlite_error)?;
    count_table_rows(&conn, table)
}

fn read_roundtrip_rows(
    path: &Path,
    table: &str,
    columns: &[SqliteColumn],
    order_by: Option<&str>,
    pool: &LiveMemoryPool,
) -> Result<Budgeted<Vec<SqliteRow>>, ShardLoomError> {
    let conn = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)
        .map_err(sqlite_error)?;
    require_table(&conn, table)?;
    read_rows(&conn, table, columns, order_by, pool)
}

fn sqlite_local_import_export_fields(
    report: &LocalSqliteImportExportReport,
) -> Vec<(String, String)> {
    let mut fields = sqlite_local_identity_fields(report);
    fields.extend(sqlite_local_artifact_fields(report));
    fields.extend(sqlite_local_policy_fields(report));
    fields.extend(report.export_write_report.evidence_fields("sqlite_export"));
    fields.extend(roundtrip_plan_fields(&report.roundtrip_write_plan));
    append_effectful_operation_admission_matrix_fields(&mut fields);
    fields
}

fn sqlite_local_identity_fields(report: &LocalSqliteImportExportReport) -> Vec<(String, String)> {
    vec![
        (
            "fallback_execution_allowed".to_string(),
            "false".to_string(),
        ),
        (
            "mode".to_string(),
            "sqlite_local_import_export_smoke".to_string(),
        ),
        (
            "schema_version".to_string(),
            report.schema_version.to_string(),
        ),
        ("adapter_id".to_string(), report.adapter_id.to_string()),
        (
            "source_adapter_id".to_string(),
            report.source_adapter_id.to_string(),
        ),
        (
            "source_database_path".to_string(),
            report.source_db.display().to_string(),
        ),
        (
            "canonical_source_database_path".to_string(),
            report.canonical_source_db.display().to_string(),
        ),
        ("sqlite_table".to_string(), report.table.clone()),
        ("column_order".to_string(), report.column_order.join(",")),
        (
            "column_declared_types".to_string(),
            report.column_declared_types.join(","),
        ),
        (
            "not_null_columns".to_string(),
            empty_as_none(report.not_null_columns.join(",")),
        ),
        (
            "primary_key_columns".to_string(),
            empty_as_none(report.primary_key_columns.join(",")),
        ),
        (
            "source_row_count".to_string(),
            report.source_row_count.to_string(),
        ),
        (
            "exported_row_count".to_string(),
            report.exported_row_count.to_string(),
        ),
        (
            "roundtrip_row_count".to_string(),
            report.roundtrip_row_count.to_string(),
        ),
        (
            "source_database_digest".to_string(),
            report.source_database_digest.clone(),
        ),
    ]
}

fn sqlite_local_artifact_fields(report: &LocalSqliteImportExportReport) -> Vec<(String, String)> {
    vec![
        (
            "export_jsonl_path".to_string(),
            report.export_write_report.target_path.display().to_string(),
        ),
        (
            "export_jsonl_digest".to_string(),
            report.export_jsonl_digest.clone(),
        ),
        (
            "roundtrip_database_path".to_string(),
            report
                .roundtrip_write_plan
                .target_path
                .display()
                .to_string(),
        ),
        (
            "roundtrip_database_digest".to_string(),
            report.roundtrip_database_digest.clone(),
        ),
        (
            "source_roundtrip_content_digest".to_string(),
            report.source_roundtrip_content_digest.clone(),
        ),
        (
            "roundtrip_content_digest".to_string(),
            report.roundtrip_content_digest.clone(),
        ),
        (
            "roundtrip_replay_verification_method".to_string(),
            report.roundtrip_replay_verification_method.to_string(),
        ),
        (
            "roundtrip_replay_verified".to_string(),
            report.roundtrip_replay_verified.to_string(),
        ),
        (
            "order_by".to_string(),
            report
                .order_by
                .clone()
                .unwrap_or_else(|| "none".to_string()),
        ),
        (
            "allow_overwrite".to_string(),
            report.allow_overwrite.to_string(),
        ),
    ]
}

fn sqlite_local_policy_fields(report: &LocalSqliteImportExportReport) -> Vec<(String, String)> {
    vec![
        (
            "sqlite_sql_execution_scope".to_string(),
            report.sqlite_sql_execution_scope.to_string(),
        ),
        (
            "sqlite_query_pushdown_allowed".to_string(),
            report.sqlite_query_pushdown_allowed.to_string(),
        ),
        (
            "sqlite_ordering_execution_scope".to_string(),
            report.sqlite_ordering_execution_scope.to_string(),
        ),
        (
            "credential_policy_status".to_string(),
            report.credential_policy_status.to_string(),
        ),
        (
            "network_policy".to_string(),
            report.network_policy.to_string(),
        ),
        (
            "dynamic_loading_performed".to_string(),
            report.dynamic_loading_performed.to_string(),
        ),
        (
            "extension_code_executed".to_string(),
            report.extension_code_executed.to_string(),
        ),
        (
            "external_effect_executed".to_string(),
            report.external_effect_executed.to_string(),
        ),
        (
            "fallback_attempted".to_string(),
            report.fallback_attempted.to_string(),
        ),
        (
            "external_engine_invoked".to_string(),
            report.external_engine_invoked.to_string(),
        ),
        (
            "claim_gate_status".to_string(),
            report.claim_gate_status.to_string(),
        ),
        (
            "claim_boundary".to_string(),
            report.claim_boundary.to_string(),
        ),
    ]
}

fn roundtrip_plan_fields(plan: &WorkspaceSafeLocalWritePlan) -> Vec<(String, String)> {
    vec![
        (
            "sqlite_roundtrip_workspace_path_safety_status".to_string(),
            "enforced".to_string(),
        ),
        (
            "sqlite_roundtrip_workspace_root".to_string(),
            plan.path_safety_report.workspace_root.clone(),
        ),
        (
            "sqlite_roundtrip_canonical_workspace_root".to_string(),
            plan.path_safety_report.canonical_workspace_root.clone(),
        ),
        (
            "sqlite_roundtrip_requested_output_path".to_string(),
            plan.path_safety_report.requested_output_path.clone(),
        ),
        (
            "sqlite_roundtrip_canonical_output_path".to_string(),
            plan.path_safety_report.canonical_output_path.clone(),
        ),
        (
            "sqlite_roundtrip_within_workspace".to_string(),
            plan.path_safety_report.within_workspace.to_string(),
        ),
        (
            "sqlite_roundtrip_overwrite_allowed".to_string(),
            plan.overwrite_allowed.to_string(),
        ),
        (
            "sqlite_roundtrip_target_existed_before".to_string(),
            plan.target_existed_before.to_string(),
        ),
    ]
}

fn safe_sqlite_declared_type(declared: &str) -> &'static str {
    let normalized = declared.to_ascii_uppercase();
    if normalized.contains("INT") {
        "INTEGER"
    } else if normalized.contains("CHAR")
        || normalized.contains("CLOB")
        || normalized.contains("TEXT")
    {
        "TEXT"
    } else if normalized.contains("BLOB") {
        "BLOB"
    } else if normalized.contains("REAL")
        || normalized.contains("FLOA")
        || normalized.contains("DOUB")
    {
        "REAL"
    } else {
        "NUMERIC"
    }
}

fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

struct QuotedIdentifier<'a>(&'a str);

impl std::fmt::Display for QuotedIdentifier<'_> {
    fn fmt(&self, out: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        out.write_char('"')?;
        for ch in self.0.chars() {
            if ch == '"' {
                out.write_char('"')?;
            }
            out.write_char(ch)?;
        }
        out.write_char('"')
    }
}

struct JsonString<'a>(&'a str);

impl std::fmt::Display for JsonString<'_> {
    fn fmt(&self, out: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        out.write_char('"')?;
        for ch in self.0.chars() {
            match ch {
                '\\' => out.write_str("\\\\")?,
                '"' => out.write_str("\\\"")?,
                '\n' => out.write_str("\\n")?,
                '\r' => out.write_str("\\r")?,
                '\t' => out.write_str("\\t")?,
                '\u{08}' => out.write_str("\\b")?,
                '\u{0C}' => out.write_str("\\f")?,
                c if c.is_control() => write!(out, "\\u{:04X}", c as u32)?,
                c => out.write_char(c)?,
            }
        }
        out.write_char('"')
    }
}

struct FnvDigest(u64);

impl FnvDigest {
    fn new() -> Self {
        Self(0xcbf2_9ce4_8422_2325)
    }

    fn update(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.0 ^= u64::from(*byte);
            self.0 = self.0.wrapping_mul(0x0100_0000_01b3);
        }
    }

    fn finish(self) -> String {
        format!("fnv64:{:016x}", self.0)
    }
}

impl std::fmt::Write for FnvDigest {
    fn write_str(&mut self, value: &str) -> std::fmt::Result {
        self.update(value.as_bytes());
        Ok(())
    }
}

fn fnv64_digest_bytes(bytes: &[u8]) -> String {
    let mut digest = FnvDigest::new();
    digest.update(bytes);
    digest.finish()
}

fn empty_as_none(value: String) -> String {
    if value.is_empty() {
        "none".to_string()
    } else {
        value
    }
}

#[allow(clippy::needless_pass_by_value)]
fn sqlite_error(error: rusqlite::Error) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "SQLite local adapter smoke failed: {error}; no fallback execution was attempted"
    ))
}

#[allow(clippy::needless_pass_by_value)] // Direct map_err adapter.
fn fixture_io_error(error: std::io::Error) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "SQLite fixture I/O failed: {error}; no fallback execution was attempted"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn column(name: &str, declared_type: &str) -> SqliteColumn {
        SqliteColumn {
            name: name.to_string(),
            declared_type: declared_type.to_string(),
            not_null: false,
            primary_key_position: 0,
        }
    }

    #[test]
    fn sqlite_rows_and_rendering_retain_shared_credit_and_release_on_denial() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute("CREATE TABLE data (value TEXT)", []).unwrap();
        let payload = "a\"\né".repeat(512);
        conn.execute("INSERT INTO data VALUES (?1)", [&payload])
            .unwrap();
        let columns = vec![column("value", "TEXT")];
        let tiny = LiveMemoryPool::new(256).unwrap();
        let error = read_rows(&conn, "data", &columns, None, &tiny).unwrap_err();
        assert!(error.to_string().contains("memory reservation denied"));
        assert_eq!(tiny.snapshot().reserved_bytes, 0);

        let pool = LiveMemoryPool::new(128 << 10).unwrap();
        let columns = load_table_columns(&conn, "data", &pool).unwrap();
        let rows = read_rows(&conn, "data", columns.value(), Some("value"), &pool).unwrap();
        assert_eq!(
            rows.value()[0].cells,
            vec![SqliteCell::Text(payload.clone())]
        );
        assert!(pool.snapshot().reserved_bytes >= payload.len() as u64);
        let retained = pool.snapshot().reserved_bytes;
        let full = pool
            .reserve(pool.snapshot().limit_bytes - retained)
            .unwrap();
        let error = render_jsonl(columns.value(), rows.value(), &pool).unwrap_err();
        assert!(error.to_string().contains("memory reservation denied"));
        drop(full);
        assert_eq!(pool.snapshot().reserved_bytes, retained);
        let jsonl = render_jsonl(columns.value(), rows.value(), &pool).unwrap();
        let decoded: serde_json::Value = serde_json::from_str(jsonl.value()).unwrap();
        assert_eq!(decoded["value"], payload);
        drop((jsonl, rows, columns));
        assert_eq!(pool.snapshot().reserved_bytes, 0);
        assert!(pool.snapshot().peak_reserved_bytes <= pool.snapshot().limit_bytes);
    }

    #[test]
    fn streaming_typed_digest_and_jsonl_preserve_the_original_bytes() {
        let columns = vec![column("amount", "NUMERIC")];
        let rows = vec![
            SqliteRow {
                cells: vec![SqliteCell::Integer(1)],
            },
            SqliteRow {
                cells: vec![SqliteCell::Real(1.0)],
            },
            SqliteRow {
                cells: vec![SqliteCell::Text("é|\n".into())],
            },
            SqliteRow {
                cells: vec![SqliteCell::Null],
            },
        ];
        let canonical = concat!(
            "columns:1=name:6:amount:type:7:NUMERIC:not_null:false:primary_key_position:0",
            "\nrows:4=\ncells:1:integer:1\ncells:1:real:1.0\ncells:1:text:4:é|\n\ncells:1:null:",
        );
        assert_eq!(
            sqlite_typed_content_digest(&columns, &rows).unwrap(),
            fnv64_digest_bytes(canonical.as_bytes())
        );
        let pool = LiveMemoryPool::new(4096).unwrap();
        assert_eq!(
            render_jsonl(&columns, &rows, &pool).unwrap().value(),
            "{\"amount\":1}\n{\"amount\":1}\n{\"amount\":\"é|\\n\"}\n{\"amount\":null}\n"
        );
        assert_eq!(pool.snapshot().reserved_bytes, 0);
    }

    #[test]
    fn sqlite_fixture_digest_rejects_file_over_byte_budget() {
        let path = std::env::temp_dir().join(format!(
            "shardloom-sqlite-oversized-fixture-{}.sqlite",
            std::process::id()
        ));
        let file = fs::File::create(&path).expect("create sparse SQLite fixture");
        file.set_len(MAX_SQLITE_FIXTURE_BYTES + 1)
            .expect("set sparse SQLite fixture length");

        let error = read_file_digest(
            &path,
            "local SQLite fixture",
            &LiveMemoryPool::new(16 << 20).unwrap(),
        )
        .expect_err("oversized SQLite fixture blocked");
        let _ = fs::remove_file(&path);

        assert!(
            error
                .to_string()
                .contains("scoped SQLite fixture reads admit at most"),
            "{error}"
        );
    }

    #[test]
    fn sqlite_row_budget_rejects_oversized_table_count() {
        let error = validate_sqlite_row_budget(MAX_SQLITE_FIXTURE_ROWS + 1, "source SQLite table")
            .expect_err("oversized row count blocked");

        assert!(
            error
                .to_string()
                .contains("scoped SQLite import/export smoke admits at most"),
            "{error}"
        );
    }

    #[test]
    fn typed_content_digest_distinguishes_equal_json_values_with_different_sqlite_types() {
        let columns = vec![column("amount", "NUMERIC")];
        let integer_rows = vec![SqliteRow {
            cells: vec![SqliteCell::Integer(1)],
        }];
        let real_rows = vec![SqliteRow {
            cells: vec![SqliteCell::Real(1.0)],
        }];

        assert_ne!(
            sqlite_typed_content_digest(&columns, &integer_rows).expect("integer digest"),
            sqlite_typed_content_digest(&columns, &real_rows).expect("real digest")
        );
    }
}
