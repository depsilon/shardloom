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
    types::{Value, ValueRef},
};
use shardloom_core::{
    CommandStatus, OutputFormat, ShardLoomError, WorkspaceSafeLocalWritePlan,
    WorkspaceSafeLocalWriteReport,
};

use crate::{
    cli_output::{emit, emit_error},
    extension_planning::append_effectful_operation_admission_matrix_fields,
};

#[derive(Debug, Clone)]
struct SqliteSmokeOptions {
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

impl SqliteCell {
    fn to_json(&self) -> Result<String, ShardLoomError> {
        match self {
            Self::Null => Ok("null".to_string()),
            Self::Integer(value) => Ok(value.to_string()),
            Self::Real(value) if value.is_finite() => Ok(value.to_string()),
            Self::Real(_) => Err(ShardLoomError::InvalidOperation(
                "SQLite REAL NaN/Infinity values are not admitted by the JSONL fixture export; no fallback execution was attempted"
                    .to_string(),
            )),
            Self::Text(value) => Ok(json_string(value)),
        }
    }

    fn to_rusqlite_value(&self) -> Value {
        match self {
            Self::Null => Value::Null,
            Self::Integer(value) => Value::Integer(*value),
            Self::Real(value) => Value::Real(*value),
            Self::Text(value) => Value::Text(value.clone()),
        }
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
    let report = match run_sqlite_local_import_export_smoke(&options) {
        Ok(report) => report,
        Err(error) => {
            return emit_error(
                "sqlite-local-import-export-smoke",
                format,
                "SQLite local import/export smoke failed",
                &error,
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
        sqlite_local_import_export_fields(&report),
    );
    ExitCode::SUCCESS
}

fn parse_sqlite_smoke_options(
    mut args: std::vec::IntoIter<String>,
) -> Result<SqliteSmokeOptions, ShardLoomError> {
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

fn run_sqlite_local_import_export_smoke(
    options: &SqliteSmokeOptions,
) -> Result<LocalSqliteImportExportReport, ShardLoomError> {
    let source_db = canonical_existing_file(&options.source_db)?;
    let source_database_digest = read_file_digest(&source_db, "local SQLite fixture")?;
    let source = Connection::open_with_flags(&source_db, OpenFlags::SQLITE_OPEN_READ_ONLY)
        .map_err(sqlite_error)?;
    require_table(&source, &options.table)?;
    let columns = load_table_columns(&source, &options.table)?;
    validate_sqlite_fixture_shape(&columns, &options.table, options.order_by.as_deref())?;
    let mut rows = read_rows(&source, &options.table, &columns)?;
    apply_fixture_order(&mut rows, &columns, options.order_by.as_deref())?;
    let jsonl = render_jsonl(&columns, &rows)?;
    let export_workspace_root =
        shardloom_core::infer_local_output_workspace_root(&options.export_jsonl)?;
    let export_write_report = shardloom_core::write_workspace_safe_bytes(
        export_workspace_root,
        &options.export_jsonl,
        options.allow_overwrite,
        "local SQLite table export JSONL",
        jsonl.as_bytes(),
    )?;
    let roundtrip_workspace_root =
        shardloom_core::infer_local_output_workspace_root(&options.roundtrip_db)?;
    let roundtrip_write_plan = shardloom_core::plan_workspace_safe_local_output(
        roundtrip_workspace_root,
        &options.roundtrip_db,
        options.allow_overwrite,
    )?;
    write_roundtrip_database(&roundtrip_write_plan, &options.table, &columns, &rows)?;
    let roundtrip_row_count =
        count_roundtrip_rows(&roundtrip_write_plan.target_path, &options.table)?;
    let roundtrip_rows = read_roundtrip_rows(
        &roundtrip_write_plan.target_path,
        &options.table,
        &columns,
        options.order_by.as_deref(),
    )?;
    let roundtrip_evidence = sqlite_roundtrip_evidence(
        &columns,
        &rows,
        &roundtrip_rows,
        roundtrip_row_count,
        &roundtrip_write_plan.target_path,
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
) -> Result<SqliteRoundtripEvidence, ShardLoomError> {
    let source_content_digest = sqlite_typed_content_digest(columns, source_rows)?;
    let roundtrip_content_digest = sqlite_typed_content_digest(columns, roundtrip_rows)?;
    Ok(SqliteRoundtripEvidence {
        roundtrip_database_digest: read_file_digest(roundtrip_path, "roundtrip SQLite fixture")?,
        replay_verified: source_rows.len() == roundtrip_rows.len()
            && source_rows.len() == roundtrip_row_count
            && source_content_digest == roundtrip_content_digest,
        source_content_digest,
        roundtrip_content_digest,
    })
}

fn read_file_digest(path: &Path, label: &str) -> Result<String, ShardLoomError> {
    let bytes = fs::read(path).map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "failed to read {label} '{}': {error}; no fallback execution was attempted",
            path.display()
        ))
    })?;
    Ok(fnv64_digest_bytes(&bytes))
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
    if let Some(order_by) = order_by {
        if !columns.iter().any(|column| column.name == order_by) {
            return Err(ShardLoomError::InvalidOperation(format!(
                "SQLite order-by column {order_by:?} is not present in table {table:?}; no fallback execution was attempted"
            )));
        }
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

fn load_table_columns(conn: &Connection, table: &str) -> Result<Vec<SqliteColumn>, ShardLoomError> {
    let mut statement = conn
        .prepare(&format!("PRAGMA table_info({})", quote_identifier(table)))
        .map_err(sqlite_error)?;
    let mut rows = statement.query([]).map_err(sqlite_error)?;
    let mut columns = Vec::new();
    while let Some(row) = rows.next().map_err(sqlite_error)? {
        columns.push(SqliteColumn {
            name: row.get::<_, String>(1).map_err(sqlite_error)?,
            declared_type: row.get::<_, String>(2).unwrap_or_default(),
            not_null: row.get::<_, i64>(3).map_err(sqlite_error)? != 0,
            primary_key_position: row.get::<_, i64>(5).map_err(sqlite_error)?,
        });
    }
    Ok(columns)
}

fn read_rows(
    conn: &Connection,
    table: &str,
    columns: &[SqliteColumn],
) -> Result<Vec<SqliteRow>, ShardLoomError> {
    let selected_columns = columns
        .iter()
        .map(|column| quote_identifier(&column.name))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!("SELECT {selected_columns} FROM {}", quote_identifier(table));
    let mut statement = conn.prepare(&sql).map_err(sqlite_error)?;
    let mut sqlite_rows = statement.query([]).map_err(sqlite_error)?;
    let mut rows = Vec::new();
    while let Some(row) = sqlite_rows.next().map_err(sqlite_error)? {
        let mut cells = Vec::with_capacity(columns.len());
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
                ValueRef::Text(value) => SqliteCell::Text(
                    std::str::from_utf8(value)
                        .map_err(|error| {
                            ShardLoomError::InvalidOperation(format!(
                                "SQLite TEXT value is not UTF-8: {error}; no fallback execution was attempted"
                            ))
                        })?
                        .to_string(),
                ),
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
    Ok(rows)
}

fn apply_fixture_order(
    rows: &mut [SqliteRow],
    columns: &[SqliteColumn],
    order_by: Option<&str>,
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
        (left, right) => sqlite_cell_kind(left)
            .cmp(sqlite_cell_kind(right))
            .then_with(|| sqlite_cell_sort_value(left).cmp(&sqlite_cell_sort_value(right))),
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

fn sqlite_cell_sort_value(cell: &SqliteCell) -> String {
    match cell {
        SqliteCell::Null => String::new(),
        SqliteCell::Integer(value) => format!("{value:020}"),
        SqliteCell::Real(value) => format!("{value:020.12}"),
        SqliteCell::Text(value) => value.clone(),
    }
}

fn render_jsonl(columns: &[SqliteColumn], rows: &[SqliteRow]) -> Result<String, ShardLoomError> {
    let mut out = String::new();
    for row in rows {
        out.push('{');
        for (index, column) in columns.iter().enumerate() {
            if index > 0 {
                out.push(',');
            }
            let value = row.cells.get(index).ok_or_else(|| {
                ShardLoomError::InvalidOperation(
                    "SQLite row width did not match column metadata; no fallback execution was attempted"
                        .to_string(),
                )
            })?;
            let _ = write!(out, "{}:{}", json_string(&column.name), value.to_json()?);
        }
        out.push_str("}\n");
    }
    Ok(out)
}

fn sqlite_typed_content_digest(
    columns: &[SqliteColumn],
    rows: &[SqliteRow],
) -> Result<String, ShardLoomError> {
    let mut canonical = String::new();
    let _ = write!(canonical, "columns:{}=", columns.len());
    for (index, column) in columns.iter().enumerate() {
        if index > 0 {
            canonical.push('|');
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
        canonical.push('\n');
        let _ = write!(canonical, "cells:{}:", row.cells.len());
        for (index, cell) in row.cells.iter().enumerate() {
            if index > 0 {
                canonical.push('|');
            }
            sqlite_typed_cell_digest_fragment(&mut canonical, cell)?;
        }
    }
    Ok(fnv64_digest_text(&canonical))
}

fn sqlite_typed_cell_digest_fragment(
    out: &mut String,
    cell: &SqliteCell,
) -> Result<(), ShardLoomError> {
    match cell {
        SqliteCell::Null => out.push_str("null:"),
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
            out.push_str(value);
        }
    }
    Ok(())
}

fn write_roundtrip_database(
    plan: &WorkspaceSafeLocalWritePlan,
    table: &str,
    columns: &[SqliteColumn],
    rows: &[SqliteRow],
) -> Result<(), ShardLoomError> {
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
    let column_defs = columns
        .iter()
        .map(|column| {
            format!(
                "{} {}",
                quote_identifier(&column.name),
                safe_sqlite_declared_type(&column.declared_type)
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    conn.execute(
        &format!("CREATE TABLE {} ({column_defs})", quote_identifier(table)),
        [],
    )
    .map_err(sqlite_error)?;
    let placeholders = (0..columns.len())
        .map(|_| "?")
        .collect::<Vec<_>>()
        .join(", ");
    let insert_sql = format!(
        "INSERT INTO {} ({}) VALUES ({placeholders})",
        quote_identifier(table),
        columns
            .iter()
            .map(|column| quote_identifier(&column.name))
            .collect::<Vec<_>>()
            .join(", ")
    );
    let tx = conn.transaction().map_err(sqlite_error)?;
    {
        let mut statement = tx.prepare(&insert_sql).map_err(sqlite_error)?;
        for row in rows {
            let values = row
                .cells
                .iter()
                .map(SqliteCell::to_rusqlite_value)
                .collect::<Vec<_>>();
            statement
                .execute(params_from_iter(values.iter()))
                .map_err(sqlite_error)?;
        }
    }
    tx.commit().map_err(sqlite_error)
}

fn count_roundtrip_rows(path: &Path, table: &str) -> Result<usize, ShardLoomError> {
    let conn = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)
        .map_err(sqlite_error)?;
    let count: i64 = conn
        .query_row(
            &format!("SELECT COUNT(*) FROM {}", quote_identifier(table)),
            [],
            |row| row.get(0),
        )
        .map_err(sqlite_error)?;
    usize::try_from(count).map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "roundtrip SQLite row count was invalid: {error}; no fallback execution was attempted"
        ))
    })
}

fn read_roundtrip_rows(
    path: &Path,
    table: &str,
    columns: &[SqliteColumn],
    order_by: Option<&str>,
) -> Result<Vec<SqliteRow>, ShardLoomError> {
    let conn = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)
        .map_err(sqlite_error)?;
    require_table(&conn, table)?;
    let mut rows = read_rows(&conn, table, columns)?;
    apply_fixture_order(&mut rows, columns, order_by)?;
    Ok(rows)
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

fn json_string(value: &str) -> String {
    let mut out = String::with_capacity(value.len() + 2);
    out.push('"');
    for ch in value.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{08}' => out.push_str("\\b"),
            '\u{0C}' => out.push_str("\\f"),
            c if c.is_control() => {
                let _ = write!(out, "\\u{:04X}", c as u32);
            }
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

fn fnv64_digest_bytes(bytes: &[u8]) -> String {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0100_0000_01b3);
    }
    format!("fnv64:{hash:016x}")
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

fn fnv64_digest_text(text: &str) -> String {
    fnv64_digest_bytes(text.as_bytes())
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
