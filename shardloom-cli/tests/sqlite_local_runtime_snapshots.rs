use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
};

use rusqlite::Connection;

fn field(key: &str, value: &str) -> String {
    format!("{{\"key\":\"{key}\",\"value\":\"{value}\"}}")
}

fn unique_target_dir(label: &str) -> PathBuf {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("workspace root")
        .join("target")
        .join(format!(
            "{label}-{}-{}",
            std::process::id(),
            std::thread::current().name().unwrap_or("test")
        ));
    fs::create_dir_all(&dir).expect("target dir");
    dir
}

fn create_sqlite_fixture(path: &Path) {
    let conn = Connection::open(path).expect("open fixture db");
    conn.execute(
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, label TEXT NOT NULL, amount REAL)",
        [],
    )
    .expect("create table");
    conn.execute(
        "INSERT INTO orders (id, label, amount) VALUES (1, 'alpha', 8.5)",
        [],
    )
    .expect("insert row 1");
    conn.execute(
        "INSERT INTO orders (id, label, amount) VALUES (2, 'beta', NULL)",
        [],
    )
    .expect("insert row 2");
}

fn create_blob_sqlite_fixture(path: &Path) {
    let conn = Connection::open(path).expect("open fixture db");
    conn.execute("CREATE TABLE blobs (id INTEGER, payload BLOB)", [])
        .expect("create blob table");
}

fn run_json(args: &[String]) -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(args)
        .output()
        .expect("command runs");
    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        output.stderr.is_empty(),
        "stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).expect("stdout is utf8")
}

fn run_json_error(args: &[String]) -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(args)
        .output()
        .expect("command runs");
    assert!(
        !output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        output.stderr.is_empty(),
        "stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).expect("stdout is utf8")
}

#[test]
fn sqlite_local_import_export_smoke_exports_jsonl_and_roundtrips_without_effects() {
    let dir = unique_target_dir("sqlite-local-import-export-smoke");
    let source_db = dir.join("orders.sqlite");
    let export_jsonl = dir.join("orders.jsonl");
    let roundtrip_db = dir.join("orders-roundtrip.sqlite");
    create_sqlite_fixture(&source_db);

    let output = run_json(&[
        "sqlite-local-import-export-smoke".to_string(),
        source_db.display().to_string(),
        "--table".to_string(),
        "orders".to_string(),
        "--export-jsonl".to_string(),
        export_jsonl.display().to_string(),
        "--roundtrip-db".to_string(),
        roundtrip_db.display().to_string(),
        "--order-by".to_string(),
        "id".to_string(),
        "--format".to_string(),
        "json".to_string(),
    ]);

    assert!(output.contains("\"command\":\"sqlite-local-import-export-smoke\""));
    assert!(output.contains(&field(
        "schema_version",
        "shardloom.local_sqlite_import_export_smoke.v1"
    )));
    assert!(output.contains(&field("adapter_id", "local_sqlite_file_adapter")));
    assert!(output.contains(&field("source_adapter_id", "sqlite_input_adapter")));
    assert!(output.contains(&field("sqlite_table", "orders")));
    assert!(output.contains(&field("column_order", "id,label,amount")));
    assert!(output.contains(&field("source_row_count", "2")));
    assert!(output.contains(&field("exported_row_count", "2")));
    assert!(output.contains(&field("roundtrip_row_count", "2")));
    assert!(output.contains("\"source_roundtrip_content_digest\",\"value\":\"fnv64:"));
    assert!(output.contains("\"roundtrip_content_digest\",\"value\":\"fnv64:"));
    assert!(output.contains(&field(
        "roundtrip_replay_verification_method",
        "canonical_typed_row_digest"
    )));
    assert!(output.contains(&field("roundtrip_replay_verified", "true")));
    assert!(output.contains(&field(
        "sqlite_sql_execution_scope",
        "single_table_scan_only"
    )));
    assert!(output.contains(&field("sqlite_query_pushdown_allowed", "false")));
    assert!(output.contains(&field(
        "sqlite_ordering_execution_scope",
        "shardloom_fixture_post_scan"
    )));
    assert!(output.contains(&field(
        "credential_policy_status",
        "not_required_local_file_only"
    )));
    assert!(output.contains(&field("network_policy", "disabled_no_network_probe")));
    assert!(output.contains(&field("dynamic_loading_performed", "false")));
    assert!(output.contains(&field("extension_code_executed", "false")));
    assert!(output.contains(&field("external_effect_executed", "false")));
    assert!(output.contains(&field("fallback_attempted", "false")));
    assert!(output.contains(&field("external_engine_invoked", "false")));
    assert!(output.contains(&field("claim_gate_status", "fixture_smoke_only")));
    assert!(output.contains(&field(
        "effectful_operation_admission_row_local_sqlite_import_export_support_status",
        "fixture_smoke_supported"
    )));
    assert!(export_jsonl.exists());
    assert!(roundtrip_db.exists());
    let jsonl = fs::read_to_string(export_jsonl).expect("jsonl");
    assert!(jsonl.contains("\"label\":\"alpha\""));
    assert!(jsonl.contains("\"amount\":null"));
}

#[test]
fn sqlite_local_import_export_smoke_blocks_blob_columns() {
    let dir = unique_target_dir("sqlite-local-import-export-smoke-blob");
    let source_db = dir.join("blobs.sqlite");
    let export_jsonl = dir.join("blobs.jsonl");
    let roundtrip_db = dir.join("blobs-roundtrip.sqlite");
    create_blob_sqlite_fixture(&source_db);

    let output = run_json_error(&[
        "sqlite-local-import-export-smoke".to_string(),
        source_db.display().to_string(),
        "--table".to_string(),
        "blobs".to_string(),
        "--export-jsonl".to_string(),
        export_jsonl.display().to_string(),
        "--roundtrip-db".to_string(),
        roundtrip_db.display().to_string(),
        "--format".to_string(),
        "json".to_string(),
    ]);

    assert!(output.contains("\"status\":\"error\""));
    assert!(output.contains("declares BLOB storage"));
    assert!(output.contains("no fallback execution was attempted"));
    assert!(!export_jsonl.exists());
    assert!(!roundtrip_db.exists());
}
