//! Public ordinary SQL chooses metadata binding before native physical demand.
use super::*;
use shardloom_core::ScalarValue;
use std::{
    fs,
    os::unix::fs::MetadataExt as _,
    sync::atomic::{AtomicU64, Ordering},
};

struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "shardloom-public-demand-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).unwrap();
        let fixture = Self(path);
        Self::write(&fixture.path());
        fixture
    }
    fn path(&self) -> PathBuf {
        self.0.join("source.parquet")
    }
    fn write(path: &Path) {
        let names = vec!["id".into(), "value".into(), "unused".into()];
        let rows = (0..4)
            .map(|n| {
                vec![
                    ("id".into(), ScalarValue::Int64(n)),
                    ("value".into(), ScalarValue::Int64(100 + n)),
                    ("unused".into(), ScalarValue::Utf8(format!("unused-{n}"))),
                ]
            })
            .collect::<Vec<_>>();
        fs::write(
            path,
            shardloom_vortex::encode_flat_parquet_rows(&names, &rows).unwrap(),
        )
        .unwrap();
    }
    fn request(path: &Path) -> PublicWorkflowRouteRequest {
        let mut request = PublicWorkflowRouteRequest::new("sql".into());
        request.resources.memory_bytes = Some((32_u64 << 20).to_string());
        request.resources.max_parallelism = Some("1".into());
        request.input_uri = Some(path.display().to_string());
        request.input_format = Some("parquet".into());
        request.sql_statement = Some(format!("SELECT value FROM '{}' WHERE id=2", path.display()));
        request.bounded = true;
        request
    }
    fn prepare(
        request: &PublicWorkflowRouteRequest,
    ) -> Result<PreparedVortexRelational, ShardLoomError> {
        prepare_with_source(
            request.sql_statement.as_ref().unwrap(),
            request,
            None,
            PublicSourcePreparations::default(),
        )
        .map(|(operation, _)| operation)
    }
    fn target(path: &Path) -> PathBuf {
        auto_prepared_vortex_target_path_with_schema(path.to_str().unwrap(), "parquet", None)
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}

#[test]
fn public_parquet_demand_executes_fresh_sql_without_a_prepared_artifact() {
    let fixture = Fixture::new();
    let request = Fixture::request(&fixture.path());
    let operation = Fixture::prepare(&request).unwrap();
    assert!(!Fixture::target(&fixture.path()).exists());
    for _ in 0..2 {
        let collected = operation
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        assert_eq!(collected.result_jsonl.value().trim(), r#"{"value":102}"#);
        let input = collected.execution.input.as_ref().unwrap();
        assert_eq!(input.columnar_sources, 1);
        assert_eq!(input.columnar_projected_columns, 2);
        assert_eq!(input.columnar_available_columns, 3);
        assert!(
            !collected
                .execution
                .native_io_certificate
                .side_effects
                .fallback_attempted
        );
        assert!(!Fixture::target(&fixture.path()).exists());
    }
    assert!(!fixture.0.join(".shardloom").exists());
}

#[test]
fn public_parquet_nested_computation_and_reduction_narrow_bound_scan_columns() {
    let fixture = Fixture::new();
    let source = format!("(SELECT * FROM '{}') AS q", fixture.path().display());
    for (sql, expected, columns) in [
        (
            format!("SELECT COUNT(*) AS rows FROM {source}"),
            r#"{"rows":4}"#,
            1,
        ),
        (
            format!("SELECT SUM(value) AS total FROM {source}"),
            r#"{"total":406.0}"#,
            1,
        ),
        (
            format!("SELECT SUM(value + 1) AS total FROM {source}"),
            r#"{"total":410.0}"#,
            1,
        ),
        (
            format!("SELECT value + 7 AS result FROM {source} ORDER BY result LIMIT 1"),
            r#"{"result":107}"#,
            1,
        ),
        (
            format!("SELECT 7 AS result FROM {source} LIMIT 1"),
            r#"{"result":7}"#,
            1,
        ),
    ] {
        let mut request = Fixture::request(&fixture.path());
        request.sql_statement = Some(sql);
        let collected = Fixture::prepare(&request)
            .unwrap()
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        assert_eq!(collected.result_jsonl.value().trim(), expected);
        assert_eq!(
            collected
                .execution
                .input
                .as_ref()
                .unwrap()
                .columnar_projected_columns,
            columns
        );
        assert!(!Fixture::target(&fixture.path()).exists());
    }
}

#[test]
fn public_parquet_join_demand_keeps_hidden_keys_after_nested_star_binding() {
    let fixture = Fixture::new();
    let other = fixture.0.join("right.parquet");
    Fixture::write(&other);
    let mut request = Fixture::request(&fixture.path());
    request.sql_statement = Some(format!(
        "SELECT SUM(f.value) AS total FROM (SELECT * FROM '{}') AS f \
         INNER JOIN (SELECT * FROM '{}') AS d ON f.id=d.id",
        fixture.path().display(),
        other.display(),
    ));
    let collected = Fixture::prepare(&request)
        .unwrap()
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(collected.result_jsonl.value().trim(), r#"{"total":406.0}"#);
    let input = collected.execution.input.as_ref().unwrap();
    assert_eq!(input.columnar_sources, 2);
    assert_eq!(input.columnar_available_columns, 6);
    assert_eq!(input.columnar_projected_columns, 3);
    assert!(!fixture.0.join(".shardloom").exists());
}

#[test]
fn public_parquet_demand_keeps_unused_fallible_projection_errors() {
    let fixture = Fixture::new();
    for projection in ["id", "COUNT(*) AS rows"] {
        let mut request = Fixture::request(&fixture.path());
        request.sql_statement = Some(format!(
            "SELECT {projection} FROM (SELECT id, CAST(unused AS BIGINT) AS invalid FROM '{}') AS q",
            fixture.path().display(),
        ));
        let operation = Fixture::prepare(&request).unwrap();
        assert!(
            operation
                .collect_jsonl(&CancellationToken::default())
                .is_err()
        );
        assert!(!Fixture::target(&fixture.path()).exists());
    }
}

#[test]
fn public_parquet_explicit_preparation_and_existing_artifact_reuse_remain_available() {
    let fixture = Fixture::new();
    let mut request = Fixture::request(&fixture.path());
    request.execution_policy = "prepare_once".into();
    let prepared = Fixture::prepare(&request).unwrap();
    let first = prepared
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(first.result_jsonl.value().trim(), r#"{"value":102}"#);
    assert!(first.execution.input.is_none());
    let target = Fixture::target(&fixture.path());
    let before = fs::metadata(&target).unwrap();
    request.execution_policy = "vortex_middle".into();
    let reused = Fixture::prepare(&request).unwrap();
    let result = reused.collect_jsonl(&CancellationToken::default()).unwrap();
    assert_eq!(result.result_jsonl.value(), first.result_jsonl.value());
    assert!(result.execution.input.is_none());
    let after = fs::metadata(&target).unwrap();
    assert_eq!(
        (before.ino(), before.len(), before.mtime_nsec()),
        (after.ino(), after.len(), after.mtime_nsec())
    );
    // A stale cache fails admission; it must not restart full preparation.
    let replacement = fixture.0.join("replacement.parquet");
    Fixture::write(&replacement);
    fs::rename(replacement, fixture.path()).unwrap();
    assert!(Fixture::prepare(&request).is_err());
    assert_eq!(fs::metadata(&target).unwrap().ino(), before.ino());
}

#[test]
fn public_parquet_directory_and_content_digest_use_demand_without_persistence() {
    let fixture = Fixture::new();
    let directory = fixture.0.join("parts");
    fs::create_dir(&directory).unwrap();
    Fixture::write(&directory.join("a.parquet"));
    Fixture::write(&directory.join("b.parquet"));
    for (path, fingerprint, count) in [
        (&directory, None, 2),
        (&directory, Some("content_digest"), 2),
        (&fixture.path(), Some("content_digest"), 1),
        (&fixture.path(), Some(" Full-Content-Digest "), 1),
    ] {
        let mut request = Fixture::request(path);
        request.source_fingerprint_policy = fingerprint.map(str::to_owned);
        let operation = Fixture::prepare(&request).unwrap();
        let result = operation
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        assert_eq!(result.result_jsonl.value().lines().count(), count);
        let input = result.execution.input.as_ref().unwrap();
        assert_eq!(input.columnar_sources, 1);
        assert_eq!(input.columnar_files, count as u64);
        assert_eq!(
            input.columnar_content_fingerprints.len(),
            if fingerprint.is_some() { count } else { 0 }
        );
        assert_eq!(
            input.columnar_fingerprint_read_bytes,
            if fingerprint.is_some() {
                fs::metadata(fixture.path()).unwrap().len() * count as u64
            } else {
                0
            }
        );
        assert!(!Fixture::target(path).exists());
    }
}

#[test]
fn public_parquet_fingerprint_policy_rejects_unknown_and_reuses_explicit_content_preparation() {
    let fixture = Fixture::new();
    let mut request = Fixture::request(&fixture.path());
    request.source_fingerprint_policy = Some("unknown".into());
    assert!(
        Fixture::prepare(&request)
            .err()
            .unwrap()
            .to_string()
            .contains("fingerprint policy")
    );
    assert!(!Fixture::target(&fixture.path()).exists());
    request.source_fingerprint_policy = Some("content".into());
    request.execution_policy = "prepare_once".into();
    let operation = Fixture::prepare(&request).unwrap();
    let result = operation
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(result.result_jsonl.value().trim(), r#"{"value":102}"#);
    assert!(result.execution.input.is_none());
    let target = Fixture::target(&fixture.path());
    let before = fs::metadata(&target).unwrap();
    request.execution_policy = "vortex_middle".into();
    request.source_fingerprint_policy = Some("content_digest".into());
    let reused = Fixture::prepare(&request)
        .unwrap()
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(reused.result_jsonl.value(), result.result_jsonl.value());
    assert!(reused.execution.input.is_none());
    assert_eq!(fs::metadata(target).unwrap().ino(), before.ino());
}

#[test]
fn public_parquet_schema_override_is_rejected_instead_of_ignored_by_demand_reader() {
    let fixture = Fixture::new();
    let mut request = Fixture::request(&fixture.path());
    request.source_schema = Some("id:int64,value:int64,unused:utf8".into());
    let error = Fixture::prepare(&request)
        .err()
        .expect("columnar schema is authoritative");
    assert!(
        error.to_string().contains("embedded schema evidence"),
        "{error}"
    );
    let target = auto_prepared_vortex_target_path_with_schema(
        fixture.path().to_str().unwrap(),
        "parquet",
        request.source_schema.as_deref(),
    );
    assert!(!target.exists());
}
