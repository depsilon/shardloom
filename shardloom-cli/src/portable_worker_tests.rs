use super::*;
use crate::native::portable_worker_store::{
    ArtifactKind, DurablePreparedSourceManifest, ReclaimOutcome,
};
use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_ipc::writer::FileWriter as ArrowIpcWriter;
use arrow_schema::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter as ParquetArrowWriter;
use std::{
    fs,
    path::Path,
    sync::atomic::{AtomicUsize, Ordering},
};

struct TempWorkspace {
    dir: PathBuf,
}

impl TempWorkspace {
    fn new(tag: &str) -> Self {
        static SEQ: AtomicUsize = AtomicUsize::new(0);
        let id = SEQ.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "shardloom-portable-worker-test-{}-{tag}-{id}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        Self { dir }
    }

    fn path(&self, rel: &str) -> PathBuf {
        self.dir.join(rel)
    }
}

impl Drop for TempWorkspace {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.dir);
    }
}

fn sample_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("grp", DataType::Utf8, false),
        Field::new("val", DataType::Int64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1_i64, 2, 3, 4])),
            Arc::new(StringArray::from(vec!["alpha", "beta", "alpha", "beta"])),
            Arc::new(Int64Array::from(vec![10_i64, 20, 30, 40])),
        ],
    )
    .unwrap()
}

fn write_parquet_fixture(path: &Path) {
    let batch = sample_batch();
    let file = fs::File::create(path).unwrap();
    let mut writer = ParquetArrowWriter::try_new(file, batch.schema(), None).unwrap();
    writer.write(&batch).unwrap();
    let _ = writer.close().unwrap();
}

fn write_arrow_ipc_fixture(path: &Path) {
    let batch = sample_batch();
    let file = fs::File::create(path).unwrap();
    let mut writer = ArrowIpcWriter::try_new(file, &batch.schema()).unwrap();
    writer.write(&batch).unwrap();
    writer.finish().unwrap();
}

fn write_vortex_fixture(path: &Path, parquet_src: &Path) {
    let resources =
        ExecutionResources::from_bytes(64 << 20, 1, ExecutionResourceOrigin::ExecutionCall)
            .unwrap();
    let session = ResidentVortexSession::new(resources.memory_bytes(), 1).unwrap();
    let src_str = parquet_src.to_str().unwrap();
    let sql = format!("SELECT * FROM '{}'", src_str.replace('\'', "''"));
    let bindings = [SqlSourceBinding {
        reference: src_str.to_string(),
        source: SqlSource::File {
            format: "parquet".to_string(),
            schema: None,
        },
    }];
    let op = crate::native::prepare_sql_operation(
        &sql,
        resources,
        &session,
        &bindings,
        None,
        &CancellationToken::default(),
    )
    .unwrap();
    op.write_fields(
        path,
        VortexLocalPrimitiveRowExportFormat::Vortex,
        true,
        &CancellationToken::default(),
        None,
        false,
    )
    .unwrap();
}

fn write_json_and_csv_fixtures(jsonl_path: &Path, csv_path: &Path) {
    let jsonl = concat!(
        "{\"id\":1,\"grp\":\"alpha\",\"val\":10}\n",
        "{\"id\":2,\"grp\":\"beta\",\"val\":20}\n",
        "{\"id\":3,\"grp\":\"alpha\",\"val\":30}\n",
        "{\"id\":4,\"grp\":\"beta\",\"val\":40}\n"
    );
    fs::write(jsonl_path, jsonl).unwrap();
    let csv = concat!(
        "id,grp,val\n",
        "1,alpha,10\n",
        "2,beta,20\n",
        "3,alpha,30\n",
        "4,beta,40\n"
    );
    fs::write(csv_path, csv).unwrap();
}

fn default_job(
    request_id: &str,
    attempt_id: &str,
    durable_root: &Path,
    sql: &str,
    inputs: Vec<WorkerInputSpec>,
    routing_policy: WorkerRoutingPolicy,
) -> PortableWorkerJob {
    PortableWorkerJob {
        schema_version: PORTABLE_WORKER_JOB_SCHEMA_VERSION.to_string(),
        request_id: request_id.to_string(),
        attempt_id: attempt_id.to_string(),
        authorization_scope_id: "tenant-alpha.analytics".to_string(),
        sql: sql.to_string(),
        inputs,
        resources: WorkerResourceLimits {
            memory_bytes: 64 << 20,
            max_parallelism: 1,
            float_policy: FloatPolicy::Strict,
            recursion_limit: RecursionLimit::default(),
            deadline_ms: Some(15_000),
            spill_buffer_bytes: Some(8 << 20),
            deployment_limits: Some(ExecutionResourceLimits::new(Some(64 << 20), Some(1)).unwrap()),
        },
        output: WorkerOutputSpec {
            format: "jsonl".to_string(),
            inline_max_bytes: 64 << 10,
            durable_root: durable_root.to_path_buf(),
        },
        routing_policy,
        allow_cross_request_result_reuse: false,
    }
}

fn parse_jsonl_rows(bytes: &[u8]) -> Vec<serde_json::Value> {
    std::str::from_utf8(bytes)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
        .collect()
}

#[test]
fn portable_worker_parity_across_vortex_parquet_arrow_ipc_json_and_csv() {
    let ws = TempWorkspace::new("parity");
    let parquet_path = ws.path("input.parquet");
    let ipc_path = ws.path("input.arrow");
    let vortex_path = ws.path("input.vortex");
    let jsonl_path = ws.path("input.jsonl");
    let csv_path = ws.path("input.csv");
    write_parquet_fixture(&parquet_path);
    write_arrow_ipc_fixture(&ipc_path);
    write_vortex_fixture(&vortex_path, &parquet_path);
    write_json_and_csv_fixtures(&jsonl_path, &csv_path);

    let text_schema = Some("id:int64,grp:utf8,val:int64".to_string());
    let cases = [
        ("vortex", vortex_path, None),
        ("parquet", parquet_path, None),
        ("arrow-ipc", ipc_path, None),
        ("jsonl", jsonl_path, text_schema.clone()),
        ("csv", csv_path, text_schema),
    ];
    let expected = vec![
        serde_json::json!({"grp": "alpha", "total": 30.0}),
        serde_json::json!({"grp": "beta", "total": 60.0}),
    ];
    for (idx, (format, path, schema)) in cases.into_iter().enumerate() {
        let root = ws.path(&format!("store-{idx}"));
        let req = format!("req-parity-{idx}");
        let job = default_job(
            &req,
            "attempt-1",
            &root,
            "SELECT grp, SUM(val) AS total FROM 'orders' WHERE id >= 2 GROUP BY grp ORDER BY grp",
            vec![WorkerInputSpec {
                reference: "orders".to_string(),
                path,
                format: format.to_string(),
                schema,
                expected_sha256: None,
                expected_size_bytes: None,
            }],
            WorkerRoutingPolicy::PrepareAndReuse,
        );
        let report = PortableWorker::execute(&job, &CancellationToken::default()).unwrap();
        assert!(!report.fallback_attempted);
        assert!(!report.external_engine_invoked);
        assert_eq!(report.claim_gate_status, "not_claim_grade");
        assert_eq!(report.session_final_reserved_bytes, 0);
        let rows = parse_jsonl_rows(report.inline_payload_bytes.as_deref().unwrap());
        assert_eq!(rows, expected, "parity mismatch on input format {format}");
        assert!(!root.join("attempts").join(&req).join("attempt-1").exists());
    }
}

#[test]
fn portable_worker_fresh_worker_reuses_durable_vortex_after_producer_disappears() {
    let ws = TempWorkspace::new("reuse");
    let parquet_path = ws.path("orders.parquet");
    write_parquet_fixture(&parquet_path);
    let (sha, size) = sha256_file(&parquet_path).unwrap();
    let store_root = ws.path("durable");
    let spec = || WorkerInputSpec {
        reference: "orders".to_string(),
        path: parquet_path.clone(),
        format: "parquet".to_string(),
        schema: None,
        expected_sha256: Some(sha.clone()),
        expected_size_bytes: Some(size),
    };
    let sql = "SELECT grp, SUM(val) AS total FROM orders GROUP BY grp ORDER BY grp";
    let first = default_job(
        "req-1",
        "attempt-1",
        &store_root,
        sql,
        vec![spec()],
        WorkerRoutingPolicy::PrepareAndReuse,
    );
    let r1 = PortableWorker::execute(&first, &CancellationToken::default()).unwrap();
    assert_eq!(r1.route_selected, "prepared_and_executed_vortex");
    assert_eq!(r1.prepared_artifact_ids.len(), 1);
    assert_eq!(r1.session_final_reserved_bytes, 0);

    // Second completely fresh worker requires the persisted durable artifact.
    let second = default_job(
        "req-2",
        "attempt-1",
        &store_root,
        sql,
        vec![spec()],
        WorkerRoutingPolicy::RequirePrepared,
    );
    let r2 = PortableWorker::execute(&second, &CancellationToken::default()).unwrap();
    assert_eq!(r2.route_selected, "reused_durable_prepared_vortex");
    assert_eq!(r2.prepared_artifact_ids, r1.prepared_artifact_ids);
    assert_eq!(r2.inline_payload_bytes, r1.inline_payload_bytes);
    assert_eq!(r2.session_final_reserved_bytes, 0);
}

#[test]
fn portable_worker_rejects_stale_corrupted_missing_incompatible_unauthorized_and_secrets() {
    let ws = TempWorkspace::new("adversarial");
    let parquet_path = ws.path("orders.parquet");
    write_parquet_fixture(&parquet_path);
    let (sha, size) = sha256_file(&parquet_path).unwrap();
    let store_root = ws.path("durable");
    let sql = "SELECT COUNT(*) AS cnt FROM 'orders'";

    // 1. Missing required prepared artifact fails deterministically.
    let missing_job = default_job(
        "req-miss",
        "att-1",
        &store_root,
        sql,
        vec![WorkerInputSpec {
            reference: "orders".to_string(),
            path: parquet_path.clone(),
            format: "parquet".to_string(),
            schema: None,
            expected_sha256: Some(sha.clone()),
            expected_size_bytes: Some(size),
        }],
        WorkerRoutingPolicy::RequirePrepared,
    );
    assert!(
        PortableWorker::execute(&missing_job, &CancellationToken::default())
            .err()
            .unwrap()
            .to_string()
            .contains("missing")
    );

    // 2. Seed durable prepared artifact once.
    let prep_job = default_job(
        "req-seed",
        "att-1",
        &store_root,
        sql,
        missing_job.inputs.clone(),
        WorkerRoutingPolicy::PrepareAndReuse,
    );
    let seed = PortableWorker::execute(&prep_job, &CancellationToken::default()).unwrap();
    let artifact_id = seed.prepared_artifact_ids[0].clone();

    // 3. Unauthorized caller scope cannot open existing artifact_id.
    let wrong_scope_identity = DurablePreparedSourceIdentity::current(
        sha.clone(),
        size,
        "parquet".to_string(),
        FloatPolicy::Strict,
        RecursionLimit::default(),
        "tenant-other".to_string(),
    )
    .unwrap();
    let store = DurableArtifactStore::new(store_root.clone());
    assert!(
        store
            .open_validated_prepared_by_id(&artifact_id, &wrong_scope_identity)
            .is_err()
    );

    // 4. Stale expected sha256 on input fails before binding.
    let mut stale_job = missing_job.clone();
    stale_job.request_id = "req-stale".to_string();
    stale_job.routing_policy = WorkerRoutingPolicy::DirectOnly;
    stale_job.inputs[0].expected_sha256 = Some("0".repeat(64));
    assert!(
        PortableWorker::execute(&stale_job, &CancellationToken::default())
            .err()
            .unwrap()
            .to_string()
            .contains("sha256 changed")
    );

    // 5. Cross-request result caching enabled fails deterministically.
    let mut cached_job = prep_job.clone();
    cached_job.allow_cross_request_result_reuse = true;
    assert!(
        PortableWorker::execute(&cached_job, &CancellationToken::default())
            .err()
            .unwrap()
            .to_string()
            .contains("cross-request")
    );

    // 6. Secret-like strings are rejected cleanly.
    let mut secret_job = prep_job;
    secret_job.request_id = "req-secret".to_string();
    secret_job.sql = "SELECT * FROM 'orders' WHERE note = 'password=hunter2'".to_string();
    assert!(PortableWorker::execute(&secret_job, &CancellationToken::default()).is_err());
}

#[test]
fn portable_worker_rejects_corrupted_or_incompatible_prepared_artifacts() {
    let ws = TempWorkspace::new("corrupt");
    let parquet_path = ws.path("orders.parquet");
    write_parquet_fixture(&parquet_path);
    let (sha, size) = sha256_file(&parquet_path).unwrap();
    let store_root = ws.path("durable");
    let sql = "SELECT COUNT(*) AS cnt FROM 'orders'";
    let inputs = vec![WorkerInputSpec {
        reference: "orders".to_string(),
        path: parquet_path,
        format: "parquet".to_string(),
        schema: None,
        expected_sha256: Some(sha.clone()),
        expected_size_bytes: Some(size),
    }];
    let prep_job = default_job(
        "req-seed",
        "att-1",
        &store_root,
        sql,
        inputs.clone(),
        WorkerRoutingPolicy::PrepareAndReuse,
    );
    let seed = PortableWorker::execute(&prep_job, &CancellationToken::default()).unwrap();
    let artifact_id = seed.prepared_artifact_ids[0].clone();

    // Corrupted prepared data.vortex fails closed.
    let data_vortex = store_root
        .join("prepared")
        .join(&artifact_id)
        .join("data.vortex");
    fs::write(&data_vortex, b"corrupted-vortex-bytes").unwrap();
    let corrupt_job = default_job(
        "req-corrupt",
        "att-1",
        &store_root,
        sql,
        inputs,
        WorkerRoutingPolicy::RequirePrepared,
    );
    assert!(
        PortableWorker::execute(&corrupt_job, &CancellationToken::default())
            .err()
            .unwrap()
            .to_string()
            .contains("corrupted")
    );

    // Incompatible runtime_version in manifest fails closed.
    let store = DurableArtifactStore::new(store_root.clone());
    let manifest_path = store_root
        .join("prepared")
        .join(&artifact_id)
        .join("manifest.json");
    let mut manifest: DurablePreparedSourceManifest =
        serde_json::from_slice(&fs::read(&manifest_path).unwrap()).unwrap();
    manifest.identity.runtime_version = "0.0.0-incompatible".to_string();
    fs::write(&manifest_path, serde_json::to_vec(&manifest).unwrap()).unwrap();
    assert!(
        store
            .open_validated_prepared_by_id(
                &artifact_id,
                &DurablePreparedSourceIdentity::current(
                    sha,
                    size,
                    "parquet".to_string(),
                    FloatPolicy::Strict,
                    RecursionLimit::default(),
                    "tenant-alpha.analytics".to_string(),
                )
                .unwrap()
            )
            .is_err()
    );
}

#[test]
fn portable_worker_atomic_commit_fencing_preserves_first_winner_across_competing_attempts() {
    let ws = TempWorkspace::new("fencing");
    let parquet_path = ws.path("orders.parquet");
    write_parquet_fixture(&parquet_path);
    let store_root = ws.path("durable");

    let reports: Vec<PortableWorkerReport> = std::thread::scope(|scope| {
        let mut handles = Vec::new();
        for idx in 0..4_usize {
            let root_ref = &store_root;
            let path_ref = &parquet_path;
            handles.push(scope.spawn(move || {
                let job = default_job(
                    "req-race",
                    &format!("attempt-{idx}"),
                    root_ref,
                    "SELECT COUNT(*) AS cnt, SUM(val) AS total FROM 'orders'",
                    vec![WorkerInputSpec {
                        reference: "orders".to_string(),
                        path: path_ref.clone(),
                        format: "parquet".to_string(),
                        schema: None,
                        expected_sha256: None,
                        expected_size_bytes: None,
                    }],
                    WorkerRoutingPolicy::PrepareAndReuse,
                );
                PortableWorker::execute(&job, &CancellationToken::default()).unwrap()
            }));
        }
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });

    let mut committed_count = 0_usize;
    let mut fenced_count = 0_usize;
    let mut winners = std::collections::BTreeSet::new();
    for report in &reports {
        assert_eq!(report.session_final_reserved_bytes, 0);
        match &report.commit_outcome {
            CommitOutcome::Committed { manifest } => {
                committed_count += 1;
                winners.insert(manifest.winning_attempt_id.clone());
            }
            CommitOutcome::FencedAlreadyCommitted {
                winning_attempt_id,
                manifest,
            } => {
                fenced_count += 1;
                assert_eq!(winning_attempt_id, &manifest.winning_attempt_id);
                winners.insert(winning_attempt_id.clone());
            }
        }
    }
    assert_eq!(committed_count, 1);
    assert_eq!(fenced_count, 3);
    assert_eq!(winners.len(), 1);

    let winner = winners.into_iter().next().unwrap();
    let artifacts_dir = store_root
        .join("committed")
        .join("req-race")
        .join("artifacts");
    let remaining: Vec<_> = fs::read_dir(&artifacts_dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    assert_eq!(remaining, vec![format!("{winner}.jsonl")]);
    assert!(
        fs::read_dir(store_root.join("attempts").join("req-race"))
            .unwrap()
            .next()
            .is_none()
    );
}

#[test]
fn portable_worker_failures_clean_attempts_and_preserve_committed_manifest() {
    let ws = TempWorkspace::new("failures");
    let parquet_path = ws.path("orders.parquet");
    write_parquet_fixture(&parquet_path);
    let store_root = ws.path("durable");
    let input = WorkerInputSpec {
        reference: "orders".to_string(),
        path: parquet_path,
        format: "parquet".to_string(),
        schema: None,
        expected_sha256: None,
        expected_size_bytes: None,
    };

    // Commit valid winner first.
    let winner = default_job(
        "req-safe",
        "attempt-win",
        &store_root,
        "SELECT COUNT(*) AS cnt FROM 'orders'",
        vec![input.clone()],
        WorkerRoutingPolicy::DirectOnly,
    );
    let win_report = PortableWorker::execute(&winner, &CancellationToken::default()).unwrap();
    assert!(matches!(
        win_report.commit_outcome,
        CommitOutcome::Committed { .. }
    ));

    // 1. Pre-cancelled retry does not clobber committed manifest and cleans attempt dir.
    let cancelled = CancellationToken::default();
    cancelled.cancel();
    let cancel_retry = default_job(
        "req-safe",
        "attempt-cancel",
        &store_root,
        "SELECT COUNT(*) AS cnt FROM 'orders'",
        vec![input.clone()],
        WorkerRoutingPolicy::DirectOnly,
    );
    assert!(PortableWorker::execute(&cancel_retry, &cancelled).is_err());
    assert!(!store_root.join("attempts/req-safe/attempt-cancel").exists());

    // 2. Tiny memory grant fails deterministically and cleans attempt dir.
    let mut tiny = default_job(
        "req-safe",
        "attempt-tiny",
        &store_root,
        "SELECT COUNT(*) AS cnt FROM 'orders'",
        vec![input.clone()],
        WorkerRoutingPolicy::DirectOnly,
    );
    tiny.resources.memory_bytes = 512;
    assert!(PortableWorker::execute(&tiny, &CancellationToken::default()).is_err());
    assert!(!store_root.join("attempts/req-safe/attempt-tiny").exists());

    // 3. Expired deadline cleans attempt dir.
    let mut expired = default_job(
        "req-safe",
        "attempt-deadline",
        &store_root,
        "SELECT COUNT(*) AS cnt FROM 'orders'",
        vec![input.clone()],
        WorkerRoutingPolicy::DirectOnly,
    );
    expired.resources.deadline_ms = Some(0);
    assert!(PortableWorker::execute(&expired, &CancellationToken::default()).is_err());
    assert!(
        !store_root
            .join("attempts/req-safe/attempt-deadline")
            .exists()
    );

    // 4. Unsupported SQL cleans attempt dir.
    let bad_sql = default_job(
        "req-safe",
        "attempt-bad-sql",
        &store_root,
        "SELECT * FROM 'nonexistent_table_xyz'",
        vec![input],
        WorkerRoutingPolicy::DirectOnly,
    );
    assert!(PortableWorker::execute(&bad_sql, &CancellationToken::default()).is_err());
    assert!(
        !store_root
            .join("attempts/req-safe/attempt-bad-sql")
            .exists()
    );

    let store = DurableArtifactStore::new(store_root);
    let intact = store.read_committed_manifest("req-safe").unwrap();
    assert_eq!(intact.winning_attempt_id, "attempt-win");
}

#[test]
fn durable_store_retained_owner_lease_blocks_reclamation_until_dropped() {
    let ws = TempWorkspace::new("leases");
    let parquet_path = ws.path("orders.parquet");
    write_parquet_fixture(&parquet_path);
    let store_root = ws.path("durable");
    let job = default_job(
        "req-lease",
        "attempt-1",
        &store_root,
        "SELECT COUNT(*) AS cnt FROM 'orders'",
        vec![WorkerInputSpec {
            reference: "orders".to_string(),
            path: parquet_path,
            format: "parquet".to_string(),
            schema: None,
            expected_sha256: None,
            expected_size_bytes: None,
        }],
        WorkerRoutingPolicy::PrepareAndReuse,
    );
    let report = PortableWorker::execute(&job, &CancellationToken::default()).unwrap();
    let prep_id = report.prepared_artifact_ids[0].clone();
    let store = DurableArtifactStore::new(store_root);

    let prep_lease = store
        .acquire_lease(ArtifactKind::Prepared, &prep_id, "consumer-1")
        .unwrap();
    let res_lease = store
        .acquire_lease(ArtifactKind::Committed, "req-lease", "reader-1")
        .unwrap();
    assert_eq!(
        store.reclaim_prepared(&prep_id).unwrap(),
        ReclaimOutcome::ProtectedByRetainedOwners { owner_count: 1 }
    );
    assert_eq!(
        store.reclaim_committed("req-lease").unwrap(),
        ReclaimOutcome::ProtectedByRetainedOwners { owner_count: 1 }
    );
    assert!(store.read_committed_manifest("req-lease").is_ok());

    drop(prep_lease);
    drop(res_lease);
    assert_eq!(
        store.reclaim_prepared(&prep_id).unwrap(),
        ReclaimOutcome::Reclaimed
    );
    assert_eq!(
        store.reclaim_committed("req-lease").unwrap(),
        ReclaimOutcome::Reclaimed
    );
}

#[test]
fn portable_worker_respects_inline_max_bytes_cutoff_and_stage_timings() {
    let ws = TempWorkspace::new("inline-cutoff");
    let parquet_path = ws.path("orders.parquet");
    write_parquet_fixture(&parquet_path);
    let store_root = ws.path("durable");
    let mut job = default_job(
        "req-handle-only",
        "attempt-1",
        &store_root,
        "SELECT id, grp, val FROM 'orders' ORDER BY id",
        vec![WorkerInputSpec {
            reference: "orders".to_string(),
            path: parquet_path,
            format: "parquet".to_string(),
            schema: None,
            expected_sha256: None,
            expected_size_bytes: None,
        }],
        WorkerRoutingPolicy::DirectOnly,
    );
    job.output.inline_max_bytes = 8;
    let report = PortableWorker::execute(&job, &CancellationToken::default()).unwrap();
    assert!(report.inline_payload_bytes.is_none());
    let CommitOutcome::Committed { manifest } = report.commit_outcome else {
        panic!("expected new committed outcome");
    };
    assert!(manifest.output_bytes > 8);
    let sum_stages = report
        .stage_durations
        .worker_startup_nanos
        .saturating_add(report.stage_durations.input_validation_nanos)
        .saturating_add(report.stage_durations.preparation_or_artifact_open_nanos)
        .saturating_add(report.stage_durations.binding_planning_nanos)
        .saturating_add(report.stage_durations.engine_execution_nanos)
        .saturating_add(report.stage_durations.output_publication_nanos);
    assert!(report.stage_durations.complete_request_nanos >= sum_stages);
    let on_disk = fs::read(store_root.join(manifest.output_rel_path)).unwrap();
    assert_eq!(parse_jsonl_rows(&on_disk).len(), 4);
}
