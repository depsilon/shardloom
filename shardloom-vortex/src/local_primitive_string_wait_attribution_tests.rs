//! Scoped UTF8 worker attribution through the production prepared aggregate.

use super::{
    VortexLocalPrimitiveExecutionPolicy,
    aggregate_chunk_jobs::observation::{Record, Session},
    native_flat_layout::SequentialNativeFlatLayout,
    prepared_aggregate::prepare_aggregate_in_session,
};
use crate::{
    VortexAggregateOrderExpr, VortexQueryPrimitiveRequest, VortexSimpleAggregateMeasure,
    VortexSimpleAggregateRequest, resident_session::ResidentVortexSession,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use shardloom_core::{ColumnRef, DatasetUri};
use shardloom_exec::compute_pool::CancellationToken;
use std::{
    fmt::Write as _,
    io::Read as _,
    path::PathBuf,
    sync::atomic::{AtomicUsize, Ordering},
};
use vortex::{
    VortexSessionDefault as _,
    array::{
        ArrayRef, IntoArray as _,
        arrays::{StructArray, VarBinViewArray},
        dtype::FieldNames,
        validity::Validity,
    },
    file::WriteOptionsSessionExt as _,
    io::{
        runtime::{BlockingRuntime as _, single::SingleThreadRuntime},
        session::RuntimeSessionExt as _,
    },
    session::VortexSession,
};

const MEMORY_BYTES: u64 = 256 << 20;
static NEXT_FIXTURE: AtomicUsize = AtomicUsize::new(0);

#[path = "local_primitive_string_delivery_tests.rs"]
mod delivery_tests;

struct Fixture {
    directory: PathBuf,
    chunks: usize,
    chunk_rows: usize,
    cardinality: usize,
}

fn key(index: usize) -> String {
    format!("group-{index:08}-{}", "x".repeat(56))
}

impl Fixture {
    fn new(chunks: usize, chunk_rows: usize, cardinality: usize) -> Self {
        assert!(chunks >= 2 && chunk_rows > 0 && cardinality > 0);
        let directory = std::env::temp_dir().join(format!(
            "shardloom-string-wait-native-{}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            NEXT_FIXTURE.fetch_add(1, Ordering::Relaxed),
        ));
        std::fs::create_dir(&directory).unwrap();
        let fixture = Self {
            directory,
            chunks,
            chunk_rows,
            cardinality,
        };
        let runtime = SingleThreadRuntime::default();
        let session = VortexSession::default().with_handle(runtime.handle());
        let first = fixture.chunk(0);
        let mut output = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(fixture.path())
            .unwrap();
        let mut writer = session
            .write_options()
            .with_strategy(SequentialNativeFlatLayout::strategy(chunks))
            .with_file_statistics(Vec::new())
            .blocking(&runtime)
            .writer(&mut output, first.dtype().clone());
        writer.push(first).unwrap();
        for index in 1..chunks {
            writer.push(fixture.chunk(index)).unwrap();
        }
        assert_eq!(writer.finish().unwrap().row_count(), fixture.source_rows());
        drop(output);
        fixture
    }

    fn chunk(&self, index: usize) -> ArrayRef {
        let values = VarBinViewArray::from_iter_str(
            (0..self.chunk_rows).map(|row| key((index * self.chunk_rows + row) % self.cardinality)),
        );
        StructArray::try_new(
            FieldNames::from(["group_key"]),
            vec![values.into_array()],
            self.chunk_rows,
            Validity::NonNullable,
        )
        .unwrap()
        .into_array()
    }

    fn source_rows(&self) -> u64 {
        u64::try_from(self.chunks.checked_mul(self.chunk_rows).unwrap()).unwrap()
    }

    fn path(&self) -> PathBuf {
        self.directory.join("source.vortex")
    }

    fn source_hash(&self) -> String {
        let mut source = std::fs::File::open(self.path()).unwrap();
        let mut hasher = Sha256::new();
        let mut buffer = [0; 8 * 1024];
        loop {
            let bytes = source.read(&mut buffer).unwrap();
            if bytes == 0 {
                break;
            }
            hasher.update(&buffer[..bytes]);
        }
        hasher
            .finalize()
            .iter()
            .fold(String::with_capacity(64), |mut text, byte| {
                write!(text, "{byte:02x}").unwrap();
                text
            })
    }

    fn request(&self) -> VortexQueryPrimitiveRequest {
        VortexQueryPrimitiveRequest::simple_aggregate(
            DatasetUri::new(self.path().display().to_string()).unwrap(),
            VortexSimpleAggregateRequest::grouped(
                vec![ColumnRef::new("group_key").unwrap()],
                vec![VortexSimpleAggregateMeasure::new(
                    "count",
                    None,
                    "frequency".into(),
                )],
            )
            .with_order_by(vec![VortexAggregateOrderExpr::new("frequency", true)]),
        )
        .with_source_order_limit(10)
    }

    fn expected(&self) -> Value {
        // A complete arithmetic oracle, independent of the partition reducer.
        let rows = self.source_rows();
        let cardinality = u64::try_from(self.cardinality).unwrap();
        let mut groups = (0..self.cardinality)
            .map(|index| {
                let count = rows / cardinality + u64::from((index as u64) < rows % cardinality);
                (key(index), count)
            })
            .filter(|(_, count)| *count > 0)
            .collect::<Vec<_>>();
        groups.sort_by(|left, right| right.1.cmp(&left.1).then(left.0.cmp(&right.0)));
        groups.truncate(10);
        json!(
            groups
                .into_iter()
                .map(|(group_key, frequency)| {
                    json!({"group_key": group_key, "frequency": frequency})
                })
                .collect::<Vec<_>>()
        )
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.directory);
    }
}

fn run(fixture: &Fixture, expected: &Value, parallelism: usize, observe_waits: bool) -> Value {
    let session = ResidentVortexSession::for_external_cpu_pool(MEMORY_BYTES, parallelism).unwrap();
    let memory = session.memory().clone();
    let mut policy =
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(parallelism, 4).unwrap();
    policy.resource_envelope.memory_budget_bytes = MEMORY_BYTES;
    let request = fixture.request();
    let prepared = prepare_aggregate_in_session(&request, policy, &session).unwrap();
    let cancellation = CancellationToken::default();
    let observation = Session::start(observe_waits);
    let result = prepared.collect_jsonl(&cancellation).unwrap();
    assert!(!cancellation.is_cancelled());
    assert!(!result.execution.report.has_errors());
    assert!(!result.execution.report.fallback_execution_allowed);
    assert!(!result.execution.report.arrow_converted);
    assert!(!result.execution.report.spill_io_performed);
    assert!(result.execution.native_io_certificate.is_certified());
    assert!(!result.execution.native_io_certificate.fallback_attempted);
    assert_eq!(
        result.execution.report.rows_selected,
        Some(fixture.source_rows())
    );
    assert_eq!(result.execution.runtime.prepared_source_opens, 1);
    assert_eq!(result.execution.runtime.completed_executions, 1);
    let rows = result
        .result_jsonl
        .value()
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(&json!(rows), expected);
    let schema: Value = serde_json::from_str(result.result_schema_json.value()).unwrap();
    assert_eq!(
        schema,
        json!({"Struct": [{
        "names": ["group_key", "frequency"],
        "dtypes": [{"Utf8": false}, {"Primitive": ["u64", false]}],
    }, false]})
    );
    let summary: Value = serde_json::from_str(
        result
            .execution
            .report
            .result_summary
            .as_deref()
            .unwrap()
            .rsplit_once(" values=")
            .unwrap()
            .1,
    )
    .unwrap();
    let mut output = json!({
        "chunks": fixture.chunks,
        "chunk_rows": fixture.chunk_rows,
        "cardinality": fixture.cardinality,
        "source_rows": fixture.source_rows(),
        "parallelism": parallelism,
        "memory_grant_bytes": MEMORY_BYTES,
        "observe_waits": observe_waits,
        "request_debug": format!("{request:?}"),
        "engine_execution_seconds": result.timing.execution.as_secs_f64(),
        "engine_execution_nanos": u64::try_from(result.timing.execution.as_nanos()).unwrap(),
        "result_serialization_nanos": u64::try_from(result.timing.result_serialization.as_nanos()).unwrap(),
        "rows": rows, "schema": schema, "summary": summary,
        "native_io_certificate_debug": format!("{:?}", result.execution.native_io_certificate),
        "runtime_debug": format!("{:?}", result.execution.runtime),
        "fallback_attempted": result.execution.native_io_certificate.fallback_attempted,
    });
    drop((result, prepared, session));
    let owners = observation.finish();
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    assert_owner_route(fixture, parallelism, &summary, &owners);
    output["owners"] = json!(
        owners
            .iter()
            .map(|record| record.completed_json(parallelism, observe_waits))
            .collect::<Vec<_>>()
    );
    output["final_reserved_bytes"] = json!(memory.snapshot().reserved_bytes);
    output
}

fn assert_owner_route(fixture: &Fixture, parallelism: usize, summary: &Value, owners: &[Record]) {
    if fixture.chunks == 32 && fixture.chunk_rows == 16_384 {
        assert_eq!(summary["aggregate_workers_partition_count"], 64);
        assert_eq!(
            summary["aggregate_workers_partition_complete_groups"],
            fixture.cardinality
        );
        assert_eq!(
            summary["aggregate_workers_partition_committed_rows"],
            fixture.source_rows()
        );
        assert_eq!(summary["aggregate_workers_partition_native_handoffs"], 0);
        assert_eq!(summary["aggregate_workers_partition_retry_jobs"], 0);
        assert!(owners.iter().any(|record| record.submitted > 0));
        if parallelism > 1 {
            assert!(
                owners
                    .iter()
                    .any(|record| record.pool.is_some_and(|pool| pool.completed_jobs > 0))
            );
        }
    }
    if fixture.chunks == 64 && fixture.chunk_rows == 16_384 && fixture.cardinality == 2 {
        assert!(owners.is_empty());
    }
}

#[test]
fn native_string_wait_capture_checks_complete_counts_and_refunds() {
    for cardinality in [2, 127] {
        let fixture = Fixture::new(2, 128, cardinality);
        let expected = fixture.expected();
        let hash = fixture.source_hash();
        for parallelism in [1, 3] {
            for observe_waits in [false, true] {
                assert!(run(&fixture, &expected, parallelism, observe_waits)["owners"].is_array());
            }
        }
        assert_eq!(fixture.source_hash(), hash);
    }
}

#[test]
fn native_string_wait_capture_checks_real_partitions_before_release_attribution() {
    for cardinality in [2, 98_317] {
        let fixture = Fixture::new(32, 16_384, cardinality);
        let expected = fixture.expected();
        let hash = fixture.source_hash();
        let output = run(&fixture, &expected, 3, true);
        assert_eq!(output["owners"].as_array().unwrap().len(), 1);
        assert_eq!(fixture.source_hash(), hash);
    }
}

#[test]
fn native_string_wait_capture_preserves_heavy_hitter_worker_decline() {
    let fixture = Fixture::new(64, 16_384, 2);
    let expected = fixture.expected();
    let output = run(&fixture, &expected, 3, true);
    assert_eq!(output["owners"], json!([]));
}

#[test]
#[ignore = "isolated release-only string worker attribution; no performance assertion"]
fn native_string_worker_wait_attribution() {
    assert!(
        !std::hint::black_box(cfg!(debug_assertions)),
        "use the release test binary"
    );
    let fixtures = [
        (2, 128, 2),
        (4, 8_192, 2),
        (32, 16_384, 2),
        (32, 16_384, 98_317),
        (64, 16_384, 2),
    ]
    .into_iter()
    .map(|(chunks, rows, cardinality)| Fixture::new(chunks, rows, cardinality))
    .collect::<Vec<_>>();
    let hashes = fixtures
        .iter()
        .map(Fixture::source_hash)
        .collect::<Vec<_>>();
    let expected = fixtures.iter().map(Fixture::expected).collect::<Vec<_>>();
    let mut case = 0;
    for ((fixture, hash), expected) in fixtures.iter().zip(&hashes).zip(&expected) {
        for parallelism in [1, 4, 10] {
            let order = if case % 2 == 0 {
                [false, true]
            } else {
                [true, false]
            };
            for observe_waits in order {
                let mut output = run(fixture, expected, parallelism, observe_waits);
                output["case"] = json!(case);
                output["source_sha256"] = json!(hash);
                println!("STRING_WAIT_OBSERVATION {output}");
            }
            case += 1;
        }
    }
    assert_eq!(case, 15);
    for (fixture, hash) in fixtures.iter().zip(&hashes) {
        assert_eq!(&fixture.source_hash(), hash);
    }
    println!("STRING_WAIT_SOURCES_UNCHANGED true");
}
