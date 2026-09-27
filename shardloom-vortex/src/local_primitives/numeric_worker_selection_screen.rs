use crate::{
    VortexAggregateOrderExpr, VortexQueryPrimitiveRequest, VortexSimpleAggregateMeasure,
    VortexSimpleAggregateRequest,
};
use shardloom_core::{ColumnRef, DatasetUri};
use std::path::{Path, PathBuf};
use vortex::{
    VortexSessionDefault as _,
    array::{
        IntoArray as _,
        arrays::{PrimitiveArray, StructArray},
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

const CHUNK_ROWS: usize = 65_536;

struct Fixture(PathBuf);

impl Fixture {
    fn new(root: &Path, values: &[u64]) -> Self {
        static NEXT_FIXTURE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        assert!(
            !values.is_empty(),
            "initial C2.b fixture requires input rows"
        );
        let sequence = NEXT_FIXTURE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let stamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock is before UNIX epoch")
            .as_nanos();
        let path = root.join(format!(
            "shardloom-c2b-u64-{}-{stamp}-{sequence}.vortex",
            std::process::id(),
        ));

        // Acquire ownership before installing Drop cleanup; create_new failure
        // must never remove a path owned by another caller.
        let mut output = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .expect("create unique native Vortex fixture");
        let fixture = Self(path);

        let chunks = values.chunks(CHUNK_ROWS).collect::<Vec<_>>();
        let arrays = chunks
            .iter()
            .map(|chunk| {
                StructArray::try_new(
                    FieldNames::from(["alias_key"]),
                    vec![
                        chunk
                            .iter()
                            .copied()
                            .collect::<PrimitiveArray>()
                            .into_array(),
                    ],
                    chunk.len(),
                    Validity::NonNullable,
                )
                .expect("construct UInt64 native source chunk")
                .into_array()
            })
            .collect::<Vec<_>>();
        let runtime = SingleThreadRuntime::default();
        let session = VortexSession::default().with_handle(runtime.handle());
        let mut writer = session
            .write_options()
            .with_strategy(
                super::native_flat_layout::SequentialNativeFlatLayout::strategy(arrays.len()),
            )
            .with_file_statistics(Vec::new())
            .blocking(&runtime)
            .writer(&mut output, arrays[0].dtype().clone());
        for array in arrays {
            writer.push(array).expect("write native Vortex chunk");
        }
        assert_eq!(
            writer
                .finish()
                .expect("finish native Vortex fixture")
                .row_count(),
            u64::try_from(values.len()).expect("row count fits u64"),
        );
        fixture
    }

    fn query(&self) -> VortexQueryPrimitiveRequest {
        VortexQueryPrimitiveRequest::simple_aggregate(
            DatasetUri::new(self.0.display().to_string()).expect("fixture URI"),
            VortexSimpleAggregateRequest::grouped(
                vec![ColumnRef::new("alias_key").expect("group column")],
                vec![VortexSimpleAggregateMeasure::new(
                    "count",
                    None,
                    "n_rows".to_string(),
                )],
            )
            .with_order_by(vec![VortexAggregateOrderExpr::new("n_rows", true)]),
        )
        .with_source_order_limit(10)
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

use super::{
    VortexLocalPrimitiveExecutionPolicy, VortexLocalPrimitiveExecutionStatus,
    aggregate_count_workers::ADMISSION_TEST_WORKERS, execute_vortex_local_primitive_with_policy,
};

#[derive(Clone, Copy, Debug)]
enum Distribution {
    Skewed,
    Uniform,
    NearUnique,
}

impl Distribution {
    fn values(self, rows: usize) -> Vec<u64> {
        (0..u64::try_from(rows).unwrap())
            .map(|index| match self {
                Self::Skewed => {
                    if index % 10 == 0 {
                        1 + (index / 10) % 15
                    } else {
                        0
                    }
                }
                Self::Uniform => index % 1024,
                Self::NearUnique => index.saturating_sub(u64::from(index % 16 == 0)),
            })
            .collect()
    }
}

fn expected_top10(values: &[u64]) -> serde_json::Value {
    let mut counts = std::collections::BTreeMap::<u64, u64>::new();
    for value in values {
        *counts.entry(*value).or_default() += 1;
    }
    let mut counts: Vec<_> = counts.into_iter().collect();
    counts.sort_unstable_by(|(left_key, left_count), (right_key, right_count)| {
        right_count
            .cmp(left_count)
            .then_with(|| left_key.cmp(right_key))
    });
    serde_json::Value::Array(
        counts
            .into_iter()
            .take(10)
            .map(|(key, count)| serde_json::json!({"alias_key":key,"n_rows":count}))
            .collect(),
    )
}

fn complete_call(
    request: &VortexQueryPrimitiveRequest,
    direct: bool,
    rows: usize,
    expected: &serde_json::Value,
) -> (f64, serde_json::Value) {
    let policy = VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(12, 1).unwrap();
    ADMISSION_TEST_WORKERS.with(|flag| {
        assert!(
            flag.replace(Some(!direct)).is_none(),
            "no override may leak from a previous call"
        );
    });
    let started = std::time::Instant::now();
    let mut report = execute_vortex_local_primitive_with_policy(request, policy).unwrap();
    let status = report.status;
    let external = report.external_effects_executed;
    let fallback_allowed = report.fallback_execution_allowed;
    let scanned = report.rows_scanned;
    let arrays = report.arrays_read_count;
    let native_io_certified = super::local_primitive_native_io_certificate(request, &report)
        .unwrap()
        .is_certified();
    let summary = report.result_summary.take().unwrap();
    drop(report);
    let elapsed = started.elapsed().as_secs_f64();
    assert!(ADMISSION_TEST_WORKERS.with(std::cell::Cell::get).is_none());
    assert_eq!(status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert!(!external && !fallback_allowed);
    assert!(native_io_certified);
    assert_eq!(scanned, u64::try_from(rows).unwrap());
    assert!(arrays > 0);
    let (_, text) = summary.rsplit_once(" values=").unwrap();
    let payload: serde_json::Value = serde_json::from_str(text).unwrap();
    assert_eq!(&payload["values"], expected);
    if direct {
        assert!(payload.get("aggregate_workers_submitted_chunks").is_none());
        assert!(
            payload["aggregate_provider_background_workers"]
                .as_u64()
                .unwrap()
                > 0
        );
        assert!(
            payload["aggregate_provider_cpu_scope"]
                .as_str()
                .unwrap()
                .contains("actual_aggregate_worker_admission_declined_before_scan")
        );
    } else {
        assert_eq!(
            payload["aggregate_workers_rows"],
            u64::try_from(rows).unwrap()
        );
        assert!(
            payload["aggregate_workers_submitted_chunks"]
                .as_u64()
                .unwrap()
                > 0
        );
        assert_eq!(
            payload["aggregate_workers_submitted_chunks"],
            payload["aggregate_workers_completed_chunks"]
        );
        assert_eq!(payload["aggregate_workers_outstanding_chunks"], 0);
    }
    (
        elapsed,
        serde_json::json!({
            "summary":summary,"values":payload["values"],"rows_scanned":scanned,
            "arrays_read_count":arrays,"external_effects_executed":external,
            "native_io_certified":native_io_certified,
            "fallback_execution_allowed":fallback_allowed,
            "route":if direct {"direct_native"} else {"count_workers"},
        }),
    )
}

#[test]
fn single_numeric_workers_and_direct_path_agree_through_complete_public_calls() {
    for distribution in [
        Distribution::Skewed,
        Distribution::Uniform,
        Distribution::NearUnique,
    ] {
        let values = distribution.values(512);
        let fixture = Fixture::new(&std::env::temp_dir(), &values);
        let request = fixture.query();
        let expected = expected_top10(&values);
        for direct in [false, true, false] {
            let _ = complete_call(&request, direct, values.len(), &expected);
        }
    }
}

#[test]
#[ignore = "bounded public-call selector screen; guarded local root, release and serial execution required"]
#[allow(clippy::assertions_on_constants)]
fn paired_small_numeric_count_selection_screen() {
    use sha2::{Digest as _, Sha256};
    use std::fmt::Write as _;
    assert!(!cfg!(debug_assertions));
    let root = PathBuf::from(
        std::env::var_os("SHARDLOOM_COUNT_SCREEN_ROOT")
            .expect("guarded runner must supply the fixture directory"),
    );
    assert!(root.is_dir());
    for rows in [128, 1024, 8192, 65_536, 262_144, 1_048_576] {
        for (case, distribution) in [
            Distribution::Skewed,
            Distribution::Uniform,
            Distribution::NearUnique,
        ]
        .into_iter()
        .enumerate()
        {
            let values = distribution.values(rows);
            let fixture = Fixture::new(&root, &values);
            let request = fixture.query();
            let source_bytes = std::fs::read(&fixture.0).unwrap();
            let mut source_sha256 = String::with_capacity(64);
            for byte in Sha256::digest(&source_bytes) {
                write!(&mut source_sha256, "{byte:02x}").unwrap();
            }
            let source_len = source_bytes.len();
            drop(source_bytes);
            let expected = expected_top10(&values);
            for run in 0..5 {
                let order = if (run + case) % 2 == 0 {
                    [false, true]
                } else {
                    [true, false]
                };
                for (position, direct) in order.into_iter().enumerate() {
                    let (seconds, evidence) = complete_call(&request, direct, rows, &expected);
                    println!(
                        "C2B_RECORD {}",
                        serde_json::json!({
                            "rows":rows,"distribution":format!("{distribution:?}"),
                            "run":run + 1,"position":position + 1,"seconds":seconds,
                            "source_sha256":source_sha256,"source_bytes":source_len,
                            "memory_gb":1,"requested_parallelism":12,
                            "evidence":evidence,
                        })
                    );
                }
            }
        }
    }
}
