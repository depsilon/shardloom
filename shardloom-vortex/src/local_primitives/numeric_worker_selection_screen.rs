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
fn direct_max_rows() -> usize {
    usize::try_from(super::aggregate_count_workers::SMALL_NUMERIC_DIRECT_MAX_ROWS).unwrap()
}

struct Fixture(PathBuf);

impl Fixture {
    fn new(root: &Path, values: &[u64]) -> Self {
        let arrays = values
            .chunks(CHUNK_ROWS)
            .map(|chunk| {
                chunk
                    .iter()
                    .copied()
                    .collect::<PrimitiveArray>()
                    .into_array()
            })
            .collect();
        Self::from_arrays(root, arrays)
    }

    fn from_arrays(root: &Path, columns: Vec<vortex::array::ArrayRef>) -> Self {
        static NEXT_FIXTURE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        assert!(!columns.is_empty());
        let rows: usize = columns.iter().map(vortex::array::ArrayRef::len).sum();
        let sequence = NEXT_FIXTURE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let stamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock is before UNIX epoch")
            .as_nanos();
        let path = root.join(format!(
            "shardloom-c2b-native-{}-{stamp}-{sequence}.vortex",
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

        let arrays = columns
            .into_iter()
            .map(|column| {
                let len = column.len();
                StructArray::try_new(
                    FieldNames::from(["alias_key"]),
                    vec![column],
                    len,
                    Validity::NonNullable,
                )
                .expect("construct native source chunk")
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
            u64::try_from(rows).expect("row count fits u64"),
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
    expected_counts(values, 10)
}

fn expected_counts<T: Ord + Copy + Into<serde_json::Value>>(
    values: &[T],
    limit: usize,
) -> serde_json::Value {
    let mut counts = std::collections::BTreeMap::<T, u64>::new();
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
            .take(limit)
            .map(|(key, count)| serde_json::json!({"alias_key":key.into(),"n_rows":count}))
            .collect(),
    )
}

fn encoded_cases(rows: usize) -> Vec<(&'static str, vortex::array::ArrayRef, serde_json::Value)> {
    use vortex::array::{
        VortexSessionExecute as _,
        arrays::{ConstantArray, DictArray},
    };
    use vortex::encodings::fastlanes::BitPackedData;
    let unsigned: Vec<u64> = (0..rows).map(|i| (i % 29) as u64).collect();
    let mut ctx = VortexSession::default().create_execution_ctx();
    let packed = BitPackedData::encode(
        &unsigned
            .iter()
            .copied()
            .collect::<PrimitiveArray>()
            .into_array(),
        5,
        &mut ctx,
    )
    .unwrap()
    .into_array();
    let dictionary = [0_u64, 17, u64::MAX, 8, 1];
    let decoded: Vec<u64> = (0..rows)
        .map(|i| dictionary[i % dictionary.len()])
        .collect();
    let dict = DictArray::try_new(
        (0..rows)
            .map(|i| u8::try_from(i % dictionary.len()).unwrap())
            .collect::<PrimitiveArray>()
            .into_array(),
        dictionary
            .into_iter()
            .collect::<PrimitiveArray>()
            .into_array(),
    )
    .unwrap()
    .into_array();
    let signed64: Vec<i64> = (0..rows)
        .map(|i| [i64::MIN, -1, 0, i64::MAX][i % 4])
        .collect();
    let signed32: Vec<i32> = (0..rows)
        .map(|i| [i32::MIN, -1, 0, i32::MAX][i % 4])
        .collect();
    vec![
        ("packed_u64", packed, expected_top10(&unsigned)),
        ("dictionary_u64", dict, expected_top10(&decoded)),
        (
            "constant_u64",
            ConstantArray::new(17_u64, rows).into_array(),
            serde_json::json!([{"alias_key":17,"n_rows":rows}]),
        ),
        (
            "signed_i64",
            signed64
                .iter()
                .copied()
                .collect::<PrimitiveArray>()
                .into_array(),
            expected_counts(&signed64, 10),
        ),
        (
            "signed_i32",
            signed32
                .iter()
                .copied()
                .collect::<PrimitiveArray>()
                .into_array(),
            expected_counts(&signed32, 10),
        ),
    ]
}

fn selection(evidence: &serde_json::Value) -> serde_json::Value {
    let text = evidence["summary"]
        .as_str()
        .unwrap()
        .rsplit_once(" values=")
        .unwrap()
        .1;
    serde_json::from_str::<serde_json::Value>(text).unwrap()["aggregate_worker_selection"].clone()
}

#[test]
fn small_count_automatic_selection_preserves_boundaries_encodings_and_nulls() {
    let root = std::env::temp_dir();
    for rows in [1, 257, 4093, 8192, 8193, 32_768, 32_769] {
        let values = Distribution::NearUnique.values(rows);
        let fixture = Fixture::new(&root, &values);
        for cap in [1, 7, 128] {
            let mut query = fixture.query();
            query.source_order_limit = Some(cap);
            let expected = expected_counts(&values, cap);
            let (_, evidence) = complete_call_with_policy(
                &query,
                None,
                rows <= direct_max_rows(),
                rows,
                &expected,
                VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(12, 1).unwrap(),
            );
            assert_eq!(selection(&evidence).is_string(), rows <= direct_max_rows());
        }
    }
    for (name, array, expected) in encoded_cases(4093) {
        let fixture = Fixture::from_arrays(&root, vec![array]);
        let (_, evidence) = complete_call_with_policy(
            &fixture.query(),
            None,
            true,
            4093,
            &expected,
            VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(12, 1).unwrap(),
        );
        assert_eq!(selection(&evidence), "small_numeric_count_direct", "{name}");
    }
    let nullable: Vec<Option<u64>> = (0..513)
        .map(|i| if i % 3 == 0 { None } else { Some(i % 5) })
        .collect();
    let fixture = Fixture::from_arrays(
        &root,
        vec![PrimitiveArray::from_option_iter(nullable.iter().copied()).into_array()],
    );
    let (_, evidence) = complete_call_with_policy(
        &fixture.query(),
        None,
        true,
        513,
        &expected_counts(&nullable, 10),
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(12, 1).unwrap(),
    );
    assert!(
        selection(&evidence).is_null(),
        "nullable route is unchanged"
    );
}

#[test]
fn small_count_selection_preserves_low_memory_and_single_lane_admission() {
    let values = Distribution::NearUnique.values(4093);
    let fixture = Fixture::new(&std::env::temp_dir(), &values);
    let expected = expected_top10(&values);
    for (parallelism, bytes, selected) in [
        (12, 32 << 20, true),
        (12, 4 << 20, false),
        (1, 32 << 20, false),
    ] {
        let mut policy =
            VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(parallelism, 1).unwrap();
        policy.resource_envelope.memory_budget_bytes = bytes;
        policy.resource_envelope.group_state_soft_item_budget =
            usize::try_from(bytes / 128).unwrap();
        let (_, evidence) = complete_call_with_policy(
            &fixture.query(),
            None,
            selected,
            values.len(),
            &expected,
            policy,
        );
        assert_eq!(selection(&evidence).is_string(), selected);
    }
}

#[test]
fn selected_small_count_preserves_prepared_cancellation_generation_and_cleanup() {
    use crate::resident_session::ResidentVortexSession;
    use shardloom_exec::compute_pool::CancellationToken;
    use std::io::{Read as _, Seek as _, Write as _};
    let values = Distribution::NearUnique.values(4093);
    for replace in [false, true] {
        let fixture = Fixture::new(&std::env::temp_dir(), &values);
        let resident = ResidentVortexSession::for_external_cpu_pool(32 << 20, 4).unwrap();
        let prepared = resident.prepare_file(&fixture.0).unwrap();
        let query = fixture.query();
        let cancelled = CancellationToken::default();
        cancelled.cancel();
        let error = prepared
            .with_native_execution_controlled(&cancelled, |_, _| -> shardloom_core::Result<()> {
                panic!("cancelled call must not execute");
            })
            .unwrap_err();
        assert!(error.to_string().contains("cancel"));
        let policy = VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(4, 1).unwrap();
        let late_cancel = CancellationToken::default();
        let error = prepared
            .with_native_execution_controlled(&late_cancel, |file, context| {
                let result = super::read_prepared_vortex_simple_aggregate_scan(
                    &DatasetUri::new(fixture.0.display().to_string()).unwrap(),
                    &query,
                    policy,
                    file,
                    context.native_session(),
                    context.runtime(),
                    Some(resident.memory()),
                    None,
                )?;
                late_cancel.cancel();
                Ok(result)
            })
            .err()
            .unwrap();
        assert!(error.to_string().contains("cancel"));
        let outcome = prepared.with_native_execution(|file, session, runtime| {
            let result = super::read_prepared_vortex_simple_aggregate_scan(
                &DatasetUri::new(fixture.0.display().to_string()).unwrap(),
                &query,
                policy,
                file,
                session,
                runtime,
                Some(resident.memory()),
                None,
            )?;
            let payload: serde_json::Value = serde_json::from_str(&result.result_summary).unwrap();
            assert_eq!(payload["values"], expected_top10(&values));
            assert_eq!(
                payload["aggregate_worker_selection"],
                "small_numeric_count_direct"
            );
            if replace {
                let replacement = Fixture::new(&std::env::temp_dir(), &values);
                std::fs::rename(&replacement.0, &fixture.0).unwrap();
            } else {
                let mut source = std::fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(&fixture.0)
                    .unwrap();
                let mut byte = [0];
                source.read_exact(&mut byte).unwrap();
                source.rewind().unwrap();
                byte[0] ^= 1;
                source.write_all(&byte).unwrap();
            }
            Ok(result)
        });
        assert!(
            outcome
                .err()
                .unwrap()
                .to_string()
                .contains("prepared source changed")
        );
        drop(prepared);
        assert_eq!(resident.snapshot().memory.reserved_bytes, 0);
    }
}

fn complete_call(
    request: &VortexQueryPrimitiveRequest,
    direct: bool,
    rows: usize,
    expected: &serde_json::Value,
) -> (f64, serde_json::Value) {
    let policy = VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(12, 1).unwrap();
    complete_call_with_policy(request, Some(direct), direct, rows, expected, policy)
}

fn complete_call_with_policy(
    request: &VortexQueryPrimitiveRequest,
    choice: Option<bool>,
    direct: bool,
    rows: usize,
    expected: &serde_json::Value,
    policy: VortexLocalPrimitiveExecutionPolicy,
) -> (f64, serde_json::Value) {
    ADMISSION_TEST_WORKERS.with(|flag| {
        assert!(
            flag.replace(choice.map(|direct| !direct)).is_none(),
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

#[test]
#[ignore = "held-out release screen; guarded local root and serial execution required"]
#[allow(clippy::assertions_on_constants)]
fn held_out_small_numeric_count_selection_screen() {
    assert!(!cfg!(debug_assertions));
    let root = PathBuf::from(std::env::var_os("SHARDLOOM_COUNT_SCREEN_ROOT").unwrap());
    assert!(root.is_dir());
    for rows in [1, 257, 4093, 8191, 8192, 8193, 32_768] {
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
            screen_three_choices(
                &fixture,
                rows,
                &format!("{distribution:?}"),
                case,
                &expected_top10(&values),
            );
        }
    }
    for (case, (name, array, expected)) in encoded_cases(4093).into_iter().enumerate() {
        let fixture = Fixture::from_arrays(&root, vec![array]);
        screen_three_choices(&fixture, 4093, name, case, &expected);
    }
}

fn screen_three_choices(
    fixture: &Fixture,
    rows: usize,
    name: &str,
    case: usize,
    expected: &serde_json::Value,
) {
    use sha2::{Digest as _, Sha256};
    use std::fmt::Write as _;
    let request = fixture.query();
    let bytes = std::fs::read(&fixture.0).unwrap();
    let mut source_sha256 = String::with_capacity(64);
    for byte in Sha256::digest(&bytes) {
        write!(&mut source_sha256, "{byte:02x}").unwrap();
    }
    let source_len = bytes.len();
    drop(bytes);
    for run in 0..5 {
        // Rotate all three choices; automatic selection is timed as a public call.
        let mut choices = [Some(false), Some(true), None];
        choices.rotate_left((case + run) % 3);
        for (position, choice) in choices.into_iter().enumerate() {
            let direct = choice.unwrap_or(rows <= direct_max_rows());
            let (seconds, evidence) = complete_call_with_policy(
                &request,
                choice,
                direct,
                rows,
                expected,
                VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(12, 1).unwrap(),
            );
            if choice.is_none() {
                assert_eq!(selection(&evidence).is_string(), rows <= direct_max_rows());
            }
            println!(
                "C2B_RECORD {}",
                serde_json::json!({
                    "rows":rows,"distribution":name,"run":run+1,"position":position+1,"seconds":seconds,
                    "choice":match choice {Some(false)=>"workers",Some(true)=>"direct",None=>"automatic"},
                    "source_sha256":source_sha256,"source_bytes":source_len,
                    "memory_gb":1,"requested_parallelism":12,"evidence":evidence,
                    "selection_max_rows":direct_max_rows(),
                })
            );
        }
    }
}

#[test]
#[ignore = "extended-boundary release screen; guarded local root and serial execution required"]
#[allow(clippy::assertions_on_constants)]
fn extended_small_numeric_count_selection_screen() {
    assert!(!cfg!(debug_assertions));
    let root = PathBuf::from(std::env::var_os("SHARDLOOM_COUNT_SCREEN_ROOT").unwrap());
    assert!(root.is_dir());
    for rows in [16_381, 24_593, 32_767, 32_768, 32_769, 49_152, 65_536] {
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
            screen_three_choices(
                &fixture,
                rows,
                &format!("{distribution:?}"),
                case,
                &expected_top10(&values),
            );
        }
    }
    for (case, (name, array, expected)) in encoded_cases(16_381).into_iter().enumerate() {
        let fixture = Fixture::from_arrays(&root, vec![array]);
        screen_three_choices(&fixture, 16_381, name, case, &expected);
    }
}
