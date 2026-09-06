use super::super::{
    DatasetUri, DiagnosticSeverity, ProjectionRequest, VortexLocalPrimitiveExecutionPolicy,
    VortexLocalPrimitiveExecutionStatus, VortexSortRowsRequest,
    execute_vortex_local_partitioned_primitive_with_policy,
    execute_vortex_local_primitive_with_policy, local_primitive_native_io_certificate,
    local_primitive_native_io_safe, local_vortex_runtime,
};
use super::query_run_store::{OWNERSHIP_MARKER, bounded_marker_bytes};
use super::*;
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::PathBuf,
    sync::atomic::AtomicU64,
};
use vortex::{file::WriteOptionsSessionExt as _, io::runtime::BlockingRuntime as _};
static NEXT_WORKSPACE: AtomicU64 = AtomicU64::new(0);
use vortex::{
    VortexSessionDefault as _, array::iter::ArrayIteratorAdapter,
    io::session::RuntimeSessionExt as _,
};

struct Workspace(PathBuf);
impl Workspace {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "shardloom-sort-spill-test-{}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            NEXT_WORKSPACE.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn policy(&self) -> VortexSortSpillPolicy {
        VortexSortSpillPolicy::new(&self.0, 32 * 1024 * 1024, 4 * 1024 * 1024).unwrap()
    }
    fn assert_empty(&self) {
        assert_eq!(fs::read_dir(&self.0).unwrap().count(), 0);
    }
}
impl Drop for Workspace {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn source_candidate(ordinal: usize, value: StatValue) -> SortRowCandidate {
    SortRowCandidate {
        ordinal,
        source_partition_index: 0,
        source_ordinal: ordinal,
        values: vec![value],
    }
}

#[test]
fn merge_geometry_covers_owned_overlap_and_retains_full_large_run_metadata_charge() {
    // Eight resident row queues; three scalar columns during one refill; five
    // row-width buffers for input, conversion and queued/active writer overlap.
    // Keep a further factor of two within the reserved per-row allowance.
    let owned_per_row = MERGE_FAN_IN * size_of::<SpillRow>()
        + 3 * size_of::<StatValue>()
        + 5 * size_of::<SpillRow>();
    assert!(2 * owned_per_row as u64 <= MERGE_BYTES_PER_BLOCK_ROW);
    for (memory, expected) in [
        (1_u64 << 20, (256, 2)),
        (2 << 20, (256, 8)),
        (4 << 20, (1024, 8)),
    ] {
        let (rows, fan_in) = merge_geometry(memory / 2).unwrap();
        assert_eq!((rows, fan_in), expected);
        assert!(
            MERGE_FIXED_BYTES
                + fan_in as u64 * MERGE_READER_BYTES
                + rows as u64 * MERGE_BYTES_PER_BLOCK_ROW
                <= memory / 2
        );
    }
    let input = NumericSortSpill::metadata_for_rows(65_536, 1024).unwrap();
    let output = NumericSortSpill::metadata_for_rows(131_072, 1024).unwrap();
    assert_eq!(input, 69_632);
    assert_eq!(output, 135_168);
    assert_eq!(input * 2 + output, 274_432);
    assert!(input * 2 + output <= (4 << 20) / 8);
    // The old leaf geometry really exceeds the same unchanged metadata budget.
    assert!(
        NumericSortSpill::metadata_for_rows(65_536, 256).unwrap() * 2
            + NumericSortSpill::metadata_for_rows(131_072, 256).unwrap()
            > (4 << 20) / 8
    );
}

#[test]
fn run_reader_refills_one_native_leaf_without_host_core_prefetch() {
    let workspace = Workspace::new();
    let runtime = local_vortex_runtime(VortexLocalPrimitiveExecutionPolicy::new(4).unwrap());
    let session = VortexSession::default().with_handle(runtime.handle());
    let mut spill = NumericSortSpill::new(
        &workspace.policy(),
        false,
        VortexSortTiePolicy::First,
        true,
        1,
    )
    .unwrap();
    let block_rows = spill.block_rows;
    let mut candidates = (0..block_rows * 3 + 17)
        .map(|row| source_candidate(row, StatValue::Int64(i64::try_from(row).unwrap())))
        .collect();
    spill.flush(&mut candidates, &runtime, &session).unwrap();
    let mut reader = RunReader::open(
        &spill.runs[0],
        &spill.store,
        Arc::clone(&spill.merge),
        &runtime,
        &session,
    )
    .unwrap();
    assert_eq!(reader.reader.next_block_offset(), 0);
    for index in 0..block_rows {
        assert_eq!(
            reader.next_row(&runtime).unwrap().unwrap().source,
            index as u64
        );
        assert_eq!(reader.reader.next_block_offset(), block_rows as u64);
        assert!(reader.rows.len() < block_rows);
    }
    assert_eq!(
        reader.next_row(&runtime).unwrap().unwrap().source,
        block_rows as u64
    );
    assert_eq!(reader.reader.next_block_offset(), 2 * block_rows as u64);
    drop(reader);
    drop(spill);
    workspace.assert_empty();
}

#[test]
fn minimum_budget_merges_multiple_runs_with_exact_high_offset_and_cleanup() {
    let workspace = Workspace::new();
    let mut policy = workspace.policy();
    policy.memory_bytes = 1 << 20;
    let runtime = local_vortex_runtime(VortexLocalPrimitiveExecutionPolicy::single_threaded());
    let session = VortexSession::default().with_handle(runtime.handle());
    let mut spill =
        NumericSortSpill::new(&policy, true, VortexSortTiePolicy::First, true, 7).unwrap();
    assert_eq!(spill.merge_fan_in, 2);
    let mut candidates = Vec::with_capacity(spill.capacity_rows());
    for row in 0..7168 {
        candidates.push(source_candidate(
            row,
            StatValue::Int64((1_i64 << 60) + i64::try_from(row).unwrap()),
        ));
        spill
            .flush_if_full(&mut candidates, &runtime, &session)
            .unwrap();
    }
    let (selected, report) = spill
        .finish(&mut candidates, 6987, 7, &runtime, &session)
        .unwrap();
    assert_eq!(
        selected
            .iter()
            .map(|row| row.source_ordinal)
            .collect::<Vec<_>>(),
        (6987..6994).map(|index| 7167 - index).collect::<Vec<_>>()
    );
    assert!(report.runs_written > 1 && report.merge_passes > 0);
    assert!(report.max_open_runs <= 3);
    assert!(report.peak_reserved_bytes <= policy.memory_bytes);
    assert!(report.owned_cleanup_completed);
    workspace.assert_empty();
}

#[test]
fn insufficient_run_metadata_fails_explicitly_and_releases_owned_files() {
    let workspace = Workspace::new();
    let mut policy = workspace.policy();
    policy.memory_bytes = 1 << 20;
    let runtime = local_vortex_runtime(VortexLocalPrimitiveExecutionPolicy::single_threaded());
    let session = VortexSession::default().with_handle(runtime.handle());
    let mut spill =
        NumericSortSpill::new(&policy, false, VortexSortTiePolicy::First, true, 7).unwrap();
    let memory = spill.memory.clone();
    let mut candidates = Vec::with_capacity(spill.capacity_rows());
    let failure = (0..131_072)
        .find_map(|row| {
            candidates.push(source_candidate(
                row,
                StatValue::Int64(i64::try_from(row).unwrap()),
            ));
            spill
                .flush_if_full(&mut candidates, &runtime, &session)
                .err()
        })
        .expect("a large run's simultaneous metadata must exceed the 1 MiB scope");
    assert!(failure.to_string().contains("memory"));
    assert!(
        failure
            .to_string()
            .contains("fallback execution was not attempted")
    );
    drop(spill);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    workspace.assert_empty();
}

#[test]
#[allow(clippy::too_many_lines)] // One end-to-end fixture keeps the exact-value and certificate checks together.
fn public_numeric_sort_spill_returns_complete_values_and_scoped_native_certificate() {
    use vortex::array::arrays::VarBinViewArray;
    const ROWS: usize = 50_000;
    const OFFSET: usize = 45_003;
    let fixture = Workspace::new();
    let workspace = Workspace::new();
    let path = fixture.0.join("shipping.vortex");
    let runtime = local_vortex_runtime(VortexLocalPrimitiveExecutionPolicy::single_threaded());
    let session = VortexSession::default().with_handle(runtime.handle());
    let keys = (0..ROWS)
        .map(|index| i64::try_from((index * 37) % 997).unwrap() - 498)
        .collect::<Vec<_>>();
    let labels = (0..ROWS)
        .map(|index| format!("港-{index}-shipment"))
        .collect::<Vec<_>>();
    let array = StructArray::new(
        ["priority", "shipment_sequence", "destination"].into(),
        vec![
            PrimitiveArray::from_iter(keys.iter().copied()).into_array(),
            PrimitiveArray::from_iter(0..u64::try_from(ROWS).unwrap()).into_array(),
            VarBinViewArray::from_iter_str(labels.iter().map(String::as_str)).into_array(),
        ],
        ROWS,
        Validity::NonNullable,
    )
    .into_array();
    let mut output = File::create(&path).unwrap();
    session
        .write_options()
        .blocking(&runtime)
        .write(
            &mut output,
            ArrayIteratorAdapter::new(array.dtype().clone(), [Ok(array)].into_iter()),
        )
        .unwrap();
    drop(output);
    for descending in [false, true] {
        for tie_policy in [VortexSortTiePolicy::First, VortexSortTiePolicy::Last] {
            let request = VortexQueryPrimitiveRequest::sort_rows(
                DatasetUri::new(path.display().to_string()).unwrap(),
                ProjectionRequest::All,
                None,
                VortexSortRowsRequest::new(vec![crate::VortexAggregateOrderExpr::new(
                    "priority", descending,
                )])
                .with_offset(OFFSET)
                .with_tie_policy(tie_policy)
                .with_spill(workspace.policy()),
                7,
            );
            let report = execute_vortex_local_primitive_with_policy(
                &request,
                VortexLocalPrimitiveExecutionPolicy::single_threaded(),
            )
            .unwrap();
            assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
            assert!(!report.fallback_execution_allowed);
            assert!(!report.arrow_converted);
            let evidence = report.state_budget.native_sort_spill.as_ref().unwrap();
            assert!(evidence.runs_written > 8);
            assert!(evidence.merge_passes > 0);
            let values: serde_json::Value = serde_json::from_str(
                report
                    .result_summary
                    .as_ref()
                    .unwrap()
                    .split_once(" values=")
                    .unwrap()
                    .1,
            )
            .unwrap();
            let mut expected = (0..ROWS).collect::<Vec<_>>();
            expected.sort_unstable_by(|left, right| {
                let order = if descending {
                    keys[*right].cmp(&keys[*left])
                } else {
                    keys[*left].cmp(&keys[*right])
                };
                order.then_with(|| {
                    if tie_policy == VortexSortTiePolicy::Last {
                        right.cmp(left)
                    } else {
                        left.cmp(right)
                    }
                })
            });
            let expected = expected[OFFSET..OFFSET + 7].iter().map(|index| serde_json::json!({
                "priority": keys[*index], "shipment_sequence": index, "destination": labels[*index],
            })).collect::<Vec<_>>();
            assert_eq!(values["values"], serde_json::json!(expected));
            assert_eq!(values["retained_candidate_rows"], 7);
            assert!(local_primitive_native_io_safe(&request, &report));
            let certificate = local_primitive_native_io_certificate(&request, &report).unwrap();
            assert!(!certificate.fallback_attempted);
            assert!(certificate.diagnostics.iter().all(|diagnostic| !matches!(
                diagnostic.severity,
                DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
            )));
            let mut counterfeit = report.clone();
            counterfeit
                .state_budget
                .native_sort_spill
                .as_mut()
                .unwrap()
                .owned_cleanup_completed = false;
            assert!(!local_primitive_native_io_safe(&request, &counterfeit));
            let mut unadmitted = request.clone();
            unadmitted.sort_rows.as_mut().unwrap().spill = None;
            assert!(!local_primitive_native_io_safe(&unadmitted, &report));
            workspace.assert_empty();
        }
    }
}

#[test]
fn public_sort_spill_rejects_source_replacement_between_key_and_payload_passes() {
    struct ResetHook;
    impl Drop for ResetHook {
        fn drop(&mut self) {
            BEFORE_MATERIALIZATION.with(|hook| hook.borrow_mut().take());
        }
    }

    for replace_inode in [false, true] {
        let fixture = Workspace::new();
        let workspace = Workspace::new();
        let path = fixture.0.join("source.vortex");
        let replacement = fixture.0.join("replacement.vortex");
        let runtime = local_vortex_runtime(VortexLocalPrimitiveExecutionPolicy::single_threaded());
        let session = VortexSession::default().with_handle(runtime.handle());
        for (target, keys) in [(&path, [3_i64, 1, 2]), (&replacement, [100, 200, 300])] {
            let array = StructArray::new(
                ["priority", "payload"].into(),
                [
                    PrimitiveArray::from_iter(keys).into_array(),
                    PrimitiveArray::from_iter(keys.map(|key| key * 10)).into_array(),
                ],
                3,
                Validity::NonNullable,
            )
            .into_array();
            session
                .write_options()
                .blocking(&runtime)
                .write(
                    File::create(target).unwrap(),
                    ArrayIteratorAdapter::new(array.dtype().clone(), [Ok(array)].into_iter()),
                )
                .unwrap();
        }
        let request = VortexQueryPrimitiveRequest::sort_rows(
            DatasetUri::new(path.display().to_string()).unwrap(),
            ProjectionRequest::All,
            None,
            VortexSortRowsRequest::new(vec![crate::VortexAggregateOrderExpr::new(
                "priority", false,
            )])
            .with_spill(workspace.policy()),
            2,
        );
        let target = path.clone();
        BEFORE_MATERIALIZATION.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move || {
                if replace_inode {
                    fs::rename(&replacement, &target).unwrap();
                } else {
                    fs::copy(&replacement, &target).unwrap();
                }
            }));
        });
        let _reset = ResetHook;
        let error = execute_vortex_local_primitive_with_policy(
            &request,
            VortexLocalPrimitiveExecutionPolicy::single_threaded(),
        )
        .unwrap_err();
        assert!(error.to_string().contains("source changed"));
        assert!(BEFORE_MATERIALIZATION.with(|hook| hook.borrow().is_none()));
        workspace.assert_empty();

        // A fresh operation may read the current generation after the failed
        // operation releases its stale identity and all owned spill files.
        let report = execute_vortex_local_primitive_with_policy(
            &request,
            VortexLocalPrimitiveExecutionPolicy::single_threaded(),
        )
        .unwrap();
        assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
        workspace.assert_empty();
    }
}

#[test]
fn admission_rejects_unsupported_shapes_before_workspace_creation() {
    use vortex::array::dtype::{DType, Nullability, PType};
    let workspace = Workspace::new();
    let request = VortexQueryPrimitiveRequest::sort_rows(
        DatasetUri::new("/unused.vortex").unwrap(),
        ProjectionRequest::All,
        None,
        VortexSortRowsRequest::new(vec![crate::VortexAggregateOrderExpr::new("value", false)])
            .with_spill(workspace.policy()),
        1,
    );
    for dtype in [
        DType::Primitive(PType::I64, Nullability::Nullable),
        DType::Primitive(PType::F64, Nullability::NonNullable),
    ] {
        assert!(admit(&request, &dtype, u64::MAX, 1).is_err());
    }
    let dtype = DType::Primitive(PType::I64, Nullability::NonNullable);
    let mut invalid = request.clone();
    invalid.sort_rows.as_mut().unwrap().tie_policy = VortexSortTiePolicy::All;
    assert!(admit(&invalid, &dtype, u64::MAX, 1).is_err());
    assert!(admit(&request, &dtype, 1, 1).is_err());
    let mut invalid = request.clone();
    invalid
        .sort_rows
        .as_mut()
        .unwrap()
        .order_by
        .push(crate::VortexAggregateOrderExpr::new("other", false));
    assert!(admit(&invalid, &dtype, u64::MAX, 1).is_err());
    assert!(
        execute_vortex_local_partitioned_primitive_with_policy(
            &request,
            &[],
            VortexLocalPrimitiveExecutionPolicy::single_threaded()
        )
        .is_err()
    );
    workspace.assert_empty();
}

#[test]
fn native_runs_merge_exact_signed_extremes_ties_and_large_offset_with_bounded_handles() {
    const ROWS: usize = 50_000;
    const OFFSET: usize = 45_003;
    const LIMIT: usize = 7;
    let runtime = local_vortex_runtime(VortexLocalPrimitiveExecutionPolicy::single_threaded());
    let session = VortexSession::default().with_handle(runtime.handle());
    for descending in [false, true] {
        for tie_policy in [VortexSortTiePolicy::First, VortexSortTiePolicy::Last] {
            let workspace = Workspace::new();
            let policy = workspace.policy();
            let mut spill =
                NumericSortSpill::new(&policy, descending, tie_policy, true, LIMIT).unwrap();
            let mut candidates = Vec::with_capacity(spill.capacity_rows());
            let mut expected = Vec::new();
            for ordinal in 0..ROWS {
                let value = match ordinal % 1009 {
                    0 => i64::MIN,
                    1 => i64::MAX,
                    _ => i64::try_from((ordinal * 37) % 997).unwrap() - 498,
                };
                expected.push((value, ordinal));
                candidates.push(source_candidate(ordinal, StatValue::Int64(value)));
                spill
                    .flush_if_full(&mut candidates, &runtime, &session)
                    .unwrap();
                assert!(
                    spill
                        .runs
                        .windows(2)
                        .all(|runs| runs[0].level > runs[1].level)
                );
                assert!(spill.runs.len() <= MAX_LIVE_RUNS);
            }
            expected.sort_unstable_by(|(left, left_ordinal), (right, right_ordinal)| {
                let key = if descending {
                    right.cmp(left)
                } else {
                    left.cmp(right)
                };
                key.then_with(|| {
                    if tie_policy == VortexSortTiePolicy::Last {
                        right_ordinal.cmp(left_ordinal)
                    } else {
                        left_ordinal.cmp(right_ordinal)
                    }
                })
            });
            let (selected, report) = spill
                .finish(&mut candidates, OFFSET, LIMIT, &runtime, &session)
                .unwrap();
            assert_eq!(
                selected
                    .iter()
                    .map(|row| row.source_ordinal)
                    .collect::<Vec<_>>(),
                expected[OFFSET..OFFSET + LIMIT]
                    .iter()
                    .map(|(_, ordinal)| *ordinal)
                    .collect::<Vec<_>>()
            );
            assert!(report.runs_written > MERGE_FAN_IN as u64);
            assert!(report.merge_passes >= 1);
            assert_eq!(report.runs_written, report.runs_validated);
            assert!(report.max_open_runs <= MERGE_FAN_IN + 1);
            assert!(report.peak_reserved_bytes <= policy.memory_bytes);
            assert!(report.peak_disk_bytes <= policy.quota_bytes);
            assert!(report.owned_cleanup_completed);
            workspace.assert_empty();
        }
    }
}

#[test]
fn native_sort_runs_preserve_unsigned_values_above_i64_and_empty_or_past_end_results() {
    let runtime = local_vortex_runtime(VortexLocalPrimitiveExecutionPolicy::single_threaded());
    let session = VortexSession::default().with_handle(runtime.handle());
    for offset in [0, 3, 10] {
        let workspace = Workspace::new();
        let spill = NumericSortSpill::new(
            &workspace.policy(),
            false,
            VortexSortTiePolicy::First,
            false,
            3,
        )
        .unwrap();
        let mut candidates = [u64::MAX, 0, (i64::MAX as u64) + 1]
            .into_iter()
            .enumerate()
            .map(|(ordinal, value)| source_candidate(ordinal, StatValue::UInt64(value)))
            .collect();
        let (selected, _) = spill
            .finish(&mut candidates, offset, 3, &runtime, &session)
            .unwrap();
        assert_eq!(
            selected
                .iter()
                .map(|row| row.source_ordinal)
                .collect::<Vec<_>>(),
            if offset == 0 {
                vec![1, 2, 0]
            } else {
                Vec::new()
            }
        );
        workspace.assert_empty();
    }
    let workspace = Workspace::new();
    let spill = NumericSortSpill::new(
        &workspace.policy(),
        false,
        VortexSortTiePolicy::First,
        true,
        1,
    )
    .unwrap();
    let (selected, report) = spill
        .finish(&mut Vec::new(), 0, 1, &runtime, &session)
        .unwrap();
    assert!(selected.is_empty());
    assert_eq!(report.runs_written, 0);
    workspace.assert_empty();
}

#[test]
fn quota_failure_and_cancellation_remove_every_owned_native_run() {
    let runtime = local_vortex_runtime(VortexLocalPrimitiveExecutionPolicy::single_threaded());
    let session = VortexSession::default().with_handle(runtime.handle());
    let workspace = Workspace::new();
    let mut policy = workspace.policy();
    policy.quota_bytes = MARKER_BYTE_RESERVATION + 32;
    let mut spill =
        NumericSortSpill::new(&policy, false, VortexSortTiePolicy::First, true, 1).unwrap();
    let mut candidates = vec![source_candidate(0, StatValue::Int64(2))];
    assert!(
        spill
            .flush(&mut candidates, &runtime, &session)
            .unwrap_err()
            .to_string()
            .contains("quota")
    );
    drop(spill);
    workspace.assert_empty();

    let policy = workspace.policy();
    let mut spill =
        NumericSortSpill::new(&policy, false, VortexSortTiePolicy::First, true, 1).unwrap();
    spill.flush(&mut candidates, &runtime, &session).unwrap();
    assert_eq!(spill.runs.len(), 1);
    policy.cancel();
    assert!(
        spill
            .finish(&mut Vec::new(), 0, 1, &runtime, &session)
            .err()
            .expect("cancelled spill")
            .to_string()
            .contains("cancelled")
    );
    workspace.assert_empty();
}

#[test]
fn truncated_and_changed_native_runs_fail_before_merge_and_cleanup_remains_owned() {
    use std::io::{Seek, SeekFrom};
    let runtime = local_vortex_runtime(VortexLocalPrimitiveExecutionPolicy::single_threaded());
    let session = VortexSession::default().with_handle(runtime.handle());
    for truncate in [false, true] {
        let workspace = Workspace::new();
        let mut spill = NumericSortSpill::new(
            &workspace.policy(),
            false,
            VortexSortTiePolicy::First,
            true,
            1,
        )
        .unwrap();
        let mut candidates = vec![source_candidate(0, StatValue::Int64(7))];
        spill.flush(&mut candidates, &runtime, &session).unwrap();
        let run = &spill.runs[0];
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&run.native.path)
            .unwrap();
        if truncate {
            file.set_len(run.native.bytes - 1).unwrap();
        } else {
            let mut byte = [0];
            file.read_exact(&mut byte).unwrap();
            file.seek(SeekFrom::Start(0)).unwrap();
            byte[0] ^= 1;
            file.write_all(&byte).unwrap();
        }
        drop(file);
        assert!(spill.open_merge(&runtime, &session).is_err());
        drop(spill);
        workspace.assert_empty();
    }
}

#[cfg(unix)]
#[test]
fn verified_run_generation_rejects_replacement_and_mutation_after_native_open() {
    let runtime = local_vortex_runtime(VortexLocalPrimitiveExecutionPolicy::single_threaded());
    let session = VortexSession::default().with_handle(runtime.handle());
    for replace in [false, true] {
        let workspace = Workspace::new();
        let mut spill = NumericSortSpill::new(
            &workspace.policy(),
            false,
            VortexSortTiePolicy::First,
            true,
            4,
        )
        .unwrap();
        spill
            .flush(
                &mut vec![
                    source_candidate(0, StatValue::Int64(7)),
                    source_candidate(1, StatValue::Int64(9)),
                ],
                &runtime,
                &session,
            )
            .unwrap();
        let run = &spill.runs[0];
        let mut reader = RunReader::open(
            run,
            &spill.store,
            Arc::clone(&spill.merge),
            &runtime,
            &session,
        )
        .unwrap();
        reader.refill(&runtime).unwrap();
        let path = run.native.path.clone();
        let saved = path.with_extension("saved");
        if replace {
            fs::rename(&path, &saved).unwrap();
            fs::copy(&saved, &path).unwrap();
        } else {
            // Same inode and unchanged length; changing mtime/ctime still
            // invalidates the generation captured before checksum verification.
            let bytes = fs::read(&path).unwrap();
            fs::write(&path, &bytes).unwrap();
        }
        assert!(
            reader
                .refill(&runtime)
                .unwrap_err()
                .to_string()
                .contains("prepared source")
        );
        let merge = RunMerge {
            readers: vec![reader],
            heads: BinaryHeap::new(),
            policy: workspace.policy(),
            failed: false,
            runtime: &runtime,
        };
        // Bounded top-K can stop with prefetched rows remaining. Its final
        // boundary must validate every run even without requesting EOF.
        assert!(
            merge
                .validate_sources()
                .unwrap_err()
                .to_string()
                .contains("prepared source")
        );
        drop(merge);
        if replace {
            assert!(spill.store.cleanup().is_err());
            assert!(path.exists());
            fs::remove_file(&path).unwrap();
            fs::rename(&saved, &path).unwrap();
        }
        drop(spill);
        workspace.assert_empty();
    }
}

#[test]
fn recovery_marker_read_is_bounded_even_if_the_stream_grows_after_admission() {
    struct GrowingReader {
        consumed: usize,
    }
    impl Read for GrowingReader {
        fn read(&mut self, output: &mut [u8]) -> std::io::Result<usize> {
            output.fill(b' ');
            self.consumed += output.len();
            Ok(output.len())
        }
    }
    let mut reader = GrowingReader { consumed: 0 };
    assert!(
        bounded_marker_bytes(&mut reader)
            .unwrap_err()
            .to_string()
            .contains("byte bound")
    );
    assert_eq!(reader.consumed as u64, MARKER_BYTE_RESERVATION / 2 + 1);
    let valid = b"{\"schema\":\"test\"}";
    assert_eq!(bounded_marker_bytes(valid.as_slice()).unwrap(), valid);
}

#[test]
fn spill_rejects_unsupported_keys_and_reservation_growth_without_leaking_files() {
    let runtime = local_vortex_runtime(VortexLocalPrimitiveExecutionPolicy::single_threaded());
    let session = VortexSession::default().with_handle(runtime.handle());
    let workspace = Workspace::new();
    let mut spill = NumericSortSpill::new(
        &workspace.policy(),
        false,
        VortexSortTiePolicy::First,
        true,
        1,
    )
    .unwrap();
    let mut candidates = vec![source_candidate(0, StatValue::Null)];
    assert!(spill.flush(&mut candidates, &runtime, &session).is_err());
    let state_before = spill.memory.snapshot().reserved_bytes;
    assert!(spill.memory.reserve(spill.policy.memory_bytes).is_err());
    assert_eq!(spill.memory.snapshot().reserved_bytes, state_before);
    drop(spill);
    workspace.assert_empty();
}

#[test]
fn crash_recovery_refuses_unknown_files_then_removes_only_recorded_inodes() {
    let runtime = local_vortex_runtime(VortexLocalPrimitiveExecutionPolicy::single_threaded());
    let session = VortexSession::default().with_handle(runtime.handle());
    let workspace = Workspace::new();
    let policy = workspace.policy();
    let mut spill =
        NumericSortSpill::new(&policy, false, VortexSortTiePolicy::First, true, 1).unwrap();
    spill
        .flush(
            &mut vec![source_candidate(0, StatValue::Int64(1))],
            &runtime,
            &session,
        )
        .unwrap();
    let directory = spill.store.directory().to_path_buf();
    let unknown = directory.join("user-note.txt");
    fs::write(&unknown, b"preserve me").unwrap();
    assert!(
        recover(&policy, &directory)
            .unwrap_err()
            .to_string()
            .contains("unknown file")
    );
    assert_eq!(fs::read(&unknown).unwrap(), b"preserve me");
    assert!(spill.runs[0].native.path.exists());
    fs::remove_file(unknown).unwrap();
    recover(&policy, &directory).unwrap();
    drop(spill);
    workspace.assert_empty();
}

#[test]
fn recovery_tolerates_interrupted_known_file_cleanup() {
    let workspace = Workspace::new();
    let policy = workspace.policy();
    let runtime = local_vortex_runtime(VortexLocalPrimitiveExecutionPolicy::single_threaded());
    let session = VortexSession::default().with_handle(runtime.handle());
    let mut spill =
        NumericSortSpill::new(&policy, false, VortexSortTiePolicy::First, true, 1).unwrap();
    spill
        .flush(
            &mut vec![source_candidate(0, StatValue::Int64(7))],
            &runtime,
            &session,
        )
        .unwrap();
    fs::remove_file(&spill.runs[0].native.path).unwrap();
    policy.cleanup_abandoned(spill.store.directory()).unwrap();
    workspace.assert_empty();
}

#[cfg(unix)]
#[test]
fn private_workspace_preserves_replaced_ownership_marker() {
    use std::os::unix::fs::PermissionsExt as _;
    let workspace = Workspace::new();
    let policy = workspace.policy();
    let mut spill =
        NumericSortSpill::new(&policy, false, VortexSortTiePolicy::First, true, 1).unwrap();
    assert_eq!(
        fs::metadata(spill.store.directory())
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o700
    );
    let marker = spill.store.directory().join(OWNERSHIP_MARKER);
    let saved = spill.store.directory().join("saved-owner.json");
    fs::rename(&marker, &saved).unwrap();
    fs::write(&marker, b"unrelated owner replacement").unwrap();
    assert!(spill.store.cleanup().is_err());
    assert!(spill.store.write_marker().is_err());
    assert_eq!(fs::read(&marker).unwrap(), b"unrelated owner replacement");
    fs::remove_file(&marker).unwrap();
    fs::rename(&saved, &marker).unwrap();
    drop(spill);
    workspace.assert_empty();
}

#[cfg(unix)]
#[test]
fn symlink_workspaces_and_replaced_run_inodes_are_rejected_without_deleting_targets() {
    use std::os::unix::fs::symlink;
    let runtime = local_vortex_runtime(VortexLocalPrimitiveExecutionPolicy::single_threaded());
    let session = VortexSession::default().with_handle(runtime.handle());
    let workspace = Workspace::new();
    let link = workspace.0.join("link");
    symlink(&workspace.0, &link).unwrap();
    let mut policy = workspace.policy();
    policy.workspace = link.clone();
    assert!(NumericSortSpill::new(&policy, false, VortexSortTiePolicy::First, true, 1).is_err());
    fs::remove_file(link).unwrap();

    let policy = workspace.policy();
    let mut spill =
        NumericSortSpill::new(&policy, false, VortexSortTiePolicy::First, true, 1).unwrap();
    spill
        .flush(
            &mut vec![source_candidate(0, StatValue::Int64(1))],
            &runtime,
            &session,
        )
        .unwrap();
    let path = spill.runs[0].native.path.clone();
    let original = spill.store.directory().join("original.vortex");
    fs::rename(&path, &original).unwrap();
    fs::write(&path, b"unowned replacement").unwrap();
    assert!(spill.store.cleanup().is_err());
    assert!(recover(&policy, spill.store.directory()).is_err());
    assert_eq!(fs::read(&path).unwrap(), b"unowned replacement");
    fs::remove_file(&path).unwrap();
    fs::rename(original, path).unwrap();
    drop(spill);
    workspace.assert_empty();
}
