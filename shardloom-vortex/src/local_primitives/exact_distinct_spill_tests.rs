use super::super::super::{
    VortexLocalPrimitiveExecutionPolicy, aggregate_chunk_jobs::ChunkWorkerContext,
    local_vortex_runtime,
};
use super::*;
use shardloom_exec::compute_pool::CancellationToken;
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    io::{Read as _, Seek as _, Write as _},
    sync::atomic::AtomicU64,
};
use vortex::{
    VortexSessionDefault as _,
    array::{
        ArrayRef, IntoArray as _, VortexSessionExecute as _,
        arrays::{DictArray, PrimitiveArray},
        validity::Validity,
    },
    io::{runtime::BlockingRuntime as _, session::RuntimeSessionExt as _},
};

static NEXT: AtomicU64 = AtomicU64::new(0);
struct Workspace(PathBuf);
impl Workspace {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "shardloom-distinct-spill-{}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn policy(&self) -> Policy {
        Policy {
            workspace: self.0.clone(),
            quota_bytes: 32 << 20,
            memory_bytes: 2 << 20,
            cancellation: Arc::new(AtomicBool::new(false)),
        }
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

fn runtime() -> (LocalVortexRuntime, VortexSession) {
    let runtime = local_vortex_runtime(VortexLocalPrimitiveExecutionPolicy::single_threaded());
    let session = VortexSession::default().with_handle(runtime.handle());
    (runtime, session)
}
fn unsigned(group: u64, value: u64) -> Pair {
    Pair::new(
        AggregateIntegerKeyPart {
            bits: group,
            signed: false,
        },
        AggregateIntegerKeyPart {
            bits: value,
            signed: false,
        },
    )
}
fn mixed(group: i64, value: u64) -> Pair {
    Pair::new(
        AggregateIntegerKeyPart {
            bits: u64::from_ne_bytes(group.to_ne_bytes()),
            signed: true,
        },
        AggregateIntegerKeyPart {
            bits: value,
            signed: false,
        },
    )
}
fn logical(key: AggregateIntegerKeyPart) -> i128 {
    if key.signed {
        i128::from(i64::from_ne_bytes(key.bits.to_ne_bytes()))
    } else {
        i128::from(key.bits)
    }
}
fn result_rows(result: &SpilledDistinctResult, offset: usize) -> Vec<(i128, u64)> {
    let mut rows = Vec::new();
    result
        .visit(offset, |key, count| {
            rows.push((logical(key), count));
            Ok(())
        })
        .unwrap();
    rows
}

#[test]
fn exact_distinct_spill_many_native_runs_complete_pairs_global_order_and_refunds() {
    let workspace = Workspace::new();
    let policy = workspace.policy();
    let memory = LiveMemoryPool::new(policy.memory_bytes).unwrap();
    let (runtime, session) = runtime();
    let mut spill = ExactDistinctSpill::new(policy, memory.clone(), true, false, 130).unwrap();
    let mut expected = BTreeMap::<i64, BTreeSet<u64>>::new();
    let mut weight = 0_u64;
    for index in 0..131_072_u64 {
        let value = (1_u64 << 61) + index % 65_536;
        let group = i64::try_from((index % 65_536) % 257).unwrap() - 128;
        let count = index % 3 + 1;
        expected.entry(group).or_default().insert(value);
        spill
            .push(mixed(group, value), count, &runtime, &session)
            .unwrap();
        weight += count;
        if (index + 1).is_multiple_of(8192) {
            spill.flush(&runtime, &session).unwrap();
        }
    }
    for value in 0..1000_u64 {
        expected
            .entry(i64::MIN)
            .or_default()
            .insert(u64::MAX - value);
        spill
            .push(mixed(i64::MIN, u64::MAX - value), 1, &runtime, &session)
            .unwrap();
        weight += 1;
    }
    let mut ordered = expected
        .iter()
        .map(|(&group, values)| (i128::from(group), u64::try_from(values.len()).unwrap()))
        .collect::<Vec<_>>();
    ordered.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0)));
    let result = spill.finish(&runtime, &session).unwrap();
    assert_eq!(result_rows(&result, 0)[0], (i128::from(i64::MIN), 1000));
    assert_eq!(
        result_rows(&result, 123),
        ordered.into_iter().skip(123).take(7).collect::<Vec<_>>()
    );
    assert_eq!(result.evidence.rows, weight);
    assert_eq!(result.evidence.complete_pairs, 66_536);
    assert_eq!(result.evidence.groups, 258);
    assert!(result.evidence.runs_written >= 17);
    assert_eq!(result.evidence.runs_validated, result.evidence.runs_written);
    assert!(result.evidence.buffer_capacity_pairs >= BLOCK_ROWS);
    assert!(result.evidence.merge_passes >= 5);
    assert!(result.evidence.peak_disk_bytes > 0);
    assert!(result.evidence.peak_reserved_bytes <= 2 << 20);
    assert!(
        memory.snapshot().reserved_bytes > 0,
        "final selection owns its capacity after store teardown"
    );
    workspace.assert_empty();
    drop(result);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

fn integer_fixtures() -> Vec<(ArrayRef, [i128; 5], bool)> {
    macro_rules! fixture {
        ($type:ty, $signed:expr) => {{
            let values: [$type; 5] = [<$type>::MIN, 7, <$type>::MAX, 7, <$type>::MAX];
            (
                PrimitiveArray::new(values.to_vec(), Validity::NonNullable).into_array(),
                values.map(i128::from),
                $signed,
            )
        }};
    }
    vec![
        fixture!(i8, true),
        fixture!(i16, true),
        fixture!(i32, true),
        fixture!(i64, true),
        fixture!(u8, false),
        fixture!(u16, false),
        fixture!(u32, false),
        fixture!(u64, false),
    ]
}

#[test]
fn exact_distinct_spill_all_native_widths_extrema_and_dictionary_domain_roundtrip() {
    let workspace = Workspace::new();
    let (runtime, session) = runtime();
    let fixtures = integer_fixtures();
    for (group, groups, group_signed) in &fixtures {
        for (value, values, value_signed) in &fixtures {
            let policy = workspace.policy();
            let memory = LiveMemoryPool::new(policy.memory_bytes).unwrap();
            let mut spill =
                ExactDistinctSpill::new(policy, memory.clone(), *group_signed, *value_signed, 5)
                    .unwrap();
            let reversed =
                PrimitiveArray::new(vec![4_u8, 3, 2, 1, 0], Validity::NonNullable).into_array();
            let dictionary = DictArray::try_new(reversed.clone(), value.take(reversed).unwrap())
                .unwrap()
                .into_array();
            let partial = super::super::count(
                group,
                &dictionary,
                session.create_execution_ctx(),
                &ChunkWorkerContext::Inline(CancellationToken::default()),
                &memory,
            )
            .unwrap();
            partial
                .visit(|pair, weight| spill.push(pair, weight, &runtime, &session))
                .unwrap();
            drop(partial);
            let result = spill.finish(&runtime, &session).unwrap();
            let mut expected = BTreeMap::<i128, BTreeSet<i128>>::new();
            for (&group, &value) in groups.iter().zip(values) {
                expected.entry(group).or_default().insert(value);
            }
            let mut expected = expected
                .into_iter()
                .map(|(group, values)| (group, u64::try_from(values.len()).unwrap()))
                .collect::<Vec<_>>();
            expected.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0)));
            assert_eq!(result_rows(&result, 0), expected);
            assert_eq!(result.evidence.rows, 5);
            drop(result);
            workspace.assert_empty();
            assert_eq!(memory.snapshot().reserved_bytes, 0);
        }
    }
}

#[test]
fn exact_distinct_spill_quota_cancel_and_invalid_weight_cleanup_are_terminal() {
    let workspace = Workspace::new();
    let (runtime, session) = runtime();
    for failure in 0..4 {
        let mut policy = workspace.policy();
        if failure == 0 {
            policy.quota_bytes = 32 * 1024 + 100;
        }
        let cancel = Arc::clone(&policy.cancellation);
        let memory = LiveMemoryPool::new(policy.memory_bytes).unwrap();
        let mut spill = ExactDistinctSpill::new(policy, memory.clone(), false, false, 7).unwrap();
        for value in 0..2048 {
            spill
                .push(unsigned(value % 17, value), 1, &runtime, &session)
                .unwrap();
        }
        if failure == 0 {
            assert!(spill.flush(&runtime, &session).is_err());
        } else {
            spill.flush(&runtime, &session).unwrap();
            match failure {
                1 => {
                    let mut merge = RunMerge::new(
                        spill.runs.iter().map(|run| &run.native),
                        &spill.store,
                        Arc::clone(&spill.work),
                        spill.policy.clone(),
                        0,
                        &runtime,
                        &session,
                    )
                    .unwrap();
                    cancel.store(true, Ordering::Release);
                    assert!(
                        merge
                            .next()
                            .unwrap()
                            .unwrap_err()
                            .to_string()
                            .contains("cancelled")
                    );
                    assert!(merge.next().is_none(), "cancelled merge must be terminal");
                    drop(merge);
                }
                2 => {
                    assert!(spill.push(unsigned(0, 0), 0, &runtime, &session).is_err());
                }
                _ => {
                    assert!(spill.push(mixed(-1, 0), 1, &runtime, &session).is_err());
                }
            }
        }
        assert!(spill.finish(&runtime, &session).is_err());
        workspace.assert_empty();
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
    let policy = workspace.policy();
    policy.cancellation.store(true, Ordering::Release);
    let memory = LiveMemoryPool::new(policy.memory_bytes).unwrap();
    assert!(ExactDistinctSpill::new(policy, memory.clone(), false, false, 7).is_err());
    workspace.assert_empty();
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn exact_distinct_spill_checksum_and_valid_checksum_semantic_corruption_rejected() {
    let workspace = Workspace::new();
    let (runtime, session) = runtime();
    for failure in 0..4 {
        let policy = workspace.policy();
        let memory = LiveMemoryPool::new(policy.memory_bytes).unwrap();
        let mut spill = ExactDistinctSpill::new(policy, memory.clone(), false, false, 7).unwrap();
        if failure == 0 {
            spill.push(unsigned(1, 1), 1, &runtime, &session).unwrap();
            spill.flush(&runtime, &session).unwrap();
            let mut file = fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&spill.runs[0].native.path)
                .unwrap();
            let mut byte = [0];
            file.read_exact(&mut byte).unwrap();
            byte[0] ^= 1;
            file.rewind().unwrap();
            file.write_all(&byte).unwrap();
            file.sync_all().unwrap();
        } else {
            let rows = match failure {
                1 => vec![
                    Record {
                        pair: unsigned(1, 2),
                        weight: 1,
                    },
                    Record {
                        pair: unsigned(1, 1),
                        weight: 1,
                    },
                ],
                2 => vec![Record {
                    pair: unsigned(1, 1),
                    weight: 0,
                }],
                _ => vec![Record {
                    pair: mixed(-1, 1),
                    weight: 1,
                }],
            };
            let native = spill
                .store
                .write_arrays(
                    &spec(u64::try_from(rows.len()).unwrap()).unwrap(),
                    [Ok(runs::array(&rows))].into_iter(),
                    &runtime,
                    &session,
                    &spill.work,
                )
                .unwrap();
            spill.evidence.rows = u64::try_from(rows.len()).unwrap();
            spill.runs.push(Run { native, level: 0 });
        }
        assert!(spill.finish(&runtime, &session).is_err());
        workspace.assert_empty();
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn exact_distinct_spill_compaction_charges_live_inputs_and_failed_output_together() {
    let workspace = Workspace::new();
    let (runtime, session) = runtime();
    let policy = workspace.policy();
    let memory = LiveMemoryPool::new(policy.memory_bytes).unwrap();
    let mut probe = ExactDistinctSpill::new(policy, memory.clone(), false, false, 1).unwrap();
    for value in 0..BLOCK_ROWS {
        probe
            .push(
                unsigned(1, u64::try_from(value).unwrap()),
                1,
                &runtime,
                &session,
            )
            .unwrap();
    }
    probe.flush(&runtime, &session).unwrap();
    let run_bytes = probe.runs[0].native.bytes;
    drop(probe);
    workspace.assert_empty();
    assert_eq!(memory.snapshot().reserved_bytes, 0);

    let mut policy = workspace.policy();
    policy.quota_bytes = 32 * 1024 + 4 * run_bytes + run_bytes / 2;
    let quota = policy.quota_bytes;
    let mut spill = ExactDistinctSpill::new(policy, memory.clone(), false, false, 1).unwrap();
    for run in 0..FAN_IN {
        for value in 0..BLOCK_ROWS {
            spill
                .push(
                    unsigned(1, u64::try_from(run * BLOCK_ROWS + value).unwrap()),
                    1,
                    &runtime,
                    &session,
                )
                .unwrap();
        }
        let result = spill.flush(&runtime, &session);
        if run + 1 == FAN_IN {
            assert!(
                result.is_err(),
                "output quota may not ignore its live merge inputs"
            );
        } else {
            result.unwrap();
        }
    }
    assert!(spill.store.snapshot().peak_disk_bytes <= quota);
    assert!(spill.finish(&runtime, &session).is_err());
    workspace.assert_empty();
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn exact_distinct_spill_empty_result_block_ownership_and_geometry_reservations() {
    let workspace = Workspace::new();
    let (runtime, session) = runtime();
    let policy = workspace.policy();
    let memory = LiveMemoryPool::new(policy.memory_bytes).unwrap();
    let spill = ExactDistinctSpill::new(policy.clone(), memory.clone(), false, false, 7).unwrap();
    let result = spill.finish(&runtime, &session).unwrap();
    assert!(result_rows(&result, 0).is_empty());
    assert_eq!(result.evidence.rows, 0);
    assert_eq!(result.evidence.runs_written, 0);
    workspace.assert_empty();
    drop(result);
    assert_eq!(memory.snapshot().reserved_bytes, 0);

    let mut spill = ExactDistinctSpill::new(policy, memory.clone(), false, false, 1).unwrap();
    for value in 0..2048 {
        spill
            .push(unsigned(1, value), 1, &runtime, &session)
            .unwrap();
    }
    spill.flush(&runtime, &session).unwrap();
    let mut reader = spill
        .store
        .open(
            &spill.runs[0].native,
            &run_dtype(),
            &runtime,
            &session,
            Arc::clone(&spill.work),
        )
        .unwrap();
    assert_eq!(reader.next_block_offset(), 0);
    let block = reader.next_block(&runtime).unwrap().unwrap();
    assert_eq!(reader.next_block_offset(), BLOCK_ROWS as u64);
    assert_eq!(block.array().len(), BLOCK_ROWS);
    drop(spill);
    workspace.assert_empty();
    assert!(memory.snapshot().reserved_bytes > 0);
    assert!(
        reader.next_block(&runtime).is_err(),
        "removed run cannot silently reopen or continue"
    );
    let column = super::super::super::logical_field_from_native_array(block.array(), "weight")
        .unwrap()
        .execute::<PrimitiveArray>(&mut session.create_execution_ctx())
        .unwrap();
    assert!(column.as_slice::<u64>().iter().all(|weight| *weight == 1));
    drop(column);
    drop(reader);
    drop(block);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    // Actual raw widths + a factor of two for native input/writer overlap,
    // four retained provider footer reads, output conversion and fixed heads.
    let input_and_output = (FAN_IN + 2) * BLOCK_ROWS * (3 * size_of::<u64>() + size_of::<u8>());
    let conversion = BLOCK_ROWS * size_of::<Record>();
    assert!(
        u64::try_from(2 * input_and_output + conversion + FAN_IN * (64 << 10) + (64 << 10))
            .unwrap()
            <= MERGE_BYTES
    );
}
