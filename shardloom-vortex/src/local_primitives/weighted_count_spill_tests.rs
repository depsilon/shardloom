use super::super::{
    aggregate_chunk_jobs::ChunkWorkerContext,
    compound_count_partial::{self, CompoundPartial},
    string_count_partial::StringCountPartial,
};
use super::*;
use shardloom_exec::compute_pool::CancellationToken;
use std::{
    collections::BTreeMap,
    fs,
    io::{Read as _, Seek as _, Write as _},
    sync::atomic::AtomicUsize,
};
use vortex::{
    VortexSessionDefault as _,
    array::{
        IntoArray as _, VortexSessionExecute as _,
        arrays::{DictArray, PrimitiveArray, VarBinViewArray},
        validity::Validity,
    },
    io::{runtime::current::CurrentThreadRuntime, session::RuntimeSessionExt as _},
};

static NEXT: AtomicUsize = AtomicUsize::new(0);
struct Workspace(PathBuf);
impl Workspace {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "shardloom-weighted-spill-{}-{}-{}",
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
            quota_bytes: 64 << 20,
            memory_bytes: 8 << 20,
            max_key_bytes: 64,
            cancellation: Arc::new(AtomicBool::new(false)),
        }
    }
    fn empty(&self) {
        assert_eq!(fs::read_dir(&self.0).unwrap().count(), 0);
    }
}
impl Drop for Workspace {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
fn runtime() -> (CurrentThreadRuntime, VortexSession) {
    let runtime = CurrentThreadRuntime::new();
    let session = VortexSession::default().with_handle(runtime.handle());
    (runtime, session)
}
fn signed(value: i64) -> AggregateIntegerKeyPart {
    AggregateIntegerKeyPart {
        bits: u64::from_ne_bytes(value.to_ne_bytes()),
        signed: true,
    }
}
fn logical(key: AggregateIntegerKeyPart) -> i128 {
    if key.signed {
        i128::from(i64::from_ne_bytes(key.bits.to_ne_bytes()))
    } else {
        i128::from(key.bits)
    }
}
type LogicalRow = (Option<i128>, String, u64);
fn collect(result: &SpilledCountResult, offset: usize) -> Vec<LogicalRow> {
    let mut rows = Vec::new();
    result
        .visit(offset, |key, text, count| {
            rows.push((key.map(logical), text.to_owned(), count));
            Ok(())
        })
        .unwrap();
    rows
}
fn expected(
    oracle: BTreeMap<(Option<i128>, String), u64>,
    order: KeyOrder,
    retained: usize,
    offset: usize,
) -> Vec<LogicalRow> {
    let mut rows = oracle
        .into_iter()
        .map(|((number, text), count)| (number, text, count))
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| {
        right.2.cmp(&left.2).then_with(|| match order {
            KeyOrder::Text => left.1.cmp(&right.1),
            KeyOrder::IntegerText { .. } => left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)),
            KeyOrder::TextInteger { .. } => left.1.cmp(&right.1).then_with(|| left.0.cmp(&right.0)),
        })
    });
    rows.truncate(retained);
    rows.into_iter().skip(offset).collect()
}

#[test]
fn weighted_count_spill_native_runs_complete_weights_global_winner_ties_and_offsets() {
    let workspace = Workspace::new();
    let (runtime, session) = runtime();
    for order in [
        KeyOrder::Text,
        KeyOrder::IntegerText { signed: true },
        KeyOrder::TextInteger { signed: true },
    ] {
        let policy = workspace.policy();
        let memory = LiveMemoryPool::new(policy.memory_bytes).unwrap();
        let mut spill = WeightedCountSpill::new(policy, memory.clone(), order, 130).unwrap();
        let mut oracle = BTreeMap::new();
        let mut total = 0_u64;
        for index in 0..16_384_u64 {
            let text = format!("東京-{:03}", index % 257);
            let numeric = if order == KeyOrder::Text {
                None
            } else {
                Some(signed(i64::try_from(index % 31).unwrap() - 15))
            };
            let weight = index % 7 + 1;
            spill
                .push(numeric, &text, weight, &runtime, &session)
                .unwrap();
            *oracle.entry((numeric.map(logical), text)).or_insert(0) += weight;
            total += weight;
            if (index + 1).is_multiple_of(1024) {
                spill.flush(&runtime, &session).unwrap();
            }
        }
        // This late key wins globally; every earlier run has already completed.
        // Equal positive tail weights also exercise declared complete-key ties.
        for (key, text, weight) in [
            (i64::MIN, "late-winner", 1_000_000),
            (i64::MAX, "same", 99),
            (i64::MIN, "same", 99),
        ] {
            let numeric = (order != KeyOrder::Text).then(|| signed(key));
            spill
                .push(numeric, text, weight, &runtime, &session)
                .unwrap();
            *oracle
                .entry((numeric.map(logical), text.to_owned()))
                .or_insert(0) += weight;
            total += weight;
        }
        let result = spill.finish(&runtime, &session).unwrap();
        assert_eq!(collect(&result, 123), expected(oracle, order, 130, 123));
        assert_eq!(result.evidence.source_weight, total);
        assert!(result.evidence.runs_written >= 20);
        assert_eq!(result.evidence.runs_written, result.evidence.runs_validated);
        assert!(result.evidence.merge_passes >= 4);
        assert!(
            result.evidence.encoded_text_bytes_copied > 0
                && result.evidence.merge_head_text_bytes_copied > 0
        );
        assert!(result.evidence.peak_reserved_bytes <= memory.snapshot().limit_bytes);
        assert_eq!(memory.snapshot().reserved_bytes, result.reserved_bytes());
        workspace.empty();
        drop(result);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn weighted_count_spill_all_integer_widths_and_dictionary_domains_use_complete_values() {
    let workspace = Workspace::new();
    let (runtime, session) = runtime();
    macro_rules! signed_array {
        ($t:ty) => {{
            let values = [<$t>::MIN, 7, <$t>::MAX, 7];
            (
                PrimitiveArray::new(values.to_vec(), Validity::NonNullable).into_array(),
                values.map(i128::from),
                true,
            )
        }};
    }
    macro_rules! unsigned_array {
        ($t:ty) => {{
            let values = [<$t>::MIN, 7, <$t>::MAX, 7];
            (
                PrimitiveArray::new(values.to_vec(), Validity::NonNullable).into_array(),
                values.map(i128::from),
                false,
            )
        }};
    }
    for (numbers, values, signed) in [
        signed_array!(i8),
        signed_array!(i16),
        signed_array!(i32),
        signed_array!(i64),
        unsigned_array!(u8),
        unsigned_array!(u16),
        unsigned_array!(u32),
        unsigned_array!(u64),
    ] {
        for numeric_first in [true, false] {
            let order = if numeric_first {
                KeyOrder::IntegerText { signed }
            } else {
                KeyOrder::TextInteger { signed }
            };
            let policy = workspace.policy();
            let memory = LiveMemoryPool::new(policy.memory_bytes).unwrap();
            let mut spill = WeightedCountSpill::new(policy, memory.clone(), order, 10).unwrap();
            let mut oracle = BTreeMap::new();
            for (codes, dictionary) in [
                ([2_u8, 0, 1, 3], ["東京", "", "α", "東京"]),
                ([1, 3, 0, 2], ["", "α", "東京", "東京"]),
            ] {
                let text = DictArray::try_new(
                    PrimitiveArray::new(codes.to_vec(), Validity::NonNullable).into_array(),
                    VarBinViewArray::from_iter_str(dictionary).into_array(),
                )
                .unwrap()
                .into_array();
                let mut lease = memory
                    .reserve(
                        CompoundPartial::bytes(numbers.len()).unwrap()
                            + CompoundPartial::dictionary_hash_bytes(&text).unwrap(),
                    )
                    .unwrap();
                let mut partial = compound_count_partial::count(
                    &numbers,
                    &text,
                    session.create_execution_ctx(),
                    &ChunkWorkerContext::Inline(CancellationToken::default()),
                    &mut lease,
                )
                .unwrap();
                partial.force_collision_hashes();
                partial
                    .visit(|key, value, count| {
                        spill.push(Some(key.part()), value, count, &runtime, &session)
                    })
                    .unwrap();
                drop(partial);
                drop(lease);
                for (&value, text) in values.iter().zip(["α", "東京", "", "東京"]) {
                    *oracle.entry((Some(value), text.to_owned())).or_insert(0) += 1;
                }
                spill.flush(&runtime, &session).unwrap();
            }
            let result = spill.finish(&runtime, &session).unwrap();
            assert_eq!(collect(&result, 0), expected(oracle, order, 10, 0));
            assert_eq!(result.evidence.source_weight, 8);
            drop(result);
            workspace.empty();
            assert_eq!(memory.snapshot().reserved_bytes, 0);
        }
    }
}

#[test]
fn weighted_count_spill_native_weighted_partial_empty_and_small_are_lazy() {
    let workspace = Workspace::new();
    let (runtime, session) = runtime();
    for input in [
        vec![],
        vec![
            ("東京", 0, 3),
            ("", 0, 7),
            ("東京", 0, 11),
            ("different", 0, 14),
        ],
    ] {
        let policy = workspace.policy();
        let memory = LiveMemoryPool::new(policy.memory_bytes).unwrap();
        let mut spill =
            WeightedCountSpill::new(policy, memory.clone(), KeyOrder::Text, 10).unwrap();
        assert_eq!(spill.evidence.buffer_bytes, spill.arena.capacity());
        assert!(spill.evidence.buffer_bytes as u64 <= memory.snapshot().reserved_bytes);
        let partial = StringCountPartial::benchmark_weighted(&input, &memory).unwrap();
        let mut oracle = BTreeMap::new();
        for &(text, _, count) in &input {
            *oracle.entry((None, text.to_owned())).or_insert(0) += count;
        }
        partial
            .for_each_count(|text, count| spill.push(None, text, count, &runtime, &session))
            .unwrap();
        drop(partial);
        assert!(spill.store.is_none());
        let result = spill.finish(&runtime, &session).unwrap();
        assert_eq!(collect(&result, 0), expected(oracle, KeyOrder::Text, 10, 0));
        assert_eq!(result.evidence.runs_written, 0);
        assert_eq!(result.evidence.peak_disk_bytes, 0);
        assert_eq!(result.evidence.encoded_text_bytes_copied, 0);
        assert_eq!(result.evidence.merge_head_text_bytes_copied, 0);
        workspace.empty();
        drop(result);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn weighted_count_spill_repeated_complete_keys_reduce_native_records_and_written_bytes() {
    let workspace = Workspace::new();
    let (runtime, session) = runtime();
    let mut observations = Vec::new();
    for repeated in [true, false] {
        let policy = workspace.policy();
        let memory = LiveMemoryPool::new(policy.memory_bytes).unwrap();
        let mut spill =
            WeightedCountSpill::new(policy, memory.clone(), KeyOrder::Text, 1024).unwrap();
        let mut oracle = BTreeMap::new();
        for index in 0..1024 {
            let text = if repeated {
                "shared00".to_owned()
            } else {
                format!("{index:08}")
            };
            spill.push(None, &text, 3, &runtime, &session).unwrap();
            *oracle.entry((None, text)).or_insert(0) += 3;
        }
        spill.flush(&runtime, &session).unwrap();
        assert_eq!(spill.runs[0].native.rows, if repeated { 1 } else { 1024 });
        let result = spill.finish(&runtime, &session).unwrap();
        assert_eq!(
            collect(&result, 0),
            expected(oracle, KeyOrder::Text, 1024, 0)
        );
        assert_eq!(result.evidence.source_records, 1024);
        assert_eq!(result.evidence.source_weight, 3072);
        assert_eq!(
            result.evidence.initial_run_records,
            if repeated { 1 } else { 1024 }
        );
        assert_eq!(
            result.evidence.native_records_written,
            result.evidence.initial_run_records
        );
        assert_eq!(result.evidence.runs_written, 1);
        assert!(result.evidence.native_bytes_written > 0);
        observations.push((
            result.evidence.native_bytes_written,
            result.evidence.encoded_text_bytes_copied,
        ));
        drop(result);
        workspace.empty();
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
    assert_eq!(observations[0].1, 8);
    assert_eq!(observations[1].1, 8 * 1024);
    assert!(
        observations[0].0 < observations[1].0,
        "actual repeated-key native file must omit the duplicate records"
    );
}

#[test]
fn weighted_count_spill_text_byte_pressure_and_oversize_admission_refund() {
    let workspace = Workspace::new();
    let (runtime, session) = runtime();
    let mut policy = workspace.policy();
    policy.max_key_bytes = 4096;
    let memory = LiveMemoryPool::new(policy.memory_bytes).unwrap();
    let mut spill = WeightedCountSpill::new(policy, memory.clone(), KeyOrder::Text, 7).unwrap();
    let mut oracle = BTreeMap::new();
    for index in 0..1025 {
        let text = format!("{:04}{}", index % 19, "x".repeat(4092));
        spill.push(None, &text, 3, &runtime, &session).unwrap();
        *oracle.entry((None, text)).or_insert(0) += 3;
    }
    assert!(
        !spill.runs.is_empty(),
        "byte pressure must flush well before the row capacity"
    );
    assert!(spill.buffer.len() < spill.evidence.buffer_rows);
    assert!(spill.evidence.block_rows * (4096 + 32) <= BLOCK_BYTES);
    let result = spill.finish(&runtime, &session).unwrap();
    assert_eq!(collect(&result, 2), expected(oracle, KeyOrder::Text, 7, 2));
    drop(result);
    workspace.empty();
    assert_eq!(memory.snapshot().reserved_bytes, 0);

    let policy = workspace.policy();
    let mut spill = WeightedCountSpill::new(policy, memory.clone(), KeyOrder::Text, 7).unwrap();
    assert!(
        spill
            .push(None, &"x".repeat(65), 1, &runtime, &session)
            .is_err()
    );
    assert_eq!(spill.evidence.source_text_bytes_copied, 0);
    assert!(spill.store.is_none());
    assert!(spill.finish(&runtime, &session).is_err());
    workspace.empty();
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    let mut policy = workspace.policy();
    policy.memory_bytes = 4 << 20;
    let small = LiveMemoryPool::new(policy.memory_bytes).unwrap();
    assert!(WeightedCountSpill::new(policy, small.clone(), KeyOrder::Text, 1_000_000).is_err());
    workspace.empty();
    assert_eq!(small.snapshot().reserved_bytes, 0);
}

#[test]
fn weighted_count_spill_quota_cancel_invalid_weight_and_key_are_terminal() {
    let workspace = Workspace::new();
    let (runtime, session) = runtime();
    for failure in 0..5 {
        let mut policy = workspace.policy();
        if failure == 0 {
            policy.quota_bytes = 32 * 1024 + 100;
        }
        let cancel = Arc::clone(&policy.cancellation);
        let memory = LiveMemoryPool::new(policy.memory_bytes).unwrap();
        let mut spill = WeightedCountSpill::new(policy, memory.clone(), KeyOrder::Text, 7).unwrap();
        for index in 0..1024 {
            spill
                .push(None, &format!("{index:08}"), 1, &runtime, &session)
                .unwrap();
        }
        if failure == 0 {
            assert!(spill.flush(&runtime, &session).is_err());
        } else {
            spill.flush(&runtime, &session).unwrap();
            match failure {
                1 => {
                    let mut merge = RunMerge::new(
                        spill.runs.iter(),
                        spill.store.as_ref().unwrap(),
                        Arc::clone(&spill.work),
                        spill.policy.clone(),
                        spill.order,
                        Arc::clone(&spill.copies),
                        &runtime,
                        &session,
                    )
                    .unwrap();
                    cancel.store(true, Ordering::Release);
                    assert!(
                        merge
                            .next()
                            .unwrap()
                            .err()
                            .unwrap()
                            .to_string()
                            .contains("cancelled")
                    );
                    assert!(merge.next().is_none());
                    drop(merge);
                }
                2 => {
                    assert!(spill.push(None, "bad", 0, &runtime, &session).is_err());
                }
                3 => {
                    assert!(
                        spill
                            .push(Some(signed(-1)), "bad", 1, &runtime, &session)
                            .is_err()
                    );
                }
                _ => {
                    assert!(
                        spill
                            .push(None, "overflow", u64::MAX, &runtime, &session)
                            .is_err()
                    );
                }
            }
        }
        assert!(spill.finish(&runtime, &session).is_err());
        workspace.empty();
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn weighted_count_spill_native_corruption_signature_order_and_weight_are_rejected() {
    let workspace = Workspace::new();
    let (runtime, session) = runtime();
    for corruption in 0..5 {
        let policy = workspace.policy();
        let memory = LiveMemoryPool::new(policy.memory_bytes).unwrap();
        let mut spill = WeightedCountSpill::new(policy, memory.clone(), KeyOrder::Text, 7).unwrap();
        if corruption == 0 {
            spill.push(None, "ok", 2, &runtime, &session).unwrap();
            spill.flush(&runtime, &session).unwrap();
            let path = &spill.runs[0].native.path;
            let mut file = fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(path)
                .unwrap();
            let mut byte = [0];
            file.read_exact(&mut byte).unwrap();
            file.rewind().unwrap();
            byte[0] ^= 0x55;
            file.write_all(&byte).unwrap();
            file.sync_all().unwrap();
        } else {
            spill.ensure_store().unwrap();
            let rows = match corruption {
                1 => vec![(0, b"z".as_slice(), 1), (0, b"a".as_slice(), 1)],
                2 => vec![(0, b"a".as_slice(), 0), (0, b"b".as_slice(), 2)],
                3 => vec![(1, b"a".as_slice(), 1), (1, b"b".as_slice(), 1)],
                _ => vec![(0, b"a".as_slice(), 1), (0, b"b".as_slice(), 1)],
            };
            let signature = if corruption == 4 { 2 } else { 0 };
            let spec = spill.spec(2, 1).unwrap();
            let array = runs::array(rows.into_iter(), signature, &spill.copies).unwrap();
            let native = spill
                .store
                .as_mut()
                .unwrap()
                .write_arrays(
                    &spec,
                    std::iter::once(Ok(array)),
                    &runtime,
                    &session,
                    &spill.work,
                )
                .unwrap();
            spill.runs.push(Run {
                native,
                level: 0,
                max_key_bytes: 1,
            });
            spill.evidence.source_weight = 2;
        }
        assert!(spill.finish(&runtime, &session).is_err());
        workspace.empty();
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn weighted_count_spill_input_output_quota_overlap_preserves_source_runs_until_failure() {
    let workspace = Workspace::new();
    let (runtime, session) = runtime();
    let policy = workspace.policy();
    let memory = LiveMemoryPool::new(policy.memory_bytes).unwrap();
    let mut probe = WeightedCountSpill::new(policy, memory.clone(), KeyOrder::Text, 7).unwrap();
    for index in 0..1024 {
        probe
            .push(None, &format!("{index:08}"), 1, &runtime, &session)
            .unwrap();
    }
    probe.flush(&runtime, &session).unwrap();
    let run_bytes = probe.runs[0].native.bytes;
    drop(probe);
    workspace.empty();
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    let mut policy = workspace.policy();
    policy.quota_bytes = (32 << 10) + run_bytes * 4 + 100;
    let mut spill = WeightedCountSpill::new(policy, memory.clone(), KeyOrder::Text, 7).unwrap();
    for run in 0..4 {
        for index in 0..1024 {
            spill
                .push(None, &format!("{index:08}"), 1, &runtime, &session)
                .unwrap();
        }
        let result = spill.flush(&runtime, &session);
        if run < 3 {
            result.unwrap();
        } else {
            assert!(
                result.is_err(),
                "four existing inputs leave no quota for merged output"
            );
        }
    }
    assert!(spill.store.as_ref().unwrap().snapshot().peak_disk_bytes <= spill.policy.quota_bytes);
    assert!(spill.finish(&runtime, &session).is_err());
    workspace.empty();
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn weighted_count_spill_actual_short_long_mixed_run_geometry_and_complete_merge() {
    let workspace = Workspace::new();
    let (runtime, session) = runtime();
    let mut admitted_work = None;
    for widths in [
        [8, 8, 8, 8],
        [BLOCK_BYTES; 4],
        [8, BLOCK_BYTES, 8, BLOCK_BYTES],
    ] {
        let mut policy = workspace.policy();
        policy.max_key_bytes = BLOCK_BYTES;
        let memory = LiveMemoryPool::new(policy.memory_bytes).unwrap();
        let mut spill = WeightedCountSpill::new(policy, memory.clone(), KeyOrder::Text, 7).unwrap();
        // Actual short input must not shrink the up-front worst-case head and
        // conversion reservation. Only persisted run geometry adapts.
        let work_bytes = spill.work.bytes();
        assert_eq!(*admitted_work.get_or_insert(work_bytes), work_bytes);
        let mut oracle = BTreeMap::new();
        for (batch, width) in widths.into_iter().enumerate() {
            let rows = if width == 8 { 256 } else { 4 };
            for index in 0..rows {
                let text = format!("{batch:02}{index:06}{}", "x".repeat(width - 8));
                let weight = u64::try_from(index % 3 + 1).unwrap();
                spill.push(None, &text, weight, &runtime, &session).unwrap();
                *oracle.entry((None, text)).or_insert(0) += weight;
            }
            spill.flush(&runtime, &session).unwrap();
            if batch < FAN_IN - 1 {
                let run = spill.runs.last().unwrap();
                assert_eq!(run.max_key_bytes, width);
                assert_eq!(run.native.block_rows, if width == 8 { 1024 } else { 1 });
            }
        }
        assert_eq!(spill.runs.len(), 1);
        let merged = &spill.runs[0];
        let max_key = widths.into_iter().max().unwrap();
        assert_eq!(merged.level, 1);
        assert_eq!(merged.max_key_bytes, max_key);
        assert_eq!(
            merged.native.block_rows,
            if max_key == 8 { 1024 } else { 1 }
        );
        let result = spill.finish(&runtime, &session).unwrap();
        assert_eq!(collect(&result, 2), expected(oracle, KeyOrder::Text, 7, 2));
        assert_eq!(result.evidence.max_run_key_bytes, max_key);
        assert_eq!(
            result.evidence.block_rows,
            if max_key == 8 { 1024 } else { 1 }
        );
        assert_eq!(
            result.evidence.max_block_rows,
            if widths.contains(&8) { 1024 } else { 1 }
        );
        assert_eq!(result.evidence.runs_written, 5);
        assert_eq!(result.evidence.runs_validated, 5);
        assert_eq!(result.evidence.merge_passes, 1);
        assert!(result.evidence.peak_reserved_bytes <= memory.snapshot().limit_bytes);
        drop(result);
        workspace.empty();
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn weighted_count_spill_rejects_forged_run_key_and_block_geometry() {
    let workspace = Workspace::new();
    let (runtime, session) = runtime();
    for corruption in ["understated_key", "overstated_key", "block_rows"] {
        let mut policy = workspace.policy();
        policy.max_key_bytes = BLOCK_BYTES;
        let memory = LiveMemoryPool::new(policy.memory_bytes).unwrap();
        let mut spill = WeightedCountSpill::new(policy, memory.clone(), KeyOrder::Text, 7).unwrap();
        spill.push(None, "complete", 3, &runtime, &session).unwrap();
        spill.flush(&runtime, &session).unwrap();
        let run = &mut spill.runs[0];
        match corruption {
            "understated_key" => run.max_key_bytes = 7,
            "overstated_key" => run.max_key_bytes = 9,
            _ => run.native.block_rows = 1,
        }
        assert!(spill.finish(&runtime, &session).is_err());
        workspace.empty();
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}
