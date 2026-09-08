use super::{OwnedSpillResult, SpillAccumulator};
use crate::VortexAggregateSpillPolicy;
use shardloom_exec::live_memory::LiveMemoryPool;
use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
};
use vortex::{
    VortexSessionDefault as _,
    array::{
        ArrayRef, IntoArray as _,
        arrays::{PrimitiveArray, StructArray},
        dtype::{DType, FieldNames, Nullability, PType},
        validity::Validity,
    },
    io::{
        runtime::{BlockingRuntime as _, current::CurrentThreadRuntime},
        session::RuntimeSessionExt as _,
    },
    session::VortexSession,
};

struct Workspace(PathBuf);
impl Workspace {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "shardloom-distinct-public-accumulator-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn policy(&self) -> VortexAggregateSpillPolicy {
        VortexAggregateSpillPolicy::new(self.0.clone(), 64 << 20, 4 << 20).unwrap()
    }
    fn empty(&self) {
        assert_eq!(std::fs::read_dir(&self.0).unwrap().count(), 0);
    }
}
impl Drop for Workspace {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn chunk(rows: &[(i16, u64)]) -> ArrayRef {
    StructArray::try_new(
        FieldNames::from(["renamed_member", "renamed_cohort"]),
        vec![
            PrimitiveArray::new(
                rows.iter().map(|pair| pair.1).collect::<Vec<_>>(),
                Validity::NonNullable,
            )
            .into_array(),
            PrimitiveArray::new(
                rows.iter().map(|pair| pair.0).collect::<Vec<_>>(),
                Validity::NonNullable,
            )
            .into_array(),
        ],
        rows.len(),
        Validity::NonNullable,
    )
    .unwrap()
    .into_array()
}

fn accumulator(
    policy: &VortexAggregateSpillPolicy,
    parent: &LiveMemoryPool,
    session: &VortexSession,
    retained: usize,
) -> SpillAccumulator {
    SpillAccumulator::new(
        policy,
        ["renamed_cohort".into(), "renamed_member".into()],
        [
            DType::Primitive(PType::I16, Nullability::NonNullable),
            DType::Primitive(PType::U64, Nullability::NonNullable),
        ],
        retained,
        parent,
        session,
    )
    .unwrap()
}

fn values(result: &OwnedSpillResult) -> Vec<(i64, u64)> {
    let mut output = Vec::new();
    result
        .result
        .visit(0, |key, count| {
            assert!(key.signed);
            output.push((i64::from_ne_bytes(key.bits.to_ne_bytes()), count));
            Ok(())
        })
        .unwrap();
    output
}

#[test]
fn exact_distinct_spill_accumulator_retains_query_envelope_through_result_and_lazy_small_input() {
    let workspace = Workspace::new();
    let policy = workspace.policy();
    let parent = LiveMemoryPool::new(16 << 20).unwrap();
    let runtime = CurrentThreadRuntime::new();
    let session = VortexSession::default().with_handle(runtime.handle());
    for rows in [vec![], vec![(2, u64::MAX), (2, u64::MAX), (-1, 7), (2, 3)]] {
        let mut accumulator = accumulator(&policy, &parent, &session, 4);
        workspace.empty();
        accumulator.push(&chunk(&rows), &runtime).unwrap();
        workspace.empty();
        let result = accumulator.finish(&runtime).unwrap();
        let expected = if rows.is_empty() {
            vec![]
        } else {
            vec![(2, 2), (-1, 1)]
        };
        assert_eq!(values(&result), expected);
        assert_eq!(result.result.evidence.runs_written, 0);
        assert_eq!(result.reserved_bytes(), policy.memory_bytes);
        assert!(result.selected_reserved_bytes() > 0);
        assert!(result.selected_reserved_bytes() <= result.reserved_bytes());
        assert_eq!(parent.snapshot().reserved_bytes, policy.memory_bytes);
        workspace.empty();
        drop(result);
        assert_eq!(parent.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn exact_distinct_spill_accumulator_multiple_native_runs_complete_reordered_values_and_parent_refund()
 {
    let workspace = Workspace::new();
    let policy = workspace.policy();
    let parent = LiveMemoryPool::new(16 << 20).unwrap();
    let runtime = CurrentThreadRuntime::new();
    let session = VortexSession::default().with_handle(runtime.handle());
    let rows = (0..131_072)
        .map(|index| {
            let logical = index % 65_536;
            (
                i16::try_from(logical % 257).unwrap() - 128,
                (1_u64 << 61) + u64::try_from(logical).unwrap(),
            )
        })
        .collect::<Vec<_>>();
    let mut expected = BTreeMap::<i64, BTreeSet<u64>>::new();
    for (group, member) in &rows {
        expected
            .entry(i64::from(*group))
            .or_default()
            .insert(*member);
    }
    let mut expected = expected
        .into_iter()
        .map(|(key, members)| (key, u64::try_from(members.len()).unwrap()))
        .collect::<Vec<_>>();
    expected
        .sort_unstable_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0)));
    expected.truncate(130);
    let mut accumulator = accumulator(&policy, &parent, &session, 130);
    for rows in rows.chunks(4096) {
        accumulator.push(&chunk(rows), &runtime).unwrap();
    }
    let result = accumulator.finish(&runtime).unwrap();
    assert_eq!(values(&result), expected);
    assert!(result.result.evidence.runs_written >= 4);
    assert_eq!(
        result.result.evidence.runs_written,
        result.result.evidence.runs_validated
    );
    assert_eq!(result.result.evidence.complete_pairs, 65_536);
    assert_eq!(result.result.evidence.rows, 131_072);
    assert!(result.result.evidence.peak_reserved_bytes <= policy.memory_bytes);
    assert_eq!(parent.snapshot().reserved_bytes, policy.memory_bytes);
    workspace.empty();
    drop(result);
    assert_eq!(parent.snapshot().reserved_bytes, 0);
}

#[test]
fn exact_distinct_spill_accumulator_parent_denial_schema_and_cancel_have_no_escaped_owners() {
    let workspace = Workspace::new();
    let policy = workspace.policy();
    let runtime = CurrentThreadRuntime::new();
    let session = VortexSession::default().with_handle(runtime.handle());
    let small = LiveMemoryPool::new(2 << 20).unwrap();
    assert!(
        SpillAccumulator::new(
            &policy,
            ["g".into(), "v".into()],
            [
                DType::Primitive(PType::I16, Nullability::NonNullable),
                DType::Primitive(PType::U64, Nullability::NonNullable)
            ],
            7,
            &small,
            &session
        )
        .is_err()
    );
    assert_eq!(small.snapshot().reserved_bytes, 0);
    workspace.empty();
    let parent = LiveMemoryPool::new(16 << 20).unwrap();
    assert!(
        SpillAccumulator::new(
            &policy,
            ["g".into(), "v".into()],
            [
                DType::Primitive(PType::F64, Nullability::NonNullable),
                DType::Primitive(PType::U64, Nullability::NonNullable)
            ],
            7,
            &parent,
            &session
        )
        .is_err()
    );
    assert_eq!(parent.snapshot().reserved_bytes, 0);
    let mut accumulator = accumulator(&policy, &parent, &session, 7);
    let rows = (0..65_537_u64)
        .map(|value| (1_i16, value))
        .collect::<Vec<_>>();
    for rows in rows.chunks(4096) {
        accumulator.push(&chunk(rows), &runtime).unwrap();
    }
    policy.cancel();
    assert!(accumulator.finish(&runtime).is_err());
    workspace.empty();
    assert_eq!(parent.snapshot().reserved_bytes, 0);
}

#[test]
fn exact_distinct_spill_accumulator_source_error_is_terminal_and_refunds_owned_runs() {
    let workspace = Workspace::new();
    let policy = workspace.policy();
    let parent = LiveMemoryPool::new(16 << 20).unwrap();
    let runtime = CurrentThreadRuntime::new();
    let session = VortexSession::default().with_handle(runtime.handle());
    let mut accumulator = accumulator(&policy, &parent, &session, 7);
    let rows = (0..65_537_u64)
        .map(|value| (1_i16, value))
        .collect::<Vec<_>>();
    for rows in rows.chunks(4096) {
        accumulator.push(&chunk(rows), &runtime).unwrap();
    }
    assert!(std::fs::read_dir(&workspace.0).unwrap().next().is_some());

    // The logical column still exists, but its width changed after admission.
    // Already-written complete pairs must never become a successful prefix result.
    let malformed = StructArray::try_new(
        FieldNames::from(["renamed_member", "renamed_cohort"]),
        vec![
            PrimitiveArray::new(vec![7_u64], Validity::NonNullable).into_array(),
            PrimitiveArray::new(vec![1_i32], Validity::NonNullable).into_array(),
        ],
        1,
        Validity::NonNullable,
    )
    .unwrap()
    .into_array();
    let error = accumulator.push(&malformed, &runtime).err().unwrap();
    assert!(
        error
            .to_string()
            .contains("source projection changed its admitted dtype")
    );
    let error = accumulator.push(&chunk(&[(1, 8)]), &runtime).err().unwrap();
    assert!(error.to_string().contains("cannot accept more source rows"));
    let error = accumulator.finish(&runtime).err().unwrap();
    assert!(error.to_string().contains("cannot publish a result"));
    workspace.empty();
    assert_eq!(parent.snapshot().reserved_bytes, 0);
}
