use super::super::{
    aggregate_chunk_jobs::ChunkWorkerContext, string_count_partial::StringCountPartial,
    string_count_partitions::StringCountPartitions, weighted_count_spill_admission,
};
use super::*;
use crate::{
    VortexAggregateOrderExpr, VortexQueryPrimitiveRequest, VortexSimpleAggregateMeasure,
    VortexSimpleAggregateRequest,
};
use shardloom_core::{ColumnRef, DatasetUri};
use shardloom_exec::compute_pool::CancellationToken;
use std::{collections::BTreeMap, path::PathBuf};
use vortex::{
    VortexSessionDefault as _,
    array::{
        IntoArray as _,
        arrays::{DictArray, StructArray, VarBinViewArray},
        dtype::FieldNames,
        validity::Validity,
    },
    io::{runtime::current::CurrentThreadRuntime, session::RuntimeSessionExt as _},
};

struct Workspace(PathBuf);
impl Workspace {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "shardloom-weighted-owner-{}-{}",
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
        VortexAggregateSpillPolicy::new(self.0.clone(), 64 << 20, 8 << 20).unwrap()
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
fn runtime() -> (CurrentThreadRuntime, VortexSession) {
    let runtime = CurrentThreadRuntime::new();
    let session = VortexSession::default().with_handle(runtime.handle());
    (runtime, session)
}
fn request(workspace: &Workspace, groups: &[&str]) -> VortexQueryPrimitiveRequest {
    VortexQueryPrimitiveRequest::simple_aggregate(
        DatasetUri::new(workspace.0.join("source.vortex").display().to_string()).unwrap(),
        VortexSimpleAggregateRequest::grouped(
            groups
                .iter()
                .map(|name| ColumnRef::new(*name).unwrap())
                .collect(),
            vec![VortexSimpleAggregateMeasure::new(
                "count",
                None,
                "frequency".into(),
            )],
        )
        .with_order_by(vec![VortexAggregateOrderExpr::new("frequency", true)])
        .with_spill(workspace.policy()),
    )
    .with_source_order_limit(7)
}
fn chunk(numbers: ArrayRef, codes: Vec<u8>, dictionary: &[&str]) -> ArrayRef {
    let text = DictArray::try_new(
        PrimitiveArray::new(codes, Validity::NonNullable).into_array(),
        VarBinViewArray::from_iter_str(dictionary.iter().copied()).into_array(),
    )
    .unwrap()
    .into_array();
    let rows = numbers.len();
    StructArray::try_new(
        FieldNames::from(["tag", "number"]),
        vec![text, numbers],
        rows,
        Validity::NonNullable,
    )
    .unwrap()
    .into_array()
}
fn logical(key: AggregateIntegerKeyPart) -> i128 {
    if key.signed {
        i128::from(i64::from_ne_bytes(key.bits.to_ne_bytes()))
    } else {
        i128::from(key.bits)
    }
}

#[test]
fn weighted_count_spill_accumulator_all_integer_widths_domain_values_and_parent_refunds() {
    let workspace = Workspace::new();
    let (runtime, session) = runtime();
    macro_rules! values {
        ($t:ty) => {{
            let values = [<$t>::MIN, 7, <$t>::MAX, 7];
            (
                PrimitiveArray::new(values.to_vec(), Validity::NonNullable).into_array(),
                values.map(i128::from),
            )
        }};
    }
    for (numbers, logical_numbers) in [
        values!(i8),
        values!(i16),
        values!(i32),
        values!(i64),
        values!(u8),
        values!(u16),
        values!(u32),
        values!(u64),
    ] {
        for groups in [["number", "tag"], ["tag", "number"]] {
            let source = chunk(
                numbers.clone(),
                vec![2, 0, 1, 3],
                &["東京", "", "α", "東京"],
            );
            let request = request(&workspace, &groups);
            let contract = weighted_count_spill_admission::admit(&request, source.dtype()).unwrap();
            let parent = LiveMemoryPool::new(32 << 20).unwrap();
            let mut accumulator =
                Accumulator::new(&workspace.policy(), contract, &parent, &session).unwrap();
            accumulator.push_source(&source, &runtime).unwrap();
            accumulator
                .push_source(
                    &chunk(
                        numbers.clone(),
                        vec![1, 3, 0, 2],
                        &["", "α", "東京", "東京"],
                    ),
                    &runtime,
                )
                .unwrap();
            let result = accumulator.finish(&runtime).unwrap();
            assert_eq!(
                parent.snapshot().reserved_bytes,
                workspace.policy().memory_bytes
            );
            let mut actual = BTreeMap::new();
            result
                .result
                .visit(0, |key, text, count| {
                    actual.insert((logical(key.unwrap()), text.to_owned()), count);
                    Ok(())
                })
                .unwrap();
            let mut expected = BTreeMap::new();
            for (number, text) in logical_numbers.into_iter().zip(["α", "東京", "", "東京"]) {
                *expected.entry((number, text.to_owned())).or_insert(0) += 2;
            }
            assert_eq!(actual, expected);
            assert_eq!(result.source_work.dictionary_batches, 2);
            assert_eq!(result.result.evidence.source_weight, 8);
            assert_eq!(result.result.evidence.runs_written, 0);
            drop(result);
            assert_eq!(parent.snapshot().reserved_bytes, 0);
            workspace.empty();
        }
    }
}

#[test]
fn weighted_count_spill_accumulator_drained_real_partitions_and_untouched_suffix_are_exact_once() {
    let workspace = Workspace::new();
    let (runtime, session) = runtime();
    let parent = LiveMemoryPool::new(32 << 20).unwrap();
    let source = chunk(
        PrimitiveArray::new(vec![0_i64], Validity::NonNullable).into_array(),
        vec![0],
        &["tag"],
    );
    let contract =
        weighted_count_spill_admission::admit(&request(&workspace, &["tag"]), source.dtype())
            .unwrap();
    let mut accumulator =
        Accumulator::new(&workspace.policy(), contract, &parent, &session).unwrap();
    let partitions = StringCountPartitions::try_new(&parent, 16, 7)
        .unwrap()
        .unwrap();
    let worker = ChunkWorkerContext::Inline(CancellationToken::default());
    let mut task = parent.reserve(64 << 10).unwrap();
    let prefix =
        StringCountPartial::benchmark_weighted(&[("kept", 0, 3), ("same", 0, 5)], &parent).unwrap();
    assert!(
        partitions
            .reduce(prefix, &worker, &mut task)
            .unwrap()
            .deferred
            .is_none()
    );
    partitions.request_pressure();
    let suffix =
        StringCountPartial::benchmark_weighted(&[("same", 0, 7), ("late", 0, 9)], &parent).unwrap();
    let suffix = partitions
        .reduce(suffix, &worker, &mut task)
        .unwrap()
        .deferred
        .unwrap();
    accumulator
        .transfer_drained_epoch(
            24,
            |visit| partitions.replay_and_release(|text, weight| visit(None, text, weight)),
            |visit| suffix.for_each_count(|text, weight| visit(None, text, weight)),
            &runtime,
        )
        .unwrap();
    drop(suffix);
    drop(partitions);
    drop(task);
    let result = accumulator.finish(&runtime).unwrap();
    let mut rows = Vec::new();
    result
        .result
        .visit(0, |key, text, weight| {
            assert!(key.is_none());
            rows.push((text.to_owned(), weight));
            Ok(())
        })
        .unwrap();
    assert_eq!(
        rows,
        vec![("same".into(), 12), ("late".into(), 9), ("kept".into(), 3)]
    );
    assert_eq!(result.source_work.drained_epochs, 1);
    assert_eq!(result.source_work.committed_weight, 8);
    assert_eq!(result.source_work.deferred_weight, 16);
    assert_eq!(parent.snapshot().reserved_bytes, 8 << 20);
    drop(result);
    assert_eq!(parent.snapshot().reserved_bytes, 0);
    workspace.empty();
}

#[test]
fn weighted_count_spill_accumulator_failed_transfer_source_shape_and_parent_admission_are_terminal()
{
    let workspace = Workspace::new();
    let (runtime, session) = runtime();
    let source = chunk(
        PrimitiveArray::new(vec![0_i64], Validity::NonNullable).into_array(),
        vec![0],
        &["tag"],
    );
    let contract =
        weighted_count_spill_admission::admit(&request(&workspace, &["tag"]), source.dtype())
            .unwrap();
    let small = LiveMemoryPool::new(4 << 20).unwrap();
    assert!(Accumulator::new(&workspace.policy(), contract.clone(), &small, &session).is_err());
    assert_eq!(small.snapshot().reserved_bytes, 0);
    workspace.empty();
    for failure in ["mismatch", "visitor", "dtype", "cancel"] {
        let parent = LiveMemoryPool::new(32 << 20).unwrap();
        let policy = workspace.policy();
        let mut accumulator =
            Accumulator::new(&policy, contract.clone(), &parent, &session).unwrap();
        match failure {
            "mismatch" => assert!(
                accumulator
                    .transfer_drained_epoch(
                        2,
                        |visit| visit(None, "prefix", 1),
                        |_| Ok(()),
                        &runtime
                    )
                    .is_err()
            ),
            "visitor" => assert!(
                accumulator
                    .transfer_drained_epoch(
                        2,
                        |visit| visit(None, "prefix", 1),
                        |_| Err(failed("injected untouched suffix failure")),
                        &runtime
                    )
                    .is_err()
            ),
            "dtype" => {
                let changed = StructArray::try_new(
                    FieldNames::from(["tag"]),
                    vec![PrimitiveArray::new(vec![1_i64], Validity::NonNullable).into_array()],
                    1,
                    Validity::NonNullable,
                )
                .unwrap()
                .into_array();
                assert!(accumulator.push_source(&changed, &runtime).is_err());
            }
            _ => {
                policy.cancel();
                assert!(accumulator.push_source(&source, &runtime).is_err());
            }
        }
        assert!(accumulator.push_source(&source, &runtime).is_err());
        assert!(accumulator.finish(&runtime).is_err());
        assert_eq!(parent.snapshot().reserved_bytes, 0);
        workspace.empty();
    }
}
