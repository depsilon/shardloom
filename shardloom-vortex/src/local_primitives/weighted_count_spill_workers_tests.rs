use super::*;
use crate::{
    VortexAggregateOrderExpr, VortexAggregateSpillPolicy, VortexQueryPrimitiveRequest,
    VortexSimpleAggregateMeasure, VortexSimpleAggregateRequest,
};
use shardloom_core::{ColumnRef, DatasetUri};
use std::{collections::BTreeMap, path::PathBuf};
use vortex::{
    VortexSessionDefault as _,
    array::{
        IntoArray as _,
        arrays::{ConstantArray, DictArray, PrimitiveArray, StructArray, VarBinViewArray},
        dtype::FieldNames,
        validity::Validity,
    },
    io::{runtime::current::CurrentThreadRuntime, session::RuntimeSessionExt as _},
};

struct Workspace(PathBuf);
impl Workspace {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "shardloom-weighted-workers-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
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
fn chunk(text: ArrayRef) -> ArrayRef {
    let len = text.len();
    StructArray::try_new(
        FieldNames::from(["tag"]),
        vec![text],
        len,
        Validity::NonNullable,
    )
    .unwrap()
    .into_array()
}
fn plain(values: &[String]) -> ArrayRef {
    chunk(VarBinViewArray::from_iter_str(values.iter().map(String::as_str)).into_array())
}
fn setup(
    workspace: &Workspace,
    source: &ArrayRef,
    parallelism: usize,
    entries: usize,
    quota: u64,
) -> (
    CurrentThreadRuntime,
    LiveMemoryPool,
    Accumulator,
    Workers,
    Arc<AtomicBool>,
) {
    let policy = VortexAggregateSpillPolicy::new(workspace.0.clone(), quota, 8 << 20).unwrap();
    let request = VortexQueryPrimitiveRequest::simple_aggregate(
        DatasetUri::new(workspace.0.join("input.vortex").display().to_string()).unwrap(),
        VortexSimpleAggregateRequest::grouped(
            vec![ColumnRef::new("tag").unwrap()],
            vec![VortexSimpleAggregateMeasure::new(
                "count",
                None,
                "frequency".into(),
            )],
        )
        .with_order_by(vec![VortexAggregateOrderExpr::new("frequency", true)])
        .with_spill(policy.clone()),
    )
    .with_source_order_limit(3);
    let contract =
        super::super::super::weighted_count_spill_admission::admit(&request, source.dtype())
            .unwrap();
    let runtime = CurrentThreadRuntime::new();
    let session = VortexSession::default().with_handle(runtime.handle());
    let memory = LiveMemoryPool::new(32 << 20).unwrap();
    let accumulator = Accumulator::new(&policy, contract, &memory, &session).unwrap();
    let workers = Workers::try_new(
        accumulator.worker_contract(),
        parallelism,
        entries,
        accumulator.worker_memory(),
        accumulator.worker_session(),
        Arc::clone(&policy.cancellation),
    )
    .unwrap()
    .unwrap();
    (runtime, memory, accumulator, workers, policy.cancellation)
}
fn collected(
    result: &super::super::super::weighted_count_spill_accumulator::OwnedResult,
) -> Vec<(String, u64)> {
    let mut rows = Vec::new();
    result
        .result
        .visit(0, |number, text, count| {
            assert!(number.is_none());
            rows.push((text.to_owned(), count));
            Ok(())
        })
        .unwrap();
    rows
}
fn oracle(values: impl IntoIterator<Item = String>) -> Vec<(String, u64)> {
    let mut counts = BTreeMap::new();
    for value in values {
        *counts.entry(value).or_insert(0_u64) += 1;
    }
    let mut counts: Vec<_> = counts.into_iter().collect();
    counts.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
    counts.truncate(3);
    counts
}

#[test]
fn weighted_count_workers_pressure_transfers_committed_prefix_and_untouched_suffix_at_each_grant() {
    for parallelism in [1, 2, 4, 8, 12] {
        let workspace = Workspace::new();
        // A drained native dictionary partial must preserve its domain counts
        // when later plain source values contribute to the same final merge.
        let values = ["same", "東京", "late", ""];
        let codes: Vec<u8> = (0_u8..96).map(|row| row % 4).collect();
        let source = chunk(
            DictArray::try_new(
                PrimitiveArray::new(codes.clone(), Validity::NonNullable).into_array(),
                VarBinViewArray::from_iter_str(values).into_array(),
            )
            .unwrap()
            .into_array(),
        );
        let (runtime, memory, mut accumulator, mut workers, cancellation) =
            setup(&workspace, &source, parallelism, 1, 64 << 20);
        assert!(workers.submit(&source).unwrap());
        workers.drain().unwrap();
        assert!(workers.pressured());
        assert!(workers.partitions.committed_rows.load(Ordering::Acquire) > 0);
        assert!(!workers.deferred.is_empty());
        workers.stop_for_transfer().unwrap();
        workers.transfer(&mut accumulator, &runtime).unwrap();
        let later = vec!["winner".to_owned(); 101];
        accumulator.push_source(&plain(&later), &runtime).unwrap();
        let result = accumulator.finish(&runtime).unwrap();
        assert_eq!(
            collected(&result),
            oracle(
                codes
                    .iter()
                    .map(|code| values[*code as usize].to_owned())
                    .chain(later)
            )
        );
        assert_eq!(result.result.evidence.source_weight, 197);
        assert_eq!(result.source_work.drained_epochs, 1);
        assert!(result.source_work.committed_weight > 0 && result.source_work.deferred_weight > 0);
        assert_eq!(
            result.source_work.committed_weight + result.source_work.deferred_weight,
            96
        );
        assert_eq!(result.source_work.worker_jobs, 1);
        assert_eq!(result.source_work.dictionary_batches, 1);
        assert!(result.source_work.workers_created < parallelism.max(1));
        assert!(!cancellation.load(Ordering::Acquire));
        assert_eq!(memory.snapshot().reserved_bytes, 8 << 20);
        drop(result);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
        workspace.empty();
    }
}

#[test]
fn weighted_count_workers_preserve_projected_dictionary_domains_and_actual_evidence() {
    use vortex::array::arrays::Dict;
    use vortex::array::scalar_fn::fns::get_item::GetItem;

    let projected = |first: &str, second: &str, codes: Vec<u8>| {
        let mut values = vec![first.to_owned(), second.to_owned()];
        values.extend((0..62).map(|index| format!("unreferenced{index}")));
        let inner = chunk(
            DictArray::try_new(
                PrimitiveArray::new(codes, Validity::NonNullable).into_array(),
                VarBinViewArray::from_iter_str(values.iter().map(String::as_str)).into_array(),
            )
            .unwrap()
            .into_array(),
        );
        // Construct the same native projection node the scan may return, without
        // `apply_bound` eagerly simplifying a directly available Struct child.
        let field = GetItem::try_new(inner, "tag").unwrap().into_array();
        assert!(
            !field.is::<Dict>(),
            "fixture must exercise the projected wrapper"
        );
        chunk(field)
    };
    let first = projected("alpha", "東京", vec![0, 1, 0]);
    let second = projected("東京", "alpha", vec![0, 1, 0, 1, 1]);
    let workspace = Workspace::new();
    let (_runtime, memory, mut accumulator, mut workers, cancellation) =
        setup(&workspace, &first, 2, usize::MAX, 64 << 20);
    assert!(workers.submit(&first).unwrap());
    assert!(workers.submit(&second).unwrap());
    workers.drain().unwrap();
    assert_eq!(workers.dictionaries, 2);
    let (rows, groups) = workers.fitted_rows_and_groups().unwrap();
    assert_eq!((rows, groups), (8, 2));
    workers.record(&mut accumulator).unwrap();
    let result = accumulator
        .finish_fitted(rows, groups, |visit| workers.select_fitted(visit))
        .unwrap();
    drop(workers);
    assert_eq!(
        collected(&result),
        vec![("alpha".into(), 5), ("東京".into(), 3)]
    );
    assert_eq!(result.source_work.dictionary_batches, 2);
    assert_eq!(result.source_work.worker_jobs, 2);
    assert!(result.source_work.fitted_partition_selection);
    assert_eq!(result.result.evidence.runs_written, 0);
    assert!(!cancellation.load(Ordering::Acquire));
    drop(result);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    workspace.empty();
}

#[test]
fn weighted_count_workers_fitted_partition_topk_skips_runs_even_beyond_run_buffer_capacity() {
    let workspace = Workspace::new();
    let values: Vec<String> = (0..2500)
        .map(|index| format!("key{index:06}{}", "x".repeat(512)))
        .collect();
    let source = plain(&values);
    let (_runtime, memory, mut accumulator, mut workers, cancellation) =
        setup(&workspace, &source, 4, usize::MAX, 64 << 20);
    assert!(workers.submit(&source).unwrap());
    let later = vec!["winner".to_owned(); 200];
    workers.before_next().unwrap();
    assert!(workers.submit(&plain(&later)).unwrap());
    workers.drain().unwrap();
    assert!(!workers.pressured());
    let (rows, groups) = workers.fitted_rows_and_groups().unwrap();
    workers.record(&mut accumulator).unwrap();
    let result = accumulator
        .finish_fitted(rows, groups, |visit| workers.select_fitted(visit))
        .unwrap();
    drop(workers);
    assert_eq!(collected(&result), oracle(values.into_iter().chain(later)));
    assert!(result.source_work.fitted_partition_selection);
    assert_eq!(result.result.evidence.runs_written, 0);
    assert_eq!(result.result.evidence.source_text_bytes_copied, 0);
    assert_eq!(result.result.evidence.groups, 2501);
    assert!(!cancellation.load(Ordering::Acquire));
    workspace.empty();
    drop(result);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn weighted_count_workers_handoff_writes_exact_native_runs_and_preserves_failure_cleanup() {
    let values: Vec<String> = (0..6000)
        .map(|index| format!("key{index:06}{}", "x".repeat(512)))
        .collect();
    let source = plain(&values);
    for failure in ["none", "quota", "cancel"] {
        let workspace = Workspace::new();
        let quota = if failure == "quota" {
            32 << 10
        } else {
            64 << 20
        };
        let (runtime, memory, mut accumulator, mut workers, cancellation) =
            setup(&workspace, &source, 2, 1, quota);
        assert!(workers.submit(&source).unwrap());
        workers.stop_for_transfer().unwrap();
        if failure == "cancel" {
            cancellation.store(true, Ordering::Release);
        }
        let transfer = workers.transfer(&mut accumulator, &runtime);
        if failure == "none" {
            transfer.unwrap();
            let result = accumulator.finish(&runtime).unwrap();
            assert_eq!(collected(&result), oracle(values.clone()));
            assert_eq!(result.result.evidence.source_weight, 6000);
            assert!(result.result.evidence.runs_written >= 2);
            assert_eq!(
                result.result.evidence.runs_written,
                result.result.evidence.runs_validated
            );
            assert!(result.result.evidence.peak_reserved_bytes <= 8 << 20);
            drop(result);
        } else {
            assert!(transfer.is_err());
            assert!(accumulator.finish(&runtime).is_err());
        }
        workspace.empty();
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn weighted_count_workers_atomic_initial_denial_keeps_current_chunk_untouched() {
    let workspace = Workspace::new();
    let values = vec!["a".to_owned(), "b".to_owned(), "a".to_owned()];
    let source = plain(&values);
    let (runtime, memory, mut accumulator, mut workers, cancellation) =
        setup(&workspace, &source, 2, usize::MAX, 64 << 20);
    workers.deny_next_initial = true;
    assert!(!workers.submit(&source).unwrap());
    assert_eq!(workers.jobs.submitted(), 0);
    workers.stop_for_transfer().unwrap();
    workers.transfer(&mut accumulator, &runtime).unwrap();
    accumulator.push_source(&source, &runtime).unwrap();
    let result = accumulator.finish(&runtime).unwrap();
    assert_eq!(collected(&result), oracle(values));
    assert_eq!(result.result.evidence.source_weight, 3);
    assert_eq!(
        result.source_work.committed_weight + result.source_work.deferred_weight,
        0
    );
    assert!(!cancellation.load(Ordering::Acquire));
    drop(result);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    workspace.empty();
}

#[cfg(target_pointer_width = "64")]
#[test]
fn weighted_count_workers_constant_weight_overflow_is_terminal_without_large_allocation() {
    let workspace = Workspace::new();
    let source = chunk(ConstantArray::new("same", usize::MAX).into_array());
    let (runtime, memory, accumulator, mut workers, _) =
        setup(&workspace, &source, 1, usize::MAX, 64 << 20);
    assert!(workers.submit(&source).unwrap());
    workers.drain().unwrap();
    let error = workers
        .submit(&chunk(ConstantArray::new("same", 1).into_array()))
        .unwrap_err();
    assert!(error.to_string().contains("overflow"));
    drop(workers);
    assert!(accumulator.finish(&runtime).is_err());
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    workspace.empty();
}
