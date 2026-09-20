//! Physical-key admission must preserve every dependent value and its errors.

use super::*;
use crate::{
    VortexAggregateExpression, VortexAggregateOrderExpr, VortexSimpleAggregateMeasure,
    VortexSimpleAggregateRequest,
};
use shardloom_core::{ColumnRef, DatasetUri};
use vortex::{
    VortexSessionDefault as _,
    array::{
        IntoArray as _,
        arrays::{ConstantArray, DictArray, StructArray},
        dtype::FieldNames,
        memory::MemorySessionExt as _,
    },
};

fn request(offsets: &[i64]) -> VortexSimpleAggregateRequest {
    VortexSimpleAggregateRequest::grouped(
        vec![ColumnRef::new("renamed_number").unwrap()],
        vec![VortexSimpleAggregateMeasure::new(
            "count",
            None,
            "occurrences".into(),
        )],
    )
    .with_group_expressions(
        offsets
            .iter()
            .enumerate()
            .map(|(index, &offset)| {
                VortexAggregateExpression::new(
                    format!("shifted_{index}"),
                    ColumnRef::new("renamed_number").unwrap(),
                    "add_offset",
                )
                .with_argument_offset(offset)
            })
            .collect(),
    )
    .with_order_by(vec![VortexAggregateOrderExpr::new("occurrences", true)])
}

fn chunk(values: ArrayRef) -> ArrayRef {
    let rows = values.len();
    StructArray::try_new(
        FieldNames::from(["renamed_number"]),
        vec![values],
        rows,
        Validity::NonNullable,
    )
    .unwrap()
    .into_array()
}

fn execute(
    chunks: &[ArrayRef],
    request: &VortexSimpleAggregateRequest,
    limit: usize,
    workers: usize,
) -> Result<serde_json::Value> {
    let columns = vec!["renamed_number".to_owned()];
    let mut states =
        GroupedAggregateStates::new(request, Some(limit), &columns, false, false).unwrap();
    assert_eq!(states.group_key_indices.len(), 1);
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    let session = VortexSession::default().with_allocator(Arc::new(
        crate::owned_buffers::ReservedHostAllocator::new(memory.clone()),
    ));
    let mut jobs = CountWorkers::admit(
        &states,
        chunks[0].dtype(),
        &columns,
        VortexLocalPrimitiveExecutionPolicy::new(workers).unwrap(),
        &session,
        &memory,
    )?
    .expect("one exact physical integer key must admit workers");
    let result = (|| {
        for chunk in chunks {
            jobs.before_next(&mut states)?;
            if !jobs.submit(chunk, &mut states)? {
                assert!(states.update_compact_direct_from_chunk(chunk, &columns, None)?);
            }
        }
        jobs.finish(&mut states)?;
        let (_, mut summary) = states.result_row_count_and_summary(Some(limit))?;
        jobs.annotate_summary(&mut summary)?;
        let summary: serde_json::Value = serde_json::from_str(&summary).unwrap();
        assert_eq!(summary["aggregate_workers_outstanding_chunks"], 0);
        assert!(
            summary["aggregate_workers_shared_live_peak_bytes"]
                .as_u64()
                .unwrap()
                <= 1 << 20
        );
        Ok(summary)
    })();
    drop((jobs, session));
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    result
}

#[test]
fn physical_integer_key_workers_keep_i32_and_reconstruct_all_dependent_outputs() {
    let chunks = vec![
        chunk(
            PrimitiveArray::new(
                vec![i32::MIN, 7, 7, 10, 10, i32::MAX],
                Validity::NonNullable,
            )
            .into_array(),
        ),
        chunk(ConstantArray::new(7_i32, 3).into_array()),
        chunk(
            DictArray::try_new(
                PrimitiveArray::new(vec![0_u8, 1, 0], Validity::NonNullable).into_array(),
                PrimitiveArray::new(vec![10_i32, i32::MIN], Validity::NonNullable).into_array(),
            )
            .unwrap()
            .into_array(),
        ),
        chunk(PrimitiveArray::new(Vec::<i32>::new(), Validity::NonNullable).into_array()),
    ];
    let request = request(&[-1, -2, -3]).with_offset(1);
    let public = VortexQueryPrimitiveRequest::simple_aggregate(
        DatasetUri::new("renamed.vortex").unwrap(),
        request.clone(),
    )
    .with_source_order_limit(2);
    assert!(request_may_be_admitted(&public));
    for workers in [1, 2, 4, 8, 12] {
        let result = execute(&chunks, &request, 2, workers).unwrap();
        assert_eq!(
            result["values"],
            serde_json::json!([
                {"renamed_number":10, "shifted_0":9, "shifted_1":8, "shifted_2":7, "occurrences":4},
                {"renamed_number":i32::MIN, "shifted_0":i64::from(i32::MIN)-1, "shifted_1":i64::from(i32::MIN)-2, "shifted_2":i64::from(i32::MIN)-3, "occurrences":2},
            ])
        );
        assert_eq!(result["candidate_groups"], 4);
        assert_eq!(result["aggregate_workers_integer_dictionary_handoff"], true);
        assert_eq!(result["aggregate_workers_submitted_chunks"], 3);
        assert_eq!(result["aggregate_workers_rows"], 9);
        assert_eq!(result["aggregate_workers_native_constant_chunks"], 1);
    }
}

#[test]
fn physical_integer_key_workers_preserve_complete_ties_across_partials() {
    let chunks = (1..=5_i32)
        .map(|value| {
            chunk(
                PrimitiveArray::new(vec![value, value, value, 0, 0], Validity::NonNullable)
                    .into_array(),
            )
        })
        .collect::<Vec<_>>();
    let request = request(&[17]).with_offset(1);
    for workers in [1, 4] {
        let result = execute(&chunks, &request, 2, workers).unwrap();
        assert_eq!(
            result["values"],
            serde_json::json!([
                {"renamed_number":1, "shifted_0":18, "occurrences":3},
                {"renamed_number":2, "shifted_0":19, "occurrences":3},
            ])
        );
        assert_eq!(result["candidate_groups"], 6);
        assert_eq!(
            result["group_output_strategy"],
            "complete_weighted_integer_partition_topk"
        );
        assert_eq!(
            result["aggregate_workers_integer_partition_selection_jobs"],
            64
        );
        assert_eq!(result["aggregate_workers_rows"], 25);
    }
}

#[test]
fn physical_integer_partition_limit_boundary_preserves_existing_count_results() {
    let chunks = [chunk(
        PrimitiveArray::new(vec![3_i64, 3, 9], Validity::NonNullable).into_array(),
    )];
    for workers in [1, 4] {
        for (offset, limit, partitioned) in [(1, 127, true), (1, 128, false), (0, 0, false)] {
            let result =
                execute(&chunks, &request(&[1]).with_offset(offset), limit, workers).unwrap();
            assert_eq!(
                result["aggregate_workers_integer_partition_selection_jobs"] == 64,
                partitioned
            );
            assert_eq!(
                result["values"],
                if limit == 0 {
                    serde_json::json!([])
                } else {
                    serde_json::json!([{"renamed_number": 9, "shifted_0": 10, "occurrences": 1}])
                }
            );
        }
    }
}

#[test]
fn physical_integer_key_workers_reject_overflow_even_in_a_discarded_group() {
    for workers in [1, 4] {
        let signed = [chunk(
            PrimitiveArray::new(vec![0_i64, 0, 0, i64::MAX], Validity::NonNullable).into_array(),
        )];
        let error = execute(&signed, &request(&[1]), 1, workers).unwrap_err();
        assert!(error.to_string().contains("overflowed int64"));
        let unsigned = [chunk(
            PrimitiveArray::new(vec![9_u64, 9, 9, 0], Validity::NonNullable).into_array(),
        )];
        let error = execute(&unsigned, &request(&[-1]), 1, workers).unwrap_err();
        assert!(error.to_string().contains("underflowed uint64"));
        // Numeric dictionary chunks retain the existing native path but still
        // must not hide a dependent error when the offending key loses top-K.
        let dictionary = [chunk(
            DictArray::try_new(
                PrimitiveArray::new(vec![0_u8, 0, 0, 1], Validity::NonNullable).into_array(),
                PrimitiveArray::new(vec![0_i64, i64::MAX], Validity::NonNullable).into_array(),
            )
            .unwrap()
            .into_array(),
        )];
        let error = execute(&dictionary, &request(&[1]), 1, workers).unwrap_err();
        assert!(error.to_string().contains("overflowed int64"));
    }
}

#[test]
fn physical_integer_key_workers_decline_missing_identity_nullable_and_float_sources() {
    let columns = vec!["renamed_number".to_owned()];
    let request = request(&[-1]);
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    let session = VortexSession::default();
    for array in [
        PrimitiveArray::new(vec![1_i32, 2], Validity::AllValid).into_array(),
        PrimitiveArray::new(vec![1_f64, 2.0], Validity::NonNullable).into_array(),
    ] {
        let chunk = chunk(array);
        let states =
            GroupedAggregateStates::new(&request, Some(1), &columns, false, false).unwrap();
        assert!(
            CountWorkers::admit(
                &states,
                chunk.dtype(),
                &columns,
                VortexLocalPrimitiveExecutionPolicy::new(4).unwrap(),
                &session,
                &memory,
            )
            .unwrap()
            .is_none()
        );
    }
    let mut expression_only = request.clone();
    expression_only.group_by.clear();
    let states =
        GroupedAggregateStates::new(&expression_only, Some(1), &columns, false, false).unwrap();
    assert!(!numeric_state_admitted(&states));
    let public = VortexQueryPrimitiveRequest::simple_aggregate(
        DatasetUri::new("renamed.vortex").unwrap(),
        expression_only,
    )
    .with_source_order_limit(1);
    assert!(!request_may_be_admitted(&public));
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn physical_i32_count_partial_owns_exact_native_width_without_input_widening() {
    use super::super::aggregate_chunk_jobs::AggregateChunkJobs;
    let memory = LiveMemoryPool::new(4096).unwrap();
    let mut jobs = AggregateChunkJobs::new(1, 1, 4096, memory.clone()).unwrap();
    let values = [i32::MIN, i32::MAX, -7, -7];
    jobs.submit(
        numeric_count_partial::partial_bytes::<i32>(values.len()).unwrap(),
        move |worker, lease| numeric_count_partial::count_numeric_values(&values, worker, lease),
    )
    .unwrap();
    let completed = jobs.join_next().unwrap().unwrap();
    assert_eq!(
        completed.value().pairs(),
        &[(i32::MIN, 1), (-7, 2), (i32::MAX, 1)]
    );
    let retained = completed
        .consume(|partial| Ok(partial.reserved_bytes()))
        .unwrap();
    assert_eq!(
        retained,
        numeric_count_partial::partial_bytes::<i32>(4).unwrap()
    );
    drop(jobs);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[cfg(all(feature = "vortex-write", unix))]
#[test]
fn physical_integer_key_workers_are_dispatched_by_the_native_file_query_route() {
    use vortex::{
        file::WriteOptionsSessionExt as _,
        io::{
            runtime::{BlockingRuntime as _, current::CurrentThreadRuntime},
            session::RuntimeSessionExt as _,
        },
        layout::layouts::flat::writer::FlatLayoutStrategy,
    };
    struct File(std::path::PathBuf);
    impl Drop for File {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }
    let file = File(std::env::temp_dir().join(format!(
        "shardloom-physical-key-worker-{}-{}.vortex",
        std::process::id(),
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos(),
    )));
    let runtime = CurrentThreadRuntime::new();
    let session = VortexSession::default().with_handle(runtime.handle());
    let array =
        chunk(PrimitiveArray::new(vec![9_i32, 9, 9, 4, 4, -1], Validity::NonNullable).into_array());
    session
        .write_options()
        .with_strategy(Arc::new(FlatLayoutStrategy::default()))
        .blocking(&runtime)
        .write(
            std::fs::File::create(&file.0).unwrap(),
            array.to_array_iterator(),
        )
        .unwrap();
    let request = VortexQueryPrimitiveRequest::simple_aggregate(
        DatasetUri::new(file.0.display().to_string()).unwrap(),
        request(&[-1, -2, -3]),
    )
    .with_source_order_limit(2);
    for workers in [1, 4] {
        let report = super::super::execute_vortex_local_primitive_with_policy(
            &request,
            VortexLocalPrimitiveExecutionPolicy::new(workers).unwrap(),
        )
        .unwrap();
        let (_, payload) = report
            .result_summary
            .as_deref()
            .unwrap()
            .rsplit_once(" values=")
            .expect("simple aggregate values payload");
        let result: serde_json::Value = serde_json::from_str(payload).unwrap();
        assert_eq!(
            result["values"],
            serde_json::json!([
                {"renamed_number":9,"shifted_0":8,"shifted_1":7,"shifted_2":6,"occurrences":3},
                {"renamed_number":4,"shifted_0":3,"shifted_1":2,"shifted_2":1,"occurrences":2},
            ])
        );
        assert!(
            result["aggregate_workers_submitted_chunks"]
                .as_u64()
                .unwrap()
                > 0
        );
        assert_eq!(result["aggregate_workers_rows"], 6);
        assert_eq!(result["candidate_groups"], 3);
        assert_eq!(
            result["group_output_strategy"],
            "complete_weighted_integer_partition_topk"
        );
        assert_eq!(result["aggregate_workers_outstanding_chunks"], 0);
        assert!(!report.fallback_execution_allowed);
    }
}
