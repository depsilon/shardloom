use super::*;
use crate::local_primitives::prepared_dispatch::{
    execute_sort_native_result, for_each_sort_batch, prepare_source_in_session,
};
use crate::resident_session::{ConfigurableResidentSession, NativeExecutionContext};
use vortex::array::VortexSessionExecute as _;

fn resources(bytes: u64) -> ExecutionResources {
    ExecutionResources::from_bytes(bytes, 2, ExecutionResourceOrigin::ExecutionCall).unwrap()
}

fn assert_sorted_batch(
    array: &ArrayRef,
    context: &NativeExecutionContext<'_>,
    dtype: &DType,
    seen: &mut usize,
) {
    assert_eq!(array.dtype(), dtype);
    assert!(array.len() <= 2048);
    let key = crate::local_primitives::logical_field_from_native_array(array, KEY).unwrap();
    let payload = crate::local_primitives::logical_field_from_native_array(array, PAYLOAD).unwrap();
    let mut execution = context.native_session().create_execution_ctx();
    for row in 0..array.len() {
        let expected = expected_sorted_row(*seen);
        assert_eq!(
            scalar_json(&key.execute_scalar(row, &mut execution).unwrap()),
            expected[KEY]
        );
        assert_eq!(
            scalar_json(&payload.execute_scalar(row, &mut execution).unwrap()),
            expected[PAYLOAD]
        );
        *seen += 1;
    }
}

#[test]
fn prepared_sort_keeps_output_batches_bounded_across_provider_layout_splits() {
    for rows in [0, 2048, 2049, 8193] {
        let fixture = Fixture::new(rows);
        let controller = ConfigurableResidentSession::new(resources(MEMORY_BYTES)).unwrap();
        let request = sort_request(fixture.uri(), rows.max(1));
        let cancellation = CancellationToken::default();
        let memory = controller
            .with_resources(
                resources(MEMORY_BYTES),
                &cancellation,
                |session, admitted| {
                    let policy = VortexLocalPrimitiveExecutionPolicy::from_resources(admitted)?;
                    let source =
                        prepare_source_in_session(&request, policy, session, &cancellation)?;
                    let mut seen = 0;
                    let mut batches = 0;
                    let execution = for_each_sort_batch(
                        &request,
                        policy,
                        &source,
                        &cancellation,
                        |array, context| {
                            assert_sorted_batch(&array, context, &fixture.dtype, &mut seen);
                            batches += 1;
                            Ok(())
                        },
                    )?;
                    assert_eq!(seen, rows);
                    assert_eq!(execution.report.rows_scanned, rows as u64);
                    assert_eq!(execution.report.max_chunk_rows, rows.min(CHUNK_ROWS));
                    if rows > 2048 {
                        assert!(batches > 1);
                        assert!(execution.report.max_chunk_rows > 2048);
                    }
                    assert_native_certificate(&execution.report, &execution.native_io_certificate);
                    let retained =
                        execute_sort_native_result(&request, policy, &source, &cancellation)?;
                    assert_every_row(&retained.result, &fixture.dtype, rows, expected_sorted_row);
                    assert_eq!(retained.execution.runtime.prepared_source_opens, 1);
                    assert_eq!(retained.execution.runtime.completed_executions, 2);
                    Ok(session.shared_memory_pool())
                },
            )
            .unwrap();
        drop(controller);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn prepared_sort_selects_input_splits_from_each_current_memory_grant() {
    let rows = 150_001;
    let fixture = Fixture::with_chunk_rows(rows, rows);
    let controller = ConfigurableResidentSession::new(resources(MEMORY_BYTES)).unwrap();
    let request = sort_request(fixture.uri(), 17);
    let cancellation = CancellationToken::default();
    let (source, memory) = controller
        .with_resources(
            resources(MEMORY_BYTES),
            &cancellation,
            |session, admitted| {
                let policy = VortexLocalPrimitiveExecutionPolicy::from_resources(admitted)?;
                Ok((
                    prepare_source_in_session(&request, policy, session, &cancellation)?,
                    session.shared_memory_pool(),
                ))
            },
        )
        .unwrap();
    for bytes in [MEMORY_BYTES, 8 << 20, MEMORY_BYTES, 8 << 20] {
        controller
            .with_resources(resources(bytes), &cancellation, |session, admitted| {
                let policy = VortexLocalPrimitiveExecutionPolicy::from_resources(admitted)?;
                let retained =
                    execute_sort_native_result(&request, policy, &source, &cancellation)?;
                assert_every_row(&retained.result, &fixture.dtype, 17, expected_sorted_row);
                assert_native_certificate(
                    &retained.execution.report,
                    &retained.execution.native_io_certificate,
                );
                let summary = retained.execution.report.result_summary.as_ref().unwrap();
                let (_, summary) = summary.split_once(" values=").unwrap();
                let summary: serde_json::Value = serde_json::from_str(summary).unwrap();
                if bytes == MEMORY_BYTES {
                    assert!(retained.execution.report.max_chunk_rows > 2048);
                    assert!(retained.execution.report.max_chunk_rows <= 100_000);
                    assert_eq!(
                        summary["sort_scan_split_policy"],
                        "vortex_layout_with_scratch_headroom"
                    );
                } else {
                    assert_eq!(retained.execution.report.max_chunk_rows, 2048);
                    assert_eq!(
                        summary["sort_scan_split_policy"],
                        "fixed_2048_for_scratch_headroom"
                    );
                }
                let observed = session.snapshot();
                assert_eq!(observed.prepared_source_opens, 1);
                assert_eq!(observed.memory.limit_bytes, bytes);
                assert!(observed.memory.allocation_peak_reserved_bytes <= bytes);
                Ok(())
            })
            .unwrap();
    }
    drop(source);
    drop(controller);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}
