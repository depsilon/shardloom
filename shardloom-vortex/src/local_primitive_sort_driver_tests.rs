use super::*;
use crate::local_primitives::prepared_dispatch::{
    ExecutedVortexSort, execute_sort_native_result, for_each_sort_batch, prepare_source_in_session,
};
use crate::resident_session::{ConfigurableResidentSession, NativeExecutionContext};
use shardloom_exec::compute_pool::bounded_cpu_parallelism;
use vortex::array::VortexSessionExecute as _;

const RESULT_ROWS: usize = 65;
const CPU_SCOPE: &str = "same_prepared_source;admitted_grant;temporary_drivers_joined_before_return;no_concurrent_sort_worker_pool;not_measured_utilization_or_io_threads";

fn resources(lanes: usize) -> ExecutionResources {
    ExecutionResources::from_bytes(MEMORY_BYTES, lanes, ExecutionResourceOrigin::ExecutionCall)
        .unwrap()
}

fn assert_provider_can_run_while_caller_waits(context: &NativeExecutionContext<'_>, lanes: usize) {
    assert_eq!(context.cpu_lanes(), bounded_cpu_parallelism(lanes));
    if context.cpu_lanes() == 1 {
        return;
    }
    let caller = std::thread::current().id();
    let (send, receive) = std::sync::mpsc::channel();
    context
        .runtime()
        .handle()
        .spawn(async move {
            let thread = std::thread::current();
            send.send((thread.id(), thread.name().unwrap_or_default().to_owned()))
                .unwrap();
        })
        .detach();
    let (thread, name) = receive
        .recv_timeout(std::time::Duration::from_secs(5))
        .expect("prepared sort must drive native tasks while its caller is blocked");
    assert_ne!(thread, caller);
    assert!(name.starts_with("shardloom-resident-driver-"), "{name}");
}

fn assert_batch_rows(array: &ArrayRef, context: &NativeExecutionContext<'_>, seen: &mut usize) {
    let names = array.dtype().as_struct_fields_opt().unwrap().names();
    let columns = names
        .iter()
        .map(|name| {
            crate::local_primitives::logical_field_from_native_array(array, name.as_ref()).unwrap()
        })
        .collect::<Vec<_>>();
    let mut execution = context.native_session().create_execution_ctx();
    for row in 0..array.len() {
        let values = names
            .iter()
            .zip(&columns)
            .map(|(name, column)| {
                (
                    name.to_string(),
                    scalar_json(&column.execute_scalar(row, &mut execution).unwrap()),
                )
            })
            .collect::<serde_json::Map<_, _>>();
        assert_eq!(
            serde_json::Value::Object(values),
            expected_sorted_row(*seen)
        );
        *seen += 1;
    }
}

fn assert_execution(execution: &ExecutedVortexSort, lanes: usize, completed: u64) {
    assert_native_certificate(&execution.report, &execution.native_io_certificate);
    let drivers = bounded_cpu_parallelism(lanes) - 1;
    assert_eq!(execution.runtime.provider_background_workers, drivers);
    assert_eq!(
        execution.runtime.admitted_max_parallelism,
        bounded_cpu_parallelism(lanes)
    );
    assert_eq!(execution.runtime.prepared_source_opens, 1);
    assert_eq!(execution.runtime.completed_executions, completed);
    let summary = execution.report.result_summary.as_ref().unwrap();
    let (_, values) = summary.split_once(" values=").unwrap();
    let values: serde_json::Value = serde_json::from_str(values).unwrap();
    assert_eq!(values["sort_provider_background_workers"], drivers);
    assert_eq!(values["sort_provider_cpu_scope"], CPU_SCOPE);
}

#[test]
fn configurable_prepared_sort_drives_native_tasks_under_each_operation_grant() {
    let fixture = Fixture::new(CHUNK_ROWS + 1);
    let controller = ConfigurableResidentSession::new(resources(2)).unwrap();
    let request = sort_request(fixture.uri(), RESULT_ROWS);
    let cancellation = CancellationToken::default();
    let (source, memory) = controller
        .with_resources(resources(2), &cancellation, |session, admitted| {
            let policy = VortexLocalPrimitiveExecutionPolicy::from_resources(admitted)?;
            Ok((
                prepare_source_in_session(&request, policy, session, &cancellation)?,
                session.shared_memory_pool(),
            ))
        })
        .unwrap();
    let mut completed = 0;
    for lanes in [2, 1, 2, 1] {
        controller
            .with_resources(resources(lanes), &cancellation, |session, admitted| {
                let policy = VortexLocalPrimitiveExecutionPolicy::from_resources(admitted)?;
                let mut seen = 0;
                let streamed = for_each_sort_batch(
                    &request,
                    policy,
                    &source,
                    &cancellation,
                    |array, context| {
                        if seen == 0 {
                            assert_provider_can_run_while_caller_waits(context, lanes);
                        }
                        assert_eq!(array.dtype(), &fixture.dtype);
                        assert_batch_rows(&array, context, &mut seen);
                        Ok(())
                    },
                )?;
                completed += 1;
                assert_eq!(seen, RESULT_ROWS);
                assert_execution(&streamed, lanes, completed);
                assert_eq!(session.snapshot().provider_background_workers, 0);

                let retained =
                    execute_sort_native_result(&request, policy, &source, &cancellation)?;
                completed += 1;
                assert_execution(&retained.execution, lanes, completed);
                assert_every_row(
                    &retained.result,
                    &fixture.dtype,
                    RESULT_ROWS,
                    expected_sorted_row,
                );
                assert_eq!(session.snapshot().provider_background_workers, 0);
                Ok(())
            })
            .unwrap();
    }
    assert_eq!(controller.snapshot().prepared_source_opens, 1);
    assert_eq!(controller.snapshot().completed_executions, 8);
    drop(source);
    drop(controller);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn configurable_sort_consumer_errors_and_cancellation_release_ownership_and_allow_retry() {
    let fixture = Fixture::new(CHUNK_ROWS + 1);
    let controller = ConfigurableResidentSession::new(resources(2)).unwrap();
    let request = sort_request(fixture.uri(), RESULT_ROWS);
    let (source, memory) = controller
        .with_resources(
            resources(2),
            &CancellationToken::default(),
            |session, admitted| {
                let policy = VortexLocalPrimitiveExecutionPolicy::from_resources(admitted)?;
                Ok((
                    prepare_source_in_session(
                        &request,
                        policy,
                        session,
                        &CancellationToken::default(),
                    )?,
                    session.shared_memory_pool(),
                ))
            },
        )
        .unwrap();
    for cancel in [false, true] {
        let cancellation = CancellationToken::default();
        controller
            .with_resources(resources(2), &cancellation, |session, admitted| {
                let policy = VortexLocalPrimitiveExecutionPolicy::from_resources(admitted)?;
                let before = session.snapshot();
                let mut callbacks = 0;
                let failed =
                    for_each_sort_batch(&request, policy, &source, &cancellation, |_, context| {
                        callbacks += 1;
                        assert_provider_can_run_while_caller_waits(context, 2);
                        if cancel {
                            cancellation.cancel();
                            context.check_cancelled()
                        } else {
                            Err(shardloom_core::ShardLoomError::InvalidOperation(
                                "injected sort consumer failure".into(),
                            ))
                        }
                    });
                let Err(error) = failed else {
                    panic!("consumer failure must be terminal")
                };
                let expected = if cancel {
                    "cancel"
                } else {
                    "injected sort consumer failure"
                };
                assert!(error.to_string().contains(expected), "{error}");
                assert_eq!(callbacks, 1);
                let after = session.snapshot();
                assert_eq!(after.completed_executions, before.completed_executions);
                assert_eq!(after.prepared_source_opens, 1);
                assert_eq!(after.provider_background_workers, 0);
                assert_eq!(after.memory.reserved_bytes, before.memory.reserved_bytes);
                Ok(())
            })
            .unwrap();
        let retry_token = CancellationToken::default();
        controller
            .with_resources(resources(2), &retry_token, |session, admitted| {
                let policy = VortexLocalPrimitiveExecutionPolicy::from_resources(admitted)?;
                let before = session.snapshot().completed_executions;
                let result = execute_sort_native_result(&request, policy, &source, &retry_token)?;
                assert_execution(&result.execution, 2, before + 1);
                assert_every_row(
                    &result.result,
                    &fixture.dtype,
                    RESULT_ROWS,
                    expected_sorted_row,
                );
                Ok(())
            })
            .unwrap();
    }
    drop(source);
    drop(controller);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}
