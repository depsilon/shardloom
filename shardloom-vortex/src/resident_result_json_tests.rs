use crate::{VortexQueryPrimitiveRequest, local_primitives::collect::prepare_rows_in_session};
use shardloom_core::{ColumnRef, DatasetUri};
use shardloom_plan::ProjectionRequest;

fn prepared() -> (
    crate::resident_session::ResidentVortexSession,
    crate::local_primitives::collect::PreparedVortexCollect,
) {
    let source = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/local_primitive_struct_five.vortex");
    let session = crate::resident_session::ResidentVortexSession::new(8 << 20, 1).unwrap();
    let request = VortexQueryPrimitiveRequest::project(
        DatasetUri::new(source.display().to_string()).unwrap(),
        ProjectionRequest::columns(vec![
            ColumnRef::new("metric").unwrap(),
            ColumnRef::new("value").unwrap(),
        ]),
    )
    .with_source_order_limit(5);
    let prepared = prepare_rows_in_session(&request, &session).unwrap();
    (session, prepared)
}

#[test]
fn retained_result_json_materializes_after_prepared_source_and_session_drop() {
    let (session, prepared) = prepared();
    let memory = session.memory().clone();
    let arrays = prepared.execute_arrays().unwrap();
    assert_eq!(session.snapshot().completed_executions, 1);
    drop(prepared);
    drop(session);
    let values = arrays
        .to_bounded_json(&["metric".into(), "value".into()], 4096)
        .unwrap();
    let actual: serde_json::Value = serde_json::from_str(values.value()).unwrap();
    let expected = (1..=5)
        .map(|value| serde_json::json!({"value":value,"metric":value*10}))
        .collect::<Vec<_>>();
    assert_eq!(actual, serde_json::json!(expected));
    assert!(memory.snapshot().reserved_bytes > 0);
    drop(arrays);
    assert!(memory.snapshot().reserved_bytes >= u64::try_from(values.value().capacity()).unwrap());
    drop(values);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn retained_result_json_rejects_invalid_fields_and_bound_then_remains_usable() {
    let (session, prepared) = prepared();
    let arrays = prepared.execute_arrays().unwrap();
    let before = session.snapshot().memory.reserved_bytes;
    for names in [
        vec![],
        vec![String::new()],
        vec!["value".into(), "value".into()],
        vec!["absent".into()],
    ] {
        assert!(arrays.to_bounded_json(&names, 4096).is_err());
        assert_eq!(session.snapshot().memory.reserved_bytes, before);
    }
    for limit in [0, 1, 8 * 1024 * 1024 + 1] {
        assert!(arrays.to_bounded_json(&["value".into()], limit).is_err());
        assert_eq!(session.snapshot().memory.reserved_bytes, before);
    }
    let output = arrays.to_bounded_json(&["value".into()], 4096).unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(output.value()).unwrap(),
        serde_json::json!([{"value":1},{"value":2},{"value":3},{"value":4},{"value":5}])
    );
    assert_eq!(
        session.snapshot().completed_executions,
        1,
        "sink does not rerun the query"
    );
    drop(output);
    drop(arrays);
    drop(prepared);
    assert_eq!(session.snapshot().memory.reserved_bytes, 0);
}

#[test]
fn retained_result_json_and_prepared_sink_share_admission_after_projection() {
    use std::{
        sync::{Arc, mpsc},
        time::Duration,
    };
    let (session, prepared) = prepared();
    let arrays = prepared.execute_arrays().unwrap();
    let runtime = Arc::clone(&arrays.runtime);
    let (scan_ready, scan_reached) = mpsc::channel();
    let (resume, resumed) = mpsc::channel();
    let (completed, completion) = mpsc::channel();
    let prepared_done = completed.clone();
    let prepared_thread = std::thread::spawn(move || {
        let result = prepared.execute_with_after_scan(|| {
            scan_ready.send(()).unwrap();
            resumed.recv_timeout(Duration::from_secs(5)).unwrap();
        });
        prepared_done.send(()).unwrap();
        result
    });
    // The projection has completed: holding the gate now must stop the prepared
    // JSON stage itself, not merely block that operation's preceding scan.
    scan_reached.recv_timeout(Duration::from_secs(5)).unwrap();
    let admission = runtime.admission.lock().unwrap();
    resume.send(()).unwrap();
    let retained_thread = std::thread::spawn(move || {
        let result = arrays.to_bounded_json(&["metric".into(), "value".into()], 4096);
        completed.send(()).unwrap();
        result
    });
    let while_held = completion.recv_timeout(Duration::from_millis(50));
    drop(admission);
    // Join before asserting the blocked state so an assertion cannot leave a
    // worker waiting on a test-owned admission guard.
    let prepared_result = prepared_thread.join().unwrap().unwrap();
    let retained_result = retained_thread.join().unwrap().unwrap();
    assert!(matches!(while_held, Err(mpsc::RecvTimeoutError::Timeout)));
    let expected = (1..=5)
        .map(|value| serde_json::json!({"value":value,"metric":value*10}))
        .collect::<Vec<_>>();
    for json in [prepared_result.values_json.value(), retained_result.value()] {
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(json).unwrap(),
            serde_json::json!(expected)
        );
    }
    assert!(prepared_result.native_io_certificate.is_certified());
    assert_eq!(session.snapshot().completed_executions, 2);
    drop(prepared_result);
    drop(retained_result);
    assert_eq!(session.snapshot().memory.reserved_bytes, 0);
}
