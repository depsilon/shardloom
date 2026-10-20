use super::*;
use serde_json::Value;

fn parsed(capture: Capture, operation_succeeded: bool) -> Value {
    serde_json::from_str(&capture.finish(operation_succeeded)).expect("observation JSON")
}

fn callback(rows: usize, bytes: usize) {
    let mut sink = sink_scope(rows);
    converted(rows, bytes);
    sent();
    acknowledged();
    sink.complete();
}

#[test]
fn credit_window_trace_preserves_acknowledgments_after_earlier_callback_exit() {
    for detailed in [false, true] {
        let capture = Capture::begin_window(detailed, true).unwrap();
        let mut engine = engine_scope();
        window_policy(Some(4096), 1024);
        let mut first = sink_scope(2048);
        converted(2048, 100);
        sent();
        first.complete();
        drop(first);
        let mut second = sink_scope(2048);
        converted(2048, 200);
        sent();
        acknowledged_ordinal(0);
        acknowledged_ordinal(1);
        second.complete();
        drop(second);
        engine.complete();
        drop(engine);
        window_released();
        let report = parsed(capture, true);
        assert_eq!(report["valid"], true);
        assert_eq!(
            report["schema_version"],
            "shardloom.output_window_observation.v2"
        );
        assert_eq!(report["window"]["maximum_unacknowledged_batches"], 2);
        assert_eq!(
            report["window"]["maximum_unacknowledged_referenced_bytes"],
            300
        );
        assert_eq!(report["window"]["abandoned_batches"], 0);
        assert_eq!(report["window"]["metadata_final_bytes"], 0);
        if detailed {
            assert!(
                report["batches"][0]["callback_exit_ns"].as_u64().unwrap()
                    <= report["batches"][0]["acknowledged_ns"].as_u64().unwrap()
            );
            assert!(
                report["batches"][1]["send_complete_ns"].as_u64().unwrap()
                    <= report["batches"][0]["acknowledged_ns"].as_u64().unwrap()
            );
        }
    }
}

#[test]
fn credit_window_trace_distinguishes_ineligible_and_unrequested_ordinary_demand() {
    for requested in [false, true] {
        let capture = Capture::begin_window(true, requested).unwrap();
        let mut engine = engine_scope();
        window_policy(None, 0);
        for ordinal in 0..2 {
            let mut sink = sink_scope(17);
            converted(17, 136);
            sent();
            acknowledged_ordinal(ordinal);
            sink.complete();
        }
        engine.complete();
        drop(engine);
        window_released();
        let report = parsed(capture, true);
        assert_eq!(report["valid"], true);
        assert_eq!(
            report["window"]["selected_policy"],
            if requested {
                "ordinary_ineligible"
            } else {
                "ordinary_not_requested"
            }
        );
        assert_eq!(report["window"]["metadata_reserved_bytes"], 0);
        assert_eq!(report["window"]["maximum_unacknowledged_batches"], 1);
    }
}

#[test]
fn credit_window_trace_rejects_duplicate_and_out_of_order_acknowledgments() {
    for duplicate in [false, true] {
        let capture = Capture::begin_window(true, true).unwrap();
        let engine = engine_scope();
        window_policy(Some(34), 1024);
        let first = sink_scope(17);
        converted(17, 136);
        sent();
        drop(first);
        let second = sink_scope(17);
        converted(17, 136);
        sent();
        if !duplicate {
            acknowledged_ordinal(1);
        }
        acknowledged_ordinal(0);
        if duplicate {
            acknowledged_ordinal(0);
        }
        acknowledged_ordinal(1);
        drop(second);
        drop(engine);
        window_released();
        let report = parsed(capture, false);
        assert_eq!(report["valid"], false);
        assert_eq!(report["acknowledged_count"], 2);
        assert_eq!(report["window"]["outstanding_batches"], 0);
    }
}

#[test]
fn credit_window_trace_does_not_turn_abandonment_into_acknowledgment_or_success() {
    for success in [false, true] {
        let capture = Capture::begin_window(true, true).unwrap();
        let mut engine = engine_scope();
        window_policy(Some(17), 1024);
        let mut sink = sink_scope(17);
        converted(17, 136);
        sent();
        if success {
            sink.complete();
        }
        drop(sink);
        if success {
            engine.complete();
        }
        drop(engine);
        window_released();
        let report = parsed(capture, success);
        assert_eq!(report["valid"], !success);
        assert_eq!(report["acknowledged_count"], 0);
        assert!(report["batches"][0]["acknowledged_ns"].is_null());
        assert_eq!(report["window"]["abandoned_batches"], 1);
        assert_eq!(report["window"]["abandoned_referenced_bytes"], 136);
        assert_eq!(report["window"]["outstanding_batches"], 0);
        assert_eq!(report["window"]["metadata_final_bytes"], 0);
    }
}

#[test]
fn credit_window_trace_rejects_oversize_send_and_third_unacknowledged_batch() {
    for oversized in [false, true] {
        let capture = Capture::begin_window(true, true).unwrap();
        let engine = engine_scope();
        window_policy(Some(51), 1024);
        for _ in 0..if oversized { 1 } else { 3 } {
            let sink = sink_scope(17);
            converted(17, if oversized { (2 << 20) + 1 } else { 136 });
            sent();
            drop(sink);
        }
        drop(engine);
        window_released();
        assert_eq!(parsed(capture, false)["valid"], false);
    }
}

#[test]
fn credit_window_trace_requires_actual_metadata_and_release() {
    for release in [false, true] {
        let capture = Capture::begin_window(true, true).unwrap();
        let engine = engine_scope();
        window_policy(Some(17), if release { 1023 } else { 1024 });
        drop(engine);
        if release {
            window_released();
        }
        assert_eq!(parsed(capture, false)["valid"], false);
    }
}

fn completed(detailed: bool, callbacks: &[(usize, usize)]) -> Value {
    let capture = Capture::begin(detailed).expect("capture begins");
    let mut engine = engine_scope();
    for &(rows, bytes) in callbacks {
        callback(rows, bytes);
    }
    engine.complete();
    drop(engine);
    parsed(capture, true)
}

#[test]
fn trace_mode_records_complete_callbacks_and_disjoint_timing() {
    let report = completed(true, &[(17, 136), (3, 24)]);
    assert_eq!(report["valid"], true);
    assert_eq!(report["operation_succeeded"], true);
    assert_eq!(report["engine_succeeded"], true);
    assert_eq!(report["engine_calls"], 1);
    assert_eq!(report["batch_count"], 2);
    assert_eq!(report["output_rows"], 20);
    assert_eq!(report["ready_count"], 2);
    assert_eq!(report["sent_count"], 2);
    assert_eq!(report["acknowledged_count"], 2);
    assert_eq!(report["maximum_batch_referenced_arrow_bytes"], 136);

    let start = report["engine_start_ns"].as_u64().expect("engine start");
    let end = report["engine_end_ns"].as_u64().expect("engine end");
    let elapsed = report["native_call_elapsed_ns"].as_u64().expect("elapsed");
    let engine_ns = report["engine_execution_ns"].as_u64().expect("engine time");
    let sink_ns = report["synchronous_sink_ns"].as_u64().expect("sink time");
    assert!(start <= end);
    assert_eq!(elapsed, end - start);
    assert_eq!(elapsed, engine_ns + sink_ns);

    let batches = report["batches"].as_array().expect("trace records");
    assert_eq!(batches.len(), 2);
    let mut callback_sum = 0_u64;
    let mut previous_exit = start;
    for (record, (rows, bytes)) in batches.iter().zip([(17, 136), (3, 24)]) {
        assert_eq!(record["rows"], rows);
        assert_eq!(record["referenced_arrow_bytes"], bytes);
        assert_eq!(record["callback_succeeded"], true);
        let enter = record["callback_enter_ns"].as_u64().unwrap();
        let ready = record["converted_ready_ns"].as_u64().unwrap();
        let send = record["send_complete_ns"].as_u64().unwrap();
        let ack = record["acknowledged_ns"].as_u64().unwrap();
        let exit = record["callback_exit_ns"].as_u64().unwrap();
        assert!(
            previous_exit <= enter
                && enter <= ready
                && ready <= send
                && send <= ack
                && ack <= exit
                && exit <= end
        );
        previous_exit = exit;
        callback_sum += exit - enter;
    }
    assert_eq!(callback_sum, sink_ns);
}

#[test]
fn clock_mode_keeps_counts_without_batch_trace() {
    let report = completed(false, &[(17, 136), (3, 24)]);
    assert_eq!(report["valid"], true);
    assert_eq!(report["mode"], "clock");
    assert_eq!(report["batch_count"], 2);
    assert_eq!(report["output_rows"], 20);
    assert_eq!(report["ready_count"], 2);
    assert_eq!(report["sent_count"], 2);
    assert_eq!(report["acknowledged_count"], 2);
    assert_eq!(report["maximum_batch_referenced_arrow_bytes"], 136);
    assert_eq!(report["batches"].as_array().unwrap().len(), 0);
}

#[test]
fn nested_capture_is_rejected_without_corrupting_outer_capture() {
    let outer = Capture::begin(true).expect("outer capture");
    assert!(Capture::begin(false).is_err());
    let mut engine = engine_scope();
    callback(1, 8);
    engine.complete();
    drop(engine);
    assert_eq!(parsed(outer, true)["valid"], true);

    let later = Capture::begin(false).expect("capture after outer drop");
    let mut engine = engine_scope();
    callback(2, 16);
    engine.complete();
    drop(engine);
    assert_eq!(parsed(later, true)["valid"], true);
}

#[test]
fn nested_engine_nested_sink_and_callback_outside_engine_invalidate_capture() {
    let capture = Capture::begin(true).unwrap();
    let mut engine = engine_scope();
    let mut nested_engine = engine_scope();
    nested_engine.complete();
    drop(nested_engine);
    callback(1, 8);
    engine.complete();
    drop(engine);
    assert_eq!(parsed(capture, true)["valid"], false);

    let capture = Capture::begin(true).unwrap();
    let mut engine = engine_scope();
    let mut sink = sink_scope(1);
    let nested_sink = sink_scope(1);
    drop(nested_sink);
    converted(1, 8);
    sent();
    acknowledged();
    sink.complete();
    drop(sink);
    engine.complete();
    drop(engine);
    assert_eq!(parsed(capture, true)["valid"], false);

    let capture = Capture::begin(true).unwrap();
    callback(1, 8);
    assert_eq!(parsed(capture, true)["valid"], false);
}

#[test]
fn missing_or_out_of_order_marks_and_wrong_cardinality_invalidate() {
    for missing_stage in 1..=3 {
        let capture = Capture::begin(true).unwrap();
        let mut engine = engine_scope();
        let mut sink = sink_scope(2);
        if missing_stage != 1 {
            converted(2, 16);
        }
        if missing_stage != 2 {
            sent();
        }
        if missing_stage != 3 {
            acknowledged();
        }
        sink.complete();
        drop(sink);
        engine.complete();
        drop(engine);
        assert_eq!(
            parsed(capture, true)["valid"],
            false,
            "missing stage {missing_stage}"
        );
    }

    let capture = Capture::begin(true).unwrap();
    let mut engine = engine_scope();
    let mut sink = sink_scope(1);
    sent();
    converted(1, 8);
    acknowledged();
    sink.complete();
    drop(sink);
    engine.complete();
    drop(engine);
    assert_eq!(parsed(capture, true)["valid"], false);

    let capture = Capture::begin(true).unwrap();
    let mut engine = engine_scope();
    let mut sink = sink_scope(2);
    converted(1, 16);
    sent();
    acknowledged();
    sink.complete();
    drop(sink);
    engine.complete();
    drop(engine);
    assert_eq!(parsed(capture, true)["valid"], false);
}

#[test]
fn callback_bound_invalidates_both_modes_and_trace_remains_bounded() {
    for detailed in [false, true] {
        let capture = Capture::begin(detailed).unwrap();
        let mut engine = engine_scope();
        for _ in 0..=MAX_BATCHES {
            callback(1, 8);
        }
        engine.complete();
        drop(engine);
        let report = parsed(capture, true);
        assert_eq!(report["valid"], false);
        assert_eq!(report["batch_count"], MAX_BATCHES + 1);
        assert!(report["batches"].as_array().unwrap().len() <= MAX_BATCHES);
    }
}

#[test]
fn aborted_callback_and_engine_are_reported_and_capture_can_restart() {
    let capture = Capture::begin(true).unwrap();
    {
        let _engine = engine_scope();
        let _sink = sink_scope(1);
        converted(1, 8);
        sent();
        // Dropping both scopes without complete models an early-return path.
    }
    let report = parsed(capture, false);
    assert_eq!(report["operation_succeeded"], false);
    assert_eq!(report["engine_succeeded"], false);
    assert_eq!(report["valid"], true);
    assert_eq!(report["batches"][0]["callback_succeeded"], false);

    let later = Capture::begin(false).expect("capture after aborted operation");
    let mut engine = engine_scope();
    callback(1, 8);
    engine.complete();
    drop(engine);
    assert_eq!(parsed(later, true)["valid"], true);
}

#[test]
fn worker_thread_capture_is_independent_of_parent_capture() {
    let parent = Capture::begin(true).unwrap();
    let child = std::thread::spawn(|| {
        let capture = Capture::begin(false).expect("thread-local capture");
        let mut engine = engine_scope();
        callback(3, 24);
        engine.complete();
        drop(engine);
        parsed(capture, true)
    })
    .join()
    .expect("worker capture completes");

    let mut engine = engine_scope();
    callback(2, 16);
    engine.complete();
    drop(engine);
    let parent = parsed(parent, true);
    assert_eq!(child["valid"], true);
    assert_eq!(child["output_rows"], 3);
    assert_eq!(parent["valid"], true);
    assert_eq!(parent["output_rows"], 2);
}

#[test]
fn observer_calls_without_active_capture_are_harmless() {
    let mut engine = engine_scope();
    callback(1, 8);
    engine.complete();
    drop(engine);
    converted(1, 8);
    sent();
    acknowledged();
    let capture = Capture::begin(false).expect("no leaked active capture");
    drop(capture);
}

#[test]
fn empty_native_calls_are_complete_without_invented_callbacks() {
    for detailed in [false, true] {
        let report = completed(detailed, &[]);
        assert_eq!(report["valid"], true);
        assert_eq!(report["operation_succeeded"], true);
        assert_eq!(report["output_rows"], 0);
        assert_eq!(report["batch_count"], 0);
        assert_eq!(report["synchronous_sink_ns"], 0);
        assert_eq!(
            report["native_call_elapsed_ns"],
            report["engine_execution_ns"]
        );
    }
}
