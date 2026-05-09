use shardloom_exec::{
    AdaptiveSizingPolicy, BackpressurePlanInput, BoundedMemoryPolicy, ByteSize,
    DynamicSizingFeedbackInput, DynamicSizingFeedbackStatus, DynamicWorkShapingReport,
    DynamicWorkShapingStatus, SizingFeedbackSignal, SizingFeedbackSignalKind, plan_backpressure,
    plan_dynamic_sizing_feedback, plan_dynamic_work_shaping,
};

fn feedback(kind: SizingFeedbackSignalKind) -> shardloom_exec::DynamicSizingFeedbackReport {
    let mut input = DynamicSizingFeedbackInput::new(AdaptiveSizingPolicy::memory_limited(
        ByteSize::from_gib(8),
    ));
    input.add_signal(SizingFeedbackSignal::new(kind, kind.as_str()));
    plan_dynamic_sizing_feedback(input)
}

fn backpressure() -> shardloom_exec::BackpressurePlanReport {
    plan_backpressure(
        BackpressurePlanInput::new(
            BoundedMemoryPolicy::required(ByteSize::from_gib(8)).with_spill(true),
            4,
        )
        .expect("backpressure input")
        .with_estimated_chunk_bytes(ByteSize::from_mib(256)),
    )
    .expect("backpressure report")
}

#[test]
fn dynamic_work_shaping_is_report_only() {
    let feedback = feedback(SizingFeedbackSignalKind::MemoryPressureHigh);
    let backpressure = backpressure();
    let report = plan_dynamic_work_shaping("memory-pressure", &feedback, &backpressure);

    assert_eq!(
        report.status,
        DynamicWorkShapingStatus::NeedsRuntimeIntegration
    );
    assert_eq!(
        report.feedback_status,
        DynamicSizingFeedbackStatus::TargetReduced
    );
    assert!(report.target_task_bytes_changed);
    assert!(report.bounded_backpressure);
    assert_eq!(report.max_parallelism, 4);
    assert!(!report.runtime_feedback_loop_ready);
    assert!(!report.policy_application_ready);
    assert!(!report.benchmark_evidence_ready);
    assert!(!report.streams_executed);
    assert!(!report.tasks_executed);
    assert!(!report.feedback_applied);
    assert!(!report.policy_mutated);
    assert!(!report.data_read);
    assert!(!report.data_materialized);
    assert!(!report.object_store_io);
    assert!(!report.write_io);
    assert!(!report.spill_io_performed);
    assert!(!report.fallback_execution_allowed);
    assert!(!report.fallback_attempted);
    assert!(report.is_side_effect_free());
}

#[test]
fn dynamic_work_shaping_surfaces_blockers() {
    let feedback = feedback(SizingFeedbackSignalKind::ObjectStoreThrottled);
    let backpressure = backpressure();
    let report = plan_dynamic_work_shaping("object-store-throttled", &feedback, &backpressure);

    assert_eq!(
        DynamicWorkShapingReport::surface_order(),
        vec![
            "adaptive_sizing_policy",
            "feedback_signals",
            "target_task_policy",
            "backpressure_policy",
            "bounded_memory_policy",
            "scheduler_queue_policy",
            "runtime_application_loop",
            "benchmark_evidence",
            "no_fallback_policy"
        ]
    );
    assert_eq!(
        report.blocked_surface_order,
        vec!["runtime_application_loop", "benchmark_evidence"]
    );
    assert_eq!(report.blocked_surface_count, 2);
    assert_eq!(report.planned_surface_count, 7);
}
