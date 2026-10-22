use super::*;
#[cfg(feature = "output-window-observation")]
use crate::output_window_observation as observation;
use crate::{
    VortexLocalPrimitiveExecutionPolicy,
    local_primitives::prepared_relational::prepare_relational_with_schema_in_session,
    relational_query::{
        VortexRelationalLimit, VortexRelationalPlan, VortexRelationalProject, VortexRelationalScan,
    },
    resident_memory_source::ResidentMemorySource,
    resident_session::ResidentVortexSession,
};
use arrow_array::{cast::AsArray as _, types::Int64Type};
use shardloom_core::{
    ColumnRef, DatasetUri, ExecutionResourceOrigin, ExecutionResources, ExprId, Expression,
    ExpressionKind,
};
use shardloom_exec::compute_pool::CancellationToken;
use shardloom_plan::ProjectionRequest;
use std::cell::RefCell;

fn prepared(
    rows: usize,
    transform: impl FnOnce(VortexRelationalPlan) -> VortexRelationalPlan,
) -> (PreparedVortexRelational, ResidentVortexSession) {
    let bytes = 64 << 20;
    let session = ResidentVortexSession::new(bytes, 1).unwrap();
    let source = ResidentMemorySource::from_int64_range(&session, "n", -3, 2, rows).unwrap();
    let uri = DatasetUri::new("memory://credit-window").unwrap();
    let policy = VortexLocalPrimitiveExecutionPolicy::from_resources(
        ExecutionResources::from_bytes(bytes, 1, ExecutionResourceOrigin::Context).unwrap(),
    )
    .unwrap();
    let prepared = prepare_relational_with_schema_in_session(policy, &session, |schemas| {
        schemas.register_memory_source(uri.clone(), |_| Ok(source))?;
        Ok(transform(VortexRelationalPlan::Scan(
            VortexRelationalScan {
                source_uri: uri,
                projection: ProjectionRequest::All,
                predicate: None,
            },
        )))
    })
    .unwrap();
    (prepared, session)
}

fn project(input: VortexRelationalPlan, fields: usize) -> VortexRelationalPlan {
    VortexRelationalPlan::Project(Box::new(VortexRelationalProject {
        input,
        expressions: (0..fields)
            .map(|index| {
                (
                    format!("n{index}"),
                    Expression::new(
                        ExprId::new("credit-column").unwrap(),
                        ExpressionKind::Column(ColumnRef::new("n").unwrap()),
                    ),
                )
            })
            .collect(),
    }))
}

#[test]
fn credit_window_requires_fifo_unique_acknowledgments_before_more_production() {
    let pool = LiveMemoryPool::new(4096).unwrap();
    let mut window = OutputCreditWindow::new(3 * BATCH_ROWS, &pool).unwrap();
    assert_eq!(window.enqueue(BATCH_ROWS, 100).unwrap(), 0);
    assert!(!window.requires_acknowledgment());
    assert_eq!(window.enqueue(BATCH_ROWS, 200).unwrap(), 1);
    assert!(window.requires_acknowledgment());
    assert!(window.enqueue(BATCH_ROWS, 300).is_err());
    assert!(window.acknowledge(1).is_err());
    assert_eq!(window.oldest_ordinal(), Some(0));
    window.acknowledge(0).unwrap();
    assert!(window.acknowledge(0).is_err());
    assert_eq!(window.oldest_ordinal(), Some(1));
    assert_eq!(window.enqueue(BATCH_ROWS, 300).unwrap(), 2);
    window.acknowledge(1).unwrap();
    assert!(window.requires_acknowledgment());
    assert!(window.finish().is_err());
    window.acknowledge(2).unwrap();
    window.finish().unwrap();
    assert!(window.acknowledge(2).is_err());
    drop(window);
    assert_eq!(pool.snapshot().reserved_bytes, 0);
}

#[test]
fn credit_window_enforces_exact_inclusive_byte_bounds_without_losing_state() {
    let pool = LiveMemoryPool::new(4096).unwrap();
    let mut window = OutputCreditWindow::new(3 * BATCH_ROWS, &pool).unwrap();
    assert!(window.enqueue(BATCH_ROWS, BATCH_BYTES + 1).is_err());
    assert_eq!(window.enqueue(BATCH_ROWS, BATCH_BYTES).unwrap(), 0);
    assert!(window.enqueue(BATCH_ROWS, usize::MAX).is_err());
    assert_eq!(window.enqueue(BATCH_ROWS, BATCH_BYTES).unwrap(), 1);
    assert_eq!(window.bytes, WINDOW_BYTES);
    assert!(window.enqueue(BATCH_ROWS, 0).is_err());
    window.acknowledge(0).unwrap();
    assert_eq!(window.enqueue(BATCH_ROWS, BATCH_BYTES).unwrap(), 2);
    window.acknowledge(1).unwrap();
    window.acknowledge(2).unwrap();
    window.finish().unwrap();
}

#[test]
fn credit_window_checks_empty_partial_final_and_maximum_partitions() {
    let pool = LiveMemoryPool::new(4096).unwrap();
    for rows in [0, 1, 17, 2048, 2049, 65_536] {
        let mut window = OutputCreditWindow::new(rows, &pool).unwrap();
        assert!(window.finish().is_err());
        let mut remaining = rows;
        for ordinal in 0..rows.div_ceil(BATCH_ROWS).max(1) {
            let batch_rows = remaining.min(BATCH_ROWS);
            assert!(window.enqueue(batch_rows + 1, 8).is_err());
            assert_eq!(window.enqueue(batch_rows, 8).unwrap(), ordinal);
            assert!(window.finish().is_err());
            window.acknowledge(ordinal).unwrap();
            remaining -= batch_rows;
        }
        window.finish().unwrap();
        assert!(window.enqueue(0, 0).is_err());
        drop(window);
        assert_eq!(pool.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn credit_window_reserves_metadata_before_construction_and_never_credits_payload() {
    assert!(OutputCreditWindow::new(17, &LiveMemoryPool::new(1023).unwrap()).is_err());
    assert!(std::mem::size_of::<OutputCreditWindow>() <= usize::try_from(METADATA_BYTES).unwrap());
    let pool = LiveMemoryPool::new(4096).unwrap();
    let payload = pool.reserve(2048).unwrap();
    let mut window = OutputCreditWindow::new(17, &pool).unwrap();
    assert_eq!(pool.snapshot().reserved_bytes, 3072);
    window.enqueue(17, 2048).unwrap();
    window.acknowledge(0).unwrap();
    window.finish().unwrap();
    assert_eq!(pool.snapshot().reserved_bytes, 3072);
    drop(window);
    assert_eq!(pool.snapshot().reserved_bytes, 2048);
    drop(payload);
    assert_eq!(pool.snapshot().reserved_bytes, 0);
}

#[test]
fn credit_window_admission_uses_actual_row_and_field_boundaries() {
    for rows in [0, 17, 65_536, 65_537] {
        let (prepared, session) = prepared(rows, std::convert::identity);
        let pool = session.shared_memory_pool();
        let baseline = pool.snapshot().reserved_bytes;
        assert!(prepared.output_credit_window(false).unwrap().is_none());
        assert_eq!(pool.snapshot().reserved_bytes, baseline);
        let admission = prepared.output_credit_window(true).unwrap();
        assert_eq!(admission.is_some(), rows <= 65_536);
        if let Some(window) = &admission {
            assert_eq!(window.expected_rows(), rows);
            assert_eq!(window.metadata_bytes(), METADATA_BYTES);
            assert_eq!(pool.snapshot().reserved_bytes, baseline + METADATA_BYTES);
        }
        drop(admission);
        assert_eq!(pool.snapshot().reserved_bytes, baseline);
        drop((prepared, session));
        assert_eq!(pool.snapshot().reserved_bytes, 0);
    }
    for fields in [1, 64, 65] {
        let (prepared, session) = prepared(17, |scan| project(scan, fields));
        let pool = session.shared_memory_pool();
        assert_eq!(
            prepared.output_credit_window(true).unwrap().is_some(),
            fields <= 64
        );
        drop((prepared, session));
        assert_eq!(pool.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn credit_window_rejects_operator_and_dynamic_roots_without_running_them() {
    let (limited, _) = prepared(17, |scan| {
        VortexRelationalPlan::Limit(Box::new(VortexRelationalLimit {
            input: scan,
            offset: 0,
            count: 17,
        }))
    });
    assert!(limited.output_credit_window(true).unwrap().is_none());
    let (nested, _) = prepared(17, |scan| {
        VortexRelationalPlan::Project(Box::new(VortexRelationalProject {
            input: project(scan, 1),
            expressions: vec![(
                "outer".into(),
                Expression::new(
                    ExprId::new("outer-column").unwrap(),
                    ExpressionKind::Column(ColumnRef::new("n0").unwrap()),
                ),
            )],
        }))
    });
    assert!(nested.output_credit_window(true).unwrap().is_none());
    let (mut dynamic, _) = prepared(17, std::convert::identity);
    dynamic.root = PreparedRoot::Dynamic(Box::new(|_| {
        panic!("admission must not lower dynamic input")
    }));
    assert!(dynamic.output_credit_window(true).unwrap().is_none());
}

#[test]
fn credit_window_metadata_pressure_is_explicit_and_retryable() {
    let (prepared, session) = prepared(17, std::convert::identity);
    let pool = session.shared_memory_pool();
    let baseline = pool.snapshot().reserved_bytes;
    let occupied = pool
        .reserve(pool.snapshot().limit_bytes - baseline - 1023)
        .unwrap();
    assert!(prepared.output_credit_window(true).is_err());
    assert_eq!(pool.snapshot().reserved_bytes, baseline + occupied.bytes());
    assert_eq!(session.snapshot().completed_executions, 0);
    drop(occupied);
    let admitted = prepared.output_credit_window(true).unwrap().unwrap();
    drop(admitted);
    assert_eq!(pool.snapshot().reserved_bytes, baseline);
}

#[test]
fn credit_window_initialization_is_inside_native_clock_and_arrow_owners_outlive_credits() {
    let (prepared, session) = prepared(17, std::convert::identity);
    let pool = session.shared_memory_pool();
    let ledger = RefCell::new(None::<OutputCreditWindow>);
    #[cfg(feature = "output-window-observation")]
    let capture = observation::Capture::begin_window(true, true).unwrap();
    let mut retained = None;
    let completed = prepared
        .for_each_arrow_batch_with_output_window(
            &CancellationToken::default(),
            || true,
            |window| {
                let window = window.unwrap();
                #[cfg(feature = "output-window-observation")]
                observation::window_policy(Some(window.expected_rows()), window.metadata_bytes());
                *ledger.borrow_mut() = Some(window);
                Ok(())
            },
            |batch| {
                let mut ledger = ledger.borrow_mut();
                let window = ledger.as_mut().unwrap();
                let bytes = batch.get_array_memory_size();
                #[cfg(feature = "output-window-observation")]
                observation::converted(batch.num_rows(), bytes);
                let ordinal = window.enqueue(batch.num_rows(), bytes)?;
                #[cfg(feature = "output-window-observation")]
                observation::sent();
                retained = Some(batch.slice(3, 4));
                window.acknowledge(ordinal)?;
                #[cfg(feature = "output-window-observation")]
                observation::acknowledged_ordinal(ordinal);
                window.finish()
            },
        )
        .unwrap();
    drop(ledger.into_inner());
    #[cfg(feature = "output-window-observation")]
    {
        observation::window_released();
        let report: serde_json::Value = serde_json::from_str(&capture.finish(true)).unwrap();
        assert_eq!(report["valid"], true);
        assert_eq!(report["window"]["metadata_reserved_bytes"], 1024);
        assert_eq!(report["window"]["metadata_final_bytes"], 0);
        assert_eq!(report["window"]["outstanding_batches"], 0);
    }
    assert!(completed.execution.native_io_certificate.is_certified());
    drop((completed, prepared, session));
    assert!(pool.snapshot().reserved_bytes > 0);
    assert_eq!(
        retained
            .as_ref()
            .unwrap()
            .column(0)
            .as_primitive::<Int64Type>()
            .values()
            .as_ref(),
        &[3, 5, 7, 9]
    );
    drop(retained);
    assert_eq!(pool.snapshot().reserved_bytes, 0);
}

#[test]
fn credit_window_initializer_failure_precedes_output_and_cleans_reservation() {
    let (prepared, session) = prepared(17, std::convert::identity);
    let pool = session.shared_memory_pool();
    let baseline = pool.snapshot().reserved_bytes;
    let error = prepared
        .for_each_arrow_batch_with_output_window(
            &CancellationToken::default(),
            || true,
            |window| {
                assert_eq!(window.unwrap().metadata_bytes(), METADATA_BYTES);
                Err(failed("consumer admission refused"))
            },
            |_| panic!("output preceded admission"),
        )
        .err()
        .unwrap();
    assert!(error.to_string().contains("consumer admission refused"));
    assert_eq!(pool.snapshot().reserved_bytes, baseline);
    assert_eq!(session.snapshot().completed_executions, 0);
    assert!(
        prepared
            .for_each_arrow_batch(&CancellationToken::default(), |_| Ok(()))
            .is_ok()
    );
}
