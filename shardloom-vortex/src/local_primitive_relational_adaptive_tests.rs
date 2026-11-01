//! Real execution controls for changing shared headroom, values and cleanup.

use super::*;
use crate::relational_query::VortexRelationalSetKind;

fn memory_plan(rows: usize, ceiling: u64) -> PreparedVortexRelational {
    let uri = DatasetUri::new("memory://adaptive-scan").unwrap();
    let scan = VortexRelationalPlan::Scan(VortexRelationalScan {
        source_uri: uri.clone(),
        projection: shardloom_plan::ProjectionRequest::All,
        predicate: None,
    });
    let plan = VortexRelationalPlan::Set(Box::new(VortexRelationalSet {
        left: scan.clone(),
        right: scan,
        kind: VortexRelationalSetKind::UnionAll,
    }));
    let mut allocation = policy();
    allocation.resource_envelope.memory_budget_bytes = ceiling;
    prepare_relational_with_schema(allocation, |binding| {
        binding.register_memory_source(uri.clone(), |session| {
            ResidentMemorySource::from_int64_range(session, "n", -13, 7, rows)
        })?;
        Ok(plan.clone())
    })
    .unwrap()
}

fn counter(proof: &str, name: &str) -> u64 {
    proof
        .split_whitespace()
        .find_map(|value| {
            value
                .strip_prefix(name)
                .and_then(|suffix| suffix.strip_prefix('='))
        })
        .unwrap_or_else(|| panic!("missing execution counter {name}"))
        .parse()
        .unwrap()
}

#[test]
fn native_adaptive_scan_changes_real_ranges_and_regrows_after_retained_owner_release() {
    const ROWS: usize = 65_539;
    for ceiling in [2 << 20, 8 << 20] {
        let prepared = memory_plan(ROWS, ceiling);
        let memory = prepared.session.memory().clone();
        let baseline = memory.snapshot().reserved_bytes;
        let mut retained = None;
        let mut calls = 0;
        let mut seen = 0;
        let result = prepared
            .for_each_batch(&CancellationToken::default(), |array, context| {
                let mut execution = context.native_session().create_execution_ctx();
                let values = crate::local_primitives::logical_field_from_native_array(&array, "n")?
                    .execute::<PrimitiveArray>(&mut execution)
                    .unwrap();
                for value in values.as_slice::<i64>() {
                    assert_eq!(*value, i64::try_from(seen % ROWS).unwrap() * 7 - 13);
                    seen += 1;
                }
                drop((values, execution));
                calls += 1;
                if calls == 1 {
                    let free = memory.snapshot().limit_bytes - memory.snapshot().reserved_bytes;
                    retained = Some(memory.reserve(free - (80 << 10))?);
                } else if calls == 24 {
                    retained.take();
                }
                Ok(())
            })
            .unwrap();
        assert_eq!(seen, 2 * ROWS);
        assert!(retained.is_none());
        let proof = &result
            .native_io_certificate
            .source_pushdown_report
            .proof_basis;
        assert!(counter(proof, "adaptive_native_scan_headroom_limited_windows") > 0);
        assert!(counter(proof, "adaptive_native_scan_shrunk_windows") > 0);
        assert!(counter(proof, "adaptive_native_scan_grown_windows") > 0);
        assert!(counter(proof, "adaptive_native_scan_minimum_rows") < 8192);
        assert_eq!(counter(proof, "adaptive_native_scan_maximum_rows"), 8192);
        assert_eq!(
            counter(proof, "adaptive_native_scan_ceiling_bytes"),
            ceiling
        );
        assert!(result.runtime.memory.peak_reserved_bytes <= ceiling);
        assert!(result.native_io_certificate.is_certified());
        assert!(!result.native_io_certificate.side_effects.fallback_attempted);
        drop(result);
        assert_eq!(memory.snapshot().reserved_bytes, baseline);
        drop(prepared);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn native_adaptive_scan_failure_and_cancellation_do_not_repeat_the_consumer() {
    for cancel in [false, true] {
        let prepared = memory_plan(32_771, 2 << 20);
        let memory = prepared.session.memory().clone();
        let baseline = memory.snapshot().reserved_bytes;
        let token = CancellationToken::default();
        let mut calls = 0;
        let mut retained = None;
        let error = prepared
            .for_each_batch(&token, |_, context| {
                calls += 1;
                if calls == 1 {
                    let snapshot = memory.snapshot();
                    retained = Some(
                        memory
                            .reserve(snapshot.limit_bytes - snapshot.reserved_bytes - (80 << 10))?,
                    );
                }
                if calls == 8 {
                    if cancel {
                        context.cancellation().cancel();
                    } else {
                        return Err(failed("adaptive consumer sentinel"));
                    }
                }
                Ok(())
            })
            .err()
            .expect("consumer failure or cancellation must terminate execution")
            .to_string();
        assert_eq!(calls, 8);
        assert!(
            error.contains(if cancel {
                "cancelled"
            } else {
                "adaptive consumer sentinel"
            }),
            "{error}"
        );
        drop(retained);
        assert_eq!(memory.snapshot().reserved_bytes, baseline);
        drop(prepared);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}
