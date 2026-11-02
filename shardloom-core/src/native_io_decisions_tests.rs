use super::*;

#[test]
fn native_decisions_distinguish_unobserved_admitted_declined_and_executed() {
    let mut decisions = NativeIoCapabilityDecisions::default();
    let capability = NativeIoCapability::MetadataCount;
    assert_eq!(decisions.get(capability), None);
    decisions
        .record(
            capability,
            NativeIoDecisionOutcome::Admitted,
            NativeIoDecisionReason::ExactHeldMetadata,
            NativeIoObservedWork::default(),
        )
        .unwrap();
    let admitted = decisions.get(capability).unwrap();
    assert_eq!(
        (admitted.considered, admitted.admitted, admitted.executed),
        (1, 1, 0)
    );
    assert_eq!(admitted.observed.input_rows, None);
    decisions
        .record(
            capability,
            NativeIoDecisionOutcome::Declined,
            NativeIoDecisionReason::SharedSourceUsage,
            NativeIoObservedWork::default(),
        )
        .unwrap();
    decisions
        .record(
            capability,
            NativeIoDecisionOutcome::Executed,
            NativeIoDecisionReason::ExactHeldMetadata,
            NativeIoObservedWork {
                input_rows: Some(0),
                ..NativeIoObservedWork::default()
            },
        )
        .unwrap();
    let row = decisions.get(capability).unwrap();
    assert_eq!(
        (row.considered, row.admitted, row.declined, row.executed),
        (3, 2, 1, 1)
    );
    assert_eq!(row.observed.input_rows, Some(0));
    assert_eq!(row.observed.copied_buffer_bytes, None);
    assert!(row.has_admission_reason(NativeIoDecisionReason::ExactHeldMetadata));
    assert!(row.has_decline_reason(NativeIoDecisionReason::SharedSourceUsage));
    assert!(!row.has_admission_reason(NativeIoDecisionReason::SharedSourceUsage));
    assert_eq!(decisions.get(NativeIoCapability::ExistingOrder), None);
}

#[test]
fn native_decisions_aggregate_fixed_storage_and_preserve_reason_sets() {
    let mut decisions = NativeIoCapabilityDecisions::default();
    for _ in 0..10_000 {
        decisions
            .record(
                NativeIoCapability::RepresentationRetention,
                NativeIoDecisionOutcome::Executed,
                NativeIoDecisionReason::OwnedRepresentation,
                NativeIoObservedWork {
                    transferred_buffer_bytes: Some(24),
                    ..NativeIoObservedWork::default()
                },
            )
            .unwrap();
    }
    let row = decisions
        .get(NativeIoCapability::RepresentationRetention)
        .unwrap();
    assert_eq!(row.considered, 10_000);
    assert_eq!(row.executed, 10_000);
    assert_eq!(row.observed.transferred_buffer_bytes, Some(240_000));
    assert!(std::mem::size_of::<NativeIoCapabilityDecisions>() <= 1024);
    let labels: std::collections::BTreeSet<_> = NativeIoDecisionReason::ALL
        .map(NativeIoDecisionReason::as_str)
        .into();
    assert_eq!(labels.len(), NativeIoDecisionReason::ALL.len());
}

#[test]
fn native_decisions_reject_overflow_without_partial_updates() {
    let mut decisions = NativeIoCapabilityDecisions::default();
    let capability = NativeIoCapability::NativeReplay;
    decisions
        .record(
            capability,
            NativeIoDecisionOutcome::Executed,
            NativeIoDecisionReason::ResidentRetention,
            NativeIoObservedWork {
                output_rows: Some(u64::MAX),
                ..NativeIoObservedWork::default()
            },
        )
        .unwrap();
    let before = decisions;
    assert!(
        decisions
            .record(
                capability,
                NativeIoDecisionOutcome::Executed,
                NativeIoDecisionReason::AdmittedNativeSpill,
                NativeIoObservedWork {
                    output_rows: Some(1),
                    ..NativeIoObservedWork::default()
                }
            )
            .is_err()
    );
    assert_eq!(decisions, before);
    decisions.0[capability as usize].considered = u64::MAX;
    let before = decisions;
    assert!(
        decisions
            .record(
                capability,
                NativeIoDecisionOutcome::Declined,
                NativeIoDecisionReason::InsufficientHeadroom,
                NativeIoObservedWork::default()
            )
            .is_err()
    );
    assert_eq!(decisions, before);
}

#[test]
fn native_decisions_merge_disjoint_owners_and_bulk_region_outcomes() {
    let mut decisions = NativeIoCapabilityDecisions::single(
        NativeIoCapability::NativeReplay,
        NativeIoDecisionOutcome::Executed,
        NativeIoDecisionReason::AdmittedNativeSpill,
        NativeIoObservedWork {
            output_rows: Some(7),
            ..NativeIoObservedWork::default()
        },
    );
    let mut source = NativeIoCapabilityDecisions::default();
    source
        .record_many(
            NativeIoCapability::RegionExclusion,
            3,
            NativeIoDecisionOutcome::Executed,
            NativeIoDecisionReason::ExclusionProved,
            NativeIoObservedWork {
                regions: Some(3),
                excluded_regions: Some(3),
                ..NativeIoObservedWork::default()
            },
        )
        .unwrap();
    source
        .record_many(
            NativeIoCapability::RegionExclusion,
            2,
            NativeIoDecisionOutcome::Declined,
            NativeIoDecisionReason::ExclusionNotProved,
            NativeIoObservedWork {
                regions: Some(2),
                excluded_regions: Some(0),
                ..NativeIoObservedWork::default()
            },
        )
        .unwrap();
    decisions.merge(&source).unwrap();
    let regions = decisions.get(NativeIoCapability::RegionExclusion).unwrap();
    assert_eq!(
        (
            regions.considered,
            regions.admitted,
            regions.declined,
            regions.executed
        ),
        (5, 3, 2, 3)
    );
    assert_eq!(regions.observed.regions, Some(5));
    assert_eq!(regions.observed.excluded_regions, Some(3));
    assert_eq!(regions.observed.input_rows, None);
    assert!(regions.has_admission_reason(NativeIoDecisionReason::ExclusionProved));
    assert!(regions.has_decline_reason(NativeIoDecisionReason::ExclusionNotProved));
    assert_eq!(
        decisions
            .get(NativeIoCapability::NativeReplay)
            .unwrap()
            .observed
            .output_rows,
        Some(7)
    );
    assert_eq!(decisions.get(NativeIoCapability::MetadataCount), None);
    let before = decisions;
    assert!(
        decisions
            .record_many(
                NativeIoCapability::RegionExclusion,
                0,
                NativeIoDecisionOutcome::Executed,
                NativeIoDecisionReason::ExclusionProved,
                NativeIoObservedWork::default(),
            )
            .is_err()
    );
    assert_eq!(decisions, before);
}

#[test]
fn native_decisions_merge_overflow_keeps_every_earlier_capability_unchanged() {
    let mut decisions = NativeIoCapabilityDecisions::single(
        NativeIoCapability::RepresentationRetention,
        NativeIoDecisionOutcome::Executed,
        NativeIoDecisionReason::OwnedRepresentation,
        NativeIoObservedWork {
            transferred_buffer_bytes: Some(u64::MAX),
            ..NativeIoObservedWork::default()
        },
    );
    let mut incoming = NativeIoCapabilityDecisions::single(
        NativeIoCapability::MetadataCount,
        NativeIoDecisionOutcome::Executed,
        NativeIoDecisionReason::ExactHeldMetadata,
        NativeIoObservedWork {
            input_rows: Some(0),
            ..NativeIoObservedWork::default()
        },
    );
    incoming
        .record(
            NativeIoCapability::RepresentationRetention,
            NativeIoDecisionOutcome::Executed,
            NativeIoDecisionReason::OwnedRepresentation,
            NativeIoObservedWork {
                transferred_buffer_bytes: Some(1),
                ..NativeIoObservedWork::default()
            },
        )
        .unwrap();
    let before = decisions;
    assert!(decisions.merge(&incoming).is_err());
    assert_eq!(decisions, before);
    assert!(
        decisions
            .record_many(
                NativeIoCapability::RepresentationRetention,
                u64::MAX,
                NativeIoDecisionOutcome::Declined,
                NativeIoDecisionReason::RequiredRepresentationCopy,
                NativeIoObservedWork::default(),
            )
            .is_err()
    );
    assert_eq!(decisions, before);
}
