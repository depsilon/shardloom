//! One serialization of typed runtime decisions for shared public reports.

use shardloom_core::native_io_decisions::{
    NativeIoCapability, NativeIoCapabilityDecisions, NativeIoDecisionReason,
};

pub(super) fn append(
    fields: &mut Vec<(String, String)>,
    prefix: &str,
    decisions: &NativeIoCapabilityDecisions,
) {
    fields.push((
        format!("{prefix}_decision_schema_version"),
        "shardloom.native_io_decisions.v1".into(),
    ));
    fields.push((format!("{prefix}_decision_scope"),
        "instrumented_owner_boundaries;unobserved_capabilities_and_work_omitted;observed_work_sums_only;no_inferred_savings_or_rss".into()));
    for capability in NativeIoCapability::ALL {
        let Some(row) = decisions.get(capability) else {
            continue;
        };
        let prefix = format!("{prefix}_decision_{}", capability.as_str());
        for (name, value) in [
            ("considered", row.considered),
            ("admitted", row.admitted),
            ("declined", row.declined),
            ("executed", row.executed),
        ] {
            fields.push((format!("{prefix}_{name}"), value.to_string()));
        }
        for (name, admission) in [("admission_reasons", true), ("decline_reasons", false)] {
            let reasons = NativeIoDecisionReason::ALL
                .into_iter()
                .filter(|&reason| {
                    if admission {
                        row.has_admission_reason(reason)
                    } else {
                        row.has_decline_reason(reason)
                    }
                })
                .map(NativeIoDecisionReason::as_str)
                .collect::<Vec<_>>()
                .join(",");
            fields.push((format!("{prefix}_{name}"), reasons));
        }
        let observed = row.observed;
        for (name, value) in [
            ("input_rows", observed.input_rows),
            ("output_rows", observed.output_rows),
            ("regions", observed.regions),
            ("excluded_regions", observed.excluded_regions),
            ("copied_buffer_bytes", observed.copied_buffer_bytes),
            (
                "transferred_buffer_bytes",
                observed.transferred_buffer_bytes,
            ),
            ("spill_runs", observed.spill_runs),
        ] {
            if let Some(value) = value {
                fields.push((format!("{prefix}_observed_{name}"), value.to_string()));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use shardloom_core::native_io_decisions::{NativeIoDecisionOutcome, NativeIoObservedWork};

    #[test]
    fn native_decision_fields_preserve_unknown_zero_and_admission_without_execution() {
        let mut decisions = NativeIoCapabilityDecisions::default();
        decisions
            .record(
                NativeIoCapability::CorrelatedMembership,
                NativeIoDecisionOutcome::Admitted,
                NativeIoDecisionReason::PureEqualityCorrelation,
                NativeIoObservedWork::default(),
            )
            .unwrap();
        decisions
            .record(
                NativeIoCapability::NativeReplay,
                NativeIoDecisionOutcome::Executed,
                NativeIoDecisionReason::ResidentRetention,
                NativeIoObservedWork {
                    output_rows: Some(0),
                    ..NativeIoObservedWork::default()
                },
            )
            .unwrap();
        let mut fields = Vec::new();
        append(&mut fields, "native_io", &decisions);
        let fields: std::collections::BTreeMap<_, _> = fields.into_iter().collect();
        assert_eq!(
            fields["native_io_decision_correlated_membership_admitted"],
            "1"
        );
        assert_eq!(
            fields["native_io_decision_correlated_membership_executed"],
            "0"
        );
        assert_eq!(
            fields["native_io_decision_native_replay_observed_output_rows"],
            "0"
        );
        assert!(!fields.contains_key("native_io_decision_native_replay_observed_input_rows"));
        assert!(!fields.contains_key("native_io_decision_metadata_count_considered"));
        assert_eq!(
            fields["native_io_decision_correlated_membership_admission_reasons"],
            "pure_equality_correlation"
        );
    }
}
