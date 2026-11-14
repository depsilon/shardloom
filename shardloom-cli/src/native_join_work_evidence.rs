//! Machine-readable rendering for completed native join work.

use shardloom_core::native_join_work::NativeJoinWork;

pub(super) fn append(fields: &mut Vec<(String, String)>, prefix: &str, work: &NativeJoinWork) {
    let report = serde_json::json!({
        "schema_version": "shardloom.native_join_work.v1",
        "scope": "completed_native_join_work;runtime_filters_after_key_validation;rejected_rows_avoid_index_lookups_not_source_io;skew_counts_actual_contiguous_range_use;cyclic_counts_actual_witness_search",
        "runtime_filter": {
            "strategy": "three_bit_bloom_membership",
            "build_provenance": "completed_native_exact_key_index",
            "application_boundary": "validated_nonnull_join_probe_hash;positive_requires_exact_native_equality",
            "considered": work.runtime_filter.considered,
            "admitted": work.runtime_filter.admitted,
            "declined_small": work.runtime_filter.declined_small,
            "declined_memory": work.runtime_filter.declined_memory,
            "build_keys": work.runtime_filter.build_keys,
            "retained_bytes": work.runtime_filter.retained_bytes,
            "probe_rows": work.runtime_filter.probe_rows,
            "rejected_rows": work.runtime_filter.rejected_rows,
            "index_lookups_avoided": work.runtime_filter.index_lookups_avoided,
        },
        "skew": {
            "strategy": "stable_contiguous_ordinals_from_completed_duplicate_chains",
            "considered": work.skew.considered,
            "hot_groups": work.skew.hot_groups,
            "hot_rows": work.skew.hot_rows,
            "admitted_groups": work.skew.admitted_groups,
            "admitted_rows": work.skew.admitted_rows,
            "declined_memory": work.skew.declined_memory,
            "range_visits": work.skew.range_visits,
            "rows_visited": work.skew.rows_visited,
        },
        "cyclic": {
            "executions": work.cyclic.executions,
            "last_build_rows": work.cyclic.last_build_rows,
            "middle_build_rows": work.cyclic.middle_build_rows,
            "outer_rows": work.cyclic.outer_rows,
            "middle_visits": work.cyclic.middle_visits,
            "last_probes": work.cyclic.last_probes,
            "witness_exits": work.cyclic.witness_exits,
            "gathered_rows": work.cyclic.gathered_rows,
            "middle_payload_rows_validated": work.cyclic.middle_payload_rows_validated,
            "last_payload_rows_validated": work.cyclic.last_payload_rows_validated,
            "payload_cells_validated": work.cyclic.payload_cells_validated,
        },
    });
    fields.push((format!("{prefix}_join_work_json"), report.to_string()));
}

#[cfg(test)]
mod tests {
    use super::*;
    use shardloom_core::native_join_work::{
        NativeCyclicExistenceWork, NativeRuntimeFilterWork, NativeSkewWork,
    };

    #[test]
    fn join_work_json_preserves_all_child_counters_and_zero_values() {
        let work = NativeJoinWork {
            runtime_filter: NativeRuntimeFilterWork {
                considered: 1,
                admitted: 2,
                declined_small: 0,
                declined_memory: 4,
                build_keys: 5,
                retained_bytes: 6,
                probe_rows: 7,
                rejected_rows: 8,
                index_lookups_avoided: 9,
            },
            skew: NativeSkewWork {
                considered: 10,
                hot_groups: 11,
                hot_rows: 12,
                admitted_groups: 13,
                admitted_rows: 14,
                declined_memory: 15,
                range_visits: 16,
                rows_visited: 17,
            },
            cyclic: NativeCyclicExistenceWork {
                executions: 18,
                last_build_rows: 19,
                middle_build_rows: 20,
                outer_rows: 21,
                middle_visits: 22,
                last_probes: 23,
                witness_exits: 24,
                gathered_rows: 25,
                middle_payload_rows_validated: 26,
                last_payload_rows_validated: 27,
                payload_cells_validated: 28,
            },
        };
        let mut fields = Vec::new();

        append(&mut fields, "prepared", &work);

        assert_eq!(fields.len(), 1);
        assert_eq!(fields[0].0, "prepared_join_work_json");
        let value: serde_json::Value = serde_json::from_str(&fields[0].1).unwrap();
        assert_eq!(
            value,
            serde_json::json!({
                "schema_version": "shardloom.native_join_work.v1",
                "scope": "completed_native_join_work;runtime_filters_after_key_validation;rejected_rows_avoid_index_lookups_not_source_io;skew_counts_actual_contiguous_range_use;cyclic_counts_actual_witness_search",
                "runtime_filter": {
                    "strategy": "three_bit_bloom_membership",
                    "build_provenance": "completed_native_exact_key_index",
                    "application_boundary": "validated_nonnull_join_probe_hash;positive_requires_exact_native_equality",
                    "considered": 1,
                    "admitted": 2,
                    "declined_small": 0,
                    "declined_memory": 4,
                    "build_keys": 5,
                    "retained_bytes": 6,
                    "probe_rows": 7,
                    "rejected_rows": 8,
                    "index_lookups_avoided": 9,
                },
                "skew": {
                    "strategy": "stable_contiguous_ordinals_from_completed_duplicate_chains",
                    "considered": 10,
                    "hot_groups": 11,
                    "hot_rows": 12,
                    "admitted_groups": 13,
                    "admitted_rows": 14,
                    "declined_memory": 15,
                    "range_visits": 16,
                    "rows_visited": 17,
                },
                "cyclic": {
                    "executions": 18,
                    "last_build_rows": 19,
                    "middle_build_rows": 20,
                    "outer_rows": 21,
                    "middle_visits": 22,
                    "last_probes": 23,
                    "witness_exits": 24,
                    "gathered_rows": 25,
                    "middle_payload_rows_validated": 26,
                    "last_payload_rows_validated": 27,
                    "payload_cells_validated": 28,
                },
            })
        );
    }
}
