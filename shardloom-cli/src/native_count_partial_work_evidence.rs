//! Machine-readable rendering for completed grouped COUNT batch partial work.

use shardloom_core::native_count_partial_work::NativeCountPartialWork;

pub(super) fn append(
    fields: &mut Vec<(String, String)>,
    prefix: &str,
    work: &NativeCountPartialWork,
) {
    let report = serde_json::json!({
        "schema_version": "shardloom.native_count_partial_work.v1",
        "scope": "completed_grouped_count_batch_partial_work;input_rows_reduced_groups_and_partial_capacity_bytes_sum_admitted_batches_only;partial_capacity_bytes_is_sum_not_peak_or_live_memory;declined_counts_pre_mutation_attempts;no_file_io_avoidance_claim",
        "considered_batches": work.considered_batches,
        "admitted_batches": work.admitted_batches,
        "input_rows": work.input_rows,
        "reduced_groups": work.reduced_groups,
        "constant_batches": work.constant_batches,
        "declined_unique_batches": work.declined_unique_batches,
        "declined_capacity_batches": work.declined_capacity_batches,
        "partial_capacity_bytes": work.partial_capacity_bytes,
    });
    fields.push((
        format!("{prefix}_count_partial_work_json"),
        report.to_string(),
    ));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn count_partial_work_json_preserves_every_counter_and_zero() {
        let work = NativeCountPartialWork {
            considered_batches: 1,
            admitted_batches: 2,
            input_rows: 3,
            reduced_groups: 4,
            constant_batches: 5,
            declined_unique_batches: 0,
            declined_capacity_batches: 7,
            partial_capacity_bytes: 8,
        };
        let mut fields = Vec::new();

        append(&mut fields, "prepared", &work);

        assert_eq!(fields.len(), 1);
        assert_eq!(fields[0].0, "prepared_count_partial_work_json");
        let value: serde_json::Value = serde_json::from_str(&fields[0].1).unwrap();
        assert_eq!(
            value,
            serde_json::json!({
                "schema_version": "shardloom.native_count_partial_work.v1",
                "scope": "completed_grouped_count_batch_partial_work;input_rows_reduced_groups_and_partial_capacity_bytes_sum_admitted_batches_only;partial_capacity_bytes_is_sum_not_peak_or_live_memory;declined_counts_pre_mutation_attempts;no_file_io_avoidance_claim",
                "considered_batches": 1,
                "admitted_batches": 2,
                "input_rows": 3,
                "reduced_groups": 4,
                "constant_batches": 5,
                "declined_unique_batches": 0,
                "declined_capacity_batches": 7,
                "partial_capacity_bytes": 8,
            })
        );
    }
}
