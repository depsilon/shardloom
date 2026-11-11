//! Debug-test observations; absent from release measurements and production.

use std::cell::RefCell;

#[derive(Default, Debug, serde::Serialize)]
pub(crate) struct Counts {
    pub(crate) row_index_lookups: u64,
    pub(crate) row_index_probe_slots: u64,
    pub(crate) row_index_bucket_reads: u64,
    pub(crate) row_index_entry_reads: u64,
    pub(crate) row_index_equality_calls: u64,
    pub(crate) row_index_fingerprint_candidates: u64,
    pub(crate) row_index_large_lookups: u64,
    pub(crate) row_index_large_entry_reads: u64,
    pub(crate) row_index_large_fingerprint_candidates: u64,
    pub(crate) row_index_metadata_groups: u64,
    pub(crate) row_index_bucket_initialization_bytes: u64,
    pub(crate) row_index_metadata_initialization_bytes: u64,
    pub(crate) row_index_rehash_entries: u64,
    pub(crate) row_index_vacancy_reads: u64,
    pub(crate) key_columns: u64,
    pub(crate) key_cache_batches: u64,
    pub(crate) key_cache_slots: u64,
    pub(crate) key_cache_hits: u64,
    pub(crate) key_inline_cache_batches: u64,
    pub(crate) key_inline_cache_hits: u64,
    pub(crate) key_exact_lookups: u64,
    pub(crate) group_ordinal_rows: u64,
    pub(crate) variable_domains: u64,
    pub(crate) variable_domain_values: u64,
    pub(crate) variable_hash_slot_bytes: u64,
    pub(crate) variable_hash_values: u64,
    pub(crate) variable_hash_on_demand_values: u64,
    pub(crate) variable_hash_bytes: u64,
    pub(crate) variable_result_preparations: u64,
    pub(crate) variable_result_constant_rows: u64,
    pub(crate) variable_result_visits: u64,
    pub(crate) aggregate_value_preparations: u64,
    pub(crate) aggregate_row_updates: u64,
    pub(crate) aggregate_numeric_bound_batches: u64,
    pub(crate) aggregate_numeric_bound_rows: u64,
    pub(crate) aggregate_extrema_bound_batches: u64,
    pub(crate) aggregate_extrema_bound_rows: u64,
    pub(crate) aggregate_typed_contiguous_state_binds: u64,
    pub(crate) aggregate_typed_paged_state_binds: u64,
    pub(crate) aggregate_typed_paired_count_proof_rows: u64,
    pub(crate) aggregate_integer_extrema_fused_batches: u64,
    pub(crate) aggregate_integer_extrema_fused_rows: u64,
    pub(crate) aggregate_integer_extrema_saved_measure_rows: u64,
    pub(crate) aggregate_integer_extrema_admission_groups: u64,
    pub(crate) aggregate_float_extrema_fused_batches: u64,
    pub(crate) aggregate_float_extrema_fused_rows: u64,
    pub(crate) aggregate_float_extrema_saved_measure_rows: u64,
    pub(crate) aggregate_float_extrema_admission_groups: u64,
    pub(crate) numeric_output_copy_batches: u64,
    pub(crate) numeric_output_copy_rows: u64,
    pub(crate) numeric_gather_direct_batches: u64,
    pub(crate) numeric_gather_direct_rows: u64,
    pub(crate) payload_numeric_validation_columns: u64,
    pub(crate) payload_numeric_validation_rows: u64,
    pub(crate) payload_variable_validation_columns: u64,
    pub(crate) payload_variable_validation_rows: u64,
    pub(crate) payload_boolean_validation_columns: u64,
    pub(crate) payload_boolean_validation_rows: u64,
    pub(crate) payload_validation_scalar_calls: u64,
    pub(crate) payload_recursive_typed_validation_columns: u64,
    pub(crate) aggregate_filter_selections: u64,
    pub(crate) aggregate_numeric_state_reuses: u64,
    pub(crate) aggregate_numeric_reused_rows: u64,
    pub(crate) aggregate_numeric_reuse_comparison_groups: u64,
    pub(crate) row_location_searches: u64,
    pub(crate) order_domain_checks: u64,
    pub(crate) order_checks: u64,
    pub(crate) order_check_comparisons: u64,
    pub(crate) ordered_input_sorts: u64,
    pub(crate) ordered_input_rows: u64,
    pub(crate) order_heap_sorts: u64,
    pub(crate) sort_retained_ordinals: u64,
    pub(crate) sort_numeric_prefixes: u64,
    pub(crate) sort_numeric_prefix_rows: u64,
    pub(crate) sort_numeric_prefix_key_views: u64,
    pub(crate) sort_numeric_prefix_comparisons: u64,
    pub(crate) sort_output_rows_constructed: u64,
    pub(crate) sort_payload_rows_validated: u64,
    pub(crate) sort_streaming_prefix_batches: u64,
    pub(crate) sort_streaming_prefix_rows_validated: u64,
    pub(crate) sort_streaming_prefix_rows_copied: u64,
    pub(crate) sort_streaming_prefix_peak_rows: u64,
    pub(crate) sort_streaming_prefix_buffer_bytes_copied: u64,
    pub(crate) sort_streaming_prefix_peak_buffer_bytes: u64,
    pub(crate) predicate_compile_attempts: u64,
    pub(crate) predicate_boolean_rows: u64,
    pub(crate) expression_output_reuses: u64,
    pub(crate) expression_variable_values: u64,
    pub(crate) expression_numeric_binary_batches: u64,
    pub(crate) expression_numeric_binary_rows: u64,
    pub(crate) expression_numeric_coalesce_batches: u64,
    pub(crate) expression_numeric_coalesce_rows: u64,
    pub(crate) expression_prepared_utf8_batches: u64,
    pub(crate) expression_prepared_utf8_rows: u64,
    pub(crate) expression_numeric_scalar_batches: u64,
    pub(crate) expression_numeric_scalar_rows: u64,
    pub(crate) join_retained_right_rows: u64,
    pub(crate) subquery_equality_rewrites: u64,
    pub(crate) replay_captured_fragments: u64,
    pub(crate) replay_captured_rows: u64,
    pub(crate) join_output_rows: u64,
    pub(crate) join_output_cells: u64,
    pub(crate) join_payload_validation_only_cells: u64,
    pub(crate) window_frame_bound_evaluations: u64,
    pub(crate) window_state_construction_attempts: u64,
    pub(crate) window_state_admissions: u64,
    pub(crate) window_state_admitted_bytes: u64,
    pub(crate) window_moment_state_construction_attempts: u64,
    pub(crate) window_moment_state_admissions: u64,
    pub(crate) window_moment_state_admitted_bytes: u64,
    pub(crate) window_add_observation_attempts: u64,
    pub(crate) window_remove_observation_attempts: u64,
    pub(crate) window_additive_value_observations: u64,
    pub(crate) window_moment_value_observations: u64,
    pub(crate) window_state_finalizers: u64,
    pub(crate) window_moment_finalizers: u64,
    pub(crate) window_resident_frame_writes: u64,
    pub(crate) window_shared_groups: u64,
    pub(crate) window_shared_outputs: u64,
    pub(crate) window_exceptional_suffix_functions: u64,
    pub(crate) window_companion_cancellation_checks: u64,
}

thread_local! {
    static ACTIVE: RefCell<Option<Counts>> = const { RefCell::new(None) };
    static WINDOW_CANCEL: RefCell<Option<(u64, shardloom_exec::compute_pool::CancellationToken)>> = const { RefCell::new(None) };
}

pub(crate) struct Session;

impl Session {
    pub(crate) fn start() -> Self {
        ACTIVE.with_borrow_mut(|active| {
            assert!(active.is_none(), "native observations cannot be nested");
            *active = Some(Counts::default());
        });
        Self
    }

    pub(crate) fn cancel_after_window_writes(
        writes: u64,
        token: &shardloom_exec::compute_pool::CancellationToken,
    ) {
        assert!(writes > 0);
        ACTIVE.with_borrow(|active| {
            assert!(active.as_ref().unwrap().window_resident_frame_writes < writes);
        });
        WINDOW_CANCEL.with_borrow_mut(|cancel| {
            assert!(
                cancel.is_none(),
                "window cancellation hook cannot be nested"
            );
            *cancel = Some((writes, token.clone()));
        });
    }

    pub(crate) fn finish(self) -> Counts {
        let counts =
            ACTIVE.with_borrow_mut(|active| active.take().expect("active native observation"));
        drop(self);
        counts
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        ACTIVE.with_borrow_mut(|active| *active = None);
        WINDOW_CANCEL.with_borrow_mut(|cancel| *cancel = None);
    }
}

pub(super) fn record(change: impl FnOnce(&mut Counts)) {
    let writes = ACTIVE.with_borrow_mut(|active| {
        if let Some(counts) = active {
            change(counts);
            Some(counts.window_resident_frame_writes)
        } else {
            None
        }
    });
    WINDOW_CANCEL.with_borrow_mut(|cancel| {
        if cancel
            .as_ref()
            .is_some_and(|(limit, _)| writes.is_some_and(|writes| writes >= *limit))
        {
            cancel.take().unwrap().1.cancel();
        }
    });
}
