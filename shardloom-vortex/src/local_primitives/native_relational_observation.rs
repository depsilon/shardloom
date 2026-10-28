//! Debug-test observations; absent from release measurements and production.

use std::cell::RefCell;

#[derive(Default, Debug, serde::Serialize)]
pub(crate) struct Counts {
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
    pub(crate) variable_hash_bytes: u64,
    pub(crate) aggregate_value_preparations: u64,
    pub(crate) aggregate_row_updates: u64,
    pub(crate) aggregate_numeric_bound_batches: u64,
    pub(crate) aggregate_numeric_bound_rows: u64,
    pub(crate) aggregate_extrema_bound_batches: u64,
    pub(crate) aggregate_extrema_bound_rows: u64,
    pub(crate) numeric_output_copy_batches: u64,
    pub(crate) numeric_output_copy_rows: u64,
    pub(crate) payload_numeric_validation_columns: u64,
    pub(crate) payload_numeric_validation_rows: u64,
    pub(crate) payload_variable_validation_columns: u64,
    pub(crate) payload_variable_validation_rows: u64,
    pub(crate) payload_boolean_validation_columns: u64,
    pub(crate) payload_boolean_validation_rows: u64,
    pub(crate) payload_validation_scalar_calls: u64,
    pub(crate) aggregate_filter_selections: u64,
    pub(crate) aggregate_numeric_state_reuses: u64,
    pub(crate) aggregate_numeric_reused_rows: u64,
    pub(crate) aggregate_numeric_reuse_comparison_groups: u64,
    pub(crate) row_location_searches: u64,
    pub(crate) sort_retained_ordinals: u64,
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
    pub(crate) join_retained_right_rows: u64,
}

thread_local! {
    static ACTIVE: RefCell<Option<Counts>> = const { RefCell::new(None) };
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
    }
}

pub(super) fn record(change: impl FnOnce(&mut Counts)) {
    ACTIVE.with_borrow_mut(|active| {
        if let Some(counts) = active {
            change(counts);
        }
    });
}
