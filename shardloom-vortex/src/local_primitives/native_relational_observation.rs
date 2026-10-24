//! Debug-test observations; absent from release measurements and production.

use std::cell::RefCell;

#[derive(Default, Debug, serde::Serialize)]
pub(crate) struct Counts {
    pub(crate) key_columns: u64,
    pub(crate) variable_domains: u64,
    pub(crate) variable_domain_values: u64,
    pub(crate) variable_hash_slot_bytes: u64,
    pub(crate) variable_hash_values: u64,
    pub(crate) variable_hash_bytes: u64,
    pub(crate) aggregate_value_preparations: u64,
    pub(crate) aggregate_row_updates: u64,
    pub(crate) row_location_searches: u64,
    pub(crate) sort_retained_ordinals: u64,
    pub(crate) sort_output_rows_constructed: u64,
    pub(crate) sort_payload_rows_validated: u64,
    pub(crate) predicate_compile_attempts: u64,
    pub(crate) predicate_boolean_rows: u64,
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
