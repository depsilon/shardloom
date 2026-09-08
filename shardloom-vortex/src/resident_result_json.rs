//! Bounded sink over the actual retained native result.
//!
//! A child of `resident_session`; no Python dependency belongs here.

use super::{OwnedVortexResultBatch, Result, resident_error};
use shardloom_exec::live_memory::Budgeted;

impl OwnedVortexResultBatch {
    /// Materialize this retained result through the existing native JSON sink.
    /// No source is reopened and no query is rerun. The result's own runtime and
    /// allocator survive the original session handle. JSON is an explicit copy
    /// and materialization boundary; the returned `String` retains its reservation.
    ///
    /// # Errors
    /// Rejects empty, duplicate or oversized field lists, unknown/unsupported
    /// fields, provider failures, and row/byte/shared-memory bounds.
    pub fn to_bounded_json(
        &self,
        columns: &[String],
        max_bytes: usize,
    ) -> Result<Budgeted<String>> {
        if columns.is_empty() || columns.len() > 64 || !(1..=8 * 1024 * 1024).contains(&max_bytes) {
            return Err(resident_error(
                "result JSON requires 1..=64 fields and 1..=8 MiB output bound",
            ));
        }
        let mut seen = std::collections::HashSet::new();
        for name in columns {
            if name.is_empty() || name.len() > 256 || !seen.insert(name.as_str()) {
                return Err(resident_error(
                    "result JSON fields must be unique, nonempty and at most 256 bytes",
                ));
            }
        }
        self.render_admitted_json(columns, max_bytes)
    }

    /// Existing prepared collection has already admitted its schema. Preserve
    /// that wider field contract while sharing the same execution gate/sink.
    pub(crate) fn render_admitted_json(
        &self,
        columns: &[String],
        max_bytes: usize,
    ) -> Result<Budgeted<String>> {
        // Results retain the same execution gate after the original session
        // handle closes; concurrent result sinks must not bypass that grant.
        let _admission = self
            .runtime
            .admission
            .lock()
            .map_err(|_| resident_error("result JSON session admission poisoned"))?;
        crate::local_primitives::collect::render_owned_json(
            self,
            columns,
            &self.runtime.memory,
            max_bytes,
        )
    }
}

#[cfg(test)]
#[path = "resident_result_json_tests.rs"]
mod tests;
