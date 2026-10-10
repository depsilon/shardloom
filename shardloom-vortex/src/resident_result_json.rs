//! Bounded sink over the actual retained native result.
//!
//! A child of `resident_session`; no Python dependency belongs here.

use super::CallClass;
use super::{OwnedVortexResultBatch, Result, resident_error};
use shardloom_exec::compute_pool::CancellationToken;
use shardloom_exec::live_memory::Budgeted;

impl OwnedVortexResultBatch {
    /// Materialize this retained result through the existing native JSON sink.
    /// No source is reopened and no query is rerun. The result's own runtime and
    /// allocator survive the original session handle. JSON is an explicit copy
    /// and materialization boundary; the returned `String` retains its reservation.
    ///
    /// # Errors
    /// Rejects empty or duplicate field lists, invalid/unknown/unsupported fields,
    /// provider failures, and row/byte/shared-memory bounds. Field metadata is
    /// admitted by the retained result's shared grant.
    pub fn to_bounded_json(
        &self,
        columns: &[String],
        max_bytes: usize,
    ) -> Result<Budgeted<String>> {
        if columns.is_empty() || !(1..=8 * 1024 * 1024).contains(&max_bytes) {
            return Err(resident_error(
                "result JSON requires fields and a 1..=8 MiB output bound",
            ));
        }
        for name in columns {
            if name.is_empty() || name.len() > 256 {
                return Err(resident_error(
                    "result JSON field names must contain 1..=256 UTF8 bytes",
                ));
            }
        }
        let _metadata = crate::native_payload_schema::reserve_names(
            &self.runtime.memory,
            columns.len(),
            |index| columns[index].as_str(),
        )?;
        self.render_admitted_json(columns, max_bytes)
    }

    /// Existing prepared collection has already admitted its schema. Preserve
    /// that wider field contract while sharing the same execution gate/sink.
    pub(crate) fn render_admitted_json(
        &self,
        columns: &[String],
        max_bytes: usize,
    ) -> Result<Budgeted<String>> {
        self.validate_schema_and_rows()?;
        let fields = self
            .dtype()
            .as_struct_fields_opt()
            .ok_or_else(|| resident_error("result JSON requires a struct dtype"))?;
        for column in columns {
            if fields.field(column).is_none() {
                return Err(resident_error(&format!(
                    "result JSON field is absent from the result schema: {column}"
                )));
            }
        }
        // Results retain the same execution gate after the original session
        // handle closes; concurrent result sinks must not bypass that grant.
        let _context = self
            .runtime
            .enter(CallClass::General, CancellationToken::default())?;
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
