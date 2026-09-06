//! Native per-array decoding into the existing typed aggregate kernels.
//!
//! This is an explicit materialization boundary after direct/Dict admission,
//! not encoded execution. Native primitive owners and validity are retained for
//! one source array without an adapter numeric payload copy or width expansion.
//! Provider decode/validity allocations and RSS are not a claimed bound.
//! Aggregate first passes and refinements supply their configured query context.
//! Standalone residual-expression compatibility entrypoints retain their existing
//! default-session context; this module does not introduce a global query context.
//! Pinned Filter/FoR/BitPacked kernels can allocate `BufferMut`/`PrimitiveBuilder`
//! storage outside `HostAllocator`; using the configured context does not make
//! those provider allocations budget-owned. Even `builder_with_capacity_in` ignores
//! its allocator in this pinned release. Adapter copy and pool ownership claims
//! stay scoped; imported reserved native buffers retain their existing credits.

use std::{collections::BTreeSet, ops::Deref, time::Instant};

use shardloom_core::{Result, ShardLoomError};
use vortex::array::{
    ArrayRef, ExecutionCtx,
    arrays::PrimitiveArray,
    dtype::{DType, PType},
};

use super::{AggregateDirectColumnAccessor, NativeNumericOwner, vortex_error};

#[derive(Clone, Default)]
pub(super) struct NativeNumericAccessorWork {
    pub(super) encoded_reduction: super::encoded_numeric_reduction::EncodedNumericReductionWork,
    calls: u64,
    rows: u64,
    source_logical_bytes: u64,
    canonical_logical_bytes: u64,
    typed_value_bytes_copied: u64,
    elapsed_nanos: u128,
    max_array_rows: u64,
    columns: BTreeSet<String>,
}

impl NativeNumericAccessorWork {
    /// Record a completed provider execution and retained primitive owner.
    /// Callers keep the original typed provider error until execution succeeds.
    pub(super) fn record_native_owner(
        &mut self,
        column: &str,
        rows: u64,
        source_logical_bytes: u64,
        canonical_logical_bytes: u64,
        elapsed_nanos: u128,
    ) -> Result<()> {
        self.add(&Self {
            calls: 1,
            rows,
            source_logical_bytes,
            canonical_logical_bytes,
            elapsed_nanos,
            max_array_rows: rows,
            columns: BTreeSet::from([column.to_owned()]),
            ..Self::default()
        })
    }

    pub(super) fn add(&mut self, other: &Self) -> Result<()> {
        self.encoded_reduction.add(&other.encoded_reduction)?;
        for (total, increment) in [
            (&mut self.calls, other.calls),
            (&mut self.rows, other.rows),
            (&mut self.source_logical_bytes, other.source_logical_bytes),
            (
                &mut self.canonical_logical_bytes,
                other.canonical_logical_bytes,
            ),
            (
                &mut self.typed_value_bytes_copied,
                other.typed_value_bytes_copied,
            ),
        ] {
            *total = total
                .checked_add(increment)
                .ok_or_else(|| failed("work counter overflow"))?;
        }
        self.elapsed_nanos = self
            .elapsed_nanos
            .checked_add(other.elapsed_nanos)
            .ok_or_else(|| failed("elapsed work counter overflow"))?;
        self.max_array_rows = self.max_array_rows.max(other.max_array_rows);
        self.columns.extend(other.columns.iter().cloned());
        Ok(())
    }

    pub(super) fn observed(&self) -> bool {
        self.calls != 0
    }

    pub(super) fn annotate(&self, summary: &mut String) -> Result<()> {
        let mut value: serde_json::Value =
            serde_json::from_str(summary).map_err(|error| failed(&error.to_string()))?;
        let object = value
            .as_object_mut()
            .ok_or_else(|| failed("summary is not an object"))?;
        self.encoded_reduction.annotate(object);
        object.insert("aggregate_native_numeric_accessor".into(), serde_json::json!({
            "native_decode_calls": self.calls,
            "rows": self.rows,
            "source_array_nbytes_estimate": self.source_logical_bytes,
            "canonical_array_nbytes_estimate": self.canonical_logical_bytes,
            "typed_value_bytes_copied": self.typed_value_bytes_copied,
            "decode_and_typed_copy_nanos": u64::try_from(self.elapsed_nanos).unwrap_or(u64::MAX),
            "max_source_array_rows": self.max_array_rows,
            "columns": self.columns,
            "scope": "retained_aggregate_attempt;native_primitive_owner_original_width_and_validity;no_adapter_numeric_payload_copy;provider_decode_filter_and_validity_work_may_allocate;Array_nbytes_estimates_not_unique_allocations;referenced_child_data_can_exceed_selected_rows;dictionary_gather_copies_excluded;no_Arrow_or_external_engine;not_zero_decode_or_RSS_bound;decode_and_typed_copy_nanos_is_legacy_field_for_decode_and_owner_setup",
        }));
        if self.observed() {
            let mut materialized_columns = self.columns.clone();
            match object.get("aggregate_materialized_accessor_columns") {
                Some(serde_json::Value::String(columns)) if columns != "none" => {
                    materialized_columns.extend(columns.split(',').map(str::to_owned));
                }
                None | Some(serde_json::Value::String(_)) => {}
                Some(_) => {
                    return Err(failed(
                        "materialized accessor column display is not a string",
                    ));
                }
            }
            // This legacy comma-separated field is a display union. The nested
            // native work object retains exact column names as a structured array.
            object.insert(
                "aggregate_materialized_accessor_columns".into(),
                materialized_columns
                    .into_iter()
                    .collect::<Vec<_>>()
                    .join(",")
                    .into(),
            );
            object.insert(
                "aggregate_accessor_materialization_status".into(),
                "native_numeric_array_decode_with_typed_accessors_and_optional_other_accessors"
                    .into(),
            );
        }
        *summary = value.to_string();
        Ok(())
    }
}

#[derive(Default)]
pub(super) struct AggregateAccessorBatch {
    pub(super) values: Vec<AggregateDirectColumnAccessor>,
    pub(super) numeric_work: NativeNumericAccessorWork,
}

impl Deref for AggregateAccessorBatch {
    type Target = [AggregateDirectColumnAccessor];
    fn deref(&self) -> &Self::Target {
        &self.values
    }
}

/// Returns `None` only for a dtype outside this admitted numeric family.
/// Once admitted, native provider failures propagate without another attempt.
pub(super) fn decode(
    column: &str,
    array: &ArrayRef,
    ctx: &mut ExecutionCtx,
) -> Result<Option<(AggregateDirectColumnAccessor, NativeNumericAccessorWork)>> {
    if !matches!(array.dtype(), DType::Primitive(ptype, _) if *ptype != PType::F16) {
        return Ok(None);
    }
    #[cfg(test)]
    observe_test_query_context(ctx)?;
    let started = Instant::now();
    let primitive = array
        .clone()
        .execute::<PrimitiveArray>(ctx)
        .map_err(vortex_error)?;
    if primitive.len() != array.len() || primitive.dtype() != array.dtype() {
        return Err(failed("native execution changed dtype or row count"));
    }
    let canonical_logical_bytes = primitive.nbytes();
    let accessor =
        AggregateDirectColumnAccessor::NativeNumeric(NativeNumericOwner::new(primitive, ctx)?);
    if accessor.len() != array.len() {
        return Err(failed("typed accessor changed row count"));
    }
    let rows = u64::try_from(array.len()).map_err(|_| failed("row count overflow"))?;
    let work = NativeNumericAccessorWork {
        encoded_reduction: super::encoded_numeric_reduction::EncodedNumericReductionWork::default(),
        calls: 1,
        rows,
        source_logical_bytes: array.nbytes(),
        canonical_logical_bytes,
        typed_value_bytes_copied: 0,
        elapsed_nanos: started.elapsed().as_nanos(),
        max_array_rows: rows,
        columns: BTreeSet::from([column.to_owned()]),
    };
    Ok(Some((accessor, work)))
}

pub(super) fn failed(message: &str) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "native numeric aggregate accessor {message}; no fallback execution was attempted"
    ))
}

// A session-local test effect proves the production query supplied its exact
// configured context, independently of the provider's partial allocator coverage.
// This code is absent from production, has no shared/global state, and does not
// replace or emulate a codec. Malformed native codec error propagation and the
// pinned builder allocator gap are exercised separately.
#[cfg(test)]
#[derive(Debug)]
struct QueryContextProbe {
    allocator: vortex::array::memory::HostAllocatorRef,
    calls: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}

#[cfg(test)]
impl vortex::session::SessionVar for QueryContextProbe {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

#[cfg(test)]
impl vortex::session::VortexSessionVar for QueryContextProbe {}

#[cfg(test)]
fn observe_test_query_context(ctx: &ExecutionCtx) -> Result<()> {
    use vortex::session::SessionExt as _;
    if let Some(probe) = ctx.session().get_opt::<QueryContextProbe>() {
        if !std::sync::Arc::ptr_eq(&ctx.allocator(), &probe.allocator) {
            return Err(failed("test configured allocator identity mismatch"));
        }
        probe
            .calls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        return Err(failed("test injected query-context failure"));
    }
    Ok(())
}

#[cfg(test)]
#[path = "native_numeric_accessor_tests.rs"]
mod tests;
