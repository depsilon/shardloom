//! Native per-array decoding into the existing typed aggregate kernels.
//!
//! This is an explicit materialization boundary after direct/Dict admission,
//! not encoded execution. Temporary native buffers and copied typed values are
//! scoped to one source array. Provider allocation/RSS is not a claimed bound.

use std::{collections::BTreeSet, ops::Deref, time::Instant};

use shardloom_core::{Result, ShardLoomError};
use vortex::array::{
    ArrayRef, VortexSessionExecute as _,
    arrays::PrimitiveArray,
    dtype::{DType, PType},
};

use super::{
    AggregateDirectColumnAccessor, primitive_aggregate_column_accessor_from_primitive, vortex_error,
};

#[derive(Clone, Default)]
pub(super) struct NativeNumericAccessorWork {
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
    pub(super) fn add(&mut self, other: &Self) -> Result<()> {
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
        object.insert("aggregate_native_numeric_accessor".into(), serde_json::json!({
            "native_decode_calls": self.calls,
            "rows": self.rows,
            "source_array_nbytes_estimate": self.source_logical_bytes,
            "canonical_array_nbytes_estimate": self.canonical_logical_bytes,
            "typed_value_bytes_copied": self.typed_value_bytes_copied,
            "decode_and_typed_copy_nanos": u64::try_from(self.elapsed_nanos).unwrap_or(u64::MAX),
            "max_source_array_rows": self.max_array_rows,
            "columns": self.columns,
            "scope": "retained_aggregate_attempt;one_source_array_native_primitive_execution_then_typed_values_copy;Array_nbytes_estimates_not_unique_allocations;referenced_child_data_can_exceed_selected_rows;null_mask_work_additional;no_Arrow_or_external_engine;not_zero_decode_or_RSS_bound",
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
) -> Result<Option<(AggregateDirectColumnAccessor, NativeNumericAccessorWork)>> {
    if !matches!(array.dtype(), DType::Primitive(ptype, _) if *ptype != PType::F16) {
        return Ok(None);
    }
    let started = Instant::now();
    let mut ctx = vortex::array::legacy_session().create_execution_ctx();
    let primitive = array
        .clone()
        .execute::<PrimitiveArray>(&mut ctx)
        .map_err(vortex_error)?;
    if primitive.len() != array.len() || primitive.dtype() != array.dtype() {
        return Err(failed("native execution changed dtype or row count"));
    }
    let canonical_logical_bytes = primitive.nbytes();
    let accessor = primitive_aggregate_column_accessor_from_primitive(&primitive)
        .ok_or_else(|| failed("native primitive validity or typed slice was unsupported"))?;
    if accessor.len() != array.len() {
        return Err(failed("typed accessor changed row count"));
    }
    let rows = u64::try_from(array.len()).map_err(|_| failed("row count overflow"))?;
    let typed_value_bytes_copied = rows
        .checked_mul(8)
        .ok_or_else(|| failed("typed byte count overflow"))?;
    let work = NativeNumericAccessorWork {
        calls: 1,
        rows,
        source_logical_bytes: array.nbytes(),
        canonical_logical_bytes,
        typed_value_bytes_copied,
        elapsed_nanos: started.elapsed().as_nanos(),
        max_array_rows: rows,
        columns: BTreeSet::from([column.to_owned()]),
    };
    Ok(Some((accessor, work)))
}

fn failed(message: &str) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "native numeric aggregate accessor {message}; no fallback execution was attempted"
    ))
}

#[cfg(test)]
#[path = "native_numeric_accessor_tests.rs"]
mod tests;
