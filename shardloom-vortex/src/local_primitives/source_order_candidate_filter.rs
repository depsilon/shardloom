//! Conservative retained-key rejection before constructing string accessors.
//! The existing complete-key consumer remains responsible for exact counting.

use vortex::{
    array::{ArrayRef, IntoArray as _, arrays::FilterArray, dtype::DType},
    mask::Mask,
};

use super::{
    AggregateValueTransform, GroupedAggregateStates, NumericUtf8GroupRoles, Result, ShardLoomError,
    aggregate_column_accessor_in_context, for_each_aggregate_integer_key,
    logical_field_from_native_array,
};

#[derive(Default)]
pub(super) struct Work {
    chunks: u64,
    rows: u64,
    retained_rows: u64,
}

impl Work {
    pub(super) fn summary(&self) -> serde_json::Value {
        serde_json::json!({
            "chunks": self.chunks,
            "rows": self.rows,
            "retained_rows": self.retained_rows,
            "scope": "closed_source_order_integer_utf8_count_keys;numeric_membership_before_utf8_accessor;complete_key_equality_for_survivors;chunk_local_mask;no_physical_read_or_RSS_bound",
        })
    }
}

impl GroupedAggregateStates<'_> {
    pub(super) fn source_order_candidate_chunk(
        &mut self,
        chunk: &ArrayRef,
        columns: &[String],
        row_indices: Option<&[usize]>,
    ) -> Result<Option<ArrayRef>> {
        // A previous successful direct update certifies the retained state shape.
        // Recheck this chunk's logical dtypes before executing its numeric provider.
        if row_indices.is_some()
            || self.request.offset != 0
            || !self.admits_count_star_direct_updates()
            || !self.source_order_numeric_utf8_dictionary_direct_updates
            || !self.source_order_group_admission_closed()
            || self.group_order.is_empty()
            || self.group_order.len() > 64
            || self.group_columns.len() != 2
            || self.group_key_indices.len() != 2
        {
            return Ok(None);
        }
        let Some((roles, numeric_array)) = self.source_order_candidate_roles(chunk, columns)?
        else {
            return Ok(None);
        };
        let mut retained = rustc_hash::FxHashSet::default();
        for key in &self.group_order {
            let Some((numeric, _)) = self.source_order_numeric_utf8_key_parts(key, roles)? else {
                return Ok(None);
            };
            retained.insert((numeric.bits, numeric.signed));
        }
        let (numeric, work) = aggregate_column_accessor_in_context(
            &columns[roles.numeric_column],
            &numeric_array,
            &mut self.native_execution_ctx,
        )?;
        self.native_numeric_accessor_work.add(&work)?;
        if numeric.len() != chunk.len() {
            return Err(ShardLoomError::InvalidOperation(
                "local Vortex retained-key selection numeric row count changed; no fallback execution was attempted".into(),
            ));
        }
        let mut selected = Vec::new();
        selected.try_reserve_exact(chunk.len()).map_err(|error| {
            ShardLoomError::InvalidOperation(format!(
                "local Vortex retained-key selection reservation failed: {error}; no fallback execution was attempted"
            ))
        })?;
        for_each_aggregate_integer_key(&numeric, "retained source-order key", |_, key| {
            selected.push(retained.contains(&(key.bits, key.signed)));
            Ok(())
        })?;
        let mask = Mask::from_iter(selected);
        self.source_order_candidate_filter.chunks += 1;
        self.source_order_candidate_filter.rows += chunk.len() as u64;
        self.source_order_candidate_filter.retained_rows += mask.true_count() as u64;
        Ok(Some(FilterArray::new(chunk.clone(), mask).into_array()))
    }

    fn source_order_candidate_roles(
        &self,
        chunk: &ArrayRef,
        columns: &[String],
    ) -> Result<Option<(NumericUtf8GroupRoles, ArrayRef)>> {
        let mut numeric = None;
        let mut utf8 = None;
        for &group_index in &self.group_key_indices {
            let group = &self.group_columns[group_index];
            if !matches!(group.transform, AggregateValueTransform::Identity)
                || !group.extra_column_indices.is_empty()
            {
                return Ok(None);
            }
            let Some(column) = columns.get(group.column_index) else {
                return Ok(None);
            };
            let array = logical_field_from_native_array(chunk, column)?;
            if array.dtype().is_nullable() {
                return Ok(None);
            }
            match array.dtype() {
                DType::Primitive(ptype, _) if ptype.is_int() && numeric.is_none() => {
                    numeric = Some((group_index, group.column_index, array));
                }
                DType::Utf8(_) if utf8.is_none() => {
                    utf8 = Some((group_index, group.column_index));
                }
                _ => return Ok(None),
            }
        }
        Ok(numeric.zip(utf8).map(
            |((numeric_group, numeric_column, array), (utf8_group, utf8_column))| {
                (
                    NumericUtf8GroupRoles {
                        numeric_group,
                        numeric_column,
                        utf8_group,
                        utf8_column,
                    },
                    array,
                )
            },
        ))
    }
}

#[cfg(test)]
#[path = "source_order_candidate_filter_tests.rs"]
mod tests;
