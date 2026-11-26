//! The exact grouping owner accepts either an ordinary batch or bounded
//! references to matched source values. Only new group payloads escape.

use super::{
    native_numeric_owner::NativeNumericOwner,
    native_relational_batch::{Batch, failed, index_array, take_batch, take_column},
    native_relational_keys::{Cell, KeyColumn, KeyLookupCache, VariableDomain},
    vortex_error,
};
use crate::resident_session::NativeExecutionContext;
use shardloom_core::Result;
use std::hash::Hasher as _;
use vortex::{
    array::{
        ArrayRef,
        arrays::StructArray,
        dtype::{DType, FieldNames},
        validity::Validity,
    },
    mask::Mask,
};

pub(super) trait Rows {
    fn len(&self) -> usize;
    fn hash(&self, row: usize, nulls_equal: bool) -> Result<Option<u64>>;
    fn equal_batch(
        &self,
        row: usize,
        batch: &Batch,
        other: usize,
        nulls_equal: bool,
    ) -> Result<bool>;
    fn equal(&self, row: usize, other: usize, nulls_equal: bool) -> Result<bool>;
    fn key_cache(&self, context: &NativeExecutionContext<'_>)
    -> Result<Option<KeyLookupCache<'_>>>;
    fn gather(
        &self,
        fields: &[(String, DType)],
        rows: &[usize],
        context: &NativeExecutionContext<'_>,
    ) -> Result<ArrayRef>;
    fn variable_dict_epoch(&self) -> Option<(&VariableDomain, &Mask, &NativeNumericOwner, bool)> {
        None
    }
}

impl Rows for Batch {
    fn len(&self) -> usize {
        self.array.len()
    }
    fn hash(&self, row: usize, nulls_equal: bool) -> Result<Option<u64>> {
        self.hash(row, nulls_equal)
    }
    fn equal_batch(
        &self,
        row: usize,
        batch: &Batch,
        other: usize,
        nulls_equal: bool,
    ) -> Result<bool> {
        batch.equal(other, self, row, nulls_equal)
    }
    fn equal(&self, row: usize, other: usize, nulls_equal: bool) -> Result<bool> {
        self.equal(row, self, other, nulls_equal)
    }
    fn key_cache(
        &self,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Option<KeyLookupCache<'_>>> {
        self.key_cache(context)
    }
    fn gather(
        &self,
        fields: &[(String, DType)],
        rows: &[usize],
        context: &NativeExecutionContext<'_>,
    ) -> Result<ArrayRef> {
        take_batch(&self.array, fields, rows, context)
    }
    fn variable_dict_epoch(&self) -> Option<(&VariableDomain, &Mask, &NativeNumericOwner, bool)> {
        self.variable_dict_epoch()
    }
}

pub(super) struct DistinctMeasureRows<'a> {
    group_ordinals: Option<&'a [usize]>,
    measure_column: &'a ArrayRef,
    measure_values: &'a KeyColumn,
    valid_rows: &'a [usize],
}

impl<'a> DistinctMeasureRows<'a> {
    pub(super) fn new(
        group_ordinals: Option<&'a [usize]>,
        measure_column: &'a ArrayRef,
        measure_values: &'a KeyColumn,
        valid_rows: &'a [usize],
    ) -> Self {
        Self {
            group_ordinals,
            measure_column,
            measure_values,
            valid_rows,
        }
    }

    fn group_at(&self, row: usize) -> usize {
        self.group_ordinals
            .map_or(0, |ordinals| ordinals[self.valid_rows[row]])
    }
}

impl Rows for DistinctMeasureRows<'_> {
    fn len(&self) -> usize {
        self.valid_rows.len()
    }

    fn hash(&self, row: usize, nulls_equal: bool) -> Result<Option<u64>> {
        let mut hash = rustc_hash::FxHasher::default();
        let group = u64::try_from(self.group_at(row)).map_err(vortex_error)?;
        hash.write_u8(2);
        hash.write_u64(group);
        if !self
            .measure_values
            .hash_into(self.valid_rows[row], &mut hash)?
            && !nulls_equal
        {
            return Ok(None);
        }
        Ok(Some(hash.finish()))
    }

    fn equal_batch(
        &self,
        row: usize,
        batch: &Batch,
        other: usize,
        nulls_equal: bool,
    ) -> Result<bool> {
        let group = u64::try_from(self.group_at(row)).map_err(vortex_error)?;
        let other_group = batch
            .key_column(0)
            .ok_or_else(|| failed("distinct group key column is absent"))?
            .cell(other)?;
        if other_group != Cell::NonnegativeInteger(group) {
            return Ok(false);
        }
        let other_value = batch
            .key_column(1)
            .ok_or_else(|| failed("distinct value key column is absent"))?;
        self.measure_values
            .equals_at(self.valid_rows[row], other_value, other, nulls_equal)
    }

    fn equal(&self, row: usize, other: usize, nulls_equal: bool) -> Result<bool> {
        if self.group_at(row) != self.group_at(other) {
            return Ok(false);
        }
        self.measure_values.equals_at(
            self.valid_rows[row],
            self.measure_values,
            self.valid_rows[other],
            nulls_equal,
        )
    }

    fn key_cache(
        &self,
        _context: &NativeExecutionContext<'_>,
    ) -> Result<Option<KeyLookupCache<'_>>> {
        Ok(None)
    }

    fn gather(
        &self,
        fields: &[(String, DType)],
        rows: &[usize],
        context: &NativeExecutionContext<'_>,
    ) -> Result<ArrayRef> {
        let ordinals = index_array(rows.len(), false, context, |row| {
            Ok(Some(self.group_at(rows[row])))
        })?;
        let selection = index_array(rows.len(), false, context, |row| {
            Ok(Some(self.valid_rows[rows[row]]))
        })?;
        let value_dtype = fields
            .get(1)
            .map(|(_, dtype)| dtype)
            .ok_or_else(|| failed("distinct measure output schema is incomplete"))?;
        let column = take_column(self.measure_column, &selection, value_dtype, context)?;
        StructArray::try_new(
            FieldNames::from(["group", "value"]),
            vec![ordinals, column],
            rows.len(),
            Validity::NonNullable,
        )
        .map_err(vortex_error)
        .map(vortex::array::IntoArray::into_array)
    }
}
