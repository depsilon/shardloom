//! Bounded exact match coordinates borrow the original native value owners.
//! This is an internal consumer input, never a joined array or payload table.

use super::{
    logical_field_from_native_array,
    native_capacity::ReservedVec,
    native_relational_batch::{Batch, failed},
    native_relational_keys::{KeyColumn, KeyLookupCache},
    native_relational_rows::Rows,
    result_batch, vortex_error,
};
use crate::resident_session::NativeExecutionContext;
use shardloom_core::Result;
use std::hash::Hasher as _;
use vortex::array::{
    ArrayRef, IntoArray,
    arrays::StructArray,
    dtype::{DType, FieldNames},
    memory::HostMemorySessionExt as _,
    validity::Validity,
};

#[derive(Clone, Copy)]
pub(super) struct Origin {
    pub(super) source: usize,
    pub(super) column: usize,
}

struct Segment {
    start: usize,
    values: KeyColumn,
}

pub(super) struct Column {
    segments: ReservedVec<Segment>,
    rows: usize,
}

impl Column {
    pub(super) fn key(&self, row: usize) -> Result<(&KeyColumn, usize)> {
        if row >= self.rows {
            return Err(failed("match source ordinal exceeds its native column"));
        }
        let segment = self
            .segments
            .values
            .partition_point(|segment| segment.start <= row)
            - 1;
        let segment = &self.segments.values[segment];
        Ok((&segment.values, row - segment.start))
    }

    pub(super) fn single(&self) -> Option<&KeyColumn> {
        (self.segments.values.len() == 1).then(|| &self.segments.values[0].values)
    }
}

pub(super) struct Source {
    columns: ReservedVec<Column>,
}

impl Source {
    /// Metadata/container admission is optional. Every accepted leaf is already
    /// resident; selected value checks happen later, at the original join flush.
    pub(super) fn try_new<'a>(
        fields: &[(String, DType)],
        batches: &(impl ExactSizeIterator<Item = &'a ArrayRef> + Clone),
        context: &NativeExecutionContext<'_>,
    ) -> Result<Option<Self>> {
        let Some(mut columns) = ReservedVec::try_with_capacity(context.memory(), fields.len())?
        else {
            return Ok(None);
        };
        for (name, dtype) in fields {
            context.check_cancelled()?;
            let Some(mut segments) =
                ReservedVec::try_with_capacity(context.memory(), batches.len())?
            else {
                return Ok(None);
            };
            let mut rows = 0usize;
            for array in (*batches).clone() {
                let column = logical_field_from_native_array(array, name)?;
                if column.dtype() != dtype {
                    return Ok(None);
                }
                let Some(values) = KeyColumn::try_borrow(&column, context)? else {
                    return Ok(None);
                };
                segments.values.push(Segment {
                    start: rows,
                    values,
                });
                rows = rows
                    .checked_add(array.len())
                    .ok_or_else(|| failed("match source cardinality overflow"))?;
            }
            columns.values.push(Column { segments, rows });
        }
        Ok(Some(Self { columns }))
    }

    pub(super) fn column(&self, column: usize) -> Result<&Column> {
        self.columns
            .values
            .get(column)
            .ok_or_else(|| failed("bound match source column is absent"))
    }
}

/// Maps every admitted origin to its original source ordinal. A frame is no
/// larger than one ordinary join flush, and its vector capacities own credits.
pub(super) struct Frame {
    pub(super) rows: ReservedVec<ReservedVec<usize>>,
}

impl Frame {
    pub(super) fn extend(
        parent: Option<&Self>,
        parents: &[usize],
        right: &[usize],
        context: &NativeExecutionContext<'_>,
    ) -> Result<Self> {
        if parents.len() != right.len() {
            return Err(failed("match coordinate lengths differ"));
        }
        let prior = parent.map_or(1, |parent| parent.rows.values.len());
        let mut rows = ReservedVec::new(context.memory())?;
        rows.reserve(prior + 1)?;
        for source in 0..prior {
            let mut selected = ReservedVec::new(context.memory())?;
            selected.reserve(parents.len())?;
            for (position, &row) in parents.iter().enumerate() {
                if position.is_multiple_of(1024) {
                    context.check_cancelled()?;
                }
                selected.values.push(match parent {
                    None => row,
                    Some(parent) => *parent.rows.values[source]
                        .values
                        .get(row)
                        .ok_or_else(|| failed("parent match coordinate is absent"))?,
                });
            }
            rows.values.push(selected);
        }
        let mut selected = ReservedVec::new(context.memory())?;
        selected.reserve(right.len())?;
        selected.values.extend_from_slice(right);
        rows.values.push(selected);
        Ok(Self { rows })
    }

    pub(super) fn len(&self) -> usize {
        self.rows.values[0].values.len()
    }

    pub(super) fn source_rows(&self, source: usize) -> Result<&[usize]> {
        self.rows
            .values
            .get(source)
            .map(|rows| rows.values.as_slice())
            .ok_or_else(|| failed("match source coordinates are absent"))
    }
}

pub(super) struct Input<'a> {
    pub(super) fact: &'a Source,
    pub(super) dimensions: &'a [Source],
    pub(super) frame: &'a Frame,
    pub(super) groups: &'a [Origin],
}

impl Input<'_> {
    pub(super) fn column(&self, origin: Origin) -> Result<&Column> {
        if origin.source == 0 {
            self.fact.column(origin.column)
        } else {
            self.dimensions
                .get(origin.source - 1)
                .ok_or_else(|| failed("match dimension is absent"))?
                .column(origin.column)
        }
    }

    pub(super) fn key(&self, origin: Origin, row: usize) -> Result<(&KeyColumn, usize)> {
        let ordinal = *self
            .frame
            .source_rows(origin.source)?
            .get(row)
            .ok_or_else(|| failed("match row exceeds its selection"))?;
        self.column(origin)?.key(ordinal)
    }

    pub(super) fn validate(
        &self,
        columns: &[Origin],
        context: &NativeExecutionContext<'_>,
    ) -> Result<()> {
        // Column-major, then selected-row order, including discarded fields.
        for &origin in columns {
            for row in 0..self.frame.len() {
                if row.is_multiple_of(1024) {
                    context.check_cancelled()?;
                }
                let (values, row) = self.key(origin, row)?;
                if let KeyColumn::Numeric(values) = values {
                    if let Some(value) = values.numeric_value(row)? {
                        result_batch::validate_numeric_output(value, context.float_policy())?;
                    }
                } else {
                    values.validate_at(row)?;
                }
            }
        }
        Ok(())
    }
}

impl Rows for Input<'_> {
    fn len(&self) -> usize {
        self.frame.len()
    }

    fn hash(&self, row: usize, nulls_equal: bool) -> Result<Option<u64>> {
        let mut hash = rustc_hash::FxHasher::default();
        for &origin in self.groups {
            let (key, row) = self.key(origin, row)?;
            if !key.hash_into(row, &mut hash)? && !nulls_equal {
                return Ok(None);
            }
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
        for (index, &origin) in self.groups.iter().enumerate() {
            let (key, row) = self.key(origin, row)?;
            if !batch.key(index)?.equals_at(other, key, row, nulls_equal)? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn equal(&self, row: usize, other: usize, nulls_equal: bool) -> Result<bool> {
        for &origin in self.groups {
            let (key, row) = self.key(origin, row)?;
            let (right, other) = self.key(origin, other)?;
            if !key.equals_at(row, right, other, nulls_equal)? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn key_cache(&self, _: &NativeExecutionContext<'_>) -> Result<Option<KeyLookupCache<'_>>> {
        Ok(None)
    }

    fn gather(
        &self,
        fields: &[(String, DType)],
        rows: &[usize],
        context: &NativeExecutionContext<'_>,
    ) -> Result<ArrayRef> {
        if fields.len() != self.groups.len() {
            return Err(failed("matched group field arity changed"));
        }
        let mut columns = ReservedVec::new(context.memory())?;
        columns.reserve(fields.len())?;
        for ((_, dtype), &origin) in fields.iter().zip(self.groups) {
            context.check_cancelled()?;
            columns
                .values
                .push(result_batch::build_column_with_float_policy(
                    dtype,
                    rows.len(),
                    &context.native_session().host_allocator(),
                    context.float_policy(),
                    |position| {
                        let (key, row) = self.key(origin, rows[position])?;
                        key.borrowed_value(row)
                    },
                )?);
        }
        let (columns, _ownership) = columns.into_parts();
        StructArray::try_new(
            fields
                .iter()
                .map(|(name, _)| name.as_str())
                .collect::<FieldNames>(),
            columns,
            rows.len(),
            Validity::NonNullable,
        )
        .map(IntoArray::into_array)
        .map_err(vortex_error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resident_session::ResidentVortexSession;
    use shardloom_exec::compute_pool::CancellationToken;

    #[test]
    fn direct_join_aggregate_match_frame_refunds_on_denial_and_cancel() {
        let session = ResidentVortexSession::new(1 << 20, 1).unwrap();
        let memory = session.memory().clone();
        let token = CancellationToken::default();
        let error = session
            .with_native_execution_context(&token, |context| {
                let parent = Frame::extend(None, &[3, 7], &[4, 2], context)?;
                let initial = memory.snapshot().reserved_bytes;
                let pressure = memory.reserve(memory.snapshot().limit_bytes - initial - 1)?;
                assert!(Frame::extend(Some(&parent), &[1, 0], &[9, 8], context).is_err());
                assert_eq!(memory.snapshot().reserved_bytes, initial + pressure.bytes());
                drop(pressure);
                let child = Frame::extend(Some(&parent), &[1, 0], &[9, 8], context)?;
                assert_eq!(child.source_rows(0)?, &[7, 3]);
                assert_eq!(child.source_rows(1)?, &[2, 4]);
                assert_eq!(child.source_rows(2)?, &[9, 8]);
                drop(child);
                assert_eq!(memory.snapshot().reserved_bytes, initial);
                token.cancel();
                assert!(Frame::extend(Some(&parent), &[1, 0], &[9, 8], context).is_err());
                assert_eq!(memory.snapshot().reserved_bytes, initial);
                assert_eq!(parent.source_rows(0)?, &[3, 7]);
                Ok(())
            })
            .unwrap_err();
        assert!(error.to_string().contains("execution cancelled"), "{error}");
        drop(session);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}
