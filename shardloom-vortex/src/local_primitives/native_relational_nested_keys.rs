//! Recursive logical keys over native children, never decoded list/struct rows.

use super::{KeyColumn, failed, vortex_error};
use crate::local_primitives::{native_capacity::ReservedVec, native_list};
use shardloom_core::Result;
use shardloom_exec::{
    compute_pool::CancellationToken,
    live_memory::{LiveMemoryPool, MemoryLease},
};
use std::{cmp::Ordering, hash::Hasher as _};
use vortex::{
    array::{
        ArrayRef, ExecutionCtx,
        arrays::{StructArray, struct_::StructArrayExt as _},
        dtype::DType,
    },
    mask::Mask,
};

enum Children {
    Struct {
        valid: Mask,
        fields: ReservedVec<KeyColumn>,
    },
    List {
        coordinates: native_list::Coordinates,
        elements: Box<KeyColumn>,
    },
}

pub(in crate::local_primitives) struct Column {
    children: Children,
    dtype: DType,
    rows: usize,
    cancellation: CancellationToken,
    _metadata: MemoryLease,
    _schema: Option<MemoryLease>,
}

impl Column {
    pub(super) fn new(
        array: &ArrayRef,
        context: &mut ExecutionCtx,
        memory: &LiveMemoryPool,
        cancellation: &CancellationToken,
        schema: Option<MemoryLease>,
    ) -> Result<Self> {
        cancellation.check()?;
        // Includes this boxed owner and the list's boxed child. The root schema
        // credit covers recursive logical metadata; vectors reserve separately.
        let metadata = memory
            .reserve((std::mem::size_of::<Self>() + std::mem::size_of::<KeyColumn>()) as u64)?;
        let children = match array.dtype() {
            DType::Struct(..) => {
                let values = array
                    .clone()
                    .execute::<StructArray>(context)
                    .map_err(vortex_error)?;
                if values.len() != array.len() || values.dtype() != array.dtype() {
                    return Err(failed("struct key execution changed dtype or row count"));
                }
                let valid = values
                    .validity()
                    .map_err(vortex_error)?
                    .execute_mask(values.len(), context)
                    .map_err(vortex_error)?;
                let mut fields = ReservedVec::new(memory)?;
                fields.reserve(values.struct_fields().nfields())?;
                for field in values.iter_unmasked_fields() {
                    cancellation.check()?;
                    fields.values.push(KeyColumn::new_inner(
                        field,
                        context,
                        memory,
                        cancellation,
                        None,
                    )?);
                }
                Children::Struct { valid, fields }
            }
            DType::List(..) | DType::FixedSizeList(..) => {
                let (elements, coordinates) =
                    native_list::Column::new(array.clone(), context)?.into_indexed(context)?;
                Children::List {
                    elements: Box::new(KeyColumn::new_inner(
                        &elements,
                        context,
                        memory,
                        cancellation,
                        None,
                    )?),
                    coordinates,
                }
            }
            _ => return Err(failed("nested key has a non-nested dtype")),
        };
        cancellation.check()?;
        Ok(Self {
            children,
            dtype: array.dtype().clone(),
            rows: array.len(),
            cancellation: cancellation.clone(),
            _metadata: metadata,
            _schema: schema,
        })
    }

    pub(super) fn len(&self) -> usize {
        self.rows
    }

    pub(super) fn is_null(&self, row: usize) -> Result<bool> {
        if row >= self.rows {
            return Err(failed("nested key row exceeds native input"));
        }
        Ok(match &self.children {
            Children::Struct { valid, .. } => !valid.value(row),
            Children::List { coordinates, .. } => coordinates.at(row)?.is_none(),
        })
    }

    pub(super) fn compare_at(
        &self,
        row: usize,
        other: &Self,
        other_row: usize,
    ) -> Result<Ordering> {
        self.cancellation.check()?;
        other.cancellation.check()?;
        if !self.dtype.eq_ignore_nullability(&other.dtype) {
            return Err(failed(
                "nested keys require identical logical structure and leaf types",
            ));
        }
        match (self.is_null(row)?, other.is_null(other_row)?) {
            (true, true) => return Ok(Ordering::Equal),
            (true, false) => return Ok(Ordering::Less),
            (false, true) => return Ok(Ordering::Greater),
            (false, false) => {}
        }
        match (&self.children, &other.children) {
            (Children::Struct { fields: left, .. }, Children::Struct { fields: right, .. }) => {
                for (left, right) in left.values.iter().zip(&right.values) {
                    self.cancellation.check()?;
                    let order = left.compare_at(row, right, other_row)?;
                    if order != Ordering::Equal {
                        return Ok(order);
                    }
                }
                Ok(Ordering::Equal)
            }
            (
                Children::List {
                    coordinates: left,
                    elements: left_elements,
                },
                Children::List {
                    coordinates: right,
                    elements: right_elements,
                },
            ) => {
                let (left_start, left_count) = left
                    .at(row)?
                    .ok_or_else(|| failed("nested key validity changed"))?;
                let (right_start, right_count) = right
                    .at(other_row)?
                    .ok_or_else(|| failed("nested key validity changed"))?;
                for index in 0..left_count.min(right_count) {
                    if index.is_multiple_of(1024) {
                        self.cancellation.check()?;
                        other.cancellation.check()?;
                    }
                    let order = left_elements.compare_at(
                        left_start + index,
                        right_elements,
                        right_start + index,
                    )?;
                    if order != Ordering::Equal {
                        return Ok(order);
                    }
                }
                Ok(left_count.cmp(&right_count))
            }
            _ => Err(failed("nested key structure changed after binding")),
        }
    }

    pub(super) fn hash_into(&self, row: usize, hash: &mut rustc_hash::FxHasher) -> Result<bool> {
        self.cancellation.check()?;
        if self.is_null(row)? {
            hash.write_u8(0);
            return Ok(false);
        }
        match &self.children {
            Children::Struct { fields, .. } => {
                hash.write_u8(10);
                hash.write_usize(fields.values.len());
                let DType::Struct(dtype, _) = &self.dtype else {
                    return Err(failed("struct key dtype changed"));
                };
                for (name, field) in dtype.names().iter().zip(&fields.values) {
                    self.cancellation.check()?;
                    let name: &str = name.as_ref();
                    hash.write_usize(name.len());
                    hash.write(name.as_bytes());
                    field.hash_into(row, hash)?;
                }
            }
            Children::List {
                coordinates,
                elements,
            } => {
                hash.write_u8(if matches!(self.dtype, DType::List(..)) {
                    11
                } else {
                    12
                });
                let (start, count) = coordinates
                    .at(row)?
                    .ok_or_else(|| failed("list key validity changed"))?;
                hash.write_usize(count);
                for index in 0..count {
                    if index.is_multiple_of(1024) {
                        self.cancellation.check()?;
                    }
                    elements.hash_into(start + index, hash)?;
                }
            }
        }
        Ok(true)
    }

    pub(super) fn write_exact_key(
        &self,
        row: usize,
        output: &mut impl std::fmt::Write,
    ) -> Result<()> {
        self.cancellation.check()?;
        if self.is_null(row)? {
            return output.write_str("n;").map_err(vortex_error);
        }
        match &self.children {
            Children::Struct { fields, .. } => {
                write!(output, "S{}:", fields.values.len()).map_err(vortex_error)?;
                let DType::Struct(dtype, _) = &self.dtype else {
                    return Err(failed("struct key dtype changed"));
                };
                for (name, field) in dtype.names().iter().zip(&fields.values) {
                    self.cancellation.check()?;
                    let name: &str = name.as_ref();
                    write!(output, "{}:{name}", name.len()).map_err(vortex_error)?;
                    field.write_exact_key(row, output, &self.cancellation)?;
                }
            }
            Children::List {
                coordinates,
                elements,
            } => {
                let (start, count) = coordinates
                    .at(row)?
                    .ok_or_else(|| failed("list key validity changed"))?;
                write!(
                    output,
                    "{}{count}:",
                    if matches!(self.dtype, DType::List(..)) {
                        'L'
                    } else {
                        'F'
                    }
                )
                .map_err(vortex_error)?;
                for index in 0..count {
                    if index.is_multiple_of(1024) {
                        self.cancellation.check()?;
                    }
                    elements.write_exact_key(start + index, output, &self.cancellation)?;
                }
            }
        }
        Ok(())
    }
}
