//! Cross-field checks at the complete Arrow intake boundary, before masks change.

use crate::native_variable_tensor::Metadata;
use arrow_array::{Array, GenericListArray, OffsetSizeTrait, RecordBatch, cast::AsArray as _};
use arrow_schema::{DataType, Field};
use shardloom_core::{Result, ShardLoomError};
use shardloom_exec::live_memory::LiveMemoryPool;

const NAME: &str = "ARROW:extension:name";
const META: &str = "ARROW:extension:metadata";

fn failed(message: impl std::fmt::Display) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "rich value intake: {message}; no fallback execution was attempted"
    ))
}

fn contains(root: &Field) -> bool {
    let mut stack = vec![root];
    while let Some(field) = stack.pop() {
        if field
            .metadata()
            .get(NAME)
            .is_some_and(|name| name == crate::native_variable_tensor::ARROW_ID)
            || crate::native_vector::arrow::kind(field).is_some()
            || crate::native_spatial::arrow::kind(field).is_some()
            || crate::native_variant::arrow::is_variant(field)
        {
            return true;
        }
        match field.data_type() {
            DataType::List(child)
            | DataType::LargeList(child)
            | DataType::FixedSizeList(child, _) => stack.push(child),
            DataType::Struct(fields) => stack.extend(fields.iter().map(std::sync::Arc::as_ref)),
            _ => {}
        }
    }
    false
}

/// Compile metadata once per field. The existing bounded schema grant covers
/// the small traversal tree and JSON scratch; no per-row shape vector is made.
enum Validator {
    Variant,
    Tensor(Metadata, usize),
    Vector(crate::native_vector::Metadata),
    Spatial(crate::native_spatial::Kind),
    Struct(Vec<(usize, Self)>),
    List(Box<Self>),
    LargeList(Box<Self>),
    Fixed(Box<Self>, usize),
}

impl Drop for Validator {
    fn drop(&mut self) {
        let mut stack = Vec::new();
        let steal = |node: &mut Self, out: &mut Vec<Self>| match node {
            Self::Struct(fields) => {
                for (_, child) in std::mem::take(fields).into_iter().rev() {
                    out.push(child);
                }
            }
            Self::List(child) | Self::LargeList(child) | Self::Fixed(child, _) => {
                let inner = std::mem::replace(child.as_mut(), Self::Variant);
                out.push(inner);
            }
            Self::Variant | Self::Tensor(..) | Self::Vector(_) | Self::Spatial(_) => {}
        };
        steal(self, &mut stack);
        while let Some(mut node) = stack.pop() {
            steal(&mut node, &mut stack);
        }
    }
}

enum NewValidatorFrame<'a> {
    List,
    LargeList,
    Fixed(usize),
    Struct {
        pending: std::vec::IntoIter<(usize, &'a Field)>,
        current_index: usize,
        built: Vec<(usize, Validator)>,
    },
}

struct ValidateWork<'a> {
    validator: &'a Validator,
    array: &'a dyn Array,
    start: usize,
    end: usize,
}

impl Validator {
    fn leaf_or_kind(field: &Field) -> Result<Option<Self>> {
        if let Some(kind) = crate::native_spatial::arrow::kind(field) {
            crate::native_spatial::arrow::dtype(field).map_err(failed)?;
            return Ok(Some(Self::Spatial(kind)));
        }
        if crate::native_variant::arrow::is_variant(field) {
            if !crate::native_variant::arrow::admitted(field) {
                return Err(failed("invalid Variant storage declaration"));
            }
            return Ok(Some(Self::Variant));
        }
        if crate::native_vector::arrow::kind(field).is_some() {
            let dtype = crate::native_vector::arrow::dtype(field).map_err(failed)?;
            return Ok(Some(Self::Vector(
                crate::native_vector::metadata(&dtype)
                    .expect("validated vector metadata")
                    .clone(),
            )));
        }
        if field
            .metadata()
            .get(NAME)
            .is_some_and(|name| name == crate::native_variable_tensor::ARROW_ID)
        {
            if !crate::native_variable_tensor::arrow::admitted(field) {
                return Err(failed("invalid variable tensor storage"));
            }
            let DataType::Struct(fields) = field.data_type() else {
                unreachable!()
            };
            let DataType::FixedSizeList(_, rank) = fields[1].data_type() else {
                unreachable!()
            };
            let rank = usize::try_from(*rank).map_err(failed)?;
            let metadata = Metadata::parse(field.metadata().get(META).map_or("", String::as_str))
                .map_err(failed)?;
            metadata.validate_rank(rank).map_err(failed)?;
            return Ok(Some(Self::Tensor(metadata, rank)));
        }
        Ok(None)
    }

    fn new(root: &Field) -> Result<Self> {
        let mut stack = Vec::<NewValidatorFrame<'_>>::new();
        let mut current = root;
        loop {
            let mut completed = match Self::leaf_or_kind(current)? {
                Some(leaf) => leaf,
                None => match current.data_type() {
                    DataType::Struct(fields) => {
                        let mut pending = fields
                            .iter()
                            .enumerate()
                            .filter(|(_, field)| contains(field))
                            .map(|(index, field)| (index, field.as_ref()))
                            .collect::<Vec<_>>()
                            .into_iter();
                        if let Some((current_index, next_field)) = pending.next() {
                            stack.push(NewValidatorFrame::Struct {
                                pending,
                                current_index,
                                built: Vec::new(),
                            });
                            current = next_field;
                            continue;
                        }
                        Self::Struct(Vec::new())
                    }
                    DataType::List(child) => {
                        stack.push(NewValidatorFrame::List);
                        current = child;
                        continue;
                    }
                    DataType::LargeList(child) => {
                        stack.push(NewValidatorFrame::LargeList);
                        current = child;
                        continue;
                    }
                    DataType::FixedSizeList(child, width) => {
                        let width = usize::try_from(*width).map_err(failed)?;
                        stack.push(NewValidatorFrame::Fixed(width));
                        current = child;
                        continue;
                    }
                    _ => return Err(failed("unsupported containing rich-value layout")),
                },
            };
            loop {
                let Some(frame) = stack.pop() else {
                    return Ok(completed);
                };
                completed = match frame {
                    NewValidatorFrame::List => Self::List(Box::new(completed)),
                    NewValidatorFrame::LargeList => Self::LargeList(Box::new(completed)),
                    NewValidatorFrame::Fixed(width) => Self::Fixed(Box::new(completed), width),
                    NewValidatorFrame::Struct {
                        mut pending,
                        current_index,
                        mut built,
                    } => {
                        built.push((current_index, completed));
                        if let Some((next_index, next_field)) = pending.next() {
                            stack.push(NewValidatorFrame::Struct {
                                pending,
                                current_index: next_index,
                                built,
                            });
                            current = next_field;
                            break;
                        }
                        Self::Struct(built)
                    }
                };
            }
        }
    }

    fn validate(
        &self,
        array: &dyn Array,
        start: usize,
        end: usize,
        memory: Option<&LiveMemoryPool>,
        check: &dyn Fn() -> Result<()>,
    ) -> Result<()> {
        let mut stack = vec![ValidateWork {
            validator: self,
            array,
            start,
            end,
        }];
        while let Some(work) = stack.pop() {
            check()?;
            if work.start > work.end || work.end > work.array.len() {
                return Err(failed("rich-value parent coordinates exceed child values"));
            }
            valid_ranges(work.array, work.start, work.end, |start, end| {
                match work.validator {
                    Self::Variant => crate::native_variant::arrow::validate(
                        work.array, start, end, memory, check,
                    ),
                    Self::Tensor(metadata, rank) => {
                        validate_tensor(metadata, *rank, work.array, start, end, check)
                    }
                    Self::Vector(metadata) => crate::native_vector::arrow::values::validate(
                        metadata, work.array, start, end, check,
                    ),
                    Self::Spatial(kind) => crate::native_spatial::arrow::values::validate(
                        *kind, work.array, start, end, check,
                    ),
                    Self::Struct(fields) => {
                        let array = work
                            .array
                            .as_struct_opt()
                            .ok_or_else(|| failed("expected struct storage"))?;
                        for (index, validator) in fields.iter().rev() {
                            stack.push(ValidateWork {
                                validator,
                                array: array.column(*index).as_ref(),
                                start,
                                end,
                            });
                        }
                        Ok(())
                    }
                    Self::List(child) => {
                        push_list_work(
                            child,
                            work.array
                                .as_list_opt::<i32>()
                                .ok_or_else(|| failed("expected list storage"))?,
                            start,
                            end,
                            &mut stack,
                        );
                        Ok(())
                    }
                    Self::LargeList(child) => {
                        push_list_work(
                            child,
                            work.array
                                .as_list_opt::<i64>()
                                .ok_or_else(|| failed("expected large-list storage"))?,
                            start,
                            end,
                            &mut stack,
                        );
                        Ok(())
                    }
                    Self::Fixed(child, width) => {
                        let array = work
                            .array
                            .as_fixed_size_list_opt()
                            .ok_or_else(|| failed("expected fixed-list storage"))?;
                        let child_start = start
                            .checked_mul(*width)
                            .ok_or_else(|| failed("shape offset overflow"))?;
                        let child_end = end
                            .checked_mul(*width)
                            .ok_or_else(|| failed("shape offset overflow"))?;
                        stack.push(ValidateWork {
                            validator: child,
                            array: array.values().as_ref(),
                            start: child_start,
                            end: child_end,
                        });
                        Ok(())
                    }
                }
            })?;
        }
        Ok(())
    }
}

fn push_list_work<'a, O: OffsetSizeTrait>(
    child: &'a Validator,
    array: &'a GenericListArray<O>,
    start: usize,
    end: usize,
    stack: &mut Vec<ValidateWork<'a>>,
) {
    stack.push(ValidateWork {
        validator: child,
        array: array.values().as_ref(),
        start: array.value_offsets()[start].as_usize(),
        end: array.value_offsets()[end].as_usize(),
    });
}

fn valid_ranges(
    array: &dyn Array,
    start: usize,
    end: usize,
    mut visit: impl FnMut(usize, usize) -> Result<()>,
) -> Result<()> {
    if start == end {
        return Ok(());
    }
    if let Some(nulls) = array.nulls().filter(|nulls| nulls.null_count() != 0) {
        // Slice the bitmap view, not arrays or their child buffers. This avoids
        // rescanning preceding null ranges for every nested list row.
        for (left, right) in nulls.slice(start, end - start).valid_slices() {
            visit(start + left, start + right)?;
        }
    } else {
        visit(start, end)?;
    }
    Ok(())
}

fn validate_tensor(
    metadata: &Metadata,
    rank: usize,
    array: &dyn Array,
    start: usize,
    end: usize,
    check: &dyn Fn() -> Result<()>,
) -> Result<()> {
    let tensor = array
        .as_struct_opt()
        .ok_or_else(|| failed("expected tensor struct storage"))?;
    let data = tensor
        .column(0)
        .as_list_opt::<i32>()
        .ok_or_else(|| failed("expected tensor data list"))?;
    let shape = tensor
        .column(1)
        .as_fixed_size_list_opt()
        .ok_or_else(|| failed("expected tensor shape list"))?;
    let dimensions = shape
        .values()
        .as_primitive_opt::<arrow_array::types::Int32Type>()
        .ok_or_else(|| failed("expected Int32 tensor dimensions"))?;
    for row in start..end {
        if row.is_multiple_of(1024) {
            check()?;
        }
        if data.is_null(row) || shape.is_null(row) {
            return Err(failed(
                "present variable tensor requires data and shape lists",
            ));
        }
        let offset = usize::try_from(data.value_offsets()[row]).map_err(failed)?;
        let limit = usize::try_from(data.value_offsets()[row + 1]).map_err(failed)?;
        if offset > limit || limit > data.values().len() {
            return Err(failed("tensor data coordinates exceed values"));
        }
        let from = row
            .checked_mul(rank)
            .ok_or_else(|| failed("tensor shape offset overflow"))?;
        let to = from
            .checked_add(rank)
            .filter(|end| *end <= dimensions.len())
            .ok_or_else(|| failed("tensor shape coordinates exceed dimensions"))?;
        metadata
            .validate_dimensions(
                rank,
                (from..to).map(|index| {
                    if index.is_multiple_of(1024) {
                        check().map_err(|error| vortex::error::vortex_err!("{error}"))?;
                    }
                    if dimensions.is_null(index) {
                        Err(vortex::error::vortex_err!(
                            "tensor dimensions require nonnull int32 values"
                        ))
                    } else {
                        Ok(dimensions.value(index))
                    }
                }),
                limit - offset,
            )
            .map_err(failed)?;
    }
    Ok(())
}

pub(crate) fn validate_batch(
    batch: &RecordBatch,
    memory: Option<&LiveMemoryPool>,
    check: &dyn Fn() -> Result<()>,
) -> Result<()> {
    for (field, array) in batch.schema().fields().iter().zip(batch.columns()) {
        check()?;
        if contains(field) {
            let _workspace = if let Some(memory) = memory {
                let bytes =
                    crate::native_payload_schema::arrow_declared_storage_bytes(field, memory)?
                        .checked_add(
                            crate::native_payload_schema::arrow_extension_metadata_bytes(field)?,
                        )
                        .ok_or_else(|| failed("rich-value schema workspace overflow"))?;
                Some(memory.reserve(bytes)?)
            } else {
                crate::native_payload_schema::validate_arrow_declared_storage(field)?;
                crate::native_payload_schema::arrow_extension_metadata_bytes(field)?;
                None
            };
            Validator::new(field)?.validate(array.as_ref(), 0, array.len(), memory, check)?;
        }
    }
    check()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{
        ArrayRef, BinaryViewArray, FixedSizeListArray, LargeListArray, ListArray, StructArray,
    };
    use arrow_buffer::OffsetBuffer;
    use std::{collections::HashMap, sync::Arc};

    #[test]
    fn deep_validator_build_validate_and_drop_on_small_stack() {
        let handle = std::thread::Builder::new()
            .name("native_cross_field_arrow::small_stack".into())
            .stack_size(128 * 1024)
            .spawn(|| {
                let leaf_fields = arrow_schema::Fields::from(vec![
                    Field::new("metadata", DataType::BinaryView, false),
                    Field::new("value", DataType::BinaryView, true),
                ]);
                let meta: ArrayRef = Arc::new(BinaryViewArray::from(vec![Some(&[][..])]));
                let val: ArrayRef = Arc::new(BinaryViewArray::from(vec![Option::<&[u8]>::None]));
                let leaf_arr = StructArray::new(
                    leaf_fields.clone(),
                    vec![meta, val],
                    Some(vec![false].into()),
                );
                let mut current: ArrayRef = Arc::new(leaf_arr);
                let mut field = Field::new("v", DataType::Struct(leaf_fields), true).with_metadata(
                    HashMap::from([(NAME.into(), crate::native_variant::arrow::ID.into())]),
                );
                for level in 0..2_048 {
                    let child_field = Arc::new(field);
                    match level % 4 {
                        0 => {
                            current = Arc::new(ListArray::new(
                                child_field.clone(),
                                OffsetBuffer::new(vec![0_i32, 1].into()),
                                current,
                                None,
                            ));
                            field = Field::new("l", DataType::List(child_field), true);
                        }
                        1 => {
                            current = Arc::new(LargeListArray::new(
                                child_field.clone(),
                                OffsetBuffer::new(vec![0_i64, 1].into()),
                                current,
                                None,
                            ));
                            field = Field::new("ll", DataType::LargeList(child_field), true);
                        }
                        2 => {
                            current = Arc::new(FixedSizeListArray::new(
                                child_field.clone(),
                                1,
                                current,
                                None,
                            ));
                            field =
                                Field::new("fsl", DataType::FixedSizeList(child_field, 1), true);
                        }
                        _ => {
                            current = Arc::new(StructArray::new(
                                vec![child_field.clone()].into(),
                                vec![current],
                                None,
                            ));
                            field =
                                Field::new("s", DataType::Struct(vec![child_field].into()), true);
                        }
                    }
                }
                assert!(contains(&field));
                let validator = Validator::new(&field).unwrap();
                validator
                    .validate(current.as_ref(), 0, current.len(), None, &|| Ok(()))
                    .unwrap();
                drop(validator);
                assert!(!contains(&Field::new("plain", DataType::Int64, true)));
            })
            .unwrap();
        handle.join().unwrap();
    }
}
