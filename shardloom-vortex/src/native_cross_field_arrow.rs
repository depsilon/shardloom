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

fn contains(field: &Field) -> bool {
    if field
        .metadata()
        .get(NAME)
        .is_some_and(|name| name == crate::native_variable_tensor::ARROW_ID)
        || crate::native_vector::arrow::kind(field).is_some()
    {
        return true;
    }
    match field.data_type() {
        DataType::List(child) | DataType::LargeList(child) | DataType::FixedSizeList(child, _) => {
            contains(child)
        }
        DataType::Struct(fields) => fields.iter().any(|field| contains(field)),
        _ => false,
    }
}

/// Compile metadata once per field. The existing bounded schema grant covers
/// the small traversal tree and JSON scratch; no per-row shape vector is made.
enum Validator {
    Tensor(Metadata, usize),
    Vector(crate::native_vector::Metadata),
    Struct(Vec<(usize, Self)>),
    List(Box<Self>),
    LargeList(Box<Self>),
    Fixed(Box<Self>, usize),
}

impl Validator {
    fn new(field: &Field) -> Result<Self> {
        if crate::native_vector::arrow::kind(field).is_some() {
            let dtype = crate::native_vector::arrow::dtype(field).map_err(failed)?;
            return Ok(Self::Vector(
                crate::native_vector::metadata(&dtype)
                    .expect("validated vector metadata")
                    .clone(),
            ));
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
            return Ok(Self::Tensor(metadata, rank));
        }
        Ok(match field.data_type() {
            DataType::Struct(fields) => Self::Struct(
                fields
                    .iter()
                    .enumerate()
                    .filter(|(_, field)| contains(field))
                    .map(|(index, field)| Ok((index, Self::new(field)?)))
                    .collect::<Result<_>>()?,
            ),
            DataType::List(child) => Self::List(Box::new(Self::new(child)?)),
            DataType::LargeList(child) => Self::LargeList(Box::new(Self::new(child)?)),
            DataType::FixedSizeList(child, width) => Self::Fixed(
                Box::new(Self::new(child)?),
                usize::try_from(*width).map_err(failed)?,
            ),
            _ => return Err(failed("unsupported containing rich-value layout")),
        })
    }

    fn validate(
        &self,
        array: &dyn Array,
        start: usize,
        end: usize,
        check: &dyn Fn() -> Result<()>,
    ) -> Result<()> {
        check()?;
        if start > end || end > array.len() {
            return Err(failed("rich-value parent coordinates exceed child values"));
        }
        valid_ranges(array, start, end, |start, end| match self {
            Self::Tensor(metadata, rank) => {
                validate_tensor(metadata, *rank, array, start, end, check)
            }
            Self::Vector(metadata) => {
                crate::native_vector::arrow::values::validate(metadata, array, start, end, check)
            }
            Self::Struct(fields) => {
                let array = array
                    .as_struct_opt()
                    .ok_or_else(|| failed("expected struct storage"))?;
                for (index, validator) in fields {
                    validator.validate(array.column(*index).as_ref(), start, end, check)?;
                }
                Ok(())
            }
            Self::List(child) => child.validate_list(
                array
                    .as_list_opt::<i32>()
                    .ok_or_else(|| failed("expected list storage"))?,
                start,
                end,
                check,
            ),
            Self::LargeList(child) => child.validate_list(
                array
                    .as_list_opt::<i64>()
                    .ok_or_else(|| failed("expected large-list storage"))?,
                start,
                end,
                check,
            ),
            Self::Fixed(child, width) => {
                let array = array
                    .as_fixed_size_list_opt()
                    .ok_or_else(|| failed("expected fixed-list storage"))?;
                let start = start
                    .checked_mul(*width)
                    .ok_or_else(|| failed("shape offset overflow"))?;
                let end = end
                    .checked_mul(*width)
                    .ok_or_else(|| failed("shape offset overflow"))?;
                child.validate(array.values().as_ref(), start, end, check)
            }
        })
    }

    fn validate_list<O: OffsetSizeTrait>(
        &self,
        array: &GenericListArray<O>,
        start: usize,
        end: usize,
        check: &dyn Fn() -> Result<()>,
    ) -> Result<()> {
        self.validate(
            array.values().as_ref(),
            array.value_offsets()[start].as_usize(),
            array.value_offsets()[end].as_usize(),
            check,
        )
    }
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
                    crate::native_payload_schema::arrow_metadata_bytes(field.data_type(), memory)?
                        .checked_add(
                            crate::native_payload_schema::arrow_extension_metadata_bytes(field)?,
                        )
                        .ok_or_else(|| failed("rich-value schema workspace overflow"))?;
                Some(memory.reserve(bytes)?)
            } else {
                crate::native_payload_schema::validate_arrow_declaration(field.data_type())?;
                crate::native_payload_schema::arrow_extension_metadata_bytes(field)?;
                None
            };
            Validator::new(field)?.validate(array.as_ref(), 0, array.len(), check)?;
        }
    }
    check()
}
