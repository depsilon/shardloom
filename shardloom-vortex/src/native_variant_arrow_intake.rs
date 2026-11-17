//! Validate borrowed Arrow values in source order while retaining raw validation credits.

use super::{binary, failed, workspace};
use arrow_array::{
    Array, ArrayRef, GenericListArray, OffsetSizeTrait, StructArray, cast::AsArray as _,
};
use arrow_schema::DataType;
use parquet_variant::{Variant, VariantMetadata, VariantObject};
use shardloom_core::Result;
use shardloom_exec::live_memory::{LiveMemoryPool, MemoryLease};

#[derive(Clone, Copy)]
struct Input<'a> {
    value: Option<&'a ArrayRef>,
    typed: Option<&'a ArrayRef>,
    row: usize,
    depth: usize,
}

enum Cursor<'m, 'a> {
    Fields {
        source: &'a StructArray,
        residual: Option<VariantObject<'m, 'a>>,
        index: usize,
        row: usize,
    },
    List {
        values: &'a dyn Array,
        index: usize,
        end: usize,
    },
}

impl<'a> Cursor<'_, 'a> {
    fn list<O: OffsetSizeTrait>(list: &'a GenericListArray<O>, row: usize) -> Self {
        let offsets = list.value_offsets();
        Self::List {
            values: list.values().as_ref(),
            index: offsets[row].as_usize(),
            end: offsets[row + 1].as_usize(),
        }
    }

    fn shell(&self) -> Option<(&'a dyn Array, usize)> {
        match self {
            Self::Fields {
                source, index, row, ..
            } => source
                .columns()
                .get(*index)
                .map(|array| (array.as_ref(), *row)),
            Self::List { values, index, end } => (*index < *end).then_some((*values, *index)),
        }
    }

    fn advance(&mut self, metadata: &[u8], present: bool) -> Result<()> {
        match self {
            Self::Fields {
                source,
                index,
                residual,
                ..
            } => {
                let field = &source.fields()[*index];
                if present
                    && VariantMetadata::try_new(metadata)
                        .map_err(failed)?
                        .get_entry(field.name())
                        .is_none()
                {
                    return Err(failed(
                        "shredded object field is absent from its metadata dictionary",
                    ));
                }
                if present
                    && residual
                        .as_ref()
                        .is_some_and(|object| object.get(field.name()).is_some())
                {
                    return Err(failed("raw and shredded object fields overlap"));
                }
                *index += 1;
            }
            Self::List { index, .. } => *index += 1,
        }
        Ok(())
    }
}

struct Frame<'m, 'a> {
    cursor: Cursor<'m, 'a>,
    child_depth: usize,
    _raw_credit: Option<MemoryLease>,
}

struct Pending<'m, 'a> {
    frames: Vec<Frame<'m, 'a>>,
    capacity_credit: Option<MemoryLease>,
}

impl Drop for Pending<'_, '_> {
    fn drop(&mut self) {
        while self.frames.pop().is_some() {}
    }
}

enum Step<'m, 'a> {
    Inspect(Input<'a>),
    Children(Frame<'m, 'a>),
    Done(bool),
}

pub(super) fn parts(
    metadata: &[u8],
    value: Option<&ArrayRef>,
    typed: Option<&ArrayRef>,
    row: usize,
    memory: Option<&LiveMemoryPool>,
    check: &dyn Fn() -> Result<()>,
    max_depth: usize,
) -> Result<bool> {
    let mut pending = Pending {
        frames: Vec::new(),
        capacity_credit: memory.map(|pool| pool.reserve(0)).transpose()?,
    };
    let mut step = Step::Inspect(Input {
        value,
        typed,
        row,
        depth: 0,
    });
    loop {
        step = match step {
            Step::Inspect(input) => inspect(metadata, input, memory, check, max_depth)?,
            Step::Children(frame) => {
                if let Some((array, row)) = frame.cursor.shell() {
                    let depth = frame.child_depth;
                    crate::native_payload_schema::reserve_frame(
                        &mut pending.frames,
                        pending.capacity_credit.as_mut(),
                    )?;
                    pending.frames.push(frame);
                    shell(array, row, depth)?
                } else {
                    Step::Done(true)
                }
            }
            Step::Done(present) => {
                let Some(mut frame) = pending.frames.pop() else {
                    return Ok(present);
                };
                frame.cursor.advance(metadata, present)?;
                Step::Children(frame)
            }
        };
    }
}

fn inspect<'m, 'a>(
    metadata: &'m [u8],
    input: Input<'a>,
    memory: Option<&LiveMemoryPool>,
    check: &dyn Fn() -> Result<()>,
    max_depth: usize,
) -> Result<Step<'m, 'a>> {
    let Input {
        value,
        typed,
        row,
        depth,
    } = input;
    check()?;
    if depth > max_depth {
        return Err(failed("shredded value exceeds the current depth boundary"));
    }
    let raw = value
        .map(|value| binary(value.as_ref(), row))
        .transpose()?
        .flatten();
    let raw_credit = memory
        .map(|memory| memory.reserve(workspace(raw.map_or(0, <[u8]>::len))?))
        .transpose()?;
    let parsed = raw
        .map(|raw| super::super::value(metadata, raw, check))
        .transpose()?;
    let Some(typed) = typed.filter(|typed| typed.is_valid(row)) else {
        return Ok(Step::Done(parsed.is_some()));
    };
    if !matches!(typed.data_type(), DataType::Struct(_)) && parsed.is_some() {
        return Err(failed("conflicting raw and shredded non-object values"));
    }
    let cursor = match typed.data_type() {
        DataType::Struct(_) => {
            let residual = match parsed {
                None => None,
                Some(Variant::Object(object)) => Some(object),
                Some(_) => return Err(failed("shredded object requires an object residual")),
            };
            Cursor::Fields {
                source: typed.as_struct(),
                residual,
                index: 0,
                row,
            }
        }
        DataType::List(_) => Cursor::list(typed.as_list::<i32>(), row),
        DataType::LargeList(_) => Cursor::list(typed.as_list::<i64>(), row),
        DataType::Boolean
        | DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Utf8View
        | DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::Float32
        | DataType::Float64
        | DataType::Decimal128(..)
        | DataType::Date32
        | DataType::Time64(arrow_schema::TimeUnit::Microsecond)
        | DataType::Timestamp(
            arrow_schema::TimeUnit::Microsecond | arrow_schema::TimeUnit::Nanosecond,
            _,
        ) => return Ok(Step::Done(true)),
        _ => {
            return Err(failed(
                "unsupported shredded physical dtype; supply unshredded Variant storage",
            ));
        }
    };
    Ok(Step::Children(Frame {
        cursor,
        child_depth: depth + 1,
        _raw_credit: raw_credit,
    }))
}

fn shell<'m>(array: &dyn Array, row: usize, depth: usize) -> Result<Step<'m, '_>> {
    let array: &StructArray = array
        .as_struct_opt()
        .ok_or_else(|| failed("shredded field requires a value/typed_value struct"))?;
    if array.is_null(row) {
        return Ok(Step::Done(false));
    }
    if array.fields().is_empty()
        || array
            .fields()
            .iter()
            .any(|field| !matches!(field.name().as_str(), "value" | "typed_value"))
    {
        return Err(failed(
            "shredded field requires only value/typed_value children",
        ));
    }
    Ok(Step::Inspect(Input {
        value: array.column_by_name("value"),
        typed: array.column_by_name("typed_value"),
        row,
        depth,
    }))
}
