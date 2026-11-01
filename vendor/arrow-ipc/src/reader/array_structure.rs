// SPDX-License-Identifier: Apache-2.0
//! Pre-construction capacity for the pinned Arrow array representations.
//!
//! This walk borrows schemas, flatbuffer nodes and concrete dictionaries. It
//! does not call `to_data`, `logical_nulls`, `slice` or allocate an inspection
//! tree before admission. Payload buffers have separate owners. In particular,
//! summing their capacities would charge a shared IPC message repeatedly.

use super::{
    RecordBatchDecoder,
    decoder_memory::invalid,
    schema_owned::{add, times},
};
use arrow_array::{cast::AsArray, types::Int64Type, *};
use arrow_buffer::{Buffer, NullBuffer};
use arrow_data::{ArrayData, ArrayDataBuilder};
use arrow_schema::{ArrowError, DataType, Field, Schema};
use std::mem::size_of;

#[derive(Default)]
pub(super) struct Capacity {
    pub(super) retained: usize,
    pub(super) temporary: usize,
}

impl Capacity {
    fn include(&mut self, other: Self) -> Result<(), ArrowError> {
        self.retained = add(self.retained, other.retained)?;
        self.temporary = add(self.temporary, other.temporary)?;
        Ok(())
    }
}

/// Vec's minimum element allocation and old/new growth overlap. This is a
/// conservative requested-capacity bound, not allocator bookkeeping or RSS.
fn collection<T>(count: usize) -> Result<usize, ArrowError> {
    if count == 0 {
        return Ok(0);
    }
    times(times(count.max(4), 3)?, size_of::<T>())
}

fn boxed_types(dtype: &DataType, depth: usize) -> Result<usize, ArrowError> {
    if depth > 128 {
        return Err(invalid("IPC array nesting exceeds controlled depth 128"));
    }
    if let DataType::Dictionary(key, value) = dtype {
        add(
            2 * size_of::<DataType>(),
            add(boxed_types(key, depth + 1)?, boxed_types(value, depth + 1)?)?,
        )
    } else {
        // Fields, UnionFields, list FieldRefs and timezone strings clone Arcs.
        Ok(0)
    }
}

fn object_size(dtype: &DataType) -> usize {
    match dtype {
        DataType::Null => size_of::<NullArray>(),
        DataType::Boolean => size_of::<BooleanArray>(),
        DataType::Utf8 | DataType::Binary => size_of::<BinaryArray>(),
        DataType::LargeUtf8 | DataType::LargeBinary => size_of::<LargeBinaryArray>(),
        DataType::Utf8View | DataType::BinaryView => size_of::<BinaryViewArray>(),
        DataType::FixedSizeBinary(_) => size_of::<FixedSizeBinaryArray>(),
        DataType::List(_) => size_of::<ListArray>(),
        DataType::LargeList(_) => size_of::<LargeListArray>(),
        DataType::ListView(_) => size_of::<ListViewArray>(),
        DataType::LargeListView(_) => size_of::<LargeListViewArray>(),
        DataType::FixedSizeList(_, _) => size_of::<FixedSizeListArray>(),
        DataType::Struct(_) => size_of::<StructArray>(),
        DataType::Map(_, _) => size_of::<MapArray>(),
        DataType::Union(_, _) => size_of::<UnionArray>(),
        DataType::Dictionary(_, _) => size_of::<DictionaryArray<Int64Type>>(),
        DataType::RunEndEncoded(_, _) => size_of::<RunArray<Int64Type>>(),
        _ => size_of::<PrimitiveArray<Int64Type>>(),
    }
}

fn children(dtype: &DataType) -> usize {
    match dtype {
        DataType::Struct(fields) => fields.len(),
        DataType::Union(fields, _) => fields.len(),
        DataType::RunEndEncoded(_, _) => 2,
        DataType::List(_)
        | DataType::LargeList(_)
        | DataType::ListView(_)
        | DataType::LargeListView(_)
        | DataType::FixedSizeList(_, _)
        | DataType::Map(_, _)
        | DataType::Dictionary(_, _) => 1,
        _ => 0,
    }
}

fn buffers(dtype: &DataType, variadic: usize) -> usize {
    match dtype {
        DataType::Null
        | DataType::Struct(_)
        | DataType::FixedSizeList(_, _)
        | DataType::RunEndEncoded(_, _) => 0,
        DataType::Utf8
        | DataType::Binary
        | DataType::LargeUtf8
        | DataType::LargeBinary
        | DataType::ListView(_)
        | DataType::LargeListView(_) => 2,
        DataType::Union(_, arrow_schema::UnionMode::Dense) => 2,
        DataType::BinaryView | DataType::Utf8View => variadic.saturating_add(1),
        _ => 1,
    }
}

pub(super) fn node(
    dtype: &DataType,
    rows: usize,
    variadic: usize,
    depth: usize,
) -> Result<Capacity, ArrowError> {
    if depth > 128 {
        return Err(invalid("IPC array nesting exceeds controlled depth 128"));
    }
    let slots = if let DataType::Union(fields, _) = dtype {
        // UnionArray stores an Option<ArrayRef> slot for every id through max.
        let maximum = fields.iter().map(|(id, _)| id).max().unwrap_or(0);
        add(
            usize::try_from(maximum).map_err(|_| invalid("negative IPC union id"))?,
            1,
        )?
    } else {
        children(dtype)
    };
    let mut represented = add(
        object_size(dtype) + 2 * size_of::<usize>(),
        boxed_types(dtype, depth)?,
    )?;
    represented = add(represented, collection::<ArrayRef>(slots)?)?;
    represented = add(represented, collection::<Buffer>(buffers(dtype, variadic))?)?;
    // A fresh controlled buffer can retain the original Arrow Bytes record,
    // Bytes::Owned<Buffer + reservation>, Arc<bytes::Bytes>, and an outer Arrow
    // Bytes record. Arrow's private record is ptr/len/three-word Deallocation;
    // include the optional pool mutex as well. These are actual owner/control
    // structures, distinct from allocator bookkeeping. Shared input buffers
    // need fewer records, but never charge their payload capacity here.
    let arc_header = 2 * size_of::<usize>();
    let arrow_bytes = size_of::<[usize; 5]>()
        + size_of::<std::sync::Mutex<Option<Box<dyn Send + Sync>>>>()
        + arc_header;
    let owner = size_of::<Buffer>()
        + size_of::<std::sync::Arc<dyn super::DecoderMemoryReservation>>()
        + size_of::<usize>();
    let controls = 2 * arrow_bytes + size_of::<bytes::Bytes>() + arc_header + owner;
    represented = add(
        represented,
        times(add(buffers(dtype, variadic), 1)?, controls)?,
    )?;
    let mut construction = add(size_of::<ArrayData>(), size_of::<ArrayDataBuilder>())?;
    construction = add(construction, collection::<ArrayData>(children(dtype))?)?;
    construction = add(
        construction,
        collection::<Buffer>(add(buffers(dtype, variadic), 1)?)?,
    )?;
    construction = add(construction, 3 * size_of::<arrow_data::BufferSpec>())?;
    construction = add(construction, boxed_types(dtype, depth)?)?;
    // A nested into_data/make_array conversion can coexist with the concrete
    // child plus each ancestor's data and reconstructed representation. Charge
    // both representations per ancestor before entering that conversion.
    construction = times(add(represented, construction)?, times(add(depth, 1)?, 2)?)?;
    if matches!(
        dtype,
        DataType::Dictionary(_, _) | DataType::RunEndEncoded(_, _) | DataType::Union(_, _)
    ) || (matches!(dtype, DataType::Null) && depth > 1)
    {
        // Nonnullable Struct children can call logical_nulls. Null, dictionary
        // and run arrays construct a row bitmap; union gathers/masks may retain
        // child masks alongside its output and one temporary mask.
        let bitmap = super::decoder_memory::aligned_capacity(rows.div_ceil(8))?;
        construction = add(construction, times(bitmap, 3)?)?;
        construction = add(construction, collection::<(i8, NullBuffer)>(slots)?)?;
    }
    if let DataType::FixedSizeList(field, width) = dtype {
        if !field.is_nullable() {
            // ArrayData::validate_nulls expands the parent's validity to child
            // rows before checking a nonnullable fixed-list child.
            let width =
                usize::try_from(*width).map_err(|_| invalid("negative IPC fixed list width"))?;
            let bytes = times(rows, width)?.div_ceil(8);
            construction = add(
                construction,
                super::decoder_memory::aligned_capacity(bytes)?,
            )?;
        }
    }
    Ok(Capacity {
        retained: represented,
        temporary: construction,
    })
}

pub(super) fn array(array: &dyn Array, depth: usize) -> Result<Capacity, ArrowError> {
    let dtype = array.data_type();
    let variadic = match dtype {
        DataType::Utf8View => array.as_string_view().data_buffers().len(),
        DataType::BinaryView => array.as_binary_view().data_buffers().len(),
        _ => 0,
    };
    let mut capacity = node(dtype, array.len(), variadic, depth)?;
    match dtype {
        DataType::List(_) => capacity.include(self::array(
            array.as_list::<i32>().values().as_ref(),
            depth + 1,
        )?)?,
        DataType::LargeList(_) => capacity.include(self::array(
            array.as_list::<i64>().values().as_ref(),
            depth + 1,
        )?)?,
        DataType::ListView(_) => capacity.include(self::array(
            array.as_list_view::<i32>().values().as_ref(),
            depth + 1,
        )?)?,
        DataType::LargeListView(_) => capacity.include(self::array(
            array.as_list_view::<i64>().values().as_ref(),
            depth + 1,
        )?)?,
        DataType::FixedSizeList(_, _) => capacity.include(self::array(
            array.as_fixed_size_list().values().as_ref(),
            depth + 1,
        )?)?,
        DataType::Map(_, _) => {
            capacity.include(self::array(array.as_map().entries(), depth + 1)?)?
        }
        DataType::Struct(_) => {
            for child in array.as_struct().columns() {
                capacity.include(self::array(child.as_ref(), depth + 1)?)?;
            }
        }
        DataType::Union(fields, _) => {
            for (id, _) in fields.iter() {
                capacity.include(self::array(array.as_union().child(id).as_ref(), depth + 1)?)?;
            }
        }
        DataType::Dictionary(_, _) => capacity.include(self::array(
            array.as_any_dictionary().values().as_ref(),
            depth + 1,
        )?)?,
        DataType::RunEndEncoded(ends, _) => {
            macro_rules! run {
                ($t:ty) => {{
                    let run = array.as_run::<$t>();
                    capacity.include(node(
                        ends.data_type(),
                        run.run_ends().values().len(),
                        0,
                        depth + 1,
                    )?)?;
                    capacity.include(self::array(run.values().as_ref(), depth + 1)?)?;
                }};
            }
            match ends.data_type() {
                DataType::Int16 => run!(types::Int16Type),
                DataType::Int32 => run!(types::Int32Type),
                DataType::Int64 => run!(types::Int64Type),
                _ => return Err(invalid("invalid IPC run end type")),
            }
        }
        _ => {}
    }
    Ok(capacity)
}

fn empty(dtype: &DataType, depth: usize) -> Result<Capacity, ArrowError> {
    let mut capacity = node(dtype, 0, 0, depth)?;
    // new_empty_array may allocate the initial offset, and fresh Buffer owner
    // records even for empty payload. These owners survive with the structure.
    capacity.retained = add(capacity.retained, times(add(buffers(dtype, 0), 1)?, 64)?)?;
    match dtype {
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::ListView(field)
        | DataType::LargeListView(field)
        | DataType::FixedSizeList(field, _)
        | DataType::Map(field, _) => {
            capacity.include(empty(field.data_type(), depth + 1)?)?;
        }
        DataType::Dictionary(_, value) => capacity.include(empty(value, depth + 1)?)?,
        DataType::Struct(fields) => {
            for field in fields {
                capacity.include(empty(field.data_type(), depth + 1)?)?;
            }
        }
        DataType::Union(fields, _) => {
            for (_, field) in fields.iter() {
                capacity.include(empty(field.data_type(), depth + 1)?)?;
            }
        }
        DataType::RunEndEncoded(ends, values) => {
            capacity.include(empty(ends.data_type(), depth + 1)?)?;
            capacity.include(empty(values.data_type(), depth + 1)?)?;
        }
        _ => {}
    }
    Ok(capacity)
}

struct Encoded<'a, 'b> {
    decoder: &'a RecordBatchDecoder<'b>,
    nodes: usize,
    variadic: usize,
}

impl Encoded<'_, '_> {
    fn field(
        &mut self,
        field: &Field,
        depth: usize,
        selected: bool,
    ) -> Result<Capacity, ArrowError> {
        let nodes = self
            .decoder
            .batch
            .nodes()
            .ok_or_else(|| invalid("IPC batch has no nodes"))?;
        if self.nodes >= nodes.len() {
            return Err(invalid("IPC field node count is too small"));
        }
        let encoded = nodes.get(self.nodes);
        self.nodes += 1;
        let rows =
            usize::try_from(encoded.length()).map_err(|_| invalid("negative IPC field length"))?;
        if encoded.null_count() < 0 || encoded.null_count() > encoded.length() {
            return Err(invalid("invalid IPC field null count"));
        }
        let dtype = field.data_type();
        let variadic = if matches!(dtype, DataType::Utf8View | DataType::BinaryView) {
            let counts = self
                .decoder
                .batch
                .variadicBufferCounts()
                .ok_or_else(|| invalid("missing IPC variadic counts"))?;
            if self.variadic >= counts.len() {
                return Err(invalid("missing IPC variadic count"));
            }
            let count = usize::try_from(counts.get(self.variadic))
                .map_err(|_| invalid("negative IPC variadic count"))?;
            self.variadic += 1;
            count
                .checked_add(2)
                .ok_or_else(|| invalid("IPC variadic count overflow"))?;
            count
        } else {
            0
        };
        let mut capacity = node(dtype, rows, variadic, depth)?;
        match dtype {
            DataType::List(child)
            | DataType::LargeList(child)
            | DataType::ListView(child)
            | DataType::LargeListView(child)
            | DataType::Map(child, _) => {
                capacity.include(self.field(child, depth + 1, selected)?)?
            }
            DataType::FixedSizeList(child, width) => {
                let width = usize::try_from(*width)
                    .map_err(|_| invalid("negative IPC fixed list width"))?;
                times(rows, width)?;
                capacity.include(self.field(child, depth + 1, selected)?)?;
            }
            DataType::Struct(fields) => {
                for child in fields {
                    capacity.include(self.field(child, depth + 1, selected)?)?;
                }
            }
            DataType::Union(fields, _) => {
                for (_, child) in fields.iter() {
                    capacity.include(self.field(child, depth + 1, selected)?)?;
                }
            }
            DataType::RunEndEncoded(ends, values) => {
                capacity.include(self.field(ends, depth + 1, selected)?)?;
                capacity.include(self.field(values, depth + 1, selected)?)?;
            }
            DataType::Dictionary(_, value_type) if selected => {
                #[allow(deprecated)]
                let id = field
                    .dict_id()
                    .ok_or_else(|| invalid("IPC dictionary field has no id"))?;
                capacity.include(
                    if let Some(values) = self.decoder.dictionaries_by_id.get(&id) {
                        array(values.as_ref(), depth + 1)?
                    } else {
                        empty(value_type, depth + 1)?
                    },
                )?;
            }
            _ => {}
        }
        Ok(if selected {
            capacity
        } else {
            Capacity::default()
        })
    }
}

pub(super) fn record_batch(decoder: &RecordBatchDecoder<'_>) -> Result<Capacity, ArrowError> {
    let mut walk = Encoded {
        decoder,
        nodes: 0,
        variadic: 0,
    };
    let count = decoder
        .projection
        .map_or(decoder.schema.fields().len(), <[usize]>::len);
    let mut capacity = Capacity {
        retained: add(size_of::<RecordBatch>(), collection::<ArrayRef>(count)?)?,
        temporary: collection::<i64>(decoder.batch.variadicBufferCounts().map_or(0, |v| v.len()))?,
    };
    for (index, field) in decoder.schema.fields().iter().enumerate() {
        let selected = decoder
            .projection
            .is_none_or(|projection| projection.contains(&index));
        capacity.include(walk.field(field, 1, selected)?)?;
    }
    if walk.nodes != decoder.batch.nodes().map_or(0, |v| v.len())
        || walk.variadic != decoder.batch.variadicBufferCounts().map_or(0, |v| v.len())
    {
        return Err(invalid("IPC field or variadic count does not match schema"));
    }
    if decoder.projection.is_some() {
        capacity.retained = add(
            capacity.retained,
            add(
                size_of::<Schema>(),
                collection::<arrow_schema::FieldRef>(count)?,
            )?,
        )?;
        capacity.retained = add(
            capacity.retained,
            super::schema_owned::table::<(String, String)>(decoder.schema.metadata().capacity())?,
        )?;
        for (key, value) in decoder.schema.metadata() {
            capacity.retained = add(capacity.retained, add(key.len(), value.len())?)?;
        }
        capacity.temporary = add(capacity.temporary, collection::<(usize, ArrayRef)>(count)?)?;
    }
    Ok(capacity)
}
