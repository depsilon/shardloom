// SPDX-License-Identifier: Apache-2.0
//! Validate IPC schema shape and admit represented structures before conversion.

use super::decoder_memory::{DecoderMemory, DecoderMemoryReservation, invalid};
use arrow_schema::{ArrowError, DataType, Field, FieldRef, Schema, SchemaRef};
use std::{mem::size_of, sync::Arc};

#[cfg(test)]
#[path = "schema_owned_tests.rs"]
mod tests;

/// A decoded schema and its retained structure-capacity owner.
///
/// Keep this owner alive whenever a cloned schema or its fields outlive the
/// reader. Buffer ownership alone cannot retain bufferless schema metadata.
#[derive(Clone, Debug)]
pub struct OwnedSchema {
    schema: SchemaRef,
    credit: Arc<dyn DecoderMemoryReservation>,
}

impl OwnedSchema {
    /// Borrow the decoded schema without detaching its owner.
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Share the metadata owner when retaining a schema or derived field.
    pub fn memory_reservation(&self) -> Arc<dyn DecoderMemoryReservation> {
        self.credit.clone()
    }
}

/// Decode a validated IPC schema after admitting its named structure capacity.
///
/// The flatbuffer input requires a separate owner. The checked capacity covers
/// Arrow fields, data types, collection growth, names and custom metadata during
/// conversion and retention, not allocator bookkeeping or process RSS. Expanded
/// field nesting is limited to 128 before recursive provider construction.
pub fn read_schema_with_memory(
    schema: crate::Schema<'_>,
    memory: &dyn DecoderMemory,
) -> Result<OwnedSchema, ArrowError> {
    let capacity = schema_capacity(schema)?;
    let credit = Arc::from(memory.reserve(capacity)?);
    let schema = Arc::new(crate::convert::fb_to_schema(schema));
    Ok(OwnedSchema { schema, credit })
}

pub(super) fn add(left: usize, right: usize) -> Result<usize, ArrowError> {
    left.checked_add(right)
        .ok_or_else(|| invalid("IPC structure capacity overflow"))
}

pub(super) fn times(left: usize, right: usize) -> Result<usize, ArrowError> {
    left.checked_mul(right)
        .ok_or_else(|| invalid("IPC structure capacity overflow"))
}

pub(super) fn table<T>(entries: usize) -> Result<usize, ArrowError> {
    if entries == 0 {
        return Ok(0);
    }
    // Standard HashMap: power-of-two buckets, <=7/8 load, one control byte
    // per bucket. Four slots/entry and two SIMD groups include growth overlap.
    add(times(entries, times(4, add(size_of::<T>(), 1)?)?)?, 32)
}

fn metadata(
    metadata: Option<flatbuffers::Vector<'_, flatbuffers::ForwardsUOffset<crate::KeyValue<'_>>>>,
) -> Result<usize, ArrowError> {
    let Some(metadata) = metadata else {
        return Ok(0);
    };
    let mut bytes = table::<(String, String)>(metadata.len())?;
    for entry in metadata {
        if let (Some(key), Some(value)) = (entry.key(), entry.value()) {
            bytes = add(bytes, add(key.len(), value.len())?)?;
        }
    }
    Ok(bytes)
}

fn schema_capacity(schema: crate::Schema<'_>) -> Result<usize, ArrowError> {
    if !schema.endianness().equals_to_target_endianness() {
        return Err(invalid(
            "IPC schema endianness differs from the native provider",
        ));
    }
    let fields = schema
        .fields()
        .ok_or_else(|| invalid("IPC schema has no field vector"))?;
    let mut capacity = add(
        size_of::<Schema>() + 2 * size_of::<usize>(),
        metadata(schema.custom_metadata())?,
    )?;
    for field in fields {
        capacity = add(capacity, field_capacity(field, 1)?)?;
    }
    Ok(capacity)
}

fn field_capacity(field: crate::Field<'_>, depth: usize) -> Result<usize, ArrowError> {
    if depth > 128 {
        return Err(invalid("IPC schema nesting exceeds controlled depth 128"));
    }
    validate_type(field)?;
    // Vec<Field> growth and its conversion into Arc<Field>/Fields can overlap.
    // Dictionary wrappers add two boxed DataTypes. Union fields carry an id.
    let mut bytes = 4 * size_of::<Field>()
        + 4 * size_of::<FieldRef>()
        + 2 * size_of::<DataType>()
        + size_of::<[usize; 8]>();
    bytes = add(bytes, field.name().map_or(0, str::len))?;
    bytes = add(bytes, metadata(field.custom_metadata())?)?;
    if let Some(timestamp) = field.type_as_timestamp() {
        bytes = add(bytes, timestamp.timezone().map_or(0, str::len))?;
    }
    if let Some(children) = field.children() {
        for child in children {
            bytes = add(bytes, field_capacity(child, depth + 1)?)?;
        }
    }
    Ok(bytes)
}

fn required<T>(value: Option<T>) -> Result<T, ArrowError> {
    value.ok_or_else(|| invalid("IPC field is missing its declared type metadata"))
}

fn child_count(field: crate::Field<'_>, expected: usize) -> Result<(), ArrowError> {
    if field.children().map_or(0, |children| children.len()) != expected {
        return Err(invalid("IPC type has an invalid number of child fields"));
    }
    Ok(())
}

fn time_unit(unit: crate::TimeUnit) -> Result<(), ArrowError> {
    match unit {
        crate::TimeUnit::SECOND
        | crate::TimeUnit::MILLISECOND
        | crate::TimeUnit::MICROSECOND
        | crate::TimeUnit::NANOSECOND => Ok(()),
        _ => Err(invalid("invalid IPC time unit")),
    }
}

fn int_width(width: i32) -> Result<(), ArrowError> {
    if !matches!(width, 8 | 16 | 32 | 64) {
        return Err(invalid("invalid IPC integer width"));
    }
    Ok(())
}

fn validate_type(field: crate::Field<'_>) -> Result<(), ArrowError> {
    if let Some(dictionary) = field.dictionary() {
        int_width(required(dictionary.indexType())?.bitWidth())?;
    }
    required(field.type_())?;
    let nested = match field.type_type() {
        crate::Type::Null
        | crate::Type::Bool
        | crate::Type::Binary
        | crate::Type::LargeBinary
        | crate::Type::BinaryView
        | crate::Type::Utf8
        | crate::Type::LargeUtf8
        | crate::Type::Utf8View => false,
        crate::Type::Int => {
            int_width(required(field.type_as_int())?.bitWidth())?;
            false
        }
        crate::Type::FloatingPoint => {
            if !matches!(
                required(field.type_as_floating_point())?.precision(),
                crate::Precision::HALF | crate::Precision::SINGLE | crate::Precision::DOUBLE
            ) {
                return Err(invalid("invalid IPC floating point precision"));
            }
            false
        }
        crate::Type::Date => {
            if !matches!(
                required(field.type_as_date())?.unit(),
                crate::DateUnit::DAY | crate::DateUnit::MILLISECOND
            ) {
                return Err(invalid("invalid IPC date unit"));
            }
            false
        }
        crate::Type::Time => {
            let value = required(field.type_as_time())?;
            if !matches!(
                (value.bitWidth(), value.unit()),
                (32, crate::TimeUnit::SECOND | crate::TimeUnit::MILLISECOND)
                    | (
                        64,
                        crate::TimeUnit::MICROSECOND | crate::TimeUnit::NANOSECOND
                    )
            ) {
                return Err(invalid("invalid IPC time width or unit"));
            }
            false
        }
        crate::Type::Timestamp => {
            time_unit(required(field.type_as_timestamp())?.unit())?;
            false
        }
        crate::Type::Duration => {
            time_unit(required(field.type_as_duration())?.unit())?;
            false
        }
        crate::Type::Interval => {
            if !matches!(
                required(field.type_as_interval())?.unit(),
                crate::IntervalUnit::YEAR_MONTH
                    | crate::IntervalUnit::DAY_TIME
                    | crate::IntervalUnit::MONTH_DAY_NANO
            ) {
                return Err(invalid("invalid IPC interval unit"));
            }
            false
        }
        crate::Type::FixedSizeBinary => {
            if required(field.type_as_fixed_size_binary())?.byteWidth() < 0 {
                return Err(invalid("negative IPC fixed binary width"));
            }
            false
        }
        crate::Type::Decimal => {
            let value = required(field.type_as_decimal())?;
            if !matches!(value.bitWidth(), 32 | 64 | 128 | 256)
                || u8::try_from(value.precision()).is_err()
                || i8::try_from(value.scale()).is_err()
            {
                return Err(invalid("invalid IPC decimal width, precision or scale"));
            }
            false
        }
        crate::Type::List
        | crate::Type::LargeList
        | crate::Type::ListView
        | crate::Type::LargeListView => {
            child_count(field, 1)?;
            true
        }
        crate::Type::FixedSizeList => {
            child_count(field, 1)?;
            if required(field.type_as_fixed_size_list())?.listSize() < 0 {
                return Err(invalid("negative IPC fixed list width"));
            }
            true
        }
        crate::Type::Map => {
            required(field.type_as_map())?;
            child_count(field, 1)?;
            true
        }
        crate::Type::Struct_ => true,
        crate::Type::RunEndEncoded => {
            child_count(field, 2)?;
            true
        }
        crate::Type::Union => {
            validate_union(field)?;
            true
        }
        _ => return Err(invalid("unsupported IPC field type")),
    };
    if !nested {
        child_count(field, 0)?;
    }
    Ok(())
}

fn validate_union(field: crate::Field<'_>) -> Result<(), ArrowError> {
    let union = required(field.type_as_union())?;
    if !matches!(
        union.mode(),
        crate::UnionMode::Dense | crate::UnionMode::Sparse
    ) {
        return Err(invalid("invalid IPC union mode"));
    }
    let children = field.children().map_or(0, |children| children.len());
    if children > 128 {
        return Err(invalid("IPC union has too many child fields"));
    }
    if let Some(ids) = union.typeIds() {
        if ids.len() != children {
            return Err(invalid("IPC union field/id count mismatch"));
        }
        let mut seen = 0_u128;
        for id in ids {
            if !(0..128).contains(&id) {
                return Err(invalid("invalid IPC union type id"));
            }
            let bit = 1_u128 << id;
            if seen & bit != 0 {
                return Err(invalid("duplicate IPC union type id"));
            }
            seen |= bit;
        }
    }
    Ok(())
}
