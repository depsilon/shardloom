//! Canonical Arrow Variant identity and complete intake validation.

use super::{failed, workspace};
use arrow_array::{Array, cast::AsArray as _};
#[cfg(all(
    test,
    feature = "vortex-local-primitives",
    feature = "vortex-write",
    unix
))]
use arrow_array::{ArrayRef, StructArray};
use arrow_schema::{DataType, Field};
use parquet_variant::VariantMetadata;
use shardloom_core::Result;
use shardloom_exec::live_memory::LiveMemoryPool;
use std::sync::Arc;

pub(crate) const ID: &str = "arrow.parquet.variant";

pub(crate) fn is_variant(field: &Field) -> bool {
    field
        .metadata()
        .get("ARROW:extension:name")
        .is_some_and(|name| name == ID)
}

pub(crate) fn admitted(field: &Field) -> bool {
    let mut dtype = field.data_type();
    let mut depth = 0;
    while let DataType::Dictionary(keys, values) = dtype {
        depth += 1;
        if depth > 24
            || !matches!(
                keys.as_ref(),
                DataType::Int8
                    | DataType::Int16
                    | DataType::Int32
                    | DataType::Int64
                    | DataType::UInt8
                    | DataType::UInt16
                    | DataType::UInt32
                    | DataType::UInt64
            )
        {
            return false;
        }
        dtype = values;
    }
    is_variant(field)
        && field
            .metadata()
            .get("ARROW:extension:metadata")
            .is_none_or(String::is_empty)
        && matches!(dtype, DataType::Struct(fields)
            if fields.iter().any(|field| field.name() == "metadata" && binary_type(field.data_type()) && !field.is_nullable())
                && fields.iter().any(|field| matches!(field.name().as_str(), "value" | "typed_value"))
                && fields.iter().all(|field| matches!(field.name().as_str(), "metadata" | "value" | "typed_value")))
}

pub(crate) fn field(name: &str, nullable: bool) -> Field {
    Field::new(name, data_type(), nullable).with_metadata(
        [
            ("ARROW:extension:name".into(), ID.into()),
            ("ARROW:extension:metadata".into(), String::new()),
        ]
        .into(),
    )
}

pub(crate) fn data_type() -> DataType {
    DataType::Struct(
        vec![
            Arc::new(Field::new("metadata", DataType::Binary, false)),
            Arc::new(Field::new("value", DataType::Binary, true)),
        ]
        .into(),
    )
}

fn binary_type(dtype: &DataType) -> bool {
    matches!(
        dtype,
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView
    )
}

fn binary(array: &dyn Array, row: usize) -> Result<Option<&[u8]>> {
    if !binary_type(array.data_type()) {
        return Err(failed("encoded child requires binary storage"));
    }
    if array.is_null(row) {
        return Ok(None);
    }
    Ok(Some(match array.data_type() {
        DataType::Binary => array.as_binary::<i32>().value(row),
        DataType::LargeBinary => array.as_binary::<i64>().value(row),
        DataType::BinaryView => array.as_binary_view().value(row),
        _ => unreachable!("checked binary dtype"),
    }))
}

pub(crate) fn validate(
    array: &dyn Array,
    start: usize,
    end: usize,
    memory: Option<&LiveMemoryPool>,
    check: &dyn Fn() -> Result<()>,
) -> Result<()> {
    let Some(first) = array.as_any_dictionary_opt() else {
        return validate_rows(array, start, end, memory, check);
    };
    for row in start..end {
        let mut row = row;
        let mut dictionary = first;
        loop {
            check()?;
            if dictionary.keys().is_null(row) {
                break;
            }
            let key = dictionary_key(dictionary.keys(), row)?;
            let values = dictionary.values().as_ref();
            if key >= values.len() {
                return Err(failed("dictionary index exceeds its values"));
            }
            let Some(child) = values.as_any_dictionary_opt() else {
                validate_rows(values, key, key + 1, memory, check)?;
                break;
            };
            row = key;
            dictionary = child;
        }
    }
    Ok(())
}

fn validate_rows(
    array: &dyn Array,
    start: usize,
    end: usize,
    memory: Option<&LiveMemoryPool>,
    check: &dyn Fn() -> Result<()>,
) -> Result<()> {
    let array = array
        .as_struct_opt()
        .ok_or_else(|| failed("expected Variant struct storage"))?;
    let metadata = array
        .column_by_name("metadata")
        .ok_or_else(|| failed("missing metadata"))?;
    for row in start..end {
        check()?;
        if array.is_null(row) {
            continue;
        }
        let metadata = binary(metadata.as_ref(), row)?
            .ok_or_else(|| failed("present value requires nonnull metadata"))?;
        let _credit = memory
            .map(|memory| memory.reserve(workspace(metadata.len())?))
            .transpose()?;
        VariantMetadata::try_new(metadata).map_err(failed)?;
        walk::parts(
            metadata,
            array.column_by_name("value"),
            array.column_by_name("typed_value"),
            row,
            memory,
            check,
            24,
        )?;
    }
    Ok(())
}

fn dictionary_key(array: &dyn Array, row: usize) -> Result<usize> {
    use arrow_array::types::{
        Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
    };
    macro_rules! key {
        ($kind:ty) => {
            usize::try_from(array.as_primitive::<$kind>().value(row)).map_err(failed)
        };
    }
    match array.data_type() {
        DataType::Int8 => key!(Int8Type),
        DataType::Int16 => key!(Int16Type),
        DataType::Int32 => key!(Int32Type),
        DataType::Int64 => key!(Int64Type),
        DataType::UInt8 => key!(UInt8Type),
        DataType::UInt16 => key!(UInt16Type),
        DataType::UInt32 => key!(UInt32Type),
        DataType::UInt64 => key!(UInt64Type),
        _ => Err(failed("unsupported dictionary key type")),
    }
}

#[path = "native_variant_arrow_intake.rs"]
mod walk;

#[cfg(test)]
#[path = "native_variant_arrow_intake_reference.rs"]
mod reference;

#[cfg(test)]
#[path = "native_variant_arrow_intake_tests.rs"]
mod intake_tests;

#[cfg(all(
    test,
    feature = "vortex-local-primitives",
    feature = "vortex-write",
    unix
))]
mod tests {
    use super::*;
    use crate::{
        resident_memory_source::ResidentMemorySource, resident_session::ResidentVortexSession,
    };
    use arrow_array::{BinaryArray, Int64Array, RecordBatch};
    use arrow_schema::Schema;
    use shardloom_exec::compute_pool::CancellationToken;

    fn batch(raw: &[&[u8]], typed: Option<ArrayRef>) -> RecordBatch {
        let metadata: ArrayRef =
            Arc::new(BinaryArray::from_vec(vec![&[1_u8, 0, 0][..]; raw.len()]));
        let value: ArrayRef = Arc::new(BinaryArray::from_vec(raw.to_vec()));
        let mut fields = vec![
            Field::new("metadata", DataType::Binary, false),
            Field::new("value", DataType::Binary, true),
        ];
        let mut children = vec![metadata, value];
        if let Some(typed) = typed {
            fields.push(Field::new("typed_value", typed.data_type().clone(), true));
            children.push(typed);
        }
        let array: ArrayRef = Arc::new(StructArray::new(fields.into(), children, None));
        let field = field("payload", true).with_data_type(array.data_type().clone());
        RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![array]).unwrap()
    }

    #[test]
    fn native_variant_arrow_intake_validates_before_publication_and_releases_owners() {
        let session = ResidentVortexSession::new(16 << 20, 1).unwrap();
        let source = ResidentMemorySource::from_arrow_record_batch(
            &session,
            batch(&[&[0]], None),
            &CancellationToken::default(),
        )
        .unwrap();
        assert!(session.memory().snapshot().reserved_bytes > 0);
        drop(source);
        assert_eq!(session.memory().snapshot().reserved_bytes, 0);
        for invalid in [
            batch(&[&[255]], None),
            batch(&[&[0]], Some(Arc::new(Int64Array::from(vec![1])))),
        ] {
            assert!(
                ResidentMemorySource::from_arrow_record_batch(
                    &session,
                    invalid,
                    &CancellationToken::default()
                )
                .is_err()
            );
            assert_eq!(session.memory().snapshot().reserved_bytes, 0);
        }
        let memory = LiveMemoryPool::new(4096).unwrap();
        let data = batch(&[&[0]], None);
        assert!(validate(data.column(0).as_ref(), 0, 1, Some(&memory), &|| Ok(())).is_err());
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}
