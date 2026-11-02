// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::reader::decoder_memory_tests::Memory;
use std::collections::HashMap;

fn decode(schema: &Schema, memory: &Memory) -> Result<OwnedSchema, ArrowError> {
    let mut tracker = crate::writer::DictionaryTracker::new(false);
    let bytes = crate::convert::IpcSchemaEncoder::new()
        .with_dictionary_tracker(&mut tracker)
        .schema_to_fb(schema);
    let options = flatbuffers::VerifierOptions {
        max_depth: 1024,
        ..Default::default()
    };
    let fb = flatbuffers::root_with_opts::<crate::Schema>(&options, bytes.finished_data()).unwrap();
    read_schema_with_memory(fb, memory)
}

#[test]
fn owned_schema_preserves_nested_types_names_and_metadata_across_clones() {
    let field = Field::new(
        "metadata and values",
        DataType::Struct(
            vec![
                Field::new(
                    "when",
                    DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, Some("UTC".into())),
                    true,
                ),
                Field::new("missing", DataType::Null, true),
                Field::new(
                    "dictionary",
                    DataType::Dictionary(Box::new(DataType::Int16), Box::new(DataType::Utf8)),
                    true,
                ),
            ]
            .into(),
        ),
        false,
    )
    .with_metadata(HashMap::from([("origin".into(), "known-value".into())]));
    let schema = Schema::new_with_metadata(
        vec![field],
        HashMap::from([("schema-key".into(), "schema-value".into())]),
    );
    let memory = Memory::new(1 << 20);
    let decoded = decode(&schema, &memory).unwrap();
    assert_eq!(decoded.schema().as_ref(), &schema);
    assert!(memory.current() > 0);
    let retained = decoded.clone();
    drop(decoded);
    assert_eq!(retained.schema().as_ref(), &schema);
    assert!(memory.current() > 0);
    drop(retained);
    assert_eq!(memory.current(), 0);
}

#[test]
fn schema_admission_uses_real_width_names_and_metadata_before_construction() {
    let schema = Schema::new(vec![Field::new("n", DataType::Null, true)]);
    let memory = Memory::new(1 << 20);
    let small = decode(&schema, &memory).unwrap();
    let small_size = memory.current();
    drop(small);
    let named = Schema::new(vec![Field::new(
        "long-name".repeat(100),
        DataType::Null,
        true,
    )]);
    let large = decode(&named, &memory).unwrap();
    assert!(memory.current() > small_size);
    drop(large);
    let denied = Memory::new(small_size - 1);
    assert!(
        decode(&schema, &denied)
            .unwrap_err()
            .to_string()
            .contains("denied")
    );
    assert_eq!(denied.current(), 0);
    assert_eq!(memory.current(), 0);
}

#[test]
fn controlled_schema_depth_is_checked_before_recursive_conversion() {
    let memory = Memory::new(1 << 20);
    for (depth, valid) in [(128, true), (129, false)] {
        let mut field = Field::new("leaf", DataType::Int32, true);
        for _ in 1..depth {
            field = Field::new("items", DataType::List(Arc::new(field)), true);
        }
        let schema = Schema::new(vec![field]);
        let result = decode(&schema, &memory);
        assert_eq!(result.is_ok(), valid);
        if !valid {
            assert!(
                result
                    .as_ref()
                    .unwrap_err()
                    .to_string()
                    .contains("depth 128")
            );
        }
        drop(result);
        assert_eq!(memory.current(), 0);
    }
}

#[test]
fn malformed_schema_types_are_errors_before_admission_or_provider_panics() {
    for case in 0..5 {
        let mut fbb = flatbuffers::FlatBufferBuilder::new();
        let (tag, dtype) = match case {
            0 => (
                crate::Type::Int,
                Some(
                    crate::Int::create(
                        &mut fbb,
                        &crate::IntArgs {
                            bitWidth: 12,
                            is_signed: true,
                        },
                    )
                    .as_union_value(),
                ),
            ),
            1 => (
                crate::Type::FloatingPoint,
                Some(
                    crate::FloatingPoint::create(
                        &mut fbb,
                        &crate::FloatingPointArgs {
                            precision: crate::Precision(99),
                        },
                    )
                    .as_union_value(),
                ),
            ),
            2 => (
                crate::Type::List,
                Some(crate::List::create(&mut fbb, &crate::ListArgs::default()).as_union_value()),
            ),
            3 => (
                crate::Type::Decimal,
                Some(
                    crate::Decimal::create(
                        &mut fbb,
                        &crate::DecimalArgs {
                            precision: 1000,
                            ..Default::default()
                        },
                    )
                    .as_union_value(),
                ),
            ),
            _ => (crate::Type::Int, None),
        };
        let field = crate::Field::create(
            &mut fbb,
            &crate::FieldArgs {
                type_type: tag,
                type_: dtype,
                ..Default::default()
            },
        );
        let fields = fbb.create_vector(&[field]);
        let schema = crate::Schema::create(
            &mut fbb,
            &crate::SchemaArgs {
                fields: Some(fields),
                ..Default::default()
            },
        );
        fbb.finish_minimal(schema);
        let parsed = crate::root_as_schema(fbb.finished_data());
        if case == 4 {
            assert!(parsed.is_err());
            continue;
        }
        let schema = parsed.unwrap();
        let memory = Memory::new(0);
        let error = read_schema_with_memory(schema, &memory).unwrap_err();
        assert!(
            !error.to_string().contains("denied"),
            "case {case}: {error}"
        );
        assert_eq!(memory.current(), 0);
    }
}
