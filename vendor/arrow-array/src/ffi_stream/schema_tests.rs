// SPDX-License-Identifier: Apache-2.0
// Original ShardLoom regression tests for dictionary extension metadata.

use super::*;
use arrow_schema::{UnionMode, ffi::Flags};

fn uuid() -> FFI_ArrowSchema {
    FFI_ArrowSchema::try_new("w:16", vec![], None)
        .unwrap()
        .with_metadata([(EXTENSION_NAME, "arrow.uuid"), (EXTENSION_METADATA, "")])
        .unwrap()
}

fn dictionary(values: FFI_ArrowSchema) -> FFI_ArrowSchema {
    FFI_ArrowSchema::try_new("c", vec![], Some(values))
        .unwrap()
        .with_name("id")
        .unwrap()
        .with_flags(Flags::NULLABLE | Flags::DICTIONARY_ORDERED)
        .unwrap()
}

fn schema(field: FFI_ArrowSchema) -> FFI_ArrowSchema {
    FFI_ArrowSchema::try_new("+s", vec![field], None)
        .unwrap()
        .with_metadata([("table-note", "kept")])
        .unwrap()
}

fn assert_uuid(field: &Field) {
    assert_eq!(field.metadata()[EXTENSION_NAME], "arrow.uuid");
    assert_eq!(field.metadata()[EXTENSION_METADATA], "");
    assert_eq!(
        field.data_type(),
        &DataType::Dictionary(
            Box::new(DataType::Int8),
            Box::new(DataType::FixedSizeBinary(16))
        )
    );
}

#[test]
fn dictionary_values_preserve_extensions_and_field_attributes() {
    let raw = schema(
        dictionary(uuid())
            .with_metadata([("field-note", "also kept")])
            .unwrap(),
    );
    let imported = import(&raw).unwrap();
    assert_eq!(imported.metadata()["table-note"], "kept");
    let field = imported.field(0);
    assert_uuid(field);
    assert_eq!(field.name(), "id");
    assert!(field.is_nullable());
    assert_eq!(field.dict_is_ordered(), Some(true));
    assert_eq!(field.metadata()["field-note"], "also kept");
}

#[test]
fn nested_dictionary_extensions_preserve_each_container() {
    for format in [
        "+l", "+L", "+vl", "+vL", "+w:2", "+s", "+m", "+ud:7", "+us:7",
    ] {
        let child = if format == "+m" {
            FFI_ArrowSchema::try_new(
                "+s",
                vec![
                    FFI_ArrowSchema::try_new("i", vec![], None).unwrap(),
                    dictionary(uuid()),
                ],
                None,
            )
            .unwrap()
        } else {
            dictionary(uuid())
        };
        let raw = schema(FFI_ArrowSchema::try_new(format, vec![child], None).unwrap());
        let imported = import(&raw).unwrap();
        let leaf = match imported.field(0).data_type() {
            DataType::List(field)
            | DataType::LargeList(field)
            | DataType::ListView(field)
            | DataType::LargeListView(field)
            | DataType::FixedSizeList(field, 2) => field,
            DataType::Struct(fields) => &fields[0],
            DataType::Map(entries, false) => {
                let DataType::Struct(fields) = entries.data_type() else {
                    panic!("map entries")
                };
                &fields[1]
            }
            DataType::Union(fields, mode) => {
                assert_eq!(
                    *mode,
                    if format == "+ud:7" {
                        UnionMode::Dense
                    } else {
                        UnionMode::Sparse
                    }
                );
                let (id, field) = fields.iter().next().unwrap();
                assert_eq!(id, 7);
                field
            }
            other => panic!("unexpected container: {other:?}"),
        };
        assert_uuid(leaf);
    }
    let raw = schema(
        FFI_ArrowSchema::try_new(
            "+r",
            vec![
                FFI_ArrowSchema::try_new("i", vec![], None).unwrap(),
                dictionary(uuid()),
            ],
            None,
        )
        .unwrap(),
    );
    let imported = import(&raw).unwrap();
    let DataType::RunEndEncoded(_, values) = imported.field(0).data_type() else {
        panic!("run ends")
    };
    assert_uuid(values);
}

#[test]
fn dictionary_struct_values_keep_child_extension_metadata() {
    let values = FFI_ArrowSchema::try_new("+s", vec![dictionary(uuid())], None).unwrap();
    let imported = import(&schema(dictionary(values))).unwrap();
    let DataType::Dictionary(_, values) = imported.field(0).data_type() else {
        panic!("dictionary")
    };
    let DataType::Struct(fields) = values.as_ref() else {
        panic!("struct")
    };
    assert_uuid(&fields[0]);
    assert!(imported.field(0).metadata().is_empty());
}

#[test]
fn matching_declarations_and_dictionary_chains_are_preserved() {
    let raw = schema(
        dictionary(uuid())
            .with_metadata([(EXTENSION_NAME, "arrow.uuid")])
            .unwrap(),
    );
    assert_uuid(import(&raw).unwrap().field(0));
    let imported = import(&schema(dictionary(dictionary(uuid())))).unwrap();
    assert_eq!(imported.field(0).metadata()[EXTENSION_NAME], "arrow.uuid");
    let DataType::Dictionary(_, values) = imported.field(0).data_type() else {
        panic!("outer dictionary")
    };
    assert!(matches!(values.as_ref(), DataType::Dictionary(_, _)));
}

#[test]
fn conflicting_declarations_fail_instead_of_selecting_an_identity() {
    for metadata in [
        vec![(EXTENSION_NAME, "different.extension")],
        vec![
            (EXTENSION_NAME, "arrow.uuid"),
            (EXTENSION_METADATA, "nonempty"),
        ],
        vec![(EXTENSION_METADATA, "")],
    ] {
        let raw = schema(dictionary(uuid()).with_metadata(metadata).unwrap());
        assert!(
            import(&raw)
                .unwrap_err()
                .to_string()
                .contains("conflicting extension")
        );
    }
}

#[test]
fn unannotated_and_incomplete_declarations_are_not_inferred_or_erased() {
    let plain = FFI_ArrowSchema::try_new("w:16", vec![], None).unwrap();
    assert!(
        import(&schema(dictionary(plain)))
            .unwrap()
            .field(0)
            .metadata()
            .is_empty()
    );
    let incomplete = FFI_ArrowSchema::try_new("w:16", vec![], None)
        .unwrap()
        .with_metadata([(EXTENSION_METADATA, "unexpected")])
        .unwrap();
    let imported = import(&schema(dictionary(incomplete))).unwrap();
    assert!(!imported.field(0).metadata().contains_key(EXTENSION_NAME));
    assert_eq!(
        imported.field(0).metadata()[EXTENSION_METADATA],
        "unexpected"
    );
}
