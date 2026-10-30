use super::*;
use arrow_array::{
    ArrayRef as ArrowArrayRef, BinaryArray, Float32Array, Int8Array, Int16Array, Int32Array,
    StructArray as ArrowStructArray, UInt8Array, UInt16Array, UInt32Array,
};
use arrow_buffer::NullBuffer;

fn rich_fixture() -> (RecordBatch, Vec<serde_json::Value>) {
    let base = super::super::lifecycle_tests::typed_batch();
    let mut columns: Vec<(String, ArrowArrayRef)> = base
        .schema()
        .fields()
        .iter()
        .zip(base.columns())
        .map(|(field, array)| (field.name().clone(), Arc::clone(array)))
        .collect();
    let mut expected = vec![
        json!({"day":-1,"instant":-1,"amount":"decimal128(12,2):-12345","items":[-9,null],"flag":true}),
        json!({"day":20000,"instant":1_700_000_000_123_456_i64,"amount":"decimal128(12,2):0","items":[],"flag":false}),
        json!({"day":null,"instant":null,"amount":null,"items":null,"flag":null}),
        json!({"day":0,"instant":0,"amount":"decimal128(12,2):999999","items":[i64::MAX],"flag":true}),
    ];
    macro_rules! numbers {
        ($array:ident, $name:literal, $low:expr, $zero:expr, $high:expr) => {{
            columns.push((
                $name.into(),
                Arc::new($array::from(vec![
                    Some($low),
                    None,
                    Some($zero),
                    Some($high),
                ])),
            ));
            for (row, value) in
                expected
                    .iter_mut()
                    .zip([json!($low), json!(null), json!($zero), json!($high)])
            {
                row.as_object_mut().unwrap().insert($name.into(), value);
            }
        }};
    }
    numbers!(Int8Array, "i8", i8::MIN, 0_i8, i8::MAX);
    numbers!(Int16Array, "i16", i16::MIN, 0_i16, i16::MAX);
    numbers!(Int32Array, "i32", i32::MIN, 0_i32, i32::MAX);
    numbers!(Int64Array, "i64", i64::MIN, 0_i64, i64::MAX);
    numbers!(UInt8Array, "u8", 0_u8, 7_u8, u8::MAX);
    numbers!(UInt16Array, "u16", 0_u16, 7_u16, u16::MAX);
    numbers!(UInt32Array, "u32", 0_u32, 7_u32, u32::MAX);
    numbers!(UInt64Array, "u64", 0_u64, 7_u64, u64::MAX);
    numbers!(Float32Array, "f32", -0.0_f32, 1.25_f32, 17.5_f32);
    append_rich_columns(&mut columns, &mut expected);
    let schema = Arc::new(Schema::new(
        columns
            .iter()
            .map(|(name, array)| Field::new(name, array.data_type().clone(), true))
            .collect::<Vec<_>>(),
    ));
    (
        RecordBatch::try_new(
            schema,
            columns.into_iter().map(|(_, array)| array).collect(),
        )
        .unwrap(),
        expected,
    )
}

fn append_rich_columns(
    columns: &mut Vec<(String, ArrowArrayRef)>,
    expected: &mut [serde_json::Value],
) {
    let binary = BinaryArray::from(vec![
        Some(&b"\0\xfflong-binary-payload"[..]),
        None,
        Some(&b""[..]),
        Some(&b"abc"[..]),
    ]);
    columns.push(("bytes".into(), Arc::new(binary)));
    for (row, value) in expected.iter_mut().zip([
        json!("00ff6c6f6e672d62696e6172792d7061796c6f6164"),
        json!(null),
        json!(""),
        json!("616263"),
    ]) {
        row.as_object_mut().unwrap().insert("bytes".into(), value);
    }
    let fields = vec![
        Field::new("score", DataType::Int64, true),
        Field::new("label", DataType::Utf8, true),
    ];
    let nested = ArrowStructArray::new(
        fields.into(),
        vec![
            Arc::new(Int64Array::from(vec![Some(-13), None, Some(99), Some(21)])),
            Arc::new(StringArray::from(vec![
                Some("nested λ\0猫"),
                None,
                Some("hidden"),
                Some(""),
            ])),
        ],
        Some(NullBuffer::from(vec![true, true, false, true])),
    );
    columns.push(("record".into(), Arc::new(nested)));
    for (row, value) in expected.iter_mut().zip([
        json!({"score":-13,"label":"nested λ\0猫"}),
        json!({"score":null,"label":null}),
        json!(null),
        json!({"score":21,"label":""}),
    ]) {
        row.as_object_mut().unwrap().insert("record".into(), value);
    }
}

#[test]
fn parquet_query_transfer_preserves_all_primitive_widths_and_rich_payload_domains() {
    for dictionary in [false, true] {
        let (batch, expected) = rich_fixture();
        let fixture = ParquetFixture::with_properties(
            batch,
            WriterProperties::builder()
                .set_dictionary_enabled(dictionary)
                .set_max_row_group_row_count(Some(3))
                .build(),
        );
        let prepared = fixture.prepare(&fixture.scan(&[], None));
        let initial = prepared.snapshot().memory.reserved_bytes;
        let result = prepared
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        assert_eq!(json_rows(&result), expected);
        let input = result.execution.input.as_ref().unwrap();
        assert_eq!(input.columnar_row_groups_read, 2);
        assert_eq!(
            input.intake_columnar_buffer_bytes_copied
                + input.intake_columnar_buffer_bytes_transferred,
            referenced_bytes(&fixture)
        );
        assert!(input.end_of_input_observed && input.output_ownership_detached);
        drop(result);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, initial);
        let destination = fixture.0.join("native.vortex");
        prepared
            .write(
                &destination,
                crate::local_primitives::VortexLocalPrimitiveRowExportFormat::Vortex,
                false,
            )
            .unwrap();
        let native = prepare_relational(
            &VortexRelationalPlan::Scan(VortexRelationalScan {
                source_uri: DatasetUri::new(destination.display().to_string()).unwrap(),
                projection: ProjectionRequest::All,
                predicate: None,
            }),
            policy(),
        )
        .unwrap();
        assert_eq!(
            native.output_dtype().unwrap(),
            prepared.output_dtype().unwrap()
        );
        let result = native.collect_jsonl(&CancellationToken::default()).unwrap();
        assert_eq!(json_rows(&result), expected);
        assert!(result.execution.input.is_none());
    }
}
