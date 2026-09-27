use super::super::*;
use vortex::array::{
    IntoArray as _, VortexSessionExecute as _,
    arrays::{DictArray, PrimitiveArray, VarBinViewArray},
};

fn dictionary(values: vortex::array::ArrayRef, codes: Vec<Option<u32>>) -> vortex::array::ArrayRef {
    DictArray::try_new(PrimitiveArray::from_option_iter(codes).into_array(), values)
        .unwrap()
        .into_array()
}

#[test]
fn sparse_dictionary_handoff_preserves_rows_and_bounds_both_consumers() {
    let mut strings = vec![Some("unreferenced payload"); 4096];
    strings[7] = Some("café");
    strings[55] = None;
    strings[99] = Some("");
    let values = VarBinViewArray::from_iter_nullable_str(strings).into_array();
    let chunk = dictionary(values, vec![Some(99), Some(7), Some(7), None, Some(55)]);
    let accessor = aggregate_direct_utf8_dictionary_accessor(&chunk)
        .unwrap()
        .unwrap();
    let expected = [
        StatValue::Utf8(String::new()),
        StatValue::Utf8("café".into()),
        StatValue::Utf8("café".into()),
        StatValue::Null,
        StatValue::Null,
    ];
    for (row, value) in expected.iter().enumerate() {
        assert_eq!(&aggregate_direct_stat_value(&accessor, row).unwrap(), value);
    }
    let AggregateDirectColumnAccessor::Utf8Dictionary { values, .. } = &accessor else {
        panic!("native dictionary ownership must survive the handoff");
    };
    assert_eq!(
        values.len(),
        3,
        "unused dictionary values must not be owned"
    );
    assert_eq!(values.iter().map(|s| s.len()).sum::<usize>(), "café".len());

    let uri = DatasetUri::new("file:///tmp/sparse-dictionary.vortex").unwrap();
    let inputs =
        reader_generated_encoded_kernel_inputs_from_vortex_chunk(&uri, "sparse", &chunk).unwrap();
    let input = &inputs[0];
    assert!(input.mapping_evidence_complete());
    assert!(input.provider_boundary.is_policy_admitted());
    assert!(!input.has_forbidden_effects());
    assert_eq!(input.batch.segment.stats.row_count, Some(5));
    assert_eq!(input.batch.segment.stats.null_count, Some(2));
    let EncodedValueBatch::Dictionary { dictionary, codes } = &input.batch.values else {
        panic!("expected dictionary evidence");
    };
    assert_eq!(
        dictionary.len(),
        3,
        "evidence must not retain the unused domain"
    );
    let observed = codes
        .iter()
        .map(|code| {
            code.and_then(|code| dictionary[code as usize].clone())
                .unwrap_or(StatValue::Null)
        })
        .collect::<Vec<_>>();
    assert_eq!(observed, expected);
}

#[test]
fn sparse_dictionary_handoff_selects_encoded_values_before_canonicalization() {
    use vortex::encodings::fsst::{fsst_compress, fsst_train_compressor};
    let strings = (0..4096)
        .map(|i| format!("https://example.test/path/{i}"))
        .collect::<Vec<_>>();
    let values = VarBinViewArray::from_iter_str(&strings).into_array();
    let mut ctx = vortex::array::legacy_session().create_execution_ctx();
    let compressor = fsst_train_compressor(&values, &mut ctx).unwrap();
    let encoded = fsst_compress(&values, &compressor, &mut ctx)
        .unwrap()
        .into_array();
    let chunk = dictionary(encoded, vec![Some(3000), Some(7), Some(3000)]);
    let accessor = aggregate_direct_utf8_dictionary_accessor(&chunk)
        .unwrap()
        .unwrap();
    let AggregateDirectColumnAccessor::Utf8Dictionary {
        values, row_ids, ..
    } = &accessor
    else {
        panic!("expected dictionary accessor");
    };
    assert_eq!(values.len(), 2);
    assert_eq!(row_ids, &[1, 0, 1]);
    for (row, source) in [3000, 7, 3000].into_iter().enumerate() {
        assert_eq!(
            aggregate_direct_stat_value(&accessor, row).unwrap(),
            StatValue::Utf8(strings[source].clone())
        );
    }
}

#[test]
fn sparse_dictionary_handoff_handles_empty_all_null_and_invalid_codes() {
    let values = VarBinViewArray::from_iter_str(vec!["unused"; 32]).into_array();
    for codes in [vec![], vec![None, None]] {
        let chunk = dictionary(values.clone(), codes.clone());
        let accessor = aggregate_direct_utf8_dictionary_accessor(&chunk)
            .unwrap()
            .unwrap();
        let AggregateDirectColumnAccessor::Utf8Dictionary { values, .. } = &accessor else {
            panic!("expected dictionary accessor");
        };
        assert!(values.is_empty());
        for row in 0..codes.len() {
            assert_eq!(
                aggregate_direct_stat_value(&accessor, row).unwrap(),
                StatValue::Null
            );
        }
    }
    // Null-row placeholder codes may be arbitrary and must not be dereferenced.
    let mut null_codes = [u32::MAX];
    assert_eq!(
        dictionary_handoff::referenced_values(&values, &mut null_codes, Some(&[true]))
            .unwrap()
            .len(),
        0
    );
    assert!(
        dictionary_handoff::referenced_values(&values, &mut [u32::MAX], None)
            .unwrap_err()
            .to_string()
            .contains("valid code exceeds")
    );
    assert!(
        dictionary_handoff::referenced_values(&values, &mut [0], Some(&[]))
            .unwrap_err()
            .to_string()
            .contains("validity length mismatch")
    );
}

#[test]
fn dense_dictionary_handoff_preserves_domain_and_native_group_results() {
    let values = VarBinViewArray::from_iter_str(["same", "other", "same"]).into_array();
    let mut codes = [2, 0, 1, 2];
    let dense = dictionary_handoff::referenced_values(&values, &mut codes, None).unwrap();
    assert!(vortex::array::ArrayRef::ptr_eq(&values, &dense));
    assert_eq!(codes, [2, 0, 1, 2]);
    // Duplicated dictionary values remain equivalent group keys after remapping.
    let mut strings = vec!["unused"; 64];
    strings[7] = "same";
    strings[49] = "same";
    let chunk = dictionary(
        VarBinViewArray::from_iter_str(strings).into_array(),
        vec![Some(49), Some(7), None],
    );
    let request = VortexSimpleAggregateRequest::grouped(
        vec![ColumnRef::new("k").unwrap()],
        vec![crate::VortexSimpleAggregateMeasure::new(
            "count",
            None,
            "n".into(),
        )],
    );
    let columns = vec!["k".into()];
    let mut states = GroupedAggregateStates::new(&request, None, &columns, false, false).unwrap();
    assert!(
        states
            .update_count_star_direct_from_chunk(&chunk, &columns, None)
            .unwrap()
    );
    let (rows, summary) = states.result_row_count_and_summary(None).unwrap();
    assert_eq!(rows, 2);
    let payload: serde_json::Value = serde_json::from_str(&summary).unwrap();
    let actual = payload["values"].as_array().unwrap();
    assert!(actual.contains(&serde_json::json!({"k":"same","n":2})));
    assert!(actual.contains(&serde_json::json!({"k":null,"n":1})));
}
