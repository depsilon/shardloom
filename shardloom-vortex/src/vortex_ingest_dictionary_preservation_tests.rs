use super::*;
use crate::physical_encoding_inventory::{
    PhysicalEncodingInspectionLimits, inspect_physical_encodings,
};
use arrow_array::{
    ArrayRef as ArrowArrayRef, DictionaryArray, Int32Array, RecordBatch, StringArray,
    types::Int32Type,
};
use arrow_schema::{DataType, Field, Schema};
use std::collections::BTreeMap;
use vortex::{
    VortexSessionDefault as _,
    array::{
        VortexSessionExecute as _,
        arrays::{Struct, struct_::StructArrayExt as _},
        iter::ArrayIteratorAdapter,
    },
    file::{OpenOptionsSessionExt as _, WriteOptionsSessionExt as _},
    io::{
        runtime::{BlockingRuntime as _, current::CurrentThreadRuntime},
        session::RuntimeSessionExt as _,
    },
};

const COLUMN: &str = "renamed_domain";

struct Remove(std::path::PathBuf);
impl Drop for Remove {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

fn input(rows: usize, reverse: bool) -> (ArrayRef, Vec<Option<String>>) {
    let mut domain = vec![
        Some("東京.example"),
        Some(""),
        Some("a\0b"),
        None,
        Some("東京.example"),
    ];
    if reverse {
        domain.reverse();
    }
    let keys = (0..rows)
        .map(|row| {
            if row % 7 == 0 {
                None
            } else {
                Some(i32::try_from(row % domain.len()).unwrap())
            }
        })
        .collect::<Vec<_>>();
    let expected = keys
        .iter()
        .map(|key| key.and_then(|key| domain[usize::try_from(key).unwrap()].map(str::to_owned)))
        .collect();
    let values = Arc::new(StringArray::from(domain)) as ArrowArrayRef;
    let dictionary =
        Arc::new(DictionaryArray::<Int32Type>::try_new(Int32Array::from(keys), values).unwrap())
            as ArrowArrayRef;
    let schema = Arc::new(Schema::new(vec![Field::new(
        COLUMN,
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
        true,
    )]));
    let batch = RecordBatch::try_new(schema, vec![dictionary]).unwrap();
    (
        super::super::arrow_record_batch_to_vortex_array(batch).unwrap(),
        expected,
    )
}

#[test]
fn dictionary_preservation_signed_arrow_codes_keep_values_nulls_and_unsigned_consumer_codes() {
    let session = VortexSession::default();
    let timing = IngestStageTimings::default();
    let allowed = session
        .enabled_component_ids(ComponentKind::Array)
        .into_iter()
        .filter(|id| *id != Dict.id())
        .collect();
    let compressor = DictionaryCodes {
        compressor: BtrBlocksCompressorBuilder::default()
            .retain_allowed_encodings(&allowed)
            .build(),
        timings: timing.clone(),
    };
    let (array, _) = input(4096, false);
    let structure = array.as_::<Struct>();
    let dictionary = structure.unmasked_field(0);
    assert_eq!(
        dictionary.as_::<Dict>().codes().dtype().as_ptype(),
        PType::I32
    );
    let mut ctx = session.create_execution_ctx();
    let preserved = compressor.compress_chunk(dictionary, &mut ctx).unwrap();
    let encoded = preserved.as_::<Dict>();
    assert_eq!(encoded.codes().dtype().as_ptype(), PType::U32);
    assert!(ArrayRef::ptr_eq(
        encoded.values(),
        dictionary.as_::<Dict>().values()
    ));
    for row in 0..dictionary.len() {
        assert_eq!(
            preserved.execute_scalar(row, &mut ctx).unwrap(),
            dictionary.execute_scalar(row, &mut ctx).unwrap()
        );
    }
    let fields = timing
        .snapshot()
        .evidence_fields()
        .into_iter()
        .collect::<BTreeMap<_, _>>();
    assert_eq!(
        fields["vortex_ingest_text_dictionary_preserve_rows"],
        "4096"
    );
}

#[test]
#[allow(clippy::too_many_lines)] // Keep full persistence, ownership and consumer proof together.
fn dictionary_preservation_native_epochs_statistics_values_and_public_grouping() {
    use crate::{
        VortexLocalPrimitiveExecutionPolicy, VortexLocalPrimitiveExecutionStatus,
        VortexQueryPrimitiveRequest, VortexSimpleAggregateMeasure, VortexSimpleAggregateRequest,
        execute_vortex_local_primitive_with_policy,
    };
    use shardloom_core::{ColumnRef, DatasetUri};
    let runtime = CurrentThreadRuntime::new();
    let session = VortexSession::default().with_handle(runtime.handle());
    let timing = super::super::VortexWriterStageTiming::default();
    let mut expected = Vec::new();
    let arrays = [(4096, false), (1023, true), (17, false)]
        .into_iter()
        .map(|(rows, reverse)| {
            let (array, values) = input(rows, reverse);
            expected.extend(values);
            array
        })
        .collect::<Vec<_>>();
    let dtype = arrays[0].dtype().clone();
    let mut memory = super::super::NativeIngestMemory::new(64 << 20).unwrap();
    memory.session = memory.session.with_handle(runtime.handle());
    let strategy = super::super::large_source_fast_load_table_strategy_with_dictionaries(
        4096,
        1 << 20,
        1,
        &timing,
        &memory.session,
        true,
    );
    let bounded = super::super::bounded_ingest_layout::BoundedIngestLayout::new(
        Arc::new(strategy),
        0,
        memory.pool.reserve(0).unwrap(),
    );
    let mut bytes = Vec::new();
    let summary = memory
        .session
        .write_options()
        .with_strategy(Arc::new(bounded))
        .blocking(&runtime)
        .write(
            &mut bytes,
            ArrayIteratorAdapter::new(dtype.clone(), arrays.into_iter().map(Ok)),
        )
        .unwrap();
    drop(summary);
    assert_eq!(memory.pool.snapshot().reserved_bytes, 0);
    let file = session.open_options().open_buffer(bytes.clone()).unwrap();
    assert_eq!(file.dtype(), &dtype);
    assert_eq!(file.row_count(), expected.len() as u64);
    let inventory = runtime
        .block_on(inspect_physical_encodings(
            &file,
            PhysicalEncodingInspectionLimits::default(),
        ))
        .unwrap();
    let columns = inventory["columns"].as_array().unwrap();
    assert!(columns.iter().any(|column| {
        column["column_path"] == serde_json::json!([COLUMN])
            && column["encoding_ids"]
                .as_array()
                .unwrap()
                .contains(&serde_json::json!("vortex.dict"))
    }));
    assert!(columns.iter().any(
        |column| column["column_path"] == serde_json::json!([COLUMN])
            && column["auxiliary_path"] == serde_json::json!(["zones"])
    ));
    let mut ctx = session.create_execution_ctx();
    let mut seen = 0;
    for array in file
        .scan()
        .unwrap()
        .with_ordered(true)
        .into_array_iter(&runtime)
        .unwrap()
    {
        let array = array.unwrap();
        for row in 0..array.len() {
            let scalar = array.execute_scalar(row, &mut ctx).unwrap();
            let value = scalar.as_struct().field(COLUMN).unwrap();
            let wanted = expected[seen].as_deref().map_or_else(
                || vortex::array::scalar::Scalar::null(value.dtype().clone()),
                |v| vortex::array::scalar::Scalar::utf8(v, value.dtype().nullability()),
            );
            assert_eq!(value, wanted);
            seen += 1;
        }
    }
    assert_eq!(seen, expected.len());
    let path = std::env::temp_dir().join(format!(
        "shardloom-dictionary-preservation-{}-{}.vortex",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let path = Remove(path);
    std::fs::write(&path.0, bytes).unwrap();
    let request = VortexQueryPrimitiveRequest::simple_aggregate(
        DatasetUri::new(path.0.display().to_string()).unwrap(),
        VortexSimpleAggregateRequest::grouped(
            vec![ColumnRef::new(COLUMN).unwrap()],
            vec![VortexSimpleAggregateMeasure::new("count", None, "n".into())],
        ),
    )
    .with_source_order_limit(expected.len());
    let report = execute_vortex_local_primitive_with_policy(
        &request,
        VortexLocalPrimitiveExecutionPolicy::single_threaded(),
    )
    .unwrap();
    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert!(!report.has_errors(), "{:?}", report.diagnostics);
    assert!(!report.fallback_execution_allowed);
    let payload: serde_json::Value = serde_json::from_str(
        report
            .result_summary
            .as_ref()
            .unwrap()
            .rsplit_once(" values=")
            .unwrap()
            .1,
    )
    .unwrap();
    assert_eq!(
        payload["aggregate_vortex_dictionary_accessor_columns"],
        COLUMN
    );
    let mut oracle = BTreeMap::<Option<String>, u64>::new();
    for value in expected {
        *oracle.entry(value).or_default() += 1;
    }
    let actual = payload["values"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| {
            (
                row[COLUMN].as_str().map(str::to_owned),
                row["n"].as_u64().unwrap(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    assert_eq!(actual, oracle);
}

#[test]
fn dictionary_preservation_inadmissible_empty_tiny_and_oversized_inputs_keep_retained_bytes() {
    let runtime = CurrentThreadRuntime::new();
    let session = VortexSession::default().with_handle(runtime.handle());
    for rows in [0, 1, 8193] {
        let (array, _) = input(rows, false);
        let mut outputs = Vec::new();
        for preserve in [false, true] {
            let timing = super::super::VortexWriterStageTiming::default();
            let strategy = super::super::large_source_fast_load_table_strategy_with_dictionaries(
                4096,
                1 << 20,
                1,
                &timing,
                &session,
                preserve,
            );
            let mut bytes = Vec::new();
            session
                .write_options()
                .with_strategy(Arc::new(strategy))
                .blocking(&runtime)
                .write(&mut bytes, array.to_array_iterator())
                .unwrap();
            let fields = timing
                .stages
                .snapshot()
                .evidence_fields()
                .into_iter()
                .collect::<BTreeMap<_, _>>();
            assert_eq!(fields["vortex_ingest_text_dictionary_preserve_calls"], "0");
            outputs.push(bytes);
        }
        assert_eq!(outputs[0], outputs[1], "{rows} rows");
    }
}

#[test]
fn dictionary_preservation_lazy_values_use_retained_canonicalization() {
    use vortex::array::{
        arrays::StructArray, dtype::FieldNames, scalar_fn::fns::cast::Cast, validity::Validity,
    };
    let runtime = CurrentThreadRuntime::new();
    let session = VortexSession::default().with_handle(runtime.handle());
    let (array, _) = input(4096, false);
    let structure = array.as_::<Struct>();
    let dictionary = structure.unmasked_field(0).as_::<Dict>();
    let lazy = Cast::new(
        dictionary.values().clone(),
        dictionary.values().dtype().clone(),
    )
    .into_array();
    assert!(!lazy.is_canonical());
    assert!(
        !session
            .enabled_component_ids(ComponentKind::Array)
            .contains(&lazy.encoding_id())
    );
    let dictionary = DictArray::try_new(dictionary.codes().clone(), lazy)
        .unwrap()
        .into_array();
    let array = StructArray::try_new(
        FieldNames::from([COLUMN]),
        vec![dictionary],
        4096,
        Validity::NonNullable,
    )
    .unwrap()
    .into_array();
    let mut outputs = Vec::new();
    for preserve in [false, true] {
        let timing = super::super::VortexWriterStageTiming::default();
        let strategy = super::super::large_source_fast_load_table_strategy_with_dictionaries(
            4096,
            1 << 20,
            1,
            &timing,
            &session,
            preserve,
        );
        let mut bytes = Vec::new();
        session
            .write_options()
            .with_strategy(Arc::new(strategy))
            .blocking(&runtime)
            .write(&mut bytes, array.to_array_iterator())
            .unwrap();
        let fields = timing
            .stages
            .snapshot()
            .evidence_fields()
            .into_iter()
            .collect::<BTreeMap<_, _>>();
        assert_eq!(fields["vortex_ingest_text_dictionary_preserve_calls"], "0");
        outputs.push(bytes);
    }
    assert_eq!(outputs[0], outputs[1]);
}

#[test]
fn dictionary_preservation_all_null_codes_ignore_unused_dictionary_values() {
    use vortex::array::{
        arrays::{ConstantArray, StructArray},
        dtype::{FieldNames, Nullability},
        expr::stats::Stat,
        scalar::Scalar,
        validity::Validity,
    };
    let runtime = CurrentThreadRuntime::new();
    let session = VortexSession::default().with_handle(runtime.handle());
    let (input, _) = input(4096, false);
    let values = input
        .as_::<Struct>()
        .unmasked_field(0)
        .as_::<Dict>()
        .values()
        .clone();
    let codes = ConstantArray::new(
        Scalar::null(DType::Primitive(PType::I32, Nullability::Nullable)),
        4096,
    )
    .into_array();
    let dictionary = DictArray::try_new(codes, values).unwrap().into_array();
    let array = StructArray::try_new(
        FieldNames::from([COLUMN]),
        vec![dictionary],
        4096,
        Validity::NonNullable,
    )
    .unwrap()
    .into_array();
    for preserve in [false, true] {
        let timing = super::super::VortexWriterStageTiming::default();
        let strategy = super::super::large_source_fast_load_table_strategy_with_dictionaries(
            4096,
            1 << 20,
            1,
            &timing,
            &session,
            preserve,
        );
        let mut bytes = Vec::new();
        session
            .write_options()
            .with_strategy(Arc::new(strategy))
            .blocking(&runtime)
            .write(&mut bytes, array.to_array_iterator())
            .unwrap();
        let fields = timing
            .stages
            .snapshot()
            .evidence_fields()
            .into_iter()
            .collect::<BTreeMap<_, _>>();
        assert_eq!(
            fields["vortex_ingest_text_dictionary_preserve_calls"],
            if preserve { "1" } else { "0" }
        );
        let file = session.open_options().open_buffer(bytes).unwrap();
        assert_eq!(file.dtype(), array.dtype());
        assert_eq!(file.row_count(), 4096);
        assert_eq!(
            file.footer()
                .statistics()
                .unwrap()
                .get(0)
                .0
                .get(Stat::NullCount)
                .as_exact(),
            Scalar::from(4096_u64).into_value()
        );
        let mut ctx = session.create_execution_ctx();
        let mut seen = 0;
        for chunk in file.scan().unwrap().into_array_iter(&runtime).unwrap() {
            let chunk = chunk.unwrap();
            for row in 0..chunk.len() {
                let scalar = chunk.execute_scalar(row, &mut ctx).unwrap();
                assert!(scalar.as_struct().field(COLUMN).unwrap().is_null());
                seen += 1;
            }
        }
        assert_eq!(seen, 4096);
    }
}

#[test]
fn dictionary_preservation_late_source_failure_releases_owned_credits() {
    let runtime = CurrentThreadRuntime::new();
    let mut memory = super::super::NativeIngestMemory::new(64 << 20).unwrap();
    memory.session = memory.session.with_handle(runtime.handle());
    let timing = super::super::VortexWriterStageTiming::default();
    let strategy = super::super::large_source_fast_load_table_strategy_with_dictionaries(
        4096,
        1 << 20,
        1,
        &timing,
        &memory.session,
        true,
    );
    let bounded = super::super::bounded_ingest_layout::BoundedIngestLayout::new(
        Arc::new(strategy),
        0,
        memory.pool.reserve(0).unwrap(),
    );
    let (array, _) = input(4096, false);
    let dtype = array.dtype().clone();
    let input = ArrayIteratorAdapter::new(
        dtype,
        [
            Ok(array),
            Err(vortex_err!("injected late dictionary source failure")),
        ]
        .into_iter(),
    );
    let mut bytes = Vec::new();
    let result = memory
        .session
        .write_options()
        .with_strategy(Arc::new(bounded))
        .blocking(&runtime)
        .write(&mut bytes, input);
    assert!(
        result
            .err()
            .expect("late source failure must propagate")
            .to_string()
            .contains("injected late dictionary source failure")
    );
    let fields = timing
        .stages
        .snapshot()
        .evidence_fields()
        .into_iter()
        .collect::<BTreeMap<_, _>>();
    assert_eq!(fields["vortex_ingest_text_dictionary_preserve_calls"], "1");
    assert!(memory.pool.snapshot().peak_reserved_bytes > 0);
    assert_eq!(memory.pool.snapshot().reserved_bytes, 0);
    assert!(memory.session.open_options().open_buffer(bytes).is_err());
}
