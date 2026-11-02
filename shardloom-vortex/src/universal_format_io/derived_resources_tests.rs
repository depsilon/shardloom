use super::*;
use crate::universal_format_io::{
    self as io, IngestColumnarStreamSource,
    owned_batch::{BatchResult, OwnedBatchReader, SchemaReader},
};
use arrow_array::{DictionaryArray, Int64Array, types::Int64Type};
use arrow_schema::{Field, Schema, TimeUnit};
use std::sync::atomic::{AtomicUsize, Ordering};

const INPUT_CREDIT: u64 = 8192;

struct Reader {
    schema: SchemaRef,
    batch: Option<RecordBatch>,
    memory: LiveMemoryPool,
    calls: Arc<AtomicUsize>,
}

impl Iterator for Reader {
    type Item = BatchResult;

    fn next(&mut self) -> Option<Self::Item> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        let batch = self.batch.take()?;
        Some(
            self.memory
                .reserve(INPUT_CREDIT)
                .map(|credit| OwnedBatch::with_owner(batch, Arc::new(credit)))
                .map_err(|error| arrow_schema::ArrowError::ExternalError(Box::new(error))),
        )
    }
}

impl OwnedBatchReader for Reader {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

fn source(
    batch: RecordBatch,
    memory: &LiveMemoryPool,
    calls: &Arc<AtomicUsize>,
) -> IngestColumnarStreamSource {
    let schema = batch.schema();
    let names = schema
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect::<Vec<_>>();
    let source = io::flat_columnar_stream_source_from_reader(
        &schema,
        names.clone(),
        names.clone(),
        names,
        Some(batch.num_rows()),
        &io::FlatColumnarStreamSourcePlan::product_batches(
            8192,
            None,
            "derived_resource_test",
            "unchanged",
        ),
        Box::new(SchemaReader(Arc::clone(&schema))),
    );
    IngestColumnarStreamSource::from_owned(
        source,
        Box::new(Reader {
            schema,
            batch: Some(batch),
            memory: memory.clone(),
            calls: Arc::clone(calls),
        }),
        memory,
    )
}

fn url_batch() -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("URL", DataType::Utf8, true)])),
        vec![Arc::new(StringArray::from(vec![
            Some("https://example.test/a"),
            None,
            Some(""),
        ]))],
    )
    .unwrap()
}

#[test]
fn derived_schema_denial_precedes_source_demand_and_is_terminal() {
    let memory = LiveMemoryPool::new(1024).unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let source = source(url_batch(), &memory, &calls).with_embedded_derived_columns();
    let (metadata, _, mut reader) = source.into_parts();
    let error = reader.next().unwrap().unwrap_err();
    assert!(
        error.to_string().contains("memory reservation denied"),
        "{error}"
    );
    assert!(reader.next().is_none());
    assert_eq!(calls.load(Ordering::Relaxed), 0);
    drop((reader, metadata));
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn derived_batch_denial_releases_input_without_repeating_the_producer() {
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let source = source(url_batch(), &memory, &calls).with_embedded_derived_columns();
    let metadata_bytes = memory.snapshot().reserved_bytes;
    assert!(metadata_bytes > 0);
    let held = memory
        .reserve((1 << 20) - metadata_bytes - INPUT_CREDIT - 1)
        .unwrap();
    let (metadata, _, mut reader) = source.into_parts();
    let error = reader.next().unwrap().unwrap_err();
    assert!(
        error.to_string().contains("memory reservation denied"),
        "{error}"
    );
    assert!(reader.next().is_none());
    assert_eq!(calls.load(Ordering::Relaxed), 1);
    assert_eq!(
        memory.snapshot().reserved_bytes,
        held.bytes() + metadata_bytes
    );
    drop((reader, metadata));
    assert_eq!(memory.snapshot().reserved_bytes, held.bytes());
    drop(held);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn derived_noop_does_not_require_a_new_workspace_or_redeclare_columns() {
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let source = source(url_batch(), &memory, &calls).with_embedded_derived_columns();
    let before = memory.snapshot().reserved_bytes;
    let held = memory.reserve((1 << 20) - before).unwrap();
    let source = source.with_embedded_derived_columns();
    assert_eq!(source.reader_projection_columns.len(), 3);
    assert_eq!(memory.snapshot().reserved_bytes, before + held.bytes());
    drop(held);
    let (metadata, _, mut reader) = source.into_parts();
    let batch = reader.next().unwrap().unwrap();
    assert_eq!((batch.num_columns(), batch.num_rows()), (3, 3));
    drop((reader, metadata));
    assert!(memory.snapshot().reserved_bytes > INPUT_CREDIT);
    drop(batch);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn derived_source_metadata_retains_its_credit_after_the_reader_drops() {
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let source = source(url_batch(), &memory, &calls)
        .with_embedded_derived_columns()
        .with_source_native_embedded_derived_columns();
    let admitted = memory.snapshot().reserved_bytes;
    assert!(admitted > 0);
    let (metadata, _, reader) = source.into_parts();
    drop(reader);
    assert_eq!(calls.load(Ordering::Relaxed), 0);
    assert_eq!(memory.snapshot().reserved_bytes, admitted);
    assert_eq!(metadata.reader.schema().fields().len(), 3);
    assert!(
        metadata
            .source_dictionary_preservation_status
            .contains("not_available")
    );
    drop(metadata);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn derived_string_layouts_admit_large_values_empty_batches_and_nulls() {
    use arrow_array::types::{Int32Type, UInt32Type};

    let domain = format!("{}.test", "a".repeat(65_536));
    let url = format!("https://{domain}/path");
    let values = vec![Some(url.as_str()), None, Some("")];
    let layouts: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(values.clone())),
        Arc::new(LargeStringArray::from(values.clone())),
        Arc::new(StringViewArray::from(values)),
    ];
    for array in layouts {
        for rows in [0, 3] {
            let array = array.slice(0, rows);
            let schema = Arc::new(Schema::new(vec![Field::new(
                "URL",
                array.data_type().clone(),
                true,
            )]));
            let batch = RecordBatch::try_new(Arc::clone(&schema), vec![array]).unwrap();
            let input_bytes = batch.get_array_memory_size() as u64;
            let memory = LiveMemoryPool::new(input_bytes + (1 << 20)).unwrap();
            let input =
                OwnedBatch::with_owner(batch, Arc::new(memory.reserve(input_bytes).unwrap()));
            let specs =
                io::embedded_derived_column_specs(&schema, EmbeddedDerivedColumnMode::FullAdapter);
            let target = Arc::new(io::embedded_derived_column_schema(&schema, &specs));
            let output = append(input, &target, &specs, Some(&memory), None).unwrap();
            assert_eq!((output.num_rows(), output.num_columns()), (rows, 3));
            if rows != 0 {
                let lengths = output.column(1).as_dictionary::<Int32Type>();
                let domains = output.column(2).as_dictionary::<Int32Type>();
                assert_eq!(
                    lengths
                        .values()
                        .as_primitive::<UInt32Type>()
                        .value(usize::try_from(lengths.keys().value(0)).unwrap()),
                    u32::try_from(url.len()).unwrap()
                );
                assert_eq!(
                    domains
                        .values()
                        .as_string::<i32>()
                        .value(usize::try_from(domains.keys().value(0)).unwrap()),
                    domain
                );
                assert!(lengths.is_null(1));
                assert!(domains.is_null(1));
            }
            assert!(memory.snapshot().reserved_bytes > input_bytes);
            drop(output);
            assert_eq!(memory.snapshot().reserved_bytes, 0);
        }
    }
}

#[test]
fn derived_dictionary_workspace_includes_unreferenced_domain_values() {
    let values: ArrayRef = Arc::new(StringArray::from_iter_values(
        (0..4096).map(|index| format!("https://site{index}.test/path")),
    ));
    let dictionary =
        DictionaryArray::<Int64Type>::try_new(Int64Array::from(vec![0]), values).unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "URL",
        dictionary.data_type().clone(),
        false,
    )]));
    let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(dictionary)]).unwrap();
    let input_bytes = batch.get_array_memory_size() as u64;
    let memory = LiveMemoryPool::new(input_bytes + (64 << 10)).unwrap();
    let input = OwnedBatch::with_owner(batch, Arc::new(memory.reserve(input_bytes).unwrap()));
    let specs = io::embedded_derived_column_specs(&schema, EmbeddedDerivedColumnMode::FullAdapter);
    let target = Arc::new(io::embedded_derived_column_schema(&schema, &specs));
    let error = append(input, &target, &specs, Some(&memory), None).unwrap_err();
    assert!(
        error.to_string().contains("memory reservation denied"),
        "{error}"
    );
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn derived_dictionary_slice_keeps_shared_keys_without_recharging_the_backing_capacity() {
    let keys = Int64Array::from_iter_values((0..65_536).map(|_| 0));
    let values: ArrayRef = Arc::new(StringArray::from(vec!["https://example.test/a"]));
    let dictionary = DictionaryArray::<Int64Type>::try_new(keys, values)
        .unwrap()
        .slice(65_534, 2);
    let schema = Arc::new(Schema::new(vec![Field::new(
        "URL",
        dictionary.data_type().clone(),
        false,
    )]));
    let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(dictionary)]).unwrap();
    let input_bytes = batch.get_array_memory_size() as u64;
    let memory = LiveMemoryPool::new(input_bytes + (128 << 10)).unwrap();
    let input = OwnedBatch::with_owner(batch, Arc::new(memory.reserve(input_bytes).unwrap()));
    let specs = io::embedded_derived_column_specs(&schema, EmbeddedDerivedColumnMode::FullAdapter);
    let target = Arc::new(io::embedded_derived_column_schema(&schema, &specs));
    let output = append(input, &target, &specs, Some(&memory), None).unwrap();
    assert_eq!((output.num_rows(), output.num_columns()), (2, 3));
    let original = output.column(0).as_any_dictionary();
    let lengths = output.column(1).as_any_dictionary();
    assert!(original.keys().to_data().buffers()[0].ptr_eq(&lengths.keys().to_data().buffers()[0]));
    assert!(memory.snapshot().reserved_bytes - input_bytes < 16 << 10);
    drop(output);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn derived_planning_preserves_modes_existing_names_unicode_and_kind_order() {
    let schema = Schema::new(vec![
        Field::new("URL", DataType::Utf8, true),
        Field::new(
            "EventTime",
            DataType::Timestamp(TimeUnit::Second, None),
            true,
        ),
        Field::new("url/λ", DataType::LargeUtf8, true),
        Field::new("__shardloom_derived_utf8_len_URL", DataType::UInt32, true),
    ]);
    let names = |mode| {
        io::embedded_derived_column_specs(&schema, mode)
            .into_iter()
            .map(|spec| spec.output_column)
            .collect::<Vec<_>>()
    };
    assert_eq!(
        names(EmbeddedDerivedColumnMode::FullAdapter),
        vec![
            "__shardloom_derived_url_domain_URL",
            "__shardloom_derived_extract_minute_EventTime",
            "__shardloom_derived_date_trunc_minute_EventTime",
            "__shardloom_derived_utf8_len_url__",
            "__shardloom_derived_url_domain_url__",
        ]
    );
    assert_eq!(
        names(EmbeddedDerivedColumnMode::SourceNativeOnly),
        vec![
            "__shardloom_derived_extract_minute_EventTime",
            "__shardloom_derived_date_trunc_minute_EventTime",
        ]
    );
    assert_eq!(
        names(EmbeddedDerivedColumnMode::SourceNativeLeanRuntime),
        vec![
            "__shardloom_derived_url_domain_URL",
            "__shardloom_derived_extract_minute_EventTime",
            "__shardloom_derived_date_trunc_minute_EventTime",
        ]
    );
}
