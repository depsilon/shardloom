use super::*;
use arrow_array::{StringArray, StringViewArray};
use arrow_schema::{DataType, Field, Schema};
use parquet::{
    arrow::{
        ArrowWriter,
        arrow_reader::{ArrowReaderOptions, ParquetRecordBatchReaderBuilder},
    },
    basic::{Compression, Encoding},
    file::properties::WriterProperties,
};
use vortex::{
    array::{
        VortexSessionExecute as _,
        arrays::{Struct, StructArray, VarBinView, VarBinViewArray, struct_::StructArrayExt as _},
        expr::stats::{Precision, Stat, StatsProvider as _},
        iter::{ArrayIteratorAdapter, ArrayIteratorExt as _},
    },
    file::{OpenOptionsSessionExt as _, WriteOptionsSessionExt as _},
    io::{
        runtime::{BlockingRuntime as _, current::CurrentThreadRuntime},
        session::RuntimeSessionExt as _,
    },
};

use crate::physical_encoding_inventory::{
    PhysicalEncodingInspectionLimits, inspect_physical_encodings,
};

#[test]
#[allow(clippy::too_many_lines)] // Trace one complete real reader/intake/writer path.
fn parquet_view_intake_retains_physical_credit_but_writer_costs_referenced_bytes() {
    let values = (0..8192)
        .map(|row| (row % 7 != 0).then(|| format!("row={row:05}:{}", "encoded 東京 ".repeat(5))))
        .collect::<Vec<_>>();
    let schema = Arc::new(Schema::new(vec![Field::new("text", DataType::Utf8, true)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(StringArray::from_iter(
            values.iter().map(Option::as_deref),
        ))],
    )
    .unwrap();
    let properties = WriterProperties::builder()
        .set_dictionary_enabled(false)
        .set_encoding(Encoding::PLAIN)
        .set_compression(Compression::UNCOMPRESSED)
        .set_data_page_size_limit(2 << 20)
        .set_data_page_row_count_limit(8192)
        .build();
    let mut parquet = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut parquet, schema, Some(properties)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let view_schema = Arc::new(Schema::new(vec![Field::new(
        "text",
        DataType::Utf8View,
        true,
    )]));
    let reader = ParquetRecordBatchReaderBuilder::try_new_with_options(
        bytes::Bytes::from(parquet),
        ArrowReaderOptions::new().with_schema(view_schema),
    )
    .unwrap()
    .with_batch_size(1024)
    .build()
    .unwrap();
    let memory = NativeIngestMemory::new(32 << 20).unwrap();
    let pool = memory.pool.clone();
    let timing = IngestStageTimings::default();
    let mut inputs = Vec::new();
    let mut expected_sizes = Vec::new();
    for (index, batch) in reader.enumerate() {
        let batch = batch.unwrap();
        let views = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringViewArray>()
            .unwrap();
        let logical_bytes = values[index * 1024..(index + 1) * 1024]
            .iter()
            .flatten()
            .map(String::len)
            .sum::<usize>();
        // A reader batch references part of a larger decoded Parquet page.
        let held_bytes = views
            .data_buffers()
            .iter()
            .map(|buffer| buffer.as_slice().len())
            .sum::<usize>();
        assert!(
            held_bytes > logical_bytes * 2,
            "{held_bytes} <= 2 * {logical_bytes}"
        );
        let before = pool.snapshot().reserved_bytes;
        let mut lease = memory.reserve_input(1).unwrap();
        let input = record_batch_to_vortex_from_arrow_provider_profiled_with_memory(
            &batch,
            &FlatColumnarSourceShape {
                projected_columns: vec![ColumnarProjectedColumn {
                    column: "text".into(),
                    reader_index: 0,
                    dtype_hint: None,
                    arrow_dtype_hint: Some(DataType::Utf8View),
                }],
            },
            &timing,
            Some((&memory, &mut lease)),
        )
        .unwrap();
        assert_eq!(lease.bytes(), 0);
        let field = input.as_::<Struct>().unmasked_field(0).clone();
        assert!(field.is::<VarBinView>());
        assert!(pool.snapshot().reserved_bytes - before >= held_bytes as u64);
        assert!(field.nbytes() >= held_bytes as u64);
        // Do not precompute a statistic: the production writer must request it.
        assert!(matches!(
            field.statistics().get(Stat::UncompressedSizeInBytes),
            Precision::Absent
        ));
        expected_sizes
            .push((16 * batch.num_rows() + logical_bytes + batch.num_rows().div_ceil(8)) as u64);
        inputs.push(input);
    }
    assert_eq!(inputs.len(), 8);
    let runtime = CurrentThreadRuntime::new();
    let session = memory.session.clone().with_handle(runtime.handle());
    let mut output = Vec::new();
    let summary = session
        .write_options()
        .with_strategy(Arc::new(
            large_source_fast_load_table_strategy_with_dictionaries(
                4096,
                4 << 20,
                1,
                &VortexWriterStageTiming::default(),
                &session,
                false,
            ),
        ))
        .blocking(&runtime)
        .write(
            &mut output,
            ArrayIteratorAdapter::new(
                inputs[0].dtype().clone(),
                inputs.clone().into_iter().map(Ok),
            ),
        )
        .unwrap();
    assert_eq!(summary.row_count(), 8192);
    for (input, expected) in inputs.iter().zip(expected_sizes) {
        let field = input.as_::<Struct>().unmasked_field(0).clone();
        let Precision::Exact(actual) = field.statistics().get(Stat::UncompressedSizeInBytes) else {
            panic!("writer did not cache an exact referenced-byte cost");
        };
        assert_eq!(u64::try_from(&actual).unwrap(), expected);
        assert!(field.nbytes() > expected * 2);
    }
    let file = session.open_options().open_buffer(output).unwrap();
    let inventory = runtime
        .block_on(inspect_physical_encodings(
            &file,
            PhysicalEncodingInspectionLimits::default(),
        ))
        .unwrap();
    let data_rows = inventory["flat_references"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|leaf| {
            leaf["column_path"] == serde_json::json!(["text"])
                && leaf["auxiliary_path"] == serde_json::json!([])
        })
        .map(|leaf| leaf["rows"].as_u64().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(data_rows, [8192], "{inventory}");
    let readback = file
        .scan()
        .unwrap()
        .with_ordered(true)
        .into_array_iter(&runtime)
        .unwrap()
        .read_all()
        .unwrap();
    let mut ctx = session.create_execution_ctx();
    let table = readback.execute::<StructArray>(&mut ctx).unwrap();
    let text = table
        .unmasked_field(0)
        .clone()
        .execute::<VarBinViewArray>(&mut ctx)
        .unwrap();
    let validity = text
        .validity()
        .unwrap()
        .execute_mask(text.len(), &mut ctx)
        .unwrap();
    assert_eq!(text.len(), values.len());
    for (row, expected) in values.iter().enumerate() {
        assert_eq!(validity.value(row), expected.is_some());
        if let Some(expected) = expected {
            assert_eq!(text.bytes_at(row).as_slice(), expected.as_bytes());
        }
    }
    drop((
        validity, text, table, ctx, file, summary, inputs, session, memory,
    ));
    assert_eq!(pool.snapshot().reserved_bytes, 0);
}
