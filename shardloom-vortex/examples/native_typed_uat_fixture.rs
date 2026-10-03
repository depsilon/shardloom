//! Typed scalar payload fixtures only; public UAT executes queries through the CLI.

use arrow_array::{
    Array as _, ArrayRef, BinaryArray, Date32Array, Decimal128Array, Int64Array, RecordBatch,
    StructArray, TimestampMicrosecondArray,
    builder::{
        BinaryBuilder, Date32Builder, Decimal128Builder, Int64Builder, ListBuilder, StructBuilder,
        TimestampMicrosecondBuilder,
    },
};
use arrow_ipc::writer::FileWriter;
use arrow_schema::{DataType, Field, Fields, Schema, TimeUnit};
use std::{fs::OpenOptions, path::Path, sync::Arc};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

struct NullableColumns {
    payload: Arc<BinaryArray>,
    amount: Arc<Decimal128Array>,
    day: Arc<Date32Array>,
    instant: Arc<TimestampMicrosecondArray>,
}

fn nullable_columns() -> Result<NullableColumns> {
    let payload = Arc::new(BinaryArray::from(vec![
        Some(&[0x00, 0xff, 0x10][..]),
        Some(&[][..]),
        None,
        Some(&[0xc3, 0xa9][..]),
    ]));
    let amount = Arc::new(
        Decimal128Array::from(vec![
            Some(1_234_567_i128),
            Some(-99_999_999_999_999_999_999_999_999_999_999_999_999_i128),
            None,
            Some(0),
        ])
        .with_precision_and_scale(38, 6)?,
    );
    let day = Arc::new(Date32Array::from(vec![
        Some(-1),
        Some(20_000),
        None,
        Some(0),
    ]));
    let instant = Arc::new(TimestampMicrosecondArray::from(vec![
        Some(-1),
        Some(1_700_000_000_123_456),
        None,
        Some(0),
    ]));
    Ok(NullableColumns {
        payload,
        amount,
        day,
        instant,
    })
}

fn typed_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("payload", DataType::Binary, true),
        Field::new("amount", DataType::Decimal128(38, 6), true),
        Field::new("day", DataType::Date32, true),
        Field::new(
            "instant",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
    ]))
}

fn large_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("payload", DataType::Binary, false),
        Field::new("amount", DataType::Decimal128(38, 6), false),
        Field::new("day", DataType::Date32, false),
        Field::new(
            "instant",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
    ]))
}

fn typed() -> Result<RecordBatch> {
    let columns = nullable_columns()?;
    Ok(RecordBatch::try_new(
        typed_schema(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
            columns.payload,
            columns.amount,
            columns.day,
            columns.instant,
        ],
    )?)
}

fn append_nested_record(
    builder: &mut StructBuilder,
    columns: &NullableColumns,
    index: Option<usize>,
) {
    if let Some(index) = index {
        let payload = builder
            .field_builder::<BinaryBuilder>(0)
            .expect("binary field");
        if columns.payload.is_null(index) {
            payload.append_null();
        } else {
            payload.append_value(columns.payload.value(index));
        }

        let amount = builder
            .field_builder::<Decimal128Builder>(1)
            .expect("decimal field");
        if columns.amount.is_null(index) {
            amount.append_null();
        } else {
            amount.append_value(columns.amount.value(index));
        }

        let day = builder
            .field_builder::<Date32Builder>(2)
            .expect("date field");
        if columns.day.is_null(index) {
            day.append_null();
        } else {
            day.append_value(columns.day.value(index));
        }

        let instant = builder
            .field_builder::<TimestampMicrosecondBuilder>(3)
            .expect("timestamp field");
        if columns.instant.is_null(index) {
            instant.append_null();
        } else {
            instant.append_value(columns.instant.value(index));
        }
        builder.append(true);
    } else {
        builder
            .field_builder::<BinaryBuilder>(0)
            .expect("binary field")
            .append_null();
        builder
            .field_builder::<Decimal128Builder>(1)
            .expect("decimal field")
            .append_null();
        builder
            .field_builder::<Date32Builder>(2)
            .expect("date field")
            .append_null();
        builder
            .field_builder::<TimestampMicrosecondBuilder>(3)
            .expect("timestamp field")
            .append_null();
        builder.append(false);
    }
}

fn nested_fields() -> Fields {
    Fields::from(vec![
        Field::new("payload", DataType::Binary, true),
        Field::new("amount", DataType::Decimal128(38, 6), true),
        Field::new("day", DataType::Date32, true),
        Field::new(
            "instant",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
    ])
}

fn nested() -> Result<RecordBatch> {
    let columns = nullable_columns()?;
    let fields = nested_fields();
    let details = StructArray::new(
        fields.clone(),
        vec![
            Arc::clone(&columns.payload) as ArrayRef,
            Arc::clone(&columns.amount) as ArrayRef,
            Arc::clone(&columns.day) as ArrayRef,
            Arc::clone(&columns.instant) as ArrayRef,
        ],
        Some(vec![true, false, true, true].into()),
    );

    let record_builder = StructBuilder::new(
        fields.clone(),
        vec![
            Box::new(BinaryBuilder::new()),
            Box::new(Decimal128Builder::with_capacity(3).with_precision_and_scale(38, 6)?),
            Box::new(Date32Builder::new()),
            Box::new(TimestampMicrosecondBuilder::new()),
        ],
    );
    let mut records = ListBuilder::new(record_builder);
    append_nested_record(records.values(), &columns, Some(0));
    append_nested_record(records.values(), &columns, None);
    records.append(true);
    records.append(true);
    records.append(false);
    append_nested_record(records.values(), &columns, Some(3));
    records.append(true);

    let records = records.finish();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("details", DataType::Struct(fields.clone()), true),
        Field::new(
            "records",
            DataType::List(Arc::new(Field::new("item", DataType::Struct(fields), true))),
            true,
        ),
    ]));
    Ok(RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
            Arc::new(details),
            Arc::new(records),
        ],
    )?)
}

fn large() -> Result<RecordBatch> {
    const ROWS: usize = 65_541;
    let mut ids = Int64Builder::with_capacity(ROWS);
    let mut payloads = BinaryBuilder::new();
    let mut amounts = Decimal128Builder::with_capacity(ROWS).with_precision_and_scale(38, 6)?;
    let mut days = Date32Builder::with_capacity(ROWS);
    let mut instants = TimestampMicrosecondBuilder::with_capacity(ROWS);

    for row in 0..ROWS {
        let row = i64::try_from(row)?;
        let offset = row - 32_770;
        ids.append_value(row);
        payloads.append_value([u8::try_from(row % 251)?, 0, 255]);
        amounts.append_value(i128::from(offset) * 1_000_001);
        days.append_value(i32::try_from(offset)?);
        instants.append_value(offset * 1_000_001);
    }

    let schema = large_schema();
    Ok(RecordBatch::try_new(
        schema,
        vec![
            Arc::new(ids.finish()),
            Arc::new(payloads.finish()),
            Arc::new(amounts.finish()),
            Arc::new(days.finish()),
            Arc::new(instants.finish()),
        ],
    )?)
}

fn write(root: &Path, name: &str, batch: &RecordBatch) -> Result<()> {
    let output = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(root.join(name))?;
    let mut writer = FileWriter::try_new(output, batch.schema().as_ref())?;
    writer.write(batch)?;
    writer.finish()?;
    Ok(())
}

fn main() -> Result<()> {
    let arguments = std::env::args_os().skip(1).collect::<Vec<_>>();
    if arguments.len() != 1 {
        return Err("usage: native_typed_uat_fixture <guarded-existing-output-directory>".into());
    }
    let root = Path::new(&arguments[0]);
    if !root.is_dir() {
        return Err("fixture directory must already exist under the UAT storage guard".into());
    }

    let typed = typed()?;
    let typed_empty = RecordBatch::new_empty(typed.schema());
    let typed_nested = nested()?;
    let typed_large = large()?;
    write(root, "typed.data", &typed)?;
    write(root, "typed-empty.data", &typed_empty)?;
    write(root, "typed-nested.data", &typed_nested)?;
    write(root, "typed-large.data", &typed_large)?;
    println!(
        "typed Arrow IPC fixtures: {} typed rows, {} empty rows, {} nested rows, {} large rows",
        typed.num_rows(),
        typed_empty.num_rows(),
        typed_nested.num_rows(),
        typed_large.num_rows(),
    );
    Ok(())
}
