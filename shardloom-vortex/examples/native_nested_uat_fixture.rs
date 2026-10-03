//! Typed input fixtures only; public UAT executes all queries through the CLI.

use arrow_array::{
    Array, ArrayRef, BooleanArray, Int64Array, RecordBatch, StringArray, StructArray, UInt64Array,
    builder::{Int64Builder, ListBuilder, StringBuilder, StructBuilder, UInt64Builder},
};
use arrow_ipc::writer::FileWriter;
use arrow_schema::{DataType, Field, Fields, Schema};
use std::{fs::OpenOptions, path::Path, sync::Arc};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

fn numbers(builder: &mut ListBuilder<Int64Builder>, values: Option<&[Option<i64>]>) {
    if let Some(values) = values {
        for value in values {
            builder.values().append_option(*value);
        }
    }
    builder.append(values.is_some());
}

fn record(builder: &mut StructBuilder, values: Option<&[Option<i64>]>, label: Option<&str>) {
    numbers(
        builder
            .field_builder::<ListBuilder<Int64Builder>>(0)
            .expect("list field"),
        values,
    );
    builder
        .field_builder::<StringBuilder>(1)
        .expect("text field")
        .append_option(label);
    builder.append(values.is_some());
}

fn small() -> Result<RecordBatch> {
    let mut items = ListBuilder::new(ListBuilder::new(Int64Builder::new()));
    numbers(items.values(), Some(&[Some(9), None]));
    numbers(items.values(), Some(&[]));
    numbers(items.values(), None);
    items.append(true);
    items.append(true);
    items.append(false);
    numbers(items.values(), Some(&[Some(-4), None]));
    numbers(items.values(), Some(&[]));
    items.append(true);

    let fields = Fields::from(vec![
        Field::new(
            "code",
            DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
            true,
        ),
        Field::new("label", DataType::Utf8, true),
    ]);
    let mut records = ListBuilder::new(StructBuilder::new(
        fields,
        vec![
            Box::new(ListBuilder::new(Int64Builder::new())),
            Box::new(StringBuilder::new()),
        ],
    ));
    record(records.values(), Some(&[Some(9), None]), Some("a'b"));
    record(records.values(), None, None);
    records.append(true);
    records.append(true);
    records.append(false);
    record(records.values(), Some(&[Some(-4)]), Some("東京"));
    records.append(true);

    let detail = StructArray::new(
        vec![
            Field::new("tag", DataType::Utf8, true),
            Field::new("enabled", DataType::Boolean, true),
        ]
        .into(),
        vec![
            Arc::new(StringArray::from(vec![Some("a'b"), Some(""), None, None])),
            Arc::new(BooleanArray::from(vec![
                Some(true),
                None,
                Some(false),
                None,
            ])),
        ],
        Some(vec![true, true, true, false].into()),
    );
    batch(vec![
        ("id", Arc::new(Int64Array::from(vec![1, 2, 3, 4])), false),
        ("items", Arc::new(items.finish()), true),
        ("records", Arc::new(records.finish()), true),
        ("detail", Arc::new(detail), true),
    ])
}

fn large() -> Result<RecordBatch> {
    let mut items = ListBuilder::new(ListBuilder::new(Int64Builder::new()));
    let mut ids = UInt64Builder::new();
    for row in 0..65_541i64 {
        ids.append_value(u64::try_from(row)?);
        numbers(items.values(), Some(&[Some(row)]));
        numbers(items.values(), Some(&[None]));
        items.append(true);
    }
    batch(vec![
        ("id", Arc::new(ids.finish()), false),
        ("items", Arc::new(items.finish()), false),
    ])
}

fn unsigned() -> Result<RecordBatch> {
    let mut items = ListBuilder::new(UInt64Builder::new());
    for value in [
        Some(0),
        Some((1 << 63) - 1),
        Some(1 << 63),
        Some(u64::MAX),
        None,
    ] {
        items.values().append_option(value);
    }
    items.append(true);
    batch(vec![
        ("id", Arc::new(UInt64Array::from(vec![1])), false),
        ("items", Arc::new(items.finish()), false),
    ])
}

fn batch(columns: Vec<(&str, ArrayRef, bool)>) -> Result<RecordBatch> {
    let schema = Schema::new(
        columns
            .iter()
            .map(|(name, column, nullable)| {
                Field::new(*name, column.data_type().clone(), *nullable)
            })
            .collect::<Vec<_>>(),
    );
    Ok(RecordBatch::try_new(
        Arc::new(schema),
        columns.into_iter().map(|(_, column, _)| column).collect(),
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
        return Err("usage: native_nested_uat_fixture <guarded-existing-output-directory>".into());
    }
    let root = Path::new(&arguments[0]);
    if !root.is_dir() {
        return Err("fixture directory must already exist under the UAT storage guard".into());
    }
    write(root, "nested.data", &small()?)?;
    write(root, "large.data", &large()?)?;
    write(root, "unsigned.data", &unsigned()?)?;
    println!("typed Arrow IPC fixtures: 4 nested rows, 65541 nested rows, 1 uint64 boundary row");
    Ok(())
}
