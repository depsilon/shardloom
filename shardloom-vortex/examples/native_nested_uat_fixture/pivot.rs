//! Small Arrow IPC sources for the independent nested-pivot acceptance oracles.

use super::{Result, batch, write};
use arrow_array::{
    ArrayRef, Int64Array, RecordBatch, StringArray,
    builder::{
        FixedSizeListBuilder, Int16Builder, Int64Builder, ListBuilder, StringBuilder, StructBuilder,
    },
};
use arrow_schema::{DataType, Field, Fields};
use serde_json::Value;
use std::{io, path::Path, sync::Arc};

const CORE_ORACLES: &str =
    include_str!("../../../docs/architecture/fixtures/native-nested-pivot-state/core-oracles.json");
const TYPED_ORACLES: &str = include_str!(
    "../../../docs/architecture/fixtures/native-nested-pivot-state/typed-oracles.json"
);

type FixtureBuilder = fn(&[Value], i64) -> Result<RecordBatch>;

pub(super) fn write_all(root: &Path) -> Result<()> {
    let core: Value = serde_json::from_str(CORE_ORACLES)?;
    let typed: Value = serde_json::from_str(TYPED_ORACLES)?;

    write_section(
        root,
        &core,
        "list_index_and_cells",
        &["entity", "category", "amount"],
        None,
        "pivot-list_index_and_cells.data",
        list_index_and_cells,
    )?;
    write_section(
        root,
        &core,
        "list_domains",
        &["entity", "category", "amount"],
        None,
        "pivot-list_domains.data",
        list_domains,
    )?;
    write_section(
        root,
        &core,
        "nested_extrema_margins",
        &["entity", "category", "amount"],
        None,
        "pivot-nested_extrema_margins.data",
        nested_extrema_margins,
    )?;
    write_section(
        root,
        &typed,
        "struct_index_fixed_domains",
        &["entity", "category", "amount"],
        Some([
            ("entity", "struct<a:int16!,b:utf8!>?"),
            ("category", "fixed_size_list<int64!,2>!"),
            ("amount", "struct<tag:utf8!,numbers:list<int64?>!>?"),
        ]),
        "pivot-struct_index_fixed_domains.data",
        struct_index_fixed_domains,
    )?;
    write_section(
        root,
        &typed,
        "fixed_index_struct_domains",
        &["entity", "category", "amount"],
        Some([
            ("entity", "fixed_size_list<int16!,2>?"),
            ("category", "struct<a:int64!>?"),
            ("amount", "list<utf8?>?"),
        ]),
        "pivot-fixed_index_struct_domains.data",
        fixed_index_struct_domains,
    )
}

fn write_section(
    root: &Path,
    oracle: &Value,
    section_name: &str,
    expected_columns: &[&str],
    expected_schema: Option<[(&str, &str); 3]>,
    file_name: &str,
    builder: FixtureBuilder,
) -> Result<()> {
    let section = oracle
        .get(section_name)
        .ok_or_else(|| invalid(format!("oracle section {section_name} is absent")))?;
    if let Some(columns) = section.get("source_columns") {
        let actual_columns = columns
            .as_array()
            .ok_or_else(|| {
                invalid(format!(
                    "oracle section {section_name} source_columns is not an array"
                ))
            })?
            .iter()
            .map(|column| {
                column.as_str().ok_or_else(|| {
                    invalid(format!(
                        "oracle section {section_name} has a non-string source column"
                    ))
                })
            })
            .collect::<Result<Vec<_>>>()?;
        if actual_columns != expected_columns {
            return Err(invalid(format!(
                "oracle section {section_name} source_columns differ"
            )));
        }
    } else if expected_schema.is_none() {
        return Err(invalid(format!(
            "oracle section {section_name} has no source_columns"
        )));
    }
    if let Some(expected_schema) = expected_schema {
        let schema = section
            .get("source_schema")
            .and_then(Value::as_object)
            .ok_or_else(|| {
                invalid(format!(
                    "oracle section {section_name} has no source_schema"
                ))
            })?;
        if schema.len() != expected_schema.len()
            || expected_schema
                .iter()
                .any(|(name, dtype)| schema.get(*name).and_then(Value::as_str) != Some(*dtype))
        {
            return Err(invalid(format!(
                "oracle section {section_name} source_schema differs"
            )));
        }
    }
    let rows = section
        .get("source")
        .and_then(Value::as_array)
        .ok_or_else(|| invalid(format!("oracle section {section_name} has no source rows")))?;
    if rows.is_empty() {
        return Err(invalid(format!(
            "oracle section {section_name} must have source rows"
        )));
    }
    let split = rows.len() / 2;
    let first = builder(&rows[..split], 0)?;
    let second = builder(
        &rows[split..],
        i64::try_from(split).map_err(|_| invalid("pivot fixture row ordinal exceeds i64"))?,
    )?;
    write(root, file_name, &[&first, &second])
}

fn list_index_and_cells(rows: &[Value], start: i64) -> Result<RecordBatch> {
    let positions = positions(rows, start)?;
    let mut entities = list_i64_builder();
    let mut categories = Vec::with_capacity(rows.len());
    let mut amounts = list_i64_builder();
    for row in rows {
        let fields = source_row(row)?;
        append_i64_list(&mut entities, &fields[0])?;
        categories.push(string(&fields[1], "category")?.to_owned());
        append_i64_list(&mut amounts, &fields[2])?;
    }
    batch(vec![
        ("position", Arc::new(positions) as ArrayRef, false),
        ("entity", Arc::new(entities.finish()), true),
        ("category", Arc::new(StringArray::from(categories)), false),
        ("amount", Arc::new(amounts.finish()), true),
    ])
}

fn list_domains(rows: &[Value], start: i64) -> Result<RecordBatch> {
    let positions = positions(rows, start)?;
    let mut entities = Vec::with_capacity(rows.len());
    let mut categories = list_i64_builder();
    let mut amounts = Vec::with_capacity(rows.len());
    for row in rows {
        let fields = source_row(row)?;
        entities.push(string(&fields[0], "entity")?.to_owned());
        append_i64_list(&mut categories, &fields[1])?;
        amounts.push(i64_value(&fields[2], "amount")?);
    }
    batch(vec![
        ("position", Arc::new(positions) as ArrayRef, false),
        ("entity", Arc::new(StringArray::from(entities)), false),
        ("category", Arc::new(categories.finish()), true),
        ("amount", Arc::new(Int64Array::from(amounts)), false),
    ])
}

fn nested_extrema_margins(rows: &[Value], start: i64) -> Result<RecordBatch> {
    let positions = positions(rows, start)?;
    let mut entities = Vec::with_capacity(rows.len());
    let mut categories = Vec::with_capacity(rows.len());
    let mut amounts = list_i64_builder();
    for row in rows {
        let fields = source_row(row)?;
        entities.push(string(&fields[0], "entity")?.to_owned());
        categories.push(string(&fields[1], "category")?.to_owned());
        append_i64_list(&mut amounts, &fields[2])?;
    }
    batch(vec![
        ("position", Arc::new(positions) as ArrayRef, false),
        ("entity", Arc::new(StringArray::from(entities)), false),
        ("category", Arc::new(StringArray::from(categories)), false),
        ("amount", Arc::new(amounts.finish()), true),
    ])
}

fn struct_index_fixed_domains(rows: &[Value], start: i64) -> Result<RecordBatch> {
    let positions = positions(rows, start)?;
    let entity_fields = Fields::from(vec![
        Field::new("a", DataType::Int16, false),
        Field::new("b", DataType::Utf8, false),
    ]);
    let mut entities = StructBuilder::new(
        entity_fields,
        vec![
            Box::new(Int16Builder::new()),
            Box::new(StringBuilder::new()),
        ],
    );
    let mut categories = fixed_i64_builder();
    let amount_fields = Fields::from(vec![
        Field::new("tag", DataType::Utf8, false),
        Field::new("numbers", list_i64_type(), false),
    ]);
    let mut amounts = StructBuilder::new(
        amount_fields,
        vec![Box::new(StringBuilder::new()), Box::new(list_i64_builder())],
    );

    for row in rows {
        let fields = source_row(row)?;
        append_entity_struct(&mut entities, &fields[0])?;
        append_fixed_i64(&mut categories, &fields[1])?;
        append_amount_struct(&mut amounts, &fields[2])?;
    }
    batch(vec![
        ("position", Arc::new(positions) as ArrayRef, false),
        ("entity", Arc::new(entities.finish()), true),
        ("category", Arc::new(categories.finish()), false),
        ("amount", Arc::new(amounts.finish()), true),
    ])
}

fn fixed_index_struct_domains(rows: &[Value], start: i64) -> Result<RecordBatch> {
    let positions = positions(rows, start)?;
    let mut entities = fixed_i16_builder();
    let mut categories = StructBuilder::new(
        Fields::from(vec![Field::new("a", DataType::Int64, false)]),
        vec![Box::new(Int64Builder::new())],
    );
    let mut amounts = list_utf8_builder();
    for row in rows {
        let fields = source_row(row)?;
        append_fixed_i16(&mut entities, &fields[0])?;
        append_category_struct(&mut categories, &fields[1])?;
        append_utf8_list(&mut amounts, &fields[2])?;
    }
    batch(vec![
        ("position", Arc::new(positions) as ArrayRef, false),
        ("entity", Arc::new(entities.finish()), true),
        ("category", Arc::new(categories.finish()), true),
        ("amount", Arc::new(amounts.finish()), true),
    ])
}

fn positions(rows: &[Value], start: i64) -> Result<Int64Array> {
    let values = (0..rows.len())
        .map(|offset| {
            start
                .checked_add(
                    i64::try_from(offset).map_err(|_| invalid("pivot row ordinal exceeds i64"))?,
                )
                .ok_or_else(|| invalid("pivot row ordinal exceeds i64"))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Int64Array::from(values))
}

fn source_row(row: &Value) -> Result<&[Value]> {
    let fields = row
        .as_array()
        .ok_or_else(|| invalid("pivot source row is not an array"))?;
    if fields.len() != 3 {
        return Err(invalid(
            "pivot source row must contain exactly three fields",
        ));
    }
    Ok(fields)
}

fn list_i64_type() -> DataType {
    DataType::List(Arc::new(Field::new("item", DataType::Int64, true)))
}

fn list_i64_builder() -> ListBuilder<Int64Builder> {
    ListBuilder::new(Int64Builder::new()).with_field(Arc::new(Field::new(
        "item",
        DataType::Int64,
        true,
    )))
}

fn list_utf8_builder() -> ListBuilder<StringBuilder> {
    ListBuilder::new(StringBuilder::new()).with_field(Arc::new(Field::new(
        "item",
        DataType::Utf8,
        true,
    )))
}

fn fixed_i64_builder() -> FixedSizeListBuilder<Int64Builder> {
    FixedSizeListBuilder::new(Int64Builder::new(), 2).with_field(Arc::new(Field::new(
        "item",
        DataType::Int64,
        false,
    )))
}

fn fixed_i16_builder() -> FixedSizeListBuilder<Int16Builder> {
    FixedSizeListBuilder::new(Int16Builder::new(), 2).with_field(Arc::new(Field::new(
        "item",
        DataType::Int16,
        false,
    )))
}

fn append_i64_list(builder: &mut ListBuilder<Int64Builder>, value: &Value) -> Result<()> {
    if value.is_null() {
        builder.append(false);
        return Ok(());
    }
    for item in value
        .as_array()
        .ok_or_else(|| invalid("expected nullable int64 list"))?
    {
        builder
            .values()
            .append_option(optional_i64(item, "list item")?);
    }
    builder.append(true);
    Ok(())
}

fn append_utf8_list(builder: &mut ListBuilder<StringBuilder>, value: &Value) -> Result<()> {
    if value.is_null() {
        builder.append(false);
        return Ok(());
    }
    for item in value
        .as_array()
        .ok_or_else(|| invalid("expected nullable UTF-8 list"))?
    {
        if item.is_null() {
            builder.values().append_null();
        } else {
            builder
                .values()
                .append_value(string(item, "UTF-8 list item")?);
        }
    }
    builder.append(true);
    Ok(())
}

fn append_fixed_i64(builder: &mut FixedSizeListBuilder<Int64Builder>, value: &Value) -> Result<()> {
    let values = value
        .as_array()
        .ok_or_else(|| invalid("expected two-element fixed-size-list<int64>"))?;
    if values.len() != 2 {
        return Err(invalid(
            "fixed-size-list<int64> source value must have width two",
        ));
    }
    for item in values {
        builder
            .values()
            .append_value(i64_value(item, "fixed-size-list<int64> item")?);
    }
    builder.append(true);
    Ok(())
}

fn append_fixed_i16(builder: &mut FixedSizeListBuilder<Int16Builder>, value: &Value) -> Result<()> {
    let is_valid = !value.is_null();
    let values: &[Value] = if is_valid {
        value
            .as_array()
            .ok_or_else(|| invalid("expected nullable two-element fixed-size-list<int16>"))?
    } else {
        &[]
    };
    if is_valid && values.len() != 2 {
        return Err(invalid(
            "fixed-size-list<int16> source value must have width two",
        ));
    }
    if is_valid {
        for value in values {
            builder
                .values()
                .append_value(i16_value(value, "fixed-size-list<int16> item")?);
        }
    } else {
        builder.values().append_value(0);
        builder.values().append_value(0);
    }
    builder.append(is_valid);
    Ok(())
}

fn append_entity_struct(builder: &mut StructBuilder, value: &Value) -> Result<()> {
    let is_valid = !value.is_null();
    let (a, b) = if is_valid {
        let object = exact_object(value, &["a", "b"], "entity struct")?;
        (
            i16_value(
                object
                    .get("a")
                    .ok_or_else(|| invalid("entity struct is missing a"))?,
                "entity.a",
            )?,
            string(
                object
                    .get("b")
                    .ok_or_else(|| invalid("entity struct is missing b"))?,
                "entity.b",
            )?
            .to_owned(),
        )
    } else {
        (0, String::new())
    };
    builder
        .field_builder::<Int16Builder>(0)
        .ok_or_else(|| invalid("entity struct has no int16 field builder"))?
        .append_value(a);
    builder
        .field_builder::<StringBuilder>(1)
        .ok_or_else(|| invalid("entity struct has no UTF-8 field builder"))?
        .append_value(b);
    builder.append(is_valid);
    Ok(())
}

fn append_amount_struct(builder: &mut StructBuilder, value: &Value) -> Result<()> {
    let is_valid = !value.is_null();
    let tag = if is_valid {
        let object = exact_object(value, &["tag", "numbers"], "amount struct")?;
        string(
            object
                .get("tag")
                .ok_or_else(|| invalid("amount struct is missing tag"))?,
            "amount.tag",
        )?
        .to_owned()
    } else {
        String::new()
    };
    builder
        .field_builder::<StringBuilder>(0)
        .ok_or_else(|| invalid("amount struct has no tag builder"))?
        .append_value(tag);
    let numbers_builder = builder
        .field_builder::<ListBuilder<Int64Builder>>(1)
        .ok_or_else(|| invalid("amount struct has no numbers builder"))?;
    if is_valid {
        let object = exact_object(value, &["tag", "numbers"], "amount struct")?;
        let numbers = object
            .get("numbers")
            .ok_or_else(|| invalid("amount struct is missing numbers"))?;
        if numbers.is_null() {
            return Err(invalid("amount.numbers must be a non-null list"));
        }
        append_i64_list(numbers_builder, numbers)?;
    } else {
        numbers_builder.append(true);
    }
    builder.append(is_valid);
    Ok(())
}

fn append_category_struct(builder: &mut StructBuilder, value: &Value) -> Result<()> {
    let is_valid = !value.is_null();
    let a = if is_valid {
        let object = exact_object(value, &["a"], "category struct")?;
        i64_value(
            object
                .get("a")
                .ok_or_else(|| invalid("category struct is missing a"))?,
            "category.a",
        )?
    } else {
        0
    };
    builder
        .field_builder::<Int64Builder>(0)
        .ok_or_else(|| invalid("category struct has no int64 builder"))?
        .append_value(a);
    builder.append(is_valid);
    Ok(())
}

fn exact_object<'a>(
    value: &'a Value,
    names: &[&str],
    label: &str,
) -> Result<&'a serde_json::Map<String, Value>> {
    let object = value
        .as_object()
        .ok_or_else(|| invalid(format!("{label} source value is not an object")))?;
    if object.len() != names.len() || names.iter().any(|name| !object.contains_key(*name)) {
        return Err(invalid(format!("{label} source fields differ")));
    }
    Ok(object)
}

fn string<'a>(value: &'a Value, label: &str) -> Result<&'a str> {
    value
        .as_str()
        .ok_or_else(|| invalid(format!("{label} source value is not a string")))
}

fn i64_value(value: &Value, label: &str) -> Result<i64> {
    value
        .as_i64()
        .ok_or_else(|| invalid(format!("{label} source value is not an int64")))
}

fn i16_value(value: &Value, label: &str) -> Result<i16> {
    i16::try_from(i64_value(value, label)?)
        .map_err(|_| invalid(format!("{label} source value is outside int16")))
}

fn optional_i64(value: &Value, label: &str) -> Result<Option<i64>> {
    if value.is_null() {
        Ok(None)
    } else {
        i64_value(value, label).map(Some)
    }
}

fn invalid(message: impl Into<String>) -> Box<dyn std::error::Error> {
    Box::new(io::Error::new(io::ErrorKind::InvalidData, message.into()))
}
