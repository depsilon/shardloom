// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use crate::DataType;
use std::fmt::Display;
use std::{collections::HashMap, fmt};

enum DisplayWork<'a> {
    Type(&'a DataType),
    NamedField(&'a crate::Field),
    ListTail(&'a crate::Field),
    Metadata(&'a HashMap<String, String>),
    Fields(&'a [crate::FieldRef], bool),
    Union(&'a crate::UnionFields, usize),
    Text(&'static str),
}

impl Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut current = DisplayWork::Type(self);
        let mut pending = Vec::new();
        loop {
            match current {
                DisplayWork::Type(dtype) => match dtype {
                    Self::Null => f.write_str("Null")?,
                    Self::Boolean => f.write_str("Boolean")?,
                    Self::Int8 => f.write_str("Int8")?,
                    Self::Int16 => f.write_str("Int16")?,
                    Self::Int32 => f.write_str("Int32")?,
                    Self::Int64 => f.write_str("Int64")?,
                    Self::UInt8 => f.write_str("UInt8")?,
                    Self::UInt16 => f.write_str("UInt16")?,
                    Self::UInt32 => f.write_str("UInt32")?,
                    Self::UInt64 => f.write_str("UInt64")?,
                    Self::Float16 => f.write_str("Float16")?,
                    Self::Float32 => f.write_str("Float32")?,
                    Self::Float64 => f.write_str("Float64")?,
                    Self::Timestamp(unit, Some(zone)) => write!(f, "Timestamp({unit}, {zone:?})")?,
                    Self::Timestamp(unit, None) => write!(f, "Timestamp({unit})")?,
                    Self::Date32 => f.write_str("Date32")?,
                    Self::Date64 => f.write_str("Date64")?,
                    Self::Time32(unit) => write!(f, "Time32({unit})")?,
                    Self::Time64(unit) => write!(f, "Time64({unit})")?,
                    Self::Duration(unit) => write!(f, "Duration({unit})")?,
                    Self::Interval(unit) => write!(f, "Interval({unit:?})")?,
                    Self::Binary => f.write_str("Binary")?,
                    Self::FixedSizeBinary(size) => write!(f, "FixedSizeBinary({size:?})")?,
                    Self::LargeBinary => f.write_str("LargeBinary")?,
                    Self::BinaryView => f.write_str("BinaryView")?,
                    Self::Utf8 => f.write_str("Utf8")?,
                    Self::LargeUtf8 => f.write_str("LargeUtf8")?,
                    Self::Utf8View => f.write_str("Utf8View")?,
                    Self::List(field)
                    | Self::ListView(field)
                    | Self::LargeList(field)
                    | Self::LargeListView(field)
                    | Self::FixedSizeList(field, _) => {
                        match dtype {
                            Self::List(_) => f.write_str("List(")?,
                            Self::ListView(_) => f.write_str("ListView(")?,
                            Self::LargeList(_) => f.write_str("LargeList(")?,
                            Self::LargeListView(_) => f.write_str("LargeListView(")?,
                            Self::FixedSizeList(_, size) => write!(f, "FixedSizeList({size} x ")?,
                            _ => unreachable!(),
                        }
                        if !field.is_nullable() {
                            f.write_str("non-null ")?;
                        }
                        pending.push(DisplayWork::Text(")"));
                        pending.push(DisplayWork::ListTail(field));
                        current = DisplayWork::Type(field.data_type());
                        continue;
                    }
                    Self::Struct(fields) => {
                        f.write_str("Struct(")?;
                        pending.push(DisplayWork::Text(")"));
                        current = DisplayWork::Fields(fields, true);
                        continue;
                    }
                    Self::Union(fields, mode) => {
                        write!(f, "Union({mode:?}")?;
                        pending.push(DisplayWork::Text(")"));
                        current = DisplayWork::Union(fields, 0);
                        continue;
                    }
                    Self::Dictionary(key, value) => {
                        f.write_str("Dictionary(")?;
                        pending.push(DisplayWork::Text(")"));
                        pending.push(DisplayWork::Type(value));
                        pending.push(DisplayWork::Text(", "));
                        current = DisplayWork::Type(key);
                        continue;
                    }
                    Self::Decimal32(precision, scale) => {
                        write!(f, "Decimal32({precision}, {scale})")?
                    }
                    Self::Decimal64(precision, scale) => {
                        write!(f, "Decimal64({precision}, {scale})")?
                    }
                    Self::Decimal128(precision, scale) => {
                        write!(f, "Decimal128({precision}, {scale})")?
                    }
                    Self::Decimal256(precision, scale) => {
                        write!(f, "Decimal256({precision}, {scale})")?
                    }
                    Self::Map(field, sorted) => {
                        f.write_str("Map(")?;
                        pending.push(DisplayWork::Text(if *sorted {
                            ", sorted)"
                        } else {
                            ", unsorted)"
                        }));
                        current = DisplayWork::NamedField(field);
                        continue;
                    }
                    Self::RunEndEncoded(ends, values) => {
                        f.write_str("RunEndEncoded(")?;
                        pending.push(DisplayWork::Text(")"));
                        pending.push(DisplayWork::NamedField(values));
                        pending.push(DisplayWork::Text(", "));
                        current = DisplayWork::NamedField(ends);
                        continue;
                    }
                },
                DisplayWork::NamedField(field) => {
                    write!(f, "{:?}: ", field.name())?;
                    if !field.is_nullable() {
                        f.write_str("non-null ")?;
                    }
                    pending.push(DisplayWork::Metadata(field.metadata()));
                    current = DisplayWork::Type(field.data_type());
                    continue;
                }
                DisplayWork::ListTail(field) => {
                    if field.name() != "item" {
                        write!(f, ", field: '{}'", field.name())?;
                    }
                    write!(f, "{}", FormatMetadata(field.metadata()))?;
                }
                DisplayWork::Metadata(metadata) => write!(f, "{}", FormatMetadata(metadata))?,
                DisplayWork::Fields(fields, first) => {
                    if let Some((field, tail)) = fields.split_first() {
                        if !first {
                            f.write_str(", ")?;
                        }
                        pending.push(DisplayWork::Fields(tail, false));
                        current = DisplayWork::NamedField(field);
                        continue;
                    }
                }
                DisplayWork::Union(fields, index) => {
                    if let Some((id, field)) = fields.get(index) {
                        write!(f, ", {id:?}: (")?;
                        pending.push(DisplayWork::Union(fields, index + 1));
                        pending.push(DisplayWork::Text(")"));
                        current = DisplayWork::NamedField(field);
                        continue;
                    }
                }
                DisplayWork::Text(text) => f.write_str(text)?,
            }
            match pending.pop() {
                Some(next) => current = next,
                None => return Ok(()),
            }
        }
    }
}

/// Adapter to format a metadata HashMap consistently.
struct FormatMetadata<'a>(&'a HashMap<String, String>);

impl fmt::Display for FormatMetadata<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let metadata = self.0;
        if metadata.is_empty() {
            Ok(())
        } else {
            let mut entries: Vec<(&String, &String)> = metadata.iter().collect();
            entries.sort_by(|a, b| a.0.cmp(b.0));
            write!(f, ", metadata: ")?;
            f.debug_map().entries(entries).finish()
        }
    }
}

#[cfg(test)]
mod tests {

    use std::sync::Arc;

    use crate::Field;

    use super::*;

    #[test]
    fn test_display_list() {
        let list_data_type = DataType::List(Arc::new(Field::new_list_field(DataType::Int32, true)));
        let list_data_type_string = list_data_type.to_string();
        let expected_string = "List(Int32)";
        assert_eq!(list_data_type_string, expected_string);
    }

    #[test]
    fn test_display_list_view() {
        let list_view_data_type =
            DataType::ListView(Arc::new(Field::new("item", DataType::Int32, true)));
        let list_view_data_type_string = list_view_data_type.to_string();
        let expected_string = "ListView(Int32)";
        assert_eq!(list_view_data_type_string, expected_string);
    }

    #[test]
    fn test_display_list_with_named_field() {
        let list_data_type = DataType::List(Arc::new(Field::new("foo", DataType::UInt64, false)));
        let list_data_type_string = list_data_type.to_string();
        let expected_string = "List(non-null UInt64, field: 'foo')";
        assert_eq!(list_data_type_string, expected_string);
    }

    #[test]
    fn test_display_list_view_with_named_field() {
        let list_view_data_type =
            DataType::ListView(Arc::new(Field::new("bar", DataType::UInt64, false)));
        let list_view_data_type_string = list_view_data_type.to_string();
        let expected_string = "ListView(non-null UInt64, field: 'bar')";
        assert_eq!(list_view_data_type_string, expected_string);
    }

    #[test]
    fn test_display_nested_list() {
        let nested_data_type = DataType::List(Arc::new(Field::new_list_field(
            DataType::List(Arc::new(Field::new_list_field(DataType::UInt64, false))),
            false,
        )));
        let nested_data_type_string = nested_data_type.to_string();
        let nested_expected_string = "List(non-null List(non-null UInt64))";
        assert_eq!(nested_data_type_string, nested_expected_string);
    }

    #[test]
    fn test_display_nested_list_view() {
        let nested_view_data_type = DataType::ListView(Arc::new(Field::new_list_field(
            DataType::ListView(Arc::new(Field::new_list_field(DataType::UInt64, false))),
            false,
        )));
        let nested_view_data_type_string = nested_view_data_type.to_string();
        let nested_view_expected_string = "ListView(non-null ListView(non-null UInt64))";
        assert_eq!(nested_view_data_type_string, nested_view_expected_string);
    }

    #[test]
    fn test_display_list_with_metadata() {
        let mut field = Field::new_list_field(DataType::Int32, true);
        let metadata = HashMap::from([("foo1".to_string(), "value1".to_string())]);
        field.set_metadata(metadata);
        let list_data_type = DataType::List(Arc::new(field));
        let list_data_type_string = list_data_type.to_string();
        let expected_string = "List(Int32, metadata: {\"foo1\": \"value1\"})";

        assert_eq!(list_data_type_string, expected_string);
    }

    #[test]
    fn test_display_list_view_with_metadata() {
        let mut field = Field::new_list_field(DataType::Int32, true);
        let metadata = HashMap::from([("foo2".to_string(), "value2".to_string())]);
        field.set_metadata(metadata);
        let list_view_data_type = DataType::ListView(Arc::new(field));
        let list_view_data_type_string = list_view_data_type.to_string();
        let expected_string = "ListView(Int32, metadata: {\"foo2\": \"value2\"})";
        assert_eq!(list_view_data_type_string, expected_string);
    }

    #[test]
    fn test_display_large_list() {
        let large_list_data_type =
            DataType::LargeList(Arc::new(Field::new_list_field(DataType::Int32, true)));
        let large_list_data_type_string = large_list_data_type.to_string();
        let expected_string = "LargeList(Int32)";
        assert_eq!(large_list_data_type_string, expected_string);

        // Test with named field
        let large_list_named =
            DataType::LargeList(Arc::new(Field::new("bar", DataType::UInt64, false)));
        let large_list_named_string = large_list_named.to_string();
        let expected_named_string = "LargeList(non-null UInt64, field: 'bar')";
        assert_eq!(large_list_named_string, expected_named_string);

        // Test with metadata
        let mut field = Field::new_list_field(DataType::Int32, true);
        let metadata = HashMap::from([("key1".to_string(), "value1".to_string())]);
        field.set_metadata(metadata);
        let large_list_metadata = DataType::LargeList(Arc::new(field));
        let large_list_metadata_string = large_list_metadata.to_string();
        let expected_metadata_string = "LargeList(Int32, metadata: {\"key1\": \"value1\"})";
        assert_eq!(large_list_metadata_string, expected_metadata_string);
    }

    #[test]
    fn test_display_large_list_view() {
        let large_list_view_data_type =
            DataType::LargeListView(Arc::new(Field::new("item", DataType::Int32, true)));
        let large_list_view_data_type_string = large_list_view_data_type.to_string();
        let expected_string = "LargeListView(Int32)";
        assert_eq!(large_list_view_data_type_string, expected_string);

        // Test with named field
        let large_list_view_named =
            DataType::LargeListView(Arc::new(Field::new("bar", DataType::UInt64, false)));
        let large_list_view_named_string = large_list_view_named.to_string();
        let expected_named_string = "LargeListView(non-null UInt64, field: 'bar')";
        assert_eq!(large_list_view_named_string, expected_named_string);

        // Test with metadata
        let mut field = Field::new_list_field(DataType::Int32, true);
        let metadata = HashMap::from([("key1".to_string(), "value1".to_string())]);
        field.set_metadata(metadata);
        let large_list_view_metadata = DataType::LargeListView(Arc::new(field));
        let large_list_view_metadata_string = large_list_view_metadata.to_string();
        let expected_metadata_string = "LargeListView(Int32, metadata: {\"key1\": \"value1\"})";
        assert_eq!(large_list_view_metadata_string, expected_metadata_string);
    }

    #[test]
    fn test_display_fixed_size_list() {
        let fixed_size_list =
            DataType::FixedSizeList(Arc::new(Field::new_list_field(DataType::Int32, true)), 5);
        let fixed_size_list_string = fixed_size_list.to_string();
        let expected_string = "FixedSizeList(5 x Int32)";
        assert_eq!(fixed_size_list_string, expected_string);

        // Test with named field
        let fixed_size_named =
            DataType::FixedSizeList(Arc::new(Field::new("baz", DataType::UInt64, false)), 3);
        let fixed_size_named_string = fixed_size_named.to_string();
        let expected_named_string = "FixedSizeList(3 x non-null UInt64, field: 'baz')";
        assert_eq!(fixed_size_named_string, expected_named_string);

        // Test with metadata
        let mut field = Field::new_list_field(DataType::Int32, true);
        let metadata = HashMap::from([("key2".to_string(), "value2".to_string())]);
        field.set_metadata(metadata);
        let fixed_size_metadata = DataType::FixedSizeList(Arc::new(field), 4);
        let fixed_size_metadata_string = fixed_size_metadata.to_string();
        let expected_metadata_string = "FixedSizeList(4 x Int32, metadata: {\"key2\": \"value2\"})";
        assert_eq!(fixed_size_metadata_string, expected_metadata_string);
    }

    #[test]
    fn test_display_struct() {
        let fields = vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, true),
        ];
        let struct_data_type = DataType::Struct(fields.into());
        let struct_data_type_string = struct_data_type.to_string();
        let expected_string = "Struct(\"a\": non-null Int32, \"b\": Utf8)";
        assert_eq!(struct_data_type_string, expected_string);

        // Test with metadata
        let mut field_with_metadata = Field::new("b", DataType::Utf8, true);
        let metadata = HashMap::from([
            ("key".to_string(), "value".to_string()),
            ("key2".to_string(), "value2".to_string()),
        ]);
        field_with_metadata.set_metadata(metadata);
        let struct_fields_with_metadata =
            vec![Field::new("a", DataType::Int32, false), field_with_metadata];
        let struct_data_type_with_metadata = DataType::Struct(struct_fields_with_metadata.into());
        let struct_data_type_with_metadata_string = struct_data_type_with_metadata.to_string();
        let expected_string_with_metadata = "Struct(\"a\": non-null Int32, \"b\": Utf8, metadata: {\"key\": \"value\", \"key2\": \"value2\"})";
        assert_eq!(
            struct_data_type_with_metadata_string,
            expected_string_with_metadata
        );
    }

    #[test]
    fn test_display_union() {
        let fields = vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, true),
        ];
        let type_ids = vec![0, 1];
        let union_fields = type_ids
            .into_iter()
            .zip(fields.into_iter().map(Arc::new))
            .collect();

        let union_data_type = DataType::Union(union_fields, crate::UnionMode::Sparse);
        let union_data_type_string = union_data_type.to_string();
        let expected_string = "Union(Sparse, 0: (\"a\": non-null Int32), 1: (\"b\": Utf8))";
        assert_eq!(union_data_type_string, expected_string);

        // Test with metadata
        let mut field_with_metadata = Field::new("b", DataType::Utf8, true);
        let metadata = HashMap::from([("key".to_string(), "value".to_string())]);
        field_with_metadata.set_metadata(metadata);
        let union_fields_with_metadata = vec![
            (0, Arc::new(Field::new("a", DataType::Int32, false))),
            (1, Arc::new(field_with_metadata)),
        ]
        .into_iter()
        .collect();
        let union_data_type_with_metadata =
            DataType::Union(union_fields_with_metadata, crate::UnionMode::Sparse);
        let union_data_type_with_metadata_string = union_data_type_with_metadata.to_string();
        let expected_string_with_metadata = "Union(Sparse, 0: (\"a\": non-null Int32), 1: (\"b\": Utf8, metadata: {\"key\": \"value\"}))";
        assert_eq!(
            union_data_type_with_metadata_string,
            expected_string_with_metadata
        );
    }

    #[test]
    fn test_display_map() {
        let entry_field = Field::new(
            "entries",
            DataType::Struct(
                vec![
                    Field::new("key", DataType::Utf8, false),
                    Field::new("value", DataType::Int32, true),
                ]
                .into(),
            ),
            false,
        );
        let map_data_type = DataType::Map(Arc::new(entry_field), true);
        let map_data_type_string = map_data_type.to_string();
        let expected_string =
            "Map(\"entries\": non-null Struct(\"key\": non-null Utf8, \"value\": Int32), sorted)";
        assert_eq!(map_data_type_string, expected_string);

        // Test with metadata
        let mut entry_field_with_metadata = Field::new(
            "entries",
            DataType::Struct(
                vec![
                    Field::new("key", DataType::Utf8, false),
                    Field::new("value", DataType::Int32, true),
                ]
                .into(),
            ),
            false,
        );
        let metadata = HashMap::from([("key".to_string(), "value".to_string())]);
        entry_field_with_metadata.set_metadata(metadata);
        let map_data_type_with_metadata = DataType::Map(Arc::new(entry_field_with_metadata), true);
        let map_data_type_with_metadata_string = map_data_type_with_metadata.to_string();
        let expected_string_with_metadata = "Map(\"entries\": non-null Struct(\"key\": non-null Utf8, \"value\": Int32), metadata: {\"key\": \"value\"}, sorted)";
        assert_eq!(
            map_data_type_with_metadata_string,
            expected_string_with_metadata
        );
    }

    #[test]
    fn test_display_run_end_encoded() {
        let run_ends_field = Arc::new(Field::new("run_ends", DataType::UInt32, false));
        let values_field = Arc::new(Field::new("values", DataType::Int32, true));
        let ree_data_type = DataType::RunEndEncoded(run_ends_field.clone(), values_field.clone());
        let ree_data_type_string = ree_data_type.to_string();
        let expected_string = "RunEndEncoded(\"run_ends\": non-null UInt32, \"values\": Int32)";
        assert_eq!(ree_data_type_string, expected_string);

        // Test with metadata
        let mut run_ends_field_with_metadata = Field::new("run_ends", DataType::UInt32, false);
        let metadata = HashMap::from([("key".to_string(), "value".to_string())]);
        run_ends_field_with_metadata.set_metadata(metadata);
        let ree_data_type_with_metadata =
            DataType::RunEndEncoded(Arc::new(run_ends_field_with_metadata), values_field.clone());
        let ree_data_type_with_metadata_string = ree_data_type_with_metadata.to_string();
        let expected_string_with_metadata = "RunEndEncoded(\"run_ends\": non-null UInt32, metadata: {\"key\": \"value\"}, \"values\": Int32)";
        assert_eq!(
            ree_data_type_with_metadata_string,
            expected_string_with_metadata
        );
    }

    #[test]
    fn test_display_dictionary() {
        let dict_data_type =
            DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8));
        let dict_data_type_string = dict_data_type.to_string();
        let expected_string = "Dictionary(Int8, Utf8)";
        assert_eq!(dict_data_type_string, expected_string);

        // Test with complex index and value types
        let complex_dict_data_type = DataType::Dictionary(
            Box::new(DataType::Int16),
            Box::new(DataType::Struct(
                vec![
                    Field::new("a", DataType::Int32, false),
                    Field::new("b", DataType::Utf8, true),
                ]
                .into(),
            )),
        );
        let complex_dict_data_type_string = complex_dict_data_type.to_string();
        let expected_complex_string =
            "Dictionary(Int16, Struct(\"a\": non-null Int32, \"b\": Utf8))";
        assert_eq!(complex_dict_data_type_string, expected_complex_string);
    }

    #[test]
    fn test_display_interval() {
        let interval_year_month = DataType::Interval(crate::IntervalUnit::YearMonth);
        let interval_year_month_string = interval_year_month.to_string();
        let expected_year_month_string = "Interval(YearMonth)";
        assert_eq!(interval_year_month_string, expected_year_month_string);

        let interval_day_time = DataType::Interval(crate::IntervalUnit::DayTime);
        let interval_day_time_string = interval_day_time.to_string();
        let expected_day_time_string = "Interval(DayTime)";
        assert_eq!(interval_day_time_string, expected_day_time_string);

        let interval_month_day_nano = DataType::Interval(crate::IntervalUnit::MonthDayNano);
        let interval_month_day_nano_string = interval_month_day_nano.to_string();
        let expected_month_day_nano_string = "Interval(MonthDayNano)";
        assert_eq!(
            interval_month_day_nano_string,
            expected_month_day_nano_string
        );
    }

    #[test]
    fn test_display_timestamp() {
        let timestamp_without_tz = DataType::Timestamp(crate::TimeUnit::Microsecond, None);
        let timestamp_without_tz_string = timestamp_without_tz.to_string();
        let expected_without_tz_string = "Timestamp(µs)";
        assert_eq!(timestamp_without_tz_string, expected_without_tz_string);

        let timestamp_with_tz =
            DataType::Timestamp(crate::TimeUnit::Nanosecond, Some(Arc::from("UTC")));
        let timestamp_with_tz_string = timestamp_with_tz.to_string();
        let expected_with_tz_string = "Timestamp(ns, \"UTC\")";
        assert_eq!(timestamp_with_tz_string, expected_with_tz_string);
    }
}
