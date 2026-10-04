//! Static nested admission at the existing Arrow-to-Vortex intake boundary.

use arrow_array::{Array, GenericListArray, OffsetSizeTrait, cast::AsArray as _};
use arrow_schema::{DataType, Field};
use shardloom_core::{Result, ShardLoomError};

pub(super) fn family(dtype: &DataType) -> Result<Option<&'static str>> {
    let family = match dtype {
        DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(..) => "list",
        DataType::Struct(_) => "struct",
        _ => return Ok(None),
    };
    crate::native_payload_schema::arrow_metadata_bytes(dtype)?;
    Ok(Some(family))
}

pub(super) fn validate_field(field: &Field) -> Result<()> {
    if family(field.data_type())?.is_some() && field.metadata().contains_key("ARROW:extension:name")
    {
        return Err(ShardLoomError::InvalidOperation(
            "native nested intake does not admit Arrow extension fields; no fallback execution was attempted".into(),
        ));
    }
    Ok(())
}

/// Preserve the existing finite-float intake policy throughout the retained
/// child domain. Null parents and children outside a sliced list's offsets are
/// not logical values. Schema validation bounds recursion before this traversal.
pub(super) fn validate_values(column: &str, array: &dyn Array) -> Result<()> {
    match array.data_type() {
        DataType::List(_) => validate_list_values(column, array.as_list::<i32>()),
        DataType::LargeList(_) => validate_list_values(column, array.as_list::<i64>()),
        DataType::FixedSizeList(..) => {
            let list = array.as_fixed_size_list();
            let width = usize::try_from(list.value_length()).map_err(|_| {
                ShardLoomError::InvalidOperation(format!(
                    "native nested intake column '{column}' has a negative fixed-size-list width; no fallback execution was attempted"
                ))
            })?;
            for_valid_parent_ranges(array, |start, end| {
                validate_child_range(column, list.values().as_ref(), start * width, end * width)
            })
        }
        DataType::Struct(_) => for_valid_parent_ranges(array, |start, end| {
            for child in array.as_struct().columns() {
                validate_child_range(column, child.as_ref(), start, end)?;
            }
            Ok(())
        }),
        _ => super::reject_columnar_non_finite_floats(column, array),
    }
}

fn validate_list_values<O: OffsetSizeTrait>(
    column: &str,
    list: &GenericListArray<O>,
) -> Result<()> {
    let offsets = list.value_offsets();
    for_valid_parent_ranges(list, |start, end| {
        validate_child_range(
            column,
            list.values().as_ref(),
            offsets[start].as_usize(),
            offsets[end].as_usize(),
        )
    })
}

fn for_valid_parent_ranges(
    array: &dyn Array,
    mut validate: impl FnMut(usize, usize) -> Result<()>,
) -> Result<()> {
    if array.is_empty() {
        return Ok(());
    }
    if let Some(nulls) = array.nulls().filter(|nulls| nulls.null_count() != 0) {
        for (start, end) in nulls.valid_slices() {
            validate(start, end)?;
        }
        Ok(())
    } else {
        validate(0, array.len())
    }
}

fn validate_child_range(column: &str, child: &dyn Array, start: usize, end: usize) -> Result<()> {
    if start == end {
        Ok(())
    } else if start == 0 && end == child.len() {
        validate_values(column, child)
    } else {
        validate_values(column, child.slice(start, end - start).as_ref())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Float64Array, ListArray, builder::Float64Builder, types::Float64Type};
    use std::sync::Arc;

    #[test]
    fn nested_intake_checks_the_shared_schema_budget_before_provider_conversion() {
        let mut dtype = DataType::Float64;
        for _ in 0..25 {
            dtype = DataType::List(Arc::new(Field::new("item", dtype, true)));
        }
        assert!(family(&dtype).unwrap_err().to_string().contains("depth 24"));
        for dtype in [
            DataType::Struct(Vec::<Field>::new().into()),
            DataType::List(Arc::new(Field::new("item", DataType::Float16, true))),
            DataType::Struct(
                vec![
                    Field::new("same", DataType::Boolean, true),
                    Field::new("same", DataType::Boolean, true),
                ]
                .into(),
            ),
        ] {
            assert!(family(&dtype).is_err());
        }
        let extension = Field::new("item", DataType::Utf8, true)
            .with_metadata([("ARROW:extension:name".into(), "custom".into())].into());
        assert!(family(&DataType::List(Arc::new(extension))).is_err());
    }

    #[test]
    fn nested_intake_keeps_finite_float_admission_for_all_children() {
        let input = ListArray::from_iter_primitive::<Float64Type, _, _>([
            Some(vec![Some(1.0), None]),
            None,
            Some(vec![Some(f64::INFINITY)]),
        ]);
        assert_eq!(family(input.data_type()).unwrap(), Some("list"));
        assert!(
            validate_values("items", &input)
                .unwrap_err()
                .to_string()
                .contains("non-finite")
        );
        let mut builder = arrow_array::builder::ListBuilder::new(Float64Builder::new());
        builder.values().append_value(-4.0);
        builder.append(true);
        builder.append(false);
        assert!(validate_values("items", &builder.finish()).is_ok());
        assert!(
            super::super::arrow_column_family("old_scalar", &Float64Array::from(vec![1.0])).is_ok()
        );
    }

    #[test]
    fn nested_intake_validates_only_reachable_child_ranges() {
        use arrow_array::{
            ArrayRef, StructArray,
            builder::{FixedSizeListBuilder, LargeListBuilder, ListBuilder},
        };

        let mut lists = ListBuilder::new(Float64Builder::new());
        let mut large = LargeListBuilder::new(Float64Builder::new());
        let mut fixed = FixedSizeListBuilder::new(Float64Builder::new(), 2);
        for (valid, values) in [
            (false, [f64::NAN, f64::INFINITY]),
            (true, [1.0, 2.0]),
            (false, [f64::NEG_INFINITY, f64::NAN]),
            (true, [-3.0, 4.0]),
        ] {
            lists.values().append_slice(&values);
            large.values().append_slice(&values);
            fixed.values().append_slice(&values);
            lists.append(valid);
            large.append(valid);
            fixed.append(valid);
        }
        let structure = StructArray::new(
            vec![Field::new("value", DataType::Float64, false)].into(),
            vec![Arc::new(Float64Array::from(vec![
                f64::NAN,
                1.0,
                f64::INFINITY,
                2.0,
            ]))],
            Some(vec![false, true, false, true].into()),
        );
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(lists.finish()),
            Arc::new(large.finish()),
            Arc::new(fixed.finish()),
            Arc::new(structure),
        ];
        for array in arrays {
            for (offset, len) in [(0, 4), (1, 2), (0, 1), (0, 0)] {
                let view = array.slice(offset, len);
                assert!(
                    validate_values("payload", view.as_ref()).is_ok(),
                    "{:?} {offset} {len}",
                    array.data_type()
                );
            }
            let visible = arrow_array::make_array(
                array.to_data().into_builder().nulls(None).build().unwrap(),
            );
            assert!(
                validate_values("payload", visible.as_ref())
                    .unwrap_err()
                    .to_string()
                    .contains("non-finite")
            );
        }

        // A sliced list retains children before and after its logical range.
        let values = ListArray::from_iter_primitive::<Float64Type, _, _>([
            Some(vec![Some(f64::NAN)]),
            Some(vec![Some(7.0)]),
            Some(vec![Some(f64::INFINITY)]),
        ]);
        assert!(validate_values("sliced", &values.slice(1, 1)).is_ok());
        assert!(validate_values("sliced", &values.slice(1, 0)).is_ok());
        assert!(validate_values("visible", &values).is_err());
    }
}
