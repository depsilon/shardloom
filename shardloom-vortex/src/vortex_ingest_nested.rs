//! Static nested admission at the existing Arrow-to-Vortex intake boundary.

use arrow_array::{Array, cast::AsArray as _};
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
/// child domain. Schema validation bounds recursion before this traversal.
pub(super) fn validate_values(column: &str, array: &dyn Array) -> Result<()> {
    match array.data_type() {
        DataType::List(_) => validate_values(column, array.as_list::<i32>().values().as_ref()),
        DataType::LargeList(_) => validate_values(column, array.as_list::<i64>().values().as_ref()),
        DataType::FixedSizeList(..) => {
            validate_values(column, array.as_fixed_size_list().values().as_ref())
        }
        DataType::Struct(_) => {
            for child in array.as_struct().columns() {
                validate_values(column, child.as_ref())?;
            }
            Ok(())
        }
        _ => super::reject_columnar_non_finite_floats(column, array),
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
}
