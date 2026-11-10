//! Static nested admission at the existing Arrow-to-Vortex intake boundary.

use arrow_array::{Array, GenericListArray, OffsetSizeTrait, cast::AsArray as _};
use arrow_schema::{DataType, Field};
use shardloom_core::{FloatPolicy, Result, ShardLoomError};
use shardloom_exec::live_memory::LiveMemoryPool;

/// Arrow 59 validates a nonnullable fixed-list child by expanding the parent's
/// validity bitmap. Its recursive validation releases each bitmap before the
/// next child, so reserve the largest aligned scratch buffer, not their sum.
pub(crate) fn validation_workspace_bytes(data: &arrow_data::ArrayData) -> Result<u64> {
    let own = match (data.data_type(), data.nulls()) {
        (DataType::FixedSizeList(field, width), Some(nulls)) if !field.is_nullable() => {
            u64::try_from(*width)
                .ok()
                .and_then(|width| (nulls.len() as u64).checked_mul(width))
                .and_then(|bits| bits.checked_add(511))
                .map(|bits| (bits / 512) * 64)
                .and_then(|bytes| bytes.checked_add(256))
                .ok_or_else(|| ShardLoomError::InvalidOperation(
                    "native Arrow validity workspace overflow; no fallback execution was attempted".into()
                ))?
        }
        _ => 0,
    };
    data.child_data().iter().try_fold(own, |maximum, child| {
        validation_workspace_bytes(child).map(|bytes| maximum.max(bytes))
    })
}

/// Arrow permits null storage below null Struct/FixedSizeList parents even when
/// their child field is nonnullable. Vortex imports each child independently.
/// After original Arrow validation, discard only these unreachable child masks;
/// keep parent validity, logical nullability and all value buffers unchanged.
/// Call after buffer ownership admission: the existing schema grant covers the
/// reconstructed descriptors and every shared buffer retains its credited owner.
pub(crate) fn provider_batch(
    batch: arrow_array::RecordBatch,
    memory: Option<&LiveMemoryPool>,
    check: &dyn Fn() -> Result<()>,
) -> Result<arrow_array::RecordBatch> {
    fn required(dtype: &DataType, index: usize) -> bool {
        match dtype {
            DataType::Struct(fields) => !fields[index].is_nullable(),
            DataType::FixedSizeList(field, _) => !field.is_nullable(),
            _ => false,
        }
    }
    fn needs_normalization(data: &arrow_data::ArrayData) -> bool {
        data.child_data().iter().enumerate().any(|(index, child)| {
            (required(data.data_type(), index) && child.null_count() != 0)
                || needs_normalization(child)
        })
    }
    fn normalize(data: arrow_data::ArrayData) -> Result<arrow_data::ArrayData> {
        let (dtype, length, nulls, offset, buffers, children) = data.into_parts();
        let children = children
            .into_iter()
            .enumerate()
            .map(|(index, child)| {
                let child = normalize(child)?;
                if required(&dtype, index) && child.null_count() != 0 {
                    child
                        .into_builder()
                        .nulls(None)
                        .build()
                        .map_err(super::vortex_error)
                } else {
                    Ok(child)
                }
            })
            .collect::<Result<Vec<_>>>()?;
        arrow_data::ArrayData::builder(dtype)
            .len(length)
            .offset(offset)
            .buffers(buffers)
            .nulls(nulls)
            .child_data(children)
            .build()
            .map_err(super::vortex_error)
    }
    crate::native_cross_field_arrow::validate_batch(&batch, memory, check)?;
    let (schema, columns, rows) = batch.into_parts();
    let columns = columns
        .into_iter()
        .map(|array| {
            if !matches!(
                array.data_type(),
                DataType::Struct(_)
                    | DataType::List(_)
                    | DataType::LargeList(_)
                    | DataType::FixedSizeList(..)
            ) {
                return Ok(array);
            }
            let data = array.to_data();
            if !needs_normalization(&data) {
                return Ok(array);
            }
            let _validation = memory
                .map(|memory| memory.reserve(validation_workspace_bytes(&data)?))
                .transpose()?;
            data.validate_full().map_err(super::vortex_error)?;
            normalize(data).map(arrow_array::make_array)
        })
        .collect::<Result<Vec<_>>>()?;
    arrow_array::RecordBatch::try_new_with_options(
        schema,
        columns,
        &arrow_array::RecordBatchOptions::new().with_row_count(Some(rows)),
    )
    .map_err(super::vortex_error)
}

pub(super) fn family(dtype: &DataType) -> Result<Option<&'static str>> {
    let family = match dtype {
        DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(..) => "list",
        DataType::Struct(_) => "struct",
        _ => return Ok(None),
    };
    crate::native_payload_schema::validate_arrow_declaration(dtype)?;
    Ok(Some(family))
}

pub(super) fn validate_field(field: &Field) -> Result<()> {
    if crate::native_variant::arrow::is_variant(field) {
        if !crate::native_variant::arrow::admitted(field) {
            return Err(crate::native_variant::failed(
                "invalid Arrow Variant declaration",
            ));
        }
        return crate::native_payload_schema::validate_arrow_declared_storage(field);
    }
    if crate::native_uuid::arrow::is_uuid(field) {
        return crate::native_payload_schema::validate_arrow_declared_storage(field);
    }
    if family(field.data_type())?.is_some()
        && !crate::native_payload_schema::arrow_field_metadata_admitted(field)
    {
        return Err(ShardLoomError::InvalidOperation(
            "native nested intake does not admit Arrow extension fields; no fallback execution was attempted".into(),
        ));
    }
    Ok(())
}

/// Preserve finite floats and declared decimal precision throughout the retained
/// child domain. Null parents and children outside a sliced list's offsets are
/// not logical values. Schema validation bounds recursion before this traversal.
#[cfg(test)]
pub(crate) fn validate_values(column: &str, array: &dyn Array) -> Result<()> {
    validate_values_with_float_policy(column, array, FloatPolicy::Strict)
}

/// The explicit floating domain changes only nonfinite admission. Decimal,
/// nested reachability and all structural validations remain identical.
pub(crate) fn validate_values_with_float_policy(
    column: &str,
    array: &dyn Array,
    float_policy: FloatPolicy,
) -> Result<()> {
    macro_rules! decimal {
        ($kind:ty, $precision:expr) => {
            array.as_primitive::<$kind>().validate_decimal_precision(*$precision)
                .map_err(|error| ShardLoomError::InvalidOperation(format!(
                    "native columnar intake column '{column}' exceeds its declared decimal precision: {error}; no fallback execution was attempted"
                )))
        };
    }
    match array.data_type() {
        DataType::Decimal32(precision, _) => decimal!(arrow_array::types::Decimal32Type, precision),
        DataType::Decimal64(precision, _) => decimal!(arrow_array::types::Decimal64Type, precision),
        DataType::Decimal128(precision, _) => {
            decimal!(arrow_array::types::Decimal128Type, precision)
        }
        DataType::Decimal256(precision, _) => {
            decimal!(arrow_array::types::Decimal256Type, precision)
        }
        DataType::List(field) => validate_list_values(
            column,
            array.as_list::<i32>(),
            field_float_policy(field, float_policy),
        ),
        DataType::LargeList(field) => validate_list_values(
            column,
            array.as_list::<i64>(),
            field_float_policy(field, float_policy),
        ),
        DataType::FixedSizeList(field, _) => {
            let list = array.as_fixed_size_list();
            let width = usize::try_from(list.value_length()).map_err(|_| {
                ShardLoomError::InvalidOperation(format!(
                    "native nested intake column '{column}' has a negative fixed-size-list width; no fallback execution was attempted"
                ))
            })?;
            for_valid_parent_ranges(array, |start, end| {
                validate_child_range(
                    column,
                    list.values().as_ref(),
                    start * width,
                    end * width,
                    field_float_policy(field, float_policy),
                )
            })
        }
        DataType::Struct(fields) => for_valid_parent_ranges(array, |start, end| {
            for (field, child) in fields.iter().zip(array.as_struct().columns()) {
                validate_child_range(
                    column,
                    child.as_ref(),
                    start,
                    end,
                    field_float_policy(field, float_policy),
                )?;
            }
            Ok(())
        }),
        _ if float_policy == FloatPolicy::Strict => {
            super::reject_columnar_non_finite_floats(column, array)
        }
        _ => Ok(()),
    }
}

/// Variant carriage does not observe typed floating components numerically.
/// Cross-field validation still checks its encoded/shredded values.
pub(crate) fn field_float_policy(field: &Field, policy: FloatPolicy) -> FloatPolicy {
    if crate::native_variant::arrow::is_variant(field) {
        FloatPolicy::Extended
    } else {
        policy
    }
}

/// Replace the provider's flat decimal scan at legacy/streaming conversion with
/// the same ancestor-aware check used by resident intake. This does not admit
/// additional types or change the caller's floating-point policy.
pub(crate) fn validate_provider_decimal_values(batch: &arrow_array::RecordBatch) -> Result<()> {
    fn contains_decimal(dtype: &DataType) -> bool {
        match dtype {
            DataType::Decimal32(..)
            | DataType::Decimal64(..)
            | DataType::Decimal128(..)
            | DataType::Decimal256(..) => true,
            DataType::Struct(fields) => fields
                .iter()
                .any(|field| contains_decimal(field.data_type())),
            DataType::List(field)
            | DataType::LargeList(field)
            | DataType::FixedSizeList(field, _) => contains_decimal(field.data_type()),
            _ => false,
        }
    }
    for (field, array) in batch.schema().fields().iter().zip(batch.columns()) {
        if contains_decimal(field.data_type()) {
            validate_values_with_float_policy(field.name(), array.as_ref(), FloatPolicy::Extended)?;
        }
    }
    Ok(())
}

fn validate_list_values<O: OffsetSizeTrait>(
    column: &str,
    list: &GenericListArray<O>,
    float_policy: FloatPolicy,
) -> Result<()> {
    let offsets = list.value_offsets();
    for_valid_parent_ranges(list, |start, end| {
        validate_child_range(
            column,
            list.values().as_ref(),
            offsets[start].as_usize(),
            offsets[end].as_usize(),
            float_policy,
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

fn validate_child_range(
    column: &str,
    child: &dyn Array,
    start: usize,
    end: usize,
    float_policy: FloatPolicy,
) -> Result<()> {
    if start == end {
        Ok(())
    } else if start == 0 && end == child.len() {
        validate_values_with_float_policy(column, child, float_policy)
    } else {
        validate_values_with_float_policy(
            column,
            child.slice(start, end - start).as_ref(),
            float_policy,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Float64Array, ListArray, builder::Float64Builder, types::Float64Type};
    use std::sync::Arc;

    #[test]
    #[cfg(feature = "vortex-local-primitives")]
    fn provider_decimal_validation_covers_reachable_nested_and_sliced_storage() {
        use crate::resident_memory_source::ResidentMemorySource;
        use crate::resident_session::ResidentVortexSession;
        use arrow_array::{
            ArrayRef, Decimal128Array, FixedSizeListArray, LargeListArray, ListArray, RecordBatch,
            StructArray,
        };
        use arrow_schema::Schema;
        use shardloom_exec::compute_pool::CancellationToken;

        let decimal = DataType::Decimal128(2, 0);
        let values: ArrayRef = Arc::new(
            Decimal128Array::from(vec![123, 9, -12, 999])
                .with_precision_and_scale(2, 0)
                .unwrap(),
        );
        let item = Arc::new(Field::new("amount", decimal.clone(), false));
        let nulls = Some(vec![false, true, true, false].into());
        let inputs: Vec<ArrayRef> = vec![
            Arc::new(StructArray::new(
                vec![item.clone()].into(),
                vec![values.clone()],
                nulls.clone(),
            )),
            Arc::new(ListArray::new(
                item.clone(),
                arrow_buffer::OffsetBuffer::new(vec![0i32, 1, 2, 3, 4].into()),
                values.clone(),
                nulls.clone(),
            )),
            Arc::new(LargeListArray::new(
                item.clone(),
                arrow_buffer::OffsetBuffer::new(vec![0i64, 1, 2, 3, 4].into()),
                values.clone(),
                nulls.clone(),
            )),
            Arc::new(FixedSizeListArray::new(item, 1, values, nulls.clone())),
            Arc::new(
                Decimal128Array::new(vec![123, 9, -12, 999].into(), nulls)
                    .with_precision_and_scale(2, 0)
                    .unwrap(),
            ),
        ];
        let batch = |array: ArrayRef| {
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new(
                    "value",
                    array.data_type().clone(),
                    true,
                )])),
                vec![array],
            )
            .unwrap()
        };
        for input in inputs {
            for (offset, len) in [(0, 4), (1, 2), (0, 1), (0, 0), (4, 0)] {
                let batch = batch(input.slice(offset, len));
                let converted =
                    super::super::arrow_record_batch_to_vortex_array(batch.clone()).unwrap();
                assert_eq!(converted.len(), len);
                let session = ResidentVortexSession::new(1 << 20, 1).unwrap();
                let source = ResidentMemorySource::from_arrow_record_batch(
                    &session,
                    batch,
                    &CancellationToken::default(),
                )
                .unwrap();
                assert_eq!(source.row_count(), len);
                drop(source);
                assert_eq!(session.snapshot().memory.reserved_bytes, 0);
            }
            let visible = arrow_array::make_array(
                input.to_data().into_builder().nulls(None).build().unwrap(),
            );
            let batch = batch(visible);
            let error =
                super::super::arrow_record_batch_to_vortex_array(batch.clone()).unwrap_err();
            assert!(
                error.to_string().contains("declared decimal precision"),
                "{error}"
            );
            let session = ResidentVortexSession::new(1 << 20, 1).unwrap();
            assert!(
                ResidentMemorySource::from_arrow_record_batch(
                    &session,
                    batch,
                    &CancellationToken::default()
                )
                .is_err()
            );
            assert_eq!(session.snapshot().memory.reserved_bytes, 0);
        }
    }

    #[test]
    fn dense_embedding_provider_mask_bridge_preserves_parent_and_value_buffers() {
        use arrow_array::{FixedSizeListArray, Float32Array, RecordBatch};
        use arrow_schema::Schema;
        let values = Arc::new(Float32Array::from(vec![
            Some(-0.0),
            Some(f32::from_bits(0xff80_0042)),
            None,
            None,
        ]));
        let pointer = values.values().as_ptr();
        let vector = Arc::new(FixedSizeListArray::new(
            Arc::new(Field::new("item", DataType::Float32, false)),
            2,
            values.clone(),
            Some(arrow_buffer::NullBuffer::from(vec![true, false])),
        ));
        let schema = Arc::new(Schema::new(vec![Field::new(
            "v",
            vector.data_type().clone(),
            true,
        )]));
        let batch = RecordBatch::try_new(schema.clone(), vec![vector]).unwrap();
        let denied = LiveMemoryPool::new(256).unwrap();
        assert!(provider_batch(batch.clone(), Some(&denied), &|| Ok(())).is_err());
        assert_eq!(denied.snapshot().reserved_bytes, 0);
        let memory = LiveMemoryPool::new(64 << 10).unwrap();
        let normalized = provider_batch(batch, Some(&memory), &|| Ok(())).unwrap();
        assert_eq!(memory.snapshot().reserved_bytes, 0);
        assert_eq!(memory.snapshot().peak_reserved_bytes, 320);
        assert_eq!(normalized.schema(), schema);
        let vector = normalized.column(0).as_fixed_size_list();
        assert!(vector.is_valid(0) && vector.is_null(1));
        let child = vector
            .values()
            .as_primitive::<arrow_array::types::Float32Type>();
        assert_eq!(child.null_count(), 0);
        assert_eq!(child.values().as_ptr(), pointer);
        assert_eq!(child.value(0).to_bits(), (-0.0_f32).to_bits());
        assert_eq!(child.value(1).to_bits(), 0xff80_0042);
        // The producer's validity is immutable; only the private descriptor
        // changed, and nullable component fields must never lose their masks.
        assert_eq!(values.null_count(), 2);
        let nullable = Arc::new(FixedSizeListArray::new(
            Arc::new(Field::new("item", DataType::Float32, true)),
            2,
            values,
            None,
        ));
        let nullable =
            RecordBatch::try_from_iter(vec![("v", nullable as arrow_array::ArrayRef)]).unwrap();
        let normalized = provider_batch(nullable, Some(&memory), &|| Ok(())).unwrap();
        assert_eq!(
            normalized
                .column(0)
                .as_fixed_size_list()
                .values()
                .null_count(),
            2
        );
    }

    #[test]
    fn nested_intake_checks_the_shared_schema_budget_before_provider_conversion() {
        let mut dtype = DataType::Float64;
        for _ in 0..25 {
            dtype = DataType::List(Arc::new(Field::new("item", dtype, true)));
        }
        assert!(family(&dtype).unwrap_err().to_string().contains("depth 24"));
        for dtype in [
            DataType::Struct(Vec::<Field>::new().into()),
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Decimal256(39, 0),
                true,
            ))),
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
