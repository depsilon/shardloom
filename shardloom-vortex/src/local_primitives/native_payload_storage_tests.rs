use super::*;
use crate::{
    local_primitives::{
        logical_field_from_native_array as field,
        native_relational_batch::index_array,
        native_relational_records as records,
        native_relational_spill::{Ordering, State},
    },
    relational_query::VortexRelationalSpillPolicy,
    resident_session::ResidentVortexSession,
};
use shardloom_exec::compute_pool::CancellationToken;
use vortex::{
    array::{
        arrays::{
            DecimalArray, DictArray, MaskedArray, PrimitiveArray, decimal::DecimalArrayExt as _,
            list::ListArraySlotsExt as _,
        },
        dtype::{DecimalDType, FieldNames, i256},
        scalar::DecimalValue,
    },
    buffer::Buffer,
};

const F32_BITS: [u32; 8] = [
    0x7f80_0001,
    0xffc0_0137,
    0x7f80_0000,
    0xff80_0000,
    0x8000_0000,
    0,
    0x3f80_0000,
    0xbf80_0000,
];
const F64_BITS: [u64; 8] = [
    0x7ff0_0000_0000_0001,
    0xfff8_0000_0000_0137,
    0x7ff0_0000_0000_0000,
    0xfff0_0000_0000_0000,
    0x8000_0000_0000_0000,
    0,
    0x3ff0_0000_0000_0000,
    0xbff0_0000_0000_0000,
];

#[test]
fn native_payload_value_admission_keeps_many_retained_structs_within_the_shared_grant() {
    let session = ResidentVortexSession::new(16 << 20, 1).unwrap();
    let retained_child = session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let source = StructArray::try_new(
                FieldNames::from(["value"]),
                vec![PrimitiveArray::from_iter(0..2048_i64).into_array()],
                2048,
                Validity::NonNullable,
            )
            .map_err(vortex_error)?
            .into_array();
            let mut retained = ReservedVec::new(context.memory())?;
            let mut execution = context.native_session().create_execution_ctx();
            for row in 0..source.len() {
                let indices = index_array(1, false, context, |_| Ok(Some(row)))?;
                let value = take(&source, &indices, source.dtype(), context)?;
                let child = field(&value, "value")?
                    .execute::<PrimitiveArray>(&mut execution)
                    .map_err(vortex_error)?;
                assert_eq!(child.to_buffer::<i64>()[0], i64::try_from(row).unwrap());
                retained.push(value)?;
            }
            assert!(context.memory().snapshot().reserved_bytes < 8 << 20);
            let child = field(&retained.values[2047], "value")?;
            drop(retained);
            assert!(context.memory().snapshot().reserved_bytes >= metadata_bytes(source.dtype())?);
            let indices = index_array(1, false, context, |_| Ok(Some(0)))?;
            let before = context.memory().snapshot().reserved_bytes;
            let hold = context.memory().reserve((16 << 20) - before - 1)?;
            assert!(take(&source, &indices, source.dtype(), context).is_err());
            drop(hold);
            assert_eq!(context.memory().snapshot().reserved_bytes, before);
            Ok(child)
        })
        .unwrap();
    assert!(session.memory().snapshot().reserved_bytes > 0);
    drop(retained_child);
    assert_eq!(session.memory().snapshot().reserved_bytes, 0);
}

#[test]
fn native_payload_record_admission_preserves_wide_values_empty_children_and_nested_bounds() {
    let fields = (0..1025)
        .map(|index| format!("field{index}"))
        .collect::<Vec<_>>();
    let source = StructArray::try_new(
        FieldNames::from(fields.iter().map(String::as_str).collect::<Vec<_>>()),
        (0..1025_i64)
            .map(|value| PrimitiveArray::from_iter([value]).into_array())
            .collect::<Vec<_>>(),
        1,
        Validity::NonNullable,
    )
    .unwrap()
    .into_array();
    let session = ResidentVortexSession::new(16 << 20, 1).unwrap();
    let children =
        session.with_native_execution_context(&CancellationToken::default(), |context| {
            let indices = index_array(1, false, context, |_| Ok(Some(0)))?;
            let before = context.memory().snapshot().reserved_bytes;
            assert!(take(&source, &indices, source.dtype(), context).is_err());
            assert!(defaults(source.dtype(), 0, context).is_err());
            assert_eq!(context.memory().snapshot().reserved_bytes, before);
            let record = take_record(&source, &indices, source.dtype(), context)?;
            assert_eq!(record.dtype(), source.dtype());
            let mut execution = context.native_session().create_execution_ctx();
            for (index, name) in fields.iter().enumerate() {
                let child = field(&record, name)?
                    .execute::<PrimitiveArray>(&mut execution)
                    .map_err(vortex_error)?;
                assert_eq!(child.to_buffer::<i64>()[0], i64::try_from(index).unwrap());
            }
            let child = field(&record, "field1024")?;
            drop(record);
            let metadata = crate::native_payload_schema::schema_bytes(source.dtype())?;
            assert!(context.memory().snapshot().reserved_bytes >= metadata);
            let empty = empty_record(source.dtype(), context)?;
            assert!(empty.is_empty());
            assert_eq!(empty.dtype(), source.dtype());
            let empty_child = field(&empty, "field1024")?;
            drop(empty);
            assert!(context.memory().snapshot().reserved_bytes >= metadata * 2);
            drop((child, empty_child));
            assert_eq!(context.memory().snapshot().reserved_bytes, before);
            let hold = context.memory().reserve((16 << 20) - before - 1)?;
            assert!(take_record(&source, &indices, source.dtype(), context).is_err());
            assert!(empty_record(source.dtype(), context).is_err());
            drop(hold);
            assert_eq!(context.memory().snapshot().reserved_bytes, before);
            Ok(())
        });
    children.unwrap();
    assert_eq!(session.memory().snapshot().reserved_bytes, 0);
}

fn measures(rows: usize) -> ArrayRef {
    let decimal = DecimalDType::new(2, 0);
    StructArray::try_new(
        FieldNames::from([records::ORDINAL, "f32", "f64", "decimal"]),
        vec![
            PrimitiveArray::from_iter((0..rows as u64).rev()).into_array(),
            PrimitiveArray::from_iter((0..rows).map(|i| f32::from_bits(F32_BITS[i % 8])))
                .into_array(),
            PrimitiveArray::from_iter((0..rows).map(|i| f64::from_bits(F64_BITS[i % 8])))
                .into_array(),
            DecimalArray::try_new(
                Buffer::from(
                    (0..rows)
                        .map(|i| [123_i128, -999, 7][i % 3])
                        .collect::<Vec<_>>(),
                ),
                decimal,
                Validity::NonNullable,
            )
            .unwrap()
            .into_array(),
        ],
        rows,
        Validity::NonNullable,
    )
    .unwrap()
    .into_array()
}

fn verify(
    source: &ArrayRef,
    originals: impl IntoIterator<Item = usize>,
    context: &NativeExecutionContext<'_>,
) -> Result<()> {
    let mut execution = context.native_session().create_execution_ctx();
    let f32s = field(source, "f32")?
        .execute::<PrimitiveArray>(&mut execution)
        .map_err(vortex_error)?;
    let f64s = field(source, "f64")?
        .execute::<PrimitiveArray>(&mut execution)
        .map_err(vortex_error)?;
    let decimals = field(source, "decimal")?
        .execute::<DecimalArray>(&mut execution)
        .map_err(vortex_error)?;
    for (row, original) in originals.into_iter().enumerate() {
        assert_eq!(
            f32s.to_buffer::<f32>()[row].to_bits(),
            F32_BITS[original % 8]
        );
        assert_eq!(
            f64s.to_buffer::<f64>()[row].to_bits(),
            F64_BITS[original % 8]
        );
        let value = vortex::array::match_each_decimal_value_type!(decimals.values_type(), |D| {
            DecimalValue::from(decimals.buffer::<D>()[row]).cast::<i128>()
        });
        assert_eq!(value, Some([123, -999, 7][original % 3]));
    }
    Ok(())
}

#[test]
fn private_window_payload_preserves_unobserved_storage_and_public_validation() {
    let session = ResidentVortexSession::new(2 << 20, 1).unwrap();
    let output = session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let source = measures(8);
            let indices = index_array(8, false, context, |row| Ok(Some(7 - row)))?;
            let output = take_with_policy(
                &source,
                &indices,
                source.dtype(),
                CopyPolicy::PreserveUnobserved,
                context,
            )?;
            verify(&output, (0..8).rev(), context)?;
            assert!(take(&source, &indices, source.dtype(), context).is_err());
            // A finite selected row stays valid without inspecting the excluded rows.
            let one = index_array(1, false, context, |_| Ok(Some(5)))?;
            take(&source, &one, source.dtype(), context)?;
            Ok(output)
        })
        .unwrap();
    assert!(session.memory().snapshot().reserved_bytes > 0);
    let retained = output.slice(0..1).unwrap();
    drop(output);
    assert!(session.memory().snapshot().reserved_bytes > 0);
    drop(retained);
    assert_eq!(session.memory().snapshot().reserved_bytes, 0);
}

#[test]
fn private_window_payload_preserves_wide_decimal_limbs_through_native_selection_wrappers() {
    let values = [
        i256::from_parts(0x1234_5678_90ab_cdef, 1_i128 << 40),
        i256::from_parts(u128::MAX - 7, -(1_i128 << 40)),
        i256::from_i128(-7),
        i256::MAX,
        i256::MIN,
    ];
    let decimal = DecimalDType::new(2, 0);
    let source = DecimalArray::try_new(
        Buffer::from(values.to_vec()),
        decimal,
        Validity::NonNullable,
    )
    .unwrap()
    .into_array();
    let masked = MaskedArray::try_new(source, Validity::from_iter([true, true, false, true, true]))
        .unwrap()
        .into_array();
    let dictionary = DictArray::try_new(
        PrimitiveArray::from_iter([4u8, 0, 3, 1, 2]).into_array(),
        masked,
    )
    .unwrap()
    .into_array();
    let source = ChunkedArray::try_new(
        vec![
            dictionary.slice(0..2).unwrap(),
            dictionary.slice(2..5).unwrap(),
        ],
        dictionary.dtype().clone(),
    )
    .unwrap()
    .into_array();
    let session = ResidentVortexSession::new(2 << 20, 1).unwrap();
    let output = session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let indices = index_array(6, true, context, |row| {
                Ok([Some(3), Some(1), None, Some(0), Some(4), Some(2)][row])
            })?;
            let output = take_with_policy(
                &source,
                &indices,
                source.dtype(),
                CopyPolicy::PreserveUnobserved,
                context,
            )?;
            let mut execution = context.native_session().create_execution_ctx();
            let output = output
                .execute::<DecimalArray>(&mut execution)
                .map_err(vortex_error)?;
            assert_eq!(output.dtype(), source.dtype());
            let valid = output
                .validity()
                .map_err(vortex_error)?
                .execute_mask(6, &mut execution)
                .map_err(vortex_error)?;
            let actual = output.buffer::<i256>();
            for (row, expected) in [
                Some(values[1]),
                Some(values[0]),
                None,
                Some(values[4]),
                None,
                Some(values[3]),
            ]
            .into_iter()
            .enumerate()
            {
                assert_eq!(valid.value(row), expected.is_some());
                if let Some(expected) = expected {
                    assert_eq!(actual[row].to_parts(), expected.to_parts());
                }
            }
            Ok(output.into_array())
        })
        .unwrap();
    assert!(session.memory().snapshot().reserved_bytes > 0);
    drop(output);
    assert_eq!(session.memory().snapshot().reserved_bytes, 0);
}

#[test]
fn private_window_payload_observed_decimal_rejects_wrapped_values_without_panicking() {
    let source = DecimalArray::try_new(
        Buffer::from(vec![-999_i128, 7]),
        DecimalDType::new(2, 0),
        Validity::NonNullable,
    )
    .unwrap()
    .into_array();
    let chunks = ChunkedArray::try_new(
        vec![source.slice(0..1).unwrap(), source.slice(1..2).unwrap()],
        source.dtype().clone(),
    )
    .unwrap()
    .into_array();
    let session = ResidentVortexSession::new(2 << 20, 1).unwrap();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let indices = index_array(2, false, context, |row| Ok(Some(row)))?;
            for source in [&source, &chunks] {
                let baseline = context.memory().snapshot().reserved_bytes;
                assert!(take(source, &indices, source.dtype(), context).is_err());
                assert_eq!(context.memory().snapshot().reserved_bytes, baseline);
            }
            Ok(())
        })
        .unwrap();
    assert_eq!(session.memory().snapshot().reserved_bytes, 0);
}

#[test]
fn private_window_payload_keeps_valid_nested_float_bits_and_masks_null_list_children() {
    let source = ListArray::try_new(
        PrimitiveArray::from_iter(F32_BITS.map(f32::from_bits)).into_array(),
        PrimitiveArray::from_iter([0u64, 2, 8]).into_array(),
        Validity::from_iter([true, false]),
    )
    .unwrap()
    .into_array();
    let session = ResidentVortexSession::new(2 << 20, 1).unwrap();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let indices = index_array(3, true, context, |row| Ok([Some(1), Some(0), None][row]))?;
            let output = take_with_policy(
                &source,
                &indices,
                source.dtype(),
                CopyPolicy::PreserveUnobserved,
                context,
            )?;
            let mut execution = context.native_session().create_execution_ctx();
            let output = output.as_::<vortex::array::arrays::List>();
            let values = output
                .elements()
                .clone()
                .execute::<PrimitiveArray>(&mut execution)
                .map_err(vortex_error)?;
            assert_eq!(values.len(), 2, "null list payload must not escape");
            assert_eq!(
                values
                    .to_buffer::<f32>()
                    .iter()
                    .map(|value| value.to_bits())
                    .collect::<Vec<_>>(),
                F32_BITS[..2]
            );
            assert!(take(&source, &indices, source.dtype(), context).is_err());
            Ok(())
        })
        .unwrap();
    assert_eq!(session.memory().snapshot().reserved_bytes, 0);
}

#[test]
fn private_window_payload_masks_nested_parents_and_releases_denied_copies() {
    let session = ResidentVortexSession::new(2 << 20, 1).unwrap();
    let token = CancellationToken::default();
    let result = session.with_native_execution_context(&token, |context| {
        let list = ListArray::try_new(
            PrimitiveArray::from_iter([f32::from_bits(F32_BITS[0]), -0.0]).into_array(),
            PrimitiveArray::from_iter([0u64, 1, 2]).into_array(),
            Validity::from_iter([false, true]),
        )
        .unwrap()
        .into_array();
        let source = StructArray::try_new(
            FieldNames::from(["list"]),
            vec![list],
            2,
            Validity::from_iter([true, false]),
        )
        .unwrap()
        .into_array();
        let indices = index_array(3, true, context, |row| Ok([Some(0), Some(1), None][row]))?;
        let before = context.memory().snapshot().reserved_bytes;
        let output = take_with_policy(
            &source,
            &indices,
            source.dtype(),
            CopyPolicy::PreserveUnobserved,
            context,
        )?;
        let mut execution = context.native_session().create_execution_ctx();
        assert!(
            !output
                .execute_scalar(0, &mut execution)
                .map_err(vortex_error)?
                .is_null()
        );
        for row in 1..3 {
            assert!(
                output
                    .execute_scalar(row, &mut execution)
                    .map_err(vortex_error)?
                    .is_null()
            );
        }
        let list = field(&output, "list")?;
        for row in 0..3 {
            assert!(
                list.execute_scalar(row, &mut execution)
                    .map_err(vortex_error)?
                    .is_null()
            );
        }
        drop((output, list));
        assert_eq!(context.memory().snapshot().reserved_bytes, before);
        let hold = context.memory().reserve((2 << 20) - before - 1)?;
        assert!(
            take_with_policy(
                &source,
                &indices,
                source.dtype(),
                CopyPolicy::PreserveUnobserved,
                context
            )
            .is_err()
        );
        drop(hold);
        assert_eq!(context.memory().snapshot().reserved_bytes, before);
        token.cancel();
        assert!(
            take_with_policy(
                &source,
                &indices,
                source.dtype(),
                CopyPolicy::PreserveUnobserved,
                context
            )
            .is_err()
        );
        assert_eq!(context.memory().snapshot().reserved_bytes, before);
        Ok(())
    });
    assert!(result.unwrap_err().to_string().contains("cancelled"));
    assert_eq!(session.memory().snapshot().reserved_bytes, 0);
}

#[test]
fn private_window_order_storage_preserves_bits_through_resident_and_spilled_merges() {
    let workspace = std::env::temp_dir().join(format!(
        "shardloom-window-copy-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir(&workspace).unwrap();
    let policy = VortexRelationalSpillPolicy::new(&workspace, 64 << 20, 1 << 20).unwrap();
    for rows in [9usize, 24_013] {
        let session = ResidentVortexSession::new(16 << 20, 1).unwrap();
        session
            .with_native_execution_context(&CancellationToken::default(), |context| {
                let source = measures(rows);
                let DType::Struct(fields, _) = source.dtype() else {
                    unreachable!()
                };
                let fields = fields
                    .names()
                    .iter()
                    .zip(fields.fields())
                    .map(|(name, dtype)| (name.to_string(), dtype))
                    .collect();
                let mut spec = records::order(fields, vec![records::ORDINAL.into()])?;
                spec.copy_policy = CopyPolicy::PreserveUnobserved;
                let state = State::new(&policy, context)?;
                let mut order = Ordering::new(&spec, &state, 997, context)?;
                order.build(source, context)?;
                let stored = order.retain(context)?;
                assert_eq!(stored.rows(), rows as u64);
                let mut start = 0;
                while let Some(block) = stored.read_block_at(start, context)? {
                    let start_usize = usize::try_from(start).unwrap();
                    let end = start_usize + block.array().len();
                    verify(
                        block.array(),
                        (start_usize..end).map(|row| rows - 1 - row),
                        context,
                    )?;
                    start = end as u64;
                }
                assert_eq!(start, rows as u64);
                stored.finish(context)?;
                let report = state.finish()?;
                assert_eq!(report.runs_written > 0, rows > 9);
                assert_eq!(report.merge_passes > 0, rows > 9);
                assert!(report.owned_cleanup_completed);
                Ok(())
            })
            .unwrap();
        assert_eq!(session.memory().snapshot().reserved_bytes, 0);
        assert_eq!(std::fs::read_dir(&workspace).unwrap().count(), 0);
    }
    for key in ["f32", "f64", "decimal"] {
        let session = ResidentVortexSession::new(16 << 20, 1).unwrap();
        session
            .with_native_execution_context(&CancellationToken::default(), |context| {
                let source = measures(9);
                let DType::Struct(fields, _) = source.dtype() else {
                    unreachable!()
                };
                let fields = fields
                    .names()
                    .iter()
                    .zip(fields.fields())
                    .map(|(name, dtype)| (name.to_string(), dtype))
                    .collect();
                let mut spec = records::order(fields, vec![key.into()])?;
                spec.copy_policy = CopyPolicy::PreserveUnobserved;
                let state = State::new(&policy, context)?;
                let mut order = Ordering::new(&spec, &state, 3, context)?;
                assert!(order.build(source, context).is_err(), "observed key={key}");
                drop(order);
                assert!(state.finish()?.owned_cleanup_completed);
                Ok(())
            })
            .unwrap();
        assert_eq!(session.memory().snapshot().reserved_bytes, 0);
        assert_eq!(std::fs::read_dir(&workspace).unwrap().count(), 0);
    }
    std::fs::remove_dir(&workspace).unwrap();
}
