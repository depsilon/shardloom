use super::*;
use crate::resident_session::ResidentVortexSession;
use vortex::array::{
    IntoArray as _, VortexSessionExecute as _,
    arrays::{DictArray, ExtensionArray, NullArray, StructArray, VarBinArray},
    dtype::{FieldNames, Nullability},
    extension::datetime::{Date, TimeUnit, Timestamp},
    validity::Validity,
};

fn key(array: &ArrayRef, memory: &LiveMemoryPool, policy: FloatPolicy) -> KeyColumn {
    KeyColumn::values_with_float_policy(
        array,
        &mut vortex::array::array_session().create_execution_ctx(),
        memory,
        &CancellationToken::default(),
        policy,
    )
    .unwrap()
}

#[test]
#[allow(clippy::too_many_lines)] // Keep the complete admitted/declined metadata matrix together.
fn ordered_input_flat_domain_proof_keeps_width_kind_decimal_and_temporal_metadata() {
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    let timestamp = |unit, timezone: Option<&str>| {
        ExtensionArray::new(
            Timestamp::new_with_tz(unit, timezone.map(Into::into), Nullability::Nullable).erased(),
            PrimitiveArray::from_option_iter([Some(0i64), None]).into_array(),
        )
        .into_array()
    };
    let values = vec![
        ("null", NullArray::new(2).into_array()),
        (
            "bool",
            BoolArray::from_iter([Some(false), None]).into_array(),
        ),
        ("bool", BoolArray::from_iter([false, true]).into_array()),
        (
            "i8",
            PrimitiveArray::from_option_iter([Some(-1i8), None]).into_array(),
        ),
        (
            "i16",
            PrimitiveArray::from_option_iter([Some(-1i16), None]).into_array(),
        ),
        (
            "i32",
            PrimitiveArray::from_option_iter([Some(-1i32), None]).into_array(),
        ),
        (
            "i64",
            PrimitiveArray::from_option_iter([Some(-1i64), None]).into_array(),
        ),
        ("i64", PrimitiveArray::from_iter([0i64, 1]).into_array()),
        (
            "u8",
            PrimitiveArray::from_option_iter([Some(1u8), None]).into_array(),
        ),
        (
            "u16",
            PrimitiveArray::from_option_iter([Some(1u16), None]).into_array(),
        ),
        (
            "u32",
            PrimitiveArray::from_option_iter([Some(1u32), None]).into_array(),
        ),
        (
            "u64",
            PrimitiveArray::from_option_iter([Some(u64::MAX), None]).into_array(),
        ),
        (
            "f32",
            PrimitiveArray::from_option_iter([Some(-0.0f32), None]).into_array(),
        ),
        (
            "f64",
            PrimitiveArray::from_option_iter([Some(-0.0f64), None]).into_array(),
        ),
        (
            "utf8",
            VarBinViewArray::from_iter_nullable_str([Some("λ"), None]).into_array(),
        ),
        (
            "utf8",
            DictArray::try_new(
                PrimitiveArray::from_iter([1u8, 0]).into_array(),
                VarBinViewArray::from_iter_str(["λ", ""]).into_array(),
            )
            .unwrap()
            .into_array(),
        ),
        (
            "binary",
            VarBinArray::from(vec![Some(&b"\xff\0"[..]), None]).into_array(),
        ),
        (
            "decimal3-1",
            DecimalArray::from_option_iter([Some(-9i16), None], DecimalDType::new(3, 1))
                .into_array(),
        ),
        (
            "decimal3-1",
            DecimalArray::from_option_iter([Some(-9i128), None], DecimalDType::new(3, 1))
                .into_array(),
        ),
        (
            "decimal4-1",
            DecimalArray::from_option_iter([Some(-9i128), None], DecimalDType::new(4, 1))
                .into_array(),
        ),
        (
            "decimal3-2",
            DecimalArray::from_option_iter([Some(-9i128), None], DecimalDType::new(3, 2))
                .into_array(),
        ),
        (
            "date",
            ExtensionArray::new(
                Date::new(TimeUnit::Days, Nullability::Nullable).erased(),
                PrimitiveArray::from_option_iter([Some(-1i32), None]).into_array(),
            )
            .into_array(),
        ),
        ("micros", timestamp(TimeUnit::Microseconds, None)),
        ("nanos", timestamp(TimeUnit::Nanoseconds, None)),
        ("nanos-utc", timestamp(TimeUnit::Nanoseconds, Some("UTC"))),
        (
            "nanos-chicago",
            timestamp(TimeUnit::Nanoseconds, Some("America/Chicago")),
        ),
        (
            "nested",
            StructArray::new(
                FieldNames::from(["child"]),
                vec![PrimitiveArray::from_iter([0i64, 1]).into_array()],
                2,
                Validity::NonNullable,
            )
            .into_array(),
        ),
    ];
    let keys = values
        .iter()
        .map(|(kind, array)| (*kind, key(array, &memory, FloatPolicy::Strict)))
        .collect::<Vec<_>>();
    for (a, left) in &keys {
        for (b, right) in &keys {
            assert_eq!(
                left.same_flat_order_domain(right),
                a == b && *a != "nested",
                "{a}/{b}"
            );
            if left.same_flat_order_domain(right) {
                for row in 0..2 {
                    left.validate_at(row).unwrap();
                    right.validate_at(row).unwrap();
                    for other in 0..2 {
                        left.compare_at(row, right, other).unwrap();
                    }
                }
            }
        }
    }
    drop(keys);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn ordered_input_domain_proof_does_not_replace_strict_float_validation() {
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    let array = PrimitiveArray::from_iter([0.0f64, -0.0, f64::NAN, f64::INFINITY]).into_array();
    let strict = key(&array, &memory, FloatPolicy::Strict);
    let extended = key(&array, &memory, FloatPolicy::Extended);
    assert!(strict.same_flat_order_domain(&strict));
    assert!(extended.same_flat_order_domain(&extended));
    assert!(!strict.same_flat_order_domain(&extended));
    assert!(strict.validate_at(2).is_err());
    assert!(strict.validate_at(3).is_err());
    for row in 0..4 {
        extended.validate_at(row).unwrap();
    }
    assert_eq!(
        extended.compare_at(0, &extended, 1).unwrap(),
        Ordering::Equal
    );
    assert_eq!(
        extended.compare_at(3, &extended, 2).unwrap(),
        Ordering::Less
    );
}

#[test]
fn ordered_input_table_proof_declines_missing_and_changed_late_domains_without_allocation() {
    use super::super::native_relational_batch::{Batch, Table};
    let array = |column: ArrayRef| {
        let len = column.len();
        StructArray::new(
            FieldNames::from(["key"]),
            vec![column],
            len,
            Validity::NonNullable,
        )
        .into_array()
    };
    let session = ResidentVortexSession::new(1 << 20, 1).unwrap();
    let memory = session.memory().clone();
    let cancel = CancellationToken::default();
    let error = session
        .with_native_execution_context(&cancel, |context| {
            let mut table = Table::new(context.memory())?;
            assert!(table.has_uniform_order_domain(1, context)?);
            for row in 0..3 {
                let batch = Batch::with_names(
                    array(PrimitiveArray::from_iter([i64::from(row)]).into_array()),
                    ["key"],
                    context,
                )?;
                batch.validate_keys(0)?;
                table.push(batch)?;
            }
            let before = memory.snapshot().reserved_bytes;
            assert!(table.has_uniform_order_domain(1, context)?);
            assert!(!table.has_uniform_order_domain(2, context)?);
            assert_eq!(memory.snapshot().reserved_bytes, before);
            // A later all-NULL batch of a different dtype must still decline.
            let later = Batch::with_names(
                array(PrimitiveArray::from_option_iter([None::<i32>]).into_array()),
                ["key"],
                context,
            )?;
            later.validate_keys(0)?;
            table.push(later)?;
            assert!(!table.has_uniform_order_domain(1, context)?);
            cancel.cancel();
            assert!(table.has_uniform_order_domain(1, context).is_err());
            Ok(())
        })
        .unwrap_err();
    // The enclosing context also checks cancellation after the closure returns.
    assert!(error.to_string().contains("execution cancelled"));
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}
