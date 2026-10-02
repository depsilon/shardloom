use super::*;
use crate::resident_session::ResidentVortexSession;
use shardloom_exec::compute_pool::CancellationToken;
use vortex::array::{
    VortexSessionExecute as _,
    arrays::PrimitiveArray,
    dtype::{Nullability, PType},
};

fn input(keys: &[Option<u64>], ids: &[u32]) -> ArrayRef {
    StructArray::try_new(
        FieldNames::from(["key", "id"]),
        vec![
            PrimitiveArray::from_option_iter(keys.iter().copied()).into_array(),
            PrimitiveArray::from_iter(ids.iter().copied()).into_array(),
        ],
        keys.len(),
        Validity::NonNullable,
    )
    .unwrap()
    .into_array()
}

fn spec(kind: Kind) -> Spec {
    let mut fields = vec![(
        "left_id".to_owned(),
        DType::Primitive(
            PType::U32,
            if matches!(kind, Kind::Right | Kind::Full) {
                Nullability::Nullable
            } else {
                Nullability::NonNullable
            },
        ),
    )];
    let mut columns = vec![(Side::Left, "id".to_owned())];
    if !matches!(kind, Kind::LeftSemi | Kind::LeftAnti) {
        fields.push((
            "right_id".to_owned(),
            DType::Primitive(
                PType::U32,
                if matches!(kind, Kind::Left | Kind::Full) {
                    Nullability::Nullable
                } else {
                    Nullability::NonNullable
                },
            ),
        ));
        columns.push((Side::Right, "id".to_owned()));
    }
    let keys = if kind == Kind::Cross {
        vec![]
    } else {
        vec!["key".to_owned()]
    };
    Spec {
        kind,
        condition: None,
        left_keys: keys.clone(),
        right_keys: keys,
        fields,
        columns,
    }
}

type Pair = (Option<u64>, Option<u64>);

fn pairs(array: &ArrayRef) -> Vec<Pair> {
    let mut execution = vortex::array::legacy_session().create_execution_ctx();
    let fields = array.dtype().as_struct_fields_opt().unwrap();
    let columns = fields
        .names()
        .iter()
        .map(|name| logical_field_from_native_array(array, name.as_ref()).unwrap())
        .collect::<Vec<_>>();
    let mut value = |row, column| -> Option<u64> {
        if column >= columns.len() {
            return None;
        }
        match result_batch::scalar_value(&columns[column], row, &mut execution).unwrap() {
            result_batch::Value::Null => None,
            result_batch::Value::UInt(value) => Some(value),
            _ => panic!("unexpected ID dtype"),
        }
    };
    (0..array.len())
        .map(|row| (value(row, 0), value(row, 1)))
        .collect()
}

#[test]
fn every_native_join_kind_preserves_duplicates_nulls_order_and_integer_payload_width() {
    let left = input(
        &[Some(2), Some(1), Some(2), None, Some(u64::MAX)],
        &[10, 11, 12, 13, 14],
    );
    let right = [
        input(&[Some(1), Some(2), None], &[20, 21, 22]),
        input(&[Some(2), Some(3), Some(u64::MAX)], &[23, 24, 25]),
    ];
    let matched = vec![
        (Some(10), Some(21)),
        (Some(10), Some(23)),
        (Some(11), Some(20)),
        (Some(12), Some(21)),
        (Some(12), Some(23)),
        (Some(14), Some(25)),
    ];
    let mut outer_left = matched.clone();
    outer_left.insert(5, (Some(13), None));
    let mut outer_right = matched.clone();
    outer_right.extend([(None, Some(22)), (None, Some(24))]);
    let mut full = outer_left.clone();
    full.extend([(None, Some(22)), (None, Some(24))]);
    let cross = (10..15)
        .flat_map(|left| (20..26).map(move |right| (Some(left), Some(right))))
        .collect::<Vec<_>>();
    for (kind, expected) in [
        (Kind::Inner, matched),
        (Kind::Left, outer_left),
        (Kind::Right, outer_right),
        (Kind::Full, full),
        (
            Kind::LeftSemi,
            vec![
                (Some(10), None),
                (Some(11), None),
                (Some(12), None),
                (Some(14), None),
            ],
        ),
        (Kind::LeftAnti, vec![(Some(13), None)]),
        (Kind::Cross, cross),
    ] {
        let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
        let memory = session.memory().clone();
        let binding = spec(kind);
        let mut observed = Vec::new();
        let mut retained = Vec::new();
        session
            .with_native_execution_context(&CancellationToken::default(), |context| {
                let mut join = Join::new(&binding, context.memory())?;
                for array in &right {
                    join.build(array.clone(), context)?;
                }
                let mut consume = |array: ArrayRef| {
                    assert!(array.len() <= 2);
                    assert_eq!(
                        array.dtype(),
                        &DType::struct_(binding.fields.clone(), Nullability::NonNullable)
                    );
                    observed.extend(pairs(&array));
                    retained.push(array);
                    Ok(())
                };
                let rows = join.consume(left.clone(), context, 2, &mut consume)?
                    + join.finish(context, 2, &mut consume)?;
                assert_eq!(usize::try_from(rows).unwrap(), expected.len());
                Ok(())
            })
            .unwrap();
        assert_eq!(observed, expected, "{kind:?}");
        assert!(memory.snapshot().reserved_bytes > 0);
        drop(session);
        assert_eq!(
            retained.iter().flat_map(pairs).collect::<Vec<_>>(),
            expected
        );
        drop(retained);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn empty_sides_null_extension_and_cancellation_have_no_leaked_native_state() {
    for kind in [
        Kind::Inner,
        Kind::Left,
        Kind::Right,
        Kind::Full,
        Kind::LeftSemi,
        Kind::LeftAnti,
        Kind::Cross,
    ] {
        for empty_left in [false, true] {
            let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
            let binding = spec(kind);
            let mut observed = Vec::new();
            session
                .with_native_execution_context(&CancellationToken::default(), |context| {
                    let mut join = Join::new(&binding, context.memory())?;
                    if empty_left {
                        join.build(input(&[None], &[20]), context)?;
                    }
                    let mut consume = |array: ArrayRef| {
                        observed.extend(pairs(&array));
                        Ok(())
                    };
                    if !empty_left {
                        join.consume(input(&[None], &[10]), context, 1, &mut consume)?;
                    }
                    join.finish(context, 1, &mut consume)?;
                    Ok(())
                })
                .unwrap();
            let expected = if empty_left && matches!(kind, Kind::Right | Kind::Full) {
                vec![(None, Some(20))]
            } else if !empty_left && matches!(kind, Kind::Left | Kind::Full | Kind::LeftAnti) {
                vec![(Some(10), None)]
            } else {
                vec![]
            };
            assert_eq!(observed, expected, "{kind:?}, empty_left={empty_left}");
            assert_eq!(session.memory().snapshot().reserved_bytes, 0);
        }
    }
    let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
    let binding = spec(Kind::Cross);
    let cancellation = CancellationToken::default();
    let mut delivered = 0;
    assert!(
        session
            .with_native_execution_context(&cancellation, |context| {
                let mut join = Join::new(&binding, context.memory())?;
                join.build(input(&[Some(1), Some(2)], &[20, 21]), context)?;
                join.consume(
                    input(&[Some(1), Some(2)], &[10, 11]),
                    context,
                    1,
                    &mut |_| {
                        delivered += 1;
                        cancellation.cancel();
                        Ok(())
                    },
                )?;
                Ok(())
            })
            .is_err()
    );
    assert_eq!(delivered, 1);
    assert_eq!(session.memory().snapshot().reserved_bytes, 0);
}

#[test]
fn native_relational_join_null_root_hides_keys_and_payload_before_matching() {
    let left = StructArray::new(
        FieldNames::from(["key", "id"]),
        vec![
            PrimitiveArray::from_iter([1u64, 1, 2]).into_array(),
            PrimitiveArray::from_iter([10u32, 11, 12]).into_array(),
        ],
        3,
        Validity::from_iter([true, false, true]),
    )
    .into_array();
    let right = input(&[Some(1), Some(2)], &[20, 21]);
    let binding = spec(Kind::Full);
    let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
    let mut observed = Vec::new();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let mut join = Join::new(&binding, context.memory())?;
            join.build(right.clone(), context)?;
            let mut consume = |array: ArrayRef| {
                observed.extend(pairs(&array));
                Ok(())
            };
            join.consume(left.clone(), context, 1, &mut consume)?;
            join.finish(context, 1, &mut consume)?;
            Ok(())
        })
        .unwrap();
    assert_eq!(
        observed,
        [(Some(10), Some(20)), (None, None), (Some(12), Some(21))]
    );
    assert_eq!(session.memory().snapshot().reserved_bytes, 0);
}
