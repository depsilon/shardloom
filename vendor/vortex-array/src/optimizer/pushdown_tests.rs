// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 ShardLoom contributors

use std::any::Any;
use std::sync::Arc;
use std::thread::Builder;

use vortex_error::VortexResult;
use vortex_mask::Mask;
use vortex_session::VortexSession;

use crate::Canonical;
use crate::IntoArray;
use crate::VortexSessionExecute;
use crate::arrays::FixedSizeListArray;
use crate::arrays::ListArray;
use crate::arrays::ListViewArray;
use crate::arrays::MapArray;
use crate::arrays::PrimitiveArray;
use crate::arrays::Slice;
use crate::arrays::StructArray;
use crate::builtins::ArrayBuiltins;
use crate::dtype::DType;
use crate::dtype::FieldNames;
use crate::dtype::MapDType;
use crate::dtype::Nullability;
use crate::dtype::PType;
use crate::dtype::StructFields;
use crate::dtype::UnionVariants;
use crate::expr::stats::Precision;
use crate::expr::stats::Stat;
use crate::expr::stats::StatsProviderExt;
use crate::extension::datetime::TimeUnit;
use crate::extension::datetime::Timestamp;
use crate::scalar::Scalar;
use crate::validity::Validity;

fn small_stack<F: FnOnce() + Send + 'static>(name: &str, f: F) {
    Builder::new()
        .name(name.to_owned())
        .stack_size(128 * 1024)
        .spawn(f)
        .unwrap()
        .join()
        .unwrap();
}

#[test]
fn shallow_pushdown_preserves_slice_stats_and_retained_owner() -> VortexResult<()> {
    let mut ctx = VortexSession::empty().create_execution_ctx();
    let base = PrimitiveArray::from_iter([10_i32, 10, 10, 10]).into_array();
    drop(base.statistics().compute_stat(Stat::IsConstant, &mut ctx)?);
    drop(base.statistics().compute_stat(Stat::IsSorted, &mut ctx)?);
    let st = StructArray::try_new(
        FieldNames::from(["x"]),
        vec![base],
        4,
        Validity::NonNullable,
    )?
    .into_array();
    st.statistics().set(Stat::IsSorted, Precision::exact(true));

    let sliced = st.slice(1..3)?;
    assert_eq!(sliced.len(), 2);
    assert_eq!(
        sliced.statistics().get_as::<bool>(Stat::IsSorted),
        Precision::exact(true)
    );

    let mut deep_struct = PrimitiveArray::from_iter([1_i32, 2, 3, 4]).into_array();
    for _ in 0..10 {
        deep_struct = StructArray::try_new(
            FieldNames::from(["f"]),
            vec![deep_struct],
            4,
            Validity::NonNullable,
        )?
        .into_array();
    }
    deep_struct
        .statistics()
        .set(Stat::IsSorted, Precision::exact(true));
    let deep_sliced = deep_struct.slice(1..3)?;
    assert_eq!(
        deep_sliced.statistics().get_as::<bool>(Stat::IsSorted),
        Precision::exact(true)
    );

    let owner: Arc<dyn Any + Send + Sync> = Arc::new(99_usize);
    let owned = st.with_retained_owner(Arc::clone(&owner))?;
    let owned_slice = owned.slice(0..2)?;
    assert!(owned_slice.is::<Slice>());
    assert_eq!(Arc::strong_count(&owner), 3);

    let nullable = StructArray::try_new(
        FieldNames::from(["x"]),
        vec![PrimitiveArray::from_iter([1_i32, 2, 3, 4]).into_array()],
        4,
        Validity::from_iter([true, false, true, true]),
    )?
    .into_array();
    let idx = PrimitiveArray::from_option_iter([Some(2_u64), None, Some(0)]).into_array();
    let taken = nullable.take(idx)?;
    assert_eq!(taken.execute_scalar(0, &mut ctx)?, nullable.execute_scalar(2, &mut ctx)?);
    assert!(taken.execute_scalar(1, &mut ctx)?.is_null());
    assert_eq!(taken.execute_scalar(2, &mut ctx)?, nullable.execute_scalar(0, &mut ctx)?);

    let filtered = nullable.filter(Mask::from_iter([true, false, false, true]))?;
    assert_eq!(filtered.len(), 2);
    assert_eq!(filtered.execute_scalar(1, &mut ctx)?, nullable.execute_scalar(3, &mut ctx)?);

    drop((owned_slice, owned));
    assert_eq!(Arc::strong_count(&owner), 1);
    Ok(())
}

#[test]
fn deep_2048_mixed_nested_compute_empty_and_scalar_on_128k_stack() {
    small_stack("deep_2048_mixed_nested_compute", || {
        const DEPTH: usize = 2_048;
        let session = VortexSession::empty();
        let mut ctx = session.create_execution_ctx();

        let mut arr = PrimitiveArray::from_iter([10_i32, 20, 30, 40]).into_array();
        let mut cast_dtype = DType::Primitive(PType::I64, Nullability::Nullable);

        for level in 0..DEPTH {
            match level % 4 {
                0 => {
                    arr = StructArray::try_new(
                        FieldNames::from(["f"]),
                        vec![arr],
                        4,
                        Validity::NonNullable,
                    )
                    .unwrap()
                    .into_array();
                    cast_dtype = DType::Struct(
                        StructFields::new(FieldNames::from(["f"]), vec![cast_dtype]),
                        Nullability::Nullable,
                    );
                }
                1 => {
                    arr = FixedSizeListArray::new(arr, 1, Validity::NonNullable, 4).into_array();
                    cast_dtype = DType::FixedSizeList(Arc::new(cast_dtype), 1, Nullability::Nullable);
                }
                2 => {
                    arr = ListArray::try_new(
                        arr,
                        PrimitiveArray::from_iter([0_u64, 1, 2, 3, 4]).into_array(),
                        Validity::NonNullable,
                        &mut ctx,
                    )
                    .unwrap()
                    .into_array();
                    cast_dtype = DType::List(Arc::new(cast_dtype), Nullability::Nullable);
                }
                _ => {
                    let val_dtype = arr.dtype().clone();
                    let entries_struct = StructArray::try_new(
                        FieldNames::from(["key", "value"]),
                        vec![PrimitiveArray::from_iter([1_i32, 2, 3, 4]).into_array(), arr],
                        4,
                        Validity::NonNullable,
                    )
                    .unwrap()
                    .into_array();
                    let entries = ListViewArray::try_new(
                        entries_struct,
                        PrimitiveArray::from_iter([0_u64, 1, 2, 3]).into_array(),
                        PrimitiveArray::from_iter([1_u64, 1, 1, 1]).into_array(),
                        Validity::NonNullable,
                        &mut ctx,
                    )
                    .unwrap();
                    let map_dt = MapDType::try_new(
                        DType::Primitive(PType::I32, Nullability::NonNullable),
                        val_dtype,
                        false,
                    )
                    .unwrap();
                    arr = MapArray::try_new(map_dt, entries).unwrap().into_array();
                    let cast_map = MapDType::try_new(
                        DType::Primitive(PType::I32, Nullability::NonNullable),
                        cast_dtype,
                        false,
                    )
                    .unwrap();
                    cast_dtype = DType::Map(cast_map, Nullability::Nullable);
                }
            }
        }

        let sliced = arr.slice(1..3).unwrap();
        let filtered = arr.filter(Mask::from_iter([true, false, true, false])).unwrap();
        let taken = arr.take(PrimitiveArray::from_iter([2_u64, 0]).into_array()).unwrap();
        let casted = arr.cast(cast_dtype.clone()).unwrap();

        let s0 = sliced.execute_scalar(0, &mut ctx).unwrap();
        let orig1 = arr.execute_scalar(1, &mut ctx).unwrap();
        let _: &Scalar = &s0;
        assert_eq!(s0, orig1);
        assert_eq!(filtered.execute_scalar(1, &mut ctx).unwrap(), arr.execute_scalar(2, &mut ctx).unwrap());
        assert_eq!(taken.execute_scalar(0, &mut ctx).unwrap(), arr.execute_scalar(2, &mut ctx).unwrap());

        let empty = Canonical::empty(&cast_dtype);
        assert!(empty.is_empty());

        let canon = casted.clone().execute::<Canonical>(&mut ctx).unwrap();
        assert_eq!(canon.len(), 4);

        drop((canon, empty, orig1, s0, casted, taken, filtered, sliced, arr));
    });
}

#[test]
fn deep_2048_extension_union_empty_and_scalar_on_128k_stack() {
    small_stack("deep_2048_extension_union", || {
        let mut dtype = DType::Extension(Timestamp::new(TimeUnit::Microseconds, Nullability::NonNullable).erased());
        for level in 0..2_048 {
            if level % 2 == 0 {
                let variants =
                    UnionVariants::try_new(["a"].into(), vec![dtype], vec![0]).unwrap();
                dtype = DType::Union(variants, Nullability::NonNullable);
            } else {
                dtype = DType::Struct(
                    StructFields::new(FieldNames::from(["u"]), vec![dtype]),
                    Nullability::NonNullable,
                );
            }
        }
        let empty = Canonical::empty(&dtype);
        assert!(empty.is_empty());
        drop((empty, dtype));
    });
}
