//! Full storage-domain round trips without the provider's calendar scalar path.

use super::*;
use serde_json::{Value, json};
use vortex::array::{
    ExecutionCtx,
    arrays::{
        BoolArray, ExtensionArray, FixedSizeListArray, ListArray, ListViewArray,
        extension::ExtensionArraySlotsExt as _, fixed_size_list::FixedSizeListArrayExt as _,
        listview::ListViewArraySlotsExt as _, struct_::StructArrayExt as _,
    },
    extension::datetime::{Date, TimeUnit, Timestamp},
};

#[derive(Debug, Clone, Copy)]
enum Values {
    Endpoints,
    AllNull,
    Empty,
}

#[derive(Debug, Clone, Copy)]
enum Route {
    Array,
    Stream,
    Retained,
    ColumnCandidate,
}

fn timestamp(values: &[Option<i64>]) -> ArrayRef {
    ExtensionArray::new(
        Timestamp::new(TimeUnit::Microseconds, Nullability::Nullable).erased(),
        PrimitiveArray::from_option_iter(values.iter().copied()).into_array(),
    )
    .into_array()
}

fn fixture(profile: Values) -> ArrayRef {
    let valid = !matches!(profile, Values::AllNull);
    let stamp = timestamp(&[
        valid.then_some(i64::MIN),
        valid.then_some(-1),
        valid.then_some(0),
        valid.then_some(i64::MAX),
        None,
    ]);
    let day = ExtensionArray::new(
        Date::new(TimeUnit::Days, Nullability::Nullable).erased(),
        PrimitiveArray::from_option_iter([
            valid.then_some(i32::MIN),
            valid.then_some(-1),
            valid.then_some(0),
            valid.then_some(i32::MAX),
            None,
        ])
        .into_array(),
    )
    .into_array();
    let fixed = FixedSizeListArray::new(stamp.clone(), 1, Validity::NonNullable, 5).into_array();
    let list = ListArray::new(
        fixed,
        PrimitiveArray::from_iter([0_u32, 1, 2, 3, 4, 5]).into_array(),
        Validity::NonNullable,
    )
    .into_array();
    let nested = StructArray::new(
        FieldNames::from(["items"]),
        vec![list],
        5,
        Validity::from_iter([valid, false, valid, valid, valid]),
    )
    .into_array();
    let array = StructArray::new(
        FieldNames::from([
            "timestamp",
            "day",
            "nested",
            "plain",
            "renamed_payload",
            "flag",
        ]),
        vec![
            stamp,
            day,
            nested,
            PrimitiveArray::from_option_iter([
                valid.then_some(10_i64),
                valid.then_some(11),
                None,
                valid.then_some(13),
                valid.then_some(14),
            ])
            .into_array(),
            VarBinViewArray::from_iter_nullable_str([
                valid.then_some("東京"),
                valid.then_some("λ"),
                None,
                valid.then_some(""),
                valid.then_some("last"),
            ])
            .into_array(),
            BoolArray::from_iter([
                valid.then_some(true),
                valid.then_some(false),
                valid.then_some(true),
                None,
                valid.then_some(false),
            ])
            .into_array(),
        ],
        5,
        Validity::NonNullable,
    )
    .into_array();
    if matches!(profile, Values::Empty) {
        array.slice(0..0).unwrap()
    } else {
        array
    }
}

// Scalar construction of an extension is precisely the restricted provider
// boundary under test. Compare its integer storage and separately assert DType,
// field order, extension metadata, list shape and every parent validity bit.
fn values(array: &ArrayRef, execution: &mut ExecutionCtx) -> Vec<Value> {
    match array.dtype() {
        DType::Extension(_) => {
            let extension = array.clone().execute::<ExtensionArray>(execution).unwrap();
            values(extension.storage(), execution)
        }
        DType::Struct(fields, _) => {
            let array = array.clone().execute::<StructArray>(execution).unwrap();
            let children = array
                .iter_unmasked_fields()
                .map(|child| values(child, execution))
                .collect::<Vec<_>>();
            let validity = array.validity().unwrap();
            (0..array.len())
                .map(|row| {
                    if !validity.execute_is_valid(row, execution).unwrap() {
                        return Value::Null;
                    }
                    Value::Object(
                        fields
                            .names()
                            .iter()
                            .zip(&children)
                            .map(|(name, child)| (name.to_string(), child[row].clone()))
                            .collect(),
                    )
                })
                .collect()
        }
        DType::List(..) => {
            let array = array.clone().execute::<ListViewArray>(execution).unwrap();
            let offsets = array
                .offsets()
                .clone()
                .execute::<PrimitiveArray>(execution)
                .unwrap();
            let sizes = array
                .sizes()
                .clone()
                .execute::<PrimitiveArray>(execution)
                .unwrap();
            let validity = array.validity().unwrap();
            (0..array.len())
                .map(|row| {
                    if !validity.execute_is_valid(row, execution).unwrap() {
                        return Value::Null;
                    }
                    let start = offsets
                        .execute_scalar(row, execution)
                        .unwrap()
                        .as_primitive()
                        .as_::<usize>()
                        .unwrap();
                    let size = sizes
                        .execute_scalar(row, execution)
                        .unwrap()
                        .as_primitive()
                        .as_::<usize>()
                        .unwrap();
                    Value::Array(values(
                        &array.elements().slice(start..start + size).unwrap(),
                        execution,
                    ))
                })
                .collect()
        }
        DType::FixedSizeList(..) => {
            let array = array
                .clone()
                .execute::<FixedSizeListArray>(execution)
                .unwrap();
            let validity = array.validity().unwrap();
            (0..array.len())
                .map(|row| {
                    if validity.execute_is_valid(row, execution).unwrap() {
                        Value::Array(values(
                            &array.fixed_size_list_elements_at(row).unwrap(),
                            execution,
                        ))
                    } else {
                        Value::Null
                    }
                })
                .collect()
        }
        _ => (0..array.len())
            .map(|row| json!(array.execute_scalar(row, execution).unwrap().to_string()))
            .collect(),
    }
}

fn write_and_verify(
    context: &LocalVortexWriteContext,
    path: &Path,
    composition: Composition,
    route: Route,
    array: &ArrayRef,
) {
    let decision = decision(composition, path);
    let memory = matches!(route, Route::Retained | Route::ColumnCandidate)
        .then(|| NativeIngestMemory::new(16 << 20).unwrap());
    let timing = VortexWriterStageTiming::default();
    let dtype = array.dtype().clone();
    let mut execution = context.session.create_execution_ctx();
    let expected = values(array, &mut execution);
    if matches!(route, Route::Array) {
        let result = context
            .write_array(
                path,
                array,
                false,
                &decision,
                "test",
                &LiveMemoryPool::new(16 << 20).unwrap(),
            )
            .unwrap();
        assert!(
            result
                .writer_layout_strategy_applied
                .contains("file_min_max=omitted_for_full_domain_timestamp")
        );
        assert!(
            result
                .writer_compression_policy
                .contains("timestamp_fields=preserved_uncompressed")
        );
    } else {
        let (options, _) = stream_options(
            context,
            &decision,
            &timing,
            memory.as_ref(),
            &dtype,
            if matches!(route, Route::ColumnCandidate) {
                StreamFooterLayout::ColumnAddressable
            } else {
                StreamFooterLayout::RetainedRows
            },
            None,
        )
        .unwrap();
        // Interleaved empty chunks exercise the retained source-batch boundary.
        let batches = [
            array.slice(0..0).unwrap(),
            array.clone(),
            array.slice(0..0).unwrap(),
        ];
        let summary = options
            .blocking(&context.runtime)
            .write(
                fs::File::create(path).unwrap(),
                ArrayIteratorAdapter::new(dtype.clone(), batches.into_iter().map(Ok)),
            )
            .unwrap();
        assert_eq!(summary.row_count(), u64::try_from(array.len()).unwrap());
    }
    let file = context
        .runtime
        .block_on(context.session.open_options().open_path(path))
        .unwrap();
    assert_eq!(file.dtype(), &dtype);
    let mut actual = Vec::new();
    for chunk in file
        .scan()
        .unwrap()
        .with_ordered(true)
        .into_array_iter(&context.runtime)
        .unwrap()
    {
        let chunk = chunk.unwrap();
        assert_eq!(chunk.dtype(), &dtype);
        actual.extend(values(&chunk, &mut execution));
    }
    assert_eq!(actual, expected, "{composition:?} {route:?}");
    verify_statistics(&file, array, &mut execution);
    drop(file);
    if let Some(memory) = memory {
        assert_eq!(memory.pool.snapshot().reserved_bytes, 0);
        assert_eq!(memory.pool.snapshot().denied_reservations, 0);
    }
}

fn verify_statistics(
    file: &vortex::file::VortexFile,
    array: &ArrayRef,
    execution: &mut ExecutionCtx,
) {
    let statistics = file.footer().statistics().unwrap();
    for index in 0..statistics.stats_sets().len() {
        let (stats, _) = statistics.get(index);
        assert!(stats.get(Stat::Min).is_absent());
        assert!(stats.get(Stat::Max).is_absent());
        if !array.is_empty() {
            let source = if array.dtype().is_struct() {
                array
                    .as_::<vortex::array::arrays::Struct>()
                    .unmasked_field(index)
                    .clone()
            } else {
                array.clone()
            };
            let nulls = (0..source.len())
                .filter(|row| {
                    !source
                        .validity()
                        .unwrap()
                        .execute_is_valid(*row, execution)
                        .unwrap()
                })
                .count();
            assert_eq!(
                stats.get(Stat::NullCount).as_exact(),
                Scalar::from(u64::try_from(nulls).unwrap()).into_value()
            );
            if matches!(source.dtype(), DType::Bool(_)) && nulls < source.len() {
                assert_eq!(
                    stats.get(Stat::Sum).as_exact(),
                    Scalar::from(2_u64).into_value()
                );
            }
        }
    }
}

#[test]
fn full_domain_temporal_writer_profiles_preserve_values_types_nulls_and_statistics_policy() {
    let directory = Directory::new();
    LOCAL_VORTEX_WRITE_CONTEXT.with(|cell| {
        let context = cell.borrow();
        let _drivers =
            crate::resident_worker_group::ResidentWorkerGroup::new(&context.runtime, 1).unwrap();
        for composition in [
            Composition::Default,
            Composition::FastLoad,
            Composition::Balanced,
            Composition::SourceText,
        ] {
            for route in [
                Route::Array,
                Route::Stream,
                Route::Retained,
                Route::ColumnCandidate,
            ] {
                for profile in [Values::Endpoints, Values::AllNull, Values::Empty] {
                    let path = directory
                        .0
                        .join(format!("{composition:?}-{route:?}-{profile:?}.vortex"));
                    write_and_verify(&context, &path, composition, route, &fixture(profile));
                }
                let path = directory
                    .0
                    .join(format!("root-{composition:?}-{route:?}.vortex"));
                write_and_verify(
                    &context,
                    &path,
                    composition,
                    route,
                    &timestamp(&[Some(i64::MIN), Some(i64::MAX), None]),
                );
            }
        }
    });
}

#[test]
fn date_only_files_keep_full_domain_minimum_and_maximum_statistics() {
    let directory = Directory::new();
    LOCAL_VORTEX_WRITE_CONTEXT.with(|cell| {
        let context = cell.borrow();
        let _drivers =
            crate::resident_worker_group::ResidentWorkerGroup::new(&context.runtime, 1).unwrap();
        let source = fixture(Values::Endpoints);
        let source = source.as_::<vortex::array::arrays::Struct>();
        let array = StructArray::new(
            FieldNames::from(["day"]),
            vec![source.unmasked_field(1).clone()],
            5,
            Validity::NonNullable,
        )
        .into_array();
        for composition in [
            Composition::Default,
            Composition::FastLoad,
            Composition::Balanced,
            Composition::SourceText,
        ] {
            let path = directory
                .0
                .join(format!("date-only-{composition:?}.vortex"));
            let decision = decision(composition, &path);
            let result = context
                .write_array(
                    &path,
                    &array,
                    false,
                    &decision,
                    "test",
                    &LiveMemoryPool::new(16 << 20).unwrap(),
                )
                .unwrap();
            assert!(
                !result
                    .writer_layout_strategy_applied
                    .contains("omitted_for_full_domain_timestamp")
            );
            let file = context
                .runtime
                .block_on(context.session.open_options().open_path(&path))
                .unwrap();
            let statistics = file.footer().statistics().unwrap();
            let (stats, _) = statistics.get(0);
            assert_eq!(
                stats.get(Stat::Min).as_exact(),
                Scalar::from(i32::MIN).into_value()
            );
            assert_eq!(
                stats.get(Stat::Max).as_exact(),
                Scalar::from(i32::MAX).into_value()
            );
            assert_eq!(
                stats.get(Stat::NullCount).as_exact(),
                Scalar::from(1_u64).into_value()
            );
        }
    });
}
