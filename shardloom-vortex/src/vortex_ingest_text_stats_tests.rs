//! Text compression must retain file pruning metadata without eagerly computing
//! temporary sortedness statistics that the selected Zstd encoding discards.

use super::*;
use vortex::{
    array::{
        ArrayRef, IntoArray as _, VortexSessionExecute as _,
        arrays::{PrimitiveArray, VarBinArray, VarBinViewArray},
        dtype::{DType, Nullability},
        expr::stats::Stat,
        iter::ArrayIteratorAdapter,
        scalar::Scalar,
    },
    file::{OpenOptionsSessionExt as _, VortexFile, WriteOptionsSessionExt as _},
};

fn write_text(input: &ArrayRef, context: &LocalVortexWriteContext) -> VortexFile {
    let strategy =
        large_source_fast_zstd_text_leaf_strategy(3, 1, &VortexWriterStageTiming::default());
    let mut bytes = Vec::new();
    context
        .session
        .write_options()
        .with_strategy(strategy)
        .blocking(&context.runtime)
        .write(
            &mut bytes,
            ArrayIteratorAdapter::new(input.dtype().clone(), [Ok(input.clone())].into_iter()),
        )
        .unwrap();
    assert!(bytes.len() < 64 * 1024);
    context.session.open_options().open_buffer(bytes).unwrap()
}

#[test]
fn selected_text_codec_does_not_compute_discarded_sortedness() {
    let input = VarBinArray::from_iter(
        [
            Some("zzzzzzzzzzzzzzzz"),
            None,
            Some(""),
            Some("東京-λ"),
            Some("a"),
        ],
        DType::Utf8(Nullability::Nullable),
    )
    .into_array();
    let context = LocalVortexWriteContext::open();
    let _file = write_text(&input, &context);
    assert!(
        input
            .statistics()
            .to_owned()
            .get(Stat::IsSorted)
            .is_absent()
    );
    assert!(
        input
            .statistics()
            .to_owned()
            .get(Stat::IsStrictSorted)
            .is_absent()
    );
}

#[test]
fn selected_text_codec_preserves_non_text_passthrough_statistics() {
    let values = [3_i64, 1, 2];
    let input = PrimitiveArray::from_iter(values).into_array();
    let context = LocalVortexWriteContext::open();
    let file = write_text(&input, &context);
    assert_eq!(
        input.statistics().to_owned().get(Stat::IsSorted).as_exact(),
        Scalar::from(false).into_value(),
        "passthrough arrays keep the provider's original pre-compression statistics",
    );
    assert_eq!(file.dtype(), input.dtype());
    assert_eq!(file.row_count(), 3);
    let stats = &file.footer().statistics().unwrap().stats_sets()[0];
    for (stat, expected) in [(Stat::Min, 1_i64), (Stat::Max, 3), (Stat::Sum, 6)] {
        assert_eq!(
            stats.get(stat).as_exact(),
            Scalar::from(expected).into_value()
        );
    }
    assert_eq!(
        stats.get(Stat::NullCount).as_exact(),
        Scalar::from(0_u64).into_value(),
    );
    let mut execution = context.session.create_execution_ctx();
    let mut seen = 0;
    for array in file
        .scan()
        .unwrap()
        .with_ordered(true)
        .into_array_iter(&context.runtime)
        .unwrap()
    {
        let array = array.unwrap();
        for row in 0..array.len() {
            assert_eq!(
                array.execute_scalar(row, &mut execution).unwrap(),
                Scalar::from(values[seen]),
            );
            seen += 1;
        }
    }
    assert_eq!(seen, values.len());
}

#[test]
fn selected_text_codec_preserves_values_and_file_pruning_statistics() {
    let context = LocalVortexWriteContext::open();
    for values in [
        vec![],
        vec![None, None, None],
        vec![Some("")],
        vec![Some("a"), Some("b"), Some("c")],
        vec![
            Some("zzzzzzzzzzzzzzzz"),
            None,
            Some(""),
            Some("東京-λ"),
            Some("a"),
            Some("a"),
        ],
    ] {
        for views in [false, true] {
            let dtype = DType::Utf8(Nullability::Nullable);
            let input = if views {
                VarBinViewArray::from_iter(values.iter().copied(), dtype.clone()).into_array()
            } else {
                VarBinArray::from_iter(values.iter().copied(), dtype.clone()).into_array()
            };
            let file = write_text(&input, &context);
            assert_eq!(file.dtype(), &dtype);
            assert_eq!(file.row_count(), u64::try_from(values.len()).unwrap());
            if !values.is_empty() {
                let stats = &file.footer().statistics().unwrap().stats_sets()[0];
                assert_eq!(
                    stats.get(Stat::NullCount).as_exact(),
                    Scalar::from(
                        u64::try_from(values.iter().filter(|v| v.is_none()).count()).unwrap()
                    )
                    .into_value(),
                );
                for (stat, expected) in [
                    (Stat::Min, values.iter().flatten().min()),
                    (Stat::Max, values.iter().flatten().max()),
                ] {
                    if let Some(expected) = expected {
                        assert_eq!(
                            stats.get(stat).as_exact(),
                            Scalar::utf8(*expected, Nullability::Nullable).into_value()
                        );
                    }
                }
            }
            let mut execution = context.session.create_execution_ctx();
            let mut seen = 0;
            for array in file
                .scan()
                .unwrap()
                .with_ordered(true)
                .into_array_iter(&context.runtime)
                .unwrap()
            {
                let array = array.unwrap();
                for row in 0..array.len() {
                    let expected = values[seen].map_or_else(
                        || Scalar::null(dtype.clone()),
                        |value| Scalar::utf8(value, Nullability::Nullable),
                    );
                    assert_eq!(array.execute_scalar(row, &mut execution).unwrap(), expected);
                    seen += 1;
                }
            }
            assert_eq!(seen, values.len());
        }
    }
}
