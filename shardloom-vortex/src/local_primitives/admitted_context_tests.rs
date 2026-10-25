//! The helper must keep both the allocator and provider registry of its caller.

use super::{aggregate_column_accessor_in_context, aggregate_direct_stat_value};
use crate::owned_buffers::ReservedHostAllocator;
use shardloom_core::{ColumnRef, ComparisonOp, ScalarValue, StatValue};
use shardloom_exec::live_memory::LiveMemoryPool;
use std::sync::Arc;
use vortex::{
    VortexSessionDefault as _,
    array::{
        ArrayRef, ExecutionCtx, IntoArray as _, VortexSessionExecute as _,
        arrays::{
            BoolArray, ChunkedArray, DictArray, FixedSizeListArray, ListViewArray, PrimitiveArray,
            StructArray, VarBinViewArray,
        },
        memory::HostMemorySessionExt as _,
        validity::Validity,
    },
    buffer::BitBuffer,
    encodings::zstd::{Zstd, ZstdData},
    session::VortexSession,
};

fn session(memory: &LiveMemoryPool) -> VortexSession {
    let session = VortexSession::default()
        .with_host_allocator(Arc::new(ReservedHostAllocator::new(memory.clone())));
    crate::native_provider_memory::install(&session, memory.clone());
    session
}

fn compressed(values: &PrimitiveArray) -> ArrayRef {
    let data = ZstdData::from_primitive_without_dict(
        values,
        0,
        8,
        &mut VortexSession::default().create_execution_ctx(),
    )
    .unwrap();
    Zstd::try_new(values.dtype().clone(), data, values.validity().unwrap())
        .unwrap()
        .into_array()
}

fn encoded_validity(codes: &[u32]) -> Validity {
    // A scalar validity lookup must decode the codes. A chunked canonical Bool
    // fixture alone would select a child without allocating and miss the defect.
    Validity::Array(
        DictArray::try_new(
            compressed(&PrimitiveArray::from_iter(codes.iter().copied())),
            BoolArray::from_iter([true, false]).into_array(),
        )
        .unwrap()
        .into_array(),
    )
}

fn dictionary(codes: ArrayRef, values: ArrayRef) -> ArrayRef {
    DictArray::try_new(codes, values).unwrap().into_array()
}

fn assert_aggregate_admission(input: &ArrayRef, expected: &[StatValue]) {
    let budget = 16 << 20;
    let memory = LiveMemoryPool::new(budget).unwrap();
    let mut ctx = session(&memory).create_execution_ctx();
    let pressure = memory.reserve(budget - 1).unwrap();
    let error = aggregate_column_accessor_in_context("renamed_value", input, &mut ctx)
        .err()
        .expect("native helper must refuse execution with one byte available");
    assert!(
        error.to_string().contains("memory reservation denied"),
        "{error}"
    );
    assert!(memory.snapshot().denied_reservations > 0);
    assert_eq!(memory.snapshot().reserved_bytes, budget - 1);
    drop(pressure);
    assert_eq!(memory.snapshot().reserved_bytes, 0);

    // Retry the same array and execution context after competing work releases
    // credit. A cached failure or a replaced session must not affect the result.
    let (accessor, _) =
        aggregate_column_accessor_in_context("renamed_value", input, &mut ctx).unwrap();
    let actual = (0..input.len())
        .map(|row| aggregate_direct_stat_value(&accessor, row).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(actual, expected);
    drop(accessor);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn admitted_context_boolean_materialization_refuses_and_retries() {
    let first = BoolArray::from_iter([true, false]).into_array();
    let dtype = first.dtype().clone();
    let input = ChunkedArray::try_new(
        vec![first, BoolArray::from_iter([false, true]).into_array()],
        dtype,
    )
    .unwrap()
    .into_array();
    assert_aggregate_admission(&input, &[true, false, false, true].map(StatValue::Boolean));
}

#[test]
fn admitted_context_numeric_dictionary_values_refuse_and_retry() {
    let input = dictionary(
        PrimitiveArray::from_iter([1u32, 0, 1, 0]).into_array(),
        compressed(&PrimitiveArray::from_iter([11i64, -7])),
    );
    assert_aggregate_admission(&input, &[-7, 11, -7, 11].map(StatValue::Int64));
}

#[test]
fn admitted_context_numeric_dictionary_codes_refuse_and_retry() {
    let input = dictionary(
        compressed(&PrimitiveArray::from_iter([1u32, 0, 1, 0])),
        PrimitiveArray::from_iter([11i64, -7]).into_array(),
    );
    assert_aggregate_admission(&input, &[-7, 11, -7, 11].map(StatValue::Int64));
}

#[test]
fn admitted_context_utf8_dictionary_codes_refuse_and_retry() {
    let input = dictionary(
        compressed(&PrimitiveArray::from_iter([1u32, 0, 1, 0])),
        VarBinViewArray::from_iter_str(["first", "λ-second"]).into_array(),
    );
    assert_aggregate_admission(
        &input,
        &["λ-second", "first", "λ-second", "first"].map(|s| StatValue::Utf8(s.into())),
    );
}

#[test]
fn admitted_context_dictionary_value_validity_refuses_and_retries() {
    let input = dictionary(
        PrimitiveArray::from_iter([1u32, 0, 1, 0]).into_array(),
        PrimitiveArray::new(vec![11i64, -7], encoded_validity(&[0, 1])).into_array(),
    );
    assert_aggregate_admission(
        &input,
        &[
            StatValue::Null,
            StatValue::Int64(11),
            StatValue::Null,
            StatValue::Int64(11),
        ],
    );
}

#[test]
fn admitted_context_dictionary_code_validity_refuses_and_retries() {
    let input = dictionary(
        PrimitiveArray::new(vec![1u32, 0, 1, 0], encoded_validity(&[0, 1, 0, 0])).into_array(),
        PrimitiveArray::from_iter([11i64, -7]).into_array(),
    );
    assert_aggregate_admission(
        &input,
        &[
            StatValue::Int64(-7),
            StatValue::Null,
            StatValue::Int64(-7),
            StatValue::Int64(11),
        ],
    );
}

#[test]
fn admitted_context_boolean_validity_refuses_and_retries() {
    let input = BoolArray::new(BitBuffer::new_set(4), encoded_validity(&[0, 1, 0, 0])).into_array();
    assert_aggregate_admission(
        &input,
        &[
            StatValue::Boolean(true),
            StatValue::Null,
            StatValue::Boolean(true),
            StatValue::Boolean(true),
        ],
    );
}

#[test]
fn admitted_context_native_numeric_validity_keeps_existing_admission() {
    let input =
        PrimitiveArray::new(vec![1i64, -2, 3, -4], encoded_validity(&[0, 1, 0, 0])).into_array();
    assert_aggregate_admission(
        &input,
        &[
            StatValue::Int64(1),
            StatValue::Null,
            StatValue::Int64(3),
            StatValue::Int64(-4),
        ],
    );
}

fn assert_helper_admission<T: PartialEq + std::fmt::Debug>(
    mut operation: impl FnMut(&mut ExecutionCtx) -> shardloom_core::Result<T>,
    expected: &T,
) {
    let budget = 16 << 20;
    let memory = LiveMemoryPool::new(budget).unwrap();
    let mut ctx = session(&memory).create_execution_ctx();
    let pressure = memory.reserve(budget - 1).unwrap();
    let error = operation(&mut ctx).expect_err("one remaining byte must not admit decoding");
    assert!(
        error.to_string().contains("memory reservation denied"),
        "{error}"
    );
    assert!(memory.snapshot().denied_reservations > 0);
    assert_eq!(memory.snapshot().reserved_bytes, budget - 1);
    drop(pressure);
    let actual = operation(&mut ctx).unwrap();
    assert_eq!(&actual, expected);
    drop(actual);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

fn export_cases() -> Vec<(ArrayRef, Vec<StatValue>)> {
    let text = VarBinViewArray::from_iter_str(["港-first", "λ-second"]);
    let encoded_text = Zstd::from_var_bin_view_without_dict(
        &text,
        0,
        4,
        &mut VortexSession::default().create_execution_ctx(),
    )
    .unwrap()
    .into_array();
    vec![
        (
            compressed(&PrimitiveArray::from_iter([11i64, -7])),
            [11, -7].map(StatValue::Int64).to_vec(),
        ),
        (
            encoded_text,
            ["港-first", "λ-second"]
                .map(|s| StatValue::Utf8(s.into()))
                .to_vec(),
        ),
        (
            BoolArray::new(BitBuffer::new_set(2), encoded_validity(&[0, 1])).into_array(),
            vec![StatValue::Boolean(true), StatValue::Null],
        ),
        (
            PrimitiveArray::new(vec![11i64, -7], encoded_validity(&[0, 1])).into_array(),
            vec![StatValue::Int64(11), StatValue::Null],
        ),
    ]
}

#[test]
fn admitted_context_full_row_export_preserves_provider_errors_and_values() {
    for (input, expected) in export_cases() {
        let chunk = StructArray::new(["renamed"].into(), vec![input], 2, Validity::NonNullable)
            .into_array();
        assert_helper_admission(
            |ctx| super::row_export_columns_from_chunk(&chunk, &["renamed".into()], ctx),
            &vec![expected],
        );
    }
}

#[test]
fn admitted_context_selected_row_export_preserves_order_duplicates_and_nulls() {
    for (input, expected) in export_cases() {
        let selected = vec![
            expected[1].clone(),
            expected[0].clone(),
            expected[1].clone(),
        ];
        assert_helper_admission(
            |ctx| {
                super::row_export_selected_columns_from_chunk(
                    &input,
                    &["renamed".into()],
                    &[1, 0, 1],
                    ctx,
                )
            },
            &vec![selected],
        );
    }
}

fn assert_predicate_fast_admission(
    input: &ArrayRef,
    predicate: &super::MaterializedPredicateEvaluator,
) {
    let columns = ["renamed".into()];
    assert_helper_admission(
        |ctx| predicate.fast_count_matches_in_chunk(input, &columns, ctx),
        &Some(2),
    );
    assert_helper_admission(
        |ctx| predicate.fast_candidate_row_indices_in_chunk(input, &columns, ctx),
        &Some(vec![1, 3]),
    );
    assert_helper_admission(
        |ctx| predicate.fast_exact_candidate_row_indices_in_chunk(input, &columns, ctx),
        &Some(vec![1, 3]),
    );
}

fn assert_predicate_admission(input: &ArrayRef, predicate: &super::MaterializedPredicateEvaluator) {
    assert_predicate_fast_admission(input, predicate);
    let columns = ["renamed".into()];
    assert_helper_admission(
        |ctx| {
            predicate.filter_column_scoped_candidate_row_indices_in_chunk(
                input,
                &columns,
                &[0, 1, 2, 3],
                ctx,
            )
        },
        &Some(vec![1, 3]),
    );
    assert_helper_admission(
        |ctx| {
            predicate.filter_materialized_candidate_row_indices_in_chunk(
                input,
                &columns,
                &[0, 1, 2, 3],
                ctx,
            )
        },
        &vec![1, 3],
    );
}

#[test]
fn admitted_context_numeric_predicates_keep_owner_through_refinement() {
    let input = dictionary(
        compressed(&PrimitiveArray::from_iter([1u32, 0, 1, 0])),
        PrimitiveArray::from_iter([11i64, -7]).into_array(),
    );
    assert_predicate_admission(
        &input,
        &super::MaterializedPredicateEvaluator::Compare {
            column: "renamed".into(),
            column_index: 0,
            op: ComparisonOp::Gt,
            value: StatValue::Int64(0),
        },
    );
    assert_predicate_admission(
        &input,
        &super::MaterializedPredicateEvaluator::InList {
            column: "renamed".into(),
            column_index: 0,
            values: vec![StatValue::Int64(11)],
            negated: false,
        },
    );
}

#[test]
fn admitted_context_text_predicates_keep_owner_through_refinement() {
    let input = dictionary(
        compressed(&PrimitiveArray::from_iter([1u32, 0, 1, 0])),
        VarBinViewArray::from_iter_str(["matched-λ", "absent"]).into_array(),
    );
    assert_predicate_admission(
        &input,
        &super::MaterializedPredicateEvaluator::StringContains {
            column: "renamed".into(),
            column_index: 0,
            needle: "λ".into(),
            negated: false,
        },
    );
}

#[test]
fn admitted_context_null_predicates_keep_encoded_validity_owner() {
    let input =
        PrimitiveArray::new(vec![11i64, -7, 3, 5], encoded_validity(&[0, 1, 0, 1])).into_array();
    assert_predicate_admission(
        &input,
        &super::MaterializedPredicateEvaluator::IsNull { column_index: 0 },
    );
}

#[test]
fn admitted_context_conjunction_keeps_owner_after_narrowing_candidates() {
    use super::MaterializedPredicateEvaluator as Predicate;
    let text = dictionary(
        compressed(&PrimitiveArray::from_iter([1u32, 0, 1, 0])),
        VarBinViewArray::from_iter_str(["matched-λ", "absent"]).into_array(),
    );
    let chunk = StructArray::new(
        ["selector", "renamed"].into(),
        vec![
            PrimitiveArray::from_iter([0u32, 1, 0, 1]).into_array(),
            text,
        ],
        4,
        Validity::NonNullable,
    )
    .into_array();
    let predicate = Predicate::And(vec![
        Predicate::Compare {
            column: "selector".into(),
            column_index: 0,
            op: ComparisonOp::Eq,
            value: StatValue::UInt64(1),
        },
        Predicate::StringContains {
            column: "renamed".into(),
            column_index: 1,
            needle: "λ".into(),
            negated: false,
        },
    ]);
    assert!(super::predicate_candidate_materialization_admitted(2, 4));
    assert_helper_admission(
        |ctx| {
            predicate.fast_exact_candidate_row_indices_in_chunk(
                &chunk,
                &["selector".into(), "renamed".into()],
                ctx,
            )
        },
        &Some(vec![1, 3]),
    );
}

#[test]
fn admitted_context_fsst_predicate_and_mask_keep_encoded_offsets_owner() {
    use vortex::array::{
        arrays::{FilterArray, VarBinArray, varbin::VarBinArraySlotsExt as _},
        dtype::{DType, Nullability},
    };
    use vortex::encodings::fsst::{
        FSST, FSSTArrayExt as _, FSSTArraySlotsExt as _, fsst_compress, fsst_train_compressor,
    };
    use vortex::mask::Mask;

    let mut fixture_ctx = VortexSession::default().create_execution_ctx();
    let values = VarBinArray::from_iter(
        ["absent", "matched-λ", "absent", "matched-λ"].map(Some),
        DType::Utf8(Nullability::NonNullable),
    )
    .into_array();
    let compressor = fsst_train_compressor(&values, &mut fixture_ctx).unwrap();
    let fsst = fsst_compress(&values, &compressor, &mut fixture_ctx).unwrap();
    let codes = fsst.codes();
    let offsets = codes
        .offsets()
        .clone()
        .execute::<PrimitiveArray>(&mut fixture_ctx)
        .unwrap();
    let codes = VarBinArray::new(
        compressed(&offsets),
        codes.bytes().clone(),
        codes.dtype().clone(),
        codes.validity().unwrap(),
        &mut vortex::array::VortexSessionExecute::create_execution_ctx(
            &vortex::array::array_session(),
        ),
    );
    let encoded = FSST::try_new_with_symbol_table(
        fsst.dtype().clone(),
        fsst.data().symbol_table(),
        codes,
        fsst.uncompressed_lengths().clone(),
        &mut fixture_ctx,
    )
    .unwrap()
    .into_array();
    let predicate = super::MaterializedPredicateEvaluator::StringContains {
        column: "renamed".into(),
        column_index: 0,
        needle: "λ".into(),
        negated: false,
    };
    // The encoded DFA path needs only offsets and can use the admitted Zstd
    // provider. Full FSST materialization has a narrower metadata contract.
    assert_predicate_fast_admission(&encoded, &predicate);
    let memory = LiveMemoryPool::new(16 << 20).unwrap();
    let mut ctx = session(&memory).create_execution_ctx();
    let error = predicate
        .filter_materialized_candidate_row_indices_in_chunk(
            &encoded,
            &["renamed".into()],
            &[0, 1, 2, 3],
            &mut ctx,
        )
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("metadata decoder vortex.zstd has no admitted allocation bound"),
        "{error}"
    );
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    let error = predicate
        .filter_column_scoped_candidate_row_indices_in_chunk(
            &encoded,
            &["renamed".into()],
            &[0, 1, 2, 3],
            &mut ctx,
        )
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("metadata decoder vortex.zstd has no admitted allocation bound"),
        "{error}"
    );
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    let filtered =
        FilterArray::new(encoded, Mask::from_iter([true, true, false, true])).into_array();
    assert_helper_admission(
        |ctx| predicate.fast_count_matches_in_chunk(&filtered, &["renamed".into()], ctx),
        &Some(2),
    );
    assert_helper_admission(
        |ctx| predicate.fast_candidate_row_indices_in_chunk(&filtered, &["renamed".into()], ctx),
        &Some(vec![1, 2]),
    );
}

fn list_fixture(encoded_part: &str) -> ArrayRef {
    let values = PrimitiveArray::from_iter([11i64, -7, 3, 5]);
    let offsets = PrimitiveArray::from_iter([0u32, 2]);
    let sizes = PrimitiveArray::from_iter([2u32, 2]);
    let values = if encoded_part == "values" {
        compressed(&values)
    } else {
        values.into_array()
    };
    let offsets = if encoded_part == "offsets" {
        compressed(&offsets)
    } else {
        offsets.into_array()
    };
    let sizes = if encoded_part == "sizes" {
        compressed(&sizes)
    } else {
        sizes.into_array()
    };
    let validity = if encoded_part == "validity" {
        encoded_validity(&[0, 1])
    } else {
        Validity::NonNullable
    };
    ListViewArray::new(
        values,
        offsets,
        sizes,
        validity,
        &mut vortex::array::VortexSessionExecute::create_execution_ctx(
            &vortex::array::array_session(),
        ),
    )
    .into_array()
}

fn assert_list_admission(input: &ArrayRef, parent_nullable: bool) {
    let elements = vec![
        vec![ScalarValue::Int64(11), ScalarValue::Int64(-7)],
        if parent_nullable {
            vec![ScalarValue::Null]
        } else {
            vec![ScalarValue::Int64(3), ScalarValue::Int64(5)]
        },
    ];
    assert_helper_admission(
        |ctx| super::list_element_rows_from_vortex_array("renamed", input, ctx),
        &elements,
    );
    assert_helper_admission(
        |ctx| super::scalar_values_from_vortex_array("renamed", input, ctx),
        &elements
            .into_iter()
            .map(ScalarValue::List)
            .collect::<Vec<_>>(),
    );
    assert_helper_admission(
        |ctx| super::row_key_values_from_vortex_array("renamed", input, ctx),
        &vec![
            "l:2:4:i:11;4:i:-7;".to_owned(),
            if parent_nullable {
                "l:parent-null"
            } else {
                "l:2:3:i:3;3:i:5;"
            }
            .to_owned(),
        ],
    );
}

#[test]
fn admitted_context_list_offsets_and_sizes_refuse_and_retry() {
    for part in ["offsets", "sizes"] {
        assert_list_admission(&list_fixture(part), false);
    }
}

#[test]
fn admitted_context_list_values_and_validity_refuse_and_retry() {
    for part in ["values", "validity"] {
        assert_list_admission(&list_fixture(part), part == "validity");
    }
}

#[test]
fn admitted_context_null_and_empty_lists_do_not_decode_unused_offsets() {
    let memory = LiveMemoryPool::new(1).unwrap();
    let mut ctx = session(&memory).create_execution_ctx();
    for count in [0, 2] {
        let input = ListViewArray::new(
            PrimitiveArray::from_iter([11i64, -7]).into_array(),
            compressed(&PrimitiveArray::from_iter(vec![0u32; count])),
            compressed(&PrimitiveArray::from_iter(vec![1u32; count])),
            Validity::AllInvalid,
            &mut vortex::array::VortexSessionExecute::create_execution_ctx(
                &vortex::array::array_session(),
            ),
        )
        .into_array();
        assert_eq!(
            super::list_element_rows_from_vortex_array("renamed", &input, &mut ctx).unwrap(),
            vec![vec![ScalarValue::Null]; count],
        );
        assert_eq!(
            super::row_key_values_from_vortex_array("renamed", &input, &mut ctx).unwrap(),
            vec!["l:parent-null".to_owned(); count],
        );
    }
    assert_eq!(memory.snapshot().denied_reservations, 0);
    assert_eq!(memory.snapshot().peak_reserved_bytes, 0);
}

#[test]
fn admitted_context_fixed_list_values_and_validity_refuse_and_retry() {
    let values = PrimitiveArray::from_iter([11i64, -7, 3, 5]);
    let encoded =
        FixedSizeListArray::new(compressed(&values), 2, Validity::NonNullable, 2).into_array();
    assert_list_admission(&encoded, false);
    let nullable =
        FixedSizeListArray::new(values.into_array(), 2, encoded_validity(&[0, 1]), 2).into_array();
    assert_list_admission(&nullable, true);
}

#[test]
fn admitted_context_struct_validity_and_nested_child_keep_owner() {
    let nullable = StructArray::new(
        ["metric"].into(),
        vec![PrimitiveArray::from_iter([11i64, -7]).into_array()],
        2,
        encoded_validity(&[0, 1]),
    )
    .into_array();
    assert_helper_admission(
        |ctx| super::scalar_values_from_vortex_array("renamed", &nullable, ctx),
        &vec![
            ScalarValue::Struct(vec![("metric".into(), ScalarValue::Int64(11))]),
            ScalarValue::Null,
        ],
    );
    assert_helper_admission(
        |ctx| super::row_key_values_from_vortex_array("renamed", &nullable, ctx),
        &vec![
            "r:1:6:metric=4:i:11;".to_owned(),
            "r:parent-null".to_owned(),
        ],
    );
    let nested = StructArray::new(
        ["items"].into(),
        vec![list_fixture("values")],
        2,
        Validity::NonNullable,
    )
    .into_array();
    assert_helper_admission(
        |ctx| super::scalar_values_from_vortex_array("renamed", &nested, ctx),
        &vec![
            ScalarValue::Struct(vec![(
                "items".into(),
                ScalarValue::List(vec![ScalarValue::Int64(11), ScalarValue::Int64(-7)]),
            )]),
            ScalarValue::Struct(vec![(
                "items".into(),
                ScalarValue::List(vec![ScalarValue::Int64(3), ScalarValue::Int64(5)]),
            )]),
        ],
    );
}

#[test]
fn admitted_context_shape_only_aggregate_cannot_decode_without_owner() {
    let request = crate::VortexSimpleAggregateRequest::grouped(
        vec![ColumnRef::new("renamed").unwrap()],
        vec![crate::VortexSimpleAggregateMeasure::new(
            "count",
            None,
            "rows".into(),
        )],
    );
    let columns = vec!["renamed".into()];
    let mut states = super::GroupedAggregateStates::new_with_resource_envelope(
        &request,
        None,
        &columns,
        false,
        false,
        super::VortexLocalPrimitiveResourceEnvelope::new(4, 2).unwrap(),
        None,
    )
    .unwrap();
    let input = compressed(&PrimitiveArray::from_iter([11i64, -7]));
    let error = states
        .update_count_star_direct_from_chunk(&input, &columns, None)
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("requires its admitted execution context"),
        "{error}"
    );
}

#[test]
fn admitted_context_materialization_reason_never_decodes_corrupt_text() {
    let text = VarBinViewArray::from_iter_str(["long enough text to require a compressed frame"]);
    let input = Zstd::from_var_bin_view_without_dict(
        &text,
        0,
        4,
        &mut VortexSession::default().create_execution_ctx(),
    )
    .unwrap()
    .into_array();
    let mut parts = input
        .as_::<Zstd>()
        .data()
        .clone()
        .into_parts(Validity::NonNullable);
    let frame = &parts.frames[0];
    parts.frames[0] = frame.slice(..frame.len() - 1);
    let corrupt = Zstd::try_new(
        input.dtype().clone(),
        ZstdData::new(parts.dictionary, parts.frames, parts.metadata, parts.n_rows),
        parts.validity,
    )
    .unwrap()
    .into_array();
    assert_eq!(
        super::aggregate_direct_materialization_blocker(&corrupt),
        "cg21.aggregate_accessor.utf8_chunk_dictionary_provider_miss",
    );
    let memory = LiveMemoryPool::new(16 << 20).unwrap();
    let error = super::row_export_values_from_vortex_array(
        "renamed",
        &corrupt,
        &mut session(&memory).create_execution_ctx(),
    )
    .unwrap_err();
    assert!(!error.to_string().contains("unsupported dtype"), "{error}");
    assert_eq!(memory.snapshot().denied_reservations, 0);
    assert!(memory.snapshot().peak_reserved_bytes > 0);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}
