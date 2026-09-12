use super::*;
use crate::{local_primitives as runtime, resident_session::ResidentVortexSession};
use shardloom_core::{ComparisonOp, PredicateExpr, StatValue};
use vortex::array::arrays::{
    DictArray, Primitive, Struct, VarBin, VarBinViewArray, struct_::StructArrayExt as _,
    varbin::VarBinArraySlotsExt as _,
};

fn text_request(
    path: &Path,
    offset: usize,
    limit: usize,
    explicit_tie: bool,
) -> VortexQueryPrimitiveRequest {
    let mut order = vec![VortexAggregateOrderExpr::new(COUNT, true)];
    if explicit_tie {
        order.push(VortexAggregateOrderExpr::new(KEY, false));
    }
    VortexQueryPrimitiveRequest::simple_aggregate(
        DatasetUri::new(path.display().to_string()).unwrap(),
        VortexSimpleAggregateRequest::grouped(
            vec![ColumnRef::new(KEY).unwrap()],
            vec![VortexSimpleAggregateMeasure::new(
                "count",
                None,
                COUNT.into(),
            )],
        )
        .with_order_by(order)
        .with_offset(offset),
    )
    .with_source_order_limit(limit)
}

fn strings(values: &[&str]) -> ArrayRef {
    VarBinViewArray::from_iter_str(values.iter().copied()).into_array()
}

fn text_source(fixture: &Fixture, batches: Vec<ArrayRef>) -> PathBuf {
    let path = fixture.0.join("source.vortex");
    let runtime =
        runtime::local_vortex_runtime(VortexLocalPrimitiveExecutionPolicy::single_threaded());
    let session = VortexSession::default().with_handle(runtime.handle());
    let dtype = DType::struct_(
        [(KEY, batches[0].dtype().clone())],
        Nullability::NonNullable,
    );
    let mut file = fs::File::create(&path).unwrap();
    let mut writer = session
        .write_options()
        .with_strategy(
            runtime::native_flat_layout::SequentialNativeFlatLayout::strategy(batches.len()),
        )
        .with_file_statistics(Vec::new())
        .blocking(&runtime)
        .writer(&mut file, dtype);
    for keys in batches {
        let rows = keys.len();
        writer
            .push(
                StructArray::new([KEY].into(), vec![keys], rows, Validity::NonNullable)
                    .into_array(),
            )
            .unwrap();
    }
    writer.finish().unwrap();
    path
}

fn text_oracle(values: &[&str], offset: usize, limit: usize) -> Vec<(String, u64)> {
    let mut counts = BTreeMap::<String, u64>::new();
    for key in values {
        *counts.entry((*key).to_owned()).or_default() += 1;
    }
    let mut rows = counts.into_iter().collect::<Vec<_>>();
    rows.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0)));
    rows.into_iter().skip(offset).take(limit).collect()
}

fn native_rows(array: &ArrayRef, key: &str, count: &str) -> Vec<(String, u64)> {
    let fields = array.as_opt::<Struct>().unwrap();
    assert_eq!(
        fields.struct_fields().field(key),
        Some(DType::Utf8(Nullability::NonNullable))
    );
    assert_eq!(
        fields.struct_fields().field(count),
        Some(DType::Primitive(PType::U64, Nullability::NonNullable))
    );
    let key_array = fields.unmasked_field_by_name(key).unwrap();
    let count_array = fields.unmasked_field_by_name(count).unwrap();
    let keys = key_array.as_opt::<VarBin>().unwrap();
    let offset_array = keys.offsets().as_opt::<Primitive>().unwrap();
    let offsets = offset_array.as_slice::<u64>();
    let counts_array = count_array.as_opt::<Primitive>().unwrap();
    let counts = counts_array.as_slice::<u64>();
    assert_eq!(offsets.len(), array.len() + 1);
    assert_eq!(offsets[0], 0);
    assert_eq!(offsets.last().copied(), Some(keys.bytes().len() as u64));
    assert_eq!(counts.len(), array.len());
    offsets
        .windows(2)
        .zip(counts)
        .map(|(offsets, count)| {
            let start = usize::try_from(offsets[0]).unwrap();
            let end = usize::try_from(offsets[1]).unwrap();
            let bytes = &keys.bytes()[start..end];
            (std::str::from_utf8(bytes).unwrap().to_owned(), *count)
        })
        .collect()
}

fn payload(execution: &ExecutedVortexAggregate) -> serde_json::Value {
    serde_json::from_str(
        execution
            .report
            .result_summary
            .as_ref()
            .unwrap()
            .rsplit_once(" values=")
            .unwrap()
            .1,
    )
    .unwrap()
}

fn check_owned_execution(
    prepared: &PreparedVortexAggregate,
    expected: &[(String, u64)],
    rows: usize,
    worker_evidence: bool,
) {
    let memory = prepared.session.memory().clone();
    let baseline = memory.snapshot().reserved_bytes;
    let result = prepared.execute_owned().unwrap();
    let work = payload(&result.execution);
    assert!(work["values"].is_null());
    assert_eq!(
        work["aggregate_result_boundary"],
        "owned_native_utf8_columns;no_JSON_or_StatValue_output_rows"
    );
    assert_eq!(
        native_rows(&result.result.arrays()[0], KEY, COUNT),
        expected
    );
    assert!(result.execution.native_io_certificate.is_certified());
    assert!(!result.execution.report.fallback_execution_allowed);
    if worker_evidence {
        assert_eq!(work["aggregate_workers_rows"], rows);
        assert_eq!(work["aggregate_workers_outstanding_chunks"], 0);
        assert_eq!(
            work["aggregate_workers_submitted_chunks"],
            work["aggregate_workers_completed_chunks"]
        );
    }
    drop(result);
    assert_eq!(memory.snapshot().reserved_bytes, baseline);
}

#[test]
fn owned_utf8_count_complete_dictionary_domains_ties_offsets_and_native_pressure_match_oracle() {
    let fixture = Fixture::new();
    let values = [
        "東京",
        "",
        "é",
        "e\u{301}",
        "東京",
        "line\n\"\\\0",
        "é",
        "",
        "κόσμος",
    ];
    let dictionary = DictArray::try_new(
        PrimitiveArray::new(vec![0_u8, 1, 0, 2, 3, 1, 0], Validity::NonNullable).into_array(),
        strings(&["", "東京", "é", "e\u{301}"]),
    )
    .unwrap()
    .into_array()
    .slice(1..6)
    .unwrap();
    let second = DictArray::try_new(
        PrimitiveArray::new(vec![0_u8, 3, 1, 2], Validity::NonNullable).into_array(),
        strings(&["line\n\"\\\0", "", "κόσμος", "é"]),
    )
    .unwrap()
    .into_array();
    let path = text_source(&fixture, vec![dictionary, second]);
    for parallelism in [1, 2, 4] {
        for explicit_tie in [false, true] {
            for offset in [0, 1, 20] {
                let query = text_request(&path, offset, 5, explicit_tie);
                let prepared = prepare_aggregate(
                    &query,
                    VortexLocalPrimitiveExecutionPolicy::new(parallelism).unwrap(),
                )
                .unwrap();
                let memory = prepared.session.memory().clone();
                let expected = text_oracle(&values, offset, 5);
                for pressure in [false, true, false] {
                    runtime::aggregate_count_workers::ADMISSION_TEST_PRESSURE
                        .with(|flag| flag.set(pressure && !explicit_tie));
                    check_owned_execution(
                        &prepared,
                        &expected,
                        values.len(),
                        !pressure && !explicit_tie,
                    );
                }
                let ordinary = prepared.execute().unwrap();
                let expected = expected
                    .into_iter()
                    .map(|(key, count)| serde_json::json!({KEY:key, COUNT:count}))
                    .collect::<Vec<_>>();
                assert_eq!(payload(&ordinary)["values"], serde_json::json!(expected));
                assert_eq!(prepared.snapshot().prepared_source_opens, 1);
                assert_eq!(prepared.snapshot().completed_executions, 4);
                drop(ordinary);
                drop(prepared);
                assert_eq!(memory.snapshot().reserved_bytes, 0);
            }
        }
    }
}

#[test]
fn owned_utf8_count_completed_compact_and_general_states_keep_exact_text() {
    let query = text_request(Path::new("/not-opened.vortex"), 0, 3, false);
    let aggregate = query.simple_aggregate.as_ref().unwrap();
    let columns = vec![KEY.to_owned()];
    let dtype = DType::struct_(
        [(KEY, DType::Utf8(Nullability::NonNullable))],
        Nullability::NonNullable,
    );
    for compact in [false, true] {
        let mut states =
            runtime::GroupedAggregateStates::new(aggregate, Some(3), &columns, false, false)
                .unwrap();
        let input = vec![vec![
            StatValue::Utf8("東京".into()),
            StatValue::Utf8(String::new()),
            StatValue::Utf8("東京".into()),
        ]];
        if compact {
            for row in 0..3 {
                assert!(
                    states
                        .update_count_star_direct_from_materialized_columns(&input, row)
                        .unwrap()
                );
            }
        } else {
            states.update(&input, 3).unwrap();
        }
        let session = ResidentVortexSession::new(1 << 20, 1).unwrap();
        let mut output =
            runtime::aggregate_owned::OwnedAggregateFinalizer::new(&query, &dtype, &session)
                .unwrap();
        output.finish(&states).unwrap();
        let (array, ownership) = output.into_array().unwrap();
        assert_eq!(
            native_rows(&array, KEY, COUNT),
            vec![("東京".into(), 2), (String::new(), 1)]
        );
        drop((array, ownership));
        assert_eq!(session.memory().snapshot().reserved_bytes, 0);
    }
}

#[test]
fn owned_utf8_count_exact_interned_counts_keep_u64_and_reject_unfinished_sketches() {
    let query = text_request(Path::new("/not-opened.vortex"), 0, 3, false);
    let aggregate = query.simple_aggregate.as_ref().unwrap();
    let columns = vec![KEY.to_owned()];
    let dtype = DType::struct_(
        [(KEY, DType::Utf8(Nullability::NonNullable))],
        Nullability::NonNullable,
    );
    let mut states =
        runtime::GroupedAggregateStates::new(aggregate, Some(3), &columns, false, false).unwrap();
    let first = states.string_interner.intern("東京").unwrap();
    let second = states.string_interner.intern("").unwrap();
    states.string_count_topk_exact_counts = Some(
        [(first, u64::MAX), (second, (1_u64 << 53) + 1)]
            .into_iter()
            .collect(),
    );
    states.string_count_topk_first_pass_exact_counts = true;
    let session = ResidentVortexSession::new(1 << 20, 1).unwrap();
    let mut output =
        runtime::aggregate_owned::OwnedAggregateFinalizer::new(&query, &dtype, &session).unwrap();
    output.finish(&states).unwrap();
    let (array, ownership) = output.into_array().unwrap();
    assert_eq!(
        native_rows(&array, KEY, COUNT),
        vec![
            ("東京".into(), u64::MAX),
            (String::new(), (1_u64 << 53) + 1)
        ]
    );
    drop((array, ownership));
    states.string_count_topk_exact_counts = None;
    states.string_count_topk_heavy_hitter_enabled = true;
    states.string_count_topk_total_weight = 5;
    let mut output =
        runtime::aggregate_owned::OwnedAggregateFinalizer::new(&query, &dtype, &session).unwrap();
    assert!(
        output
            .finish(&states)
            .unwrap_err()
            .to_string()
            .contains("exact refinement")
    );
    drop(output);
    assert_eq!(session.memory().snapshot().reserved_bytes, 0);
}

#[test]
fn owned_utf8_count_cloned_children_and_slices_outlive_source_and_result() {
    let fixture = Fixture::new();
    let path = text_source(&fixture, vec![strings(&["東京", "é", "東京", "", "é"])]);
    let query = text_request(&path, 0, 3, false);
    let prepared =
        prepare_aggregate(&query, VortexLocalPrimitiveExecutionPolicy::new(4).unwrap()).unwrap();
    let memory = prepared.session.memory().clone();
    let result = prepared.execute_owned().unwrap();
    let array = result.result.arrays()[0].clone();
    let fields = array.as_opt::<Struct>().unwrap();
    assert_eq!(
        native_rows(&array, KEY, COUNT),
        text_oracle(&["東京", "é", "東京", "", "é"], 0, 3)
    );
    let child = fields.unmasked_field(0).clone();
    let retained_child = child.clone();
    assert!(ArrayRef::ptr_eq(&child, &retained_child));
    let slice = child.slice(1..3).unwrap();
    drop(prepared);
    fs::remove_file(path).unwrap();
    drop(result);
    drop(array);
    let bytes = child.nbytes();
    assert!(memory.snapshot().reserved_bytes >= bytes);
    drop(child);
    assert!(memory.snapshot().reserved_bytes >= bytes);
    drop(retained_child);
    assert!(memory.snapshot().reserved_bytes >= bytes);
    assert_eq!(slice.len(), 2);
    drop(slice);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn prepared_utf8_count_reuses_one_source_and_invalidates_filtered_and_empty_results() {
    for empty in [false, true] {
        let fixture = Fixture::new();
        let path = text_source(
            &fixture,
            vec![strings(if empty {
                &[]
            } else {
                &["a", "東京", "東京"]
            })],
        );
        let mut query = text_request(&path, 0, 4, false);
        query.predicate = Some(PredicateExpr::Compare {
            column: ColumnRef::new(KEY).unwrap(),
            op: ComparisonOp::Eq,
            value: StatValue::Utf8("東京".into()),
        });
        let disposition = prepare_aggregate_for_optional_reuse(
            &query,
            VortexLocalPrimitiveExecutionPolicy::new(2).unwrap(),
        )
        .unwrap()
        .unwrap();
        let PreparedAggregateDisposition::Reusable(prepared) = disposition else {
            panic!("admitted UTF8 COUNT must be retained");
        };
        let memory = prepared.session.memory().clone();
        for _ in 0..2 {
            let result = prepared.execute_owned().unwrap();
            assert_eq!(
                native_rows(&result.result.arrays()[0], KEY, COUNT),
                if empty {
                    vec![]
                } else {
                    vec![("東京".into(), 2)]
                }
            );
        }
        assert_eq!(prepared.snapshot().prepared_source_opens, 1);
        assert_eq!(prepared.snapshot().completed_executions, 2);
        fs::write(&path, b"changed generation").unwrap();
        assert!(
            prepared
                .execute_owned()
                .err()
                .unwrap()
                .to_string()
                .contains("prepared source changed")
        );
        drop(prepared);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn owned_utf8_count_empty_native_sink_retains_utf8_schema_and_zero_offset() {
    for values in [vec![], vec!["東京", "", "東京"]] {
        let fixture = Fixture::new();
        let path = text_source(&fixture, vec![strings(&values)]);
        let query = text_request(&path, 0, 3, false);
        let prepared =
            prepare_aggregate(&query, VortexLocalPrimitiveExecutionPolicy::new(2).unwrap())
                .unwrap();
        let result = prepared.execute_owned().unwrap();
        assert_eq!(
            native_rows(&result.result.arrays()[0], KEY, COUNT),
            text_oracle(&values, 0, 3)
        );
        drop(prepared);
        fs::remove_file(path).unwrap();
        let target = fixture.0.join("result.vortex");
        result
            .write(
                &target,
                runtime::VortexLocalPrimitiveRowExportFormat::Vortex,
                false,
            )
            .unwrap();
        let session = ResidentVortexSession::new(16 << 20, 1).unwrap();
        let source = session.prepare_file(&target).unwrap();
        assert_eq!(
            source.dtype().as_struct_fields_opt().unwrap().field(KEY),
            Some(DType::Utf8(Nullability::NonNullable))
        );
        let replay = source
            .prepare_projection(&[KEY, COUNT], 8, 64 << 10)
            .unwrap()
            .execute()
            .unwrap();
        let expected = text_oracle(&values, 0, 3)
            .into_iter()
            .map(|(key, count)| serde_json::json!({KEY:key, COUNT:count}))
            .collect::<Vec<_>>();
        assert_eq!(rendered(&replay), serde_json::json!(expected));
    }
}

#[test]
fn owned_utf8_count_admission_rejects_nullable_wrong_shape_and_unbounded_outputs() {
    let path = Path::new("/must-not-open-owned-utf8.vortex");
    let session = ResidentVortexSession::new(64 << 10, 1).unwrap();
    let dtype = DType::struct_(
        [(KEY, DType::Utf8(Nullability::NonNullable))],
        Nullability::NonNullable,
    );
    let query = text_request(path, 0, 3, false);
    for invalid in [
        DType::struct_(
            [(KEY, DType::Utf8(Nullability::Nullable))],
            Nullability::NonNullable,
        ),
        DType::struct_(
            [(KEY, DType::Utf8(Nullability::NonNullable))],
            Nullability::Nullable,
        ),
        DType::struct_(
            [(KEY, DType::Primitive(PType::F64, Nullability::NonNullable))],
            Nullability::NonNullable,
        ),
    ] {
        assert!(
            runtime::aggregate_owned::OwnedAggregateFinalizer::new(&query, &invalid, &session)
                .is_err()
        );
    }
    let mut cases = vec![
        text_request(path, 0, 0, false),
        text_request(path, 65_536, 1, false),
        text_request(path, usize::MAX, 1, false),
    ];
    for kind in [
        "count_column",
        "distinct",
        "measure",
        "key",
        "order",
        "alias",
        "unbounded",
    ] {
        let mut invalid = query.clone();
        let aggregate = invalid.simple_aggregate.as_mut().unwrap();
        match kind {
            "count_column" => aggregate.measures[0].column = Some(ColumnRef::new(KEY).unwrap()),
            "distinct" => {
                aggregate.measures[0].function = "count_distinct".into();
                aggregate.measures[0].column = Some(ColumnRef::new(KEY).unwrap());
            }
            "measure" => aggregate.measures.push(VortexSimpleAggregateMeasure::new(
                "count",
                None,
                "extra".into(),
            )),
            "key" => aggregate.group_by.push(ColumnRef::new("extra").unwrap()),
            "order" => aggregate.order_by[0].descending = false,
            "alias" => aggregate.measures[0].alias = KEY.into(),
            "unbounded" => invalid.source_order_limit = None,
            _ => unreachable!(),
        }
        cases.push(invalid);
    }
    let workspace = Fixture::new();
    let mut spill = query.clone();
    spill.simple_aggregate = Some(spill.simple_aggregate.take().unwrap().with_spill(
        crate::VortexAggregateSpillPolicy::new(&workspace.0, 16 << 20, 4 << 20).unwrap(),
    ));
    assert!(
        prepare_aggregate(&spill, VortexLocalPrimitiveExecutionPolicy::new(2).unwrap())
            .err()
            .unwrap()
            .to_string()
            .contains("explicit spill")
    );
    cases.push(spill);
    for invalid in cases {
        assert!(
            runtime::aggregate_owned::OwnedAggregateFinalizer::new(&invalid, &dtype, &session)
                .is_err()
        );
        assert!(!runtime::aggregate_owned::utf8_count_admitted(
            &invalid, &dtype
        ));
        assert_eq!(session.memory().snapshot().reserved_bytes, 0);
    }
}

#[test]
fn owned_utf8_count_complete_byte_bound_and_selection_denial_release_ownership() {
    let query = text_request(Path::new("/not-opened.vortex"), 0, 1, false);
    let columns = vec![KEY.into()];
    let aggregate = query.simple_aggregate.as_ref().unwrap();
    let dtype = DType::struct_(
        [(KEY, DType::Utf8(Nullability::NonNullable))],
        Nullability::NonNullable,
    );
    let session = ResidentVortexSession::new(1 << 20, 1).unwrap();
    let memory = session.memory().clone();
    let mut states =
        runtime::GroupedAggregateStates::new(aggregate, Some(1), &columns, false, false).unwrap();
    let id = states.string_interner.intern(&"x".repeat(8 << 20)).unwrap();
    states.string_count_topk_exact_counts = Some([(id, 1)].into_iter().collect());
    let mut output =
        runtime::aggregate_owned::OwnedAggregateFinalizer::new(&query, &dtype, &session).unwrap();
    assert!(
        output
            .finish(&states)
            .unwrap_err()
            .to_string()
            .contains("byte admission")
    );
    drop(output);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    let mut output =
        runtime::aggregate_owned::OwnedAggregateFinalizer::new(&query, &dtype, &session).unwrap();
    let baseline = memory.snapshot().reserved_bytes;
    let guard = memory
        .reserve(memory.snapshot().limit_bytes - baseline)
        .unwrap();
    assert!(output.finish(&states).is_err());
    drop(guard);
    drop(output);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn prepared_utf8_count_declines_nullable_schema_without_reopening() {
    let fixture = Fixture::new();
    let nullable =
        VarBinViewArray::from_iter_nullable_str([Some("x"), None, Some("x")]).into_array();
    let path = text_source(&fixture, vec![nullable]);
    let query = text_request(&path, 0, 3, false);
    let disposition = prepare_aggregate_for_optional_reuse(
        &query,
        VortexLocalPrimitiveExecutionPolicy::new(2).unwrap(),
    )
    .unwrap()
    .unwrap();
    let PreparedAggregateDisposition::Unretained(operation) = disposition else {
        panic!("nullable UTF8 must decline retained admission");
    };
    let session = operation.0.session.clone();
    let result = operation.execute().unwrap();
    assert!(result.native_io_certificate.is_certified());
    assert_eq!(session.snapshot().prepared_source_opens, 1);
    assert_eq!(session.snapshot().completed_executions, 1);
    assert_eq!(payload(&result)["rows"], 2);
    assert!(
        prepare_aggregate(&query, VortexLocalPrimitiveExecutionPolicy::new(2).unwrap()).is_err()
    );
}

#[test]
fn owned_utf8_count_native_constant_and_sliced_view_sources_preserve_logical_values() {
    for (keys, values) in [
        (
            vortex::array::arrays::ConstantArray::new("東京", 5).into_array(),
            vec!["東京"; 5],
        ),
        (
            strings(&["ignore", "東京", "", "東京", "ignore"])
                .slice(1..4)
                .unwrap(),
            vec!["東京", "", "東京"],
        ),
    ] {
        let fixture = Fixture::new();
        let path = text_source(&fixture, vec![keys]);
        let prepared = prepare_aggregate(
            &text_request(&path, 0, 3, false),
            VortexLocalPrimitiveExecutionPolicy::new(2).unwrap(),
        )
        .unwrap();
        let result = prepared.execute_owned().unwrap();
        assert_eq!(
            native_rows(&result.result.arrays()[0], KEY, COUNT),
            text_oracle(&values, 0, 3)
        );
        assert!(result.execution.native_io_certificate.is_certified());
    }
}

#[test]
fn owned_utf8_count_large_complete_output_uses_refined_partitions_and_completed_jobs() {
    for repeats in [8, 32] {
        check_large_complete_output(repeats);
    }
}

fn check_large_complete_output(repeats: u64) {
    let fixture = Fixture::new();
    let keys = (0..32_768)
        .map(|index| format!("renamed-東京-{index:05}-\"\\"))
        .collect::<Vec<_>>();
    let refs = keys.iter().map(String::as_str).collect::<Vec<_>>();
    // Below 1M rows the existing workers retain complete partitions. Above 1M
    // at this 4 GiB grant, existing serial heavy-hitter refinement is selected.
    // Both must produce every requested native output without JSON construction.
    let path = text_source(&fixture, (0..repeats).map(|_| strings(&refs)).collect());
    let query = text_request(&path, 0, keys.len(), false);
    let prepared =
        prepare_aggregate(&query, VortexLocalPrimitiveExecutionPolicy::new(4).unwrap()).unwrap();
    let memory = prepared.session.memory().clone();
    let baseline = memory.snapshot().reserved_bytes;
    let result = prepared.execute_owned().unwrap();
    let work = payload(&result.execution);
    let expected = keys
        .iter()
        .map(|key| (key.clone(), repeats))
        .collect::<Vec<_>>();
    assert_eq!(
        native_rows(&result.result.arrays()[0], KEY, COUNT),
        expected
    );
    assert_eq!(work["candidate_groups"], 32_768);
    if repeats == 8 {
        assert_eq!(work["aggregate_workers_rows"], repeats * 32_768);
        assert_eq!(work["aggregate_workers_outstanding_chunks"], 0);
        assert_eq!(
            work["aggregate_workers_submitted_chunks"],
            work["aggregate_workers_completed_chunks"]
        );
        assert!(work["aggregate_workers_completed_chunks"].as_u64().unwrap() >= repeats);
        assert_eq!(
            work["candidate_group_scope"],
            "complete_key_partition_global_groups"
        );
    } else {
        assert!(work["aggregate_workers_rows"].is_null());
        assert_eq!(
            work["candidate_group_scope"],
            "completed_exact_candidate_set;not_a_global_distinct_group_total"
        );
    }
    drop(result);
    assert_eq!(memory.snapshot().reserved_bytes, baseline);
    drop(prepared);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn owned_utf8_count_global_winner_and_typed_source_replay_preserve_complete_results() {
    use runtime::aggregate_count_workers::{SOURCE_SCAN_TEST_FAULT, SourceScanTestFault};
    const PARALLELISM: usize = 4;
    let fixture = Fixture::new();
    let mut all = Vec::new();
    let mut batches = Vec::new();
    // More chunks than the P4 pending window force a completed partial before
    // EOF, where the source-fault hook stops running. Three chunks could all
    // reach EOF before the first background job inserted any partition group.
    for local in ["first", "second", "third"]
        .into_iter()
        .cycle()
        .take(PARALLELISM * 2 + 1)
    {
        let mut rows = vec![local; 6];
        rows.extend(["winner"; 5]);
        all.extend(rows.iter().copied());
        batches.push(strings(&rows));
    }
    let path = text_source(&fixture, batches);
    let query = text_request(&path, 0, 1, false);
    for fault_kind in [
        None,
        Some(SourceScanTestFault::OwnedDenial),
        Some(SourceScanTestFault::CorruptionWithConcurrentDenial),
    ] {
        let corrupt = matches!(
            fault_kind,
            Some(SourceScanTestFault::CorruptionWithConcurrentDenial)
        );
        let prepared = prepare_aggregate(
            &query,
            VortexLocalPrimitiveExecutionPolicy::new(PARALLELISM).unwrap(),
        )
        .unwrap();
        let memory = prepared.session.memory().clone();
        let baseline = memory.snapshot().reserved_bytes;
        SOURCE_SCAN_TEST_FAULT.with(|fault| fault.set(fault_kind));
        let result = prepared.execute_owned();
        // Clear the thread-local even if a result assertion below fails.
        assert!(SOURCE_SCAN_TEST_FAULT.with(std::cell::Cell::take).is_none());
        if corrupt {
            assert!(result.err().unwrap().to_string().contains("corrupt"));
        } else {
            let result = result.unwrap();
            assert_eq!(
                native_rows(&result.result.arrays()[0], KEY, COUNT),
                text_oracle(&all, 0, 1)
            );
            assert!(result.execution.native_io_certificate.is_certified());
            assert_utf8_global_winner_work(
                &payload(&result.execution),
                all.len(),
                fault_kind.is_some(),
            );
        }
        assert_eq!(prepared.snapshot().prepared_source_opens, 1);
        assert_eq!(
            prepared.snapshot().completed_executions,
            u64::from(!corrupt)
        );
        assert_eq!(memory.snapshot().reserved_bytes, baseline);
        drop(prepared);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

fn assert_utf8_global_winner_work(work: &serde_json::Value, rows: usize, replayed: bool) {
    if replayed {
        assert_eq!(work["aggregate_workers_partition_source_replays"], 1);
        assert!(
            work["aggregate_provider_cpu_scope"]
                .as_str()
                .unwrap()
                .contains("no_concurrent_aggregate_worker_pool")
        );
    } else {
        assert_eq!(work["aggregate_workers_rows"], u64::try_from(rows).unwrap());
        assert_eq!(work["candidate_groups"], 4);
        assert_eq!(
            work["candidate_group_scope"],
            "complete_key_partition_global_groups"
        );
        assert_eq!(
            work["aggregate_workers_submitted_chunks"],
            work["aggregate_workers_completed_chunks"]
        );
        assert!(work["aggregate_workers_completed_chunks"].as_u64().unwrap() >= 9);
    }
}

#[test]
#[cfg(feature = "universal-format-io")]
fn owned_utf8_count_compatibility_sinks_keep_native_schema_and_all_values() {
    use runtime::VortexLocalPrimitiveRowExportFormat as Format;
    for format in [Format::ArrowIpc, Format::Parquet] {
        for empty in [false, true] {
            let fixture = Fixture::new();
            let path = text_source(
                &fixture,
                vec![strings(if empty { &[] } else { &["東京", "", "é", "東京", "é", "東京"] })],
            );
            let query = text_request(&path, 0, 3, false);
            let prepared =
                prepare_aggregate(&query, VortexLocalPrimitiveExecutionPolicy::new(2).unwrap())
                    .unwrap();
            let result = prepared.execute_owned().unwrap();
            let session = prepared.session.clone();
            drop(prepared);
            fs::remove_file(path).unwrap();
            let target = fixture.0.join(format.as_str());
            let report = result.write(&target, format, false).unwrap();
            assert_eq!(report.projected_columns, vec![KEY, COUNT]);
            assert_eq!(session.snapshot().completed_executions, 1);
            let batches = if format == Format::ArrowIpc {
                arrow_ipc::reader::FileReader::try_new(fs::File::open(&target).unwrap(), None)
                    .unwrap()
                    .collect::<std::result::Result<Vec<_>, _>>()
                    .unwrap()
            } else {
                parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
                    fs::File::open(&target).unwrap(),
                )
                .unwrap()
                .build()
                .unwrap()
                .collect::<std::result::Result<Vec<_>, _>>()
                .unwrap()
            };
            let mut counts = Vec::new();
            let mut labels = Vec::new();
            for batch in batches {
                assert_eq!(batch.num_columns(), 2);
                assert_eq!(batch.schema().field(0).name(), KEY);
                assert_eq!(batch.schema().field(1).name(), COUNT);
                let values = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<arrow_array::UInt64Array>()
                    .unwrap();
                counts.extend((0..batch.num_rows()).map(|row| values.value(row)));
                labels.extend(arrow_text(batch.column(0).as_ref()));
            }
            assert_eq!(counts, if empty { vec![] } else { vec![3, 2, 1] });
            assert_eq!(labels, if empty { vec![] } else { vec!["東京", "é", ""] });
            assert_eq!(session.snapshot().memory.reserved_bytes, 0);
        }
    }
}

#[cfg(feature = "universal-format-io")]
fn arrow_text(array: &dyn arrow_array::Array) -> Vec<String> {
    assert_eq!(array.null_count(), 0);
    if let Some(values) = array.as_any().downcast_ref::<arrow_array::StringArray>() {
        (0..array.len())
            .map(|row| values.value(row).to_owned())
            .collect()
    } else if let Some(values) = array
        .as_any()
        .downcast_ref::<arrow_array::LargeStringArray>()
    {
        (0..array.len())
            .map(|row| values.value(row).to_owned())
            .collect()
    } else if let Some(values) = array
        .as_any()
        .downcast_ref::<arrow_array::StringViewArray>()
    {
        (0..array.len())
            .map(|row| values.value(row).to_owned())
            .collect()
    } else {
        panic!("compatibility output must retain a UTF8 dtype");
    }
}
