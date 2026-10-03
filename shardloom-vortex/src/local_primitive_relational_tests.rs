use super::*;
use crate::relational_query::{
    VortexRelationalJoin, VortexRelationalJoinColumn, VortexRelationalJoinKey,
    VortexRelationalJoinKind as JoinKind, VortexRelationalScan, VortexRelationalSet,
    VortexRelationalSide as Side,
};
use shardloom_core::{ColumnRef, DatasetUri};
use std::{
    fs,
    sync::atomic::{AtomicUsize, Ordering},
};
use vortex::{
    VortexSessionDefault as _,
    array::{IntoArray as _, arrays::PrimitiveArray, dtype::PType},
    file::WriteOptionsSessionExt as _,
    io::{
        runtime::{BlockingRuntime as _, current::CurrentThreadRuntime},
        session::RuntimeSessionExt as _,
    },
    session::VortexSession,
};

static NEXT: AtomicUsize = AtomicUsize::new(0);
#[path = "local_primitive_relational_aggregate_tests.rs"]
mod aggregate_tests;
#[path = "local_primitive_relational_correlated_tests.rs"]
mod correlated_tests;
#[path = "local_primitive_relational_dynamic_tests.rs"]
mod dynamic_tests;
#[path = "local_primitive_relational_expression_tests.rs"]
mod expression_tests;
#[path = "local_primitive_relational_join_condition_tests.rs"]
mod join_condition_tests;
#[path = "local_primitive_relational_nested_tests.rs"]
mod nested_tests;
#[path = "local_primitive_relational_spill_tests.rs"]
mod spill_tests;
#[path = "local_primitive_relational_subquery_tests.rs"]
mod subquery_tests;
#[path = "local_primitive_relational_unary_tests.rs"]
mod unary_tests;
#[path = "local_primitive_relational_window_tests.rs"]
mod window_tests;
#[cfg(feature = "universal-format-io")]
#[path = "local_primitive_relational_writer_tests.rs"]
mod writer_tests;
struct Fixture(PathBuf);
impl Fixture {
    fn new(array: ArrayRef, chunk_rows: usize) -> Self {
        Self::with_metadata(array, chunk_rows, vec![])
    }

    fn with_metadata(array: ArrayRef, chunk_rows: usize, metadata: Vec<(&str, Vec<u8>)>) -> Self {
        let directory = std::env::temp_dir().join(format!(
            "shardloom-relational-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&directory).unwrap();
        let fixture = Self(directory);
        let runtime = CurrentThreadRuntime::new();
        let session = VortexSession::default().with_handle(runtime.handle());
        let mut file = fs::File::create(fixture.path()).unwrap();
        let mut writer = session
            .write_options()
            .with_metadata_segments(metadata)
            .with_strategy(
                super::super::native_flat_layout::SequentialNativeFlatLayout::strategy(
                    array.len().div_ceil(chunk_rows).max(1),
                ),
            )
            .with_file_statistics(Vec::new())
            .blocking(&runtime)
            .writer(&mut file, array.dtype().clone());
        if array.is_empty() {
            writer.push(array).unwrap();
        } else {
            for start in (0..array.len()).step_by(chunk_rows) {
                writer
                    .push(
                        array
                            .slice(start..array.len().min(start + chunk_rows))
                            .unwrap(),
                    )
                    .unwrap();
            }
        }
        writer.finish().unwrap();
        fixture
    }
    fn path(&self) -> PathBuf {
        self.0.join("input.vortex")
    }
    fn scan(&self) -> VortexRelationalPlan {
        VortexRelationalPlan::Scan(VortexRelationalScan {
            source_uri: DatasetUri::new(self.path().display().to_string()).unwrap(),
            projection: shardloom_plan::ProjectionRequest::All,
            predicate: None,
        })
    }
    fn replace(&self) {
        let replacement = self.0.join("replacement.vortex");
        fs::copy(self.path(), &replacement).unwrap();
        fs::rename(replacement, self.path()).unwrap();
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}

fn policy() -> VortexLocalPrimitiveExecutionPolicy {
    let mut policy = VortexLocalPrimitiveExecutionPolicy::single_threaded();
    policy.resource_envelope.memory_budget_bytes = 32 << 20;
    policy
}

fn keyed(keys: &[Option<u64>], ids: &[u32]) -> ArrayRef {
    StructArray::try_new(
        FieldNames::from(["entity", "amount"]),
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

fn join(left: &Fixture, right: &Fixture, kind: JoinKind) -> VortexRelationalPlan {
    VortexRelationalPlan::Join(Box::new(VortexRelationalJoin {
        left: left.scan(),
        right: right.scan(),
        kind,
        condition: None,
        keys: if kind == JoinKind::Cross {
            vec![]
        } else {
            vec![VortexRelationalJoinKey {
                left: ColumnRef::new("entity").unwrap(),
                right: ColumnRef::new("entity").unwrap(),
            }]
        },
        columns: vec![
            VortexRelationalJoinColumn {
                side: Side::Left,
                column: ColumnRef::new("amount").unwrap(),
                output_column: "debit".into(),
            },
            VortexRelationalJoinColumn {
                side: Side::Right,
                column: ColumnRef::new("amount").unwrap(),
                output_column: "credit".into(),
            },
        ],
    }))
}

fn json_rows(collected: &CollectedVortexRelational) -> Vec<serde_json::Value> {
    collected
        .result_jsonl
        .value()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

#[test]
fn native_relational_prepared_join_reuses_sources_and_validates_after_final_consumer() {
    let left = Fixture::new(keyed(&[Some(2), None, Some(1)], &[10, 11, 12]), 1);
    let right = Fixture::new(keyed(&[Some(2), Some(2), Some(3)], &[20, 21, 22]), 1);
    let prepared = prepare_relational(&join(&left, &right, JoinKind::Full), policy()).unwrap();
    let expected = serde_json::json!([
        {"debit":10,"credit":20},{"debit":10,"credit":21},
        {"debit":11,"credit":null},{"debit":12,"credit":null},{"debit":null,"credit":22},
    ]);
    for call in 1..=3 {
        let collected = prepared
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        assert_eq!(serde_json::json!(json_rows(&collected)), expected);
        assert!(collected.execution.native_io_certificate.is_certified());
        assert_eq!(collected.execution.bytes_decoded, None);
        assert_eq!(collected.execution.prepared_sources, 2);
        assert_eq!(collected.execution.runtime.prepared_source_opens, 2);
        assert_eq!(collected.execution.runtime.completed_executions, call);
    }
    let before = prepared.session.memory().snapshot().reserved_bytes;
    let mut replaced = false;
    assert!(
        prepared
            .for_each_batch(&CancellationToken::default(), |_, _| {
                if !replaced {
                    right.replace();
                    replaced = true;
                }
                Ok(())
            })
            .is_err()
    );
    assert!(replaced);
    assert_eq!(prepared.snapshot().completed_executions, 3);
    assert_eq!(prepared.session.memory().snapshot().reserved_bytes, before);
    assert!(
        prepared
            .collect_jsonl(&CancellationToken::default())
            .is_err()
    );
}

#[test]
fn native_relational_self_join_opens_once_and_empty_output_keeps_its_bound_schema() {
    let fixture = Fixture::new(keyed(&[], &[]), 1);
    let prepared = prepare_relational(&join(&fixture, &fixture, JoinKind::Left), policy()).unwrap();
    assert_eq!(prepared.snapshot().prepared_source_opens, 1);
    let result = prepared.execute_owned().unwrap();
    assert_eq!(result.execution.output_rows, 0);
    assert_eq!(result.execution.output_batches, 1);
    assert_eq!(
        result.result.dtype(),
        &DType::struct_(
            [
                (
                    "debit",
                    DType::Primitive(PType::U32, Nullability::NonNullable)
                ),
                (
                    "credit",
                    DType::Primitive(PType::U32, Nullability::Nullable)
                ),
            ],
            Nullability::NonNullable
        )
    );
    assert!(
        !result
            .execution
            .native_io_certificate
            .side_effects
            .data_read
    );
}

#[test]
fn native_relational_schema_lowering_and_execution_share_each_prepared_source() {
    let left = Fixture::new(keyed(&[Some(1)], &[10]), 1);
    let right = Fixture::new(keyed(&[Some(1)], &[20]), 1);
    let prepared = prepare_relational_with_schema(policy(), |schemas| {
        for fixture in [&left, &right, &left] {
            let uri = DatasetUri::new(fixture.path().display().to_string())?;
            assert_eq!(schemas.source_columns(&uri)?, vec!["entity", "amount"]);
        }
        Ok(join(&left, &right, JoinKind::Inner))
    })
    .unwrap();
    assert_eq!(prepared.snapshot().prepared_source_opens, 2);
    assert_eq!(prepared.snapshot().completed_executions, 0);
    let result = prepared
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(
        json_rows(&result),
        vec![serde_json::json!({"debit":10,"credit":20})]
    );
    assert_eq!(result.execution.runtime.prepared_source_opens, 2);
    assert_eq!(result.execution.runtime.completed_executions, 1);
}

fn single(name: &str, values: ArrayRef) -> ArrayRef {
    let rows = values.len();
    StructArray::try_new(
        FieldNames::from([name]),
        vec![values],
        rows,
        Validity::NonNullable,
    )
    .unwrap()
    .into_array()
}

#[test]
fn native_relational_sets_align_by_position_and_prove_null_equal_lossless_semantics() {
    let left = Fixture::new(
        single(
            "signed_value",
            PrimitiveArray::from_option_iter([Some(-1_i8), Some(2), Some(2), None]).into_array(),
        ),
        2,
    );
    let right = Fixture::new(
        single(
            "different_name",
            PrimitiveArray::from_option_iter([Some(2_u8), Some(3), None, Some(255)]).into_array(),
        ),
        1,
    );
    for (kind, expected) in [
        (
            SetKind::UnionAll,
            serde_json::json!([-1, 2, 2, null, 2, 3, null, 255]),
        ),
        (
            SetKind::UnionDistinct,
            serde_json::json!([-1, 2, null, 3, 255]),
        ),
        (SetKind::Intersect, serde_json::json!([2, null])),
        (SetKind::Except, serde_json::json!([-1])),
    ] {
        let plan = VortexRelationalPlan::Set(Box::new(VortexRelationalSet {
            left: left.scan(),
            right: right.scan(),
            kind,
        }));
        let prepared = prepare_relational(&plan, policy()).unwrap();
        let PreparedRoot::Bound(root) = &prepared.root else {
            panic!("static plan")
        };
        assert_eq!(
            root.fields[0].1,
            DType::Primitive(PType::I16, Nullability::Nullable)
        );
        for _ in 0..2 {
            let collected = prepared
                .collect_jsonl(&CancellationToken::default())
                .unwrap();
            let actual = json_rows(&collected)
                .into_iter()
                .map(|row| row["signed_value"].clone())
                .collect::<Vec<_>>();
            assert_eq!(serde_json::json!(actual), expected, "{kind:?}");
        }
    }
}

#[test]
fn native_relational_schema_denial_occurs_before_payload_execution() {
    let left = Fixture::new(
        single("v", PrimitiveArray::from_iter([i64::MIN]).into_array()),
        1,
    );
    let right = Fixture::new(
        single("w", PrimitiveArray::from_iter([u64::MAX]).into_array()),
        1,
    );
    let session = ResidentVortexSession::new(32 << 20, 1).unwrap();
    let plan = VortexRelationalPlan::Set(Box::new(VortexRelationalSet {
        left: left.scan(),
        right: right.scan(),
        kind: SetKind::UnionAll,
    }));
    let error = prepare_relational_in_session(&plan, policy(), &session)
        .err()
        .unwrap();
    assert!(
        error
            .to_string()
            .contains("no lossless native common dtype")
    );
    assert_eq!(session.snapshot().completed_executions, 0);
    assert_eq!(session.memory().snapshot().reserved_bytes, 0);
}

#[test]
fn native_relational_string_joins_compare_dictionary_domains_and_honor_root_validity() {
    use vortex::array::arrays::{DictArray, VarBinViewArray};
    let text = "東京\0a payload longer than an inline view";
    let make = |codes: Vec<u8>, domain: &[&str], payload: &[&str], validity| {
        StructArray::new(
            FieldNames::from(["entity", "amount"]),
            vec![
                DictArray::try_new(
                    PrimitiveArray::from_iter(codes).into_array(),
                    VarBinViewArray::from_iter_str(domain.iter().copied()).into_array(),
                )
                .unwrap()
                .into_array(),
                VarBinViewArray::from_iter_str(payload.iter().copied()).into_array(),
            ],
            payload.len(),
            validity,
        )
        .into_array()
    };
    let left_array = make(
        vec![0, 1, 2],
        &[text, "hidden", ""],
        &[text, "invisible", "empty"],
        Validity::from_iter([true, false, true]),
    );
    // The upstream file writer rejects nullable root structs. Normalize root
    // validity into logical fields at this test's persistence boundary; direct
    // nullable-root execution has its own native-array kernel test.
    let left = Fixture::new(
        StructArray::new(
            FieldNames::from(["entity", "amount"]),
            ["entity", "amount"].into_iter().map(|name| {
                crate::local_primitives::logical_field_from_native_array(&left_array, name).unwrap()
            }),
            left_array.len(),
            Validity::NonNullable,
        )
        .into_array(),
        1,
    );
    let right = Fixture::new(
        make(
            vec![1, 0, 1],
            &["", text],
            &["first", "second", "third"],
            Validity::NonNullable,
        ),
        1,
    );
    let prepared = prepare_relational(&join(&left, &right, JoinKind::Full), policy()).unwrap();
    let result = prepared
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(
        json_rows(&result),
        vec![
            serde_json::json!({"debit":text,"credit":"first"}),
            serde_json::json!({"debit":text,"credit":"third"}),
            serde_json::json!({"debit":null,"credit":null}),
            serde_json::json!({"debit":"empty","credit":"second"}),
        ]
    );
    drop(result);
    let memory = prepared.session.memory().clone();
    let result = prepared.execute_owned().unwrap();
    let cloned = result.result.arrays().to_vec();
    drop((result, prepared));
    assert!(memory.snapshot().reserved_bytes > 0);
    drop(cloned);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}
