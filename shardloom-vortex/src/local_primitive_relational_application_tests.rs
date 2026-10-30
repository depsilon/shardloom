//! Application execution is explicit, ordered, bounded and ownership-safe.

use super::*;
use crate::application_batch::{
    ApplicationBatchContext, ApplicationBatchDeclaration, ApplicationBatchFunction,
    ApplicationBatchLimits, ApplicationBatchOutput,
};
use crate::relational_query::VortexRelationalLimit;
use serde_json::json;
use shardloom_core::expression::{Determinism, EffectLevel, NullBehavior};
use std::sync::{Arc, Mutex};
use vortex::array::arrays::VarBinViewArray;

fn record(numbers: &[Option<i64>], words: &[Option<&str>]) -> ArrayRef {
    StructArray::try_new(
        FieldNames::from(["n", "s"]),
        vec![
            PrimitiveArray::from_option_iter(numbers.iter().copied()).into_array(),
            VarBinViewArray::from_iter_nullable_str(words.iter().copied()).into_array(),
        ],
        numbers.len(),
        Validity::NonNullable,
    )
    .unwrap()
    .into_array()
}

fn declaration() -> ApplicationBatchDeclaration {
    let schema = record(&[], &[]).dtype().clone();
    ApplicationBatchDeclaration {
        name: "triple_and_tag".into(),
        version: "1".into(),
        input: schema.clone(),
        output: schema,
        determinism: Determinism::Deterministic,
        null_behavior: NullBehavior::NullAware,
        effect: EffectLevel::PureDeterministic,
        limits: ApplicationBatchLimits {
            max_batch_rows: 3,
            max_output_bytes: 1 << 20,
            workspace_bytes: 32 << 10,
        },
    }
}

fn transformed(
    input: &ArrayRef,
    context: &ApplicationBatchContext<'_>,
) -> Result<ApplicationBatchOutput> {
    context.check_cancelled()?;
    let mut execution = context.native_session().create_execution_ctx();
    let numbers = crate::local_primitives::logical_field_from_native_array(input, "n")?;
    let words = crate::local_primitives::logical_field_from_native_array(input, "s")?;
    let mut output_numbers = Vec::new();
    let mut output_words = Vec::new();
    for row in 0..input.len() {
        output_numbers.push(
            numbers
                .execute_scalar(row, &mut execution)
                .unwrap()
                .as_primitive()
                .typed_value::<i64>()
                .map(|value| value * 3),
        );
        output_words.push(
            words
                .execute_scalar(row, &mut execution)
                .unwrap()
                .as_utf8()
                .value()
                .map(|value| format!("{}:λ", value.as_str())),
        );
    }
    let references: Vec<_> = output_words.iter().map(Option::as_deref).collect();
    Ok(record(&output_numbers, &references).into())
}

fn function(calls: &Arc<AtomicUsize>) -> ApplicationBatchFunction {
    let calls = calls.clone();
    ApplicationBatchFunction::new_trusted(declaration(), move |input, context| {
        calls.fetch_add(1, Ordering::Relaxed);
        assert!(!input.is_empty() && input.len() <= 3);
        transformed(input, context)
    })
    .unwrap()
}

fn limit(input: VortexRelationalPlan, count: usize) -> VortexRelationalPlan {
    VortexRelationalPlan::Limit(Box::new(VortexRelationalLimit {
        input,
        offset: 0,
        count,
    }))
}

#[test]
fn application_batches_bind_inertly_preserve_nullable_rows_across_partitions_and_actions() {
    let numbers: Vec<_> = (0..37)
        .map(|value| (value % 5 != 0).then_some(value))
        .collect();
    let words: Vec<_> = (0..37)
        .map(|value| (value % 7 != 0).then_some("東京"))
        .collect();
    let expected: Vec<_> = numbers.iter().zip(&words).map(|(number, word)| {
        json!({"n": number.map(|value| value * 3), "s": word.map(|word| format!("{word}:λ"))})
    }).collect();
    for partition in [1, 2, 5, 37] {
        let calls = Arc::new(AtomicUsize::new(0));
        let handle = function(&calls);
        assert_eq!(calls.load(Ordering::Relaxed), 0);
        assert_eq!(handle, handle.clone());
        assert_ne!(handle, function(&Arc::new(AtomicUsize::new(0))));
        let prepared = prepare(&scan().map_batches(handle), 4 << 20).unwrap();
        assert_eq!(calls.load(Ordering::Relaxed), 0);
        let memory = prepared.session.shared_memory_pool();
        let baseline = memory.snapshot().reserved_bytes;
        for action in 1..=2 {
            let mut chunks = numbers.chunks(partition).zip(words.chunks(partition));
            let mut input = |session: &ResidentVortexSession| {
                chunks
                    .next()
                    .map(|(numbers, words)| source(session, numbers, words))
                    .transpose()
            };
            let result = prepared
                .with_batch_input(&mut input)
                .unwrap()
                .collect_jsonl(&CancellationToken::default())
                .unwrap();
            assert_eq!(json_rows(&result), expected);
            let expected_calls: usize = numbers
                .chunks(partition)
                .map(|rows| rows.len().div_ceil(3))
                .sum();
            assert_eq!(calls.load(Ordering::Relaxed), action * expected_calls);
            let certificate = &result.execution.native_io_certificate;
            assert!(certificate.is_certified());
            assert!(!certificate.side_effects.fallback_attempted);
            assert!(
                certificate
                    .source_pushdown_report
                    .proof_basis
                    .contains("application_rows=37")
            );
            assert!(
                certificate
                    .source_pushdown_report
                    .accepted_operations
                    .iter()
                    .any(|operation| operation == "application_batch:triple_and_tag@1")
            );
            drop(result);
            assert_eq!(memory.snapshot().reserved_bytes, baseline);
        }
        drop(prepared);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn application_empty_relation_has_declared_schema_without_invoking_code() {
    let calls = Arc::new(AtomicUsize::new(0));
    let prepared = prepare(&scan().map_batches(function(&calls)), 2 << 20).unwrap();
    assert_eq!(prepared.output_dtype().unwrap(), declaration().output);
    let input = |session: &ResidentVortexSession| source(session, &[], &[]).map(Some);
    // A finite sequence includes empty batches; no application invocation is needed.
    let mut remaining = 2;
    let mut finite = |session: &ResidentVortexSession| {
        if remaining == 0 {
            return Ok(None);
        }
        remaining -= 1;
        input(session)
    };
    let result = prepared
        .with_batch_input(&mut finite)
        .unwrap()
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(json_rows(&result), [] as [serde_json::Value; 0]);
    assert_eq!(calls.load(Ordering::Relaxed), 0);
    assert!(result.execution.input.unwrap().end_of_input_observed);
}

#[test]
fn application_late_failure_is_observed_before_downstream_limit_success() {
    for count in [0, 1] {
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = calls.clone();
        let callback =
            ApplicationBatchFunction::new_trusted(declaration(), move |input, context| {
                if observed.fetch_add(1, Ordering::Relaxed) == 1 {
                    return Err(shardloom_core::ShardLoomError::new(
                        "late application failure",
                    ));
                }
                transformed(input, context)
            })
            .unwrap();
        let prepared = prepare(&limit(scan().map_batches(callback), count), 2 << 20).unwrap();
        let baseline = prepared.snapshot().memory.reserved_bytes;
        let mut once = true;
        let mut input = |session: &ResidentVortexSession| {
            if !std::mem::take(&mut once) {
                return Ok(None);
            }
            source(session, &[Some(1); 7], &[Some("x"); 7]).map(Some)
        };
        let error = prepared
            .with_batch_input(&mut input)
            .unwrap()
            .collect_jsonl(&CancellationToken::default())
            .err()
            .unwrap();
        assert!(error.to_string().contains("late application failure"));
        assert_eq!(calls.load(Ordering::Relaxed), 2);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
    }
    // A limit explicitly before the transform changes its input domain.
    let calls = Arc::new(AtomicUsize::new(0));
    let prepared = prepare(&limit(scan(), 0).map_batches(function(&calls)), 2 << 20).unwrap();
    let mut once = true;
    let mut input = |session: &ResidentVortexSession| {
        if !std::mem::take(&mut once) {
            return Ok(None);
        }
        source(session, &[Some(1); 7], &[Some("x"); 7]).map(Some)
    };
    let result = prepared
        .with_batch_input(&mut input)
        .unwrap()
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(json_rows(&result), [] as [serde_json::Value; 0]);
    assert_eq!(calls.load(Ordering::Relaxed), 0);
}

#[test]
fn application_faults_publish_no_invalid_batch_release_credit_and_allow_explicit_retry() {
    for fault in [
        "row_count",
        "schema",
        "byte_limit",
        "panic",
        "cancel",
        "error",
    ] {
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = calls.clone();
        let mut declared = declaration();
        if fault == "byte_limit" {
            declared.limits.max_output_bytes = 1;
        }
        let callback = ApplicationBatchFunction::new_trusted(declared, move |input, context| {
            let call = observed.fetch_add(1, Ordering::Relaxed);
            if call > 0 {
                return Ok(input.clone().into());
            }
            match fault {
                "row_count" => Ok(record(&[], &[]).into()),
                "schema" => Ok(PrimitiveArray::from_option_iter([Some(1_i64)])
                    .into_array()
                    .into()),
                "byte_limit" => Ok(input.clone().into()),
                "panic" => panic!("intentional application unwind"),
                "cancel" => {
                    context.cancellation().cancel();
                    Ok(input.clone().into())
                }
                "error" => Err(shardloom_core::ShardLoomError::application_function(
                    "original callback failed",
                    std::io::Error::other("original cause"),
                )),
                _ => unreachable!(),
            }
        })
        .unwrap();
        let prepared = prepare(&scan().map_batches(callback), 2 << 20).unwrap();
        let baseline = prepared.snapshot().memory.reserved_bytes;
        let mut once = true;
        let mut input = |session: &ResidentVortexSession| {
            if !std::mem::take(&mut once) {
                return Ok(None);
            }
            source(session, &[Some(1)], &[Some("x")]).map(Some)
        };
        let mut published = 0;
        let error = prepared
            .with_batch_input(&mut input)
            .unwrap()
            .for_each_batch(&CancellationToken::default(), |_, _| {
                published += 1;
                Ok(())
            })
            .err()
            .unwrap();
        assert_eq!(published, 0, "{fault}");
        assert_eq!(
            calls.load(Ordering::Relaxed),
            1,
            "{fault}: no automatic retry"
        );
        assert_eq!(
            prepared.snapshot().memory.reserved_bytes,
            baseline,
            "{fault}"
        );
        assert!(!error.to_diagnostic().fallback.attempted);
        if fault == "error" {
            use std::error::Error as _;
            assert_eq!(
                error
                    .source()
                    .unwrap()
                    .downcast_ref::<std::io::Error>()
                    .unwrap()
                    .to_string(),
                "original cause"
            );
        }
        if fault != "byte_limit" {
            once = true;
            let mut retry = |session: &ResidentVortexSession| {
                if !std::mem::take(&mut once) {
                    return Ok(None);
                }
                source(session, &[Some(1)], &[Some("x")]).map(Some)
            };
            let result = prepared
                .with_batch_input(&mut retry)
                .unwrap()
                .collect_jsonl(&CancellationToken::default())
                .unwrap();
            assert_eq!(json_rows(&result), vec![json!({"n":1,"s":"x"})]);
            assert_eq!(calls.load(Ordering::Relaxed), 2);
        }
    }
}

#[test]
fn application_encoded_output_expansion_checks_owned_bytes_before_publication() {
    use vortex::array::{arrays::ConstantArray, scalar::Scalar};

    for limit in [128, 16 << 10] {
        let mut declared = declaration();
        declared.limits.max_batch_rows = 16;
        declared.limits.max_output_bytes = limit;
        let callback = ApplicationBatchFunction::new_trusted(declared, |input, _| {
            let output = StructArray::try_new(
                FieldNames::from(["n", "s"]),
                vec![
                    ConstantArray::new(
                        Scalar::primitive(7_i64, Nullability::Nullable),
                        input.len(),
                    )
                    .into_array(),
                    ConstantArray::new(
                        Scalar::utf8("x".repeat(64), Nullability::Nullable),
                        input.len(),
                    )
                    .into_array(),
                ],
                input.len(),
                Validity::NonNullable,
            )
            .unwrap()
            .into_array();
            assert!(
                output.nbytes() <= 128,
                "encoded output satisfies both limits"
            );
            Ok(output.into())
        })
        .unwrap();
        let prepared = prepare(&scan().map_batches(callback), 2 << 20).unwrap();
        let baseline = prepared.snapshot().memory.reserved_bytes;
        let mut once = true;
        let mut input = |session: &ResidentVortexSession| {
            if !std::mem::take(&mut once) {
                return Ok(None);
            }
            source(session, &[Some(1); 16], &[Some("x"); 16]).map(Some)
        };
        let mut published = 0;
        let result = prepared
            .with_batch_input(&mut input)
            .unwrap()
            .for_each_batch(&CancellationToken::default(), |batch, _| {
                assert!(
                    batch.nbytes() > 128,
                    "independent output expands the constants"
                );
                published += batch.len();
                Ok(())
            });
        if limit == 128 {
            assert!(
                result
                    .err()
                    .unwrap()
                    .to_string()
                    .contains("visible-byte limit")
            );
            assert_eq!(published, 0);
        } else {
            result.unwrap();
            assert_eq!(published, 16);
        }
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
    }
}

#[test]
fn application_workspace_denial_and_precancellation_precede_invocation() {
    for cancel in [false, true] {
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = calls.clone();
        let mut declared = declaration();
        declared.limits.workspace_bytes = 8 << 20;
        let callback = ApplicationBatchFunction::new_trusted(declared, move |input, _| {
            observed.fetch_add(1, Ordering::Relaxed);
            Ok(input.clone().into())
        })
        .unwrap();
        let prepared = prepare(&scan().map_batches(callback), 2 << 20).unwrap();
        let baseline = prepared.snapshot().memory.reserved_bytes;
        let cancellation = CancellationToken::default();
        if cancel {
            cancellation.cancel();
        }
        let mut input =
            |session: &ResidentVortexSession| source(session, &[Some(1)], &[None]).map(Some);
        assert!(
            prepared
                .with_batch_input(&mut input)
                .unwrap()
                .execute_native(&cancellation)
                .is_err()
        );
        assert_eq!(calls.load(Ordering::Relaxed), 0);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
    }
}

#[test]
fn application_retained_input_and_independently_owned_output_survive_query_owners() {
    let retained_input = Arc::new(Mutex::new(Vec::new()));
    let caller_alias = retained_input.clone();
    let callback = ApplicationBatchFunction::new_trusted(declaration(), move |input, _| {
        caller_alias.lock().unwrap().push(input.clone());
        Ok(input.clone().into())
    })
    .unwrap();
    let prepared = prepare(&scan().map_batches(callback), 4 << 20).unwrap();
    let memory = prepared.session.shared_memory_pool();
    let baseline = memory.snapshot().reserved_bytes;
    let mut once = true;
    let mut witness = None;
    let mut input = |session: &ResidentVortexSession| {
        if !std::mem::take(&mut once) {
            return Ok(None);
        }
        let input = source(
            session,
            &[Some(i64::MIN), None, Some(i64::MAX)],
            &[Some("λ"), None, Some("")],
        )?;
        witness = Some(input.batch_release_witness()?);
        Ok(Some(input))
    };
    let mut retained_output = Vec::new();
    let completed = prepared
        .with_batch_input(&mut input)
        .unwrap()
        .for_each_batch(&CancellationToken::default(), |array, context| {
            assert_eq!(
                values(&array, context)?,
                vec![
                    json!({"n":i64::MIN,"s":"λ"}),
                    json!({"n":null,"s":null}),
                    json!({"n":i64::MAX,"s":""})
                ]
            );
            retained_output.push(array);
            Ok(())
        })
        .unwrap();
    assert_eq!(witness.as_ref().unwrap().strong_count(), 0);
    assert!(memory.snapshot().reserved_bytes > baseline);
    drop((completed, prepared));
    assert!(memory.snapshot().reserved_bytes > 0);
    retained_input.lock().unwrap().clear();
    assert_eq!(witness.unwrap().strong_count(), 0);
    assert!(memory.snapshot().reserved_bytes > 0);
    let mut execution = vortex::array::array_session().create_execution_ctx();
    let n =
        crate::local_primitives::logical_field_from_native_array(&retained_output[0], "n").unwrap();
    assert_eq!(
        n.execute_scalar(2, &mut execution)
            .unwrap()
            .as_primitive()
            .typed_value::<i64>(),
        Some(i64::MAX)
    );
    drop(n);
    drop(retained_output);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn application_reentry_is_refused_and_guard_recovers_after_return() {
    let nested = ResidentVortexSession::new(1 << 20, 1).unwrap();
    let callback = ApplicationBatchFunction::new_trusted(declaration(), move |input, _| {
        let error = nested
            .with_native_execution_context(&CancellationToken::default(), |_| -> Result<()> {
                panic!("reentry must fail before admitting nested execution")
            })
            .err()
            .unwrap();
        assert!(
            error
                .to_string()
                .contains("cannot start another native action")
        );
        Ok(input.clone().into())
    })
    .unwrap();
    let prepared = prepare(&scan().map_batches(callback), 2 << 20).unwrap();
    for _ in 0..2 {
        let mut once = true;
        let mut input = |session: &ResidentVortexSession| {
            if !std::mem::take(&mut once) {
                return Ok(None);
            }
            source(session, &[Some(1)], &[None]).map(Some)
        };
        let result = prepared
            .with_batch_input(&mut input)
            .unwrap()
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        assert_eq!(json_rows(&result), vec![json!({"n":1,"s":null})]);
    }
}

#[test]
fn application_invalid_declarations_and_input_schema_never_invoke_code() {
    for invalid in ["name", "version", "batch", "bytes", "schema", "input"] {
        let mut declared = declaration();
        match invalid {
            "name" => declared.name.clear(),
            "version" => declared.version.clear(),
            "batch" => declared.limits.max_batch_rows = 0,
            "bytes" => declared.limits.max_output_bytes = 0,
            "schema" => declared.output = DType::Bool(Nullability::Nullable),
            "input" => {
                declared.input = StructArray::try_new(
                    FieldNames::from(["wrong"]),
                    vec![PrimitiveArray::from_iter([1_i64]).into_array()],
                    1,
                    Validity::NonNullable,
                )
                .unwrap()
                .into_array()
                .dtype()
                .clone();
            }
            _ => unreachable!(),
        }
        let callback =
            ApplicationBatchFunction::new_trusted(declared, |_, _| panic!("inert admission"));
        if invalid == "input" {
            let error = prepare(&scan().map_batches(callback.unwrap()), 2 << 20)
                .err()
                .unwrap();
            assert!(error.to_string().contains("input schema differs"));
        } else {
            assert!(callback.is_err(), "{invalid}");
        }
    }
}
