//! Typed, dictionary and nested predicate keys across actual native runs.

use super::*;
use crate::local_primitives::{
    native_relational_spill::State,
    native_relational_subquery::{Kind as NativeKind, Ready, Spec},
    prepared_relational::tests::batch_input_tests::unary_tests::membership_tests::typed_tests::{
        FIELDS, typed_source,
    },
};
use vortex::array::dtype::{DType, Nullability};

fn spec(array: &ArrayRef, field: &str, kind: &str, negated: bool) -> Spec {
    let fields = array.dtype().as_struct_fields_opt().unwrap();
    let mut fields = fields
        .names()
        .iter()
        .zip(fields.fields())
        .map(|(name, dtype)| (name.to_string(), dtype))
        .collect::<Vec<_>>();
    fields.push(("answer".into(), DType::Bool(Nullability::Nullable)));
    let (kind, correlation, keys) = match kind {
        "in" => (NativeKind::In, 0, vec![field.into(), "pad".into()]),
        "exists" => (NativeKind::Exists, 2, vec![field.into(), "pad".into()]),
        "any" | "all" => (
            NativeKind::Quantified {
                comparison: ComparisonOp::Eq,
                quantifier: if kind == "any" {
                    Quantifier::Any
                } else {
                    Quantifier::All
                },
            },
            1,
            vec!["pad".into(), field.into()],
        ),
        _ => unreachable!(),
    };
    Spec {
        kind,
        guard: None,
        fields,
        left_keys: keys.clone(),
        right_keys: keys,
        correlation,
        negated,
    }
}

fn consume(array: &ArrayRef, spec: &Spec, spill: bool) -> Vec<Value> {
    let (_fixture, directory) = super::failure_tests::workspace();
    let session = ResidentVortexSession::new(64 << 20, 1).unwrap();
    let memory = session.memory().clone();
    let input_fields = array.dtype().as_struct_fields_opt().unwrap();
    let input_fields = input_fields
        .names()
        .iter()
        .zip(input_fields.fields())
        .map(|(name, dtype)| (name.to_string(), dtype))
        .collect::<Vec<_>>();
    let mut retained = Vec::new();
    let output = session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let state = spill
                .then(|| {
                    State::new(
                        &VortexRelationalSpillPolicy::new(&directory, 128 << 20, 1 << 20)?,
                        context,
                    )
                })
                .transpose()?;
            let mut ended = false;
            let mut ready =
                Ready::build(spec, &input_fields, context, 2, state.as_ref(), |consume| {
                    for start in (0..array.len()).step_by(2) {
                        consume(array.slice(start..(start + 2).min(array.len())).unwrap())?;
                    }
                    ended = true;
                    Ok(())
                })?;
            assert!(ended);
            let mut output = Vec::new();
            ready.consume(array.clone(), context, &mut |array| {
                output.extend(values(&array, context)?);
                retained.push(array);
                Ok(())
            })?;
            let report = ready.finish(context)?;
            assert_eq!(report.stages, u64::from(spill));
            if let Some(state) = state {
                let spill = state.finish()?;
                assert!(spill.runs_written > 0);
                assert!(spill.owned_cleanup_completed);
            }
            Ok(output)
        })
        .unwrap();
    super::failure_tests::clean(&directory);
    assert!(memory.snapshot().peak_reserved_bytes <= 64 << 20);
    assert!(memory.snapshot().reserved_bytes > 0);
    drop(retained);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    output
}

#[test]
fn subquery_membership_spilled_typed_dictionary_and_nested_keys_preserve_complete_rows() {
    for dictionary in [false, true] {
        let (array, rows) = typed_source(dictionary);
        for &field in &FIELDS[1..9] {
            for kind in ["in", "exists", "any", "all"] {
                for negated in [false, true] {
                    let spec = spec(&array, field, kind, negated);
                    let expected = rows
                        .iter()
                        .map(|row| {
                            let value = if row[field].is_null() {
                                (kind == "exists").then_some(false)
                            } else {
                                Some(kind != "all")
                            };
                            let mut row = row.clone();
                            row["answer"] =
                                value.map_or(Value::Null, |value| json!(value ^ negated));
                            row
                        })
                        .collect::<Vec<_>>();
                    for spill in [false, true] {
                        assert_eq!(
                            consume(&array, &spec, spill),
                            expected,
                            "{dictionary}/{field}/{kind}/{negated}/{spill}"
                        );
                    }
                }
            }
        }
    }
}
