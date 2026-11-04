//! Retained syntax never stands in for fresh source/resource admission.

use super::*;
use crate::sql_local_source_runtime::native_relational::NativeSqlDeclaration;
use std::collections::BTreeMap;

#[test]
fn retained_statement_identity_excludes_bound_values_relations_and_observations() {
    let raw = "SELECT 1 AS value";
    let plain = Arc::new(NativeRelationDeclaration::sql(raw, vec![]).unwrap());
    assert!(plain.matches_statement(raw));
    assert!(plain.clone().matches_statement(raw));
    assert!(!plain.matches_statement("SELECT 2 AS value"));
    assert!(!plain.matches_statement(" SELECT 1 AS value"));
    let named =
        NativeRelationDeclaration::sql(raw, vec![("unused".into(), plain.clone())]).unwrap();
    assert!(!named.matches_statement(raw));
    let parameter = NativeRelationDeclaration::sql_with_parameters(
        "SELECT :value AS value",
        vec![],
        BTreeMap::from([("value".into(), shardloom_core::ScalarValue::Int64(7))]),
    )
    .unwrap();
    assert!(!parameter.matches_statement("SELECT :value AS value"));
    let observed = plain
        .observe("observed", &["COUNT(*) AS rows".into()])
        .unwrap();
    assert!(!observed.matches_statement("SELECT COUNT(*) AS rows FROM __sl_observed_input"));
    assert!(
        !NativeRelationDeclaration::scan("memory://input")
            .unwrap()
            .matches_statement("SELECT * FROM 'memory://input'")
    );
}

#[test]
fn retained_statement_preparation_reuses_syntax_with_new_sources_and_fresh_credits() {
    let session = session();
    let raw = "SELECT value FROM 'memory://input' WHERE value % 2 = 0 ORDER BY value";
    let declaration = NativeRelationDeclaration::sql(raw, vec![]).unwrap();
    for start in [0, 20] {
        let bindings = [binding(
            ResidentMemorySource::from_int64_range(&session, "value", start, 1, 5).unwrap(),
        )];
        let baseline = session.snapshot().memory.reserved_bytes;
        for retained in [false, true] {
            let parses = NativeSqlDeclaration::declaration_parses();
            let operation = if retained {
                prepare_declared_sql_operation(
                    raw,
                    &declaration,
                    allocation(),
                    &session,
                    &bindings,
                    None,
                    &CancellationToken::default(),
                )
            } else {
                prepare_sql_operation(
                    raw,
                    allocation(),
                    &session,
                    &bindings,
                    None,
                    &CancellationToken::default(),
                )
            }
            .unwrap();
            assert_eq!(
                NativeSqlDeclaration::declaration_parses() - parses,
                usize::from(!retained)
            );
            let result = operation
                .execute_native(&CancellationToken::default(), None)
                .unwrap();
            assert_eq!(
                integers(&Arc::new(result.result), 0),
                [start, start + 2, start + 4]
            );
            drop((result.execution, operation));
            assert_eq!(session.snapshot().memory.reserved_bytes, baseline);
        }
    }
}

#[test]
fn different_statement_cannot_reuse_retained_syntax() {
    let session = session();
    let bindings = [binding(
        ResidentMemorySource::from_int64_range(&session, "value", 0, 1, 3).unwrap(),
    )];
    let declaration = NativeRelationDeclaration::sql("SELECT 1 AS value", vec![]).unwrap();
    let before = NativeSqlDeclaration::declaration_parses();
    let operation = prepare_declared_sql_operation(
        "SELECT value + 100 AS value FROM 'memory://input'",
        &declaration,
        allocation(),
        &session,
        &bindings,
        None,
        &CancellationToken::default(),
    )
    .unwrap();
    assert_eq!(NativeSqlDeclaration::declaration_parses(), before + 1);
    let result = operation
        .execute_native(&CancellationToken::default(), None)
        .unwrap();
    assert_eq!(integers(&Arc::new(result.result), 0), [100, 101, 102]);
}

#[test]
fn retained_statement_keeps_binding_resource_and_cancellation_failures() {
    let session = session();
    let raw = "SELECT value FROM 'memory://input'";
    let declaration = NativeRelationDeclaration::sql(raw, vec![]).unwrap();
    let mut bindings = vec![binding(
        ResidentMemorySource::from_int64_range(&session, "value", 0, 1, 3).unwrap(),
    )];
    let mut extra = bindings[0].clone();
    extra.reference = "memory://unused".into();
    bindings.push(extra);
    let original = prepare_sql_operation(
        raw,
        allocation(),
        &session,
        &bindings,
        None,
        &CancellationToken::default(),
    )
    .err()
    .expect("unused binding must fail")
    .to_string();
    let retained = prepare_declared_sql_operation(
        raw,
        &declaration,
        allocation(),
        &session,
        &bindings,
        None,
        &CancellationToken::default(),
    )
    .err()
    .expect("retention cannot hide an unused binding")
    .to_string();
    assert_eq!(retained, original);
    assert!(retained.contains("source declaration is not referenced"));
    bindings.pop();
    let baseline = session.snapshot().memory.reserved_bytes;
    let memory = session.shared_memory_pool();
    let held = memory
        .reserve(allocation().memory_bytes() - baseline - 1024)
        .unwrap();
    let parses = NativeSqlDeclaration::declaration_parses();
    let error = prepare_declared_sql_operation(
        raw,
        &declaration,
        allocation(),
        &session,
        &bindings,
        None,
        &CancellationToken::default(),
    )
    .err()
    .expect("retained syntax needs a fresh metadata grant")
    .to_string();
    assert!(error.contains("memory reservation denied"), "{error}");
    assert_eq!(NativeSqlDeclaration::declaration_parses(), parses);
    drop(held);
    assert_eq!(session.snapshot().memory.reserved_bytes, baseline);
    let cancellation = CancellationToken::default();
    cancellation.cancel();
    assert!(
        prepare_declared_sql_operation(
            raw,
            &declaration,
            allocation(),
            &session,
            &bindings,
            None,
            &cancellation,
        )
        .is_err()
    );
    assert_eq!(session.snapshot().memory.reserved_bytes, baseline);
}

#[test]
fn normalization_requires_a_new_declaration_and_preserves_aggregate_names() {
    let session = session();
    let raw = "SELECT SUM(value + 1) FROM 'memory://input'";
    let declaration = NativeRelationDeclaration::sql(raw, vec![]).unwrap();
    let bindings = [binding(
        ResidentMemorySource::from_int64_range(&session, "value", 0, 1, 3).unwrap(),
    )];
    let mut request = sql_request(raw, allocation(), &bindings, None).unwrap();
    request.infer_defaults();
    // The complete-statement file route supplies this aggregate declaration.
    // A resident input exercises its alias rewrite without creating a file.
    request.vortex_aggregate = Some(
        r#"{"measures":[{"function":"sum","column":"value","alias":"total","argument_offset":1}]}"#
            .into(),
    );
    let normalized =
        crate::public_workflow_route::resident_sql::typed_statement(raw, &request, false);
    assert_ne!(
        normalized.as_ref(),
        raw,
        "fixture must exercise alias normalization"
    );
    let mut expected = None;
    for retained in [None, Some(&declaration)] {
        let before = NativeSqlDeclaration::declaration_parses();
        let (operation, _) = resident_relational::prepare_declared_in_session(
            raw,
            retained,
            &request,
            &session,
            |schemas| register_sources(schemas, &bindings, &CancellationToken::default()),
            &CancellationToken::default(),
        )
        .unwrap();
        assert_eq!(NativeSqlDeclaration::declaration_parses(), before + 1);
        let result = operation
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        let value = result.result_jsonl.value().to_owned();
        if let Some(expected) = &expected {
            assert_eq!(&value, expected);
        } else {
            expected = Some(value);
        }
    }
}
