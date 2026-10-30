//! Immutable retained identities survive mixed SQL and typed declaration lowering.

use super::*;
use serde_json::{Value, json};
use shardloom_vortex::relational_query::{VortexRelationalSet, VortexRelationalSetKind};

fn verify(declaration: &NativeRelationDeclaration, expected: &Value, retained: u64, reused: u64) {
    let resources = ExecutionResources::from_bytes(
        32 << 20,
        1,
        shardloom_core::ExecutionResourceOrigin::Context,
    )
    .unwrap();
    let session = ResidentVortexSession::new(32 << 20, 1).unwrap();
    let (operation, _) = declaration
        .prepare(
            resources,
            &session,
            &[],
            None,
            &CancellationToken::default(),
        )
        .unwrap();
    let baseline = session.snapshot().memory.reserved_bytes;
    for _ in 0..2 {
        let result = operation
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        let rows = result
            .result_jsonl
            .value()
            .lines()
            .map(|row| serde_json::from_str::<Value>(row).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(json!(rows), *expected);
        assert_eq!(
            result.execution.composition.materialized_relations,
            retained
        );
        assert_eq!(result.execution.composition.materialized_reuses, reused);
        let fields = crate::native::execution_fields(&result.execution, false)
            .into_iter()
            .collect::<BTreeMap<_, _>>();
        for (name, value) in result.execution.composition.counters() {
            assert_eq!(fields[name], value.to_string());
        }
        assert_eq!(fields["recursion_limit"], "1000");
        assert_eq!(
            fields["local_primitive_native_io_pushdown_proof_basis"],
            result
                .execution
                .native_io_certificate
                .source_pushdown_report
                .proof_basis
        );
        drop(result);
        assert_eq!(session.snapshot().memory.reserved_bytes, baseline);
    }
}

#[test]
fn retained_typed_declaration_reuses_same_child_and_scopes_equal_templates_to_their_inputs() {
    let template = Arc::new(Plan::Input(0));
    let retained = |value| {
        Arc::new(
            NativeRelationDeclaration::compose(
                Plan::Materialized(template.clone()),
                vec![Arc::new(
                    NativeRelationDeclaration::sql(&format!("SELECT {value} AS n"), vec![])
                        .unwrap(),
                )],
            )
            .unwrap(),
        )
    };
    let first = retained(1);
    for shared in [true, false] {
        let second = if shared { first.clone() } else { retained(9) };
        let union = NativeRelationDeclaration::compose(
            Plan::Set(Box::new(VortexRelationalSet {
                left: Plan::Input(0),
                right: Plan::Input(1),
                kind: VortexRelationalSetKind::UnionAll,
            })),
            vec![first.clone(), second],
        )
        .unwrap();
        verify(
            &union,
            &json!([{"n":1},{"n":if shared {1} else {9}}]),
            if shared { 1 } else { 2 },
            u64::from(shared),
        );
    }
}

#[test]
fn retained_parameters_named_snapshot_grouping_filter_and_window_share_one_native_action() {
    let mut seed = Arc::new(NativeRelationDeclaration::sql("SELECT 0 AS seed", vec![]).unwrap());
    let query = "WITH RECURSIVE start(n) AS MATERIALIZED (SELECT seed FROM named), seq(n) AS (SELECT n FROM start UNION ALL SELECT n+1 FROM seq WHERE n<:stop), grouped AS (SELECT n,GROUPING(n) AS mask,COUNT(*) AS cnt,SUM(n) FILTER (WHERE n%2=0) AS even_total FROM seq GROUP BY GROUPING SETS ((n),())) SELECT n,mask,cnt,even_total,SUM(cnt) OVER () AS total FROM grouped ORDER BY mask,n NULLS LAST";
    let declaration = NativeRelationDeclaration::sql_with_parameters(
        query,
        vec![("named".into(), seed.clone())],
        BTreeMap::from([("stop".into(), shardloom_core::ScalarValue::Int64(3))]),
    )
    .unwrap();
    seed = Arc::new(NativeRelationDeclaration::sql("SELECT 99 AS seed", vec![]).unwrap());
    let newer =
        NativeRelationDeclaration::sql("SELECT seed FROM named", vec![("named".into(), seed)])
            .unwrap();
    verify(&newer, &json!([{"seed":99}]), 0, 0);
    verify(
        &declaration,
        &json!([
            {"n":0,"mask":0,"cnt":1,"even_total":0.0,"total":8.0},
            {"n":1,"mask":0,"cnt":1,"even_total":null,"total":8.0},
            {"n":2,"mask":0,"cnt":1,"even_total":2.0,"total":8.0},
            {"n":3,"mask":0,"cnt":1,"even_total":null,"total":8.0},
            {"n":null,"mask":1,"cnt":4,"even_total":2.0,"total":8.0},
        ]),
        1,
        0,
    );
}

#[test]
fn retained_declarations_reject_data_dependent_or_outer_captured_definitions_before_demand() {
    let options = r#"{"index":"entity","columns":"category","values":"amount","aggregate":"sum"}"#;
    let pivot = Arc::new(NativeRelationDeclaration::sql(&format!(
        "SELECT * FROM PIVOT((SELECT 1 AS entity,'a' AS category,4 AS amount), '{options}') AS p"
    ), vec![]).unwrap());
    let error = NativeRelationDeclaration::sql(
        "WITH retained AS MATERIALIZED (SELECT * FROM named) SELECT * FROM retained",
        vec![("named".into(), pivot)],
    )
    .err()
    .unwrap();
    assert!(
        error
            .to_string()
            .contains("static parameter-independent schemas"),
        "{error}"
    );
    let error = NativeRelationDeclaration::sql(
        "WITH retained AS MATERIALIZED (SELECT outer.n AS n) SELECT n FROM retained",
        vec![],
    )
    .err()
    .unwrap();
    assert!(
        error.to_string().contains("parameter-independent")
            || error.to_string().contains("not admitted"),
        "{error}"
    );
}
