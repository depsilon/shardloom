use super::*;
use crate::{
    local_primitives::prepared_dispatch,
    query_primitive::{VortexSimpleAggregateMeasure as Measure, VortexSimpleAggregateRequest},
    relational_query::VortexRelationalAggregate,
};
use serde_json::json;

#[test]
fn native_nested_source_dispatch_uses_referenced_types_including_empty_sources() {
    let fixed = FixedSizeListArray::try_new(
        PrimitiveArray::from_iter([1i64, 2, 3, 4]).into_array(),
        1,
        Validity::NonNullable,
        4,
    )
    .unwrap()
    .into_array();
    let detail = StructArray::new(
        FieldNames::from(["child"]),
        vec![lists()],
        4,
        Validity::NonNullable,
    )
    .into_array();
    let array = StructArray::new(
        FieldNames::from(["id", "items", "fixed", "detail"]),
        vec![
            PrimitiveArray::from_iter([1u32, 2, 3, 4]).into_array(),
            lists(),
            fixed,
            detail,
        ],
        4,
        Validity::NonNullable,
    )
    .into_array();
    for rows in [0, 4] {
        let fixture = Fixture::new(array.slice(0..rows).unwrap(), 2);
        let uri = DatasetUri::new(fixture.path().display().to_string()).unwrap();
        let project = VortexQueryPrimitiveRequest::project(
            uri.clone(),
            shardloom_plan::ProjectionRequest::columns(vec![column("id")]),
        );
        let source = prepared_dispatch::prepare_source(&project, policy()).unwrap();
        assert!(!prepared_dispatch::request_requires_relational(&source, &project).unwrap());
        assert!(prepared_dispatch::requires_relational(&source, None).unwrap());
        for name in ["items", "fixed", "detail"] {
            let mut count =
                VortexSimpleAggregateRequest::new(vec![Measure::new("count", None, "rows".into())]);
            count.group_by = vec![column(name)];
            let request = VortexQueryPrimitiveRequest::simple_aggregate(uri.clone(), count);
            assert!(prepared_dispatch::request_requires_relational(&source, &request).unwrap());
            for function in ["count", "count_distinct", "min", "max"] {
                let request = VortexQueryPrimitiveRequest::simple_aggregate(
                    uri.clone(),
                    VortexSimpleAggregateRequest::new(vec![Measure::new(
                        function,
                        Some(column(name)),
                        "value".into(),
                    )]),
                );
                assert!(prepared_dispatch::request_requires_relational(&source, &request).unwrap());
            }
        }
        let count = VortexQueryPrimitiveRequest::simple_aggregate(
            uri,
            VortexSimpleAggregateRequest::new(vec![Measure::new("count", None, "items".into())]),
        );
        assert!(!prepared_dispatch::request_requires_relational(&source, &count).unwrap());
        assert_eq!(
            source.retained_session().snapshot().prepared_source_opens,
            1
        );
        assert_eq!(source.retained_session().snapshot().completed_executions, 0);
    }
}

#[test]
fn native_nested_aggregate_handoff_reuses_one_source_and_fresh_state() {
    let fixture = fixture();
    let uri = DatasetUri::new(fixture.path().display().to_string()).unwrap();
    let mut count =
        VortexSimpleAggregateRequest::new(vec![Measure::new("count", None, "rows".into())]);
    count.group_by = vec![column("items")];
    let request = VortexQueryPrimitiveRequest::simple_aggregate(uri.clone(), count.clone());
    let source = prepared_dispatch::prepare_source(&request, policy()).unwrap();
    assert!(prepared_dispatch::request_requires_relational(&source, &request).unwrap());
    let session = source.retained_session();
    let prepared = prepare_relational_from_source(uri, source, policy(), |_| {
        Ok(VortexRelationalPlan::Aggregate(Box::new(
            VortexRelationalAggregate {
                input: fixture.scan(),
                group_by: count.group_by,
                measures: count.measures,
            },
        )))
    })
    .unwrap();
    let baseline = session.memory().snapshot().reserved_bytes;
    for execution in 1..=2 {
        let result = prepared
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        assert_eq!(
            json_rows(&result),
            vec![
                json!({"items":[9,null],"rows":1}),
                json!({"items":[],"rows":1}),
                json!({"items":null,"rows":1}),
                json!({"items":[-4],"rows":1}),
            ]
        );
        assert_eq!(result.execution.runtime.prepared_source_opens, 1);
        assert_eq!(result.execution.runtime.completed_executions, execution);
        drop(result);
        assert_eq!(session.memory().snapshot().reserved_bytes, baseline);
    }
    fixture.replace();
    assert!(
        prepared
            .collect_jsonl(&CancellationToken::default())
            .is_err()
    );
}
