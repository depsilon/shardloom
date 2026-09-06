//! Existing complete native queries; only the independent oracle is local.

use super::*;
use crate::{
    VortexLocalPrimitiveExecutionPolicy, VortexLocalPrimitiveExecutionReport,
    VortexLocalPrimitiveExecutionStatus, VortexQueryPrimitiveRequest, VortexSimpleAggregateMeasure,
    VortexSimpleAggregateRequest, execute_vortex_local_primitive_with_policy,
};

#[derive(Debug, Clone, Copy)]
enum Query {
    Count,
    Group,
    Contains,
    NotContains,
}

impl Query {
    fn request(self, path: &Path, rows: usize) -> VortexQueryPrimitiveRequest {
        let uri = DatasetUri::new(path.display().to_string()).unwrap();
        let column = ColumnRef::new(COLUMN).unwrap();
        match self {
            Self::Count => VortexQueryPrimitiveRequest::simple_aggregate(
                uri,
                VortexSimpleAggregateRequest::new(vec![VortexSimpleAggregateMeasure::new(
                    "count",
                    Some(column),
                    "present".to_owned(),
                )]),
            ),
            Self::Group => VortexQueryPrimitiveRequest::simple_aggregate(
                uri,
                VortexSimpleAggregateRequest::grouped(
                    vec![column],
                    vec![VortexSimpleAggregateMeasure::new(
                        "count",
                        None,
                        "occurrences".to_owned(),
                    )],
                ),
            )
            .with_source_order_limit(rows),
            Self::Contains | Self::NotContains => VortexQueryPrimitiveRequest::count_where(
                uri,
                PredicateExpr::StringContains {
                    column,
                    needle: "needle".to_owned(),
                    negated: matches!(self, Self::NotContains),
                },
            ),
        }
    }
}

struct Oracle {
    groups: BTreeMap<Option<String>, u64>,
    present: u64,
    contains: u64,
    not_contains: u64,
    values_sha256: String,
}

impl Oracle {
    fn new(input: &Input) -> Self {
        let mut groups = BTreeMap::<Option<String>, u64>::new();
        let mut contains = 0;
        let mut not_contains = 0;
        for value in &input.values {
            *groups.entry(value.clone()).or_default() += 1;
            if let Some(value) = value {
                if value.contains("needle") {
                    contains += 1;
                } else {
                    not_contains += 1;
                }
            }
        }
        let complete = groups
            .iter()
            .map(|(key, count)| json!([key, count]))
            .collect::<Vec<_>>();
        let values_sha256 = digest(&serde_json::to_vec(&complete).unwrap());
        Self {
            groups,
            present: contains + not_contains,
            contains,
            not_contains,
            values_sha256,
        }
    }

    fn verify(&self, query: Query, report: &VortexLocalPrimitiveExecutionReport) {
        assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
        assert!(!report.has_errors(), "{:?}", report.diagnostics);
        assert!(!report.fallback_execution_allowed);
        assert!(!report.external_effects_executed);
        assert!(!report.write_io && !report.spill_io_performed && !report.object_store_io);
        let summary = report.result_summary.as_deref().unwrap();
        let payload: Value =
            serde_json::from_str(summary.rsplit_once(" values=").unwrap().1).unwrap();
        match query {
            Query::Count => {
                assert_eq!(payload["rows"], 1);
                assert_eq!(payload["values"].as_object().unwrap().len(), 1);
                assert_eq!(payload["values"]["present"], self.present);
            }
            Query::Group => {
                let mut actual = BTreeMap::new();
                for row in payload["values"].as_array().unwrap() {
                    assert_eq!(row.as_object().unwrap().len(), 2);
                    assert!(row.as_object().unwrap().contains_key(COLUMN));
                    let key = if row[COLUMN].is_null() {
                        None
                    } else {
                        Some(row[COLUMN].as_str().unwrap().to_owned())
                    };
                    assert!(
                        actual
                            .insert(key, row["occurrences"].as_u64().unwrap())
                            .is_none(),
                        "duplicate group in complete result"
                    );
                }
                assert_eq!(
                    payload["rows"].as_u64().unwrap(),
                    u64::try_from(actual.len()).unwrap()
                );
                assert_eq!(actual, self.groups);
            }
            Query::Contains => assert_eq!(payload["count"], self.contains),
            Query::NotContains => assert_eq!(payload["count"], self.not_contains),
        }
    }
}

fn observed(report: &VortexLocalPrimitiveExecutionReport) -> Value {
    json!({
        "rows_scanned": report.rows_scanned, "rows_selected": report.rows_selected,
        "rows_projected": report.rows_projected, "arrays_read": report.arrays_read_count,
        "upstream_scan_called": report.upstream_scan_called, "data_read": report.data_read,
        "data_decoded": report.data_decoded, "data_materialized": report.data_materialized,
        "row_read": report.row_read, "arrow_converted": report.arrow_converted,
        "filter_pushdown_applied": report.filter_pushdown_applied,
        "upstream_filter_expression_used": report.upstream_filter_expression_used,
        "state_family": report.state_budget.state_family,
        "max_parallelism_requested": report.max_parallelism_requested,
        "fallback_execution_allowed": report.fallback_execution_allowed,
        "external_effects_executed": report.external_effects_executed,
    })
}

pub(super) fn run(
    path: &Path,
    input: &Input,
    reuse: usize,
    once: u64,
    case_started: Instant,
) -> (Value, Value) {
    let oracle = Oracle::new(input);
    let queries = [
        Query::Count,
        Query::Group,
        Query::Contains,
        Query::NotContains,
    ];
    let requests = queries.map(|query| query.request(path, input.values.len()));
    let mut times: [Vec<u64>; 4] = std::array::from_fn(|_| Vec::with_capacity(reuse));
    let mut validation_times: [Vec<u64>; 4] = std::array::from_fn(|_| Vec::with_capacity(reuse));
    let mut evidence: [Vec<Value>; 4] = std::array::from_fn(|_| Vec::with_capacity(reuse));
    let mut all_query_work = vec![0_u64; reuse];
    let mut continuous_round_completion = Vec::with_capacity(reuse);
    // Each checkpoint is a real chronological prefix of complete four-query
    // rounds; do not sum unrelated first samples taken after other warm runs.
    for (iteration, accumulated) in all_query_work.iter_mut().enumerate() {
        for (index, query) in queries.iter().copied().enumerate() {
            let policy = VortexLocalPrimitiveExecutionPolicy::new(1).unwrap();
            let start = Instant::now();
            let report =
                execute_vortex_local_primitive_with_policy(&requests[index], policy).unwrap();
            let elapsed = nanos(start);
            let start = Instant::now();
            oracle.verify(query, &report);
            validation_times[index].push(nanos(start));
            times[index].push(elapsed);
            *accumulated = accumulated.checked_add(elapsed).unwrap();
            evidence[index].push(json!({"iteration": iteration, "execution": observed(&report)}));
        }
        continuous_round_completion.push(nanos(case_started));
    }
    let samples = queries.iter().enumerate().map(|(index, query)| {
        let expected_sha = match query {
            Query::Group => oracle.values_sha256.clone(),
            Query::Count => digest(&serde_json::to_vec(&json!({"present": oracle.present})).unwrap()),
            Query::Contains => digest(&serde_json::to_vec(&json!({"count": oracle.contains})).unwrap()),
            Query::NotContains => digest(&serde_json::to_vec(&json!({"count": oracle.not_contains})).unwrap()),
        };
        json!({
            "query": format!("{query:?}"), "elapsed_nanos": times[index],
            "independent_verification_nanos": validation_times[index], "execution_samples": evidence[index],
            "complete_verified_query_values_sha256": expected_sha,
            "full_values_verified": true,
        })
    }).collect::<Vec<_>>();
    let mut checkpoints = Vec::new();
    for count in [1, 10, 100] {
        if count > reuse {
            continue;
        }
        let query_nanos = all_query_work
            .iter()
            .take(count)
            .try_fold(0_u64, |sum, value| sum.checked_add(*value))
            .unwrap();
        checkpoints.push(json!({
            "reuse_count_per_query": count, "query_families": queries.len(),
            "full_query_calls": count * queries.len(), "one_time_operational_nanos": once,
            "all_full_query_nanos": query_nanos, "operational_lifecycle_nanos": once.checked_add(query_nanos).unwrap(),
            "scope": "sum of source+native_session_setup+train/build+writer_strategy_setup+write/publication+driver_join+extra_sync+full_hash+footer_reopen+retained_writer_summary_release once, plus each actual full query; not continuous wall time; diagnostic inventory, independent validation, source hashing and intervening bookkeeping excluded",
            "continuous_case_through_round_validation_nanos": continuous_round_completion[count - 1],
            "continuous_scope": "elapsed from before source preparation through complete four-query round validation, including diagnostics, source hashing and bookkeeping; outer shared write context and temporary directory setup excluded",
            "reuse_scope": "immutable file reused; each existing native query call prepares/opens its source; no cached answers or controlled cold-cache claim",
        }));
    }
    (json!(samples), json!(checkpoints))
}
