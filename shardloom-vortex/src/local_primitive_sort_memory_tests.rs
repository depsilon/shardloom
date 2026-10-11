use super::tests::{unique_vortex_path, write_array};
use super::{
    VortexLocalPrimitiveExecutionPolicy, VortexQueryPrimitiveRequest, VortexSortRowsRequest,
    materialize_local_vortex_sort_output_rows_by_source_ordinals,
};
use crate::local_primitives::execution::execute_partitioned_in_session;
use shardloom_core::{DatasetUri, ExecutionResourceOrigin, ExecutionResources, Result, StatValue};
use shardloom_exec::compute_pool::CancellationToken;
use shardloom_plan::ProjectionRequest;
use std::{
    path::{Path, PathBuf},
    sync::atomic::{AtomicUsize, Ordering},
};

const SESSION_BYTES: u64 = 8 << 20;
static NEXT_FIXTURE: AtomicUsize = AtomicUsize::new(0);
const SORT_COLUMNS: [&str; 10] = [
    "key", "payload", "c0", "c1", "c2", "c3", "c4", "c5", "c6", "c7",
];

struct Fixture(Vec<PathBuf>);

impl Fixture {
    fn new() -> Self {
        let id = NEXT_FIXTURE.fetch_add(1, Ordering::Relaxed);
        let left = unique_vortex_path(&format!("sort-owned-wide-left-{id}"));
        let right = unique_vortex_path(&format!("sort-owned-wide-right-{id}"));
        write_wide_nullable_fixture(&left).expect("left fixture");
        std::fs::copy(&left, &right).expect("copy fixture");
        Self(vec![left, right])
    }

    fn path(&self) -> &Path {
        &self.0[0]
    }

    fn sources(&self) -> [DatasetUri; 2] {
        self.0
            .iter()
            .map(|path| DatasetUri::new(path.display().to_string()).expect("fixture URI"))
            .collect::<Vec<_>>()
            .try_into()
            .expect("two sources")
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        for path in &self.0 {
            let _ = std::fs::remove_file(path);
        }
    }
}

fn write_wide_nullable_fixture(path: &Path) -> Result<()> {
    use vortex::array::IntoArray as _;
    use vortex::array::arrays::{PrimitiveArray, StructArray, VarBinViewArray};
    use vortex::array::dtype::FieldNames;
    use vortex::array::validity::Validity;

    let mut fields = vec![
        [1_i64, 2, 3]
            .into_iter()
            .collect::<PrimitiveArray>()
            .into_array(),
        VarBinViewArray::from_iter_nullable_str([Some("a\"雪"), None, Some("long\nvalue")])
            .into_array(),
    ];
    for column in 0..8_i64 {
        fields.push(
            [10 + column, 20 + column, 30 + column]
                .into_iter()
                .collect::<PrimitiveArray>()
                .into_array(),
        );
    }
    let array = StructArray::try_new(
        FieldNames::from(SORT_COLUMNS),
        fields,
        3,
        Validity::NonNullable,
    )
    .map_err(super::vortex_error)?;
    write_array(path, &array.into_array())
}

fn session_policy() -> VortexLocalPrimitiveExecutionPolicy {
    VortexLocalPrimitiveExecutionPolicy::from_resources(
        ExecutionResources::from_bytes(SESSION_BYTES, 1, ExecutionResourceOrigin::ExecutionCall)
            .expect("fixture allocation"),
    )
    .expect("fixture policy")
}

fn request(uri: DatasetUri) -> VortexQueryPrimitiveRequest {
    VortexQueryPrimitiveRequest::sort_rows(
        uri,
        ProjectionRequest::All,
        None,
        VortexSortRowsRequest::new(vec![crate::VortexAggregateOrderExpr::new("key", true)]),
        3,
    )
}

fn expected_rows() -> Vec<serde_json::Value> {
    [3_i64, 3, 2]
        .into_iter()
        .map(|key| {
            let mut row = serde_json::Map::new();
            row.insert("key".into(), serde_json::json!(key));
            row.insert(
                "payload".into(),
                match key {
                    3 => serde_json::json!("long\nvalue"),
                    2 => serde_json::Value::Null,
                    _ => serde_json::json!("a\"雪"),
                },
            );
            for column in 0..8_i64 {
                row.insert(format!("c{column}"), serde_json::json!(key * 10 + column));
            }
            serde_json::Value::Object(row)
        })
        .collect()
}

fn decoded_result_rows(summary: &str) -> Vec<serde_json::Value> {
    let start = summary.find('{').expect("JSON result summary");
    serde_json::from_str::<serde_json::Value>(&summary[start..])
        .expect("valid JSON suffix")
        .get("values")
        .and_then(serde_json::Value::as_array)
        .expect("complete result values")
        .clone()
}

#[test]
fn partitioned_wide_sort_keeps_complete_values_and_releases_session_memory() {
    let fixture = Fixture::new();
    let sources = fixture.sources();
    let session = crate::resident_session::ResidentVortexSession::new(SESSION_BYTES, 1)
        .expect("resident session");
    let before = session.snapshot();
    let report = execute_partitioned_in_session(
        &request(sources[0].clone()),
        &sources,
        session_policy(),
        &session,
        &CancellationToken::default(),
    )
    .expect("partitioned sort");
    let summary = report
        .result_summary
        .as_deref()
        .expect("sort result summary");
    let payload = serde_json::from_str::<serde_json::Value>(
        &summary[summary.find('{').expect("JSON summary")..],
    )
    .expect("valid summary JSON");

    assert_eq!(
        report.status,
        super::VortexLocalPrimitiveExecutionStatus::Executed
    );
    assert!(!report.fallback_execution_allowed);
    assert_eq!(decoded_result_rows(summary), expected_rows());
    assert_eq!(payload["wide_output_second_pass"], true);
    assert!(session.snapshot().completed_executions > before.completed_executions);
    assert_eq!(session.snapshot().memory.reserved_bytes, 0);
}

#[test]
fn selected_sort_rows_retain_payload_credits_after_context_returns() {
    let fixture = Fixture::new();
    let session = crate::resident_session::ResidentVortexSession::new(SESSION_BYTES, 1)
        .expect("resident session");
    let columns = vec!["key".to_string(), "payload".to_string()];
    let request = request(DatasetUri::new(fixture.path().display().to_string()).unwrap());
    let selected = [2, 0, 2];
    let materialized = session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            materialize_local_vortex_sort_output_rows_by_source_ordinals(
                fixture.path(),
                &request,
                &columns,
                &selected,
                None,
                session_policy(),
                context.native_session(),
                context.runtime(),
                Some(context),
            )
        })
        .expect("materialize selected rows");

    assert_eq!(
        materialized.rows,
        vec![
            vec![StatValue::Int64(3), StatValue::Utf8("long\nvalue".into())],
            vec![StatValue::Int64(1), StatValue::Utf8("a\"雪".into())],
            vec![StatValue::Int64(3), StatValue::Utf8("long\nvalue".into())],
        ]
    );
    assert!(session.snapshot().memory.reserved_bytes > 0);
    drop(materialized);
    assert_eq!(session.snapshot().memory.reserved_bytes, 0);
}

#[test]
fn selected_sort_materialization_denial_releases_temporary_credits_and_can_retry() {
    let fixture = Fixture::new();
    let session = crate::resident_session::ResidentVortexSession::new(SESSION_BYTES, 1)
        .expect("resident session");
    let columns = vec!["key".to_string(), "payload".to_string()];
    let request = request(DatasetUri::new(fixture.path().display().to_string()).unwrap());
    let selected = [2, 0, 2];
    let held_bytes = SESSION_BYTES - 96 * 1024;
    let held = session
        .reserve_input_scratch(held_bytes)
        .expect("hold all but the bounded materialization allowance");
    let denied = session.with_native_execution_context(&CancellationToken::default(), |context| {
        materialize_local_vortex_sort_output_rows_by_source_ordinals(
            fixture.path(),
            &request,
            &columns,
            &selected,
            None,
            session_policy(),
            context.native_session(),
            context.runtime(),
            Some(context),
        )
    });
    let error = match denied {
        Ok(_) => panic!("held credits should deny materialization"),
        Err(error) => error,
    };
    assert!(
        error.to_string().contains("memory reservation denied"),
        "{error}"
    );
    assert_eq!(session.snapshot().memory.reserved_bytes, held_bytes);
    drop(held);
    assert_eq!(session.snapshot().memory.reserved_bytes, 0);

    let materialized = session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            materialize_local_vortex_sort_output_rows_by_source_ordinals(
                fixture.path(),
                &request,
                &columns,
                &selected,
                None,
                session_policy(),
                context.native_session(),
                context.runtime(),
                Some(context),
            )
        })
        .expect("retry selected rows");
    assert_eq!(
        materialized.rows,
        vec![
            vec![StatValue::Int64(3), StatValue::Utf8("long\nvalue".into())],
            vec![StatValue::Int64(1), StatValue::Utf8("a\"雪".into())],
            vec![StatValue::Int64(3), StatValue::Utf8("long\nvalue".into())],
        ]
    );
    drop(materialized);
    assert_eq!(session.snapshot().memory.reserved_bytes, 0);

    let cancelled = CancellationToken::default();
    cancelled.cancel();
    let missing = fixture.0[0].with_file_name("missing-sort-source.vortex");
    let error = session.with_native_execution_context(&cancelled, |context| {
        materialize_local_vortex_sort_output_rows_by_source_ordinals(
            &missing,
            &request,
            &columns,
            &selected,
            None,
            session_policy(),
            context.native_session(),
            context.runtime(),
            Some(context),
        )
    });
    let error = match error {
        Ok(_) => panic!("pre-cancelled call must reject before opening"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("cancelled"), "{error}");
    assert!(!missing.exists());
    assert_eq!(session.snapshot().memory.reserved_bytes, 0);
}
