//! Public aggregate ownership must admit scan reuse independently of workers.

use super::*;
use vortex::{
    VortexSessionDefault as _,
    array::{
        IntoArray as _, VortexSessionExecute as _,
        arrays::{PrimitiveArray, StructArray, VarBinViewArray},
        validity::Validity,
    },
    encodings::zstd::Zstd,
    file::WriteOptionsSessionExt as _,
    io::{
        runtime::{BlockingRuntime as _, current::CurrentThreadRuntime},
        session::RuntimeSessionExt as _,
    },
    session::VortexSession,
};

fn text_fixture() -> (Fixture, VortexQueryPrimitiveRequest, Vec<serde_json::Value>) {
    let fixture = Fixture::new();
    let values = (0..128)
        .map(|row| format!("{}:{}:東京", row % 8, "native-text".repeat(32)))
        .collect::<Vec<_>>();
    let plain = VarBinViewArray::from_iter_str(values.iter().map(String::as_str));
    let runtime = CurrentThreadRuntime::new();
    let session = VortexSession::default().with_handle(runtime.handle());
    let text =
        Zstd::from_var_bin_view_without_dict(&plain, 0, 128, &mut session.create_execution_ctx())
            .unwrap()
            .into_array();
    let array = StructArray::new(
        ["text", "category", "metric"].into(),
        vec![
            text,
            VarBinViewArray::from_iter_str(
                (0..128).map(|row| if row % 2 == 0 { "even" } else { "odd" }),
            )
            .into_array(),
            PrimitiveArray::from_iter(0_u64..128).into_array(),
        ],
        128,
        Validity::NonNullable,
    )
    .into_array();
    let mut bytes = Vec::new();
    runtime
        .block_on(
            session
                .write_options()
                .with_strategy(
                    crate::local_primitives::native_flat_layout::SequentialNativeFlatLayout::strategy(1),
                )
                .with_file_statistics(Vec::new())
                .write(&mut bytes, array.to_array_stream()),
        )
        .unwrap();
    std::fs::write(fixture.path(), bytes).unwrap();
    let mut request = fixture.request(
        VortexSimpleAggregateRequest::grouped(
            vec![
                ColumnRef::new("text").unwrap(),
                ColumnRef::new("category").unwrap(),
            ],
            vec![measure("count", None, "n")],
        )
        .with_order_by(vec![VortexAggregateOrderExpr::new("text", false)]),
    );
    request.predicate = Some(PredicateExpr::And(vec![
        PredicateExpr::StringContains {
            column: ColumnRef::new("text").unwrap(),
            needle: "native-text".into(),
            negated: false,
        },
        PredicateExpr::Compare {
            column: ColumnRef::new("metric").unwrap(),
            op: ComparisonOp::GtEq,
            value: StatValue::UInt64(64),
        },
    ]));
    let expected = values
        .iter()
        .take(8)
        .enumerate()
        .map(|(row, text)| {
            serde_json::json!({"text":text,"category":if row % 2 == 0 {"even"} else {"odd"},"n":8})
        })
        .collect();
    (fixture, request, expected)
}

#[test]
fn prepared_text_scan_reuse_uses_provider_session_memory_without_aggregate_workers() {
    for temporary in [false, true] {
        for parallelism in [1, 2] {
            let (fixture, request, expected) = text_fixture();
            let session = if temporary {
                ResidentVortexSession::for_external_cpu_pool(16 << 20, parallelism).unwrap()
            } else {
                ResidentVortexSession::new(16 << 20, parallelism).unwrap()
            };
            let prepared = prepare_aggregate_in_session(
                &request,
                VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(2, 4).unwrap(),
                &session,
            )
            .unwrap();
            // Two text grouping keys cannot enter the numeric/text worker pair.
            assert!(!prepared.worker_pool);
            assert_eq!(
                prepared.temporary_provider_drivers,
                temporary && parallelism > 1
            );
            for execution in 1..=2 {
                let result = prepared
                    .collect_jsonl(&CancellationToken::default())
                    .unwrap();
                certified(&result.execution, execution);
                assert_eq!(
                    result.execution.report.resource_envelope.max_parallelism,
                    parallelism
                );
                let details = payload(&result.execution.report);
                if temporary && parallelism > 1 {
                    // Temporary drivers have drained before the retained
                    // session snapshot. The scan report records their grant.
                    assert_eq!(
                        details["aggregate_provider_background_workers"],
                        parallelism - 1
                    );
                    assert_eq!(session.snapshot().provider_background_workers, 0);
                } else {
                    assert_eq!(
                        result.execution.runtime.provider_background_workers,
                        parallelism - 1
                    );
                }
                assert_eq!(details["aggregate_text_scan_decoded_chunks"], 1);
                assert_eq!(details["aggregate_text_scan_projected_reuses"], 1);
                let actual = result
                    .result_jsonl
                    .value()
                    .lines()
                    .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
                    .collect::<Vec<_>>();
                assert_eq!(actual, expected);
            }
            let token = CancellationToken::default();
            token.cancel();
            assert!(prepared.execute_cancellable(&token).is_err());
            assert_eq!(session.snapshot().completed_executions, 2);
            fixture.replace();
            assert!(prepared.execute().is_err());
            assert_eq!(session.snapshot().prepared_source_opens, 1);
            drop(prepared);
            assert_eq!(session.memory().snapshot().reserved_bytes, 0);
        }
    }
}

#[test]
fn prepared_text_scan_reuse_pressure_uses_the_existing_owner_and_releases_it() {
    let (_fixture, request, _) = text_fixture();
    let session = ResidentVortexSession::new(16 << 20, 2).unwrap();
    let prepared = prepare_aggregate_in_session(
        &request,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(2, 4).unwrap(),
        &session,
    )
    .unwrap();
    assert!(!prepared.worker_pool);
    let before = session.memory().snapshot();
    let pressure = session
        .memory()
        .reserve(before.limit_bytes - before.reserved_bytes - 1023)
        .unwrap();
    let error = prepared.execute().err().unwrap();
    assert!(
        error.to_string().contains("memory reservation denied"),
        "{error}"
    );
    assert!(session.memory().snapshot().denied_reservations > before.denied_reservations);
    drop(pressure);
    // A denied attempt retains neither partial groups nor a reusable answer.
    let result = prepared.execute().unwrap();
    assert_eq!(
        payload(&result.report)["aggregate_text_scan_projected_reuses"],
        1
    );
    drop(prepared);
    assert_eq!(session.memory().snapshot().reserved_bytes, 0);
}

#[test]
fn ordinary_text_scan_reuse_gets_the_same_resident_scan_memory() {
    let (fixture, request, expected) = text_fixture();
    let scan = crate::local_primitives::read_local_vortex_simple_aggregate_scan(
        request.source_uri.as_ref().unwrap(),
        &fixture.path(),
        &request,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(2, 4).unwrap(),
    )
    .unwrap();
    let details: serde_json::Value = serde_json::from_str(&scan.result_summary).unwrap();
    assert_eq!(details["aggregate_text_scan_decoded_chunks"], 1);
    assert_eq!(details["aggregate_text_scan_projected_reuses"], 1);
    assert_eq!(details["values"], serde_json::Value::Array(expected));
}
