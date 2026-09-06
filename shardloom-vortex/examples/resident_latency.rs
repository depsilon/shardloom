#[cfg(unix)]
use std::{hint::black_box, time::Instant};

#[cfg(unix)]
fn latency(samples: &[u64], boundary: &str) -> serde_json::Value {
    let mut ordered = samples.to_vec();
    ordered.sort_unstable();
    let percentile = |percent: usize| ordered[(ordered.len() * percent).div_ceil(100) - 1];
    serde_json::json!({
        "timing_boundary": boundary, "iterations": samples.len(),
        "percentile_method": "nearest_rank", "raw_nanos": samples,
        "p50_nanos": percentile(50), "p95_nanos": percentile(95),
        "p99_nanos": percentile(99), "max_nanos": ordered.last(),
    })
}

#[cfg(unix)]
type ArrayRows = Vec<String>;

#[cfg(unix)]
fn array_values(
    result: &shardloom_vortex::resident_session::OwnedVortexResultBatch,
) -> Result<ArrayRows, Box<dyn std::error::Error>> {
    use vortex::{VortexSessionDefault as _, array::VortexSessionExecute as _};
    let session = vortex::session::VortexSession::default();
    let mut context = session.create_execution_ctx();
    let mut values = Vec::new();
    for array in result.arrays() {
        for row in 0..array.len() {
            // Read logical rows through the provider. Physical child-slot
            // names need not be logical struct field names for every encoding.
            values.push(array.execute_scalar(row, &mut context)?.to_string());
        }
    }
    Ok(values)
}

#[cfg(unix)]
fn measure_collect(
    session: &shardloom_vortex::resident_session::ResidentVortexSession,
    request: shardloom_vortex::query_primitive::VortexQueryPrimitiveRequest,
    iterations: usize,
) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    use shardloom_vortex::local_primitives::collect::prepare_rows_in_session;
    let request = request.with_source_order_limit(64);
    let started = Instant::now();
    let prepared = prepare_rows_in_session(&request, session)?;
    let bind_seconds = started.elapsed().as_secs_f64();
    let reference_arrays = prepared.execute_arrays()?;
    let expected_values = array_values(&reference_arrays)?;
    let rows = reference_arrays.row_count();
    drop(reference_arrays);
    let reference_json = prepared.execute()?;
    let expected_json = reference_json.values_json.value().clone();
    drop(reference_json);
    let mut array_nanos = Vec::with_capacity(iterations);
    let mut json_nanos = Vec::with_capacity(iterations);
    for _ in 0..iterations {
        let started = Instant::now();
        let result = black_box(prepared.execute_arrays()?);
        array_nanos.push(u64::try_from(started.elapsed().as_nanos())?);
        if result.row_count() != rows || array_values(&result)? != expected_values {
            return Err("complete native array values changed".into());
        }
        drop(result);
        let started = Instant::now();
        let result = black_box(prepared.execute()?);
        json_nanos.push(u64::try_from(started.elapsed().as_nanos())?);
        if result.rows != rows || result.values_json.value() != &expected_json {
            return Err("complete JSON values changed".into());
        }
    }
    Ok(serde_json::json!({
        "prepare_seconds": bind_seconds, "rows": rows, "requested_source_order_limit": 64,
        "native_arrays": latency(&array_nanos, "prepared native scan through owned array return; value verification and result drop excluded"),
        "bounded_json": latency(&json_nanos, "prepared native scan and complete bounded JSON sink; value verification and returned JSON drop excluded"),
        "validation": "complete values compared to each surface's initial native result; regression parity, not independent oracle",
    }))
}

#[cfg(unix)]
fn measure_count_where(
    session: &shardloom_vortex::resident_session::ResidentVortexSession,
    request: &shardloom_vortex::query_primitive::VortexQueryPrimitiveRequest,
    iterations: usize,
    independent_expected: Option<u64>,
) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    use shardloom_vortex::local_primitives::{
        VortexLocalPrimitiveExecutionPolicy, prepared_count::prepare_count_where_in_session,
    };
    let before = session.snapshot();
    let started = Instant::now();
    let prepared = prepare_count_where_in_session(
        request,
        VortexLocalPrimitiveExecutionPolicy::new(2)?,
        session,
    )?;
    let prepare_seconds = started.elapsed().as_secs_f64();
    let warmup = prepared.execute()?;
    let expected = independent_expected.unwrap_or(warmup.count);
    if warmup.count != expected {
        return Err(
            "prepared filtered count differs from supplied independent expected count".into(),
        );
    }
    drop(warmup);
    let mut nanos = Vec::with_capacity(iterations);
    for _ in 0..iterations {
        let started = Instant::now();
        let result = black_box(prepared.execute()?);
        nanos.push(u64::try_from(started.elapsed().as_nanos())?);
        if result.count != expected || !result.native_io_certificate.is_certified() {
            return Err("complete prepared filtered count or native certificate changed".into());
        }
    }
    drop(prepared);
    let after = session.snapshot();
    if after.prepared_source_opens - before.prepared_source_opens != 1
        || after.completed_executions - before.completed_executions
            != u64::try_from(iterations)? + 1
    {
        return Err(
            "prepared filtered count did not retain one source and execute every call".into(),
        );
    }
    Ok(serde_json::json!({
        "prepare_seconds": prepare_seconds, "count": expected, "warmups": 1,
        "source_opens": 1, "completed_executions": iterations + 1,
        "native_count_report": latency(&nanos, "prepared full predicate scan or metadata pruning through complete scalar/report/native certificate return; verification and returned report drop excluded"),
        "validation": if independent_expected.is_some() { "caller_supplied_independent_exact_count" } else { "repeated parity against initial native result; not an independent oracle" },
        "transport_comparison": "requires identical fixture and predicate; do not subtract unlike workload timing surfaces",
    }))
}

#[cfg(unix)]
#[allow(clippy::too_many_lines)] // Keep example option admission and separately scoped timing surfaces together.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    use shardloom_core::{ColumnRef, ComparisonOp, DatasetUri, PredicateExpr, StatValue};
    use shardloom_plan::ProjectionRequest;
    use shardloom_vortex::{
        query_primitive::VortexQueryPrimitiveRequest, resident_session::ResidentVortexSession,
    };
    let mut args = std::env::args().skip(1);
    let path = args.next().ok_or(
        "usage: resident_latency INPUT [ITERATIONS] [--columns CSV] [--filter-ge COLUMN INTEGER] [--count-where] [--expected-count-where INTEGER]",
    )?;
    let iterations: usize = args.next().map_or(Ok(10_000), |arg| arg.parse())?;
    if !(100..=1_000_000).contains(&iterations) {
        return Err("iterations must be 100..=1000000".into());
    }
    let mut columns = None;
    let mut predicate = None;
    let mut count_where = false;
    let mut expected_count_where = None;
    while let Some(flag) = args.next() {
        match flag.as_str() {
            "--columns" => columns = Some(args.next().ok_or("--columns requires CSV names")?),
            "--count-where" => count_where = true,
            "--expected-count-where" => {
                expected_count_where = Some(
                    args.next()
                        .ok_or("--expected-count-where requires an integer")?
                        .parse::<u64>()?,
                );
            }
            "--filter-ge" => {
                let column = ColumnRef::new(args.next().ok_or("--filter-ge requires a column")?)?;
                let threshold = args
                    .next()
                    .ok_or("--filter-ge requires an integer")?
                    .parse()?;
                predicate = Some(PredicateExpr::Compare {
                    column,
                    op: ComparisonOp::GtEq,
                    value: StatValue::Int64(threshold),
                });
            }
            _ => return Err(format!("unknown argument: {flag}").into()),
        }
    }
    if predicate.is_some() && columns.is_none() && !count_where {
        return Err("--filter-ge requires --columns or --count-where".into());
    }
    if count_where && predicate.is_none() || expected_count_where.is_some() && !count_where {
        return Err(
            "--count-where requires --filter-ge; an expected count requires --count-where".into(),
        );
    }
    let started = Instant::now();
    let session = ResidentVortexSession::new(256 * 1024 * 1024, 2)?;
    let source = session.prepare_file(&path)?;
    let count = source.prepare_count();
    let prepare_seconds = started.elapsed().as_secs_f64();
    let expected = count.execute()?;
    let mut nanos = Vec::with_capacity(iterations);
    for _ in 0..iterations {
        let started = Instant::now();
        let rows = black_box(count.execute()?);
        nanos.push(u64::try_from(started.elapsed().as_nanos())?);
        if rows != expected {
            return Err("resident count changed".into());
        }
    }
    let mut cases = serde_json::Map::new();
    cases.insert(
        "metadata_count".into(),
        latency(
            &nanos,
            "prepared native footer count including admission and source generation validation",
        ),
    );
    if count_where {
        let request = VortexQueryPrimitiveRequest::count_where(
            DatasetUri::new(path.clone())?,
            predicate
                .clone()
                .ok_or("filtered count predicate is missing")?,
        );
        cases.insert(
            "filtered_count".into(),
            measure_count_where(&session, &request, iterations, expected_count_where)?,
        );
    }
    if let Some(columns) = columns {
        let projection = ProjectionRequest::columns(
            columns
                .split(',')
                .map(ColumnRef::new)
                .collect::<Result<Vec<_>, _>>()?,
        );
        let uri = DatasetUri::new(path)?;
        let mut requests = vec![(
            "projection",
            VortexQueryPrimitiveRequest::project(uri.clone(), projection.clone()),
        )];
        if let Some(predicate) = predicate {
            requests.push((
                "filter_projection",
                VortexQueryPrimitiveRequest::filter_and_project(uri, predicate, projection),
            ));
        }
        for (name, request) in requests {
            cases.insert(name.into(), measure_collect(&session, request, iterations)?);
        }
    }
    drop(count);
    drop(source);
    let snapshot = session.snapshot();
    if snapshot.memory.reserved_bytes != 0 {
        return Err(
            "owned native buffers remain after every prepared handle and result was dropped".into(),
        );
    }
    println!(
        "{}",
        serde_json::json!({
            "schema_version": "shardloom.resident_operation_latency.v1",
            "prepare_seconds": prepare_seconds, "iterations": iterations, "rows": expected,
            "cases": cases, "prepared_source_opens": snapshot.prepared_source_opens,
            "completed_executions": snapshot.completed_executions,
            "peak_provider_reserved_bytes": snapshot.memory.peak_reserved_bytes,
            "final_owned_reserved_bytes": snapshot.memory.reserved_bytes,
            "fallback_attempted": false, "external_engine_invoked": false,
            "scope": "prepared Rust metadata/optional full filtered count/bounded array and JSON calls; not Python, fresh-process, mixed-load or durable ingest latency",
            "claim_gate_status": "not_claim_grade", "cache_policy": "provider reader/layout metadata reused; OS cache uncontrolled; no result cache",
        })
    );
    Ok(())
}

#[cfg(not(unix))]
fn main() {
    eprintln!("resident file benchmark requires Unix source-generation identity");
}
