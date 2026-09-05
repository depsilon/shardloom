use std::{hint::black_box, time::Instant};

#[cfg(not(target_arch = "wasm32"))]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    use shardloom_vortex::resident_session::ResidentVortexSession;
    let mut args = std::env::args().skip(1);
    let path = args
        .next()
        .ok_or("usage: resident_latency INPUT [ITERATIONS]")?;
    let iterations: usize = args.next().map_or(Ok(10_000), |arg| arg.parse())?;
    if !(100..=1_000_000).contains(&iterations) {
        return Err("iterations must be 100..=1000000".into());
    }
    let started = Instant::now();
    let session = ResidentVortexSession::new(256 * 1024 * 1024, 2)?;
    let source = session.prepare_file(path)?;
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
    nanos.sort_unstable();
    let snapshot = session.snapshot();
    println!(
        "{}",
        serde_json::json!({
            "schema_version": "shardloom.resident_count_latency.v1",
            "timing_boundary": "prepared native count including admission and source generation validation",
            "prepare_seconds": prepare_seconds, "iterations": iterations, "rows": expected,
            "p50_nanos": nanos[iterations / 2], "p95_nanos": nanos[iterations * 95 / 100],
            "p99_nanos": nanos[iterations * 99 / 100], "max_nanos": nanos[iterations - 1],
            "prepared_source_opens": snapshot.prepared_source_opens,
            "completed_executions": snapshot.completed_executions,
            "peak_provider_reserved_bytes": snapshot.memory.peak_reserved_bytes,
            "fallback_attempted": false, "external_engine_invoked": false,
            "scope": "metadata-only Rust operation; not Python, scan, mixed-load or durable ingest latency"
        })
    );
    Ok(())
}

#[cfg(target_arch = "wasm32")]
fn main() {
    eprintln!("resident file benchmark requires a native host");
}
