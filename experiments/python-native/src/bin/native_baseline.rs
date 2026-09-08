//! Independent Rust call surface for the same admitted operations; not a new engine.
#![forbid(unsafe_code)]

use shardloom_core::{ColumnRef, ComparisonOp, DatasetUri, PredicateExpr, StatValue};
use shardloom_plan::ProjectionRequest;
use shardloom_vortex::{
    VortexLocalPrimitiveExecutionPolicy, VortexQueryPrimitiveRequest,
    local_primitives::{
        collect::prepare_rows_in_session, prepared_count::prepare_count_where_in_session,
    },
    resident_session::ResidentVortexSession,
};
use std::{path::PathBuf, time::Instant};

#[path = "../file_admission.rs"]
mod file_admission;

fn elapsed(started: Instant) -> u128 {
    started.elapsed().as_nanos()
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if args.len() != 2 {
        return Err("usage: native_baseline ABSOLUTE_32_ROW_FIXTURE CASE".into());
    }
    let path = PathBuf::from(&args[0]);
    let metadata = std::fs::metadata(&path)?;
    if !path.is_absolute() || !metadata.is_file() || metadata.len() > 16 << 20 {
        return Err("native baseline fixture must be absolute, regular and at most 16 MiB".into());
    }
    let admitted = file_admission::FileAdmission::capture(&path, 16 << 20)?;
    let case = args[1].as_str();
    if !matches!(
        case,
        "metadata_count" | "filtered_count" | "empty_filtered_count" | "projection"
    ) {
        return Err("unknown bounded native baseline case".into());
    }
    let started = Instant::now();
    let session = ResidentVortexSession::new(1 << 30, 2)?;
    let session_nanos = elapsed(started);
    let uri = DatasetUri::new(path.display().to_string())?;
    let names = ["nullable_label", "exact_identifier", "cohort_key"]
        .map(str::to_owned)
        .to_vec();
    let started = Instant::now();
    let count = if case == "metadata_count" {
        Some(session.prepare_file(&path)?.prepare_count())
    } else {
        None
    };
    let filtered = if matches!(case, "filtered_count" | "empty_filtered_count") {
        let request = VortexQueryPrimitiveRequest::count_where(
            uri.clone(),
            PredicateExpr::Compare {
                column: ColumnRef::new("cohort_key")?,
                op: ComparisonOp::GtEq,
                value: StatValue::Int64(if case == "filtered_count" { 24 } else { 99 }),
            },
        );
        let prepared = prepare_count_where_in_session(
            &request,
            VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(2, 1)?,
            &session,
        )?;
        if prepared.source_row_count() > 65_536 {
            return Err("filtered source exceeds logical row bound".into());
        }
        Some(prepared)
    } else {
        None
    };
    let projection = if case == "projection" {
        let request = VortexQueryPrimitiveRequest::project(
            uri,
            ProjectionRequest::columns(
                names
                    .iter()
                    .map(ColumnRef::new)
                    .collect::<shardloom_core::Result<Vec<_>>>()?,
            ),
        )
        .with_source_order_limit(32);
        Some(prepare_rows_in_session(&request, &session)?)
    } else {
        None
    };
    admitted.validate_prepared(|metadata| {
        if let Some(prepared) = &count {
            prepared.validate_file_metadata(metadata)
        } else if let Some(prepared) = &filtered {
            prepared.validate_file_metadata(metadata)
        } else {
            projection
                .as_ref()
                .expect("admitted projection case")
                .validate_file_metadata(metadata)
        }
    })?;
    let preparation_nanos = elapsed(started);
    println!(
        "{{\"kind\":\"preparation\",\"case\":\"{case}\",\"session_nanos\":{session_nanos},\"preparation_nanos\":{preparation_nanos}}}"
    );
    for sample in 0..=30 {
        let started = Instant::now();
        let (value, returned_nanos, sink_nanos, drop_nanos) = if let Some(count) = &count {
            let value = count.execute()?;
            (value.to_string(), elapsed(started), 0, 0)
        } else if let Some(count) = &filtered {
            let output = count.execute()?;
            let returned = elapsed(started);
            if !output.native_io_certificate.is_certified()
                || output.report.has_errors()
                || output.report.fallback_execution_allowed
            {
                return Err("uncertified filtered count".into());
            }
            let value = output.count.to_string();
            let started = Instant::now();
            drop(output);
            (value, returned, 0, elapsed(started))
        } else {
            let arrays = projection
                .as_ref()
                .ok_or("projection absent")?
                .execute_arrays()?;
            let returned = elapsed(started);
            let started = Instant::now();
            let json = arrays.to_bounded_json(&names, 8 << 20)?;
            let sink = elapsed(started);
            // Export for the independent Python oracle is outside both native clocks.
            let value = json.value().clone();
            let started = Instant::now();
            drop(json);
            drop(arrays);
            (value, returned, sink, elapsed(started))
        };
        let snapshot = session.snapshot();
        if snapshot.prepared_source_opens != 1 || snapshot.completed_executions != sample + 1 {
            return Err("native baseline source/execution reuse differs".into());
        }
        println!(
            "{{\"kind\":\"sample\",\"surface\":\"native_rust\",\"case\":\"{case}\",\"sample\":{sample},\"warmup\":{},\"native_return_nanos\":{returned_nanos},\"explicit_json_sink_nanos\":{sink_nanos},\"result_drop_nanos\":{drop_nanos},\"source_opens\":{},\"completed_executions\":{},\"value\":{value}}}",
            sample == 0,
            snapshot.prepared_source_opens,
            snapshot.completed_executions
        );
    }
    drop(count);
    drop(filtered);
    drop(projection);
    let snapshot = session.snapshot();
    if snapshot.memory.reserved_bytes != 0 {
        return Err("native baseline retained owned bytes after plan drop".into());
    }
    println!(
        "{{\"kind\":\"close\",\"native_owned_bytes\":0,\"native_owned_peak_bytes\":{},\"native_owned_denials\":{},\"provider_background_workers\":{}}}",
        snapshot.memory.peak_reserved_bytes,
        snapshot.memory.denied_reservations,
        snapshot.provider_background_workers
    );
    Ok(())
}
