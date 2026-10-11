//! Native execution cannot admit a policy without its platform memory owner.

#![cfg(feature = "vortex-local-primitives")]

use shardloom_core::{DatasetUri, ExecutionResourceOrigin, ExecutionResources};
use shardloom_plan::ProjectionRequest;
use shardloom_vortex::{
    VortexLocalPrimitiveExecutionPolicy, VortexLocalPrimitiveExecutionStatus,
    VortexLocalPrimitiveRowExportFormat, VortexQueryPrimitiveRequest,
    execute_vortex_local_partitioned_primitive_with_policy, execute_vortex_local_primitive,
    execute_vortex_local_primitive_row_export_with_policy,
    execute_vortex_local_primitive_with_policy,
};
use std::{path::PathBuf, process::Command};

fn fixture() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../shardloom-vortex/tests/fixtures/local_primitive_struct_five.vortex")
}

fn resources(bytes: u64) -> ExecutionResources {
    ExecutionResources::from_bytes(bytes, 1, ExecutionResourceOrigin::ExecutionCall).unwrap()
}

#[test]
#[cfg_attr(
    unix,
    ignore = "requires the non-Unix runtime branch; executed by Windows CI"
)]
fn non_unix_primitives_refuse_before_existing_or_missing_source_access() {
    for path in [fixture(), fixture().with_extension("missing.vortex")] {
        let uri = DatasetUri::new(path.display().to_string()).unwrap();
        for allocation in [resources(1), resources(4 << 30)] {
            let policy = VortexLocalPrimitiveExecutionPolicy::from_resources(allocation).unwrap();
            for request in [
                VortexQueryPrimitiveRequest::count_all(uri.clone()),
                VortexQueryPrimitiveRequest::project(uri.clone(), ProjectionRequest::all()),
            ] {
                for report in [
                    execute_vortex_local_primitive(&request, allocation).unwrap(),
                    execute_vortex_local_primitive_with_policy(&request, policy).unwrap(),
                    execute_vortex_local_partitioned_primitive_with_policy(
                        &request,
                        &[uri.clone(), uri.clone()],
                        policy,
                    )
                    .unwrap(),
                ] {
                    assert_eq!(
                        report.status,
                        VortexLocalPrimitiveExecutionStatus::BlockedByUnsupportedPrimitive
                    );
                    assert!(!report.data_read);
                    assert!(!report.upstream_scan_called);
                    assert!(!report.write_io);
                    assert!(!report.fallback_execution_allowed);
                    assert_eq!(report.arrays_read_count, 0);
                    assert!(report.result_summary.is_none());
                    assert_eq!(report.resource_envelope.declared_resources, allocation);
                    assert_eq!(
                        report.diagnostics[0].feature.as_deref(),
                        Some("native_vortex_resource_admission")
                    );
                }
            }
        }
    }
}

#[test]
#[cfg_attr(
    unix,
    ignore = "requires the non-Unix runtime branch; executed by Windows CI"
)]
fn non_unix_writer_refusal_preserves_existing_destination_and_creates_nothing() {
    let directory = std::env::temp_dir().join(format!(
        "shardloom-non-unix-admission-{}",
        std::process::id()
    ));
    std::fs::create_dir(&directory).unwrap();
    let existing = directory.join("existing.output");
    std::fs::write(&existing, b"caller-owned contents").unwrap();
    let missing = directory.join("absent-parent/new.output");
    let uri = DatasetUri::new(fixture().display().to_string()).unwrap();
    let request = VortexQueryPrimitiveRequest::project(uri, ProjectionRequest::all());
    for bytes in [1, 4 << 30] {
        let policy = VortexLocalPrimitiveExecutionPolicy::from_resources(resources(bytes)).unwrap();
        for format in [
            VortexLocalPrimitiveRowExportFormat::Jsonl,
            VortexLocalPrimitiveRowExportFormat::Csv,
        ] {
            for path in [&existing, &missing] {
                let report = execute_vortex_local_primitive_row_export_with_policy(
                    &request, path, format, true, policy,
                )
                .unwrap();
                assert_eq!(
                    report.status,
                    VortexLocalPrimitiveExecutionStatus::BlockedByUnsupportedPrimitive
                );
                assert_eq!(report.rows_written, 0);
                assert_eq!(report.arrays_read_count, 0);
                assert!(!report.evidence.side_effects.write_io);
                assert!(!report.evidence.side_effects.data_read);
                assert_eq!(
                    report.diagnostics[0].feature.as_deref(),
                    Some("native_vortex_resource_admission")
                );
            }
        }
    }
    assert_eq!(std::fs::read(existing).unwrap(), b"caller-owned contents");
    assert!(!missing.parent().unwrap().exists());
    std::fs::remove_dir_all(directory).unwrap();
}

#[test]
#[cfg_attr(
    unix,
    ignore = "requires the non-Unix runtime branch; executed by Windows CI"
)]
fn non_unix_cli_reports_refusal_without_admitted_resource_fields() {
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "vortex-run",
            fixture().to_str().unwrap(),
            "count",
            "--memory-bytes",
            "1",
            "--max-parallelism",
            "1",
            "--format",
            "json",
        ])
        .output()
        .unwrap();
    let envelope: serde_json::Value =
        serde_json::from_slice(&output.stdout).unwrap_or_else(|error| {
            panic!(
                "{error}: stdout={} stderr={}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        });
    assert!(!output.status.success());
    assert_eq!(envelope["fallback"]["attempted"], false);
    assert!(
        envelope["diagnostics"]
            .as_array()
            .unwrap()
            .iter()
            .any(|diagnostic| { diagnostic["feature"] == "native_vortex_resource_admission" })
    );
    let fields = envelope["fields"].as_array().unwrap();
    assert!(fields.iter().any(
        |field| field["key"] == "execution_resource_declared_memory_bytes" && field["value"] == "1"
    ));
    assert!(fields.iter().any(
        |field| field["key"] == "execution_resource_admitted_memory_bytes"
            && field["value"] == "unavailable"
    ));
    assert!(!fields.iter().any(
        |field| field["key"] == "execution_resource_admission_status"
            && field["value"] == "admitted"
    ));
}
