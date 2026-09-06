//! Explicit read-only artifact inspection, outside ingestion/query measurements.

use std::{fmt::Write as _, fs::File, io::Read as _, path::Path, time::Instant};

use sha2::{Digest as _, Sha256};
use shardloom_vortex::physical_encoding_inventory::{
    PhysicalEncodingInspectionLimits, inspect_physical_encodings,
};
use vortex::{
    VortexSessionDefault as _,
    file::OpenOptionsSessionExt as _,
    io::{
        runtime::{BlockingRuntime as _, current::CurrentThreadRuntime},
        session::RuntimeSessionExt as _,
    },
};

fn digest(path: &Path) -> Result<String, Box<dyn std::error::Error>> {
    let mut file = File::open(path)?;
    let mut digest = Sha256::new();
    let mut buffer = vec![0_u8; 64 * 1024];
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        digest.update(&buffer[..count]);
    }
    let mut encoded = String::with_capacity(71);
    encoded.push_str("sha256:");
    let digest = digest.finalize();
    let digest_bytes: &[u8] = digest.as_ref();
    for byte in digest_bytes {
        write!(&mut encoded, "{byte:02x}")?;
    }
    Ok(encoded)
}

fn options(
    arguments: &[String],
) -> Result<(PhysicalEncodingInspectionLimits, bool), Box<dyn std::error::Error>> {
    const USAGE: &str = "usage: physical_encoding_inventory ARTIFACT.vortex [--max-total-segment-bytes POSITIVE_BYTES] [--summary-only] (read-only; actual limits are printed)";
    let Some((_path, flags)) = arguments.split_first() else {
        return Err(USAGE.into());
    };
    let mut limits = PhysicalEncodingInspectionLimits::default();
    let mut flags = flags.iter();
    let mut summary_only = false;
    let mut byte_override = false;
    while let Some(flag) = flags.next() {
        match flag.as_str() {
            "--summary-only" if !summary_only => summary_only = true,
            "--max-total-segment-bytes" if !byte_override => {
                limits.max_total_segment_bytes = flags.next().ok_or(USAGE)?.parse()?;
                if limits.max_total_segment_bytes == 0 {
                    return Err("max-total-segment-bytes must be positive".into());
                }
                byte_override = true;
            }
            _ => return Err(USAGE.into()),
        }
    }
    Ok((limits, summary_only))
}

fn summarize(inventory: &mut serde_json::Value) -> Result<(), Box<dyn std::error::Error>> {
    let object = inventory
        .as_object_mut()
        .ok_or("inventory is not an object")?;
    for (detail, count) in [
        ("flat_references", "flat_reference_count"),
        (
            "non_flat_segment_references_not_inspected",
            "non_flat_segment_reference_count",
        ),
    ] {
        let references = object
            .remove(detail)
            .ok_or("inventory reference detail is absent")?;
        let length = references
            .as_array()
            .ok_or("inventory reference detail is not an array")?
            .len();
        object.insert(count.into(), serde_json::json!(length));
    }
    object.insert("summary_only".into(), true.into());
    object.insert("summary_scope".into(), "all_actual_array_trees_inspected;only_emitted_reference_details_omitted;rerun_without_summary_only_for_full_detail".into());
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let arguments = std::env::args().skip(1).collect::<Vec<_>>();
    let (inspection_limits, summary_only) = options(&arguments)?;
    let path = Path::new(&arguments[0]).canonicalize()?;
    let before = path.metadata()?;
    let digest_started = Instant::now();
    let artifact_sha256 = digest(&path)?;
    let digest_nanos = digest_started.elapsed().as_nanos().to_string();
    let runtime = CurrentThreadRuntime::new();
    let session = vortex::session::VortexSession::default().with_handle(runtime.handle());
    let open_started = Instant::now();
    let file = runtime.block_on(session.open_options().open_path(&path))?;
    let open_nanos = open_started.elapsed().as_nanos().to_string();
    let inspect_started = Instant::now();
    let mut inventory = runtime.block_on(inspect_physical_encodings(&file, inspection_limits))?;
    let inspect_nanos = inspect_started.elapsed().as_nanos().to_string();
    let after = path.metadata()?;
    if before.len() != after.len() || before.modified()? != after.modified()? {
        return Err("artifact changed during physical encoding inspection".into());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt as _;
        if before.dev() != after.dev()
            || before.ino() != after.ino()
            || before.ctime() != after.ctime()
            || before.ctime_nsec() != after.ctime_nsec()
        {
            return Err("artifact identity changed during physical encoding inspection".into());
        }
    }
    inventory["artifact"] = serde_json::json!({
        "path": path, "sha256": artifact_sha256, "bytes": before.len(),
        "source_consistency_scope": "caller_frozen_artifact_required;pre_post_path_metadata_guard_not_immutable_snapshot",
    });
    inventory["timing"] = serde_json::json!({
        "scope": "explicit_read_only_inspection_not_ingest_or_query_time;non_overlapping_observed_spans",
        "artifact_hash_nanos": digest_nanos, "open_nanos": open_nanos, "inspect_nanos": inspect_nanos,
    });
    if summary_only {
        summarize(&mut inventory)?;
    }
    println!("{}", serde_json::to_string_pretty(&inventory)?);
    Ok(())
}

#[test]
fn inspection_byte_override_is_explicit_positive_and_bounded() {
    let args = |items: &[&str]| {
        items
            .iter()
            .map(|item| (*item).to_string())
            .collect::<Vec<_>>()
    };
    assert_eq!(
        options(&args(&["fixture.vortex"]))
            .unwrap()
            .0
            .max_total_segment_bytes,
        32 << 30
    );
    assert_eq!(
        options(&args(&[
            "fixture.vortex",
            "--max-total-segment-bytes",
            "68719476736"
        ]))
        .unwrap()
        .0
        .max_total_segment_bytes,
        64 << 30
    );
    for items in [
        vec![],
        vec!["fixture.vortex", "--max-total-segment-bytes", "0"],
        vec!["fixture.vortex", "--unknown", "1024"],
        vec!["fixture.vortex", "--summary-only", "--summary-only"],
        vec!["fixture.vortex", "--max-total-segment-bytes"],
        vec![
            "fixture.vortex",
            "--max-total-segment-bytes",
            "18446744073709551616",
        ],
    ] {
        assert!(options(&args(&items)).is_err());
    }
    let (limits, summary) = options(&args(&[
        "fixture.vortex",
        "--summary-only",
        "--max-total-segment-bytes",
        "68719476736",
    ]))
    .unwrap();
    assert!(summary);
    assert_eq!(limits.max_total_segment_bytes, 64 << 30);
}

#[test]
fn summary_omits_only_reference_detail_and_preserves_actual_encoding_evidence() {
    let mut inventory = serde_json::json!({
        "flat_references": [{"stored_array_nodes": [{"encoding_id": "vortex.constant"}]}],
        "non_flat_segment_references_not_inspected": [],
        "columns": [{"encoding_ids": ["vortex.constant"], "referenced_segment_bytes_non_additive": 16}],
        "unique_flat_segment_bytes": 16, "segment_bytes_requested": 32, "array_nodes": 1,
        "byte_scope": "unique_globally;column_totals_non_additive_for_shared_segments",
        "limits": {"total_segment_bytes": 64_u64 << 30},
        "artifact": {"sha256": "sha256:retained"}, "complete_flat_inspection": true,
    });
    let before = inventory.clone();
    summarize(&mut inventory).unwrap();
    assert!(inventory.get("flat_references").is_none());
    assert!(
        inventory
            .get("non_flat_segment_references_not_inspected")
            .is_none()
    );
    assert_eq!(inventory["flat_reference_count"], 1);
    assert_eq!(inventory["non_flat_segment_reference_count"], 0);
    for key in [
        "columns",
        "unique_flat_segment_bytes",
        "segment_bytes_requested",
        "array_nodes",
        "byte_scope",
        "limits",
        "artifact",
        "complete_flat_inspection",
    ] {
        assert_eq!(inventory[key], before[key]);
    }
}
