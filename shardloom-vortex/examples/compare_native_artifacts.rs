//! Explicit full-value comparison outside ingest and query measurements.

#[cfg(unix)]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    use shardloom_vortex::native_artifact_comparison::{
        NativeArtifactComparisonLimits, compare_native_artifacts,
    };
    use std::path::Path;

    const USAGE: &str = "usage: compare_native_artifacts LEFT.vortex RIGHT.vortex [--memory-bytes-per-source N] [--parallelism-per-source N] [--batch-rows N] [--right-batch-rows N] [--window-rows N] [--arrow-batch-bytes N] [--max-value-bytes N]";
    let arguments = std::env::args().skip(1).collect::<Vec<_>>();
    if arguments.len() < 2 {
        return Err(USAGE.into());
    }
    let mut limits = NativeArtifactComparisonLimits::default();
    let (flags, remainder) = arguments[2..].as_chunks::<2>();
    let mut seen = std::collections::HashSet::new();
    for option in flags {
        if !seen.insert(option[0].as_str()) {
            return Err("duplicate comparison option".into());
        }
        match option[0].as_str() {
            "--memory-bytes-per-source" => limits.memory_bytes_per_source = option[1].parse()?,
            "--parallelism-per-source" => limits.parallelism_per_source = option[1].parse()?,
            "--batch-rows" => limits.left_batch_rows = option[1].parse()?,
            "--right-batch-rows" => limits.right_batch_rows = option[1].parse()?,
            "--window-rows" => limits.window_rows = option[1].parse()?,
            "--arrow-batch-bytes" => limits.arrow_batch_bytes = option[1].parse()?,
            "--max-value-bytes" => limits.max_value_bytes = option[1].parse()?,
            _ => return Err(USAGE.into()),
        }
    }
    if !remainder.is_empty() {
        return Err(USAGE.into());
    }
    let report =
        compare_native_artifacts(Path::new(&arguments[0]), Path::new(&arguments[1]), limits)?;
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}

#[cfg(not(unix))]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    Err("native artifact comparison requires Unix held-file generation validation; unsupported platform; no comparison was performed".into())
}
