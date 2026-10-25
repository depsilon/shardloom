//! Complete direct-Parquet reference validation, outside ingestion/query timing.

#[cfg(unix)]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    use shardloom_vortex::native_artifact_comparison::{
        NativeArtifactComparisonLimits, ParquetReferenceRecipe, compare_parquet_native_artifact,
    };
    use std::path::Path;

    const USAGE: &str = "usage: compare_parquet_native_artifact SOURCE.parquet RESULT.vortex --recipe source-columns|clickbench-preparation [--memory-bytes-per-source N] [--parallelism-per-source N] [--batch-rows N] [--native-batch-rows N] [--window-rows N] [--arrow-batch-bytes N] [--max-value-bytes N]";
    let arguments = std::env::args().skip(1).collect::<Vec<_>>();
    if arguments.len() < 4 {
        return Err(USAGE.into());
    }
    let mut limits = NativeArtifactComparisonLimits::default();
    let mut recipe = None;
    let mut seen = std::collections::HashSet::new();
    let (flags, remainder) = arguments[2..].as_chunks::<2>();
    if !remainder.is_empty() {
        return Err(USAGE.into());
    }
    for option in flags {
        if !seen.insert(option[0].as_str()) {
            return Err("duplicate readback option".into());
        }
        match option[0].as_str() {
            "--recipe" => {
                recipe = Some(match option[1].as_str() {
                    "source-columns" => ParquetReferenceRecipe::SourceColumns,
                    "clickbench-preparation" => ParquetReferenceRecipe::ClickBenchPreparation,
                    _ => return Err(USAGE.into()),
                });
            }
            "--memory-bytes-per-source" => limits.memory_bytes_per_source = option[1].parse()?,
            "--parallelism-per-source" => limits.parallelism_per_source = option[1].parse()?,
            "--batch-rows" => limits.left_batch_rows = option[1].parse()?,
            "--native-batch-rows" => limits.right_batch_rows = option[1].parse()?,
            "--window-rows" => limits.window_rows = option[1].parse()?,
            "--arrow-batch-bytes" => limits.arrow_batch_bytes = option[1].parse()?,
            "--max-value-bytes" => limits.max_value_bytes = option[1].parse()?,
            _ => return Err(USAGE.into()),
        }
    }
    let report = compare_parquet_native_artifact(
        Path::new(&arguments[0]),
        Path::new(&arguments[1]),
        recipe.ok_or(USAGE)?,
        limits,
    )?;
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}

#[cfg(not(unix))]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    Err("Parquet/native readback requires Unix held-generation validation; unsupported platform; no comparison was performed".into())
}
