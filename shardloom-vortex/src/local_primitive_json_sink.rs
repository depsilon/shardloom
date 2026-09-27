//! JSON-array framing over the existing native row renderer, executed once.
//! The owned spool bounds framing memory and is removed on success or failure.

use super::{
    Result, ShardLoomError, VortexLocalPrimitiveExecutionPolicy,
    VortexLocalPrimitiveRowExportFormat, VortexLocalPrimitiveRowExportReport,
    VortexQueryPrimitiveRequest, execute_vortex_local_primitive_row_export_enabled, vortex_error,
};
use std::{
    fs,
    io::{BufRead as _, Read as _, Write as _},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

struct Spool(PathBuf);
impl Drop for Spool {
    fn drop(&mut self) {
        let _ = fs::remove_file(self.0.join("rows.jsonl"));
        // Do not recursively remove unknown files if an interrupted writer left evidence.
        let _ = fs::remove_dir(&self.0);
    }
}

pub(super) fn execute(
    request: &VortexQueryPrimitiveRequest,
    output: &Path,
    overwrite: bool,
    policy: VortexLocalPrimitiveExecutionPolicy,
) -> Result<VortexLocalPrimitiveRowExportReport> {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let root = shardloom_core::infer_local_output_workspace_root(output)?;
    let plan = shardloom_core::plan_workspace_safe_local_output(&root, output, overwrite)?;
    // The final writer creates missing target parents safely. Place temporary
    // framing files in the already existing, preflighted workspace root.
    let parent = Path::new(&plan.path_safety_report.canonical_workspace_root);
    let spool_path = parent.join(format!(
        ".shardloom-json-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    fs::create_dir(&spool_path).map_err(vortex_error)?;
    let spool = Spool(spool_path);
    let rows_path = spool.0.join("rows.jsonl");
    let mut report = execute_vortex_local_primitive_row_export_enabled(
        request,
        &rows_path,
        VortexLocalPrimitiveRowExportFormat::Jsonl,
        false,
        policy,
    )?;
    report.output_path = output.display().to_string();
    report.output_format = "json";
    if report.has_errors() {
        return Ok(report);
    }
    let mut reader = std::io::BufReader::new(fs::File::open(&rows_path).map_err(vortex_error)?);
    let expected = report.rows_written;
    shardloom_core::write_workspace_safe_bytes_with_validated_producer(
        root,
        output,
        overwrite,
        "native JSON array result",
        |writer| {
            writer.write_all(b"[").map_err(vortex_error)?;
            let mut rows = 0_u64;
            loop {
                let mut line = Vec::new();
                // A single row must be bounded even when source projection streams.
                reader
                    .by_ref()
                    .take(8 * 1024 * 1024 + 1)
                    .read_until(b'\n', &mut line)
                    .map_err(vortex_error)?;
                if line.is_empty() {
                    break;
                }
                if line.len() > 8 * 1024 * 1024 || line.last() != Some(&b'\n') {
                    return Err(ShardLoomError::InvalidOperation("JSON result row exceeds 8 MiB framing bound; no fallback execution was attempted".into()));
                }
                line.pop();
                if rows > 0 {
                    writer.write_all(b",").map_err(vortex_error)?;
                }
                writer.write_all(&line).map_err(vortex_error)?;
                rows = rows.checked_add(1).ok_or_else(|| {
                    ShardLoomError::InvalidOperation("JSON result row count overflow".into())
                })?;
            }
            writer.write_all(b"]\n").map_err(vortex_error)?;
            Ok(rows)
        },
        |rows| {
            if *rows != expected {
                return Err(ShardLoomError::InvalidOperation(
                    "JSON result framing changed row count; no fallback execution was attempted"
                        .into(),
                ));
            }
            Ok(())
        },
    )?;
    report.output_path = output.display().to_string();
    report.output_format = "json";
    Ok(report)
}
