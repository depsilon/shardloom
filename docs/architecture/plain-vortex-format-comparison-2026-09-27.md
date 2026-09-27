<!-- SPDX-License-Identifier: Apache-2.0 -->

# Plain Vortex source comparison

Status: requested local benchmark, next after the verified 0.3.1 release closeout.
No new format timing or full-size conversion is claimed by this plan.

The maintainer requested an ordinary Vortex version of the resident hits Parquet
source and timing of Vortex input through ShardLoom. This is a bounded follow-up
under PERF-08/PERF-09/PERF-12 and CG-5/CG-6, not a new implementation phase or
authorization to restart the seven remaining optimization candidates. Complete
the selected release channels and deployment first; run local builds, conversion,
validation and measurements sequentially.

## Frozen source and logical contract

The resident `sources/hits.parquet` is 14,779,976,446 bytes with 99,997,497 rows,
105 columns and 226 row groups. Its footer reports non-nullable `int16`, `int32`,
`int64`, `uint16` and UTF8 string fields, with no Arrow schema metadata.

The plain Vortex fixture must preserve those logical types, nullability, column
names/order, row order and every value. Do not add derived columns, rename fields,
sort rows, or reuse the existing 112-column optimized artifact as the plain input.
Physical pages, encodings and chunk boundaries may differ. This is logical
equivalence, not an identical physical representation.

Use the existing Rust Parquet/Arrow 58.3.0 reader and upstream Vortex 0.85.0
streaming writer with its default compression, matching the approved dependency
graph and released provider. The native
[`BlockingWriter`](https://github.com/vortex-data/vortex/blob/0.85.0/vortex-file/src/writer.rs)
accepts bounded record batches; ordered native scans and the Arrow boundary
support complete-value validation. PyArrow 25.0.1 is used only to create small
correctness fixtures. This avoids adding the Python Vortex package's transitive
dependencies. No new dependency, query-engine integration or external execution
fallback is involved. Do not materialize the full dataset in memory.

Before timing, compare all source and Vortex values in bounded batches, aligning
different chunk boundaries. Check the native logical schema before requesting an
Arrow schema, so a requested cast cannot hide a changed type or nullability.
Preserve original file identities, complete hashes and validation receipts.

## Measurement boundaries

The public Parquet path constructs ShardLoom's optimized Vortex layout, including
derived columns. The current native Vortex path in
`shardloom-vortex/src/vortex_ingest.rs::prepare_native_vortex_artifact` instead
admits the same file from its footer or creates a byte-preserving copy to a new
target. It does not re-encode or add ShardLoom's derived fields. Report the actual
route and those different output contracts alongside preparation time.

- Measure one-time Parquet-to-plain-Vortex fixture construction separately.
- Measure public Parquet preparation and public Vortex preparation to distinct
  targets, sequentially with the same frozen 0.3.1 CLI and resource settings.
  Record all samples, process wall time, peak RSS, output bytes and route fields.
- Where same-file admission is measured, label it separately from target creation.
  Never treat metadata admission as reading or rewriting the full file.
- Run the existing paired Full43 harness with the same binary and two artifacts,
  alternating their order while keeping calls sequential. Validate complete
  returned results and retain all samples. This reveals query costs of the plain
  layout instead of inferring them from preparation speed.
- Keep the one-time conversion, preparation, query execution and total lifecycle
  costs separate. Report the symmetric best-of-three query totals together with
  the samples; OS page cache is uncontrolled, not a cold-cache claim.

## Storage and process ownership

Use the local-only ClickBench workspace and the existing ingest/query lock and
storage guards: 100 GiB workspace, 12 GiB free headroom and 256 MiB logs. Apply
equivalent guards, a deadline, log/RSS limits and process-group cleanup to fixture
creation. Do not run overlapping tests or builds while measuring, and do not
terminate unrelated user processes to force isolation.

Keep the original Parquet, one validated plain Vortex source, the current optimized
artifact and protected reference. Retire redundant newly generated targets only
after hash/generation checks, preserving their timings and correctness evidence.
Completed logs may be losslessly archived with verified per-member hashes.
The [storage note](local-development-storage.md) records preparatory cleanup.

The queued optimization order remains R2.a, R3.a, R3.b, R4, R6.c, R10 and conditional
R2.b in the [reviewed intake](performance-candidate-intake-2026-09-26.md).
