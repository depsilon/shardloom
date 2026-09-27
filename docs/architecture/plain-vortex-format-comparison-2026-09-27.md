<!-- SPDX-License-Identifier: Apache-2.0 -->

# Plain Vortex source comparison

Status: release closeout, full-value fixture conversion, initial Parquet/plain
lanes, and the authorized plain-Vortex handoff retry are complete. The
[public I/O repair](public-io-route-repair-2026-09-27.md) passes correctness checks.
The optimized-reference lane remains paused after 32 successful cases.
The maintainer narrowed the original repeated schedule to a quick baseline and
requested cleanup after each input lane. The boundaries below reflect that change.

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
- Retain completed collection calls from the interrupted paired schedule with
  their original binary identities. Finish missing collection calls and the
  requested Vortex/Parquet/Arrow IPC result exports with one sample per case.
  Execute complete input lanes sequentially to release storage between lanes.
  Validate complete returned results and read back each exported result outside
  the timed call. Record the repaired binary separately from released 0.3.1.
- Keep the one-time conversion, preparation, query execution and total lifecycle
  costs separate. Report individual calls and which evidence was retained;
  this mixed-version pulse is not a best-of-three or causal speedup comparison.
  OS page cache is uncontrolled, not a cold-cache claim.
- CSV, JSON and JSONL routes receive setup and small correctness checks only.
  Do not generate their full-size input fixtures or run extra performance lanes.

## Handoff retry evidence

After the shared handoff fixes, the maintainer authorized one fresh plain-Vortex
pass: all 43 queries, each collected and exported to Vortex, Parquet and Arrow IPC.
All **172 cases passed**, including complete readback of all 129 exports against
the retained native reference results. The release binary was built from
`503e67eede1236e4c5fa73dca2bcbd40d8cca570` (SHA-256
`74edd91ec3fd4f23b4f3c45601561b202bbb310d782df3e4387ca6863aa784ce`).

The regenerated 15,267,757,096-byte fixture has the same SHA-256 as the original:
`ea246b09de4d2515c2db8deeda75d6adbc7449caa04029f5aacdde0538e780ae`.
This carries forward the earlier complete-value/schema/order proof. Every retry
measurement is fresh; no timing sample is reused. Each total below sums 43 native
CLI calls through complete result/output and process exit. Readback, fixture
conversion and archival are outside those calls.

| Route | Saved baseline | Handoff retry | Elapsed reduction |
| --- | ---: | ---: | ---: |
| Collection | 330.04s | 104.73s | 68.27% |
| Vortex export | 328.22s | 106.16s | 67.65% |
| Parquet export | 345.80s | 110.73s | 67.98% |
| Arrow IPC export | 351.11s | 106.70s | 69.61% |

Q23 collection fell from 62.99s to 3.12s; its Vortex export fell from 69.86s to
2.75s. Its reader-evidence elapsed scope fell from 9.448s to 0.009s, accessor scope
from 11.004s to 1.392s, and estimated retained group-string storage from
373,729,316 to 243,711 bytes. These estimates are not process RSS: Q23 collection's
recorded peak RSS was still 6,470,041,600 bytes. Q28/Q29 collection measured
6.97s/5.81s, down from 29.49s/25.05s. The slowest remaining plain collection calls
are Q34 12.04s, Q35 11.26s, Q17 8.93s and Q19 8.30s.

These are observed single-sample comparisons with uncontrolled OS page cache.
The saved collection baseline contains 18 released and 25 repaired-binary calls;
all saved exports use `efdaa865`. All retry calls use `503e67ee`. The identical
fixture controls physical representation; this is not isolated causal attribution,
a best-of-three result, or a claim of subsecond completion for every query.

Reproduction inputs, commands, frozen binaries, harness source, hashes and full
per-case timings are retained under the local-only workspace:

- `format-io-20260927/handoff-frozen-input-identities.json`
- `format-io-20260927/io_pulse_handoff.py` (one manifest argument; fresh plain lane)
- `format-io-20260927/handoff-plain-comparison.json`
- `clickbench-100m-uat/logs/io_pulse_v2_plain_20260927T181211867110Z/summary.json`
- `clickbench-100m-uat/logs/io_pulse_handoff_plain_20260927T192735120583Z/summary.json`

All paths above are relative to `/Users/dylan/LocalData/shardloom`. All 43 result
archives and every archived member were reverified against their recorded hashes.
The wrapper reports no remaining native workload. The regenerated fixture was
hash-checked and removed after completion, reclaiming 15.27GB. The original
Parquet and protected optimized Vortex references remain. CSV/JSON/JSONL retain
small correctness coverage only; the optimized-reference lane was not resumed.

## Storage and process ownership

Use the local-only ClickBench workspace and the existing ingest/query lock and
storage guards: 100 GiB workspace, 12 GiB free headroom and 256 MiB logs. Apply
equivalent guards, a deadline, log/RSS limits and process-group cleanup to fixture
creation. Do not run overlapping tests or builds while measuring, and do not
terminate unrelated user processes to force isolation.

Keep the original Parquet, current optimized artifact and protected reference.
After all results in an input lane are captured, retire its task-owned preparation
cache or generated plain-Vortex fixture after hash/generation checks, preserving
timings, correctness evidence and small output archives.
Completed logs may be losslessly archived with verified per-member hashes.
The [storage note](local-development-storage.md) records preparatory cleanup.

The queued optimization order remains R2.a, R3.a, R3.b, R4, R6.c, R10 and conditional
R2.b in the [reviewed intake](performance-candidate-intake-2026-09-26.md).
