# Canonical numeric dictionary-probe elimination

Current policy: the [control progression ledger](../architecture/performance-control-progression-2026-09-12.md)
advances controls as faster retained versions complete validation. The status and
comparison labels below describe this historical screen, not a permanent control.

Status: retained numeric path; its two completed ingest observations are the
reference for subsequent candidates. The previous control was
**95.447305458 seconds** at `572bd52c`. No fresh control was run for this screen.
Combined runtime public-workload acceptance remains pending; these observations
do not supply a timing for that later binary.

## Changed work

The existing fast-load writer asks Vortex `DictStrategy` whether a zone should
have a dictionary root. In pinned Vortex 0.85.0, that strategy retains only a
`Dict` result and forwards the original chunk for every other result. The legacy
session's admitted encoding set excludes `Dict`, yet its compressor still
canonicalizes, compacts and gathers constant-detection statistics for primitives.

Candidate `6bc73e8de6bfe713ff6926d90374272291d40451` omits that probe only when
the actual input is a canonical primitive and the exact admission set excludes
`Dict`. Encoded inputs and admitted dictionary decisions still use the upstream
provider. Final numeric compression after coalescing, file/zone statistics,
text codecs, layout policy, CPU allocation and public semantics are unchanged.
This reuses the existing native provider; it adds no encoding or execution path.

The six focused native tests pass. They compare dictionary decisions, retained
encoded/text routes, exact values and complete file bytes including statistics
using independent arrays in each arm. Fixtures include signed/unsigned extrema,
adjacent integers above 2^53, NULLs, empty arrays, constants and floating signed
zero. The original probe-discard test still uses the original provider.

## Full-size candidate screen

| Measurement | Complete process | Peak process RSS | Artifact bytes |
|---|---:|---:|---:|
| Previous control | 95.447305458 s | 2,811,117,568 | 18,591,586,804 |
| Numeric probe candidate, first run | 90.303309291 s | 2,647,261,184 | 18,591,586,804 |
| Numeric probe candidate, second run | 93.945037458 s | 2,610,839,552 | 18,591,586,804 |

The first candidate is 5.39% below the previous recorded control. It is one
observation across different dates, with uncontrolled OS cache, not a paired
distribution. The second candidate is 1.57% below that previous control;
both outputs have identical SHA-256 values. These observations do not
establish a stable 5.39% gain. First-run native CPU work is 185.304972 user seconds
plus 13.084545 system seconds; the second uses 194.935316 plus 13.011764 seconds. It
overlaps wall time and must not be added to it. Numeric probe calls and work
nanoseconds are both zero. Other stage spans overlap and are not exclusive CPU
attribution.

Both configurations request P4 and construct one caller, one source driver, one
conversion driver and one provider driver, with three conversion prefetch slots.
Blocking I/O machinery is outside this CPU-owner count. The memory grant is
24 GiB; observed RSS is not an allocator ceiling. The candidate's conservative
native reservation peak is 6,613,045,796 bytes and final reservations are zero.

The input is the immutable 99,997,497-row, 112-column Parquet workload. Source
SHA-256 is `a390f6cb782f6aaef278c72fc1dd86c4f30bc843ebab3c159e9bd4d45ddb079f`.
The output SHA-256 is
`7181c2e578659910da176ff6c0dcfe7ce563405337f3ae88cd44e7932d92a266`, exactly
matching the retained baseline's physical representation. This identity links
all values, schema and row order to the existing complete native comparison,
and physical bytes to September 12 fresh-artifact Full43's 129 passing complete
results. It is not a fresh candidate query run or an independent statistics
oracle. The first output was verified and retired; the original source and
protected reference remain unchanged.

## Reproduction and retained records

The candidate uses ordinary `cargo build -p shardloom-cli --release --features
release-user-surfaces`, without RUSTFLAGS or PGO. Its frozen binary SHA-256 is
`f97d881ef70ea78c53d22c72a44911d79ba9ec148eef96ddd33296c3b4b37d86`.
The public ingest runner receives the immutable Parquet source, P4, 24 GiB,
one unique output, 300-second timeout, 24-GiB candidate reservation, 100-GiB
workspace ceiling, 256-MiB log ceiling and 12-GiB free-space headroom. Source
generation, residency and process-cleanup guards stay enabled.

Machine-local evidence is retained under
`/Users/dylan/LocalData/shardloom/perf-all-20260906/`:

- `ingest-probe-6bc73e8d-build.json` and release build logs;
- `ingest-probe-screen-20260912/candidate.json`, stdout/stderr and retirement receipt;
- `ingest-probe-repeat-20260912/candidate.json`, stdout/stderr and retirement receipt;
- `run_ingest_probe_screen_20260912.py` and `retire_ingest_probe_20260912.py`;
- `plan-exhaustion-numeric-focused-r2.log` with six passing tests.

The first focused compile's std/hashbrown set mismatch is preserved in
`plan-exhaustion-numeric-focused.log`; the candidate uses the provider's own
set alias. No failed measurement was discarded. The isolated candidate changes
ingest probing only; later source-generation, prepared-query and spill/result
changes require their own final-tree validation before promotion.
