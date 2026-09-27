# C7: whole-file JSON typed construction

Status: retain after complete acceptance: 31.11% lower paired complete ingest
and lower peak RSS with byte-identical native output. This completes
the separate whole-file JSON screen in C7 under RFC 0031/0033, the existing
performance intake and CG-5/CG-6 evidence obligations.

The 524,288-row, 186,301,079-byte array fixture preserves every source object in
the accepted JSONL fixture. A guarded public control ingest with CPU sampling
completed in 2.635842 seconds and peaked at 2,586,230,784 bytes RSS. This is
attribution, not an unprofiled comparison. Stacks include object parsing, row
map construction and ordered row copying before typed column construction.

## Experiment contract

Parse through the existing JSON object grammar, resolve each row's duplicate
fields in reusable slots, and append directly to inferred typed columns.
Discover columns in first-seen order; backfill missing earlier values with
nulls. Infer each column from final duplicate values, preserving the existing
whole-JSON stable scalar-type rule and all-null UTF8 default. Validate the
complete document before the native writer begins. Keep schema-hinted JSON
on its existing explicit adapter path.

The retained implementation keeps whole-file text/character parsing and whole typed
columns in chunks at the existing batch sizes, including late-null backfill.
This preserves each Arrow string buffer's existing offset range. It is not a
bounded-memory JSON parser or a whole-process reservation guarantee. Test
first/late/all nulls, changing field order, duplicates, nested text, Unicode,
numeric precision, malformed/trailing input, limits and failure atomicity.
Compare complete output values and paired complete public operations; preserve
every sample and keep useful gains without an arbitrary percentage floor.

## Vortex-first provider check

- Subject: existing explicit JSON compatibility input, with no new grammar.
- Decision: `implement_shardloom_kernel` in the input adapter; reuse the current
  parser and Arrow 58.3 builders through `shardloom-vortex::universal_format_io`.
- Provider: existing scalar dtype/append helpers, Arrow `RecordBatch` ownership,
  Vortex 0.85 `ArrayRef::from_arrow` and the native streaming writer.
- Boundary: decoded compatibility input to typed columns and native Vortex
  output. Vortex input is not decoded to execute this adapter.
- Reports/certificates: existing SourceState, materialization, Native I/O,
  resource admission and no-fallback reports; describe whole typed ownership.
- Gates: `vortex-write` and `universal-format-io`; no dependency or unsafe code.
- Residuals: ShardLoom parsing/type errors; `fallback_attempted=false` and
  `external_engine_invoked=false`. No query-engine integration.
- Reused mechanism: avoid row maps and repeated copies before scheduling work;
  retain the established batch geometry, derived metadata and native commit.
- Still blocked: streaming byte-parser redesign and general capability claims.

## Comparison and complete acceptance

The [summary](../benchmarks/json-typed-builder-screen-2026-09-27.json) and
[raw evidence](../benchmarks/json-typed-builder-screen-2026-09-27.json.gz) retain
all samples, commands, complete envelopes, source/binary identities, recipes,
CPU/RSS counters, failed attempts and per-member hashes. The host is Apple M5
/ arm64, 16 GiB, macOS 27.0. Both binaries use Rust 1.98.0, Vortex 0.85.0,
`release-user-surfaces` and portable ThinLTO, with no CPU-specific flags,
trained profile or cache eviction. Public `prepare dataframe` calls use the
same 2 GiB policy and four workers; RSS is observed, not a process-memory cap.

| Sample | Control ingest | Candidate ingest | Control peak RSS | Candidate peak RSS |
| --- | ---: | ---: | ---: | ---: |
| 1 | 2.213952 s | 2.055314 s | 2,651,357,184 B | 1,196,163,072 B |
| 2 | 2.253654 s | 1.524817 s | 3,194,159,104 B | 1,196,310,528 B |
| 3 | 2.213519 s | 1.533238 s | 3,189,800,960 B | 1,200,652,288 B |

Execution order is C1/P1/P2/C2/C3/P3. Symmetric best-valid complete ingest
falls from 2.213519 to 1.524817 seconds, **31.11% lower**. All six native
artifacts are identical: 26,252,024 bytes, SHA-256
`ce0e1a0cf072eeffda96b46729683b614fa06309ea6efd23a8dcd3e8ee50edd8`.
An executed native export verifies all 524,288 rows / 6,291,456 source and
derived cells against independent formulas and parsed source, including
canonical nested text, Unicode, nulls, numeric types and URI metadata.
Verified duplicate artifacts and the large export are removed after hashing.

The candidate is frozen at `f950a1ddeb9e8b2b6645d5ba1458cd23a9068d21`, binary
SHA-256 `6e14e8686ad169c51676ba88114a33e1856df82ec37e5285be8aa7c313f7ef29`.
The control is the accepted JSONL binary `0b4ec877243d3fc26ad709dd97dddf9d03811960cccc918e9d0e924fc6592346`.
Later changes in this unit are documentation/evidence only.

- Formatting, workspace Clippy and 3,426 workspace tests pass.
- Native-feature Clippy and 3,512 tests pass; 22 existing manual tests remain
  ignored. Five new tests cover values/schema, invalid input, atomic publication,
  null/type-state ownership and repeated chunk allocation.
- Full43 passes 129/129 complete results; best-of-three sum is 71.273789 s.
- Full-size Parquet ingest completes in 94.978078 s, producing the unchanged
  15,682,956,116-byte retained artifact. Its verified duplicate is removed.
- Held-out operators pass 456 calls at workers 1/4/12, including 24 expected
  overflow diagnostics. This does not claim the full default worker matrix.
- Five pressure checks pass, including exact SQL/DataFrame spill results,
  reservation/disk limits, quota denial and owned cleanup.

Full43 and Parquet timings are unpaired regression observations, not JSON
speedups. Saved complete query/held-out results and hashes were replayed through
their original validators. Primary semantic review is supplemented by an
independent mechanical source/coverage audit; this is not an independent
semantic-review claim.

## Resolved acceptance findings

An initial candidate failed the unchanged conversion reservation because Arrow
resets byte-builder capacity after `finish`: later chunks then grew from the
first value's arbitrary length and retained larger buffers. Recreating the
same initial builder geometry per chunk preserves admission. A regression test
compares every repeated chunk's values and retained memory with the existing
builder. The failed public run remains preserved and excluded from valid timing
comparisons; no reservation was raised or guard disabled.

The source schema digest excludes derived metadata columns, full reads report
`not_requested_full_read`, and provider certificates recognize the explicit
whole-column/batched-writer ownership. Initial provider/smoke assertion failures,
a control-profile fixture-path error and a replay field-prefix error remain in
the evidence, with their corrected proofs. JSON numeric parsing still tries
i64 then finite f64; this adds no exact-u64 or broader input support.
