# Q29 UTF8 dictionary validation and cached hashes

Status: retained after paired material gate, Full43, broad validation and
independent review.

The performance queue selected Q29 chunk dictionary construction after the
retained Q36 partition reduction. The profiling control is `58f0443597cae4c90266dbe2d4fa4481e8c16495`
(merged in PR #1451). Its saved Q29 best complete call is 10.215333 s; 5.080100 s
is nested dictionary construction inside 7.657883 s accessor setup. The accessor
sees 81,032,736 rows and creates 25,771,910 chunk-local dictionary entries, copying
3,120,823,803 UTF8 payload bytes. These are cumulative work counts, not peak memory.

Three new sampled complete CLI calls passed their full result references.
`core::str::from_utf8` directly under dictionary construction accounts for
714/3438, 841/3665 and 767/3663 main-thread samples (20.8–22.9%). These profiled
calls are attribution evidence only; sampling changes execution and cannot be
used for the retention timing gate. Evidence lives under
`/Users/dylan/LocalData/shardloom/ship-drop-20260919/q29-current-cpu-sampling-receipt.json`
and guarded run `full43_20260920T092141822185Z`.

## Candidate contract

Use a private exact byte-to-ID directory with cached content hashes. On lookup,
compare full bytes against the existing validated `Arc<str>` value. Equality to
that value proves duplicate UTF8 validity. Validate a new distinct byte sequence
before creating its owned string and publishing its first-seen ID. Hash matches
alone never establish equality or validity. Reuse stored hashes during growth.

Keep the existing owned string vector, row IDs, null masks, source classification,
native Dict precedence, weighted consumers and retained MIN/MAX ownership. Null
rows are skipped before byte access. No new dependency, unsafe code, query-specific
dispatch, partitioning, recount elimination or provider execution is introduced.
This differs from the dropped owned-count partial: it changes only construction
of the same chunk dictionary, without rerouting owners or consumers.

The existing accessor has no memory-pool handle. This change must not claim
reservation accounting or an RSS limit. Checked directory allocation and growth
do not make existing Arc values or provider allocations reservation-owned.
The new directory is dropped inside `dictionary_build_nanos`; the former
HashMap was dropped after that nested timer. Complete accessor and process
timings retain the same boundary and govern comparisons.

## Vortex-first provider check

- Subject: adapter-local UTF8 dictionary identity and validity reuse.
- Checked provider: pinned Vortex 0.85.0 `VarBinViewArray`, `bytes_at`, validity,
  native Dict dispatch and the existing scan/canonical execution boundary.
- Decision: `implement_shardloom_kernel` for the private dictionary lookup;
  reuse the existing Vortex native provider for arrays, bytes and validity.
- Evidence surface: existing `aggregate_utf8_chunk_accessor` work counters and
  exact native aggregate result certificates.
- Residual handling and materialization: unchanged; no new decode, Arrow path,
  external engine or fallback. `fallback_attempted=false` remains required.
- Gate: collision/invalid UTF8/ownership/null correctness, then same-artifact
  alternating paired Q29 complete calls against the current merged binary.
  Retain for at least one second lower best valid complete call, or at least 30%
  lower OS peak RSS without complete-time regression. Preserve every sample.
  A retained candidate then requires Full43 complete-value UAT and broad checks.
- Broader CG-5/CG-6 and production memory/serving claims remain unchanged.

## Paired complete-query evidence

Frozen candidate `3e3b887f2e6ada7be88751931776fb03a1a55852`, built with
`cargo build --release -p shardloom-cli --features release-user-surfaces`, has
SHA-256 `0deff1e7d58857aeb24cc32154ab81d0095defae843f6e152ea6a34c979a597e`.
The control is the retained runtime named above, SHA-256
`d13670fe73dc00cd515284713e51426eb836c8fe91149490c860cdac952206fe`.

The guarded alternating comparison uses the unchanged 18,591,586,804-byte native
Vortex ClickBench artifact, 99,997,497 source rows, Apple M5, 16 GiB physical RAM,
macOS 27, 24 GiB query policy and requested P12 (host ceiling 10). Policy memory
is not an OS RSS limit. Each sample includes fresh CLI startup, complete output
and exit. Shared host load and cache are uncontrolled; no cold-cache or official
ranking claim is made. Host snapshots and compression are outside timing.

| Run | Control complete seconds | Candidate complete seconds |
| --- | ---: | ---: |
| 1 | 8.993470 | 8.403008 |
| 2 | 9.011277 | 7.833920 |
| 3 | 9.014724 | 7.815263 |

Fastest valid complete calls save **1.178207 s (13.1%)**, satisfying the one-second
gate. Their OS peak RSS is 1,464,729,600 / 1,470,070,784 bytes: no memory reduction
is claimed. User+system CPU is 11.593092 / 10.341058 seconds. Whole accessor time
is 6.739873 / 5.554641 seconds; separate aggregate update is 1.668642 / 1.678741
seconds. This supports the accessor mechanism without interpreting nested timer
differences as exclusive CPU.

All six complete results match. Each preserves 1,550 accessor calls, 81,032,736
accessor rows, 25,771,910 dictionary entries, 3,120,823,803 copied bytes and
1,798,248 complete groups before HAVING. Sampled profiling was not used in these
timings. Raw evidence is guarded run `paired43_20260920T093442385005Z`; external
`audit-q29-dictionary-paired.py` replays every archived output against the saved
reference, checks binary/archive/member/result hashes and timing receipts, then
recomputes the symmetric fastest-call gate. The complete output hash is
`fc6242e120770d4cd9ffc73c5fbb0d55b12dc1b98b055bcad96945290e151308`.

Reproduce with `scripts/run_clickbench_paired_query_uat.py`, the two frozen binary
identities, the retained artifact and reference directory, `--query-ids 29
--memory-gb 24 --max-parallelism 12 --timeout 120`. Both complete commands and all
samples remain in the machine-readable evidence packet.

## Full43 and scope

Guarded run `full43_20260920T093549118609Z` passes all **129/129 complete-value
comparisons** (three calls for each of 43 queries). The observed best-of-three
sum is **71.393790 s** and Q29's best is **7.828941 s**. This suite is unpaired;
its difference from the earlier 79.856087 s suite is not attributed to this
change. References are retained complete results, not a newly independent
correctness oracle. No ingest, storage, cold-cache or production-serving gain is
claimed. Existing integer partitions still activate on Q16/Q36.

`audit-q29-dictionary-full43.py` independently replays the stored outputs through
the harness comparator, validates all 129 timing receipts and raw/compressed log
hashes, checks binary/source/query identities and recomputes the complete suite
score. Receipts and complete commands are retained under the external evidence
directory and summarized in
[the machine-readable packet](../benchmarks/q29-utf8-dictionary-validation-2026-09-20.json).

The next selected attribution is Q34/Q35 complete-key UTF8 worker reconciliation.
Their current best calls are 5.383924 / 4.851808 s. Attribute all compute workers:
partition lock acquisition, exact lookup, table growth, byte-arena copying and
native buffer-handle access. Existing reconciliation and wait spans overlap
across workers and do not establish exclusive CPU costs. Cached hashes, block
entry credits and local comparison counters already exist; rediscovering them
does not justify a new candidate. Topology/coalescing and universal state
replacement remain parked without new dominant-cost evidence.

## Validation

- `cargo fmt --all -- --check`: passed.
- `cargo clippy --workspace --all-targets -- -D warnings`: passed.
- `cargo test --workspace --all-targets`: passed, 3,424 tests.
- Native `release-user-surfaces` Clippy with `--all-targets -- -D warnings`:
  passed.
- `cargo test -p shardloom-vortex --lib --features release-user-surfaces`:
  1,852 passed; nine existing benchmark fixtures ignored.
- Public claim language, public status, workspace versions and architecture
  tracker validators: passed. The tracker keeps existing broader gates blocked.
- Independent review: no actionable correctness blocker. Forced full-hash
  collisions across growth, invalid new bytes under collisions, multibyte/empty/
  NUL strings, first-seen order, nullable/empty/all-null native inputs and selected
  MIN ownership after dropping native input/accessor owners are covered.

The unpaired regression screen found no query whose best call increased by both
more than 0.15 seconds and 10%. The changed accessor is observed on Q6/Q12/Q18/
Q19/Q22/Q23/Q29/Q37/Q38/Q39/Q40. This screen is supplementary to full-value UAT
and the paired target gate; it does not establish paired performance for every
other query.
