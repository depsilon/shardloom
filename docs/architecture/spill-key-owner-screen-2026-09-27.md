# Spill merge key ownership — R5.b

Status: retain borrowed native spill predecessor ownership under PERF-INTAKE /
RFC 0044, following merged C2.a PR #1467. Paired public-call, full regression and
semantic/resource validation are complete. R5.c workflow overlap is next.

The source audit finds existing payload transfers in ReservedHostAllocator,
resident owned intake and worker partials. Frozen ByteBuffers carry their leases;
numeric Vecs and UTF8 offsets/data move into native owners; partials retain their
arrays and credited count storage. R5.a already avoids memory-file composition.
Do not implement these transfers again or claim its measured gain twice.

A distinct copy remains in weighted COUNT spill merging. ReadBlock copies each
bounded complete UTF8 key into an independent Row so the source block can be
released. RunReader then copies that same key into `previous` for sorted-run
validation. RunMerge already owns the first Row in its heap, pops it, reads that
reader's successor, and only then returns the popped Row. The predecessor owner
therefore remains available throughout the next read.

Screen borrowing that owner for the complete-key order check instead of creating
a second predecessor copy. Preserve the first independent key, current block
release order, native Vortex runs, signatures, positive weights, signed integer
bits, declared group order, EOF validation, terminal errors, cancellation and
work leases. No new shared row allocation, source-block pinning, page abstraction,
format, admission expansion or fallback engine is proposed. Pinned Vortex's
existing native arrays/readers remain the provider; this duplicate belongs to
ShardLoom's merge consumer, not a missing Vortex buffer-transfer facility.

Freeze three bounded public native-call cases using the existing renamed-field
spill fixture: 65,536 rows with 128-byte keys and repeated compound groups in both
integer/text orders, then 131,072 rows with 64-byte unique keys. Keep the existing
8 MiB explicit operator envelope and 64 MiB disk quota. Compute complete scalar
expected values and source SHA-256 outside timing. Use fresh native execution
through the public Rust primitive call, with two requested CPU lanes, complete
report output, owned cleanup and report release. Logical request/fixture creation,
oracle verification and observer output are outside the clock. Preserve source
bytes and all route/copy/resource counters for each sample.

Run three repetitions per case in each fresh process, then three counterbalanced
process pairs for a positive candidate. Actual process RSS includes fixture and
oracle construction and is separate from credited native memory. The complete
operation and copied-byte counters decide whether removing the duplicate is
useful; there is no arbitrary percentage or one-second rejection cutoff. Do not
claim a ClickBench improvement from a pressured workload with explicit spill.

Before retention, verify exact complete results, sorted-run failures within and
across blocks, signed compound ordering, cancellation/corruption/quota failures,
terminal iteration and owner refunds. Complete required Rust/native checks,
applicable public UAT and independent review. Failed prototypes are removed while
their evidence remains. R5.c overlap stays a separate experiment.

The initial frozen control (`098934fcb6a9f9c3fe1d086c89445cdfd60a1d00`,
test executable SHA-256
`dd8363896310f5561db799622b7d96dc2de65b9ed8c4b4929c0b6dbcc8a86ae4`)
passes all nine complete calls and cleans every owned workspace. The repeated-key
cases record 20,910,592 merge-head copied bytes per call; the unique-key case
records 33,292,544 bytes. Best complete calls are 51.360 / 48.263 / 92.228 ms.
The control commit predates the C2 squash; its tree is identical to the rebased
observer commit `bc5762b1`. These measurements admit a small ownership candidate,
not a performance claim for that candidate or a production-scale spill benchmark.

The prototype removes the redundant `RunReader.previous` payload. Initial heads
have no predecessor; successor reads borrow the popped heap row, still owned by
the merge through that read. New coverage checks complete-key regression at the
native block boundary for all three key orders, terminal failure and refunds,
plus one independent head copy per key in a single native run. The existing
within-block corruption, signatures, weights, cancellation and quota tests remain.

## Paired public-call evidence

The frozen candidate is `ef8e08f3e00569b81185e4cc6899abf6c2b9547e`,
test executable SHA-256
`4ca9614c8d4524fc55a33451b922b4fd8a909b28cd183a6c6ddb81231e3ae91b`.
The admission block and three counterbalanced process pairs preserve 72 complete
calls. Every returned value matches the independent scalar oracle; matching
control/candidate calls have identical source hashes and byte lengths. Native
certification, run validation and owned cleanup pass throughout.

| Paired case | Control best of nine | Candidate best of nine | Best reduction | Median reduction |
| --- | ---: | ---: | ---: | ---: |
| Integer/text repeated groups | 46.718 ms | 44.339 ms | 5.09% | 2.60% |
| Text/integer repeated groups | 47.866 ms | 44.904 ms | 6.19% | 2.78% |
| Integer/text unique groups | 89.099 ms | 84.884 ms | 4.73% | 5.68% |

The three nine-call cohort sums are 575.496 / 576.047 / 584.550 ms control
and 539.308 / 556.424 / 554.279 ms candidate: reductions of **6.29%, 3.41%
and 5.18%**. These clocks cover native calls through complete reports plus report
release, excluding fixture/request construction, oracle checks and observer
serialization. Individual samples remain in the record; the percentages do not
promise that every call improves.

Merge-head copied bytes fall exactly 50% in every call: 20,910,592 to 10,455,296
bytes for each repeated-key case and 33,292,544 to 16,646,272 for unique keys.
Native run geometry, bytes written, encoded copies, quotas and results agree.
The 8 MiB reservation envelope and 64 MiB disk quota remain unchanged. The
slightly varying reservation peaks and fixture-inclusive process RSS do not
establish a memory reduction. No production-scale spill, ingest or ordinary
ClickBench speedup is claimed.

Measurements use Apple M5 (ten logical CPUs, 16 GiB), macOS 27.0 build 26A428,
Rust 1.98.0 and pinned Vortex 0.85. OS caches were not flushed. The guarded runner
uses a fresh process per nine-call cohort, two requested CPU lanes, a local-only
TMPDIR, workspace exclusion, disk/log limits and supervised process cleanup.
All generated sources and spill runs are removed by their owners.

Reproduction: build the ignored `complete_spill_key_owner_workflows` library
test with `--release --features release-user-surfaces`, freeze both executables
and run `run_r5b_screen.py` from the evidence packet. Run admission first, then
control/candidate, candidate/control and control/candidate process pairs. The
source patch, build receipts, guard runner and every raw sample are preserved
with the final retention evidence.

## Regression and correctness acceptance

The [retention packet](../benchmarks/spill-key-owner-2026-09-27.json) links a
compressed archive containing every raw sample, complete result envelope,
source/binary identity, runner, source patch and validation log. Its 1,117
members also preserve the failed validation and deterministic reproducer below.

Paired Full43 passes all **258 complete values** and final source/binary identity
checks. Control/candidate sums of per-query best-of-three times are
67.691993 / 67.833724 seconds. No query crosses the recorded flag of both
10% and 150 ms slower. Ordinary ClickBench calls do not enable the changed
explicit spill path; this is regression coverage, not an attributed suite gain.
The public pressured spill screen supplies the route activation and independent
scalar oracle. Full43 uses retained native reference outputs.

All 3,425 workspace tests and 1,966 native-feature all-target tests pass, with
18 intentionally ignored performance fixtures. Workspace/native Clippy with
warnings denied and formatting pass. The 28 focused spill tests include native
block-boundary ordering failure, all three complete-key orders, terminal failure,
exact copied bytes and owner refunds. Independent source review found no
actionable issue in predecessor identity, block lifetime, corruption timing,
cancellation or reservation release.

The initial native suite exposed two existing admission-fixture failures:
timestamp-only paths could collide, and a fixture constructed before exclusive
file creation could remove the other owner's file when creation failed. A fixed
clock reproduces the collision. Test-only repair adds an atomic sequence suffix
and creates the cleanup owner only after successful `create_new`. New tests
verify same-clock isolation and failed creation preserving the existing bytes;
both original admission tests and the full suite pass. Independent review found
no actionable issue in that repair. The frozen runtime measured above is
unchanged; the supplemental test patch and failed logs are archived separately.

Retain the removed copy without changing admission, reservation size, source
block retention or persisted representation. No replacement ingest is required.
This closes R5.b's scoped decision, not general spill, memory or serving gates.
