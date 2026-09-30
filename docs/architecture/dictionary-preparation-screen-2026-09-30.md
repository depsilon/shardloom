# R2.b: bounded dictionary preparation

Status: **retain** after full local acceptance and independent evidence audit.
Exact-head PR checks and merge gate availability on main.
This closes the last
candidate in the September 26 intake before its final profiling refresh. It does
not resume the release train or the paused format comparison.

The retained R10 Q29 cohort spends 2.348–2.370 seconds building first-seen UTF8
dictionaries, 2.254–2.266 seconds in the provider, and 1.813–1.830 seconds in
aggregate updates, within 6.743–6.790 second complete calls. These caller spans
motivate one preparation worker alongside the caller, with two outstanding jobs.
They do not establish the candidate's speedup.

Reuse the source-backed dictionary builder, `AggregateChunkJobs`, and the existing
transformed dense-general aggregate consumer. Canonicalization stays on the caller;
only dictionary preparation moves to the worker. Complete accessors are consumed
in source order, retaining their task lease and window through aggregation. This
preserves first-seen IDs, floating updates, HAVING, ties and independent owned-key
promotion. It does not revive the rejected owned-count-partial/recount recipe.

Admission requires the existing single-field, nonnullable UTF8, UrlDomain
dense-general proof, ordered completion, no residual predicate and no spill.
Existing native dictionaries drain older jobs and use the original consumer.
Prepared sessions that already own provider workers keep their current execution.
Upstream Vortex remains the native provider; no engine integration, new public
operator, dependency or Arrow execution boundary is introduced.

Each task pre-admits worst-case dictionary and row-directory capacity for at most
262,144 rows, plus native `nbytes` as an estimate. Native buffer capacity and
provider allocations are not fully exposed by that measure: this bounds retained
chunk count and reserved metadata, not RSS. Global aggregate state remains owned
and accounted under its existing contract. Initial capacity denial drains older
jobs and retires to the same serial builder/consumer. Errors after admission,
source corruption and cancellation fail without replay. A stage-local token links
to the operation token without cancelling successful prepared operations on drop.

Acceptance requires complete ordinary/prepared/owned values, active cancellation,
pressure retirement and refund, source-error behavior and schema rejection tests;
then one guarded paired Q29 screen. Retention requires full local correctness/UAT,
complete Full43, immutable evidence and PR checks. Slower candidates are dropped.

The prototype passes 808 native local-primitive tests (11 existing manual fixtures
ignored), native CLI/Vortex all-target Clippy and formatting. New tests cover
dictionary collision/capacity boundaries, native dictionary interleaving,
nullable/root/P1/metadata-pressure rejection, pre- and post-submission pressure,
prepared provider restoration, active operation cancellation, corruption and
typed source denial without a new worker replay. Existing cache-attempt policy is
unchanged. Initial fixture pressure was insufficient after its older task drained;
the corrected fixture grows the next chunk and proves retirement. A test-only
missing import and semicolon were corrected before freezing the candidate.

## First screen and provider-progress revision

Frozen `6be7bc02` passes all six Q29 comparisons. Control calls are
6.792158/6.661381/6.657522 seconds; candidate calls are
7.119073/6.548870/6.541147. Best improves 1.75% and median 1.69%; observed RSS
falls from 1.420–1.430 GB to 1.326–1.343 GB. Preserve the slower first candidate.
All 1,550 jobs complete, with two outstanding chunks, zero UTF8 payload copies,
and no retirement; peak task reservation is 21,760,417 bytes.

Dictionary work falls to 1.953–1.963 seconds, with only 4.4–4.8 ms caller join
wait. However, caller scan progress grows from 52–59 ms to 2.270–2.289 seconds
when the original provider drivers are removed. This is a measured new cost,
not a reason to discard the positive first result.

Admit one revision: for a CPU grant and host capacity of at least three, explicitly
partition three lanes into one caller, one dictionary worker and one native
provider driver. Reuse the existing runtime/driver lifetime without adding a
second full-size pool. P2 retains the first candidate's caller/worker split;
sessions with an existing provider pool still decline this worker family. The
provider driver remains owned through completion, errors and cancellation. If
dictionary preparation retires, it joins its worker before serial consumption;
the existing provider driver may remain. Summary evidence must report the actual
provider count and distinguish shared-budget overlap from restoration after
retirement. Test P2/P3 exact results, pressure and cancellation before re-screening.

## Revised screen

Frozen `d726aaf6` (SHA-256
`af27c8018b16920a022f8ff2cf7b58e3f7b7c24241c799beae5760c3bab6e926`)
passes another 808 native local-primitive tests, native all-target Clippy and
formatting. All six complete Q29 results in
`paired43_20260930T062505235372Z` match the retained reference. Control calls are
6.690134/6.665009/6.657506 seconds; candidate calls are
5.084356/4.587235/4.544591. Best improves **31.74%**, median **31.17%**;
every pair is faster. Observed RSS is 1.4346–1.4988 GB versus control
1.4070–1.4341 GB: retain this modest memory tradeoff, not the first prototype's
lower-RSS claim. The full acceptance result is below.

Both screens include process startup, complete CLI output and exit on the same
99,997,497-row, 112-column optimized artifact. Cache state is uncontrolled and
unrelated host work is accepted. No timing observations are replaced or combined
across the two cohorts. Rebasing onto merged R10 changes commit ancestry only;
the source tree is checked identical to this frozen revision.

## Full acceptance

The same frozen binary passes formatting, workspace and native all-target Clippy,
3,436 workspace tests, 2,037 native tests and 1,520 CLI tests. Counts overlap;
22 existing manual native fixtures remain ignored. All **258 complete results**
in `paired43_20260930T062939105987Z` match the retained full-value references.

Q29 control calls are 6.756227/6.838906/6.757140 seconds; candidate calls are
4.703962/4.717668/4.797718. Every pair is faster: best improves **30.38%**,
saving 2.052265 seconds, and median improves 30.18%. The sum of each query's
best of three falls from **54.016214 to 51.649986 seconds (4.38%)**. These are
one complete cohort's observations, not a hardware-independent guarantee.
Q29 peak RSS is 1.484–1.494 GB versus 1.407–1.429 GB; its driver, two-chunk
window and 21,760,417-byte peak task reservation stay within the admitted scope.

Preserve the negative observations. Q34 best is 5.19% slower (2.428280 versus
2.554271 seconds), with median 2.464108 versus 2.604831. Its first two candidate
pairs are slower and the third is slightly faster. Q35 best is 0.30% slower,
with median slightly faster. Q37 best is 5.82% slower, an absolute 0.010394
seconds. The Q34 worker spans show higher waiting/reconciliation in its first
two candidate calls; those overlapping spans do not establish a host or compiler
cause. No observations are replaced and no separate follow-up is spliced into
the total. Retain the material Q29 and complete-suite gain with those limits.

The [portable evidence](../benchmarks/evidence/dictionary-preparation-2026-09-30.json.xz)
preserves all **270** complete comparisons across the two screens and Full43,
raw archive/member hashes, source manifests and patches for both prototypes,
validation logs, the rebase proof, and the assembly script. It is 467,268 bytes,
SHA-256 `66c7ad17b94fc59ae0ef8a9ca7318e1bb46bca17922d370f2d65b3c161a2fed7`.
The references are retained regression oracles, not a new independent SQL oracle.
The [query profile](../benchmarks/query-profile-2026-09-30.json) contains all 43
queries' three samples, RSS and scoped timing spans from this same final cohort.

Independent audit verifies all 270 canonical results and portable envelopes,
45 raw archives and 1,080 members, both source manifests, 24 portable text
hashes, all six final validation exits and test counts, and every query-profile
sample/RSS/metric. Primary review separately accepts source ordering, admission,
owner/credit lifetime, CPU grants and cancellation/error behavior. See the
[final profile and reuse inventory](performance-profile-refresh-2026-09-30.md).

## PR fixture teardown boundary

The initial Linux native-suite run fails two final zero-reservation assertions
after cancellation/source-error recovery; both see 177,400 bytes still live.
Their immediate post-error baseline checks and fresh-query values pass. Ordinary
sessions do not synchronously join the upstream blocking-I/O pool; running reads
retain charged buffers until their closure/result owners drop. CPU-driver join
does not certify that separate I/O boundary.

The two fixtures now require final zero reservations within a bounded five-second
teardown wait, while retaining the immediate dictionary-job refund assertions.
Seven focused fixtures and native all-target Clippy pass locally; exact-head CI
must pass before merge. This changes only the `cfg(test)` fixture file. The
measured release runtime, binary and immutable performance evidence are unchanged.
