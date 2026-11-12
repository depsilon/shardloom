# Compatible resident window traversal and state

Status: recommendation 9 retained and integrated, November 11, 2026.
PR #1679 merged as `591fdb42f6ffcd5e9003b4a5c130324d7a34a62c`; all 58 PR
and 55 exact-main checks passed, review was clear, and verified retirement
and post-retirement replay completed. The local observations below are unchanged.
This is recommendation 9 in the
[shared-pipeline investigations](native-shared-pipeline-investigations-2026-11-10.md),
within item 20 and RFC 0044. It does not complete that item, the broader RFC, or
CG-1 through CG-23.

## Baseline and independent decision

The accepted source is `a640475ae66bc36cefcae42088dc23da6776e8ed`, the signed
same-tree merge of shared-index PR #1678. Its 58 PR checks and 55 exact-main
checks passed with review clear. Verified retirement and both retained evidence
replays completed before this window capture. The ordinary release runtime is
the accepted index runtime; debug attribution is separate from timing.

The fixed cohort contains 776 unchanged original window requests and 436
versioned multi-output requests. The latter comprise 218 arithmetic and 218
single-input moments cases across CLI SQL, Python SQL, typed relations,
DataFrame and embedded workflows. Selection uses semantics, work exposure,
shape and controls, not observed times.

Exactly 188 matching index-candidate observations are reused with their full
request, source, runtime, expected result, certificate and clock identities.
The remaining 1,024 baseline requests were each executed once, in isolation.
All 1,212 complete results and engine clocks passed independent replay:

| Cases | Count | Baseline engine seconds | Candidate engine seconds |
| --- | ---: | ---: | ---: |
| Original windows | 776 | 165.862585541 | 164.342145894 |
| Composed arithmetic windows | 218 | 80.648526955 | 72.880150194 |
| Composed moments windows | 218 | 94.088304715 | 62.575584459 |
| Complete selected cohort | 1,212 | 340.599417211 | 299.797880547 |

Retain the single frozen candidate under the maintainer's binary rule. All
1,212 candidate requests ran exactly once in isolation and passed complete
result, schema, certificate and engine-clock replay. Complete engine time is
**40.801536664 seconds lower (11.979332495%)**. All three family sums are lower.
The original controls remain in the denominator; their timing differences do
not establish a causal gain from shared accumulators in those single-output
requests. No eligibility, constants, selection or numerical algorithm changed
after the score.

This is a relevant cohort with explicit saved-observation reuse, not a new
complete 10k or Full43 result. It does not repair the invalid original 10k
observation. There are no warmups, repeated trials, cache manipulation or
post-observation changes to the selection, eligibility or constants.

The baseline protocol SHA-256 is
`b033be339cf37f9d1b8d04059933dc9420829a5173eb59b7a8589448b242b7ab`.
The candidate protocol SHA-256 is
`149c1aa8412e165be2a3b7a923c64fd12fd1ff2e8fa99fe323db5a10ed359e35`.
The [portable evidence packet](../benchmarks/evidence/native-shared-window-state-2026-11-11/README.md)
binds both protocols, all 2,424 saved baseline/candidate answers, independent
references, runtime/source identities, diagnostic observations and original
failed attempts. Its verifier replays saved bytes without executing a query.
Correctness, resources, ownership and no-fallback checks precede the decision;
strictly lower complete engine time retains, while equal or higher drops.
No statistical-confidence or CPU-share claim follows from one pass.

## Fixed candidate contract

Use the existing resident window group and partition schedule. A shared family
must be contiguous in that group's declared function sequence; an incompatible
frame or non-frame member ends the family. Preserve existing group order,
ranking/offset work, later fills and initial output allocation order.

The first family admits ordinary SUM/AVG followed by compatible SUM/AVG or
COUNT(value). COUNT-first and COUNT(*) retain their ordinary paths. The second
family admits single-input population/sample variance and standard deviation.
Require identical bound input/key/domain, filter, complete frame, exclusion,
order direction and null order. Compare bound/offset variants exactly, floating
offsets by bits, and calendar amounts plus existing `Kernel::same_semantics`.
Different decimal output policies remain per-output finalizers; a COUNT
companion reads the arithmetic owner's count using its input decimal domain.

Reuse the existing admitted State and its numerical observation order. Arithmetic
retains the current exact floating and decimal totals. Moments retain the aligned
cover algorithm and expose only its existing local Copy summary to multiple
finalizers. Add no persistent summary, per-row member collection, or pending
error owner. Preserve State size and charge every existing owner. All requested
output columns still materialize.

The primary output preserves its original update/finalization sequence. A
secondary finalizer failure records only the earliest failing member index and
immediately drops the diagnostic; only earlier members continue finalizing.
The primary's error retains precedence. After primary success, drop shared state
and evaluate the suffix from the saved index in original declaration order using
the existing native evaluator. That suffix must run fully if an anticipated
error does not recur. Partial results cannot escape a failed request.

Keep primary cancellation checks, poll before shared work and at most every
1,024 companion outputs, and retain final cancellation checks. Stored/spilled
function evaluation and writer scheduling remain unchanged. DISTINCT, paired
moments, quantiles and collections are outside this first shared family.

## Provider and ownership boundary

Decision: `implement_shardloom_kernel` by sharing the existing native frame owner.
The pinned Vortex 0.93.0 array aggregate surface was checked, including SumV2,
Mean and aggregate accumulators. Those are real native providers, but their
sum/NaN/overflow and partial-state contracts do not establish equivalent moving
frames, exclusions, calendar retreat, exact totals or function-major error order.
Replacing the numerical algorithm is outside this experiment.

Existing Vortex-native input/result owners, execution-provider and Native I/O
certificates remain authoritative. Source decode/materialization boundaries do
not change. There is no new dependency, Arrow execution substrate, source/sink
owner or external executor. Both `fallback_attempted=false` and
`external_engine_invoked=false` remain required.

## Verification and work attribution

Baseline diagnostics passed 33 complete requests, 16 calendar controls and 18
resident/stored error observations. The window-focused workspace regression run
passed 178 tests; 24 named existing/new controls were explicitly verified. Format
and strict native lint passed on the same source. Thirteen corruptions of the
diagnostic evidence were rejected. All temporary baseline changes were restored
before scoring, and the complete checked build inventory was reconstructed from
the clean accepted tree plus the retained diagnostic source changes.

Candidate checks preserve complete values, schema, result bits and exact
diagnostics; demonstrate reduced frame bounds, observations and state admissions;
and preserve actual output writes and finalizers. They cover decimal rounding versus
precision errors, primary/secondary overflow and nonfinite precedence, filters,
all-null/empty frames, exclusions, calendar retreat, near-match interruptions,
one-output paths, tight admission, cancellation, refunds, retry and actual spill.
The wide-output control cancels after the first of 2,049 outputs and requires
bounded response before publication. Debug counters remain absent from the
ordinary release runtime.

Cumulative admitted state bytes are not peak retained memory. Existing input and
output owners remain separate; sharing does not establish a smaller query peak.
For 4,097 rows, the three-output arithmetic control reduces state admissions
from three to one and frame-bound evaluations from 12,291 to 4,097. All 12,291
output finalizers and writes remain. The four-output moments control reduces
state admissions from four to one and frame-bound evaluations from 16,388 to
4,097, retaining all 16,388 finalizers and writes. State size remains 1,728 bytes
and the moments owner remains 256 bytes. These are cumulative debug work
observations, not release CPU attribution or a whole-query memory claim.

The same candidate source passes formatting, strict default/native workspace
lint, 3,323 default tests and 7,092 native tests with 39 intentional opt-in
ignores. The focused window suite passes 183 tests and leaves the explicit
attribution test ignored; that test runs separately and passes all 33 complete
diagnostic requests. The frozen comparison also passes 16 calendar and 18 error
controls and rejects 20 evidence corruptions. All 29 required named tests are
present. Actual discovery of the release-feature native test executable finds
4,134 tests and exactly 35 registered opt-in ignores, covered once across four
CI partitions. The broader experimental local feature set accounts for the
different ignore total. No existing ignore or scheduling weight changes.

The ordinary release CLI/library are freshly built from source fingerprint
`51e9a408564dd45e5507782727348da0204a05142a6cc1d28647abb83ea9285a`.
Only five named architecture/status documents may advance afterward. The initial
lint failure and two CI-inventory helper failures remain recorded alongside
their fixes; the four original baseline diagnostic failures also remain intact.
They caused no scored-query repeat. Documentation checks, portable replay,
corruption controls, hosted checks and final retirement complete integration.
No package publication or installed-wheel claim is made. Recommendation 10's
rolling-DISTINCT allocated-storage objective is a separate investigation.

## Hosted integration and retirement

[PR #1679](https://github.com/depsilon/shardloom/pull/1679) merged the exact
tested tree `4674fd245f1a09c84d8aed3226f2dcc01b366dda`, with parents
`a640475ae66bc36cefcae42088dc23da6776e8ed` and
`293c498fa20f6ec372a9372632f8a61a11f83cc8`. All 58 PR and 55 exact-main
checks passed; the completed review had no findings or unresolved threads.
The original failed local premerge verification remains recorded alongside
its corrected successful receipt; no hosted job was restarted.

Verified retirement removed 5,819 regular files among 5,824 entries, with no
symlinks, totaling 688,908,701 logical bytes. It retired the superseded index
runtime and redundant source/raw-ledger copies after exact identity and
closed-handle checks. The accepted window runtime, 220 current input generations,
compact complete results, original failures and three unique historical source
declarations remain retained. Both portable evidence replays passed afterward.
No storage limit changed and no payload backup or package publication was made.

The completed local closeout receipt has SHA-256
`97b80163c9c91d1186acc86522e808fa8563c8d9dd88be9a4ff1b71e316768a0`.
Its retirement manifest has SHA-256
`c6c26c7cf6f58a861cd314e71de7344212746eb04199a450038c9eb78a56c938`.
The next investigation must bind this completed closeout and retained runtime;
the old index-based window guard refers to retired artifacts and cannot be reused
unchanged. Item 20, RFC 0044 and CG-1 through CG-23 remain open.
