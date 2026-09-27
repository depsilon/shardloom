# Compact immutable candidate directories — C4

Status: retained bounded change under PERF-INTAKE / RFC 0044, after the
R5.c attribution PR #1469. Complete-query comparison and final Full43 acceptance
are recorded below; `claim_gate_status=not_claim_grade`.

The relevant fixed-key phase is source-order integer/UTF8 grouped COUNT after
its admission limit closes. Q18's saved native route retains ten groups, builds
a dictionary-code directory for each input chunk, then probes that immutable
directory while updating exact counts. The current directory stores a
`Vec<Candidate>` descriptor for every dictionary value and copies candidate lists
into each matching code slot. Its string interner remains the authority for
full-byte equality and original IDs.

Experiment: keep one candidate list per retained string ID and map each chunk
dictionary code to a machine-word list index, with an explicit absent sentinel.
Only retained strings present in the current dictionary acquire candidate lists.
An ID-only discovery pass and a second group traversal prevent unmatched groups
from retaining payloads during row counting; timing includes both traversals.
The immutable table remains local to the chunk; do not introduce a cross-chunk cache,
perfect hash dependency, cross-chunk string owner or persistent format. The
code-index table uses one machine word per value instead of three-word vector
descriptors. Include construction and every row probe in complete public Q18
timing. This is a storage-layout fact, not a claim about process RSS or total
query memory. Retain any useful measured gain; no one-second/percentage cutoff.

Vortex-first check: native dictionary codes already provide dense exact indices.
Use those indices, retaining existing source values and full-byte string-ID
binding. Q18 currently uses the host chunk dictionary accessor for SearchPhrase;
its native Vortex dictionary accessor field is `none`. These are chunk-local
codes, not evidence that persisted DictArray codes survive into this consumer.
A new perfect hash over the same codes would add construction without
removing a lookup. The mutable chunk interner and live aggregate maps are not
immutable-directory replacement targets. Optional recount paths do perform
read-only lookups, but current Q13/Q17/Q23/Q34/Q35 evidence already avoids their
second pass; their hypothetical savings cannot be credited here.

Acceptance requires unchanged source-order selection, complete-key equality,
signed/unsigned numeric handling, multiple numeric candidates per string,
duplicate/reordered dictionary values, unknown values, and malformed code
diagnostics. Compare against the frozen R5.b runtime on the retained 100M source
through complete CLI output/exit, with full value validation. If retained,
complete Full43 and required workspace/native validation before PR. No execution
fallback, Vortex input/output change, package publication or broad memory claim.

The differential fixture exposed a pre-existing correctness defect in the
generic materialized `update_row` route: it inserts owned UTF8 group keys but,
after source-order admission closes, looked up only interned keys. The bounded
fix first probes the same transformed owned key used at insertion, then preserves
the existing interned-key probe for General states seeded by direct consumers.
The owned probe is enabled only after this generic route has admitted keys;
direct-only states preserve their earlier interner miss before later key
expressions execute. Once admission closes, the owned route builds an exact
retained-prefix directory with shared string owners. Unknown prefixes stop before
later expressions execute; positions and correlations between retained key parts
are preserved. When no alternate interned representation exists, a definitive
miss does not retry another lookup. Failing fixtures reproduced unknown-string,
swapped-position, cross-group and numeric-prefix overflow cases before these
corrections. A matching prefix still evaluates and reports a genuine overflow.
Only the irredundant key columns are evaluated: dependent output expressions
remain unevaluated for unretained rows. The review's dependent-offset overflow
fixture reproduced the initial full-expression probe regression before the
key-only correction. It does not admit new groups or strings. Failing logs are
retained. COUNT and COUNT DISTINCT regressions cover this closure transition independently of the
compact directory; the latter covers both owned and interned General seeds.
Q18's comparison uses the direct candidate-directory route, so this generic
correctness fix is not credited with its performance outcome.

One unrelated recovery test failed in CI without reporting its underlying error.
Its assertion now includes that error and namespace. The local recovery tests and
fresh complete native CI pass; this improves diagnostics, not a claimed runtime
fix or explanation of the original failure.

Separate pre-existing obligation: mixed generic/direct key construction before
admission closure, and scalar-to-direct transitions after closure, require a
broader representation-normalization audit. This patch does not claim that
arbitrary mixed routes or compact-to-General state conversion are supported.

## Retention evidence

The final frozen candidate is `fd407c2c0d77482c2c2fcef8a7a93d17dd302334`, compared
with the R5.b runtime `ef8e08f3e00569b81185e4cc6899abf6c2b9547e`; R5.c changed test
attribution only. The retained source has 99,997,497 rows and 15,682,956,116 bytes.
All comparisons time native process startup, complete CLI output and exit.

| Final Q18 evidence, six calls per binary | Control | Candidate |
| --- | ---: | ---: |
| Best complete call | 269.963 ms | 270.879 ms |
| Median complete call | 288.594 ms | 282.267 ms |

The final best comparison is 0.916 ms slower (0.34%); the median is 6.327 ms lower
(2.19%). The earlier `443a532` revision recorded 0.90% lower best time and 0.70%
lower median time. Those are separate revision-specific observations. Retain the
compact code table and correctness fixes without claiming exclusive attribution,
a consistent latency improvement, whole-query memory reduction or production-wide
improvement. Every sample, including the first slower candidate calls, remains
in the evidence.

Final Full43 validates all 258 complete outputs; its best-of-three sums are
70.015689 s control and 69.958898 s candidate. This is regression coverage,
not an attributed suite speedup. The earlier Q26/Q35 timing flags do not recur.
Q17 alone crosses the final timing screen. Six reverse-order follow-up calls
validate complete values and clear that screen: best 2.947601 s control /
2.873507 s candidate. The packet keeps all 1,074 outputs across superseded and
final comparisons; no follow-up sample replaces a Full43 result.

Nine focused regressions, 3,425 workspace tests and 1,975 native tests pass
(19 native tests ignored). Formatting and strict workspace/native Clippy pass.
Static and independent PR review led to matched-only payload retention and exact
prefix short-circuit fixtures. All 40 remote checks pass on the final runtime
commit. The final evidence/documentation revision remains subject to PR review
before merge.

The [machine-readable evidence](../benchmarks/compact-candidate-directory-2026-09-27.json)
links the portable raw packet with complete envelopes, source patches, binary
hashes, build/validation logs, failed reproductions and guarded runners. Warmed
or uncontrolled caches and concurrent host work are accepted measurement context.
There is no new ingest measurement, format change or package publication.
