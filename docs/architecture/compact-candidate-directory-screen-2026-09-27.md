# Compact immutable candidate directories — C4

Status: admitted bounded experiment under PERF-INTAKE / RFC 0044, after the
R5.c attribution PR #1469. No gain is claimed before complete-query comparison.

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
The immutable table remains local to the chunk; do not introduce a cache,
perfect hash dependency, cross-chunk string owner or persistent format. The
code-index table uses one machine word per value instead of three-word vector
descriptors. Include construction and every row probe in complete public Q18
timing. This is a storage-layout fact, not a claim about process RSS or total
query memory. Retain any useful measured gain; no one-second/percentage cutoff.

Vortex-first check: native dictionary codes already provide dense exact indices.
Use those indices, retaining existing source values and full-byte string-ID
binding. A new perfect hash over the same codes would add construction without
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
Only the irredundant key columns are evaluated: dependent output expressions
remain unevaluated for unretained rows. The review's dependent-offset overflow
fixture reproduced the initial full-expression probe regression before the
key-only correction. It does not admit new groups or strings. Failing logs are
retained. COUNT and
COUNT DISTINCT regressions cover this closure transition independently of the
compact directory; the latter covers both owned and interned General seeds.
Q18's comparison uses the direct candidate-directory route, so this generic
correctness fix is not credited with its performance outcome.

Separate pre-existing obligation: mixed generic/direct key construction before
admission closure, and scalar-to-direct transitions after closure, require a
broader representation-normalization audit. This patch does not claim that
arbitrary mixed routes or compact-to-General state conversion are supported.
