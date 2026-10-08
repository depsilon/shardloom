# Retained native derived-owner observation

Status: the bounded ownership screen passes. It identifies a candidate for a
complete-workflow experiment; it does not admit a production rematerialization
policy, report a speedup or establish a process-RSS bound.

The [index](evidence/native-rematerialization-owners-2026-10-07.json) and
[portable packet](evidence/native-rematerialization-owners-2026-10-07.tar.xz)
retain the frozen protocol, temporary observer, complete outputs, independent
inspection, build identity, failed packaging attempt and exact restoration proof.
The archive is 90,016 bytes, SHA-256
`1ec5cf5c8b6ad18e19b2a683541ba930e284dc6e731415aaf2f66671e28aae12`.
All 31 members were independently reopened and checked against their manifest.

## Measured owner and controls

The observer uses actual native relational `Batch`/`Table` owners and the
existing expression evaluator. It retains an immutable source checkpoint and
ordinal keys, constructs derived payload, drops the payload-bearing array while
keeping keys/checkpoint, and regenerates each derived column exactly once.
Independent formulas compare every original and regenerated value. Key hashes
are checked before/after release. This tests actual buffer credits, not a dense
logical-byte estimate or a new simulated cache.

The frozen matrix has 2,048 and 32,768 rows; one, eight and 64 payload columns;
derived, source-alias, constant and derived-key modes; plus a large clone/slice
control. Each of the 25 cases has one CPU lane and a 256-MiB native grant.
All 12,263,424 original and 12,263,424 regenerated values match. Every case ends
with zero retained native credits and zero denied reservations.

| Observation | Result |
| --- | --- |
| Six ordinary derived-payload cases | Dropping payload reclaims actual native credits. |
| Eighteen source-alias, constant and derived-key controls | No payload credits are reclaimed; the retained dependency/key ownership still matters. |
| Largest derived case, 32,768 × 64 | 17,869,504 bytes retained before release; 17,072,128 freed; 797,376 remain for checkpoint/keys/metadata. |
| Largest payload representations | Native payload `nbytes` is 17,039,360; dense logical values are 16,777,216 bytes. Neither equals the reclaimed credit count. |
| One-column regeneration in that case | Largest sampled increment is 266,752 bytes. This is a sampled headroom observation, not a measured reconstruction peak. |
| Clone/slice control | Dropping the table and then its clone frees zero payload bytes. Dropping the final slice frees 17,072,128 bytes. |

Development-profile construction/regeneration durations remain in the raw
records. They are observational timings, not a production performance comparison.
The observer's oracle vectors and recipe metadata are outside the native pool;
the recorded pool peak is cumulative. No complete public workflow, eviction
policy, repeated-regeneration workload or spill comparison was tested.

## Disposition and restoration

Selective rematerialization may next evaluate retained non-key derived payload
with immutable dependencies, full regeneration cost and separately admitted
headroom. Do not evict keys, source aliases or constants on the assumption that
their apparent logical size will be reclaimed. Do not replay a one-shot producer.

The temporary observer and hook were removed after the screen. All 947 accepted
production assets exactly match runtime `b7de216f`; the source checkout was
`d355f0ea` over accepted merge `c16f8da7`. The first archive attempt failed before
writing because its privacy assertion matched its own generic path literals.
The corrected packaging check preserves the original failure and unchanged
observation code/results. No native rerun, threshold change, production policy,
dependency or package publication was used to resolve that packaging error.

The [remaining-scope contract](../architecture/native-local-completion-scope-2026-10-07.md)
keeps this screen distinct from the broader stateful, adapter and operational
work and from the other seven open investigations.
