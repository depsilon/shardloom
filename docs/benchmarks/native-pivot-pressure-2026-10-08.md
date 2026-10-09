# Native sparse-pivot pressure acceptance

This unit completes growing relational pivot state under the existing explicit
native spill policy. File and resident-memory sources retain the current sparse
cell semantics, dynamic schema binding, margins, limits and result consumers.
Resident execution stays the default. Direct prepared unary pivots remain
resident, and dynamic one-shot batch input still rejects before producer demand.

Status at local packet close: complete local engine acceptance, independent
packet inspection and documentation alignment. That immutable packet predates
hosted integration; the later receipt below records its completion.
The [evidence index](evidence/native-pivot-pressure-2026-10-08.json),
[portable packet](evidence/native-pivot-pressure-2026-10-08.json.xz) and
[independent inspection](evidence/native-pivot-pressure-2026-10-08-inspection.json)
bind the executable, original sources, complete values and failure history.
No comparative performance, process-RSS or package-publication claim follows.
Published v0.4.0 is unchanged. The separately approved v0.5.0 train has passed
[full fresh UAT before the version bump](release-candidate-fresh-uat-2026-10-09.md).
Installation and publication still require their release-channel proofs.

[PR #1534](https://github.com/depsilon/shardloom/pull/1534) merged at
`d7898cb95818c4c455ad770713046dbdb5a86ad1` after all 39 hosted checks passed on
`1675693e3820906bb1859e48d0cc57eec36f8603`. The accepted and merged trees match.
The [hosted receipt](evidence/native-pivot-hosted-2026-10-08.json) records exact
CI/CodeQL results, primary source review, actual preview/production search,
matching rendered guide/resource text and the working merged report link.
The hosted Codex reviewer was unavailable due to account quota; no independent
source review, automated review pass or submitted approval is inferred.

Two prior CodeQL findings concerned assertion failure messages for synthetic
test counters. Static triage found no actionable disclosure; the
[test-only cleanup](evidence/native-pivot-test-diagnostics-2026-10-09.json)
changes those messages while retaining predicates, fixtures, values and
structured counters. Fresh CI and CodeQL pass on the final head. Of 1,015
original assets, 1,012 remain byte-identical, two contain the previously accepted
harness repair and one contains that test-message cleanup. Native executable
behavior and the frozen runtime binary are unchanged. No alert was dismissed
or analysis rule suppressed.

The packet contains 53,641,616 compressed bytes with SHA-256
`e7107caebc9f374672e6a5d755feb53b521796233a2eab7ff52ce4ac71ef901a`.
Complete decompression produces 4,519,733,612 bytes with SHA-256
`349ea38fdced68f8cb25e23019d5d367d1a2264fda7fa44dbd36e9e27762e637`.
Original hashes identify local bytes; portable text substitutes local paths.
The finalizer reconstructs and reopens complete values before a separate stream
inspector checks packaged coverage, claims and raw no-fallback reports.

## Contract and reuse

The [design and reuse map](../architecture/native-pivot-pressure-2026-10-08.md)
records the Vortex-first decision. Pinned Vortex 0.85.0 supplies typed arrays,
builders, native take, Flat file writing and bounded row-range reads. ShardLoom
retains online pivot semantics, exact key comparison, admission, cancellation,
disk quota, cleanup and publication. No dependency or external engine is added.

A bounded sparse buffer flushes complete latest cell states into immutable
native runs. A later update resolves the newest exact state before applying the
existing transition. Adjacent chronological merges retain the newer replacement;
they do not reassociate floating partial totals. Index markers retain the first
representative once. Charged key bounds, two credited payload slots and held
generation checks bound lookups without a full resident index or dense matrix.

Completion preserves source-order limits, discovered domains and the existing
column/grand-margin observation order. Bounded index-position batches gather
selected values late. Shared cell transitions retain first/first-unique,
nullable and nested extrema, floating-prefix checks and exact decimal totals.
The shared native writer also drains accepted work before returning a producer
or sink failure, retaining its first I/O error and preventing failed publication.

This remains the existing relational SQL/Python/DataFrame/CLI route and its
admitted collection, incremental-result and representable writer consumers.
It adds no aggregate, type, margin or field admission. Dynamic one-shot input
needs a separate complete-plan admission contract before schema discovery can
consume a producer. Spill alone does not provide that contract.

## Complete constrained workload

One file-backed fixture contains 24,580 input rows with 6,145 distinct 768-byte
string keys. Each key receives the ordered floating updates `1e16`, `1`, `-1e16`
and `3`; the expected final value is `3`. Both spill executions check every
output value and canonical key order against that independent construction.
The same fixture supplies the constrained-denial and ample-resident controls.

| Observation | Result |
| --- | --- |
| Native input size | 19,342,360 bytes, larger than the constrained grant |
| Constrained native grant | 16,777,216 bytes (16 MiB) |
| Resident strategy at that grant | Deterministic reservation denial; credits restored |
| Ample resident control | Complete output at 512 MiB; peak tracked reservation 291,378,361 bytes |
| Explicit-spill executions | Two, each returning all 6,145 rows |
| Explicit-spill peak tracked reservation | 15,657,857 bytes in each execution |
| Index rows / domains / cells | 6,145 / 1 / 6,145 |
| Native runs / merge passes | 775 / 387 in each execution |
| Held-reader opens / lookup blocks | 776 / 37,896 in each execution |
| Peak temporary disk | 37,324,372 bytes in each execution |
| Disk quota / flush threshold | 256 MiB / 1 MiB |
| Final ownership | Owned runs removed; input/result owners released; baseline credits restored |

Lookup counters do not establish physical disk-read counts or a bottleneck.
These controls prove the declared larger-than-grant case, not arbitrary domain
width or unlimited workload size. Public pressure cases separately use the
1-GiB minimum grant and 1-MiB flush threshold; their input is below that grant.
They prove route, semantics, composition, writers and failure behavior. Neither
scope bounds all provider allocations, Python retention, conversion libraries,
allocator overhead or total process RSS.

## Semantics, composition and failures

The added public pressure family contains 3,626 checks and 1,713,893 complete
row comparisons: 2,902 positive result checks, 723 deterministic denials and one
syntax-only inspection with an uncreated input. The inspection has no separately
retained result envelope; its frozen harness and absent input are the explicit
proof boundary. Prior independent literal expectations are preserved, with only
reviewed paths, policy, labels and incremental declarations changed.

Scalar dynamic schemas are checked against the already accepted window-build
envelopes; nested schemas come from frozen literal declarations. Complete
collection and writer results cover existing aggregates, typed/nested roles,
NULLs, empty input, fill, naming collisions, margins, source limits and composition
before and after pivots. Scalar output uses all eight admitted writers. Typed
and nested output preserves its existing representability: CSV uses quoted JSON
text cells without native dtype persistence, and ORC rejects unsupported types.

The 133 incremental transactions reopen all 197,148 output rows, including two
65,541-row controls above the collection row limit. Each proof requires exact
schema, bounded batches, acknowledged delivery, complete result evidence and
successful child exit. Empty output retains one acknowledged zero-row batch
with its exact typed schema. Existing streaming coverage remains at 442 cases;
this pivot unit does not newly admit a one-shot dynamic source.

Native tests cover errors across cell updates, eviction, merge passes, margins
and output access; exact decimal cancellation/finalization; signed zero and
values hidden by validity; quota and reservation exhaustion; source changes;
run corruption, replacement and cached generations; cancellation; failing
consumers/writers and protected destinations. A real child-process termination
test rejects live/unknown ownership, cleans only owned abandoned state, and
finishes a fresh restart. Recovery does not resume an interrupted pivot.

## Verification and provenance

The accepted runtime is `5665eee5b86a585ea50b7b54cbbd43f82154222f`.
Its 1,015 frozen source assets have combined identity
`c754261c7fc394147b3c7e7d37f8d5662d1b00c6ed9520393759144668e15855`.
The locked, offline release build enables `release-user-surfaces`; executable
SHA-256 is `22eb39060595afbde22e9ed4a0f1ca87c3504c58fe68f22641c336048eee8404`.

| Verification | Observed result |
| --- | --- |
| Formatting, default workspace lint/tests | Pass; 3,145 tests, zero ignored |
| Native Vortex library and examples | 2,495 library tests pass, 24 ignored; 17 example tests pass |
| Native CLI tests and CLI/Vortex lint | Pass; 1,310 CLI tests |
| No-write, write-only, lean and MSRV feature checks | Pass |
| Python tests with optional adapters | 616 pass |
| Complete public family union | 32,497 checks; 18,595,284 complete rows |
| Added pivot pressure family within that union | 3,626 checks; 1,713,893 complete rows |
| Incremental pivot results within that family | 133 transactions; 197,148 complete rows |
| Direct unary regression | 202 checks; 131,734 complete rows |
| Completion-aware input suite | 442 retained checks; 517,499 complete pressure rows reopened |
| Resident batch / format fidelity | 48 / 19 checks |
| Admitted semantics / golden workflows | 145 / 9 stages |
| Retained Full43 reference | All 43 queries, three calls each; 129 complete results pass |
| Independent packet inspector contract | 190 positive/negative checks pass |

All 15 source gates used the same original accepted source assets. Required
commands are `cargo fmt --all -- --check`,
`cargo clippy --workspace --all-targets -- -D warnings` and
`cargo test --workspace --all-targets`; exact native/feature/MSRV commands and
logs are retained separately. Manifests, lockfile and vendored source remain
byte-identical to the accepted window build. Suites overlap; their counts must
not be added as independent workloads.

All ten public families completed successfully, but the original suite controller
rejected two identical shared oracle declarations when combining summaries.
The repair deduplicates only exactly matching cross-family declarations after
rechecking each generation and hash. Same-family duplicate sources, conflicting
metadata and duplicate cases/envelopes/archives still reject. Ten focused tests
pass. A separate successful union reuses the immutable family results without
rerunning native queries or changing expected values. The failed controller
summary, outer supervisor and all ten family receipts remain in the packet.

The PR integrates precisely that controller/test repair after freezing and
inspecting the original runtime packet: 1,013 source assets remain byte-identical,
and exactly `scripts/run_native_relational_suite.py` and its test file have
the recorded repaired hashes. All native and Python product execution sources,
case declarations, expectations and the accepted executable remain unchanged.
The support provenance and later fresh-UAT admission enforce this exact delta.
Do not describe the post-repair tree as 1,015 identical source files.

Final readback also retains three unsuccessful verifier attempts: an old union
reader, a missing benchmark-helper import path, and an incorrect assumption that
empty results omit their schema. The accepted fourth readback uses the verified
combiner and original per-family readers, the required dependency paths, and
the existing typed zero-row batch contract. No native query or oracle changed.

The [support receipt](evidence/native-pivot-support-2026-10-08.json) and
[support archive](evidence/native-pivot-support-2026-10-08.tar.xz) retain nine
passing documentation/controller checks, seven executed native examples,
generated-page sources and browser screenshots. Search for `pivot` reaches the
Python batch-result section; desktop dark and mobile light views, navigation,
resource guidance and limitations pass with no console warnings or errors.
The first browser pass exposed missing Python search content; its evidence is
retained alongside the corrected generated page and successful second pass.
Temporary tabs, viewport settings and preview servers were closed or restored.
The primary agent's adversarial source review found no blocking issue; this is
not a claim of independent or hosted automated review.

## Retained-dataset observation and storage

The retained Vortex input contains 15,682,956,489 bytes, with SHA-256
`5a6e7d7b521b891e522f32398ef2582367e814791ab537f354003ecd2f7a488d`.
The unchanged native correctness-reference packet has SHA-256
`cb83c770674073f31ad0c4224f02faeb1ba557c512595a5c2d36f92bb2344b39`.
The recorded host is macOS 27.0 arm64 with ten logical CPUs and 16 GiB physical
RAM. The request is `memory_gb=24`, `max_parallelism=12`; admission settings are
separate from physical capacity, actual use and exclusive-machine guarantees.

Across three calls per query, the sum of query minima is 63.954956 seconds,
the sum of medians is 64.819043 seconds, and all 129 native process times sum
to 195.328213 seconds. These include startup, complete public output and exit.
The supervised query-stage wall time is 226.195873 seconds, including guards,
value checks and log handling, excluding separate preflight. Highest observed
native child RSS is 5,318,213,632 bytes. Source prehashing and uncontrolled OS
cache/desktop activity are recorded. There is no fresh ingest in this packet,
paired comparison, causal speedup, official ClickBench rank or enforced RSS claim.

The first pivot-pressure preflight stopped under the free-space guard; its 3,279
partial passes receive no acceptance credit. Guarded cleanup removed the unused
Cargo debug cache and 384 generated payloads from three completed historical
pipeline cohorts, preserving all summaries, failures, source fixtures, frozen
executables and explicit regeneration records. Free-space observations rose by
approximately 68.5 GB across the removals. A fresh preflight then passed all
3,626 checks with unchanged runtime and expectations.

Full43 preflight separately stopped before any query at the unchanged 252-MiB
log threshold. Two completed historical cohorts were compacted only after
identity/open-handle checks and complete byte verification, recovering 4,575,232
accounted bytes. Finalization independently reopens all 1,032 archived original
JSON/companion members. Failed/incomplete evidence and all ceilings are retained.

The [remaining-scope contract](../architecture/native-local-completion-scope-2026-10-07.md)
preserves broader allocation coverage, unadmitted state/recovery, streaming and
adapter work, operational platform/release acceptance, and all eight conditional
investigations. The v0.5.0 authorization changes sequencing; it does not close
those obligations or permit fallback execution.
