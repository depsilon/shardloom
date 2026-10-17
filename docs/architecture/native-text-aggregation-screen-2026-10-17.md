<!-- SPDX-License-Identifier: Apache-2.0 -->

# Shared text aggregation optimization

Status: five mechanisms retained with complete local batch acceptance; hosted integration pending. This batch follows the
native file-metadata correction in PR #1565 and remains under PERF-01/04/05/10.
The maintainer requests at least five retained improvements in one cohesive PR,
with focused ship/drop checks per candidate and full acceptance at the boundary.
Ingest duplication and physical-layout economics remain the second major target
under PERF-08/09. All PERF owners and CG-1 through CG-23 remain open.

The expert comparator is an encoded-columnar maintainer checking exact grouping,
borrowed and independent ownership, checked counters, unchanged floating-point
evaluation, resource admission and complete-operation evidence.

## Current attribution and comparison boundary

The accepted control's compiled sources are frozen at `566643cd`; the final PR
head `0c4a78e3` has exactly the same 2,410 selected source files. Its CLI SHA-256 is
`4dff4bfea01887fe960e081e38a1e556a51cabe04f6af64aea53762ed77f51a6`.
The fresh native dataset, complete reference results, and unprofiled single-pass
Full43 acceptance are recorded in the
[file-metadata report](../benchmarks/native-file-metadata-buffers-2026-10-17.md).

Five new serial diagnostic samples execute Q23, Q19, Q29, Q34 and Q35 through
that same ordinary native CLI, using the accepted fresh dataset and resource
policy. Every complete result matches its reference. All source identities and
30 raw output/profiling companions are checked; child processes and the workload
lock are released. The local `hotspot-profiles-582` packet retains the original
sampling helper and explicit in-memory parameter changes. No new profiling or
testing framework is introduced. Sampled clocks receive no performance credit.

The Q29 sample observes native text decompression, domain parsing, global string
interning, and independently owned strings created inside chunk MIN/MAX updates.
Q19 also observes dictionary construction and global identity binding. These
samples establish exercised mechanisms, not exclusive stage timings or an
expected speedup. Provider, worker and caller spans can overlap.

## First candidate: defer chunk extrema ownership

The dense transformed-dictionary consumer already reduces original dictionary
values into chunk-local domain groups before merging global state. Its temporary
MIN/MAX fields currently acquire independent `Arc<str>` values every time a
local extremum changes. The dictionary's lazy ownership cache retains those
copies until the chunk ends, including values rejected by global extrema.

Keep temporary extrema borrowed from the existing dictionary. Acquire an
independent owner only when global state adopts a final chunk extremum. Reuse
one arithmetic/comparison implementation for borrowed and owned state. Keep
dictionary IDs, row counts, transform order, first-seen dictionaries, chunk order,
floating-point updates, HAVING, ordering and source selection unchanged. Global
state must never retain a provider buffer for an escaping string.

This is distinct from the historical dropped owned weighted-partial experiment:
it neither removes the original dictionary/row IDs nor reconstructs row order.
The already-retained lazy ownership cache remains useful for repeated escaping
consumers. No allocation-growth policy or worker topology changes in this step.

Vortex-first classification: `implement_shardloom_kernel` within the existing
ShardLoom grouped consumer. The pinned Vortex 0.85 `VarBinViewArray`, `BufferString`
and native array owner supply the borrowed values. Vortex's grouped kernels
consume already-grouped arrays; they do not replace this existing flat-column
domain reduction and its exact evaluation order. The current feature gates,
native provider, execution/Native I/O certificates and independent promotion
boundary remain. `fallback_attempted=false` and `external_engine_invoked=false`
remain required. No Arrow execution boundary or external engine is added.

Acceptance starts with source-backed multi-value extrema fixtures that expose
unnecessary promotions on the control. Check MIN/MAX combinations, winning and
losing chunks, Unicode, repeated weights, ties, empty/zero-weight input, overflow,
independent readability and exact release of source-buffer credit. Retain the
existing nullable and native-dictionary admission tests. Follow with matched,
unprofiled ordinary CLI comparisons against the frozen control and unaffected
query controls. A speedup is unproven until those comparisons pass.

The control fails both new promotion regressions: 96 independent strings for
one final winner, and one independent string when global state keeps neither
temporary extremum. The candidate passes all nine ownership tests, including
four new cases, and eight existing transformed-dictionary tests. Native
all-target Clippy also passes. The new arithmetic cases check zero weights,
weights above the exact integer range of `f64`, ties, shared MIN/MAX owners,
count overflow and non-finite merge rejection. The two control failures and
their test-only source patch remain retained.

The frozen ordinary-CLI screen retains this candidate. Five counterbalanced
Q29 pairs improve the fastest valid engine call from **6.765927376 s** to
**6.564265666 s**, or **2.98%**; every paired candidate call is faster. Three
single-worker pairs improve 13.184968834 s to 12.977042416 s. Three pairs each
for Q19, Q23, Q34, Q35 and Q2 raise no predeclared material-regression flags;
Q19 is 4.75% slower in this screen and remains visible in the retained samples.
All 46 calls match complete reference values and schemas, preserve resource and
no-fallback certificates, and release their processes and workload lock. The
frozen protocol, raw archives, all samples, original design and decision remain
in `text-extrema-comparison-583b`. This is focused retention, not a new Full43
result or a complete campaign acceptance claim.

## Second candidate: shared domain delimiter search

The current Q29 profile places 273 samples inside domain parsing, compared with
68 in the local weighted-state update and 72 in global interning. These are
observed nested stacks, not additive exclusive timings. Delimiter searching is
the largest observed local reduction component. The older rejected common-HTTP
branch experiment remains dropped: this candidate changes the byte-search
mechanism for every admitted URL form rather than adding scheme-specific paths.

Use the existing pinned `memchr` 2.8.3 dependency for first-colon, authority
terminator, last-userinfo delimiter, closing IPv6 bracket and port searches.
Keep scheme validation, the single case-insensitive `www.` removal, malformed
input behavior and borrowed UTF-8 slices unchanged. The same helper serves native
aggregation and existing ingest derivation; no schema, encoding, provider,
dependency, feature gate or allocation policy changes.

Vortex-first classification remains `implement_shardloom_kernel`: this is the
existing ShardLoom URL-domain semantic helper over borrowed native string values,
not a new array representation or scan provider. The pinned byte-search source
provides the portable first/last-match contract and architecture-specific
implementations; ShardLoom adds no unsafe code or external engine boundary.

Check combinations of scheme, userinfo, IPv6/Unicode/empty hosts and all authority
terminators, plus malformed schemes and long values around byte-search widths.
Prove the same expectations on the scalar control before applying the mechanism.
Then run the existing transformed-dictionary checks and a separately frozen
complete-query comparison against the retained first candidate. Ingest acceptance
at the batch boundary must preserve the complete generated native artifact.

The three new table/generated tests cover 1,214 cases and pass on both the scalar
control and candidate. Existing domain and transformed-dictionary tests and native
all-target Clippy also pass. The frozen release comparison retains this candidate:
five Q29 pairs improve the fastest valid engine call from **6.810786624 s** to
**6.401919502 s**, or **6.00%**, with all five paired deltas positive. Three
single-worker pairs improve 13.231987958 s to 12.831115125 s. Q19, Q23, Q34, Q35
and Q2 controls raise no predeclared material-regression flags. All 46 calls match
complete values and schemas and preserve native/resource certificates. Source,
input, raw archive and process-release checks pass. The original design, frozen
protocol and decision remain in `domain-search-comparison-583`. This is a focused
increment over the first candidate; no combined Full43 claim is made.

## Third candidate: reuse borrowed interner lookups (initial variant dropped)

The shared global string interner currently checks a borrowed string, constructs
its independently owned key on a miss, then hashes and searches that key again
for insertion. Q19 and Q29 profiles exercise both lookup and insertion in this
interner. The proposed change uses `hashbrown` 0.17.1's safe borrowed-entry API
to retain the initial hash and missing-key lookup. Insertion still locates its
bucket and verifies borrowed/owned key equivalence; this is not a one-probe claim.

The crate is already locked and included in the license bundle under
`MIT OR Apache-2.0`, with checksum
`ed5909b6e89a2db4456e54cd5f673791d7eca6732202bbf2a9cc504fe2f9b84a`.
Add an optional direct dependency only to `vortex-local-primitives`, with default
features disabled and `inline-more` enabled. Preserve `FxBuildHasher`; do not
introduce a new hashing policy. The source contracts are
[borrowed entry lookup](https://docs.rs/hashbrown/0.17.1/hashbrown/struct.HashMap.html#method.entry_ref)
and [checked owned insertion](https://docs.rs/hashbrown/0.17.1/hashbrown/hash_map/struct.VacantEntryRef.html#method.insert_with_key).
No external implementation is copied.

Vortex-first classification is `implement_shardloom_kernel` within the existing
consumer. Vortex dictionary/VarBinView values remain native source providers;
their local dictionary indices do not replace the consumer's persistent,
first-seen identities across chunks. Only the existing interner's lookup table
changes. The values vector, independently owned escaping strings, lazy source
promotion, forgetting/reinterning semantics, group ordering, reservations and
execution/Native I/O certificates remain unchanged. There is no new decode,
Arrow, unsafe-code or external-engine boundary.

Focused checks must cover borrowed and owned hits/misses, equal values in fresh
source owners, empty and Unicode strings, growth, forgotten IDs, reinterning,
reservation failure and exact source-credit release. Complete-query comparisons
will use the retained second candidate as control, target Q19 and Q29, and retain
single-worker and unaffected query controls. Performance remains unproven.

The candidate passes all eleven ownership tests (including the two new interner
cases), eight transformed-dictionary tests and native all-target Clippy. The
growth test checks 2,048 distinct keys, mixed ownership, duplicate hits and 683
forgotten/reinserted identities. License verification covers all 380 locked
registry packages and eight vendored providers; package versions/checksums are
unchanged, and the default native-adapter dependency graph excludes `hashbrown`.
The separate frozen comparison requires positive fastest-valid gains for both
Q19 and Q29 over five pairs each. Three pairs each exercise both single-worker
queries and Q23/Q34/Q35/Q2 controls, for 56 complete calls. The existing material
regression investigation and resource gates remain unchanged.

The original two-method variant is dropped under that frozen rule. All 56 calls
are correct, and Q19 improves 5.704150625 s to 5.321157876 s (6.71%), with all five
paired calls faster. Q29's fastest valid observation changes 6.815500251 s to
6.852283333 s (0.54% slower), despite four of five paired calls improving. The
single-worker and unaffected-query controls pass the predeclared limits; Q23
and Q34 are respectively 6.89% and 6.63% slower in fastest-valid comparisons and
remain visible. No threshold or sample is changed. `interner-comparison-584`
retains the original source patch, protocol, design, raw samples and drop decision.

### Narrowed variant: native dictionary binding only

The next variant restores the plain `intern(&str)` method's existing borrowed
`get` followed by owned insertion, and retains borrowed-entry reuse only for
`intern_dictionary`. Both methods still share the same pinned `hashbrown` table;
the table implementation change remains an affected-control risk. This narrows
the mechanism by input ownership path, with no query-name or schema-name branch.
It does not assume the first variant's Q19 improvement carries over.

Freeze a separate comparison against the retained second candidate. Q19 is the
primary target (five P12 pairs); Q29 is an affected control (five P12 pairs).
Retain three single-worker pairs for each and three pairs each for Q23/Q34/Q35/Q2,
again totaling 56 calls. Use the same fastest-valid, material-regression
investigation and resource policy. The initial variant remains a drop regardless
of this separately measured result. Re-run the focused ownership/transform tests
and native lint check before freezing the modified release build.

The narrowed variant is retained after all 56 complete calls pass. Q19's
fastest-valid engine observation improves from **5.719205624 s** to
**5.558743042 s** (2.81%); three of five paired calls improve. Q29 changes from
6.822600585 s to 6.838882500 s (0.24% slower), and the Q19/Q29 single-worker
controls are 0.16%/2.24% slower. Q23/Q34 improve 5.86%/0.59%; Q35/Q2 are
0.94%/1.22% slower. All remain within the frozen control/resource limits.
Complete values, schemas, native/resource certificates, source/input hashes,
raw archives and process-release checks pass. The independent frozen protocol,
design and retained decision are in `dictionary-entry-comparison-584`; the
initial two-method variant remains dropped. This is the third retained mechanism,
pending combined batch acceptance.

## Fourth candidate: remove the second complete-key routing buffer (dropped)

The existing numeric/minute/string count producer first constructs every typed
key, then allocates and initializes another full key buffer and copies those
keys into their hash partitions. The current profile attributes work to that
routing stage; its sampled spans are diagnostic attribution, not a query-speed
claim. Use the already computed 64-bin histogram to place keys in their existing
buffer through cycles carrying one key at a time.

Vortex-first classification is `implement_shardloom_kernel`: native typed
accessors, dictionary values and source owners remain providers, while this
consumer-specific COUNT state/routing has no corresponding admitted upstream
grouped-execution provider. Preserve the typed key, partition mixer, producer's
first-seen global string IDs, integer count arithmetic, per-partition workers
and semantic final tie ordering. No query/schema-name branch, new encoding,
decode/Arrow boundary, unsafe code or external engine is introduced.

Reserve one complete-key payload plus the existing fixed metadata. Check the
histogram's total and overflow, cancellation before/during/after placement, and
discard an interrupted local buffer before submitting it. Existing committed
state failures remain fatal rather than requesting replay. Test exact key
permutations and partition membership across empty, skewed, sorted and reversed
inputs; large renamed raw/prepared chunks must match complete serial results
across worker counts and release every lease. Confirm that the one-buffer
resource assertion rejects the control before applying the implementation.

After focused native tests and lint, freeze a separate comparison against the
retained dictionary-binding candidate: five Q19 P12 pairs, three Q19 P1 pairs,
and three P12 pairs each for Q29/Q23/Q34/Q35/Q2 (46 complete calls). Preserve the
existing fastest-valid gain, material-regression investigation and resource
policy. No performance benefit is established before that comparison.

The control fails the new one-buffer resource assertion. The candidate passes
all 18 triple-key tests, workspace formatting and native all-target Clippy,
including complete large raw/prepared results and cancellation/pressure cleanup.
The initial lint pass requested a borrowed histogram parameter and standard
method forms; those are corrected before the release/source freeze. The native
comparison is complete.

The in-place variant is dropped. All five Q19 pairs are slower; the fastest
valid engine calls change from **5.627380625 s** to **5.754487000 s** (2.26%
slower). The single-worker control is 1.73% slower. All 46 original complete
results, schemas, native/resource certificates and identity checks pass.
Q35's original median triggers the frozen control flag, despite its faster best
call. The one bounded reversed-order follow-up passes six complete calls and
does not reproduce that flag; its fastest calls are 2.922724417 s and
2.596270125 s. This does not change the failed primary target. An initial
follow-up wrapper error executes zero calls and is preserved separately; the
valid follow-up uses the existing runner's `--reverse-order` option.
`routing-comparison-585` and `routing-followup-585b` retain both original
protocols/designs, all samples, source/input hashes, archives and the drop
decision. Only the two routing source/test files are restored; all 2,410
selected sources again match the retained third candidate before the next edit.

## Compact exact string-count partials (retained)

This candidate narrows the temporary exact string-count record from
24 bytes to 16 bytes when both logical rows and the dictionary's value-index
domain fit `u32`. Keep the full `u64` representation for larger inputs and
weighted reference fixtures; this is a representation choice, not a new input
limit. The native canonical/Dict/Constant count loops select their representation
once and remain statically specialized. Preserve 64-bit hashes, exact byte
equality, complete counts, first-key order and native value owners.

This is also `implement_shardloom_kernel`: the existing Vortex native arrays
remain providers, and only the ShardLoom consumer's temporary counting state
changes. Reserve the selected width before allocation, retain the existing
resolved-dictionary capacity adjustment and deferred metadata ownership, and
preserve checked increments and cancellation. No new provider or execution
boundary is introduced. Small canonical/dictionary fixtures will exercise both
widths and compare complete values, partitioning and restored order. Actual
constant arrays on both sides of `u32::MAX` will prove exact large counts without
row expansion; a projected oversized dictionary must fail its reservation before
expanding its values. Existing pressure, global overflow and native spill
fixtures remain required. Focused validation passes 64 string-count tests,
15 count-worker tests and 32 weighted-spill tests; three existing opt-in
diagnostic benchmarks remain ignored. Workspace formatting and native
all-target Clippy pass. The first lint attempt catches unchecked test-only
conversions, which are corrected before freezing the candidate. Its separate
comparison targets Q34 and Q35 (five P12 pairs each), uses three P1 pairs for
each, and three P12 pairs each for Q19/Q29/Q23/Q2, with the existing fastest-valid
and resource policy.

The candidate is retained after all 56 complete native calls pass their result,
schema, provider, resource, source/input identity and archive checks. Q34's
fastest valid engine calls improve from **2.631022374 s** to **2.624237210 s**
(0.26%); Q35 improves from **2.637280208 s** to **2.455160875 s** (6.91%).
The single-worker controls improve 1.16% and 2.12%. None of the eight cases
triggers the frozen material-regression investigation policy. These are focused
comparisons, not a combined Full43 result. `compact-count-comparison-585` retains
the frozen design, every sample, exact source/build identities and the fourth
retention decision. Complete batch acceptance remains pending.

## Two-pass typed-key routing (retained)

The dropped in-place rearrangement avoids an allocation but introduces irregular
key movement. The changed candidate keeps sequential native input traversal:
one pure pass counts partitions, then a second pure pass writes complete keys
directly into their final partition positions. No preliminary complete-key
vector is created. Both passes use the same immutable typed numeric/minute
slices, already-bound dictionary identities, complete key constructor and
partition mixer. Within-partition source order remains stable.

Vortex-first classification remains `implement_shardloom_kernel` inside the
existing native triple-key consumer. Its Vortex array providers, feature gates,
Native I/O and execution certificates remain unchanged. Admit the existing
typed-accessor shape before reserving one complete-key buffer; other already
supported native accessor shapes keep their existing two-buffer implementation.
There is no new input limit, provider decode, ID binding, Arrow boundary or
external engine. `fallback_attempted=false` and `external_engine_invoked=false`
remain required. Reserve before allocating and check cancellation during both
passes, before publishing any input to workers. Keep first-seen global IDs,
checked counts, partition workers and semantic tie ordering unchanged.

The staged fixtures require one-buffer ownership for large raw and prepared
typed chunks, complete results across worker counts, exact key permutation and
stable order within each partition, cancellation in both passes and invalid
dictionary-code rejection. Constant numeric, minute and text columns separately
exercise the general native path. The original control's one-buffer regression
failure remains linked to the unchanged routing source. All 20 focused triple-key
tests, workspace formatting and native all-target Clippy pass, including the
additional existing prepared-aggregate fixture selected by the test filter.
The candidate is not accepted before its complete comparison.

After focused tests and native all-target Clippy, compare with the fourth
retained candidate. Predeclare five Q19 P12 pairs as the primary target, three
Q19 P1 pairs, and three P12 pairs each for Q29/Q23/Q34/Q35/Q2: 46 complete calls.
Keep the same fastest-valid, material-regression and resource policies. The
earlier in-place failure cannot be rescued or relabeled by this changed method.

The changed candidate is retained. All 46 original complete calls pass their
result, schema, provider, resource and identity checks. The original Q19 target's
fastest valid engine time improves from **5.646606042 s** to **5.536037668 s**
(1.96%); three of five pairs improve. Its single-worker control changes from
8.441221126 s to 8.468743291 s (0.33% slower).

Q35's original median, 3.174756541 s versus 3.638513209 s, triggers the frozen
investigation policy despite its faster candidate best call. The one permitted
reversed-order follow-up passes six complete calls without reproducing the
material flag; its fastest calls are 2.482832959 s and 2.487118416 s. Preserve
both cohorts and all original samples. `typed-route-comparison-586` records the
fifth retention decision with `typed-route-followup-586`; no further follow-up
is permitted. This is 52 complete calls for the changed candidate, separate from
the earlier in-place drop and from pending combined Full43 acceptance.

## Following candidates and batch acceptance

Inspect current costs before admitting subsequent mechanisms. Remaining leads
must identify work beyond the retained identity, parsing, ownership and count
changes. They are hypotheses, not authorization to revive an unchanged historical
drop. Provider scheduling is
conditional on ownership and actual utilization evidence; additional queues are
not a default optimization.

Freeze each admitted mechanism, changed source, native artifact and comparison
before timing. Preserve complete outputs and all samples; use the existing
symmetric fastest-valid rule with engine execution clocks for both roles.
Numerical historical targets guide prioritization; useful measured gains still
need proportionate complexity, correctness, memory and regression acceptance.
No partial result or overlapping worker span substitutes for a complete query.

At the cohesive boundary, run required workspace formatting, Clippy and tests,
applicable native/provider and public-surface checks, complete Full43 and the
independent renamed/null/skew/collision/arithmetic fixtures. SQL, Python and CLI
must inherit the shared implementation. Keep the separate nullable-root,
remaining resident-state external-memory, and safe output-replacement/publication
backlog visible without claiming those breadth obligations complete.

## Completed local batch acceptance

The [complete acceptance report](../benchmarks/native-text-aggregation-2026-10-17.md)
records five retained mechanisms, two dropped variants and 364 complete focused
calls. Workspace and reduced-feature tests pass 4,374 executions with no ignored
tests; native tests pass 4,022 executions with 27 unchanged manual exclusions.
All 100 direct methods, 4,827 public cases and 18 format/workflow cohorts pass.
Fresh ingestion validates all 99,997,497 rows and reproduces the preceding native
artifact byte for byte. One call to each of the 43 queries passes complete values
and schemas and records 67.405259296 seconds of engine execution.

The 28,286-member portable packet retains each original frozen design, all source
identities, raw comparison samples, rejected variants and failure/correction
history. The five gains are incremental and not additive. This single Full43
observation does not establish a causal combined improvement. Earlier pending
statements above describe each candidate's original acceptance boundary; the
linked final report supersedes them for the completed local batch only.
