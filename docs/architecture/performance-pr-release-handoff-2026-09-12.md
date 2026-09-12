<!-- SPDX-License-Identifier: Apache-2.0 -->

# Performance PR and release handoff

Status: selected source incorporation complete and independently source-reviewed.
The maintainer has paused testing, builds and benchmarks while the previously
measured enhancements are assembled for review. The combined PR has not been
validated or measured. Package version
sources remain `0.2.3`; the proposed `0.2.4` train follows the performance PR.

The PR starts from merged `origin/main` at
`5e8af695c02459be4fe7c6d3c49d3459d72a103f` on
`codex/performance-pr-20260912`. PR #1440 already contains the accepted
`2ad143da` runtime. Its squash ancestry must not cause those changes to be
reported as new work or its newer control/progression documentation to be
replaced by the older branch copies. The original local `main` checkout remains
untouched.

This is a finite incorporation decision, not a restart of the full capability
queue. The [phase plan](phased-execution-plan.md) remains the queue owner.
ShardLoom technique review: retain shared native workers and exact partition
reduction, avoid payload reads where exact held-file metadata proves results,
and avoid JSON/StatValue result-row construction through bounded native output.
Preserve capillary/PulseWeave admission, actual worker evidence, source identity,
explicit pressure behavior and result-owner lifetimes. No external query-engine
fallback, new dependency, query-name switch or general claim expansion is added.

## Include and incorporation state

The states below describe source incorporation only. Earlier receipts remain
bound to their original sources and binaries. The assembled branch still needs
its own compilation, tests and measurement before acceptance.

| Enhancement | Incorporation state | Evidence supporting selection | Boundary |
|---|---|---|---|
| Accepted native ingest, numeric/dictionary aggregation, prepared/resident execution, existing spill and owned integer DISTINCT | Already in `origin/main`; preserve | Existing merged acceptance and control/progression ledgers | These are inherited capabilities and observations, not new gains from this PR. Retain the existing numeric-ingest configuration. |
| Nonnullable UTF8-group / integer COUNT DISTINCT shared workers | Source incorporated as `4f1e3caf`, `9d7de933`; focused report `68a29064` | Frozen `4730003f`: 48 exact paired results, 96 renamed-fixture results, 129/129 exact Full43 results. Q14 raw medians 8.815227 → 2.215635 s; median paired candidate/control ratio 0.251342 | Ordinary native result route. Preserve exact full-pair equality, complete EOF group counts, integer signedness, UTF8-byte ties, offsets, bounded selection and deterministic pressure. No new UTF8 DISTINCT owned/prepared/spill contract. |
| Owned single-integer-group COUNT(*) | Source incorporated; shared finalizer, tests and accepted cost protocol | `607db50a`: 184 exact COUNT executions. At 32,768 output rows, P1 JSON/owned medians 35.714833/5.652708 ms (about 6.32×); P4 36.771832/4.295791 ms (about 8.56×). A separate 184-call integer DISTINCT control also passed | Existing public prepared aggregate and owned-result/sink boundaries; one nonnullable identity integer key, exact original key width and U64 counts. No compound keys or extra measures. |
| Prepared and owned single-UTF8-group COUNT(*) | Source incorporated; native buffers, retained admission, tests and accepted cost protocol | `b40fd02a`: 184 exact executions. At 32,768 output rows, P1 JSON/owned medians 137.740875/52.095292 ms (about 2.64×); P4 122.152666/33.099834 ms (about 3.69×) | One nonnullable identity UTF8 key and COUNT(*), bounded exact rank/output and held source generation. Selected UTF8 bytes are copied into native buffers; zero JSON/StatValue output rows does not mean whole-query zero-copy or zero-decode. |
| Exact scalar integer footer aggregates | Source incorporated; proof helper, shared scan/report wiring and tests | Replacement `8acb1263` Q19/Q7 screen: 16 exact calls total. Q7 medians 40.515333 → 12.845041 ms; each candidate Q7 proves two measures from four exact statistics, zero payload arrays/visited rows and 99,997,497 covered rows | Existing held-file metadata path; unfiltered global identity integer MIN/MAX/COUNT, optionally COUNT(*), with every measure proved before completion. Missing/inexact facts preserve the existing scan; malformed/contradictory facts remain deterministic. No SUM/AVG, grouped completion or nullable-minute worker proof. |

The owned COUNT figures compare two result representations using the same
binary and public prepared API. They are not before/after revision speedups,
ordinary CLI gains or Full43 results. Each family uses 262,144 rows and 32,768
groups, P1/P4, K32/K32768, three excluded warmup pairs and 20 alternating measured
pairs per case: 184 executions. Timing includes execution, result construction
and result drop; independent complete-value validation is outside that interval.
Every call must return to its captured prepared-source reservation baseline.
Small-output controls and all raw samples remain part of the original packets.

The footer screen's 16 executions cover two queries and both arms; it is not 16
Q7 executions. Its benefit is small relative to the full workload and does not
justify an overall query-speed claim. Source-generation validation and actual
NULL semantics must survive the port.

Independent source review covered COUNT admission, complete-state selection,
buffer grants and final-owner release, and the footer proof, held descriptor,
scan avoidance and certificate integration. It found an older COUNT admission
gap: a nullable parent Struct could pass a child nonnullability check. The port
now rejects that parent before reserving result memory, with a source regression
case for COUNT and the existing DISTINCT route. Narrow non-Unix lint annotations
cover the COUNT key kind and Unix-only footer proof construction while retaining
shared report validation. These source-review corrections remain unexecuted
during the pause.

The [owned COUNT contract](../reference/owned-count-results.md) and
[footer contract](../reference/exact-footer-aggregates.md) describe the final
scope. Existing aliases and HAVING are preserved without importing the later
aggregate projection API. The cost example retains the accepted `b40fd02a`
integer/UTF8 protocol; its later compound-key extension is excluded.

## Excluded from this incorporation

| Work | Disposition and reason |
|---|---|
| Nullable prepared-minute proof admitting three-key workers | Dropped automatic admission: measured Q19 regression 11.506181 → 60.301832 s. Preserve the original nullable serial route; do not import the proof with the footer helper. |
| Allocation/topology variants, replay caches and other previously rejected microvariants | Keep dropped under the existing control ledger. Prior correctness or architectural interest does not overturn measured rejection. |
| Unconditional text compression/zoning and broader codec portfolios | Preserve the fastest accepted ingest policy. Smaller output was a separately scoped storage tradeoff with slower lifecycle/query observations; it is not a speed winner for this PR. |
| Owned integer+UTF8 compound COUNT | Failed acceptance, not an established speedup: first ordinary warmup retained 234,404 bytes against baseline zero. No owned-arm comparison completed. Preserve failure/provider-lifetime evidence; do not import its later example protocol. |
| New UTF8 DISTINCT spill, nullable COUNT, extra owned measures/results, general join/window, projection/API expansion, ingest portfolios and further metadata/pruning work | Parked as unmeasured, separately scoped or unnecessary for these selected ports. This is not a claim that every item regressed. Include only a demonstrated necessary correctness dependency, with an explicit source review. |
| Q23 accessor redesign or another speculative optimization | No implementation selected here. Existing cost evidence may guide later bounded work; it does not expand this PR. |

## Evidence and validation boundary

The [focused DISTINCT packet](../benchmarks/focused-utf8-distinct-2026-09-12.md)
owns the tested `4730003f` identities and results: workspace formatting and
Clippy, 3,417 default tests, 3,278 native checks with nine existing ignored manual
cases, 19 focused tests already included in those totals, and the paired,
renamed-fixture and Full43 results. Its **102.005509-second Full43 observation
belongs only to that frozen runtime**. It must not be attributed to this new
combined source or substituted for a new binary's acceptance. The historical
91.215296-second timing control remains unchanged; the sequential Full43
observations do not establish a causal overall speedup.

Machine-local immutable receipt root:
`/Users/dylan/LocalData/shardloom/perf-all-20260906`.

- Integer COUNT: `owned-cost-resume-607db50a-count-r1.json`, with build receipt
  `results-resume-607db50a-build.json`; unchanged DISTINCT control:
  `owned-cost-resume-607db50a-count_distinct-r1.json`.
- UTF8 COUNT: `owned-cost-resume-b40fd02a-count-utf8-r1.json`, with build receipt
  `phase-resume-b40fd02a-build.json`.
- Footer replacement: `phase-query-8acb1263-p12-r1.json`, with build receipt
  `phase-resume-8acb1263-build.json`.
- Failed compound cost: `owned-cost-guarded-resume-43285c79-count-integer_utf8-r1.json`.

The build and execution receipts bind original source/binary/helper identities,
complete-value checks, samples and resource guards. Do not rewrite receipts,
fixtures, protected references or losslessly archived transcripts. Source review
and incorporation do not turn previous component evidence into combined-tree
validation. No tests, formatters, builds, benchmarks or release rehearsals are
run during the current pause. Existing CI/release gates remain intact; their
execution and any additional combined-tree proof await resumed authorization.

When testing resumes, start with focused native owned COUNT, existing integer
DISTINCT, UTF8 DISTINCT, footer, prepared-source and sink regressions and the
owned cost example's tests. Then run the required formatter, workspace Clippy,
workspace tests and broad native feature checks. Rebuild a source-identified
release binary before the owned complete-value/ownership protocols and new
paired query or Full43 evidence. Preserve existing
artifact/storage/concurrency guards, excluded warmups, controls and complete
result checks; a reused binary is not proof for this branch. Any required PR
governance/documentation checks also remain pending rather than implicitly
passing because this pass was source-only.

## Version and publication order

The public GitHub `v0.2.3` pre-release was published September 5, 2026; the
integration owner verified that current release through a read-only GitHub check.
The checked-in [publication verification](../release/v0.2.3-publication-verification.md)
and [channel matrix](../release/package-channel-readiness-matrix.md) retain the
GitHub, TestPyPI, PyPI and Homebrew proofs. Current package manifests and
`scripts/release_channel_contract.py` remain at `0.2.3` during this PR.

1. Finish the cohesive performance source PR and its reviewable evidence and
   limitation notes. Preserve merged main documentation and paused experiments.
2. After that PR, prepare the compatible `0.2.4` technical-preview train from its
   merged source. Root `Cargo.toml` `[workspace.package].version` is the source
   of truth. The existing `scripts/sync_workspace_package_versions.py` derives
   Python `_version.py`, website package/package-lock versions and workspace
   Cargo.lock entries; execute it only after the current pause permits it.
   The six internal crates inherit the workspace version and remain unpublished.
3. Add new `v0.2.4` release notes, source-bound validation/package evidence,
   checksums, SBOM/provenance and current documentation as the train requires.
   Preserve old releases and proofs. New artifacts cannot reuse the earlier
   binaries' validation merely because their source families were selected here.
4. Follow the existing selected-channel order when publication is authorized:
   GitHub tag/pre-release assets, TestPyPI proof, PyPI proof, then Homebrew proof.
   Bundled CLI wheels must use the existing staged native package recipe and
   channel-specific install/uninstall/smoke and checksum evidence.
5. Advance the centralized published version in
   `scripts/release_channel_contract.py`, channel/public-status matrices and
   current install documentation only after new channel proofs exist. During
   release preparation they correctly continue to describe published `0.2.3`.

This proposed patch train follows the existing compatible native-performance
release precedent. A deliberate meaningful public capability/support promotion
would require reconsidering `0.3.0`; no such broader promotion is selected here.
Neither version choice closes PERF/CG gates or authorizes production,
competitive-superiority, new package-channel or external-fallback claims.
