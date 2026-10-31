# Compact grouped-state acceptance — October 31

The candidate is **retained locally** under the unchanged complete-cohort rule.
Exactly one isolated execution of each of the original **347 queries** produced
all **12,565 expected rows and schemas**. Native engine time is **11.997152207
seconds**, against the prior accepted **12.055132613 seconds**: an observed
reduction of **0.057980406 seconds (0.48%)**. Hosted acceptance remains pending.
The under-10-engine-second target is still open.

## Runtime change and its proof

The existing dense-state owner lends a mutable slice when its payload fits in
one page. Typed COUNT, numeric updates and extrema bind that representation
before traversing rows. Larger payloads retain the existing paged access.
The borrow adds no allocation, copy, cache or source retention; element bounds
remain checked. Different measure widths can cross the boundary independently.

Paired extrema consume their existing whole-batch count-headroom and count-equality
proof: one next count is computed per non-null row and published to both extrema
and an already-admitted COUNT. Ordinary updates keep checked increments, and
failed admission keeps measure-first execution and its original error order.
Numeric widths, ordered floating arithmetic, strict nonfinite rejection,
extended NaN/signed-zero policy, cancellation and ownership stay unchanged.
There is no new engine, provider, dependency, public API or execution fallback.

The [prospective contract](../architecture/native-grouped-state-access-2026-10-31.md)
was frozen before implementation. Its original bytes are retained separately
from the current status text. This extends ShardLoom's grouped-state owner;
native Vortex value/validity preparation remains with its existing providers.

Debug observations cover 42 binding combinations across signed, unsigned and
floating values, both float policies and 1, 8, 512, 513, 1,024, 1,025 and 4,097
groups. Another 64 controls exercise paired headroom/equality admission and
decline, the last safe count and first overflow. Tests also cover empty/one-page
borrows, multi-page growth, cancellation after allocation, full-pool pressure
and final refunds. Shipping builds contain none of these observations.

Inspection retains 20 exact-address disassembly records from the frozen native
library. They show removal of single-page descriptor/page-index work and the
admitted paired count checks, while ordinary count overflow, element bounds and
paged checks remain. Floating normalization is preserved. COUNT has no separate
compiled symbol in this build; its coverage comes from source and native tests,
not a claim of a separately inspected COUNT symbol. Machine-code inspection
establishes removed instructions, not their isolated elapsed-time contribution.

## Complete engine observation

All entries are sums of native engine time; one execution per case is retained.
Positive changes below mean the candidate is slower.

| Family | Cases | Prior engine seconds | Candidate engine seconds | Change |
| --- | ---: | ---: | ---: | ---: |
| Projection | 26 | 1.032013624 | 1.017473002 | -1.41% |
| Predicates | 16 | 0.406708624 | 0.403261831 | -0.85% |
| Nullable expressions | 8 | 0.127514123 | 0.128082915 | +0.45% |
| Numeric expressions | 4 | 0.136239626 | 0.135670584 | -0.42% |
| Strings | 5 | 0.526262291 | 0.510266417 | -3.04% |
| Temporal expressions | 6 | 0.388938958 | 0.385259374 | -0.95% |
| Global aggregation | 67 | 1.321616962 | 1.322157172 | +0.04% |
| DISTINCT | 4 | 0.101489959 | 0.103582626 | +2.06% |
| Ordering | 10 | 0.350839957 | 0.346410833 | -1.26% |
| Joins | 14 | 1.151199625 | 1.144240711 | -0.60% |
| Sets | 4 | 0.004373083 | 0.004275125 | -2.24% |
| Grouped aggregation | 180 | 5.389164698 | 5.371484657 | -0.33% |
| Multiway joins | 2 | 0.923441625 | 0.916937626 | -0.70% |
| Composed report | 1 | 0.195329458 | 0.208049334 | +6.51% |
| **Complete cohort** | **347** | **12.055132613** | **11.997152207** | **-0.48%** |

Every case and family movement is preserved, including the slower composed
report, DISTINCT, nullable and global aggregates. None crosses the existing
diagnostic material-regression flags. Those flags are not a substitute benefit
gate: the maintainer's rule retains any lower complete engine total once the
correctness, resource, ownership and no-fallback gates pass.

This is one observation, with no statistical confidence or cold-cache claim.
Grouped aggregation accounts for only 0.017680041 seconds of the total observed
reduction. Other families move in both directions. The complete difference is
not an isolated causal estimate of this change, and it is not evidence of a
guaranteed 0.48% production improvement. The earlier 48-case instrumented
diagnostic remains a separate observation; its fractions are not extrapolated
to this cohort.

## Acceptance boundaries

The unchanged original corpus, independent references, five resident Parquet
inputs, baseline records and harness are frozen with the candidate source,
build receipt, native library and CLI. The driver changes only its label and
contract path; its query observation and supervision are unchanged. Existing
application/report compatibility declarations remain explicit in the protocol.

The run retains its 2 GiB grant, maximum parallelism two, deadlines, storage
ceilings, lightweight overlap checks, complete-result validation and cleanup.
All other task jobs and agents were idle during query execution. Full input
hashes and recursive inventories ran outside that interval. There were no
repeats, regenerated inputs, cache manipulation, artificial pacing or new
competitor executions. Original Polars observations and their different input
and API-clock boundary remain unchanged. Full43 remains held until item 18.

The default workspace passes 3,290 tests with no ignores. The native workspace
passes 6,579 tests with 36 explicitly ignored tests across 119 test binaries.
Default/native Clippy and formatting pass. The source Python suite completes
915 tests with 39 optional-native skips; installed-package acceptance passes all 284
selected tests with no skips using the frozen library and CLI.

Fresh public-interface coverage passes 450 complete comparisons and 263,265
rows across 30 declared cases, 21 source fixtures, five interfaces and Vortex,
Parquet and Arrow files. Independent expected results are fingerprinted before
execution. The controls exercise composed projections, renaming, optional
values, mixed measures and both sides of state-page boundaries, alongside
resource admission, complete consumption, detached source ownership and no
fallback. Native lifecycle tests additionally verify final credit refunds.

Every failed attempt remains recorded: an invalid feature selection, two
native lint/type-check attempts and the first disassembly collector's truncated
symbol. A supplemental disassembly command also failed because a symbol
contained commas; exact-address capture replaced that command. Final packet
setup initially omitted a Python test-library path and consequently lacked the
generated anchors; both setup failures and the successful correction are retained
in the verification record. No query result was discarded or rerun because of
these failures.

## Portable evidence and reproduction

The [main packet](evidence/native-grouped-state-access-2026-10-31/manifest.json)
retains complete values and schemas, original references, source versions,
build and process records, declarations, disassembly, every failure and all
347 observations. Large runtime/input payloads are represented by exact
identities. Replay uses maintained validators and never executes archived code.

```sh
python3 -B scripts/verify_grouped_state_packet.py docs/benchmarks/evidence/native-grouped-state-access-2026-10-31
python3 -B -m unittest discover -s scripts -p test_grouped_state_packet.py -v
```

Portable replay and all nine corruption controls pass. An isolated copy of the
maintained sources and packet also passes while access to the original checkout
and evidence directory, engine imports, subprocesses and network connections
is forbidden. All three checks execute zero queries. The
[verification record](evidence/native-grouped-state-verification-2026-10-31/verification.json)
binds these checks, their exact source versions and the failed packaging attempt.
Hosted acceptance and artifact retirement follow. CG-1 through CG-23, item-20
capability completion, broader owner experiments and publication remain separate work.

The first hosted native build completed, but its partition preparation rejected
the new manual grouped-cost probe because the explicit ignored-test inventory
had not been updated. The correction adds that exact probe to the inventory;
ordinary test selection and the strict inventory check remain unchanged. Local
discovery reproduces the original rejection and verifies complete, once-only
partition coverage after the correction. No runtime source or query observation
changes. The [CI correction record](evidence/native-grouped-state-ci-correction-2026-10-31.json)
retains the failing job, exact inventory difference and verification. All 12
inventory, partition and result-parser tests pass. A broader local run passes
16 of 17 tests but encounters macOS `PermissionError` during the unchanged
orphan-child cleanup test; that failure is retained and is not counted as a
passing suite. No child remains running afterward. Hosted checks still gate merge.
