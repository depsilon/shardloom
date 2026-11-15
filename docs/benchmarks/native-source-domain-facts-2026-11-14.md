<!-- SPDX-License-Identifier: Apache-2.0 -->

# Exact source-domain predicate execution: retained

The bounded C extension is **retained locally** at
`76f3c90c3b25338f3250dff10daf04eee081f9b4`. Complete results, schemas, ownership,
resource policy, native execution and actual encoded-dispatch checks pass.
Both complete engine totals decrease under the standing binary gain/loss rule.
The [architecture contract](../architecture/native-source-domain-facts-2026-11-14.md)
and [portable evidence](evidence/native-source-domain-facts-2026-11-14/README.md)
define the admission boundary and preserve every observation.

## Complete engine comparison

| Population | Requests or operations per runtime | Accepted dictionary baseline, engine seconds | Source-fact candidate, engine seconds |
| --- | ---: | ---: | ---: |
| Complete fresh-session cohort | 2,786 requests | 34.706028110 | 33.876871033 |
| Original predicate and computed-float controls | 746 requests | 33.130136190 | 32.411797647 |
| Added source-fact and ordinary-path controls | 2,040 requests | 1.575891920 | 1.465073386 |
| Separate prepared-plan lifecycles | 120 operations | 0.112387206 | 0.085650882 |

The complete fresh gain is 0.829157077 engine seconds. All 1,497 individual
regressions remain in the raw ledgers and replay report; their positive
candidate-minus-baseline differences total 0.263583494 seconds. Small finite,
all-null and empty controls include regressions, while the larger finite-float
profiles improve in aggregate. No result is discarded and no subgroup threshold
replaces the complete fresh and lifecycle gates. Each of 60 ordinary prepared
plans per runtime executes exactly two scored calls, with no unreported warmup.

These are sums of `engine_execution_seconds` within each declared population,
not a Full43 or full10k observation. The comparison has uncontrolled cache and
host state and includes identity-verified historical baseline reuse. Most of the
absolute total difference lies in the original controls. This observation does
not establish causal attribution for an individual difference, an optimization's
isolated contribution, or general performance superiority.

## Native mechanism and coverage

The already held Vortex footer supplies exact finite F32/F64 domain facts for
an alias-only filter. Admission requires an exact zero NaN count and exact finite
ordered extrema; exact all-null counts or an empty source prove an empty valid
domain. Missing, inexact or contradictory facts leave the existing typed native
evaluation in place. Computed projections and limit barriers cannot inherit
source facts for a different row or value domain.

Literal conversion is exact. Signed-zero equality becomes the interval
[-0,+0], inequality excludes both, and ordered predicates select the appropriate
signed endpoint. All six comparison operators retain typed behavior. Positive
IN expands zero to both representations in the native proof tests; the frozen
public adapters do not admit IN, so no IN comparison exposure is fabricated.
F16, nonfinite or lossy literals, null-containing IN and negated IN remain
outside the optional rewrite. Whole conjunctions are admitted together. The
existing dtype-only integer, Boolean and text gate is unchanged.

The candidate uses pinned Vortex 0.93.0 footer statistics and ALP/FastLanes
comparison adaptors inside the existing `shardloom-vortex` boundary. It adds no
codec, persisted summary format, general fact framework or execution dependency.
Source generations, original declarations, shared native state and complete
result ownership remain bound through execution. No external engine evaluates
residual work; fallback stays false.

The 746 original declarations are selected in manifest order from the saved
inventory without timing-based selection: 591 generated predicates, 25 legacy
source predicates and 130 computed-float result barriers. The 2,040 added
declarations cover 12 profiles, two sizes, 17 forms and five public surfaces:
CLI SQL, embedded workflow, Python SQL, DataFrame and typed relation. Profiles
include real ALP F32/F64, flat and no-stat controls, all-null and empty sources,
and FastLanes bitpacked/frame-of-reference U64. Nonempty public fixtures have
79 rows; work fixtures have 12,291 rows in three explicit native chunks.
Independent literal references enumerate every cell and schema, including both
signed zeros. All 24 complete native fixture readbacks match those references.

Exactly 336 accepted dictionary-runtime observations are reused with unchanged
request, reference, input, runtime and consumer bindings. They remain part of
the complete baseline total. The remaining 2,450 baseline requests and all
2,786 candidate requests execute once, followed by the two lifecycle stages.
Each route uses two threads, a 2 GiB native memory grant and the existing
deadlines, storage and process-cleanup guards. Input hashing and complete
source/evidence inventories occur outside query execution. No build, other
task work, profiling, cache manipulation or successful-observation repeat runs
alongside the comparison.

All 620 finite-source admission requirements and 150 compressed-dispatch
witnesses pass. Optional operation-scoped counters record only successful ALP,
bitpacked and frame-of-reference adaptor returns, with a reserved owner and
fail-closed attribution on overflow or registry poisoning. Empty/null shortcuts
and declined adaptors do not count. The counters include repeated/nested logical
operand rows; they are not unique rows or decoded bytes. The accepted baseline
has no equivalent counters. Requested-read observations vary and do not support
a blanket read-reduction claim. No device-I/O or process-RSS saving is inferred.

## Validation, preserved failures and source identity

The executed source fingerprint is
`d078ce9a718bb8c56985ae9376a5eccbd7725724b83483e277db56a91b3c6297`.
All 5,782 captured source files match committed blobs. The runtime was built
from parent `e5a3c3440` and its recorded dirty-source snapshot; the later source
preservation commit is not represented as the build revision. Release command:

```text
cargo build --offline --locked --release --features release-user-surfaces -p shardloom-cli -p shardloom-python
```

Required validation passes:

- Workspace format and strict default/native Clippy.
- Default workspace: 3,344 passed, zero failed or ignored; all names verified
  across 108 test binaries.
- Native workspace: 7,254 passed, zero failed, 39 declared ignored; all 7,293
  listed names verified across 123 binaries, including ten source-fact tests.
- Vortex array provider: 5,779 passed, zero failed, one ignored, plus strict
  provider Clippy with the pinned lock and complete original-crate provenance.
- File-only, local-only, all-features, minimal-workspace and reader boundaries,
  plus minimal/native checks with Rust 1.96.0.
- Installed package: 455 passed, zero failures, errors or skips. Four separate
  public tests cover all five surfaces, owned outputs, write/reopen and source
  replacement, alongside accepted dictionary/scalar controls.

The first complete native attempt exposes two operation-credit assertions and
a real observer lifetime issue: a weak registry entry can retain an Arc backing
allocation after its credit is released. The corrected owner clears its entry
before releasing the strong owner; in-flight records retain their credit and
cannot contaminate another operation. Regression tests cover ordinary, poisoned
and in-flight ownership. The two operator tests compare their operation-local
credit deltas against the legitimate observer reservation.

A later provider lint failure requires equivalent Debug formatting and an
explicit poisoning-based exception for the metadata registry mutex. R3 then
passes the full code checks, but its installed proof stops before installation
because Python's notice manifest has stale vortex-array provenance. R4 corrects
only that notice. Complete source maps prove the one-file difference, and all
three native artifacts are byte-identical to R3. A portable compatibility proof
permits explicit reuse of R3's Rust/provider/public checks while R4 passes fresh
notice and installed-package validation. License texts, versions and components
are unchanged.

Exact R1/R2/R3 snapshots and failed reports remain in the packet, together with
earlier focused failures and guard metadata. Earlier pre-runtime focused attempts
retain their logs and source hashes; they are not represented as complete source
snapshots. The original public setup failure is also retained. All four scored
stages complete without a failed or repeated query. Control Python and the
consumer environment are bound separately, retaining all 367 original consumer
files and the same physical Python 3.13.13 interpreter.

## Portable replay and remaining work

Saved replay verifies all 5,572 complete fresh results and 240 lifecycle
operations against their independent references, certificates and clocks.
All 28 saved-evidence controls and 50 portable-packet controls pass. Relocated
replay forbids original source/runtime/evidence reads, engine imports, network
access and subprocesses. The installed closeout packet independently reproduces
the retain decision. Run its `verify.py` with Python 3.13 or newer; no query
rerun or external payload is required. The seven archive parts retain 6,322
members; 452 large runtime, input or environment artifacts retain explicit
identities. Checked-in source fixtures remain bundled. Hardware, exact commands,
raw observations and diagnostic resource records remain in the packet.

The [pinned Bloom admission review](../architecture/native-bloom-admission-2026-11-14.md)
declines the current unversioned provider before performance. The subsequent
[necessary text-signature experiment](native-text-signatures-2026-11-14.md) is
declined with complete evidence and accepted source restored. Pruning/read/consumer
attribution and survivor batching are next. FSST work,
required item-19 spatial execution and optional summaries, codec choices and
bounded CPU fusion retain their own gates. Item 20, RFC 0044 and CG-1 through
CG-23 remain open where recorded. This local disposition creates no PR merge,
publication, release, writer default or competitive-superiority claim.
