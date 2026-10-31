# Retain correlated membership and compact replay capture

The reduced **B6/B9 composition is retained**. The other eight experimental
optimizations are removed; the shared Parquet metadata-admission correctness
repair remains. Both unchanged complete cohorts pass the prospective binary
gain rule:

| Cohort | Reused reference engine s | Reduced candidate engine s | Change s | Complete rows |
| --- | ---: | ---: | ---: | ---: |
| 80 supplemental workflows | 40.838371451 | 31.669017456 | -9.169353995 | 871,209 |
| 347 analytical queries | 11.979219492 | 11.978234339 | -0.000985153 | 12,565 |

The analytical result is effectively flat (0.00822% lower); the supplemental
total is 22.453% lower. The maintainer's rule has no minimum percentage gain.
Each candidate case ran once; both saved reference observations were reused
without rerunning them. These individual observations establish neither
statistical confidence nor isolated causality. The analytical comparison
baseline advances to 11.978234339 engine seconds. This does not establish the
under-ten-second goal, other-workload superiority, or completion of CG-1
through CG-23.

The original ten-owner candidate **remains rejected**: its 347-query total was
12.387645581 engine seconds against 11.979219492, despite improving the
80-workflow total to 27.771706382 against 40.838371451. Its results, failed
attempts, gates and decision are preserved. The selective retention instruction
authorized a separately declared reduced composition.

## Retained behavior and exact reduction

The [prospective contracts](../architecture/native-performance-batch-2026-10-31.md)
define both the original batch and the subsequent reduction.

- **B6, pure correlated membership:** a statically bound equality-correlated
  EXISTS/IN relation can use the existing native membership owner. Admission
  requires proved infallible integer/Boolean resident values and a restricted
  source/projection/equality shape. Initialization occurs at the first active
  outer row. Guard-inactive and empty-outer queries retain their laziness;
  callbacks, effects, fallible payloads and other unsupported shapes retain
  their existing singleton execution.
- **B9, tiny-fragment capture/replay:** the replay recorder compacts tiny flat
  fixed-width fragments directly into bounded owned blocks. It validates each
  incoming batch and releases source-backed owners before producer return.
  Suitable large blocks and unsupported schemas retain the ordinary path.
  Memory denial, spill, cancellation, late producer failure and last-output-alias
  ownership remain explicit.
- **Shared metadata correction:** the pinned Parquet parser admits row-group
  metadata through the existing query pool, including transient replacement
  overlap. Statistics, schema and pruning behavior remain intact. This avoids
  unconditional whole-footer admission under the original pressured grants;
  it is reservation accounting, not a custom allocator or process-RSS bound.

The exact reduction covers **2,662 production inputs**. Twelve B6/B9 files
remain byte-identical to the full candidate. Two shared files retain only the
required primitive-construction visibility and three test observation fields.
Twenty-eight files are restored to the corrected reference; eight new
experimental files are removed; nine metadata-repair files are unchanged.
Every other production input equals the independently verified corrected
reference. B1–B5, B7–B8 and B10 are dropped, not queued for separate retries.

Execution remains ShardLoom-native with Vortex-native state/input/output
boundaries, explicit compatibility inputs, the same memory owner and no external
execution fallback. No dependency or public-interface expansion is added.
[Item 23](../architecture/phased-execution-plan.md#engine-wide-adaptive-execution)
remains immediately after universal I/O; the metadata repair does not complete
engine-wide adaptive execution.

## Complete supplemental results

All ten original workload families remain in the unchanged 80-case grid, with
eight cases per family and 871,209 complete matching rows. Positive change is
slower. Family labels describe workloads, not isolated owner attribution.

| Family | Reference engine s | Reduced candidate engine s | Change s |
| --- | ---: | ---: | ---: |
| B1 — source prefixes | 0.111053959 | 0.110024250 | -0.001029709 |
| B2 — repeated input preparation | 0.038647791 | 0.037422372 | -0.001225419 |
| B3 — encoded reductions | 0.013099207 | 0.012939917 | -0.000159290 |
| B4 — text construction | 0.096280083 | 0.094333416 | -0.001946667 |
| B5 — residual joins | 0.121871541 | 0.120648041 | -0.001223500 |
| B6 — correlated membership | 0.861917833 | 0.007813959 | -0.854103874 |
| B7 — spill/merge workflows | 5.426739667 | 5.306060709 | -0.120678958 |
| B8 — stored lookup | 15.459978040 | 15.171196499 | -0.288781541 |
| B9 — fragment capture/replay | 18.602654124 | 10.701184418 | -7.901469706 |
| B10 — quantiles | 0.106129206 | 0.107393875 | +0.001264669 |
| **Complete cohort** | **40.838371451** | **31.669017456** | **-9.169353995** |

The original case order, SQL, fixtures, result contracts, per-case memory grants
and deadlines remain unchanged, including pressured workflows. Activation
tests are separate from the measured release products.

## Complete analytical results

The same 347 cases, five Parquet files, independent references, 2 GiB grant and
parallelism two are preserved. All 12,565 result rows match. No case or family
crosses the previously declared material-regression flags. Slower observations
remain in the complete total:

| Family | Cases | Reference engine s | Reduced candidate engine s | Change s |
| --- | ---: | ---: | ---: | ---: |
| Projection | 26 | 1.024053996 | 1.023835874 | -0.000218122 |
| Predicates | 16 | 0.403710000 | 0.404847627 | +0.001137627 |
| Nullable | 8 | 0.126778918 | 0.128221875 | +0.001442957 |
| Numeric | 4 | 0.134438208 | 0.134951167 | +0.000512959 |
| Strings | 5 | 0.509474750 | 0.510704165 | +0.001229415 |
| Temporal | 6 | 0.384818583 | 0.384819833 | +0.000001250 |
| Global aggregation | 67 | 1.322176042 | 1.320771124 | -0.001404918 |
| DISTINCT | 4 | 0.102166251 | 0.103577416 | +0.001411165 |
| Ordering | 10 | 0.347847373 | 0.349803752 | +0.001956379 |
| Joins | 14 | 1.152746624 | 1.145411794 | -0.007334830 |
| Sets | 4 | 0.004319583 | 0.004288042 | -0.000031541 |
| Grouped aggregation | 180 | 5.354105415 | 5.366830920 | +0.012725505 |
| Multiway joins | 2 | 0.914848333 | 0.904379000 | -0.010469333 |
| Composite | 1 | 0.197735416 | 0.195791750 | -0.001943666 |
| **Complete cohort** | **347** | **11.979219492** | **11.978234339** | **-0.000985153** |

The five largest individual slowdowns are `projection_limit_string_31`
(+0.003299626 s),
`aggregate_metric_group_key_category_mixed_coalesce_all` (+0.002257208 s),
`topk_11_offset_0` (+0.001357834 s),
`aggregate_optional_value_category_sum_arithmetic_all` (+0.001096459 s), and
`numeric_sub` (+0.001078584 s). Complete case records remain in the packet.
Neither family movements nor individual movements establish their causes.

## Correctness, provenance and replay

Formatting and strict native/default workspace Clippy pass. The reduced source
passes **6,603 native workspace tests** across 119 binaries (36 existing ignored
tests), and **3,290 default workspace tests** across 107 binaries (none ignored).
Sixteen required native controls bind correlation, replay, metadata and original
spill/recovery behavior to the tested source; focused correlation/lifecycle
checks also pass. Public proof includes **36 controls and 14,400 rows**, plus
**480 interface/format comparisons and 2,338,650 rows**. All **286 installed-package
tests** pass without skips, failures or errors. Both release products contain
no debug observation owner.

The runtime is freshly compiled from `73855d3dd61144d14c4259c3c53e053e62038ff0`
plus its exact source overlay, with `release-user-surfaces`. Both products
differ from the corrected reference and original full batch:

- CLI SHA-256: `651931164248233cb72b15d573e5e2dd4afac680b379a5f18e9199c870302261`.
- Native library SHA-256: `20bd685f5290cfb918023f962c932cfe51808cc777c1d458617bcd633c7715df`.

The [portable packet](evidence/native-performance-batch-2026-10-31/manifest.json)
and [acceptance](evidence/native-performance-batch-2026-10-31/acceptance.json)
retain source snapshots, exact reduction, original/revised protocols, every
observation, complete reference/result values, validation and failures. The
top-level acceptance preserves the original rejection; `retained_subset`
records the new composition and both passing gates. Structural checks prove
the worker, observer and replay functions unchanged: twelve functions for the
80-case driver and eight for the 347-case driver.

Preserved earlier failures include metadata denials and Python list/tuple
binding errors in the initial supplemental attempt; an interrupted reference
attempt caused by mutable bytecode files in the frozen inventory; and rejection
of an accidentally reused reference binary before candidate queries. The
corrected reference contains only the shared metadata repair and declared
binding/inventory corrections. No memory ceiling was raised, result reference
relaxed, or completed observation replaced. Original rich merge/fault failures
and validation history remain recorded although B7 is removed.

Queries ran sequentially with other task work idle, using unchanged storage,
concurrency, deadline, complete-result and process-cleanup guards. Full hashing
and evidence inventories ran outside query execution. The host is macOS ARM64;
operating-system cache state is uncontrolled. No Full43, competitor run, release
or package publication occurs.

Replay saved evidence without native query execution:

```bash
python -B scripts/verify_native_performance_batch_packet.py docs/benchmarks/evidence/native-performance-batch-2026-10-31
python -B -m unittest discover -s scripts -p 'test_native_performance_batch*_packet.py' -v
```

Both commands are wired into CI. All **28 corruption controls** pass. Isolated
source-only replay from 366 copied files also passes while explicitly rejecting
all **18,126 original paths**, including 564 external payload identities, and
denying native imports, subprocesses and network access. The
[verification record](evidence/native-performance-batch-verification-2026-10-31/verification.json)
preserves the first corruption-test failure: deliberately removing a harness
source caused an unexpected missing-key exception. A one-line checker correction
makes that input receive the intended deterministic rejection; both the failed
and passing attempts remain recorded. Runtime source, measurements and the main
packet are unchanged.

The [verified retirement](evidence/native-performance-batch-retirement-2026-10-31/cleanup.json)
removes 58,542 generated regular files and 15 environment links after repeated
identity and closed-handle checks. All 8,308 retired raw result/log files remain
byte-exact in the packet. Complete replay passes after removal. The reduced
runtime and installed-tested wheel, current inputs, compact observations and
failed attempts remain. The prior ordered-input runtime/wheel stays available
through hosted integration; no compiler cache, source worktree, unrelated user
files or storage ceilings change. The implementing pull request carries hosted
check, review and merge status; no package publication is implied.
