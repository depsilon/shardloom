<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native Builder Resource Acceptance

Status: local engine acceptance and independent packet inspection passed for
runtime `53cd1582a5975ac4e206a21e45415c919c5b6b4f`. Local documentation and browser
checks pass; hosted integration remains pending. This is the finite `NATIVE-BUILDER-RESOURCES`
unit under PERF-03/06/07 and CG-5/19/20/21. Published v0.4.0 and the broader gate
status are unchanged.

## Accepted behavior

Native Chunked execution previously allocated its primitive, Boolean and decimal
builder output outside the shared query grant. The existing provider now reserves
the value buffer, possible nullable bitmap and temporary empty buffer created by
builder finalization before constructing the builder or executing a child.
The same finalization overlap is also covered for the accepted string builder.
Value and validity buffers keep independent credits through clones and slices;
typed denial and failed decoding release partially built output.

The [contract](../architecture/native-builder-resources-2026-10-07.md) preserves
Vortex 0.85.0's builder and per-encoding append strategies, including direct
BitPacked append. It does not canonicalize every child in advance. The existing
vendored Zstd provider also gains primitive append through its admitted native
decoder, followed by native primitive append. This repairs an existing primitive
Zstd concatenation gap without adding a codec, allocator or external engine.

The Vortex-first decision is `use_vortex_native_provider`. The shared session
marker, PulseWeave pool and retained-buffer hooks operate below all public
front doors. Native input/output, exact dtype/scale/nullability, source order and
no-fallback certificates remain intact. No new dependency or unsafe code is added.

## Source and executable identities

The accepted snapshot contains 941 runtime source assets, with identity
`a7308b726410569306bae14bf60ddd07f57bead73d44d423f5274b5b17c10c5f`.
It was frozen before building from the recorded overlay on base
`094c50b83235cd0594e1858acfcc7aa5a3f4760a`; HEAD alone does not identify the
measured implementation. All 941 source assets also match the implementation
commit above, as verified by the evidence-staging driver.

The release CLI SHA-256 is
`382453ef8ca6a439382bc852304a16aff2a4c1d5cd5087557c9d47376ca4ef8e`.
The cost-screen test executable SHA-256 is
`209ac12638a780bf0803c00d3610e64bf381161ee8a553657ad2e2a00dc71079`.
Both are built from the same source snapshot using Rust 1.99.0. Thirteen pinned
upstream source files are checked against cached registry-archive bytes and
their Cargo.lock archive checksums. No provider version or feature is changed.

## Resource and correctness proof

The two pre-fix regressions reproduce fixed-width admission bypass and uncharged
string finalization overlap. Fourteen new ownership tests then cover:

- All primitive widths, nullable Boolean bit offsets, decimal physical widths
  and scales, exact encoded values, nested chunks, empty and all-null arrays.
- Independent value/validity lifetime, retained clones/slices, shared-grant
  overlap, finalization capacity and release after the final owner drops.
- Denial before child append, a corrupt Zstd child, and cleanup after a later
  decoder error. Native primitive Zstd output is compared with exact values.

The existing retained-buffer pointer test also passes. Native buffer ownership
wrapping is source-checked to avoid a new payload copy; the new result tests
verify post-finish slice aliases. This is not allocator-wide copy instrumentation.
The pre-fix reproductions retain their base and changed-file overlays, rather
than claiming a complete source freeze retroactively.

All 17 source gates pass, including workspace formatting/Clippy/tests, separate
vendored formatting/Clippy, native Vortex/CLI targets and examples, native builds
without writes, the lean build, Rust 1.96 compatibility checks, optional Python
tests, dependency/license/advisory checks and whitespace validation. Totals
include 3,144 default tests, 2,376 native Vortex tests with 24 ignored, 1,305
native CLI tests, 17 native example tests and 613 Python tests. Ignored tests
are not counted as executed evidence; the lifecycle test is run separately.

The same final release CLI passes 27,373 public cases over 15,820,181 rows;
202 direct cases over 131,734 rows; 48 batch checks; 19 format checks; 145
admitted semantic stages; nine golden stages; and all 129 Full43 calls.
Retained raw records include 54,733 public, 367 direct, 44 batch and 24 format
envelopes, including expected denials. Checked execution reports preserve native
routes and false fallback/external-engine fields.

## Native lifecycle cost screen

The frozen screen has ten input shapes, three row counts (32, 4,096, 65,536) and
two chunk counts (2, 16): 60 cells. Shapes cover canonical/nullable Int64,
nullable Float64 and Boolean, decimal precisions 38 and 76, BitPacked Int32,
nullable primitive Zstd, constants and nullable UTF-8. Each cell has nine pairs
after eight warm-ups per variant, with 1,024/128/16 complete calls per timed batch.
The fresh reversed-order confirmation yields 2,160 timed samples and 840,960
complete timed calls in total. Complete values are checked before and after each
timed batch; each timed call checks dtype, row count and output destruction.
All value checks and final zero-credit assertions pass.

Both variants run in the same executable, with and without installed
`ProviderMemory`. Both include the primitive-Zstd append repair. Installed
admission also includes existing child-codec/string owners, so this comparison
does not isolate the marginal cost of the new builder reservation.

The predeclared rule repeats any cell over 10% slower and requires revision if
the same cell exceeds both 10% and five microseconds per call in both orders.
Eight cells trigger the relative screen; five do so in confirmation. One cell
crosses both thresholds initially, but none crosses both in confirmation.
The correction is retained under that rule. The repeated UTF-8 costs remain
visible:

| UTF-8 rows / chunks | Forward ratio / added microseconds | Reverse ratio / added microseconds |
| --- | ---: | ---: |
| 32 / 2 | 1.4291 / 0.6000 | 1.3788 / 0.4387 |
| 32 / 16 | 1.5430 / 2.6140 | 1.5240 / 2.1007 |
| 4,096 / 2 | 1.2244 / 0.9909 | 1.2054 / 0.7279 |
| 4,096 / 16 | 1.4986 / 4.7445 | 1.4727 / 3.6357 |
| 65,536 / 16 | 1.1342 / 10.7213 | 1.1326 / 4.9297 |

The last confirmation is only 0.0703 microseconds below the absolute threshold;
the result does not establish negligible overhead. Three nullable numeric cells
exceed the relative threshold only initially, adding 0.0705–0.1240 microseconds.
All samples, negative observations and frozen thresholds remain in the packet.
There is no speedup claim or assumption that unexplained differences are noise.
OS cache and ordinary desktop activity were uncontrolled on the macOS 27 arm64
host with ten logical CPUs and 16 GiB physical RAM.

For a new reproduction, resolve Cargo output and apply the
[local storage/workload guards](../architecture/local-development-storage.md).
Build the recorded source with
`cargo test --offline --locked --release -p shardloom-vortex --features release-user-surfaces --lib --no-run --message-format=json-render-diagnostics`.
Resolve the test executable from compiler output. The packet's
`builder_experiment.py`, cost contract and analysis driver record the exact
test name, environment, input/reference hashes, variant order and scoring rule.
Use fresh admitted local paths; changed inputs, source or host observations do
not inherit the original timing claim.

## Retained-input Full43 observation

Each of the 43 ClickBench queries ran three times in new native processes
against the unchanged 15,682,956,489-byte Vortex input with 99,997,497 rows.
Every complete result matches the independently retained reference packet.
All three footer-only COUNT calls preserve native no-read/no-decode proof.

| Observation | Value |
| --- | ---: |
| Sum of the 43 query minima | 66.329675 seconds |
| Sum of the 43 query medians | 67.965349 seconds |
| Sum of all 129 calls | 204.881714 seconds |
| Maximum observed native process RSS | 5,690,572,800 bytes |

The query policy is 24 GiB and maximum parallelism 12 on the ten-CPU, 16 GiB
host. Those are distinct from physical capacity and observed RSS. Timing
includes process startup, complete output and exit; it excludes ingest and
validation. Source prehashing warms the cache; other cache state is uncontrolled.
This observation is slower than the preceding codec observation, whose minima
sum is 64.849967 seconds. They are separate cohorts, not a controlled paired
comparison or evidence attributing the difference to this change.

## Evidence and limits

The [evidence index](evidence/native-builder-resources-2026-10-07.json) links the
[immutable packet](evidence/native-builder-resources-2026-10-07.json.xz) and
[independent inspection](evidence/native-builder-resources-2026-10-07-inspection.json).
The packet SHA-256 is
`80025eb8866a04c63a5cf4f60da6c454f04a9590a7181ee0ed3fe14576b7871d`;
its compressed size is 43,420,312 bytes. The finalizer reopens complete source,
value and resource evidence. The separate stream inspector checks the full
decompressed hash, counts and raw claims after 180 adversarial contract cases.

The separate [documentation receipt](evidence/native-builder-documentation-2026-10-07.json)
binds the seven final documentation/site checks, final source, browser captures
and archived logs. Desktop and mobile review covers resource/limitation wording,
light/dark text readability, contained table overflow and both builder search
destinations. The initial omitted limitations paragraph and stale review date
were corrected before the final pass; preliminary captures remain preserved.

The original storage admission refusal stopped before queries. Three complete
historical cohorts were compacted with their summaries and original manifests
preserved. All 1,548 original members were reopened and verified, recovering
3,452,928 accounted bytes without raising storage ceilings. The first two-cohort
compaction still failed admission; that failure and the final continuation are
retained. Failed/incomplete cohorts and resident input were not removed.

The packet also preserves development compilation/lint attempts, expected
pre-fix failures and the engine-check driver's receipt-name collision. No failed
attempt is relabeled as passing. Large input/output payloads and executables
are represented by checked identities and replay sources, not embedded copies.
Original-byte and portable-packet hashes have distinct meanings.

Array/builder headers, VarBinView source-buffer handles, general child-decoder
and selection scratch, source storage, allocator bookkeeping, whole-process RSS,
general operator spill and resumable recovery remain outside this finite claim.
The accepted native append strategy does not certify every allocation made by
its children. Published packages and broad production support remain unchanged.
