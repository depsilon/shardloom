<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native Zstd Workspace Acceptance

Status: locally accepted source at `ab96cd7e5d0698d90c33ea2835896a68cb5f3543`.
Hosted integration and package publication are separate. This closes the local
implementation and acceptance portion of `NATIVE-CODEC-WORKSPACES`, under
PERF-03/06 and CG-5/19/20/21; it does not close those broader gates.

## Accepted behavior

A tiny output previously fit its query grant while Zstd allocated its C decoder
outside that grant. The same native provider now allocates its actual one-shot
decoder and optional prepared dictionary through Vortex's fallible session
allocator. Insufficient credit returns the existing typed denial. Temporary
workspaces release before the retained output is returned, including on error.
The dictionary is borrowed without a second content copy.

The [contract](../architecture/native-zstd-workspaces-2026-10-07.md) and
[RFC 0044](../rfcs/0044-resident-runtime-resource-ownership.md#pinned-zstd-decoder-workspace-decision)
record the pinned Zstandard 1.5.7 static APIs, actual length/alignment checks,
exclusive Rust borrows and no-C-free rule. Native Vortex encoding, selected
decode extent, null scatter, slicing and serialization remain intact. Legacy
Zstd v0.1–v0.7 members fail explicitly, as does decoder initialization with
another linked provider version.
Modern concatenations and skippable members retain the existing known-content-size
contract. No external execution fallback is introduced.

## Source and executable identities

The final snapshot contains 938 runtime source assets, with identity
`50762def78574a0dab48f61e38a6400f5177123fd925df3c1f682a1e62404f47`.
The release CLI SHA-256 is
`29e86226fc70ed53e24c175036e4b2c7021a143387a46029ffa54e946870cb27`.
It was built from the recorded overlay on base `83d26a0e`; every accepted source
asset is also verified against the implementation commit above.

The provider remains Vortex 0.85.0, `zstd` 0.13.3, `zstd-safe` 7.2.4 and locked
`zstd-sys` 2.0.16+zstd.1.5.7. Only the direct binding edge and experimental
static-API bindings are added; existing safe-wrapper features remain unchanged.
The workspace keeps `unsafe_code = "forbid"`; the private wrapper is isolated
inside the existing excluded Apache-2.0 provider patch. The packet records the
12 inspected upstream C/wrapper source identities and contents.

## Resource and correctness proof

The old dynamic decoder fails the new tiny-grant regression as expected: an
eight-byte result succeeds even though its decoder cannot fit the grant.
The accepted implementation passes all 24 native Zstd ownership tests:

- Exact and one-byte-short grants, actual decoder/dictionary allocation requests,
  dictionary-stage denial and credit release before retained output.
- Trained dictionaries, borrowed raw dictionary sizes, nullable Unicode,
  primitive extrema and float bits, slices, scalar/append entrypoints and native
  serialization.
- Corrupt dictionaries and frames, checksums, malformed/truncated/trailing
  members, modern/skippable concatenations and every supported legacy magic's
  explicit refusal.
- Incorrect actual allocator slice lengths/alignment, denial at every nullable
  allocation stage and overlapping calls sharing one grant.

All 17 source gates pass, including workspace formatting/Clippy/tests, separate
vendored formatting/Clippy, native Vortex/CLI targets and examples, native builds
without writes, the lean build, Rust 1.96 compatibility checks, optional Python
tests, scoped dependency/license/advisory checks and whitespace validation.
Recorded totals include 3,144 default tests, 2,362 native Vortex tests with 23
ignored, 1,305 native CLI tests, 17 native example tests and 613 Python tests.
Ignored tests are not counted as executed evidence. The release build uses
Rust 1.99.0.

The same final release CLI passes 27,373 public cases over 15,820,181 rows;
202 direct cases over 131,734 rows; 48 batch checks; 19 format checks; 145
admitted semantic stages; nine golden stages; and all 129 Full43 calls. Raw
records include 54,733 public, 367 direct, 44 batch and 24 format envelopes,
including expected denials. All inspected execution reports preserve native
routes and false fallback/external-engine fields.

## Complete-operation cost screen

The frozen test-only portfolio uses 4,096 rows in 2,048-row groups, three text
profiles, Zstd/Dictionary/FSST encodings and COUNT/group/contains/not-contains
queries. Each of its 36 cells has three samples of 100 complete native calls:
10,800 calls per executable, 21,600 total. Complete values, native readback
digests, temporary-file cleanup and zero remaining owned bytes pass for both.

| Encoding | Cells | Candidate/baseline median ratio range |
| --- | ---: | ---: |
| Retained Zstd | 12 | 0.984364–1.011753 |
| Dictionary control | 12 | 0.863170–0.994148 |
| FSST control | 12 | 0.961813–1.012345 |

The predeclared rule requires a reversed-order repeat when any Zstd cell is
over 10% slower. None crosses that threshold; all samples and controls remain
in the packet. This retains a resource-accounting correction, not a speedup.
Baseline ran before candidate on macOS 27 arm64 with ten logical CPUs and
16 GiB physical RAM. OS cache and ordinary desktop activity were uncontrolled.

The measured candidate test executable is
`1dff2dad8d28f15d820b2b88dedc91d48def77dd9d4d8544f6e6d5afdd081bed`,
built from snapshot `e40c57acd623847a5ae6aef3b8625406f860e8e6e121edd7835fd09919d50239`.
Before the final CLI build, one expression in `array.rs` was split across lines
to satisfy the vendored formatter. The packet verifies that exact whitespace
transformation and equality of the other 937 assets. It preserves both source
inventories and the earlier measured bytes; it does not claim byte-identical
snapshots or silently substitute a new timing executable.

For a new reproduction, resolve the Cargo output location and apply the
[local storage/workload guards](../architecture/local-development-storage.md)
before building each frozen source inventory. The recorded release build uses
`cargo test --offline --locked --release -p shardloom-vortex --lib --features release-user-surfaces --no-run --message-format=json`.
Resolve the test executable from that compiler output and run it serially with
`--ignored --exact vortex_ingest::text_codec_portfolio::text_codec_portfolio_release_reuse_1_10_100 --test-threads=1 --nocapture`
in a fresh admitted local temporary directory. The packet's `codec_experiment.py`,
frozen protocol and comparison driver preserve the exact original sequence,
complete-output checks and scoring rule. New host paths or source changes are
a new observation and cannot inherit the original hashes or timing claim.

## Retained-input Full43 observation

Every ClickBench query ran three times in new native processes against the
unchanged 15,682,956,489-byte Vortex input containing 99,997,497 rows. All 129
complete results match the independently retained reference packet. Footer-only
COUNT also preserves its native no-read/no-decode proof for all three runs.

| Observation | Value |
| --- | ---: |
| Sum of the 43 query minima | 64.849967 seconds |
| Sum of the 43 query medians | 65.767891 seconds |
| Sum of all 129 calls | 198.004905 seconds |
| Maximum observed native process RSS | 5,339,889,664 bytes |

The requested query policy was 24 GiB with maximum parallelism 12 on the same
ten-CPU, 16 GiB host. Policy credit, physical RAM and observed RSS are different
quantities. Timings include process startup, complete output and exit; they
exclude ingest and validation. Source prehashing warms the cache, whose state
otherwise remains uncontrolled. These are regression observations, not a paired
whole-engine speedup or measured ingest-plus-query duration.

## Preserved evidence and limits

The [evidence index](evidence/native-codec-workspaces-2026-10-07.json) links the
[immutable packet](evidence/native-codec-workspaces-2026-10-07.json.xz) and its
[independent inspection](evidence/native-codec-workspaces-2026-10-07-inspection.json).
The packet SHA-256 is
`64e2cd63e1b4118cb028a92a21d41eb5db3dbd6125403eda0b1e890b44fc4c1d`;
its compressed size is 43,758,896 bytes. The finalizer reopens original complete
values and source hashes. A separate stream inspector checks the complete
decompressed hash, structural counts and raw execution claims after 169
adversarial contract checks.

The packet retains the expected failing regression, development compile/lint
and formatter attempts, the dependency-check PATH correction and the original
Full43 storage refusal. That refusal occurred before query execution. Only
completed historical logs were compacted: all 1,032 archive members were
reopened and verified before acceptance, with unchanged storage ceilings and
source data. No failed run is presented as passing.

Reproduction inputs include all final source assets, baseline/measured source
overlays, pinned provider sources, build commands/features, frozen protocols,
fixture/oracle identities, raw results and verification drivers. Binary and
large native input/output payloads are represented by checked identities rather
than embedded copies. Portable text substitutes local paths; original-byte
hashes and packet hashes are recorded separately.

Manual review checks each FFI call against its pinned C contract and Rust
borrows. This local packet does not claim sanitizer, Miri or other-platform
runtime proof. Compression/training contexts, inactive `ZstdBuffers`, other
unreviewed builders, metadata headers, C stacks, whole-process RSS, general
operator spill and resumable recovery remain outside this finite scope.
Published v0.4.0 and the public support label remain unchanged.

The [documentation receipt](evidence/native-codec-documentation-2026-10-07.json)
binds all seven local documentation checks and the generated pages to this
source. Desktop and mobile review covers the resource text, contained table
scrolling, navigation and the two Pagefind results for `workspaces`. No examples
changed. The original missing phase-classification row and a verification-driver
filename collision are retained with the successful correction. Hosted checks
remain separate.
