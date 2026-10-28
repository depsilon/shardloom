<!-- SPDX-License-Identifier: Apache-2.0 -->

# Shared key lookup candidate acceptance

The shared-key candidate adds a bounded, per-batch exact lookup directory for eligible dense
integer or native dictionary keys, membership-only DISTINCT for zero-measure grouping, and direct
current-batch addressing during join build. It retains the existing exact native route for misses and
ineligible inputs. The directory is capped at 16,384 entries, requires at least four input rows per
slot and a batch of at least 64 rows, is bounded to one sixteenth of currently available memory, and is released before retained
payload gathering. This is local acceptance evidence for the candidate, not completion of item 20
or a platform-wide performance claim.

## Frozen comparison

The candidate executed one isolated pass of the frozen 347-case cohort. The accepted baseline is
reused; it was not rerun. Both role results cover all 12,565 output rows.

| Role | Engine seconds | Observation |
| --- | ---: | --- |
| Accepted baseline | 27.374316659 | Existing complete observation |
| Shared-key candidate | 25.508976866 | One complete observation |

The candidate is 6.814197% lower in total engine time and passes the frozen benefit gate. The
comparison reports no material case or family regressions; grouped aggregation and DISTINCT are
the materially improved declared families. A single candidate observation does not establish
statistical confidence or causal attribution. Timing is the sum of `engine_execution_seconds` for
one execution of every case. Query-time input work and native operators remain in that engine
clock; harness preparation, result serialization, result verification and other process work retain
their separate diagnostic clocks.

| Declared family | Baseline engine seconds | Candidate engine seconds | Reduction |
| --- | ---: | ---: | ---: |
| Grouped aggregation, 180 cases | 13.144569828 | 11.508791828 | 12.44% |
| DISTINCT, 4 cases | 0.319262208 | 0.263734167 | 17.39% |
| Joins, 14 cases | 2.083341375 | 1.919370334 | 7.87% |
| Multiway joins, 2 cases | 1.215298791 | 1.201107542 | 1.17% |
| Composite report, 1 case | 0.302245041 | 0.305269125 | -1.00% |

The prospective gate requires at least 10% lower complete-cohort time **or** at least 10% lower
time in a declared family. The candidate meets the family alternative. A case regression requires
both 20% and 0.01 seconds; a family regression requires both 20% and 0.05 seconds. Those thresholds
were frozen before implementation and measurement. Family weights reflect this discovery corpus,
not a production workload distribution.

The baseline is tied to head `1022a366bcd2a205a241e68cbf08421fa0cb0750`, with its original inputs
and observations identified in the protocol record. Candidate source is based on
`328d3bf0a1e81a6818b3addc2e1c35d32e1777f6`; the dirty source inventory and executable digests are
bound in the candidate runtime manifest. The measured release runtime was `shardloom` SHA-256
`553fe8f4792496ca03ef1e6f7b205a6047d960a6c1976affaa6be3a8e6745290` and `_native.abi3.so` SHA-256
`cf789e24a009904c60618539432859d1c255285b378e859bd964dedd5c169b8f`. The frozen protocol records
2 GiB memory and parallelism 2. The existing Polars observations are preserved unchanged; no new
Polars observation or Full43 run was made. Their original Arrow IPC inputs and complete-API clock
do not provide a new same-input, same-clock speed ratio against this Parquet engine observation.

The five unchanged Parquet files contain 1,000,000 fact, 100,000 entity, 10,000 product, 200,000
relationship and 731 calendar rows at balanced seed 20261026. The run uses Python SQL on macOS
27 arm64 with Python 3.13.13. Inputs were fully hashed before the pass and checked afterward;
the OS cache was uncontrolled and no prepared artifacts or cache manipulation were used. The
frozen manifest records the full inputs, query catalog, execution order, independent references,
runtime and source identities. Host/load, owned-process memory and supervised cleanup observations
are preserved in the packet.

## Correctness and validation

Fresh held-out correctness passed 160 observations across two fresh seeds and shuffled/clustered
layouts, including null-heavy and skewed inputs. Complete outputs contain 2,345,274 rows and full
input read-back covers 268,976 rows. Independent expected values cover 1,172,637 rows. The held-out
runner did not invoke a comparison engine. Native activation assertions cover integer widths and
extremes, encoded numeric inputs, nulls, changing and duplicate dictionary values, sparse and
high-cardinality decline paths, low-memory reservation and cancellation, first-occurrence DISTINCT
ordering, and cross-batch duplicate join order.

| Gate | Result |
| --- | --- |
| Native Vortex suite | 3,442 passed; 32 ignored |
| Workspace suite | 3,274 passed; none ignored |
| Native CLI suite | 1,032 passed; none ignored |
| Public interface cases | 25 cases |
| Installed native Python wheel | 201 integration tests passed; zero failures, errors, or skips |
| Evidence corruption controls | 23 passed with Python assertions disabled |

The installed wheel proof is for macOS 27 arm64 with Python 3.13.13. Its wheel is tagged
`cp310-abi3-macosx_27_0_arm64`; Linux audit and platform load verification are not established by
that proof. The five public surfaces each bind the candidate runtime; the public-interface receipt
records five interfaces and five declarations per interface. Native test totals include the stated
32 ignored opt-in measurement/fixture tests.

## Evidence, replay, and limits

The [portable acceptance](evidence/native-shared-key-lookup-2026-10-28/acceptance.json),
[manifest](evidence/native-shared-key-lookup-2026-10-28/manifest.json) and
[complete packet](evidence/native-shared-key-lookup-2026-10-28/packet.tar.xz) preserve the frozen
protocol, complete 347-query and fresh correctness results, references, source snapshots,
runtime identities, command receipts, package proof and raw failures. The baseline archive is
stored in verified chunks under the existing 16-MiB member limit; the 256-MiB expanded-file and
320-MiB archive limits are unchanged. No archived source is executed during replay.

The first packaging attempt failed at the per-file limit before chunking was added. Two interrupted
corruption-test attempts are also preserved: semantic mismatches use `ResultMismatch`, and the
width-change control now uses a representable signed width so it reaches schema comparison.
The corrected 23-test suite passes. A bounded source-only copy also replays every saved result
with isolated Python and assertions disabled, without native libraries, datasets or PyArrow.
The [post-validation record](evidence/native-shared-key-lookup-2026-10-28/verification/post-validation.json)
preserves those attempts, the exact passed commands, final packet/source binding and primary review.
Documentation and CI-contract checks pass in their normal CI mode; the architecture tracker still
reports the 76 unchecked phase-plan obligations, so this is not a release-readiness completion.

The [verified retirement](evidence/native-shared-key-lookup-2026-10-28/cleanup.json) removes 23,751
completed regular files and six environment links after repeated identities and closed-handle
checks. Temporary install/build environments, source-only copies, staged packets and superseded
runtime/wheel payloads are retired. The latest tested runtime and installed-tested wheel, original
five Parquet files and 347 independent reference files remain. Complete observations, all failures,
507 raw process identifiers and 24 historical helper/control source versions are preserved.
Full replay passes after retirement; no payload backup or storage-limit change occurs.

Replay complete results without the native runtime, PyArrow or historical absolute paths:

```sh
python3 -B scripts/verify_shared_key_lookup_packet.py docs/benchmarks/evidence/native-shared-key-lookup-2026-10-28
python3 -B -m unittest discover -s scripts -p test_shared_key_lookup_packet.py -v
```

Hashes establish internal consistency, not signed provenance. Typed MIN/MAX, value gathering, and
expression candidates remain pending. Item 20 remains open; CG-1 through CG-23 are unchanged. No
hosted acceptance or package publication is claimed by this local record.
