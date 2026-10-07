<!-- SPDX-License-Identifier: Apache-2.0 -->

# Completion-aware Native Input Acceptance

Status: local engine acceptance and independent packet inspection passed for
runtime `f14460ef262b076842d375fa05d33507c4a97027`. Documentation, actual native
examples and browser checks pass; hosted integration remains pending. This is the finite
`NATIVE-INPUT-COMPLETION` unit under PERF-03/07/11/12 and CG-5/19/20/21.
Published v0.4.0 and broader PERF/CG status are unchanged.

## Accepted execution contract

`from_batches(..., streaming=True)` moves one declared finite source through the
existing native scan/filter/project execution without first retaining the whole
input. The default `streaming=False` keeps resident intake and its existing
operator coverage. The complete streaming plan binds before producer demand;
unsupported blocking operators, repeated sources, explicit limits, dynamic
binding, multiple destinations and compatibility writers fail before consumption.

The [contract](../architecture/native-input-completion-2026-10-07.md) admits
incremental results, bounded small collection and one native Vortex destination.
It retains 1–128 nullable Int64/finite Float64/Boolean/UTF8 input fields, at most
2,048 rows per batch and 4,096 batches per source, the 8-MiB input payload frame,
the separate 16-MiB wire frame, and 32-MiB native logical intake per batch.
Existing result/type limits and output reservations remain separate.

Each source batch carries a shared ownership witness through its native buffers,
clones and slices. Output is compacted into separately credited native storage
before delivery. The input witness must expire before the next demand; a retained
input alias fails explicitly. This is an accounted copy boundary, not zero-copy
input or output. The schema owner remains constant plan metadata.

Success requires observed end-of-input, completed native consumers, source
validation, cancellation checks and all result acknowledgements. Empty or
fully filtered batches do not imply completion. Delivered rows remain provisional;
a late error cannot produce a successful prefix or publish an incomplete Vortex
file. Native sink metadata remains reserved as it grows.

The Vortex-first decision is `wrap_vortex_concept`: reuse pinned Vortex 0.85.0
arrays/dtypes, the existing session allocator, bound expressions and native sink,
with ShardLoom completion/admission evidence around their delivery. No dependency,
query-engine integration, producer replay or second query invocation is added.
SQL and Python declarations share the same native plan and false fallback fields.

## Frozen source and proof

The release CLI SHA-256 is
`81f3514ea4ca59188d8e151b94bdcb628f435679faea4af332c30d3ecd989e1d`.
All 947 runtime source assets have snapshot identity
`dd8643bb76f7cf247363279372635d643939410f77191d15195a360aaeff102c`.
The build receipt, compiler artifact, executable and source hashes are frozen
before acceptance. The staging check binds every asset to the implementation
commit above. Input fixtures use separately frozen generators; all queries use
this release CLI.

Ten native tests cover input-buffer and slice lifetimes, exact schema/session
ownership, zero/empty/all-filtered input, the 4,096/4,097 batch boundary, late
failure and writer cleanup. A CLI regression preserves the origin of its
synthetic unlimited SQL token: ordinary streaming SQL remains admitted while an
explicit user LIMIT, including the maximum integer, stays unsupported. Missing
native providers are refused before fanout can create output parents.

The 40-case actual public-peer suite checks complete typed and static nested
results, NULL/Unicode/signed boundaries, composed and SQL declarations, retained
Python output, one-shot behavior, zero producer demand on denied plans, and
inert discovery. It includes six late producer/shape/arithmetic failures across
delivery and writing, early cancellation, a failing consumer, and ten malformed
or interrupted wire sequences. Every peer exits and drains without a forced stop;
failure cases withhold success and remove owned staging. The packet reopens 36
raw envelopes, all ten wire traces and independently rebuilt complete values.

All 15 source gates pass: workspace formatting/Clippy/tests, native Vortex/CLI
tests and Clippy, examples, native builds without writes, lean builds, Rust 1.96
compatibility, optional Python tests, strict dependency audit and whitespace
validation. Executed totals are 3,145 default tests, 2,386 native Vortex tests,
1,307 native CLI tests, 17 example tests and 615 Python tests. The 24 ignored
native tests are not counted as executed evidence.

The unchanged regression declarations pass 27,373 public cases over 15,820,181
complete rows, 202 direct cases over 131,734 rows, 48 resident batch checks,
19 format checks, 145 admitted semantic stages and nine golden stages. The packet
reopens 54,733 public, 367 direct, 44 resident-batch and 24 format envelopes,
including expected denials. Their no-fallback and native-route evidence remains
explicit. The two modified Python adapters preserve resident mode by default;
all 221 existing declaration sources and the independent literal oracles match
the previously accepted scope.

## Input larger than the grant

The frozen capability gate supplies 1,152 batches of 1,024 rows, each with an
Int64 index and a 4,096-byte UTF8 value. This is 4.5 GiB of string payload,
1,179,648 rows and 4,851,019,008 cumulative native logical bytes. Each native
batch contains 4,210,954 logical bytes. A pure filter selects one row per batch;
all 1,152 complete output rows are compared with an independent literal oracle.

| Input / destination | Native grant | Complete operation | First provisional row | Peak tracked reservation | Outcome |
| --- | ---: | ---: | ---: | ---: | --- |
| Streaming / batches | 1 GiB | 11.559034 s | 0.016833 s | 71,534,488 bytes | Complete; at most one native input batch |
| Resident / batches | 1 GiB | 2.402793 s to denial | None | Not reported on failure | Expected memory denial after 239 batches; producer closed |
| Streaming / batches | 6 GiB | 12.917620 s | 0.016665 s | 71,534,488 bytes | Complete; at most one native input batch |
| Resident / batches | 6 GiB | 12.588353 s | 12.309061 s | 4,922,459,784 bytes | Complete resident control |
| Streaming / Vortex | 1 GiB | 13.309278 s | Not applicable | 81,103,316 bytes | Complete; published artifact fully reopened |

These are five single observations, not a paired speedup experiment. Their
timing includes producer generation, typed intake, native execution, complete
delivery and child exit; it excludes oracle generation and output readback.
Both ample-memory controls remain visible, including the slower streaming
observation. Time to first provisional output is separate from completion.

All successful cases observe producer end and closure. The denied resident case
closes its producer without reporting end or returning output. The Vortex result
has 5,249,964 bytes and SHA-256
`c8dc8d744b1f93c67dd037a703025b653c03b2c665e4ca95539d6790e487c8a7`;
its complete reopened values match the same oracle. Logical input bytes and
tracked native reservations are different measurements; neither bounds Python
objects, provider exclusions, allocator bookkeeping or process RSS.

## Retained-input Full43 observation

All 43 queries run three times in new native processes against the unchanged
15,682,956,489-byte Vortex input with 99,997,497 rows. All 129 complete results
match the retained independent reference. The three footer-only COUNT calls
retain native no-read/no-decode/no-row-materialization certificates.

| Observation | Value |
| --- | ---: |
| Sum of query minima | 65.632277 seconds |
| Sum of query medians | 66.799548 seconds |
| Sum of all 129 calls | 203.596933 seconds |
| Maximum observed native process RSS | 4,938,645,504 bytes |

The policy permits 24 GiB and maximum parallelism 12 on the macOS arm64 host with
ten logical CPUs and 16 GiB physical RAM. Policy, physical capacity and observed
RSS are separate. Timing includes process startup, complete output and exit;
it excludes ingest and validation. Source prehashing warms the file cache and
other cache/desktop activity is uncontrolled. This is regression evidence, not
a controlled comparison with an earlier cohort or a ClickBench speedup claim.

## Evidence and remaining limits

All seven documentation checks pass: user-surface references, public status,
use-case backlinks, site type checking/build, website readiness and whitespace.
The exact README and Field Guide streaming examples and historical resident
example execute through the frozen native CLI, produce complete expected rows
and successful final reports, and drain their children. Desktop dark and mobile
light layouts, contained code scrolling, mobile navigation, resource links and
Pagefind search pass. Search finds the streaming guide and opens its correct
section; the browser reports no warnings/errors. The temporary server is drained
and the browser viewport restored. The
[documentation receipt](evidence/native-input-documentation-2026-10-07.json)
binds the source, generated pages, logs, executed examples and screenshots in
its separately reopened archive. All 947 runtime assets remain unchanged.

The [index](evidence/native-input-completion-2026-10-07.json) links the
[immutable packet](evidence/native-input-completion-2026-10-07.json.xz) and
[independent inspection](evidence/native-input-completion-2026-10-07-inspection.json).
The packet has 44,450,908 compressed bytes and SHA-256
`bc6560f39385f72a8f58688e136983b9243bf22b6d724ef5d31e82af763873d2`.
Its original-byte and portable-text identities are recorded separately.
The finalizer reopens complete values, source assets, ownership tests, protocol
traces and all pressure reports. A separate stream inspector validates the full
decompressed hash, structural counts and raw claims after 168 mutation checks.

The original Full43 storage refusal happened before queries. Two completed
historical cohorts were compacted with every member reopened and verified,
recovering 6,725,632 accounted log bytes. The packet reopens all 1,032 archived
members. Summaries, failures, resident inputs and storage ceilings remain intact.
Development failures, diagnostic runs, pilot cases and the engine-driver receipt
name collision are retained; a failed attempt is never relabeled as passing.

Streaming remains opt-in and finite. Blocking/repeated-source plans, input spill,
compatibility writes from streamed input, general operator spill, comprehensive
allocation accounting and resumable recovery require separate work. Native
output/footer retention can still exhaust its grant. Rematerialization requires
a measured retained owner and complete dependency/reconstruction costs; transient
predicate masks are not such an owner. Other state/structure and conditional-work
candidates retain their own acceptance gates. No package publication, whole-engine
speedup, process-memory guarantee or broader PERF/CG completion is claimed.
