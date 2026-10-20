<!-- SPDX-License-Identifier: Apache-2.0 -->

# Finite original-text structural representation — October 20, 2026

Status: the frozen OnPair structural profile is **rejected**. All exactness,
activation and lifecycle checks pass, but the sampled complete text files are
46.23% larger and every regional/cohort original-write aggregate fails its
prospective cost control. Thirteen individual write controls also fail in both
cohorts. The experimental code and dependency edges are removed. This completes
the finite row-12 investigation, without closing the broader
`PERF-09.structural_payload` owner or ruling out every structural representation.

The [prospective design](native-structural-payload-design-2026-10-20.md) and
[protocol](../benchmarks/evidence/structural-payload-protocol-808.json) remain
unchanged. The [conditional sequence](native-conditional-investigations-2026-10-19.md)
continues with changed-source reuse. Eligible pre-writer idle attribution remains
a separate prerequisite to revisit before the 37 required substeps resume.
RFC 0044 and all CG-1 through CG-23 remain in force.

## Mechanism, inputs and measurement boundary

Classification: `use_vortex_native_provider`. The test-only text leaf uses pinned
`vortex-onpair` 0.85.0 and its `onpair` 0.2.0 default dictionary configuration.
The provider learns adjacent byte substrings and persists a native dictionary,
token codes, offsets, decoded lengths and validity. Four integer children use
the current edition-filtered, dictionary-disabled numeric compressor. No new
wire format, URL parsing, JSON normalization or external execution engine is used.
Ineligible fields use the existing exact Zstd leaf.

Before candidate implementation, a descriptive intake checks all 28 original
text fields in the first 32,768 rows of source row groups 0, 113 and 225. It
records complete ordered byte/validity hashes, lengths and repetition counts.
The fixed admission rule uses payload size, nonempty count, mean byte length
and repeated eight-byte prefixes. It consults no column names. Eleven of the
84 field/region combinations activate OnPair, exactly as frozen from the intake.
All 28 fields remain in every workflow and every control.

The original Parquet has 99,997,497 rows, 105 fields, 226 row groups and
14,779,976,446 bytes. Its complete pre/post SHA-256 remains
`a390f6cb782f6aaef278c72fc1dd86c4f30bc843ebab3c159e9bd4d45ddb079f`.
The fixed prefixes are finite observations, not representative sampling of
the full dataset. Both profiles share one freshly compiled, source-bound release
executable. Source prehashing and prior intake warm source data; a fresh process
does not establish a cold filesystem cache.

Six preflight workflows precede two complete cohorts of 18 measured workflows
each. Each cohort has three regions, three paired repeats and both profiles,
with the predeclared symmetric order reversal. Every field is written as a
one-field native table through the actual current table, zone, statistics,
chunk, digest and publication strategies. Complete writes include owned input
conversion, provider setup/admission, encoding, statistics, synchronization and
owner release. The scoring barrier requires all 36 measured workflows and the
final complete source hash before any comparison.

The producer exits; the guard checks and moves each file; a fresh process
independently reopens the original Parquet and all moved native files. It checks
every ordered byte and null, exact logical dtype/nullability, schema, physical
encoding tree, statistics and file/segment bounds. Null counts and bytewise
minimum/maximum values use an independent Arrow/Rust oracle, with conservative
checks for truncated native bounds. The ordinary writer then reopens and scans
each encoded file inside its measured rewrite. A third fresh process checks
the rewritten and moved output against the original source again.

Each original file also has three repeated complete reads and three calls to
each public native equality, inequality, contains and negated-contains count
family. Independent source-derived oracles validate every result. Those public
primitive reports do not expose an engine clock; the field stays unavailable.
This screen makes no Full43 engine-time or full-ingestion claim. Raw diagnostic
costs, I/O/materialization observations and host samples retain their distinct
definitions in the packet.

## Decision evidence

All 42 workflows pass, covering 2,352 original and rewritten native files and
126 fresh native processes. Original and rewritten ordered value checks each
cover 38,535,168 values, with further complete repeat-read checks. All expected
activations, ordinary consumers and no-fallback checks pass. Every original
and rewritten temporary file is identity-checked and retired after verification.

The complete-file byte medians below are summed across all 28 fields. Both
cohorts produce the same totals. These are sampled per-column files, not the
size of a concurrent full-table artifact.

| Source row group | Current files, bytes | Structural files, bytes | Structural/current |
| --- | ---: | ---: | ---: |
| 0 | 1,876,224 | 3,959,348 | 2.1103 |
| 113 | 5,318,244 | 6,219,016 | 1.1694 |
| 225 | 5,111,712 | 7,816,536 | 1.5291 |
| All three | 12,306,180 | 17,994,900 | 1.4623 |

Physical column bytes likewise grow from 12,167,796 to 17,856,516, a ratio of
1.4675. Both storage measures fail the frozen aggregate limit of 0.70 and the
per-region no-growth controls. Complete original-write cost fails its aggregate
limit in every region and both cohorts. All eleven activated field/region writes,
plus the ineligible `FlashMinor2` and `SearchPhrase` controls in row group 0,
exceed both parts of the individual regression limit in both cohorts. All
aggregate read, predicate and ordinary rewrite controls pass; no individual
consumer or rewrite failure repeats across both cohorts.

The [result index](../benchmarks/evidence/structural-results-810.json) and its
complete packet retain every raw sample, median, limit and failure witness.
No field is removed, gate relaxed or replacement cohort substituted. The finite
failure ends this profile's progression before full-size ingestion. Any future
structural candidate needs a materially different design and frozen protocol,
followed by complete-workflow, full-artifact and native-query lifecycle gates.

## Validation, restoration and ownership

The final experimental build passes five focused native tests, seven evidence-
validator tests, formatting and strict native-feature Clippy. Native tests cover
admission boundaries, exact Unicode/empty/null/NUL/escaped and irregular bytes,
native length/equality/filter/slice/gather semantics, scope isolation across
threads and panic, deterministic files, publication/move/reopen/rewrite,
output-cap refusal and reservation cleanup. Three ignored entry points are
executed by the 42 guarded workflows. The earlier intake adds two passing
summary tests and one guarded descriptive source run.

Four failed setup checks remain in the packet: two compilation attempts, one
lint attempt and one incorrect Arrow-type oracle assertion. The corrected oracle
requires native Vortex Utf8 with exact nullability and its Arrow Utf8View boundary,
while preserving the original Arrow Utf8 source and exact bytes. Two additional
pre-freeze binding refusals and their corrections are recorded. None changes
the profiles, input fields, semantic contract or frozen performance gates.

The 512-MiB native grant, finite input/file/live-payload bounds and existing
storage/concurrency/process controls remain unchanged. Reservations return to
zero and owned process groups drain. Arrow source buffers and the OnPair trainer
and decoder are not fully covered by native allocation ownership. This rejected
screen establishes no production OnPair memory or untrusted-input admission.

After verified packet readback, three tracked source/dependency files are
restored and six temporary Rust files are removed. The
[restoration receipt](../benchmarks/evidence/structural-restoration-810.json)
compares every one of the remaining 1,903 Rust/Cargo sources with Git base
`ec2b27f121ff7e5e8f740c999dca7f433737fbc7` and the complete source identities in
the [previous passing checks](../benchmarks/evidence/ingest-restored-checks-805.json).
Those checks include workspace Clippy, 3,235 workspace tests and both nine-test
native output-window configurations. They are reused after exact source equality,
not presented as fresh executions. A new
[format check](../benchmarks/evidence/structural-restored-fmt-810.json) passes.

The result packet retains 424 members, all 1,909 experimental Rust/Cargo hashes,
the exact three-file delta and six added sources, the descriptive intake,
frozen helpers, raw observations, failure history and completion barriers. It
contains no executable or native payload backup. The
[retirement receipt](../benchmarks/evidence/structural-retirement-810.json)
records 336 redundant per-case records and the obsolete experimental executable
removed after member-hash and closed-handle checks. The accepted source, output
and production runtime remain available. The
[closeout packet](../benchmarks/evidence/structural-closeout-810.json.xz)
preserves restoration, cleanup, portable-verification and acceptance evidence.

## Portable verification

The [portable checker](../benchmarks/evidence/verify_structural_packet_810.py)
pins the [result packet](../benchmarks/evidence/structural-results-810.json.xz),
verifies every member, reconstructs all experimental sources from Git and saved
deltas, checks all recorded workflows and recomputes every median and decision.
It separately verifies threshold comparisons with exact rational arithmetic.
The [post-retirement check](../benchmarks/evidence/structural-after-retirement-verification-810.json)
passes without original per-case files, experimental source edits or the retired
executable. This replays recorded evidence; it does not reread physical payloads.

```bash
python3 -B docs/benchmarks/evidence/verify_structural_packet_810.py \
  --packet docs/benchmarks/evidence/structural-results-810.json.xz \
  --repository . --scratch /path/to/local-only-scratch \
  --output /path/to/new-verification.json
```

The packet SHA-256 is
`9991f1cd88919533ca2f78260db2b93eeb51eec53e8b2f5f75ef0dee46339103`.
Physical reproduction requires regenerating the frozen artifacts under the
original guards. The missing production allocator and untrusted-input proofs,
remaining structural approaches and broader owner obligations stay explicit.
