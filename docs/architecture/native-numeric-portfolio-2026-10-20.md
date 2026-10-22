<!-- SPDX-License-Identifier: Apache-2.0 -->

# Finite numeric compression portfolio — October 20, 2026

Status: the reduced numeric profile is **rejected** under its frozen screen.
All exact checks pass and every aggregate gate passes, but one individual
column's complete-write control fails in both cohorts. No profile or
instrumentation is retained. This completes the finite row-11 investigation;
it does not close the broader `PERF-09.portfolio` owner or establish that every
possible costed portfolio is unhelpful.

The [prospective design](native-numeric-portfolio-design-2026-10-20.md) and
[frozen protocol](../benchmarks/evidence/numeric-portfolio-protocol-805.json)
remain unchanged. The [conditional sequence](native-conditional-investigations-2026-10-19.md)
continues with original-payload structural representation. Eligible pre-writer
idle attribution remains a separate missing prerequisite to revisit before the
37 required substeps resume. RFC 0044 and all CG-1 through CG-23 remain in force.

## Evaluated mechanism and scope

The screen compares the existing numeric compressor with pinned Vortex 0.85.0's
`only_cuda_compatible()` builder preset, followed by the existing edition and
dictionary filtering. This is a CPU experiment inside the existing primitive
numeric boundary; the provider method name does not imply GPU execution.
Existing encoded dictionary preservation, statistics, text handling and writer
topology retain their paths. The classification is `use_vortex_native_provider`.
No external engine or execution fallback is invoked.

The resident official Parquet source is 14,779,976,446 bytes with SHA-256
`a390f6cb782f6aaef278c72fc1dd86c4f30bc843ebab3c159e9bd4d45ddb079f`.
Each invocation independently prepares the first 32,768 rows of one of row
groups 0, 113 and 225. It selects all 77 primitive numeric fields in original
schema order and writes each field through the actual native table strategy
as a separate file. The source's frozen primitive fields are integers;
synthetic fixtures separately exercise every supported numeric width and
floating bit pattern.

Six preflight workflows precede two complete cohorts of 18 measured workflows
each: three regions, three paired repeats and two profiles. Orders reverse
symmetrically according to the protocol. The decision uses all 36 measured
workflows; preflight costs receive no decision credit. Comparative scoring
begins only after both cohorts and the final complete source hash finish.

Both arms share a freshly compiled, source-bound release test executable.
The producer exits before a fresh verifier independently reopens the original
source and every native file. The verifier checks complete ordered values,
nulls, logical types, schema, supported physical encodings, file/segment bounds,
statistics and all ordinary read/count observations. Statistics use the pinned
native statistic kernels against independently reopened input; this is shared
provider-algorithm parity, not an independent statistic implementation.

The 512-MiB native grant, 64-MiB per-file limit, 256-MiB batch-file cap and
existing storage/process guards remain fixed. Every producer and verifier
drains its process group, every native reservation returns to zero and every
temporary file is identity-checked and retired after verification. Native
credits are an ownership budget, not a total process-memory ceiling.

## Decision

All 42 workflows and all 3,234 written files pass exact verification, including
2,772 files in the measured cohorts. Every aggregate numeric/completed-write,
physical-byte, complete-file-byte and native-consumer gate passes.

`DontCountHits` in row group 113 exceeds both parts of the prospective individual
write-regression limit in **each** cohort. The rule requires rejecting the
profile even when aggregate gates pass. The
[result index](../benchmarks/evidence/numeric-portfolio-results-807.json)
and complete packet retain the exact samples, medians, gate arithmetic and
both witnesses. No favorable-column selection, post-hoc waiver or replacement
cohort is used.

This is a per-column provider screen. It does not measure the concurrent full
table, full ingestion, public query dispatch or a Full43 engine total. Source
prehashing and earlier source access have warmed data; fresh verifier owners do
not establish a cold filesystem cache. Native I/O, materialization, CPU and
elapsed observations retain their distinct meanings in the diagnostic records.
The failed finite gate requires no full-size ingest merely to repeat the result.
Future retention would need a materially different design and the complete
workflow, full-artifact and native-query lifecycle gates described in the design.

## Validation, restoration and retention

The successful experimental build passes nine focused native tests and nine
decision-checker tests, formatting and strict native-feature Clippy. Coverage
includes all numeric widths, empty/null inputs, extrema, signed zero and NaN
bits, explicit numeric dictionary preservation, profile capture across threads,
scope cleanup after panic, activation, exact native consumers and bounded writer
refusal. Two additional ignored tests are the exact producer/verifier entry
points exercised by all 42 guarded workflows.

The packet preserves all five earlier failed setup checks: two compilation
attempts, two fixture/reference test attempts and one lint attempt. The
reference corrections precede the measurement freeze: full supported statistics,
a fixture that distinguishes the profiles, and native floating equality's
bitwise distinction between signed zeros. These change neither the frozen
integer source workload nor the performance gate. All 13 setup-check receipts
and their exact logs are retained, including the final successful checks.

After preserving and reopening the result packet, the four modified tracked
source/dependency files are restored and the five temporary Rust files are
removed. The [restoration receipt](../benchmarks/evidence/numeric-portfolio-restoration-807.json)
verifies all 1,903 remaining Rust/Cargo sources against both Git base
`ca72dba5ed7bd8e5dc6ee38a4f3a59ed42bc014d` and the exact source identities in the
[previous passing checks](../benchmarks/evidence/ingest-restored-checks-805.json).
Those existing checks include required workspace Clippy, all 3,235 workspace
tests and both nine-test native output-window configurations. They are reused
after complete source equality, not reported as fresh executions. A new
[format check](../benchmarks/evidence/numeric-restored-fmt-807.json) also passes.

The compact packet retains 323 members and all 1,908 experimental source hashes,
with four tracked-file deltas and five added sources against the recorded Git
base. It contains exact raw records, failure history, source/build identities,
frozen helpers, protocol, decision and ordered completion barriers. It contains
no executable or native payload backup. The
[retirement receipt](../benchmarks/evidence/numeric-portfolio-retirement-807.json)
records removal of 252 redundant per-case records and the obsolete experimental
test executable after full member verification and closed-handle checks. The
accepted input, current accepted output and production runtime are preserved.
The [compact closeout packet](../benchmarks/evidence/numeric-portfolio-closeout-807.json.xz)
also retains the restoration/retirement helper, portable checker, new format
receipt and post-retirement verification.

## Portable verification

The [portable checker](../benchmarks/evidence/verify_numeric_portfolio_packet_807.py)
pins the complete [result packet](../benchmarks/evidence/numeric-portfolio-results-807.json.xz),
verifies every member, reconstructs all experimental Rust/Cargo sources from
Git plus the saved deltas, validates every recorded workflow and recomputes all
frozen medians and gates. It separately checks threshold comparisons using exact
rational arithmetic. It neither executes measurement helpers nor needs the
retired executable, original per-case files or live experimental source edits.
The [post-retirement reopening](../benchmarks/evidence/numeric-portfolio-after-retirement-verification-807.json)
passes. This replays recorded evidence; it does not reread retired physical files.

From a repository containing the recorded base, with an existing local-only
scratch directory and a new receipt path:

```bash
python3 -B docs/benchmarks/evidence/verify_numeric_portfolio_packet_807.py \
  --packet docs/benchmarks/evidence/numeric-portfolio-results-807.json.xz \
  --repository . --scratch /path/to/local-only-scratch \
  --output /path/to/new-verification.json
```

The packet SHA-256 is
`49b5834d666e8899b3d36f75b84466fa7bf428a4682ba260dbf6c02d64762ccf`.
Reproducing physical observations requires regenerating the frozen native
artifacts under the original guards; no new benchmark is implied by reopening
the compact evidence.
