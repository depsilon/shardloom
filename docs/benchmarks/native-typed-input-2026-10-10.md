# Exact typed input and Python result delivery

Ordinary `from_rows` and `from_batches` declarations now carry exact integer
widths, finite Float32/Float64, Boolean, UTF8, binary, Decimal128, Date32,
timezone-free microsecond timestamps and recursive list/fixed-list/struct types
into the shared native source. Resident, buffered and finite single-use input
share the existing native planner, operators, resource owners and writers.
The four original scalar declarations and inference remain compatible.

Local engine acceptance, the Python review correction and independent inspection
are complete. Refreshed support and rendered guide checks pass.
[PR #1540](https://github.com/depsilon/shardloom/pull/1540) merged at `316f52de`
after all 39 hosted checks passed on `d9ceba02`; the
[hosted receipt](evidence/native-typed-input-hosted-2026-10-10.json) verifies
identical reviewed/merged trees and production deployment and browser checks. The
[contract](../architecture/native-typed-input-2026-10-10.md),
[evidence index](evidence/native-typed-input-2026-10-10.json),
[base packet](evidence/native-typed-input-2026-10-10.json.xz) and
[Python review packet](evidence/native-typed-input-review-2026-10-10.json.xz)
preserve their distinct tested sources. Published v0.5.1 packages are unchanged;
this capability unit makes no speedup or broader completion claim.

## Values, ownership and persistence

Rich types require explicit schemas. Native construction validates exact widths,
decimal precision/scale, nullability, fixed-list shape and complete struct fields
before allocation. The existing input boundary carries typed JSON cells; native
buffers, nested metadata and conversion overlap retain the shared grant through
child aliases, slices and empty arrays. This does not account for caller-owned
Python objects, every provider allocation or total process RSS.

Pinned Vortex 0.85.0 supplies the admitted DTypes and native array constructors.
The implementation preserves the full Date32 and Int64 timestamp storage domains.
For schemas containing microsecond timestamps, the writer omits file-wide min/max
and leaves timestamp storage uncompressed because the pinned provider's calendar
validation is narrower than its storage domain. Other fields retain their layout
policies and zone statistics. Restoring safe timestamp statistics and compression
is an explicit ingest/pruning follow-up; narrowing the accepted domain is not.

Review found a separate Python output problem: valid native temporal endpoints
overflowed Python's calendar during `python_objects` conversion. The correction
returns `date`/`datetime` for representable values and exact integer epoch
days/microseconds otherwise, including nested fields. Invalid native widths,
booleans, floats and strings still fail. Arrow conversion retains temporal storage
units without passing through Python calendar objects.

## Complete verification

The 87-case typed family checks every declared field and value across rows,
resident batches and finite streaming, through SQL and DataFrame collection,
incremental delivery, native write/reopen and admitted compatibility writers.
It includes typed empty/all-null data, integer and temporal endpoints, decimals,
binary zeros, Unicode, nested nulls, empty/fixed lists, malformed native schemas,
late producer/value failures, cancellation and owned-output cleanup.

Finalization reopens 343 raw reports, 164 complete value files, 36 recursive
schema proofs, 88 output artifacts, 177 output identities, 30 float bit patterns
and 16 malformed-protocol traces. It verifies 39 expected denials, 32 timestamp
preparation certificates and 25 stream-completion certificates. These counts
overlap and must not be added as independent workloads.

| Validation | Accepted result |
| --- | --- |
| Source/build, formatting, lint, feature and MSRV gates | 16 pass; 12 reuse explicitly verified identical source |
| Default workspace tests | 3,149 pass |
| Native library tests | 2,526 pass; 24 ignored |
| Native CLI / example tests | 1,324 / 17 pass |
| Python tests after review | 633 pass, no skips |
| Typed complete workflows after review | 87 pass; 54 check Python objects |
| Retained streaming / growth workflows | 442 / 20 pass |
| Input-pressure controls | All five pass, including expected resident denial |
| Complete public regression | 32,497 cases; 18,595,284 rows compared |
| Direct unary regression | 202 cases; 131,734 rows compared |
| Batch / format adapters | 48 / 19 pass |
| Admitted semantics / golden stages | 145 / 9 pass |
| Independent inspector contract | 443 positive/negative checks pass |
| Full43 retained-input regression | All 129 complete results pass |

All twelve base workflow stages run freshly. The ignored native tests remain
separately gated benchmark, attribution, external-fixture or regeneration helpers.
The review packet freshly executes the Python suite and typed family against the
unchanged native binary. Independent readback reconstructs 36 saved complete
reports and 36 `ResultBatch` object views, checking exact Python types, values and
field order. The 18 incremental object checks belong to the actual live execution;
individual iteration frames were not retained for independent reconstruction.

The original temporal regression fails in two of the four new tests before the
fix and passes after it. A review evidence-runner attempt also rejects a successful
633-test log because buffered artifact paths follow `OK`; its subprocess passed.
The corrected reader checks the actual unittest summary and exit status, with nine
positive/negative parser checks. Both original sources and observations remain
in the review packet; the subsequent complete review stages pass.

## Provenance and timing

Native engine/public acceptance tests clean commit
`28250d0f904f9fd781e0925aa71a4b887d9003fe` and 1,030 source assets, identified by
`9cb15d2edffaad51eb317080c7c030e8483a3b0aa04d46e58fc4b83addc06b2a`.
The Rust 1.99.0 macOS arm64 release executable uses `release-user-surfaces`, with
SHA-256 `2ee4bf5d453e09679248aa28cfd08ffd2f94fa45472451cdf5832d55cbd83b18`.

The Python correction tests clean commit
`9653c2d2ed24edbfc71b3a22381e3fb266c22f02`. Exactly four conversion/test/harness
files change; all Rust, Cargo and vendor bytes remain identical. Its 1,031-asset
identity is `9f08d5544d33c145c2d5f1221c12e8f3bd2e368ade54216e2326f20e395fcfc3`.
The base native runs retain their original commit and are not relabeled as review
executions. The separate hosted receipt validates the final PR head and its merge.

Full43 runs each query three times against the retained 15,682,956,489-byte Vortex
artifact, with 24 GiB and maximum parallelism 12 explicitly configured. The sum
of query minima is 71.771214625 seconds; medians sum to 73.053518044 seconds.
All 129 native calls total 225.379178751 seconds; the guarded stage takes
259.708523792 seconds. Maximum observed native-process RSS is 4,803,231,744 bytes.
This is an unpaired retained-input correctness regression with uncontrolled OS
cache and ordinary host activity, not fresh ingest or a speedup comparison.

The base packet is 54,157,908 compressed bytes with SHA-256
`dd5d1900751130ded623a2353ea0061e1b1029d918087044bba5be91e5a954ec`;
its complete decompressed JSON has 4,674,894,587 bytes and SHA-256
`8eaf5caff38426daab08a91c1b5bae17ba5aa71058c95f7580f4d0a22f00248a`.
The review packet is 333,360 compressed bytes with SHA-256
`a7abb94519157846486afa25a7537f851befd5a8bc0f2c5eb4dc69702449a606`;
its 19,051,033 decompressed bytes hash to
`3b07548c0cf9f4ba46b068b9b8c1882842a39655f9e64ade469175f6735dd38e`.
Both independent inspections verify complete decompressed identities and required
coverage; current invalid-check, invalid-report and fallback counters are zero.
Finalizers and inspectors complete within their deadlines with child groups drained.

Full43 preflight first stops at the unchanged 252-MiB log-admission threshold,
before executing queries. One completed historical cohort's 516 closed files are
losslessly archived, with every original identity and archived byte verified,
recovering 3,231,744 accounted bytes. Finalization independently reopens all 516
members. Failed/incomplete evidence, inputs and storage ceilings remain unchanged.
The subsequent preflight and all Full43 runs pass.

## Documentation and review

Eleven structured documentation checks, use-case backlinks, 15 focused release/
documentation tests and all ten site generation/check steps pass. The site check
reports zero errors and warnings. Rendered desktop and 390-by-844 mobile review
confirms the temporal-value explanation and acceptance link; the `calendar`
search returns the updated Python section and its navigation works. No browser
warnings or errors are observed. The
[support receipt](evidence/native-typed-input-support-2026-10-10.json) binds the
source checks, site output and browser observations.

The first support evidence reader assumes a `status` field that the website
readiness v3 schema does not expose. Its three validators pass before the reader
fails; the correction checks the actual empty-blocker contract and passes. The
original helper, failure log and receipt remain preserved. The separate hosted
receipt records passing checks and production verification. Production calendar
search opens the corrected section, with its acceptance link and exact epoch-value
explanation visible and no console warnings/errors. The observed hosted viewport
is 685 pixels; the 390-pixel mobile observation above belongs to local support QA.

## Remaining work

Per-frame and small-result collection bounds remain explicit. Multiple/repeated
producers, dynamic schemas, deeper traversal, streaming compatibility destinations
and fanout retain their existing owners. The next maintainer priority is required
explicit resources across execution surfaces, followed by native Python result
ownership, columnar exchange and in-process operations. Full-domain timestamp
statistics/compression remains attached to ingest/pruning. All six remaining areas,
eight investigations and CG-1 through CG-23 remain visible under their existing
phase-plan owners and statuses.
