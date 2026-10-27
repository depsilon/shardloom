<!-- SPDX-License-Identifier: Apache-2.0 -->

# Parquet source-demand completion: 347-case acceptance

The complete 347-case Parquet observation passes with **34.960427343 engine
seconds**, compared with **50.877676708** in the accepted first adapter. This is
a **31.3% observed reduction**. All 12,565 complete result rows and their schemas
match the retained independent expectations. Both predeclared target families
improve, and no case or family exceeds the prospective material-regression
threshold. This accepts the bounded
[Parquet completion work unit](../architecture/native-parquet-demand-completion-2026-10-26.md).

Each candidate has one observation per case. These results do not establish
statistical confidence, isolate the contribution of each mechanism, or establish
superiority to another engine. The original Polars observations remain unchanged;
they use Arrow IPC and have no native engine clock. No competitor query, separate
nine-case timing pass, or Full43 pass was repeated in this continuation.

## Implemented behavior

- The demanded Parquet reader buffers only the selected compressed column chunks
  of its current admitted row group. Reads remain bounded by validated signed
  offsets, source length, cancellation, source generation and native memory
  credits. The complete cohort records 101,354 data-read requests, down from
  6,347,946. This counts observed requests, not operating-system physical I/O.
- Bound projections retain hidden residual-filter dependencies while dropping
  unrelated input columns. Source-order LIMIT/OFFSET demand crosses direct-column
  aliases and nested ranges only when safe. Predicates, sorting, aggregation,
  joins, windows and fallible expressions remain semantic barriers. Repeated
  references combine conservatively; an unrestricted use wins.
- Sorted local Parquet directories use the same demanded batch owner as files.
  Ordering, global ranges, per-file pruning, embedded-schema agreement, held
  file generations and directory membership are checked. Single-output and
  fanout sinks reject aliases and new Parquet members in the input directory
  before writing, and revalidate held sources before publication.
- Explicit content-digest identity is supported without durable preparation.
  Hashing reads all bytes and uses the existing local FNV-64 convention; it is
  not authentication or validation of undemanded pages. Unsupported schema
  overrides fail explicitly. Explicit `prepare_once` and prepared-Vortex reuse
  preserve their original guarantees.
- Shared grouped SUM/AVG binds the existing borrowed typed numeric updater once
  per batch. It preserves per-measure/per-row update order, original-width
  values, NULL handling, compensated arithmetic, cancellation and reservations.
  Cross-batch dictionary domains remain value-aware; dictionary-local codes are
  never treated as global identities. Existing native Vortex execution and
  output owners remain in place.

These are shared planner, reader, aggregate and sink changes. There are no
benchmark-ID branches, facade-only engines, new execution dependencies or
external-engine fallback paths.

## Comparable observation

The frozen protocol precedes execution. Both observations use the same 347 case
identities, relations, five verified Parquet files, independent references,
Python SQL surface, 2-GiB native memory policy and two admitted lanes. Each case
executes once. The candidate runs in isolation with standard storage, workload,
deadline, output and cleanup guards. Full hashes run outside query execution;
generation checks remain active. No cache eviction or cold-cache claim is made.

Retention requires at least a 10% reduction in the whole cohort or a declared
target family. A case regression is material only when it exceeds both 20% and
0.010 engine seconds; a family regression must exceed both 20% and 0.050 engine
seconds. The declared families are projection and grouped aggregation. The
whole-cohort gate and both family benefit gates pass, with zero material flags.

| Family | Cases | Previous engine seconds | Candidate engine seconds |
| --- | ---: | ---: | ---: |
| Projection | 26 | 7.388781 | 6.154385 |
| Predicates | 16 | 2.855953 | 0.787894 |
| Nullable | 8 | 0.310796 | 0.130049 |
| Numeric | 4 | 0.719116 | 0.608927 |
| Strings | 5 | 1.479911 | 1.331113 |
| Temporal | 6 | 1.068690 | 0.879635 |
| Global aggregation | 67 | 4.722844 | 3.268936 |
| Distinct | 4 | 0.403727 | 0.328720 |
| Ordering | 10 | 1.546506 | 1.239656 |
| Joins | 14 | 2.751243 | 2.174269 |
| Sets | 4 | 0.005245 | 0.004348 |
| Grouped aggregation | 180 | 25.859804 | 16.535105 |
| Multiway joins | 2 | 1.367619 | 1.213723 |
| Composite | 1 | 0.397443 | 0.303665 |
| **Total** | **347** | **50.877677** | **34.960427** |

Grouped aggregation improves by 36.1% in this observation. Per-case engine times
and request counts are in
[case-results.csv](evidence/native-parquet-demand-completion-2026-10-27/case-results.csv).
Complete API, resource and supervision observations remain in the diagnostic
packet with their original clock definitions.

## Correctness and package gates

| Gate | Retained receipt | Result |
| --- | --- | --- |
| Native Vortex library, `release-user-surfaces`, `ci-native` profile | `r09-native-vortex-suite` | 3,331 passed; 32 existing opt-in tests ignored |
| Public Parquet CLI routing | `r10-native-cli-tests` | 8 passed |
| Default workspace, all targets | `r13-workspace-tests` | 3,261 passed |
| Strict native/default workspace Clippy and format | `r22`, `r12`, `r14` | Passed |
| Workspace all-feature and no-default checks | `r15`, `r16` | Passed |
| Minimal CLI Parquet reader feature boundary | `r17` | Passed |
| Release CLI and native Python build | `r18` | Passed |
| Isolated wheel installation, integration, load and uninstall | `r21` | 185 passed; no failures, errors or skips |
| Complete candidate/baseline result replay | `r25`, `r26` | All 347 cases, 12,565 rows per candidate |
| Portable verifier and corruption controls, also from a clean directory | `r27`, `r28`, `r30`, `r32` | Valid packet accepted; all eight corruptions rejected |

Required workspace commands are `cargo fmt --all -- --check`,
`cargo clippy --workspace --all-targets -- -D warnings`, and
`cargo test --workspace --all-targets`; receipts include the actual locked/offline
options. Native tests cover malformed ranges, selected-chunk boundaries, hidden
columns, prefix barriers, late corruption, low memory, cancellation, multi-file
membership and ordering, output aliases, content hashing, aggregate numeric
types/representations, dictionary-domain changes, spill and fault cleanup.
Installed-wheel tests exercise directory prefixes and grouped results through
all five public interfaces. Non-CLI interfaces forbid subprocess execution.

Earlier failures are retained: test import/type errors (`r02`, `r03`, `r05`);
the writer's missing Parquet-source guard (`r07`); its corrected import (`r08`);
two test-style lint findings (`r11`); and an incorrect guard-helper path during
pre-execution freezing (`r23`). The helper path was corrected to the existing
guard without changing supervision. The `r09` runtime tests precede only two
semantics-preserving test-style edits; strict native Clippy compiles the final
test sources. All runtime changes precede the passing native suite. No benchmark
query failed or was repeated.

An additional CI-harness invocation (`r29`) passes 64 tests and encounters one
macOS `PermissionError` during process-group cleanup. The failing test passes
in isolation without a source change (`r31`); both outcomes remain recorded.
This is separate from the completed runtime and query gates. Portable replay
uses `math.fsum` for version-stable total accumulation, with the original
measured clocks unchanged. The final verifier and controls pass again in `r30`
and from a clean source-only directory in `r32`.

## Identity and portable replay

The first adapter was committed as `57b622b8` and merged in
[PR #1612](https://github.com/depsilon/shardloom/pull/1612) at `81ab0a4d` after
57 successful checks. The candidate uses that parent plus the exact source
snapshot retained with the release build; the parent commit alone does not
identify the measured working tree.

| Artifact | SHA-256 |
| --- | --- |
| Candidate CLI | `77b33b6a2eb4607db09f70e87724b74f77c3d6c0c02631d0fa6f71b5613916d9` |
| Candidate native Python library | `a9b9d623b656b83a5ce37d91cc9fd6f880088a9c306dcfb2ac3d8c49379a68fd` |
| Tested macOS arm64 wheel | `8cb6415329355af0f9dc2ad4eb49e56d22bd42d5f2102f0d2890198df753c01e` |
| Prospective protocol | `fa2791c2eb8c961d49413ae0c8f7a4e1a79656a01b3eabf2a62f8e8b3a60ee78` |

The [portable manifest](evidence/native-parquet-demand-completion-2026-10-27/manifest.json)
links complete outputs, schemas, references, source/runtime identities, jobs,
guard receipts, failed attempts, build/package proof and the complete earlier
packet. Content-addressed archive objects are bounded and read as data. The
repository verifier never imports archived code or executes a query. Large data
and binaries are represented by identity, not reconstructed from the packet.
Hashes establish internal consistency, not independently signed provenance.

After portable verification, [retirement](evidence/native-parquet-demand-completion-2026-10-27/cleanup.json)
removes 14,199 completed generated regular files and six environment links.
Exact identity and closed-handle checks cover temporary package/install/build-tool
directories, copied verifier sources, archived raw logs and the superseded
first-adapter CLI/library/wheel. The latest tested runtime and wheel, all five
Parquet inputs, independent references, compact failures and complete results
remain. The shared Cargo cache and unrelated user files are untouched; no
payload backup or storage-ceiling change is introduced. Post-retirement replay
passes in `r34`.

```sh
python3 -B scripts/verify_parquet_demand_completion_packet.py \
  docs/benchmarks/evidence/native-parquet-demand-completion-2026-10-27
python3 -B -m unittest discover -s scripts \
  -p test_parquet_demand_completion_packet.py -v
```

This closes the specified Parquet completion unit. Query-scoped validation is
still distinct from full ingestion. Conservative prefix barriers, upstream
decoder-workspace estimates and single-observation uncertainty remain explicit.
Other formats, broader item 20 work and CG-1 through CG-23 remain open. No package
publication, release, exact allocator/RSS claim, or across-the-board Polars win
is implied.
