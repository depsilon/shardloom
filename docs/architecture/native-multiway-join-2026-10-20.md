<!-- SPDX-License-Identifier: Apache-2.0 -->

# Finite native three-input join — October 20, 2026

Status: the three-input constraint probe is **not retained**. Its prototype and
initial integration pass both frozen cohorts, but the integration needed for
ordinary public projection shapes misses a control in cohort A. Cohort B
passes. Retention requires both; the result does not establish a repeated
regression. All experimental runtime changes are removed. Ordinary join
regressions remain, with fresh workspace, native and installed-package checks.

This completes the finite row-14 decision under `PERF-10.multiway_joins`.
Broader multiway planning remains open. The
[conditional sequence](native-conditional-investigations-2026-10-19.md) proceeds
to learned-index attribution, while eligible pre-writer idle attribution remains
a prerequisite to revisit before required breadth. RFC 0044, all 58 roadmap
substeps and CG-1 through CG-23 retain their separate obligations.

## Mechanism and frozen scope

The [independent intake](native-multiway-join-intake-2026-10-20.md) fixes eight
memory-source workloads before candidate implementation. The three large cyclic
targets produce 524,288, 524,288 and 1,198,372 intermediate rows per ordinary
execution, with 4,096, zero and 9,363 final rows. Duplicate/null, dense, small,
star and ordinary two-table cases supply controls. Every output column, row
order and input formula is independently specified.

The [candidate design](native-multiway-join-comparison-design-2026-10-20.md)
reuses ShardLoom's existing Vortex-native integer-key, row-index and batch/table
owners. It builds exact S.b and T.(a,c) access paths, probes all three equalities
and gathers payload only for complete matches. Duplicate products preserve
R/S/T source order. It still enumerates R/S pairs; no worst-case optimal join
claim follows. First-use preparation and per-call index construction remain
charged. No dependency, external engine, spill permission, canonicalization
route or public option is added.

Admission is confined to two left-deep inner equijoins, three pure immutable
memory scans and the stated materialized integer fields. Floating/noninteger
keys, additional predicates, transformed sources, files, one-shot streams and
explicit spill keep ordinary native execution. The
[integration gate](native-multiway-join-integration-design-2026-10-20.md) adds
default dispatch and actual execution-report evidence after the prototype
passes. The [public projection revision](native-multiway-public-projection-admission-2026-10-20.md)
then admits one bound projection containing only column references and aliases.
All other source and semantic restrictions remain intact.

## Results and decision

The unchanged protocol uses one CPU, a 64-MiB native grant, one/four-call
workflows and three paired samples per case in each of two cohorts. Cohort B
reverses case/call order and paired role ordering. Each phase has 80 preflight
queries and 304 queries per scored cohort, including unscored warmups. Both
roles use the same optimized executable and complete public preparation and
collection path. Reference creation, independent comparison and evidence
serialization remain outside the operation clocks.

| Phase | Cohort A | Cohort B | Consequence |
| --- | --- | --- | --- |
| Test-selected prototype | All gates pass | All gates pass | Integration attempt permitted |
| Default native dispatch | All gates pass | All gates pass | Public shape proof still required |
| Public projection admission | One control fails | All gates pass | Retention gate not met |

In the final phase, the one-call star control's median engine time changes by
**+13.20% in A** and **+0.25% in B**, against a 5% maximum increase. Its separate
complete-operation control also fails in A. All other engine/complete-operation
controls and all native-memory gates pass. The three intended targets reduce
engine time by 88.91–93.43% across their two call counts and cohorts, but target
gains do not override controls. These are finite paired observations, not Full43
results or general performance claims. Component clocks, individual samples,
host-load and owned-process observations remain in the packet. Ordinary desktop
activity and the absence of CPU isolation limit causal interpretation.

All 2,080 queries across intake and the three phases pass complete ordered
values/schema checks, activation or non-admission, certificates, native-credit
limits and final credit release. Independent verification checks 16,160,040
output rows and reproduces every recorded gate. Corrupted-value, schema,
activation, counter, ownership and nonfinite-clock controls are rejected.
Focused candidate tests also cover NULLs, empty inputs, duplicate output,
source aliases, integer widths, key order, projections, cancellation, consumer
failure, reservation denial and recovery. Historical tests validate the attempt;
they do not confer production acceptance.

No cohort is substituted, favorable subset selected, threshold relaxed or
unchanged failed candidate rerun. The outcome is **not retained**: both cohorts
must pass, while the protocol's stronger repeated-failure classification is
not supported here.

## Restoration and verification

Eight production files are restored byte-for-byte to the pre-experiment base,
and five experimental modules are removed. The retained native regression
checks complete cyclic values/types, NULLs, empty cases, duplicates, repeated
executions, both upper key orders, repeated source aliases and zero final
credits. Public CLI tests cover 16 frontend/join-kind/key-order combinations.
Two installed Python tests cover four command surfaces and chunked retained
source execution, including exact rows and refusal to spawn another process.

The [restoration evidence](../benchmarks/evidence/multiway-restoration-closeout-823.json)
records fresh `cargo fmt --all -- --check`, strict workspace and native-feature
Clippy, and `cargo test --workspace --all-targets`. Workspace tests pass
3,235 with no ignores; optimized native tests pass 3,041 with 30 existing
opt-in/manual ignores. The focused native and public CLI selectors pass. The
isolated installed wheel passes 113 Python tests without skips, followed by
successful uninstall and absence checks. These suites overlap.

The first public test used unsupported flat chained SQL syntax and failed
before execution. Its correction uses the existing supported derived-relation
syntax; the parser is unchanged. Historical build/test corrections remain in
the main packet. During restored-package proof, an empty retired dependency
directory prevented installation of the pinned PyArrow test consumer. That
attempt ran no ShardLoom integration tests. A fresh wheelhouse restores the
exact lockfile hash; the same native library and CLI then pass clean installation,
all integration tests and uninstall. The failed report and logs remain in the
restoration packet. PyArrow remains an optional boundary test consumer.

Every production/runtime/harness file shared with the previously accepted
output-window build is unchanged; the source differences are test files only.
Public39, Full43 and the modular comparison are therefore not repeated for this
restoration. Their earlier acceptance remains historical evidence; no fresh
pass or candidate acceptance is claimed. Current normal CLI/library and tested
wheel artifacts replace obsolete local runtime copies. No publication occurs.

## Portable evidence and cleanup

The [historical packet](../benchmarks/evidence/multiway-historical-packet-823.tar.xz)
and [manifest](../benchmarks/evidence/multiway-historical-packet-823-manifest.json)
retain source versions, protocols, observations, failures and complete row
witnesses. Verification checks 2,690 archive members, six compilation snapshots,
11,432 compilation references, 2,227 public-source references and 89 binding
references. Historical executable identities are metadata only. Labels such as
`accepted_public_sources` inside an intermediate record describe its original
filename, not an accepted multiway runtime.

The [restoration packet](../benchmarks/evidence/multiway-restoration-closeout-823.json.xz)
adds the exact restoration, all 2,222 current source identities, 1,901 compilation
inputs, complete fresh gate logs and repaired package proof. Its 71 members
include five source blobs absent from the historical pool. Together the two
packets reconstruct source and recorded results without Git or old executables.

The [retirement receipt](../benchmarks/evidence/multiway-retirement-823.json)
records 5,702 generated files removed after exact content, stable identity and
closed-handle checks. Old binaries, duplicate wheels and completed package
environments are retired without executable backups. Current inputs, current
runtime, tested wheel, pinned dependency and compact evidence remain.
[Historical replay after retirement](../benchmarks/evidence/multiway-historical-after-retirement-824.json)
rechecks every complete result; the
[restoration replay](../benchmarks/evidence/multiway-restoration-after-retirement-824.json)
passes independently after original temporary files are gone.

Recorded-evidence replay uses Python's standard library and local scratch:

```bash
python3 -B docs/benchmarks/evidence/replay_multiway_historical_823.py \
  docs/benchmarks/evidence/multiway-historical-packet-823.tar.xz \
  docs/benchmarks/evidence/multiway-historical-packet-823-manifest.json \
  /path/to/new-historical-verification.json --scratch-root /path/to/local-scratch
python3 -B docs/benchmarks/evidence/package_multiway_restoration_823.py --verify \
  docs/benchmarks/evidence/multiway-restoration-closeout-823.json.xz \
  docs/benchmarks/evidence/multiway-historical-packet-823.tar.xz \
  docs/benchmarks/evidence/multiway-historical-packet-823-manifest.json \
  /path/to/new-restoration-verification.json
```

Historical packet SHA-256:
`8d615d396758c65e44b034e66f97f7d36212dc8dee94c6697db32e96bcf6f8f3`.
Restoration packet SHA-256:
`28953e7c942338a3a67de3cdb392cd9c282cdee4d007c4009ae4b2b75c108bb7`.
Replay checks recorded observations; physical reproduction requires rebuilding
the bound sources and regenerating inputs under the current guards.
