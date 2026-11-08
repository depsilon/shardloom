<!-- SPDX-License-Identifier: Apache-2.0 -->

# Vortex and Arrow provider upgrade evidence

Local acceptance for the [November 8 provider contract](../../../architecture/native-provider-upgrade-2026-11-08.md).
The selected graph uses Vortex 0.93.0 and one Arrow/Parquet 59.3.2 runtime family.
The packet binds the complete source snapshot, dependency graph, provider patches,
runtime and installed wheel. Hosted review, merge, exact-main checks and subsequent
artifact retirement have separate receipts; this frozen local packet does not
retroactively claim those later outcomes.

| Check | Accepted observation |
| --- | --- |
| Default Rust workspace | 3,321 passed; zero failures or ignores |
| Native Rust workspace | 6,996 passed; zero failures; 37 reported opt-in ignores |
| Formatting and strict lint | Default and native configurations passed |
| Features and supported Rust | Feature configurations and Rust 1.96 default/native checks passed |
| Standalone providers | All 15 configurations have passing tests and strict lint tied to current inputs |
| Dependency and license gate | Unchanged release audit passed; no runtime fallback dependency |
| Python core | 1,247 discovered cases; 857 reported runs; 56 optional/environment skip records; zero failures/errors |
| Isolated installed wheel | 386 passed; zero failures, errors or skips; load and uninstall passed |
| Portable verification | Original and relocated replay passed; all 13 corruption controls rejected |

The fifteen validation roles share a content-identified snapshot of 5,657 source
paths. Source replay verifies all fifteen provider trees and standalone locks,
104 changed graph records, 34 new-to-prior-standalone development identities,
268 Parquet fixtures and 119 distinct published source archives. The notice
bundle covers 431 registry packages, 15 path providers, 829 legal-file records
and 249 distinct texts. ORC's unchanged Arrow 58.4.1 development dependencies
remain isolated from the engine's Arrow 59.3.2 runtime graph.

The first full native run exposed 22 compatibility regressions. The corrected
24-test compatibility cohort, 19 statistics ownership/wide-schema checks and
complete final native run pass. Decimal intake validates values reachable through
ancestor validity and slices before preserving stored width and raw bits. Ordinary
upstream imports and deserialization retain their original validation/narrowing.
RunEnd's view owners move into the array provider; shared dictionaries, nested
statistics, scalar bounds, writer admission and output ownership retain their
existing contracts. Fresh native write/read/reopen, spill and compatibility
boundaries are exercised through the ordinary installed package.

Standalone results are reused only after comparing the provider and direct patch
sources, exact lock, feature/target configuration and runner logic. Provenance-only
metadata updates are separately replayed against the current source archives.
The interrupted array/layout wrapper is not accepted as a complete run: the final
array check has its own clean before/after source validation and completed child
report. The remaining affected configurations have completed matching receipts.

The archive retains 70 original nonacceptance receipt records and their available
logs, including preparation records and child results. These are not 70 independent
runtime executions. Initial failures, storage-floor stops and evidence-checker
corrections remain visible; missing original raw logs are explicitly identified,
not reconstructed. The earlier calendar/transport/CI patch baseline is a separately
sealed nested packet with its own fifteen stages and installed proof. Replaying
that baseline verifies its earlier source, not the upgraded provider runtime.

## Offline replay

Use Python 3.11 or newer. After relocating this directory, run:

```sh
python3 -I verify.py
```

From the matching source checkout, also compare the complete source and final
documentation bindings:

```sh
python3 -I docs/release/evidence/native-provider-upgrade-2026-11-08/verify.py --repo-root .
```

Replay uses the saved sources, archives, commands, logs and identities. It does
not rebuild, install, execute engine workloads, access the network or require
the original temporary directories. `documentation.json` binds final status prose
separately from the frozen validation snapshot; compiled source, manifests, locks,
tests, scripts, provider metadata and notices must still match exactly.

The saved controls perform actual subprocess verification of the original and a
relocated copy. They reject an unsealed archive change and twelve resealed changes:
failed native tests, installed skips, an omitted original failure, changed compiled
source, a changed provider archive, omitted standalone coverage, a failed standalone
child, source drift, a failed audit policy, an altered earlier dependency baseline,
an omitted required stage and an unsafe archive path. Temporary mutated packets
are retired after their logs and mutation identities are recorded.

## Dependency dispositions and boundaries

The intended hosted dispositions are:

- #1664 is superseded by the coordinated Vortex 0.93 migration.
- #1665, #1667 and #1668 are incorporated with their original proposal ancestry
  and verified calendar, transport and CI changes.
- #1666's mixed Arrow 60/59 proposal remains unaccepted. Arrow 60 requires a
  coordinated migration across Vortex, ADBC, ORC and the Python interfaces.

These dispositions do not establish live PR closure; hosted reconciliation is
a separate step. Exact vector expressions resume after that closeout.

Vortex remains native input and highest-fidelity persistence. V2 scan execution,
new external effects, vector arithmetic and ANN remain unadmitted. Historical-file
compatibility remains outside the inherited fresh-lifecycle acceptance contract.
Selected allocation grants do not establish a whole-process RSS bound or a blanket
zero-decode claim. No external query engine executes unsupported ShardLoom work.
This packet makes no benchmark, performance, package-publication or completion
claim for item 19, item 20, RFC 0044 or CG-1 through CG-23.
