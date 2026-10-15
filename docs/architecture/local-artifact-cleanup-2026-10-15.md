<!-- SPDX-License-Identifier: Apache-2.0 -->

# Local artifact cleanup — October 15, 2026

After the [fresh UAT with separated query clocks](../benchmarks/native-query-timing-uat-2026-10-15.md)
passed, the maintainer-requested cleanup removed 16 exact file or directory roots.
Observed free space increased from 21,136,723,968 to 59,313,737,728 bytes, a
38,177,013,760-byte increase (38.18 GB). The new 15.7-GB UAT output was already
present before cleanup. Concurrent filesystem activity can affect the free-space
observation; it is separate from logical or allocated payload totals.

| Retired generated artifacts | Logical bytes | Retained reproduction evidence |
| --- | ---: | --- |
| Two superseded Vortex outputs | 31,427,221,026 | Original passing UAT receipts, source/build identities and regeneration commands; fresh accepted output and active comparison input remain |
| Two identical 0.6.0 source-archive copies | 3,094,531,540 | One fully hash-matched canonical source archive, release manifests and channel proof |
| Six completed wheel package stages and virtual environments | 3,617,710,323 | All six wheels, build/install commands, reports and failure diagnostics |

The wheel roots are `native-wheel-proof-320`, `343`, `361`, `367b`, `374b` and
`376` under `/Users/dylan/LocalData/shardloom/native-python-surface-20261010`.
Only each root's `package/` and `venv/` directories were removed: 13,746 regular
files, plus the inventoried directory and symlink entries. Symlink targets were
not followed. All six completed integration receipts passed with no failures,
errors or skips; their retained wheel bytes were rehashed before and after
cleanup. A compressed per-entry metadata inventory preserves the exact retired
paths and generations. These environments can be recreated from their retained
wheels and recorded commands; no redundant environment backup was generated.

The exact retired Vortex files, each 15,713,610,513 bytes, were fully rehashed
against their historical passing receipts before removal:

| Retired file under the local UAT `vortex/` directory | SHA-256 |
| --- | --- |
| `stream-native-389-a1.vortex` | `004f9639fa0aa527b70f326c366c37b1173dceeacb92b50b2d8fd02957155c18` |
| `unpaced-native-435-a1.vortex` | `ceac4cf2cf8e0c8bc0e3da82fed82e4e24c8238c6f1a6768246d6d975c02bffc` |

The duplicate source archives both matched the retained canonical
`release-v0.6.0/release-060-assets-1/shardloom-936313473fed-source.tar.gz`, SHA-256
`bcbabc313d6a54a5fba7ee553795c9f3b9898be3fe8db15517109550ee184bc3`.
The retired copies were in that stage's `github-prerelease-assets/` directory
and the completed GitHub installation proof's `downloads/` directory. This
changes local copy retention; it does not alter published release assets.

All paths were local and explicitly scoped. Workload exclusion, residency and
stable-generation checks, and no-open-handle checks preceded removal. Every
selected root was verified absent afterward. The original `hits.parquet`, fresh
`timed-native-461-a1.vortex` and comparison `persisted-native-343-a1.vortex`
retained their exact generations. Current frozen runtime/comparison executables,
compiler caches, source checkouts, wheels and compact pass/fail evidence remain.
No storage ceiling changed and no cloud-managed payload was hydrated.

The first attempt stopped before any deletion because the historical wheel
receipt stores a basename within its `dist/` directory. The corrected helper
resolves that exact recorded directory, verifies the same expected hash and
preserves the first helper and failure receipt. No failed attempt receives
cleanup credit.

The [portable cleanup packet](../benchmarks/evidence/local-artifact-cleanup-2026-10-15.json)
retains eight verified members: both helpers and receipts, the per-entry
metadata manifest and the three bounded inventory/provenance packets. The
historical UAT measurements remain attached to their original artifacts.
