<!-- SPDX-License-Identifier: Apache-2.0 -->

# Fresh local UAT before streaming composition

Status: passed on merged main `ed253d1ec74b724215f71f0b410123656e596696`.
This is the maintainer-requested timing and correctness observation before
resuming streaming capabilities and broader resource/failure hardening. It
does not measure the subsequent streaming-composition changes.

| Observation | Result |
| --- | --- |
| Fresh ingest from the resident official Parquet | 99,997,497 rows; 51.421543 seconds native process time |
| Full43, three fresh-process executions per query | 129/129 complete-value checks; 177.372562 seconds summed native process time |
| Actual supervised ingest plus query workflow | 284.224608 seconds |
| Modular format/workflow acceptance | 18/18 cohorts; 1,408 comparisons, including 704 native records |
| Modular supervised stage | 491.566070 seconds |
| Ingest peak RSS | 3,021,471,744 bytes; measured, not enforced |

The modular matrix covers eight raw/prepared input formats with collection,
and eight outputs from CSV input, across 22 workflows. It is not the complete
input/output Cartesian product. An independent pandas oracle is confined to
comparison; every ShardLoom result records no fallback or external execution.
The Full43 reference packet is retained native regression evidence. All 129
compressed outputs were independently reopened and compared in full, covering
1,143 returned rows and the required native I/O certificates.

The native resource policy is 24 GiB and 12 lanes on a 16-GiB Apple Silicon host.
It is not a process-RSS limit. Source hashing precedes timing; newly written
native input and OS cache effects are uncontrolled. Host load and process
CPU/RSS samples are retained. The workflow clock includes guarded ingest,
query setup and normal per-call validation, but excludes the initial source
hash, subsequent independent readback, modular acceptance, final hashes and
packaging. The modular clock is a separate supervised stage, not native engine
execution time. This observation makes no paired speedup or superiority claim.

The reused release executable has SHA-256
`490f00b2e29842b3a4b066245612a2243c8ce8da67a8decc29097e9bec4d0ff7`.
Its build receipt binds `release-user-surfaces` and runtime revision
`c6cb4c7ac4fdeda8bbf665b3299d1b7231a94e4e`. All production sources match merged
main; exactly seven selected harness files changed after that build, and their
merged identities are frozen separately. The fresh driver checks the complete
selected source set and clean tree around every stage and after completion.

The resident Parquet remains 14,779,976,446 bytes, SHA-256
`a390f6cb782f6aaef278c72fc1dd86c4f30bc843ebab3c159e9bd4d45ddb079f`.
The newly produced `stream-native-389-a1.vortex` is 15,713,610,513 bytes,
SHA-256 `004f9639fa0aa527b70f326c366c37b1173dceeacb92b50b2d8fd02957155c18`.
Generation checks surround final hashing. All owned process groups drain, the
workspace lock is released, and the existing disk/workspace/log guards pass.
No source, prior accepted native input, release or package is replaced.

The [portable evidence index](evidence/native-fresh-uat-2026-10-13.json) binds
the complete driver, helpers, build/source identities, query definitions and
references, ingest/query logs, all modular fixtures/reports and host samples.
The archive contains 13,345 members, 284,496,906 original bytes, and was
independently reopened byte-for-byte after packaging. Large input/output files
and the executable are represented by their verified identities. Replaying the
driver requires resident inputs and new output paths; completed paths are
immutable. The first packaging attempt rejected an unsupported Python tar
compression argument before writing an archive; its local failure log remains
retained, and the corrected packaging attempt is independently verified.
