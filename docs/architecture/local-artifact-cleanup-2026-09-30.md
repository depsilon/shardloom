# Recorded artifact cleanup — September 30

The resumed ship/drop work retired **4,535,181,312 allocated bytes** through
R10, final R2.b log compaction and post-merge binary retirement. This is the sum of the inspected files' allocated blocks, including earlier
binary/cache retirement and net log compaction; it is not a measurement of APFS
free-space change and is separate from the September 26 cleanup.
The later [current-runtime follow-up](#current-runtime-follow-up) records
additional retirement separately.

R10's 127,975,424 bytes are exactly two executables: its removed temporary
capacity observer (`6825f771`) and its superseded R3.b comparison control
(`dff85c33`). Retirement followed PR #1487's merge, all 40 CI checks, complete
Full43 acceptance and an independent audit of all 300 saved comparisons.

The guard checks exact hashes, size, inode, generation, single-link regular-file
status and absence of running consumers/open handles. It verifies immutable
portable evidence, acceptance receipts and the merged runtime. The only Rust
differences from the measured R10 runtime are two test-only fingerprint additions;
the guard verifies those exact additions rather than ignoring arbitrary source
changes. Dry-run and applied receipts are preserved.

R2.b's final `93ee6b39` candidate, the released 0.3.2 binary,
retained input artifacts, all 43 result references, source manifests/patches,
compressed raw logs and validation receipts remain. No new full-size ingest or
format-comparison payload was generated. No in-use or protected worktree was
removed. Superseded binaries are reproducible from their recorded source/build
receipts; the original executable file is no longer locally available.

The metadata-admission cohort stopped at Q34 when accumulated logs reached the
256 MiB guard. Its 203 saved records and 204 raw outputs remain intact. Eight
explicitly completed historical fixture directories now retain their JSON
summaries and manifests in adjacent `closed-metadata-20260930.tar.xz` archives.
Each directory's `closed-metadata-20260930.index.json` maps original basenames to
byte-identical archived members. All member hashes, file generations and lack of
active/open consumers were checked before removing the loose copies. Raw call
archives and input artifacts were unchanged. This lossless compaction recovered
**40,726,528 allocated bytes** after archive/index overhead; the guard was not raised.

Final R2.b retirement removed exactly five superseded executables:
`6be7bc02` (initial dictionary screen), `a33da94f` (R10 comparison control), and
`d726aaf6` (original dictionary/provider runtime), `5ea34b11` (drain-corrected
runtime), and `5ec7a893` (interrupted metadata-admission cohort). Their portable evidence and
build receipts remain. Removal followed PR #1488's merge, all 40 exact-head
checks, the corrected runtime's 258-result independent audit, final validation,
and unchanged runtime sources between the measured and merged source. The
guard rechecked identity, open handles and evidence before each removal, preserving
`93ee6b39`, released 0.3.2 and every retained input. The applied
`r2b-recorded-binary-cleanup.json` records **320,200,704 allocated bytes** removed,
included in the total above. The merge source is
`c4b328798ce18dcb9ee7f6220f3f7b360c4b354e` with no runtime-source difference
from the accepted measured build. Completed managed worktrees protected by a
pinned task remain intact; cleanup did not bypass that protection.

Local receipts: `closed-metadata-compaction-r2b-20260930.json`,
`r10-cleanup-dry-run.json`, `r10-recorded-binary-cleanup.json`,
`r10-merge-receipt.json`, `r4-dropped-binary-cleanup.json` and the preceding R2.a/
R3.a/R3.b cleanup receipts under
`/Users/dylan/LocalData/shardloom/performance-candidates-20260926/`.

## Current runtime follow-up

After PR #1493 merged, all 40 checks passed, and the fresh complete
[current-runtime observation](../benchmarks/current-runtime-e2e-2026-09-30.md)
passed, the maintainer requested recording its controls and cleaning up unused
artifacts. Exactly five superseded files were retired:

| Artifact | Disposition | Allocated bytes |
| --- | --- | ---: |
| `shardloom-p033-1-1d3a6c68` | Earlier text-statistics candidate/control; incorporated into current runtime. | 73,490,432 |
| `shardloom-p033-3-e89ab9e0` | Failed initial string-view candidate; superseded by its correction. | 73,490,432 |
| `shardloom-p033-3-2f5a99fb` | Accepted ingest control; incorporated into current runtime. | 73,490,432 |
| `shardloom-p033-7-3148ad05` | Intermediate string-count runtime; incorporated into current runtime. | 73,490,432 |
| `derived-dictionary-20260926.vortex` | Older physical reference; exact encoded contents retained by the current reference. | 15,682,957,312 |

This removes **15,976,919,040 allocated bytes** (15,976,909,460 logical bytes).
These are file-block sums, not measured APFS free-space gains. The freshly
generated 15,682,956,489-byte E2E duplicate was also removed after full byte
equality, as part of that run's own cleanup; it is excluded from the five-file
retirement total.

The superseded Vortex reference's complete SHA-256 was
`31cc61cfc347cf19a0328c196d59cd1eb431679311294cdc92263fef31062b35`.
The retained `profile-033-20260930.vortex` is 373 bytes larger: 285 bytes of
source provenance and 88 bytes of postscript growth. Cleanup reverified the
entire encoded-data prefix and exact schema, layout, statistics and segment
directory blocks. Saved ranges plus the original 168-byte postscript/trailer
reconstruct every old byte from the retained file; the streaming reconstruction
hash matches the complete old SHA-256. No new full-size file was written for this
verification. Replaying the original path now requires reconstruction from this
receipt or regeneration; the original executable files require rebuilding.

The guard checked saved source/binary/payload hashes and file generations,
single-link regular-file status, no running native consumer, no open handle,
the shared UAT lock, merged acceptance and both immutable experiment packets.
It wrote a preflight receipt before any removal and an applied receipt afterward.
An initial schema-check failure stopped before removing anything; that failed
preflight receipt is preserved too.

The resident Parquet source, current `83850558` executable, released 0.3.3
control, current Vortex reference, all 43 canonical result logs, manifests,
raw-output archives and build/validation receipts remain. Older artifacts with
unresolved ownership or purpose were inventoried and preserved. No shared Cargo
cache, protected worktree, cloud-managed file or unrelated user file was removed.
This supersedes earlier statements that the September 26 Vortex reference and
these four candidate executables remain locally available.

The [portable evidence packet](../benchmarks/evidence/current-runtime-e2e-2026-09-30.json.xz)
contains `closed-post033-artifact-cleanup-preflight.json`,
`closed-post033-artifact-cleanup.json`, the exact retirement script, reconstruction
record and linked experiment-packet hashes. Original local receipts live under
`/Users/dylan/LocalData/shardloom/performance-candidates-20260930/`.
