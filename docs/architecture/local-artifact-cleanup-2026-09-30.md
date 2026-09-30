# Recorded artifact cleanup — September 30

The resumed ship/drop work has retired **4,174,254,080 allocated bytes** through
R10. This is the sum of the inspected files' allocated blocks, including earlier
binary/cache retirement and net log compaction; it is not a measurement of APFS
free-space change and is separate from the September 26 cleanup.

The latest 127,975,424 bytes are exactly two executables: R10's removed temporary
capacity observer (`6825f771`) and its superseded R3.b comparison control
(`dff85c33`). Retirement followed PR #1487's merge, all 40 CI checks, complete
Full43 acceptance and an independent audit of all 300 saved comparisons.

The guard checks exact hashes, size, inode, generation, single-link regular-file
status and absence of running consumers/open handles. It verifies immutable
portable evidence, acceptance receipts and the merged runtime. The only Rust
differences from the measured R10 runtime are two test-only fingerprint additions;
the guard verifies those exact additions rather than ignoring arbitrary source
changes. Dry-run and applied receipts are preserved.

R2.b's corrected `5ea34b11` candidate, its earlier candidates and R10 comparison control, the released 0.3.2 binary,
retained input artifacts, all 43 result references, source manifests/patches,
compressed raw logs and validation receipts remain. No new full-size ingest or
format-comparison payload was generated. No in-use or protected worktree was
removed. Superseded binaries are reproducible from their recorded source/build
receipts; the original executable file is no longer locally available.

Final R2.b retirement is prepared for exactly three superseded executables:
`6be7bc02` (initial dictionary screen), `a33da94f` (R10 comparison control), and
`d726aaf6` (original dictionary/provider runtime). Their portable evidence and
build receipts remain. Removal is gated on PR #1488's merge, all 40 exact-head
checks, the corrected runtime's 258-result independent audit, final validation,
and exact test-only differences between the measured and merged source. The
guard rechecks identity, open handles and evidence before each removal, preserving
`5ea34b11`, released 0.3.2 and every retained input. These planned removals are
not included in the total above. The applied `r2b-recorded-binary-cleanup.json`
receipt will record the actual allocated bytes and cumulative total after merge.

Local receipts: `r10-cleanup-dry-run.json`, `r10-recorded-binary-cleanup.json`,
`r10-merge-receipt.json`, `r4-dropped-binary-cleanup.json` and the preceding R2.a/
R3.a/R3.b cleanup receipts under
`/Users/dylan/LocalData/shardloom/performance-candidates-20260926/`.
