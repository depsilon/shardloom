# Local artifact cleanup — September 26

The maintainer requested removal of obsolete test, branch and UAT storage while
the September 26 performance queue continues.

The inspected Cargo debug/test cache was entirely rebuildable and had no running
consumer. Removing that exact subtree reclaimed 156,599,050,240 bytes of measured
free space (145.8 GiB). Cargo's clean command refused the old target root because
it lacked `CACHEDIR.TAG`; the cleanup instead verified and removed only its
`debug` subtree. Release output, frozen comparators and evidence were preserved.
The ignored local Cargo configuration now disables incremental compilation to
limit repeated feature/test-build accumulation.

Eighteen local branch references were removed after recording their exact tips,
checking that each was an ancestor of `origin/main`, and excluding checked-out
branches. Remote references and unmerged work were preserved. App archival of the
finished `ship-drop-control-20260919` and `native-result-composition` worktrees
was refused because they are protected by a pinned task or workspace. That protection was retained;
unfinished relational and release worktrees were also preserved.

The obsolete `released-v0.2.3.vortex` payload (38,147,848,068 bytes) is retired from
local UAT storage. Before removal its complete SHA-256 matched the recorded
`6777eb4deea57cea7d83e772b3af4db2ebd77f003c38c1997ee0aadf02071c97` and its file
generation remained unchanged. September 5 ingest and complete Full43 receipts,
the recorded source revision, and that hash remain available. Historical evidence
continues to describe the run as performed; replay of that superseded payload now
requires regeneration. This retirement supersedes the September 12 statement
that the old release artifact remained untouched.

Current work retains `hits.parquet`, the protected older reference, the current
18,591,586,804-byte comparison artifact, all 43 output references, the frozen
native value/statistics comparator, and active control/candidate binaries. Fresh
control or candidate repeat output is removed only after a complete hash proves
it duplicates the corresponding retained artifact. The first candidate bulk
output is retained until its correctness and ship/drop decision are recorded.
Receipts and hashes survive bulk retirement.

R9.b's four full-ingest outputs each matched the complete SHA-256 of the retained
15,682,956,116-byte native artifact before their exact owned targets were removed.
This retired 62,731,824,464 cumulative bytes across the sequential runs, not that
much simultaneous disk usage. Source, retained artifact and binary generations
were checked. The dropped prototype and its experimental source fixtures were
removed from the final source tree; exact reproduction patches and raw evidence
remain in `docs/benchmarks/evidence/writer-subtree-occupancy-2026-09-26.json.gz`.

Exact actions, generation checks and before/after free space are recorded in
`/Users/dylan/LocalData/shardloom/performance-candidates-20260926/`:
`debug-cache-cleanup.json`, `merged-branch-cleanup-inventory.json`,
`control-r1-invalid-artifact.json`, and `bulk-artifact-cleanup.json`.
