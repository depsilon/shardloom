# Completed UAT payload cleanup — October 10, 2026

At the maintainer's request, 40,365 generated payload files from the completed
native input-growth public portfolio were archived and their redundant originals
removed. This recovered **1,287,606,272 accounted bytes** after archive and manifest
overhead. It changes storage disposition, not the original correctness evidence.

The [receipt](../benchmarks/evidence/local-artifact-cleanup-2026-10-10.json)
records source-summary, archive, manifest, helper, and supervisor identities.
The original cohort contains 32,497 passing cases across ten completed families.
Its summary and all 149 recorded input fixtures remain unchanged.

Only regular, single-link `.vortex`, `.parquet`, `.arrow_ipc`, `.avro`, `.orc`,
`.csv`, and `.jsonl` files under that cohort's generated data/log directories
were selected, excluding every source fixture recorded by the parent or child
summaries. The archive retains 1,278,695,028 original logical bytes in a
76,631,728-byte compressed file. JSON evidence, envelope archives, manifests,
frozen executables, failed/incomplete runs, and current build outputs were not
selected.

Before removal, the helper verified completion, summary and fixture hashes,
closed handles, exact archive membership, every member's length and SHA-256,
and unchanged original identities. It rechecked retained summaries and fixtures
after removal. The supervised operation exited successfully and its process
group drained. Existing storage limits were unchanged.

## Recovering an original payload

The retained local growth workspace contains:

- `growth-regression-public-all-1/completed-growth-payloads-20261010.tar.xz`
- `growth-regression-public-all-1/completed-growth-payloads-20261010.manifest.json`

Each manifest entry preserves its original path relative to the cohort, file
identity, length, and SHA-256. Read the corresponding archive member when an
original payload is needed; extract only to a new local directory and verify
its manifest hash. The original paths no longer contain these generated files.
The archive is retained locally, not embedded in the small checked-in receipt.

These are file-accounting measurements, not an exclusive measurement of host
free-space change or engine performance. Active typed-input probes and their
failure evidence remain available for the continuing implementation work.
