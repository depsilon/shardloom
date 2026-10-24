<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native operator optimization evidence

The [acceptance record](../../../architecture/native-engine-optimization-acceptance-2026-10-23.md)
defines the original gates, implementation scope, decisions and remaining proof
boundaries. Runtime `4af8fc72` passes final validation at documentation checkpoint
`dc9e53a4`. No external execution provider or package release is introduced.

- [Combined value/view/sort/predicate decision](composed-decision.json)
- [Finite sort demand](sort-prefix-decision.json)
- [Prepared expressions](expression-decision.json)
- [Shared numeric aggregate consumer](aggregate-numeric-decision.json)
- [Failed first join decision](join-first-decision.json) and
  [accepted single revision](join-revised-decision.json)
- [Historical five-surface extraction](surface-comparison.json)
- [Final validation commands, outcomes and skips](final-validation-summary.json)

The [compact archive](compact-evidence.tar.xz) preserves 4,196 original records
and protocol files. The [manifest](compact-evidence-manifest.json) maps each
member to its original path, byte length and SHA-256. The
[packaging receipt](compact-evidence-package.json) binds the archive and manifest
and confirms complete byte-for-byte readback against unchanged originals.

Archive members under `batch/` retain their relative paths from the original
local experiment directory. They include all comparison calls and result/schema
witnesses, all Full43 observations and compressed complete results, initial
failures, build/source receipts, drivers, scorers and final validation logs.
`batch/full43-join-run-1020.json` binds the accepted Full43 observation;
`batch/full43-join-1020/` contains its standard-runner outputs and timing CSV.
`batch/final-validation-outcome-1019.json` links all eight final-stage receipts.
`protocol/` contains the independent Full43 reference and shared supervisors.
`manifest.json` inside the archive matches the adjacent manifest exactly.

Preserved decision files retain their original pending-validation fields;
the final acceptance and validation summary record completion. Byte readback
does not constitute a new semantic replay or corruption-test suite. Reproducing
engine observations requires the recorded source, features and resident input,
new output identities, and the repository's normal storage/concurrency guards.
Do not run a historical driver against its already completed paths.

The [cleanup receipt](cleanup.json) and compressed
[exact retirement inventory](cleanup-inventory.json.xz) record removal of
completed generated artifacts after a second complete archive readback, source
generation checks and closed-handle checks. Original Parquet and current Vortex
input remain, together with the latest accepted CLI and measured native test
executable. Old executables, compiler outputs and package environments are not
archived as rollback backups. Git source remains the regeneration basis.

The maintainer-requested observation after opening PR #1594 is recorded
separately in [full43-pr1594-1022.json](full43-pr1594-1022.json). It reports
**48.693636581 engine seconds**, one execution per query, all 381 independent
rows, and the same accepted runtime. Its
[216-member archive](full43-pr1594-1022.tar.xz) and
[manifest](full43-pr1594-manifest-1022.json) retain complete results, the driver,
reference, source/build identities and protocol files with full byte readback.
This later observation does not replace any earlier measurement or candidate
decision.

The [PR follow-up validation](pr1594-validation-1023.json),
[31-member packet](pr1594-validation-1023.tar.xz) and
[manifest](pr1594-validation-manifest-1023.json) preserve the initial hosted
website audit and Python replay failures, their local fixes and checks, and the
temporary harness retirement. Original measurements remain unchanged; all
Rust/Cargo sources still match the accepted executable. Local checks do not
substitute for the updated PR's hosted checks.
