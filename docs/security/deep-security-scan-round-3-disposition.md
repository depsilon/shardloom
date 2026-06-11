<!-- SPDX-License-Identifier: Apache-2.0 -->

# Deep Security Scan Round 3 Disposition Matrix

Status: round-3-bounded validation complete for the current phase item. Fixed candidates have
code/tests or validator evidence; deferred candidates have explicit owner/gate boundaries. This
document does not create public vulnerability advisories, publish packages, create release tags, add
secrets, add runtime dependencies, or authorize fallback execution.

## Source Artifacts

- Scan id: `dcdd1fbe4993_20260611T130604Z_deep`
- Scan bundle:
  `/tmp/codex-security-scans/shardloom-local-repo/dcdd1fbe4993_20260611T130604Z_deep`
- Round-3 merge record:
  `/tmp/codex-security-scans/shardloom-local-repo/dcdd1fbe4993_20260611T130604Z_deep/artifacts/deep_discovery/round-03/round_merge_record.json`
- Canonical inventory:
  `/tmp/codex-security-scans/shardloom-local-repo/dcdd1fbe4993_20260611T130604Z_deep/artifacts/deep_discovery/canonical_candidate_inventory.json`
- Canonical discovery report:
  `/tmp/codex-security-scans/shardloom-local-repo/dcdd1fbe4993_20260611T130604Z_deep/artifacts/02_discovery/finding_discovery_report.md`

Round 3 was intentionally the final discovery round for this pass. The scan did not prove
saturation because round 3 added new canonical clusters.

## Merge Summary

| Field | Value |
| --- | --- |
| Completed discovery rounds | 3 |
| Round-3 workers | 6 |
| Round-3 worker-deduped rows | 26 |
| Prior canonical candidates | 25 |
| New round-3 canonical clusters | 3 |
| Round-3 rows absorbed | 23 |
| Canonical candidates after round 3 | 28 |
| Saturation status | `not_proven_round_3_added_novelty` |

New round-3 clusters:

- `SL-DEEP-026`: Local-source SQL paths can read arbitrary parseable local files when exposed to
  less-trusted callers.
- `SL-DEEP-027`: Release dry-run cleanup can delete the repository root when an environment path
  resolves to `repo_root`.
- `SL-DEEP-028`: Dependency freshness live-GitHub check can send a GitHub bearer token to a
  caller-controlled URL.

## Disposition Rules

Every row must move to one of these terminal dispositions before the phase-plan security item can
close:

- `reportable_fixed`: validation showed a plausible in-scope issue and the repo now has a fix plus
  regression evidence.
- `suppressed_with_counterevidence`: validation defeated the issue with exact source evidence.
- `not_applicable`: the candidate does not apply to the current supported product or execution
  boundary.
- `deferred_with_owner_and_gate`: the issue remains outside the current slice but is blocked by an
  explicit release or support gate.

`candidate_needs_validation` is a non-terminal discovery state. No severity or public security claim
is allowed for rows still in that state.

## Candidate Matrix

| Candidate | Family | Disposition | Current validation note |
| --- | --- | --- | --- |
| `SL-DEEP-001` | Path/workspace containment | `deferred_with_owner_and_gate` | Feature-gated synthetic spill payload write/read/delete now use workspace-safe path validation for derived `.spill` targets and reject symlink targets; broader caller-selected spill workspace-root authority is deferred to the workspace-root policy gate. |
| `SL-DEEP-002` | Path/workspace containment | `deferred_with_owner_and_gate` | Feature-gated spill lifecycle marker create/delete now use workspace-safe staged writes and marker path validation; broader caller-selected spill lifecycle workspace-root authority is deferred to the workspace-root policy gate. |
| `SL-DEEP-003` | Path/workspace containment | `deferred_with_owner_and_gate` | Local-emulator object-store staging, target writes, commit-manifest sidecars, and rollback cleanup now use workspace-safe path validation or staged writes; broader caller-selected local-emulator output authority is deferred to the workspace-root policy gate. |
| `SL-DEEP-004` | Path/workspace containment | `deferred_with_owner_and_gate` | Local table append-commit manifest writes, commit-record sidecars, and rollback cleanup now use workspace-safe path validation or staged writes; broader caller-selected table-manifest output authority is deferred to the workspace-root policy gate. |
| `SL-DEEP-005` | Path/workspace containment | `deferred_with_owner_and_gate` | Vortex committed-manifest write/verify and rollback cleanup now validate committed-manifest paths and reject symlink rollback targets; broader caller-selected staged workspace authority is deferred to the workspace-root policy gate. |
| `SL-DEEP-006` | Local-source/query control | `deferred_with_owner_and_gate` | Python query-builder raw SQL breakout vectors are blocked and typed subquery predicates remain scoped; legacy simple raw local predicates remain caller-owned until a public less-trusted SQL-authority gate is designed. |
| `SL-DEEP-007` | Path/workspace containment | `deferred_with_owner_and_gate` | Foundry-style Python metadata and evidence part files now use same-directory staged writes and reject symlink/hardlink targets; broader caller-selected local dataset-root authority is deferred to the workspace-root policy gate. |
| `SL-DEEP-008` | CI/workflow hardening | `reportable_fixed` | Validated by workflow inspection and fixed by pinning PyPI Trusted Publisher workflow actions to immutable commit SHAs. |
| `SL-DEEP-009` | Resource budget | `reportable_fixed` | SQL local-source reads now use an explicit byte budget before full buffering, with regression coverage for oversized regular files. |
| `SL-DEEP-010` | Resource budget | `reportable_fixed` | Vortex ingest local-source reads share the bounded reader and reject oversized inputs before digest/materialization work. |
| `SL-DEEP-011` | Resource budget | `reportable_fixed` | Object-store local-emulator full/range reads now enforce fixture byte caps. |
| `SL-DEEP-012` | Resource budget | `reportable_fixed` | Object-store write, recovery, and commit-manifest reads now enforce source/object/manifest byte caps. |
| `SL-DEEP-013` | Resource budget | `reportable_fixed` | Object-store partition discovery now enforces depth and directory-count budgets and skips symlink directories. |
| `SL-DEEP-014` | Resource budget | `reportable_fixed` | SQLite local import/export smokes now enforce database byte, table-row, and JSONL export byte budgets. |
| `SL-DEEP-015` | Evidence/redaction | `reportable_fixed` | Object-store read/write/recovery evidence now redacts credential-bearing URIs and emits redaction status fields. |
| `SL-DEEP-016` | Evidence/redaction | `deferred_with_owner_and_gate` | Broad planning/benchmark URI redaction remains deferred to the public-evidence field-classification gate; no public benchmark/release claim may use raw credential-bearing target URIs. |
| `SL-DEEP-017` | Evidence/redaction | `reportable_fixed` | Python client command/protocol errors now redact credential-bearing URI argv and URI substrings before exceptions are raised. |
| `SL-DEEP-018` | Path/workspace containment | `deferred_with_owner_and_gate` | Vortex prepared-state manifest sidecars now publish through workspace-safe staged writes rooted at the artifact parent; broader local-output workspace-root semantics remain deferred to the workspace-root policy gate. |
| `SL-DEEP-019` | Evidence/provenance | `reportable_fixed` | Prepared-state reuse file fingerprints, manifest digests, append-only prefix checks, and Vortex artifact evidence now use SHA-256, with algorithm evidence fields retained. |
| `SL-DEEP-020` | CI/workflow hardening | `reportable_fixed` | Validated by workflow inspection and fixed by moving package build/install work to a no-OIDC build job before the protected publish job. |
| `SL-DEEP-021` | Evidence/provenance | `reportable_fixed` | Release evidence artifact merge now records producer artifact names, per-file SHA-256s, artifact tree digests, copied refs, and digest-binding status while rejecting symlinked entries. |
| `SL-DEEP-022` | CI/workflow hardening | `reportable_fixed` | Validated by workflow inspection and fixed by pinning CodeQL, Scorecard, checkout, and SARIF upload actions in privileged security workflows. |
| `SL-DEEP-023` | Evidence/provenance | `deferred_with_owner_and_gate` | Local-emulator recovery sidecars remain local-consistency evidence only; authenticity proof is deferred to a producer-bound or signed-manifest recovery gate before public recovery claims. |
| `SL-DEEP-024` | Path/workspace containment | `reportable_fixed` | Validated by source trace and focused regression tests. Automatic append-only delta source sidecars now live under the prepared target workspace and publish through workspace-safe staged writes. |
| `SL-DEEP-025` | Evidence/redaction | `deferred_with_owner_and_gate` | Release dry-run transcripts and release-evidence merge refs now redact repo-root/temp paths; broader generated/benchmark absolute-path redaction is deferred to the public-evidence field-classification gate. |
| `SL-DEEP-026` | Local-source/query control | `deferred_with_owner_and_gate` | Local-source file reads remain an explicit caller-owned local API boundary; less-trusted callers require a future allow-root/authority policy gate before public service exposure. |
| `SL-DEEP-027` | Path/workspace containment | `reportable_fixed` | Validated by source trace and focused regression tests. Release dry-run cleanup now rejects the repository root and protected top-level directories before `rmtree`. |
| `SL-DEEP-028` | CI/workflow hardening | `reportable_fixed` | Validated by static trace and focused regression tests. Live GitHub checks now reject non-admitted URLs before `urlopen` and before any token-bearing request can be sent. |

## Validation Log

### SL-DEEP-028

Rubric:

- [x] Caller-controlled source identified: `--github-url` and `--github-token-env` in
  `scripts/check_pre_5j_dependency_freshness.py`.
- [x] Sensitive sink identified: `urllib.request.Request(url, headers=...)` with an
  `Authorization` header when a token is present.
- [x] Existing control reviewed: token lookup was optional, but the live URL was not restricted
  before request construction.
- [x] Fix applied: live mode now admits only the expected HTTPS GitHub pulls API endpoint with no
  userinfo, custom port, or fragment.
- [x] Regression evidence added: `python/tests/test_release_scripts.py` asserts unadmitted URLs do
  not reach `urlopen` and userinfo URLs are blocked.

Disposition: `reportable_fixed`.

Remaining scope: this disposition covers the dependency freshness live-GitHub helper only. It does
not close broader network or object-store authority candidates.

### SL-DEEP-008

Rubric:

- [x] Privileged workflow identified: `.github/workflows/pypi-publish-draft.yml` has a protected
  `pypi` environment and `id-token: write` in the publish path.
- [x] Mutable action sink identified: checkout, setup-python, and PyPI publish action refs were
  tag or branch based.
- [x] Fix applied: all PyPI Trusted Publisher workflow actions are pinned to immutable commit SHAs.
- [x] Regression evidence added: `check_security_posture.py` fails on mutable privileged action
  refs and passes the current pinned workflow.

Disposition: `reportable_fixed`.

### SL-DEEP-020

Rubric:

- [x] Job-wide OIDC boundary identified: GitHub Actions grants `id-token: write` at job scope.
- [x] Pre-publish install/build sink identified: the old PyPI workflow built packages in the same
  job that could request an OIDC token.
- [x] Fix applied: build/install work moved to a separate no-OIDC `build` job; the protected
  `publish` job depends on the built artifact.
- [x] Regression evidence added: `check_security_posture.py` validates the build/publish split and
  rejects package builds inside the OIDC publish job.

Disposition: `reportable_fixed`.

Remaining scope: the publish job still needs `id-token: write` for PyPI Trusted Publisher. That is
the intended publish boundary and remains protected by manual input, the `pypi` environment, and
SHA-pinned actions.

### SL-DEEP-022

Rubric:

- [x] Privileged workflows identified: CodeQL and Scorecard workflows have `security-events: write`.
- [x] Mutable analyzer/upload sink identified: CodeQL, Scorecard, checkout, and SARIF upload actions
  were tag based.
- [x] Fix applied: privileged security workflow actions are pinned to immutable commit SHAs.
- [x] Regression evidence added: `check_security_posture.py` validates SHA-pinned `uses:` refs in
  these workflows.

Disposition: `reportable_fixed`.

### SL-DEEP-027

Rubric:

- [x] Caller-controlled source identified: `--venv-dir` and `--conda-env-dir` in
  `scripts/release_dry_run_proof.py`.
- [x] Destructive sink identified: `remove_tree_under_repo()` calls `shutil.rmtree()` on existing
  clean-environment directories.
- [x] Existing control reviewed: outside-repo paths were rejected, but `repo_root` itself was
  allowed.
- [x] Fix applied: cleanup rejects `repo_root` and protected top-level repository directories while
  allowing nested release dry-run environment directories.
- [x] Regression evidence added: focused tests assert `repo_root` and top-level `target` are not
  removed, while nested `target/release-dry-run-proof/venv` cleanup still works.

Disposition: `reportable_fixed`.

### SL-DEEP-001

Rubric:

- [x] Caller-controlled source identified: `spill-payload-roundtrip` and
  `cleanup-synthetic-payload` accept a workspace path and payload id.
- [x] File-effect sinks identified: feature-gated synthetic payload write/read/roundtrip cleanup
  and explicit cleanup operate on `<workspace>/<payload_id>.spill`.
- [x] Fix applied for the concrete symlink/TOCTOU sink: writes now route through
  `write_workspace_safe_bytes`, reads validate the derived target through
  `plan_workspace_safe_local_output`, and explicit cleanup validates the target before
  `remove_file`.
- [x] Regression evidence added: feature-gated Rust tests assert preplaced payload symlinks are
  rejected for write and cleanup and do not modify the symlink destination.
- [ ] Remaining validation: decide the broader policy for caller-selected spill workspace roots
  versus intentional local CLI workspace selection.

Disposition: `deferred_with_owner_and_gate`.

Remaining scope: this mitigation closes the symlink-following write/delete path for derived payload
files. It does not yet close the larger question of whether feature-gated spill workspaces should
require an externally approved root.

### SL-DEEP-002

Rubric:

- [x] Caller-controlled source identified: `spill-lifecycle` accepts a workspace id, workspace
  path, and side-effectful mode when `spill-lifecycle-fs` is enabled.
- [x] File-effect sinks identified: marker creation, marker cleanup, and empty-workspace cleanup
  under the selected workspace path.
- [x] Fix applied for the concrete marker symlink sink: marker creation now uses
  `write_workspace_safe_bytes`, and marker cleanup validates the marker path through
  `plan_workspace_safe_local_output` before `remove_file`.
- [x] Regression evidence added: feature-gated Rust tests assert marker create and cleanup reject
  preplaced marker symlinks without modifying the symlink destination.
- [ ] Remaining validation: decide whether feature-gated spill lifecycle workspaces require an
  explicit externally approved workspace root instead of the current caller-selected root.

Disposition: `deferred_with_owner_and_gate`.

### SL-DEEP-003

Rubric:

- [x] Caller-controlled source identified: `object-store-write-smoke` accepts local-emulator
  source and target paths plus overwrite/rollback options.
- [x] File-effect sinks identified: staged object write, target commit, commit-manifest sidecar
  write, overwrite backup, and rollback cleanup.
- [x] Fix applied for the concrete symlink/staging/sidecar sinks: no-overwrite target commits and
  commit-manifest sidecars now publish through `write_workspace_safe_bytes`; overwrite staging is
  created through the workspace-safe staged writer before the existing backup/restore commit path;
  target, sidecar, and rollback deletes are validated through
  `plan_workspace_safe_local_output`.
- [x] Regression evidence added: CLI smoke tests assert target and commit-manifest symlinks are
  rejected and do not modify the symlink destination.
- [ ] Remaining validation: decide whether caller-selected local-emulator output targets require an
  explicit externally approved workspace root instead of the current inferred local output root.

Disposition: `deferred_with_owner_and_gate`.

### SL-DEEP-004

Rubric:

- [x] Caller-controlled source identified: `local-table-append-commit-rehearsal-smoke` accepts a
  local manifest target path plus overwrite/rollback options.
- [x] File-effect sinks identified: manifest write, commit-record sidecar write, and rollback
  cleanup.
- [x] Fix applied for the concrete symlink/staging/sidecar sinks: manifest and commit-record writes
  now publish through `write_workspace_safe_bytes`, and rollback cleanup validates the target before
  `remove_file`.
- [x] Regression evidence added: CLI smoke tests assert manifest-target and commit-record symlinks
  are rejected and do not modify the symlink destination.
- [ ] Remaining validation: decide whether caller-selected local table manifest targets require an
  explicit externally approved workspace root instead of the current inferred local output root.

Disposition: `deferred_with_owner_and_gate`.

### SL-DEEP-005

Rubric:

- [x] Caller-controlled source identified: Vortex staged-output and local commit paths accept a
  caller-provided staged workspace path for feature-gated local file effects.
- [x] Existing controls reviewed: finalized manifest, output payload, commit marker, and committed
  manifest creation already publish through `write_workspace_safe_bytes`.
- [x] Remaining concrete sink identified and fixed: committed-manifest idempotent verification and
  rollback cleanup now validate the committed-manifest path with
  `plan_workspace_safe_local_output` before read/delete decisions.
- [x] Regression evidence added: feature-gated Vortex tests assert rollback rejects a committed
  manifest symlink without modifying the symlink destination.
- [ ] Remaining validation: decide whether caller-selected staged workspace roots require external
  approval/provenance beyond the current local workspace contract.

Disposition: `deferred_with_owner_and_gate`.

### SL-DEEP-007

Rubric:

- [x] Caller-controlled source identified: Python `ShardLoomContext.foundry_generated_output`
  accepts local result and evidence dataset paths.
- [x] File-effect sinks identified: result metadata, evidence part, and evidence metadata writes.
- [x] Fix applied for the concrete Python sidecar sinks: Foundry-style metadata and evidence part
  writes now use a same-directory staged writer that rejects `..`, symlink targets, and hardlinked
  file targets before replacing local files.
- [x] Regression evidence added: Python context tests assert result metadata and evidence part
  symlinks are rejected without modifying the symlink destination.
- [ ] Remaining validation: decide whether caller-selected local Foundry-style dataset roots require
  an externally approved workspace root before this support graduates beyond fixture proof.

Disposition: `deferred_with_owner_and_gate`.

### SL-DEEP-018

Rubric:

- [x] Sidecar sink identified: Vortex prepared-state reuse manifests were rendered through direct
  temp-file creation and rename under artifact-adjacent `.shardloom`.
- [x] Fix applied for that sidecar sink: key/value manifests now publish through
  `write_workspace_safe_bytes`, and `.shardloom` sidecars are rooted at the artifact parent rather
  than at a potentially preplaced sidecar directory.
- [x] Regression evidence added: feature-gated Rust tests assert a preplaced `.shardloom` symlink
  blocks prepared-state reuse manifest publication.
- [ ] Remaining validation: generated-source, SQL, SQLite, Vortex ingest primary outputs,
  Python-side prepared-route manifests, and benchmark sidecars still need the broader
  workspace-root policy decision.

Disposition: `deferred_with_owner_and_gate`.

### SL-DEEP-024

Rubric:

- [x] Caller-controlled source identified: automatic append-only refinement derives a delta source
  sidecar during prepared-state reuse.
- [x] Sink identified: old code created a source-adjacent `.shardloom` directory and wrote the
  delta source with raw `fs::write`.
- [x] Fix applied: automatic delta source sidecars are now derived under the prepared target
  artifact's `.shardloom` workspace and written through `write_workspace_safe_bytes`.
- [x] Regression evidence added: the Vortex ingest append-only refinement test uses separate source
  and target directories and asserts the delta source sidecar is target-adjacent, not
  source-adjacent.

Disposition: `reportable_fixed`.

### SL-DEEP-006

Rubric:

- [x] Caller-controlled source identified: Python `LazyFrame.filter()` and `having()` still accept
  legacy raw SQL strings for caller-owned local workflows.
- [x] Injection sink identified: generated local-source SQL clauses could previously be expanded by
  clause/separator breakout tokens in raw predicate strings.
- [x] Fix applied for the concrete breakout path: raw predicate strings are now validated against
  statement, clause, set-operation, and separator breakouts while typed predicate expressions remain
  scoped subqueries.
- [x] Regression evidence added: Python query-builder tests reject `UNION`, `ORDER BY`, statement
  separators, and clause breakouts while preserving typed subquery predicates.
- [ ] Deferred gate: public or less-trusted SQL/DataFrame exposure still needs a raw-fragment
  authority policy instead of the current caller-owned local boundary.

Disposition: `deferred_with_owner_and_gate`.

### SL-DEEP-009 and SL-DEEP-010

Rubric:

- [x] Resource sinks identified: SQL local-source and Vortex ingest local-source paths read local
  text inputs before row-limit enforcement.
- [x] Fix applied: shared local-source reads now enforce a `128 MiB` byte budget before buffering or
  digest/materialization work.
- [x] Regression evidence added: Rust unit tests reject oversized regular files through the bounded
  local-source reader.

Disposition: `reportable_fixed`.

### SL-DEEP-011, SL-DEEP-012, and SL-DEEP-013

Rubric:

- [x] Resource sinks identified: object-store local-emulator full/range reads, write/recovery
  object reads, commit-manifest reads, and partition discovery traversal.
- [x] Fix applied: local-emulator objects are capped at `128 MiB`, commit manifests are capped at
  `1 MiB`, partition discovery is capped at depth `16` and `4096` directories, and symlink
  directories are skipped.
- [x] Regression evidence added: object-store runtime tests cover oversized full reads,
  oversized write/recovery objects, and partition-depth rejection.

Disposition: `reportable_fixed`.

### SL-DEEP-014

Rubric:

- [x] Resource sinks identified: SQLite local database reads, table materialization, and JSONL
  export rendering.
- [x] Fix applied: SQLite fixture databases are capped at `128 MiB`, table counts at `50,000` rows,
  and rendered JSONL exports at `128 MiB`.
- [x] Regression evidence added: SQLite runtime tests reject oversized database files and oversized
  table row counts.

Disposition: `reportable_fixed`.

### SL-DEEP-015

Rubric:

- [x] Sensitive sources identified: credential-bearing `s3://`, `gs://`, and ADLS-style URIs in
  object-store read/write/recovery command requests.
- [x] Sink identified: evidence identity fields such as `source_location`, `source_uri`, and
  `target_uri`.
- [x] Fix applied: object-store source/write/recovery evidence now redacts credential-bearing
  authorities and emits redaction status fields.
- [x] Regression evidence added: object-store runtime tests assert credential-bearing read, write,
  and recovery URIs are redacted.

Disposition: `reportable_fixed`.

### SL-DEEP-016

Rubric:

- [x] Sensitive surface identified: broad planning and benchmark evidence can include table, Vortex,
  dataset, and translation target URI-like fields.
- [x] Existing gate reviewed: public benchmark/release claims remain blocked unless selected
  evidence fields are current, classified, and safe for publication.
- [ ] Deferred gate: a repo-wide public-evidence field-classification and redaction pass must run
  before benchmark/release artifacts may claim publication readiness.

Disposition: `deferred_with_owner_and_gate`.

### SL-DEEP-017

Rubric:

- [x] Sensitive source identified: Python client command arguments can contain credential-bearing
  URI values or URI substrings in SQL text.
- [x] Sink identified: `ShardLoomCommandError.command` in command/protocol error paths.
- [x] Fix applied: command arrays are redacted before error construction; userinfo, query, and
  fragment components are stripped from URI arguments and URI substrings.
- [x] Regression evidence added: Python client tests cover command errors and protocol parse errors
  with credential-bearing URI arguments.

Disposition: `reportable_fixed`.

### SL-DEEP-019

Rubric:

- [x] Integrity sink identified: prepared-state reuse file fingerprints, manifest digests, and
  artifact evidence previously used non-cryptographic `fnv64` for admission-critical equality.
- [x] Fix applied: reuse file fingerprints, manifest digests, append-only prefix checks, and Vortex
  artifact evidence now use SHA-256; evidence includes digest-algorithm fields.
- [x] Compatibility boundary: workspace write reports may still carry the older write checksum as
  diagnostic metadata, but prepared-state reuse admission no longer relies on it.
- [x] Regression evidence added: Vortex and CLI reuse tests pass with SHA-256 artifact/reuse
  evidence and still preserve no-fallback behavior.

Disposition: `reportable_fixed`.

### SL-DEEP-021

Rubric:

- [x] Integrity sink identified: release evidence artifact merge copied downloaded CI artifacts
  without binding them to producer names or artifact tree digests.
- [x] Fix applied: merged reports now include producer artifact names, per-file SHA-256 manifests,
  artifact tree digest, copied repo-relative file refs, total bytes, file count, and digest-binding
  status.
- [x] Regression evidence added: release script tests assert digest-binding fields and reject
  symlinked downloaded artifact entries.

Disposition: `reportable_fixed`.

### SL-DEEP-023

Rubric:

- [x] Candidate validated as a claim-boundary issue: local-emulator recovery sidecars prove only
  local consistency unless bound to a producer identity or signed manifest.
- [x] Current claim boundary recorded: local-emulator recovery evidence must not be presented as
  authenticity proof for public recovery claims.
- [ ] Deferred gate: producer-bound or signed recovery-sidecar authenticity is required before a
  public recovery/authenticity claim can be enabled.

Disposition: `deferred_with_owner_and_gate`.

### SL-DEEP-025

Rubric:

- [x] Sensitive sinks identified: release dry-run transcripts and release evidence merge reports
  could include absolute local repository/temp paths.
- [x] Fix applied for release proof surfaces: dry-run transcripts now use repo-relative or
  `external-path:<name>` references and redact repo-root paths from command output; release
  artifact merge reports use repo-relative copied refs.
- [x] Regression evidence added: release script tests assert transcript JSON does not contain the
  temp repo root and command paths are redacted.
- [ ] Deferred gate: broader generated benchmark/status artifacts need the same public-evidence
  field-classification pass before publication readiness can be claimed.

Disposition: `deferred_with_owner_and_gate`.

### SL-DEEP-026

Rubric:

- [x] Candidate validated as an authority-boundary issue: local-source SQL/file APIs intentionally
  read caller-provided local files in the current CLI/Python local workflow.
- [x] Current claim boundary recorded: this is caller-owned local execution, not a less-trusted
  service or multi-tenant API.
- [ ] Deferred gate: public service exposure requires an allow-root/path-authority policy and
  deterministic denial diagnostics for untrusted local file reads.

Disposition: `deferred_with_owner_and_gate`.

## No-Fallback Boundary

The disposition process and fixes must preserve:

- `fallback_attempted=false`
- `external_engine_invoked=false`
- external engines as benchmark/test baselines only
- no package publication or release creation without explicit maintainer approval
