# Local Development Storage

## Scope

Bulk generated files must remain outside cloud-synced folders. A checkout in
Documents does not make its ignored build files exempt from iCloud syncing.
This is an operational safety rule, not a query performance optimization.

On this Mac, the local-only paths configured on 2026-09-05 are:

- Cargo outputs: `/Users/dylan/.cache/shardloom/cargo-target`.
- ClickBench workspace: `/Users/dylan/LocalData/shardloom/clickbench-100m-uat`.
- Resident official source: the workspace's `sources/hits.parquet`.

The existing Cargo output directory and resident source were moved, not copied.
The source retained its 14,779,976,446-byte length and allocated block count.
The historical Desktop evidence and cloud-only Vortex artifact were not moved,
downloaded, deleted, or represented as newly validated benchmark evidence.
Relocation does not itself reduce local disk use or repair an existing iCloud queue.

The source checkout remains at its current configured project path. Its local
`.cargo/config.toml` sets `build.target-dir`; `.git/info/exclude` keeps that
machine-specific setting out of commits. Explicit Cargo environment or command-line
overrides take precedence, so agents must verify the resolved output path.
For complete development-folder isolation, relocate the checkout itself to an
unsynced directory in a separate, coordinated project-path migration.

## Ingest Guard

`scripts/run_clickbench_ingest_uat.sh` defaults to the local-only workspace and
resolves the compiled binary through Cargo metadata unless `--binary` is supplied.
`--uat-root` also relocates the default source and target.

Before creating logs or replacing an artifact, the runner:

1. Rejects macOS Desktop, Documents, Mobile Documents, CloudStorage, and CloudDocs
   paths, including existing symlink aliases. This conservatively rejects Desktop
   and Documents even when their current sync setting is unknown or disabled.
2. Requires output and logs to stay inside the declared UAT workspace.
3. Checks existing workspace bytes plus a candidate reservation against 100 GiB.
4. Reserves the configured maximum artifact size, interpreted conservatively as
   GiB for admission, plus 12 GiB of free disk headroom.
5. Rejects more than 256 MiB of accumulated logs.
6. Takes an exclusive workspace lock before running the writer. A stale lock after
   a crash requires inspection; it is not deleted automatically.

The checks run again at each progress sample and after child completion.
Budget failure stops the native CLI and produces a nonzero result; a child that
ignores normal termination is forcibly stopped after a short grace period.
Interrupting the harness also stops the native process group and releases the
owned lock. A small supervisor records process duration independently of the
watchdog polling interval; `native_process_seconds` is the comparison clock,
while `elapsed_seconds` includes polling and harness overhead.
`native_peak_rss_bytes`, when present, is the OS-reported child-process high-water
mark, with macOS byte and Linux KiB units normalized to bytes. It is measured by
the single-child supervisor after exit, not estimated from progress samples and
not an enforced process-memory limit. Earlier records without this measurement
must not be treated as having zero peak memory.
Source residency checks still happen before removal of an existing artifact.
`--replace-existing` removes only the exact requested target. It does not delete
backups, source files, numbered copies, or unknown staging files. Existing staging
files remain visible to artifact/workspace accounting and require explicit review.

The limits can be set explicitly with `--min-free-gib`,
`--max-workspace-gib`, and `--max-log-mib`, or the corresponding
`SHARDLOOM_CLICKBENCH_UAT_*` environment variables. The existing artifact limit
and runtime limit also remain in effect. The workspace count uses the larger of
logical and allocated file bytes, counts hardlinks once, and does not follow
arbitrary directory symlinks.

These are sampled watchdog ceilings, not an APFS quota or an allocator guarantee.
Writes can overshoot between samples. They do not impose a limit on iCloud,
other applications, other workspaces, or commands that bypass the runner.
No automated deletion of old runs, cloud files, or user data is performed.

## Verification

```sh
python3 -B -m unittest discover -s scripts -p test_local_uat_storage.py -v
bash -n scripts/run_clickbench_ingest_uat.sh
```

Tests cover safe destinations, macOS synced paths, symlink/case bypasses, sparse
files, hardlinks, free space, candidate reservations, workspace and log budgets,
root override behavior, preservation on failed preflight, runaway log output,
exact-target replacement, backup/source preservation, and concurrent-run exclusion.
Fixtures are small and require no engine build.

Every full-size run must remain blocked until its storage admission passes.
Do not infer that the existing CloudDocs backlog is disposable from these checks.

## Provider References

- [Cargo build-cache location configuration](https://doc.rust-lang.org/stable/cargo/reference/build-cache.html).
- [Apple iCloud Drive file management](https://support.apple.com/en-ie/guide/mac-help/-mchl1a02d711/mac).
