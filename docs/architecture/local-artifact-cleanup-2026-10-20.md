# Global local-artifact cleanup — October 20, 2026

The maintainer requested cleanup across all ShardLoom generated storage and
retention of compact results and current artifacts, without rollback backups.
The [retention policy](local-development-storage.md#current-retention-policy)
now applies across investigations, release verification and worktree outputs.

The cleanup retires 86,309 generated files across the shared Cargo cache,
historical executables and wheels, 72 completed workflow-output directories,
temporary package environments, older worktree build/package outputs, generated
fixtures and 32 obsolete archives. It also retires the superseded
`persisted-native-343-a1.vortex`. The original Parquet and latest accepted
`fresh-roadmap-663-a1.vortex` remain available.

The retired files account for 119,829,762,048 bytes. A further 297 large result
records are stored as verified gzip: 7,820,690,048 original bytes become
1,067,268,862 compressed bytes. Their complete original result bytes are retained.
Observed free space increases by 125,396,385,792 bytes during the global cleanup.
Accounted file bytes and observed free space are different measurements;
hardlinks, filesystem allocation and host activity affect their relationship.
The preceding ingest-only retirement is recorded separately and is not added to
this observed increase.

Exact selected paths, closed handles and stable file identities are checked
before deletion. One executable pass stops when deleting a hardlink changes
another selected name's timestamp. The corrected continuation admits only that
known, self-caused change after verifying identical content. Focused tests cover
that case, unauthorized/content changes and directory-symlink preservation.
The stopped pass and its continuation remain recorded separately.

Final verification confirms all selected deletions, every compressed-result
hash, unchanged current runtime hashes, and the active ingest source/helper
freeze. The retained CLI passes its version check. Forty-six compact CI result
files are retained from obsolete hosted bundles before those bundles are removed.
Git source/evidence, ordinary project files, distinct resident source inputs and
current fixture tools remain. No storage ceiling changes or rollback archives
are introduced. This cleanup does not add benchmark or investigation acceptance.

The [compact cleanup result](../benchmarks/evidence/local-artifact-cleanup-2026-10-20.json)
records the completed scopes and current retained identities. Historical paths
in older reports describe their original observations; retired generated payloads
require regeneration if needed again.
