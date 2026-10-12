# Local artifact cleanup — October 11, 2026

The maintainer requested more local disk space during full-domain timestamp
acceptance. The completed native Vortex all-target regression was allowed to
finish before cleanup; it passed on `00cf1492b1c73246daf6f0940276466554136e32`.
Its process group was drained and its receipt/log hashes were checked.

Only `/Users/dylan/.cache/shardloom/cargo-target/debug` was selected. A fresh
inventory checked regular-file identities, rejected symlinks and hardlinks,
and hashed all 35,409 selected files. Cargo and cleanup locks, workload exclusion,
open-handle checks and the existing storage guards protected the operation.

| Disposition | Files | Logical bytes | Allocated bytes before cleanup |
| --- | ---: | ---: | ---: |
| Regenerate compiler `.o`, `.rlib`, `.rmeta` intermediates when needed | 8,404 | 19,212,347,856 | 19,226,230,784 |
| Preserve all other debug files in a verified archive | 27,005 | 19,882,050,500 | 19,976,986,624 |

The intermediate retirement supplied headroom for archiving. Every remaining
file, including executable tests and dynamic libraries, was copied into a
4,989,680,005-byte ZIP. Every archived member was reopened and checked against its
original size and SHA-256 before any corresponding original was removed.
The embedded manifest preserves original paths, file modes, identities and hashes.
The current passing Vortex test executable was independently reopened and checked
again after cleanup. Empty debug subdirectories were removed; the Cargo lock file
remained. Future compiler intermediates require a rebuild.

The archive is local-only evidence at
`/Users/dylan/LocalData/shardloom/native-python-surface-20261010/debug-cache-preserved-files-314.zip`.
Its SHA-256 is
`fe14fe173f40a9bc59915f4f2f2cff4017f4b08c41fbdcfc5a8c0989f4e1ac3c`.
The same directory retains `debug-cache-cleanup-plan-314.json`,
`debug-cache-cleanup-receipt-314.json`, the executed
`compact_debug_cache_314.py`, and the bounded supervisor's completion receipt.
The plan SHA-256 is
`d624a32cd3d17335da7cdab2316c152c88ac82ae687d597c55a96007fd432f1b`.

Observed free space rose from 14,564,405,248 to 48,164,319,232 bytes: approximately
33.60 GB. Selected allocated bytes minus archive allocation account for
34,203,037,696 bytes. These are different measures; concurrent filesystem activity
can affect the free-space observation. They are storage observations, not engine
performance measurements.

The release cache, target-root registry evidence, frozen release comparison
binaries, resident datasets, original failure receipts/logs and unrelated source
changes were preserved. Twelve protected files were hash-checked before and after,
including the frozen CLI/binding, successful/failed timestamp observations and the
maintainer's unrelated local test edit. No cloud files were removed or hydrated.
The 12-GiB free-space floor, 128-MiB reservation, 100-GiB workspace ceiling and
192-MiB log ceiling were unchanged.

Subsequent local debug validation explicitly uses `CARGO_PROFILE_DEV_DEBUG=0`
and `CARGO_PROFILE_TEST_DEBUG=0` to reduce regenerated debug-symbol storage.
Release artifacts keep the existing release profile. The already completed native
regression retains its original build profile and archived executable identity.

## Cleanup before the requested fresh full UAT

The later cleanup follows the maintainer's requested order: recover storage,
run fresh full UAT, then open the timestamp PR before new runtime work.
Completed historical logs were compacted and one exact superseded generated
payload was retired. Source inputs, current references, failed/incomplete runs
and unrelated files remain preserved.

The October 10 Full43 cohort contributes 516 original files; seven completed
September 30 attribution calls contribute another 42. All 558 originals were
hashed, copied and reopened in two compressed archives before removal. The
archives retain 23,669,156 bytes of original JSON/companion content; obsolete
gzip wrappers are represented by their original hashes. Nine summary or
attribution records remain separately preserved. An independent verifier checks
all 558 members and all nine records. This reduced accounted UAT-log storage
from 268,181,504 to 261,607,424 bytes, recovering 6,574,080 bytes under the unchanged
252-MiB admission threshold. The storage-failed count-selection history was
excluded from compaction and remains intact.

The exact retired payload was
`/Users/dylan/LocalData/shardloom/clickbench-100m-uat/vortex/release-040-20261005.vortex`,
with SHA-256
`cda1452a82a450063e89abd64ea99ccbfcaedf1269b92896bd7b8c5645a5053a`.
Its original ingest command, executable identity, successful local packet and
independent inspection remain available as regeneration evidence. The retained
October 9 replacement is
`/Users/dylan/LocalData/shardloom/clickbench-100m-uat/vortex/release-050-prebump-20261009-a2.vortex`,
with SHA-256
`c541453175a8d947f8ab8073db91cca79e7988ba9a7e143be881e5c750e1c72f`.
These files have different hashes; the retained artifact is not claimed to be
an identical physical replay. Both artifacts and the resident Parquet source
were completely hashed, checked for stable identity/residency, and checked for
open handles as applicable before the exact old output was removed.

The first cleanup attempt stopped without deleting anything because it treated
the source's macOS `UF_TRACKED` document-identity flag as a residency failure.
The corrected helper distinguishes that flag from `SF_DATALESS`, preserves the
first helper and failed receipt, and still rejects dataless or underallocated
source files. The successful attempt recovers 15,713,611,776 accounted bytes;
observed free space rises from 25,079,365,632 to 40,793,051,136 bytes. No storage
limit was raised.

The local evidence directory retains
`timestamp-full43-compaction-326.json`,
`timestamp-compaction-verification-326.json`,
`timestamp-superseded-uat-cleanup-326.json` and
`timestamp-superseded-uat-cleanup-327.json`, with their executed helpers and
guard receipts. The independently verified source/measurement archive preserves
them together with the final acceptance evidence, linked from the
[timestamp report](../benchmarks/native-timestamp-provider-2026-10-11.md#preserved-evidence-and-replay).

After the current retained-file Full43 and independent public-packet inspection
passed, its 516 original query artifacts were also compacted with unchanged
summary bytes and a complete member manifest. Every original JSON/companion
byte was verified before removal and reopened afterward. The archive retains
20,504,324 original content bytes and reduces accounted log storage from
265,699,328 to 262,377,472 bytes, recovering 3,321,856 bytes before fresh-ingest
admission. The 252-MiB admission and 256-MiB running-log ceilings are unchanged.

The supervisor and this compaction initially selected the same receipt filename,
so the supervisor replaced the detailed receipt after successful verification.
The manifest, archive, summary and verification log remained intact. The exact
444,794-byte detailed receipt was recovered from those retained records and
accepted only after matching its original SHA-256,
`7b10017acebbe46ebeb15788ed9208cfd50102ab44165e2a8faeec6220400b8a`.
All 516 members passed re-verification. The separately preserved supervisor,
recovery procedure and receipt are bound into the fresh-UAT provenance; no
native result was regenerated or altered by this repair.
