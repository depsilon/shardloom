# Arrow run-end validation correction

PR #1658's dependency gate reported
[RUSTSEC-2026-0442](https://rustsec.org/advisories/RUSTSEC-2026-0442.html)
for `arrow-data 59.3.1`. The
[upstream advisory](https://github.com/RustSec/advisory-db/blob/main/crates/arrow-data/RUSTSEC-2026-0442.md)
identifies `59.3.2` as the patched release in the existing major version.
The affected validation compared the final run end with the run-end child's
extent rather than the logical parent array's offset plus length.

The root graph updates only `arrow-data`, `arrow-buffer` and `arrow-schema`
from `59.3.1` to `59.3.2`. The latter two satisfy `arrow-data`'s minimum
requirements. Cargo resolves one version of each shared type package; all other
locked packages, Vortex 0.90.0 and the existing IPC/Avro/Parquet/ORC path patches
remain unchanged. No advisory exception, new provider, external query engine,
format admission or publication is introduced.

## Source and legal review

All three exact registry archives match their `Cargo.lock` SHA-256 identities,
and every archived member matches the local source used for the build. Each
declares Apache-2.0 and Rust 1.85. The existing license and notice text files are
byte-identical; the Python notice manifest updates only these three versions
and archive hashes. The complete notice bundle still verifies 434 registry
packages, 13 path patches, 826 legal-file records and 249 distinct texts.

The source review covers all changed production files:

- `arrow-data`: parent-extent run-end validation; union IDs and dense offsets;
  sliced byte-view equality; run-end child slices in equality/extension;
  checked fixed-size-list parent offsets; and fallible run-end null extension.
- `arrow-buffer`: the buffer pointer is updated before resizing a reservation
  can call user code and unwind.
- `arrow-schema`: union IDs are validated during deserialization, iteration
  construction and merging.

These are upstream implementation changes consumed through the existing
provider boundaries. No upstream implementation is copied into ShardLoom.
Union payloads remain unsupported. Existing encoded-input feature admission
and memory, ownership and no-fallback contracts remain in force.

## Verification contract

The original ShardLoom regression
`encoded_arrow_provider_rejects_run_ends_shorter_than_parent_extent`
failed against `59.3.1`: checked construction accepted a parent of length seven
with a final run end of six. The test covers Int16/Int32/Int64 run ends, both
oversized parent length and offset-plus-length, valid slices and empty parents.
It does not invoke an unchecked value read or claim a demonstrated application
exploit.

The patched native workspace suite passes 6,911 tests, including this regression,
with no failures and 36 existing opt-in tests ignored. Strict native lint,
formatting, the dependency/security gate and complete notice verification pass.
The rebuilt CLI and native library pass clean wheel installation, all 312
installed-package integration tests with no skips, identity verification and
uninstall checks. All six required validation/build receipts refer to the same
source and dependency files. The nine rich-column lifecycle tests continue to
check full values and metadata through slices, spill, persistence, reopening,
ownership, cancellation and compatibility boundaries.

The initial [dense-embedding acceptance packet](../release/evidence/native-dense-embedding-lifecycle-2026-11-05.json)
records the earlier dependency graph and remains byte-identical. The separate
[post-update acceptance packet](../release/evidence/native-dense-embedding-arrow-followup-2026-11-05.json)
identifies the corrected lockfile, rebuilt runtime and installed wheel. It
preserves the failed dependency job, failing old-provider regression, corrected
test imports and subsequent validation receipts. Hosted checks and review remain
the integration gate. No performance observation is rerun or reinterpreted.
