<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native temporal fidelity acceptance

Temporal units, presentation zones and fixed durations now survive the shared
native workflow and its public input/output boundaries. Extreme timestamp
compression preserves the full signed-64-bit storage domain. Calendar operations
use explicit timezone, ambiguity and month-end policies; their calendar range
remains narrower than raw timestamp transport.

[PR #1616](https://github.com/depsilon/shardloom/pull/1616) merged at `51cf54fd`
after all **57 hosted checks** passed on `34d7ee14`. The
[hosted receipt](evidence/native-temporal-fidelity-2026-10-27/hosted.json) records
every check, including native partitions, provider groups, Windows controls,
Linux/macOS installed wheels and Rust CodeQL. The whitespace-cast review finding
is corrected and tested through all five interfaces; CodeQL's fixture-log
findings are corrected without weakening value equality tests.

This is a completed work unit within
[item 20](../architecture/native-analytical-completion-2026-10-27.md#temporal-fidelity-implementation-contract).
As-of/calendar-window/resampling workflows, numeric semantics, recursive
composition and application batch functions retain their open obligations.
No package was published, no benchmark was rerun for this acceptance, and no
performance or CG completion claim follows.

## Scope and validation

The native representation preserves Date64, time-of-day, all four timestamp
and fixed-duration units, and timestamp presentation zones. Exact conversions
refuse discarded precision or overflow. Raw timestamp/duration storage retains
its full tick domain; invalid date64/time values and unknown metadata fail
explicitly. Jiff's pinned bundled timezone rules support the declared calendar
operations without consulting the host timezone database. All five interfaces
lower to the same native owners, with Vortex-native persistence and explicit
Arrow/Parquet translation. No external execution engine is introduced.

The DateTimeParts provider correction widens day counts to i64 while keeping
seconds/subseconds i32. Its serialized child metadata and decoder already admit
that width. The uncompressed day buffer grows by four bytes per row; compression
still narrows observed values. This is distinct from the earlier comparison
patch retired when upstream incorporated that work.

| Gate | Receipt | Result |
| --- | --- | --- |
| Default workspace, all targets | `r52` | 3,274 passed; no failures or ignores |
| Native workspace, `release-user-surfaces`, `ci-native` | `r63` | 6,301 passed; 35 existing ignores |
| DateTimeParts provider | `r59`, `r61` | 89 tests and strict Clippy passed |
| Default/native strict Clippy | `r49`, `r51` | Passed |
| Catalog and ADBC parameter controls | `r62` | Six tests passed |
| Final checked optimized CLI/library build | `r64`, `r65` | Passed and hash-frozen |
| Rebuilt public temporal matrix | `r66` | 22 tests; no failures, errors or skips |
| Installed wheel, isolated load and uninstall | `r68` | 199 integration tests; no failures, errors or skips |
| Original compact verification and corruption controls | `r75`, `r76` | Passed; 17 corruptions refused |
| Checkout-relative verifier and isolated source-only proof | `r78`–`r80` | Passed; 24 corruption controls, including assertions-disabled execution |

Required workspace commands are `cargo fmt --all -- --check`,
`cargo clippy --workspace --all-targets -- -D warnings` and
`cargo test --workspace --all-targets`. Recorded commands add locked/offline
resolution and bounded test concurrency. Native checks additionally enable
`release-user-surfaces`; native workspace tests use `--profile ci-native`.
Formatting passes locally and in hosted CI.

The public matrix covers all five interfaces, Vortex/Parquet/Arrow-file readback,
full-width ticks, both daylight-saving transitions, nullable nested parents,
declared Python rows/batches, ADBC parameters, retained output ownership and
failed-output cleanup. The 35 local native ignores comprise 32 existing explicit
measurement/fixture helpers and three non-Unix controls covered by Windows CI.
Earlier successful gates keep their actual source heads and dirty inventories;
the packet binds the final native-test overlay to the clean final build rather
than relabeling earlier receipts.

## Evidence and independent verification

The [manifest](evidence/native-temporal-fidelity-2026-10-27/manifest.json) and
[acceptance record](evidence/native-temporal-fidelity-2026-10-27/acceptance.json)
retain 67 original supervised observations, including 28 unsuccessful attempts,
their complete logs, source/runtime identities and package proof. Failed
intermediate checks remain failures. The compact packet has 539 logical files,
39,511,517 expanded bytes and a 1,348,692-byte archive. Its SHA-256 is
`b596e28365db1343d16583094fb0764dfa81c65b6c63339e6d8d34606c12b39a`.

From a checkout with Python 3.10 or newer, run:

```sh
python3 -B scripts/verify_native_temporal_fidelity_packet.py docs/benchmarks/evidence/native-temporal-fidelity-2026-10-27
python3 -B -m unittest discover -s scripts -p test_native_temporal_fidelity_packet.py -v
```

The verifier uses bounded content-addressed archive reading, never executes
archived code and never opens the historical machine paths as live inputs.
It checks gate results and process cleanup, log-derived Rust test counts,
public coverage/runtime binding, installed-package identity/lifecycle, source
binding and preservation of every original observation. The derived acceptance
must equal the saved record. Seven tests contain a positive replay and 24
corruption controls. They also pass under isolated Python with `-O`, so disabling
Python assertions does not bypass verification. The isolated copy contains
only source and compact evidence, without datasets or runtime binaries.

[Supplemental verification](evidence/native-temporal-fidelity-2026-10-27/verification/manifest.json)
retains packet-construction attempts, the pre-deletion refusal, cleanup receipts,
and subsequent verification. Hashes establish internal consistency, not signed
provenance. Large runtime and wheel payloads are represented by identity; this
packet neither reconstructs them nor proves every host configuration.

## Retention and cleanup

The [cleanup record](evidence/native-temporal-fidelity-2026-10-27/cleanup.json)
retires 11,823 generated regular files and six environment links after exact
identity and closed-handle checks. The retained r5 CLI, native library and
installed-tested wheel remain, together with current inputs and compact
success/failure evidence. Earlier source snapshots, temporary environments,
package-build copies and superseded runtimes are retired. User files, external
baselines and the Cargo cache remain untouched; no storage limit changes or
rollback payload backups occur. Verification succeeds after retirement.
