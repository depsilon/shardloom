<!-- SPDX-License-Identifier: Apache-2.0 -->

# Rich bounded retention candidate acceptance

The rich bounded-retention candidate extends the existing streamed ordering
prefix to validate nested, variable-width and decimal payloads before discarding
rows, while retaining only the bounded owned prefix. The candidate is retained
for the existing item-20 work queue. Its runtime, held-out correctness,
collection, installed-package and portable replay results pass the recorded local
gates, including 22 corruption controls. Hosted checks remain pending. This
does not complete item 20 or establish platform-wide runtime coverage.

The candidate follows the [item-20 rich-retention contract](../architecture/native-analytical-completion-2026-10-27.md#rich-bounded-retention-candidate-contract): the 16,384-row cap, four-cap pending-batch bound, no-spill admission, stable ordering and independently owned output remain. Validation-only traversal validates complete payloads without copying discarded values. Retained-payload copy-byte totals exclude separately built ordering keys, provider scratch, metadata and final delivery.

## Frozen comparison

The fresh release baseline and candidate each ran the same 347 cases once,
validating all 12,565 result rows. These are engine-execution times.

| Role | Engine seconds | Comparison |
| --- | ---: | --- |
| Fresh release baseline | 28.118399295 | Reference |
| Rich bounded-retention candidate | 27.374316659 | 2.646248% lower total time; 10.364811% lower in the declared projection family |

The candidate passes the declared-family benefit gate and has no material case
or family regression flags. The fresh baseline replay, compared separately with
the earlier 28.017558877-second checkpoint, retains flags for
`temporal_timestamp_hour` and `temporal_timestamp_second`. Those flags belong to
that baseline comparison; they are not candidate regressions, and their causes
are not isolated here. Each role has one observation, so these figures do not
establish statistical confidence or isolated causal attribution.

On the rich and wide projection activation cases, copied rows fell from 1,000,000
to 15,159. The corresponding retained-payload buffer-copy counts were 1,185,336
and 8,678,283 bytes. These counters exclude separately built keys, provider
scratch and result delivery; they are not total memory or RSS measurements.

## Correctness and validation

Held-out correctness passed 176 observations across eight fresh seed,
null/skew-profile and shuffled/clustered-layout cohorts. Complete outputs
contained 624,906 rows, with 262,168 full-input read-back rows. The held-out set
covers decimal, list, struct, variable-width text and temporal payloads, several
operator compositions, zero demand, small and large limits, and a full-sort
control. It is a correctness and activation check, not a second timed benefit
cohort.

| Gate | Result |
| --- | --- |
| Native Vortex suite | 3,433 passed; 32 existing measurement/fixture tests ignored |
| Default workspace suite | 3,274 passed; zero ignored |
| Native CLI suite | 1,032 passed |
| Public five-interface cases | 30 cases |
| Installed native Python wheel | 200 integration tests passed; zero failures, errors or skips |

The local public five-interface and installed-wheel evidence was collected on
macOS. The [CI workflow](../../.github/workflows/ci.yml) schedules these native Python integration tests on Linux and macOS;
the Windows compatibility lane builds and checks its separate CLI surface.
This local packet does not establish hosted-platform or Windows native-Python
runtime behavior.
Initial public-test setup errors involving the native-library filename suffix
and the `offset` keyword were corrected. The only Rust source change between
the native lint/Vortex gates and final build clarifies which key buffers the
copy-byte counter includes. The verifier reconstructs that exact earlier comment
and matches its recorded source hash. The seven failed intermediate attempts
(`r04`, `r06`, `r07`, `r09`, `r18`, `r27`, `r28`) remain preserved in the packet.

Portable packet-control attempt `r38` recorded two test failures caused by
test assumptions about which integrity check rejects altered evidence first.
The corrected `r39` run passes all 23 tests: one complete replay plus 22
corruption controls. Both attempts remain recorded.
The complete replay and controls also pass from an isolated source-only copy,
with Python assertions disabled for the controls. Documentation and CI-contract
checks pass. The release architecture tracker continues to report its 76 open
phase entries; its existing CI mode accepts that development status without
allowing release claims.

## Evidence and limits

The portable evidence is stored under
[`evidence/native-rich-bounded-retention-2026-10-28/`](evidence/native-rich-bounded-retention-2026-10-28/).
Its [manifest](evidence/native-rich-bounded-retention-2026-10-28/manifest.json),
[acceptance receipt](evidence/native-rich-bounded-retention-2026-10-28/acceptance.json)
and [content-addressed archive](evidence/native-rich-bounded-retention-2026-10-28/packet.tar.xz)
link the frozen protocol, complete comparisons, held-out outputs, test receipts
and source/runtime identities. Large Parquet inputs, binaries and the tested
wheel remain represented by their identities. Replay reads complete saved
result values and schemas without those payloads, an engine, PyArrow or archived
Python execution. Hashes establish internal consistency, not signed provenance.

From a checkout with Python 3.10 or newer:

```sh
python3 -B scripts/verify_rich_bounded_retention_packet.py docs/benchmarks/evidence/native-rich-bounded-retention-2026-10-28
python3 -B -m unittest discover -s scripts -p test_rich_bounded_retention_packet.py -v
```

[Supplemental verification](evidence/native-rich-bounded-retention-2026-10-28/verification/manifest.json)
preserves packet construction, both corruption-control attempts, isolated replay,
static checks and cleanup. The [retirement receipt](evidence/native-rich-bounded-retention-2026-10-28/cleanup.json)
records 25,974 generated regular files and six environment links removed after
identity and closed-handle checks. An initial cleanup refusal preserves all 870
small process-ID records in the compressed cleanup inventory before deletion.
Replay passes after retirement; the current tested runtime/wheel, five original
Parquet inputs, 347 original references, earlier packet and compact proof remain.

Hosted checks remain pending. No package release, new
Polars observation, Full43 run or statistical-confidence claim is included.
Item 20 remains open, including its broader analytical and performance work.
