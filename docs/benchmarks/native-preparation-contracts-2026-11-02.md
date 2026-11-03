<!-- SPDX-License-Identifier: Apache-2.0 -->

# Preparation contracts: complete-workflow acceptance

Status: local acceptance passed; hosted integration pending. The maintainer
retains the A/B/C batch under the frozen lower-combined-engine-time rule and
explicitly accepts the higher query total. D/E and further source-strategy
mechanisms remain deferred under their documented proof requirements.

## Observation and decision

One fresh preparation/publication is followed by one isolated Full43 pass on
its newly created Vortex artifact. Each query executes exactly once. All 43
complete results match the independent reference, including all 381 rows;
source, runtime and input identities remain unchanged and fallback is false.
The release runtime uses source `d8c433fe8951acacd8785df50b5c21836835c575`
with `release-user-surfaces` and the existing resource limits.

| Engine stage | Retained baseline, seconds | Candidate, seconds | Change, seconds |
| --- | ---: | ---: | ---: |
| Native creation | 36.945 | 38.936 | +1.991 |
| Prepared-state publication | 6.695 | 0.085 | -6.610 |
| First Full43 | 40.592222712 | 44.580467538 | +3.988244826 |
| Complete workflow | 84.232222712 | 83.601467538 | -0.630755174 |

The decision is **retain** because the complete workflow is lower after the
correctness, resource, ownership and no-fallback gates pass. There is no minimum
percentage gain. This historical comparison includes intervening runtime changes;
it does not isolate A/B/C, establish repeatability or claim a query-only speedup.
No baseline rerun, extra candidate trial, cache manipulation or artificial pacing
was added. Hashing and archival remain outside query execution.

Saved-result inspection finds 37 queries higher and six lower. The largest
increases are Q35 (+0.686965583), Q34 (+0.511213833), Q19 (+0.405649708),
Q9 (+0.367500042) and Q17 (+0.344855665). Q9/Q34/Q35 retain their prior
state families, projections, submitted chunk counts and maximum source chunk
sizes; Q34/Q35 retain nine peak active workers. Q19 selects the intended helper
and retains its specialized worker but is also higher. These observations do
not isolate the cause of the distributed query-time increase.

## Observed mechanisms and limits

- **Committed checksum proof:** immediate publication consumes the opaque
  writer receipt with checksum source `committed_writer_streaming_sha256` and
  zero payload bytes read for fingerprinting. Later independent hashing still
  occurs. Writer-byte identity is not an independent storage readback.
- **Sparse text frame indices:** 14,084 successful selected Zstd packing calls
  cover 2,799,929,916 valid values and 45,371,320,844 length-prefixed payload
  bytes. They request 342,244 frame offsets occupying 2,737,952 native `usize`
  bytes. These are cumulative completed-call extents, excluding failed calls
  and other codec scratch; they are not peak memory or individually timed savings.
- **Derived minute continuity:** Q19 scans `UserID`,
  `__shardloom_derived_extract_minute_EventTime` and `SearchPhrase`. Raw
  `EventTime` is absent from the scan and numeric accessor inputs. The
  `numeric_minute_string` family remains active with 1,212 submitted chunks.
  Per-column payload-read/decode savings are not inferred from these fields.

The output contains 99,997,497 rows and 112 columns in 15,732,915,000 bytes,
SHA-256 `d66b59055e76e1b47a75638810151b73e71535246aa731bbeea49956304ab8eb`.
The changed helper nullability intentionally changes the artifact metadata;
whole-file identity with the old artifact is not asserted. The provider's
unchanged frame bytes and metadata are separately checked against the original
packing representation. Ingestion validates every source row, and Full43 checks
complete independent query results; no independent all-cell readback is claimed.

The [investigation](../architecture/native-preparation-query-investigations-2026-11-02.md)
records ownership, null/error and persistence controls, D/E's missing proofs and
the report-only receipt cleanup. The
[source strategy probe](../architecture/native-source-strategy-investigation-2026-11-02.md)
is a separate parent-runtime observation: it supplies no automatic preparation
threshold or universal source default. The existing 347-case denominator remains
unchanged.

## Validation and evidence

Workspace formatting and strict default/enabled Clippy pass. Default Rust tests
pass 3,318; enabled Rust tests pass 6,875 with 36 existing opt-in ignores. The
patched Zstd provider passes separate formatting, Clippy and all 96 library
tests. Generated API/public-status checks, license-bundle validation, seven
notice tests and fifteen API-reference tests pass. The clean release build and
all 303 installed integration tests pass without skips, errors or failures;
binary identity, load and uninstall checks pass.

The [acceptance manifest](evidence/native-preparation-contracts-2026-11-02/acceptance.json)
and [verification archive](evidence/native-preparation-contracts-2026-11-02/verification.tar.xz)
preserve the frozen protocol and helpers, compiled-source snapshot and runtime
identities, installed-package report, ingest and query reports, all selected
query outputs, independent reference, baseline summary and saved comparison,
investigations, cleanup and 35 preparation receipts. Ten failed receipts remain;
the early `io720` failure has no log and is recorded as an evidence gap. No failed
or interrupted attempt is counted as a complete pass.

All 389 archive members pass complete byte readback and comparison with their
source files. The 5,660,196-byte archive contains 28,486,282 uncompressed evidence
bytes and excludes data inputs, prepared payloads, binaries, wheels and build
environments. This packages the already validated observations; archive integrity
is not a portable semantic replay verifier. Exact commands and identity-bound
inputs are retained for reproducibility. Hosted acceptance and the subsequent
documentation/version work remain separate from this local decision.
