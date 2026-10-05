<!-- SPDX-License-Identifier: Apache-2.0 -->

# Full43 comparison after nested review repairs

Fresh acceptance of candidate `3b94ba2e` passes 2,459 public workflow checks and
all 258 paired Full43 retained-result comparisons. Aggregate query timing remains
effectively unchanged at the predeclared investigation thresholds. Q21's initial
median RSS increase triggers a reversed-order repeat; the increase does not
reproduce. Both observations remain in the evidence. This report supplements the
[original nested acceptance](native-nested-full43-2026-10-03.md), which remains
unchanged, and makes no causal speedup or official benchmark claim.

## Candidate and procedure

The review repairs preserve authoritative source schemas during Python preparation,
retain nested root nullability for empty imports, preserve nested validity through
the pinned Vortex field-writer boundary, and report actual UTF8 payload-copy counts
for JSON/JSONL output. Buffered, streamed and budgeted nested intake now shares
complete type/value regressions across four existing writer profiles. Scalar text
codec selection remains covered separately.

- Candidate: clean revision `3b94ba2e1fdc7d1860398265856c44aa02a28f74`, optimized
  `release-user-surfaces`, Rust 1.99.0. Executable SHA-256:
  `ac4b6d0c49a0875953d057c45aaf1675905c727d8b4566bc59bbcc0a0717d2db`.
- Control: retained `e1133f6981833b461e1dd6a131385a61285c6468`, executable SHA-256
  `7cfffec4f65ab2df146c2d567186d7bdc8b5a2c94abd29ec9aebdd9498cfdceb`.
- Dataset: the existing 99,997,497-row ClickBench hits Vortex artifact,
  15,682,956,489 bytes, SHA-256
  `5a6e7d7b521b891e522f32398ef2582367e814791ab537f354003ecd2f7a488d`.
  All 43 statements and retained references are unchanged; no full-size input
  was regenerated.
- Host: Apple M5, arm64, 10 logical CPUs, 16 GiB physical RAM, macOS 27.0.
  Both binaries request a 24-GiB operation policy and maximum parallelism 12.
  This policy is distinct from physical RAM and measured RSS; it is not a
  total-process memory ceiling.
- Full cohort: `paired43_20261003T044858669361Z`. Each query runs three times
  per binary, alternating order. The native clock includes process startup,
  complete result output and exit. Hashing, reference validation, host snapshots
  and archive work are outside that clock. Supervised cohort wall time is
  393.787741 seconds; sums below are not end-to-end elapsed time.
- Serial workload and storage guards remain enabled. The source was hashed at
  freeze; OS cache and ordinary host activity remain uncontrolled. Observed
  one-minute load averages span 1.504–13.048. There is no answer cache or forced
  cache purge, and native builds/tests did not overlap the timed cohorts.
- Every complete result matches the retained ShardLoom reference under the
  existing exact-structure and `1e-12` finite-float comparison. This is regression
  evidence, not a fresh external correctness oracle. Successful calls retain
  no-fallback and no-external-engine certificates.

## Full cohort and required repeat

| Metric | Control | Candidate | Candidate change |
| --- | ---: | ---: | ---: |
| Sum of each query's fastest valid run | 51.112827 s | 51.254897 s | +0.2780% |
| Sum of each query's median | 52.334261 s | 51.905438 s | −0.8194% |
| Sum of all 129 native process times per role | 157.124243 s | 156.172407 s | −0.6058% |
| Geometric mean of fastest valid query times | 0.541868 s | 0.544103 s | +0.4124% |
| Maximum observed native-process peak RSS | 5,010,391,040 bytes | 5,047,681,024 bytes | +0.7443% |
| Complete retained results | 129/129 | 129/129 | All pass |

No timing screen crosses its threshold. Q21's median RSS rises from
1,427,161,088 to 1,575,895,040 bytes, a 148,733,952-byte increase (+10.4217%).
This crosses both the 10% and 32-MiB thresholds, so Q21 is repeated three times
per role with reversed order in `paired43_20261003T045627330640Z`.

| Q21 repeat metric | Control | Candidate | Candidate change |
| --- | ---: | ---: | ---: |
| Median native time | 0.723773 s | 0.747958 s | +0.024185 s; +3.3416% |
| Median peak RSS | 1,592,377,344 bytes | 1,469,136,896 bytes | −123,240,448 bytes; −7.7394% |
| Complete retained results | 3/3 | 3/3 | All pass |

The repeat takes 7.083626 supervised seconds and crosses no investigation
threshold. The memory change reverses direction; these observations do not
establish a consistent memory regression. They also do not establish a causal
memory improvement. The full cohort is not rescored with the later samples.

Thresholds remain: both 10% and 100 ms for median query time, both 10% and 32 MiB
for median query RSS, and both 5% and one second for either aggregate timing sum.
All observations, including smaller differences and CPU measurements, are retained.

## Public workflows, gates and evidence

The fresh public matrix passes 2,459 checks, including 496 nested checks,
6,636,187 complete row comparisons and 5,585 verified execution envelopes.
The evidence verifier also checks 96 expected denials without success artifacts
and 166 inert inspections. Complete nested output covers 65,541 rows through six
destinations; repeated explode writes all 131,082 flattened rows through all eight
local writers.

The correctness-suite wall clock is 395.043939 seconds, versus 202.308304 seconds
for the earlier matrix. These unpaired suite clocks include Python orchestration,
storage-tree scans, format readback, validation and compression; they do not
isolate engine time. The difference remains visible without assigning a cause or
using it as a comparable query-performance score.

Default workspace tests pass 3,446; native Vortex passes 2,227 with 23 existing
ignored tests; native CLI passes 1,577. Python passes 718 with 144 existing skips.
Configuration counts overlap. All 24 selected local gates pass. Required runtime
source hashes match the frozen build exactly. Earlier harness/consumer unit
receipts are retained only after proving their twelve test/helper/supervisor
sources unchanged. The repaired runtime receives the fresh public and paired runs.

The [immutable review acceptance packet](evidence/native-nested-composition-review-2026-10-03.json.xz)
has SHA-256
`59260176b250dc1c80f0a218fe911ab16aeeb244e800aed5428af3b4cfe1955f`.
It verifies all archived query outputs and envelope bytes, frozen source and
reference identities, regression failures and repairs, and process/lock cleanup.
The original acceptance packet remains unchanged.

| Evidence | SHA-256 |
| --- | --- |
| Fresh public summary | `b35008d82797ca38cc7aec0e6a1a44f700bdb2cec7b89b7776f7793ece2c9b51` |
| Full43 summary | `d1e16ee7b190914e79ba2c06b6a4e48ab88136455d3346b11d74dc32ce504e3c` |
| Full43 comparison | `fe77e6211535a424fc21ca345259413e7975afec8717bbdf81544e8fd08dab69` |
| Q21 repeat summary | `4dce3927478c3623017c3a11256cdb8d4a99a232f30d7b9ac1def1a4ea97b8e8` |
| Q21 repeat comparison | `e8c9a1681a26372f266ed2ba863840ba7dc4dc15ffe019a56baff300603889d8` |

Replay uses `scripts/run_clickbench_paired_query_uat.py` with these frozen binaries,
input, query file and references, `--memory-gb 24 --max-parallelism 12 --timeout 120`,
the existing serial guard and a fresh run directory. The follow-up adds
`--query-ids 21 --reverse-order`. Preserve all samples and storage ceilings.

Hosted acceptance remains blocked on the separately documented
[website dependency advisory](../dependencies/website-build-dependency-review.md).
Its proposed exception is disabled pending explicit maintainer approval; passing
exception-policy tests do not mean the vulnerable dependency is fixed. Dynamic
pivot, richer type/key/state semantics, wider adapters and resource/spill
obligations remain open. No package or release is published by this acceptance.

## October 4 website dependency follow-up

The [dependency update](../dependencies/website-build-dependency-review.md#2026-10-04-registry-update)
selects `http-cache-semantics` 4.3.0 and passes the standard dependency audit.
The unused exception proposal is removed. Earlier website-blocker statements
in this report describe its original frozen revision; they no longer identify
the current dependency posture. Hosted runtime review and checks remain separate.
The recorded benchmark results, source identities and immutable packets are unchanged.
