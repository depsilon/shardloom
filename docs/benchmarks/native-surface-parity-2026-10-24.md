<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native user-surface parity acceptance — October 24, 2026

The shared runtime corrections pass complete-result and schema acceptance across
CLI SQL, embedded workflow, Python SQL, DataFrame and typed native expressions.
The strict paired timing screen remains failed for workflow/DataFrame Q34. Its
direction reverses between corrected cohorts, with matching reported work; this
does not establish a persistent frontend penalty or statistical timing equivalence.
The finite investigation stops with that limitation recorded. Full43 does not
replace the failed screen.

## Runtime correction and source

The [architecture record](../architecture/native-user-surface-execution-parity-2026-10-24.md)
describes the scoped corrections under RFC 0044:

- Select the existing native provider from normalized, typed semantics, including
  aliases and predicates already pushed into scans. Reserve actual lowering
  scratch and concrete request metadata, then release scratch after preparation.
- Admit aliased metadata-only counts and pure filtered projections through the
  existing footer-count and resident-projection providers. General native
  execution remains available where range, expression or type equivalence is
  not proved; there is no external-engine fallback.
- Anchor constant grouped keys to required source columns and preserve mixed-key
  order. Avoid unrelated layout inspection in ordinary count evidence.
- Reuse identical selected or hidden HAVING reductions within their query scope.
  Nested subqueries retain their own reductions and semantics.
- Preserve provider-selection reasons, physical read columns and existing work
  counters through public wrappers and benchmark records.

The source baseline is `16ff752d94b399eebc02645d38e58ddbd7ce840a` plus the exact
frozen correction patch and source hashes in the packet. The final patch SHA-256
is `6c51aaba0ea9b7d2cb5b8bf64b0851889de4a8e056e52a1fd22287bb2db52d26`.
The matched release CLI SHA-256 is
`defce76182ddc8a3548a6757eeb2ab02857cf13f2f3bdeb0c54f33d10bb99f41`;
the native library SHA-256 is
`652d74b16d79b6f8bc68288bb9cc44dc21550c513402ba8c283f3830ca1daa52`.
Both use `release-user-surfaces`. No dependency or engine-clock definition changes.

## Frozen focused comparisons

Four completed cohorts each preserve 270 calls: three alternating pairs for
Q1/Q17/Q19/Q20/Q23/Q28/Q29/Q34/Q35 across four frontend comparisons and one
accepted-CLI control. The intervening second correction has fixture/preflight
evidence and no real-input paired cohort. Every completed result passes the
independent reference, and all accepted-CLI controls pass their frozen allowance.
Earlier failed frontend screens and unsuccessful checks remain in the packet.

The unchanged frontend screen requires absolute median difference at most the
larger of 10% of the faster median or 100 microseconds. Accepted-CLI controls
limit candidate regression to the larger of 10% of the control median or
100 microseconds. Resources are 24 GiB and a requested maximum of 12 lanes,
subject to the existing host and native-operation admission policy.

The final correction passes 35 of 36 frontend timing cells. Q34 workflow has
2.936739750 median engine seconds and DataFrame 2.553339792, outside the
0.2553339792-second allowance. Earlier matched Q34 medians are
2.240855500/2.238672083 (pass), then 2.297241875/2.733799875 (DataFrame slower,
fail). No samples are removed or repeated unchanged to obtain a preferred result.

The six final Q34 reports agree on 282 of 298 fields: the same producer/strategy,
URL read set, 99,997,497 rows, 3,120 completed chunks, 31,290,298 partial entries,
9,038,838,007 hashed bytes, nine active workers and no pressure transition.
Timings, peak live memory and entry-credit calls vary. Slow samples on both
interfaces have higher partition-lock waits. This locates shared-runtime
variation without proving its cause. Source inspection confirms the shared
producer and partition reconciliation; it does not justify another
frontend-specific mechanism.

The sealed attribution decision applies the original finite stopping rule.
The one-off completion harness was changed to admit the already requested
five-surface correctness pass while retaining `paired_timing_screen_passed=false`
in its plan and receipt. The standard runner, screen threshold, engine clock,
runtime and measured samples were unchanged. This progression change is part
of the evidence and is not a timing-gate pass.

## Final isolated Full43

Each surface executes every query exactly once, sequentially, with other
task-owned work idle. The standard guarded runner validates complete values,
source generation, native execution evidence and cleanup. Full input hashes
run before and after query execution. Source hashes are unchanged throughout.
The independent reference SHA-256 is
`fc5780da415dc33bd06ed0879db07fc040db5dd419e3c37ecde7cd40a315696b`.

| Surface | Engine seconds | Complete queries | Result rows |
| --- | ---: | ---: | ---: |
| CLI SQL | 48.701809680 | 43 | 381 |
| Embedded workflow | 49.967501000 | 43 | 381 |
| Python SQL | 50.826739087 | 43 | 381 |
| DataFrame | 51.412729914 | 43 | 381 |
| Typed native expressions | 52.287469919 | 43 | 381 |

All 215 results and 1,905 rows match, with equal schemas for all 43 queries and
explicit no-fallback evidence. Each time is the sum of
`engine_execution_seconds` across that surface's 43 calls. Sequential passes
are individual observations with uncontrolled OS cache state; they do not
rank frontend efficiency or prove overall improvement. Earlier observations
keep their original source and clock boundaries.

## Validation and portable replay

The final correction passes 3,246 default workspace tests and 1,444 native CLI
tests (three existing ignores), formatting and strict default/native CLI lint.
The preceding full native workspace passes 6,055 tests (35 existing ignores)
and strict lint. Exact source comparison shows only three CLI Rust files changed
after that native run; the native CLI suite covers those changes, while unchanged
Vortex/runtime code retains its broad proof. No omitted native rerun is described
as a new full-workspace run.

The final matched artifacts also pass 18 public provider tests, 215 independent
small-fixture results, 172 preparation-only declarations, all five adapter
deliveries and three adapter corruption controls. A native integration regression
executes scoped quantified-subquery HAVING cases. Runner, public-reference and
contribution validators pass. Initial failed lint, fixture and regression checks
are preserved with their corrections.

The [manifest](evidence/native-surface-parity-2026-10-24/manifest.json),
[archive](evidence/native-surface-parity-2026-10-24/evidence.tar.xz) and
[verifier](evidence/native-surface-parity-2026-10-24/verify_packet.py) retain exact
source patches, build and input identities, protocols, helpers, every paired
result, final raw output, schema, timing and failure. The verifier checks all
archive bytes, recomputes complete values and timing screens, and rejects a
false timing pass, missing final role or changed fixture value. It executes no
engine or archived helper. Bulk inputs, compiler outputs and executables are
omitted; they remain identifiable and regenerable.

```sh
python3 -B docs/benchmarks/evidence/native-surface-parity-2026-10-24/verify_packet.py \
  --check-current-source --self-test
```

Current-source checking applies to the recorded runtime and Python implementation;
later runtime categories should replay the historical packet without that flag.
The controlled Vortex provider refresh is the next queue category, with a PR
boundary before its implementation. This record closes neither independent CG
gates nor broad SQL/DataFrame parity, optional data/IO breadth, production
readiness, performance superiority or package availability.
