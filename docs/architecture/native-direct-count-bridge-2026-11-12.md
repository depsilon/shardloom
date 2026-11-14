<!-- SPDX-License-Identifier: Apache-2.0 -->

# Direct grouped COUNT bridge checkpoint

November 14 follow-up: the corrected COUNT connection is retained within the
[accepted coherent activation unit](../benchmarks/native-optimization-activation-2026-11-14.md)
at `f414d9dd3`, using separately versioned transport/schema declarations. Its
complete 2,543-case COUNT subgroup regresses from 282.761846730 to 284.152322404
engine seconds. There is **no independently accepted COUNT speedup**. The
combined unit passes its prospectively fixed whole-cohort and lifecycle gates.
The original failed baseline below remains invalid and unchanged.

Historical status: **pending; prototype not admitted**, November 12, 2026. The fixed
baseline contains 2,483 passing results and 60 failed results across 2,543
requests. No candidate benchmark query ran. The complete engine total and any
gain/loss comparison are unavailable. Accepted runtime, tests and ignored-test
registration are restored exactly; the prototype and all observations remain
reconstructible from the evidence packet.

This is a stopping checkpoint for investigation B in the
[shared-pipeline program](native-shared-pipeline-investigations-2026-11-10.md),
inside item 20 and RFC 0044. The [phase plan](phased-execution-plan.md#planned)
kept B unchecked at that checkpoint. It does not complete item 19, item 20, RFC 0044 or CG-1
through CG-23, and does not admit the later H/I/J experiments.

## Accepted starting point and fixed scope

PR #1681 closed the preceding join-mapping investigation at
`3a0608b12e40435fab1adc4afefbaa4173932737`, with all 58 PR and 55 exact-main
checks, clear review, verified retirement and post-retirement replay complete.
The accepted compatible-window runtime remains
`shared-window-state-2026-11-11/runtime-r1`. Its build source fingerprint is
`51e9a408564dd45e5507782727348da0204a05142a6cc1d28647abb83ea9285a`.
The accepted repository fingerprint is
`f3b5df1f92c9948a1aa5a3c3ab7655370fec7b807d942c19596ae586a2bfbbd5`:
eight status documents and the independently accepted two-script CI cleanup
correction differ from that older runtime build. These snapshots are not
described as entirely equal.

The cohort was fixed before scoring: all 2,098 original COUNT requests, 160
non-COUNT neighbors selected by original declaration order across existing
family/interface/format buckets, and 285 separately versioned controls.
The latter cross 19 scenarios, five public interfaces and Vortex, Parquet and
Arrow IPC inputs. Profiles follow a fixed Latin rotation. Original requests,
input generations and independent expected results are preserved. An exact
reuse audit admits 159 previous join-mapping baseline observations and requires
2,384 new baseline executions, once each. There are no warmups, repeats,
profiling, cache manipulation or timing-based selection. Other task work and
agents remained idle during query execution.

## Invalid baseline and preserved failures

| Population | Passed | Engine rejected | Engine error | Incorrect | Total |
| --- | ---: | ---: | ---: | ---: | ---: |
| Original controls | 2,258 | 0 | 0 | 0 | 2,258 |
| New versioned controls | 225 | 15 | 30 | 15 | 285 |
| Complete fixed baseline | 2,483 | 15 | 30 | 15 | 2,543 |

Every attempt and complete returned value/schema is retained. Baseline replay
wrote the failed result and then refused its all-pass admission check. That
nonzero verifier result and the original query supervisor receipt remain
separate observations. `engine_execution_seconds` is null for the cohort.
The aggregate `native_no_fallback=false` is an all-pass conjunction in this
failed-cohort report; it is not an observation that an external engine ran.
No complete-cohort no-fallback or performance claim is admitted.

The 60 failures have source-bound explanations:

- Forty-five unsigned controls use the frozen SQL adapter's `UBIGINT` spelling
  through CLI SQL, embedded workflow and Python SQL. The unchanged accepted
  parser admits `uint64`, and these requests fail before the intended native
  work. Typed-plan and DataFrame unsigned controls pass. This does not establish
  that unsigned SQL casts are unsupported.
- Fifteen mixed COUNT/SUM controls declare the SUM output as nullable Int64.
  The unchanged accepted reduction contract returns nullable Float64. Every
  complete numeric value matches exactly within the audited range, but the
  schema differs. Those requests remain incorrect; numeric agreement does not
  repair the frozen declaration.

The original adapters, requests, expected schemas, failed packets and status
counts are immutable. No failed case was removed, silently corrected or rerun.
There is no candidate protocol, scored candidate ledger, complete comparison or
speed rejection. The original failed full10k observation and its 96 failures
also remain unchanged. This checkpoint contains no new Full43 or full10k run.

## Unadmitted prototype and local validation

Feasibility diagnostics use 19 exact requests in both debug and release builds.
They distinguish two existing file-provider paths from shared Aggregate work;
their instrumented attribution does not measure an attainable optimization gain.
The prototype extends the existing Aggregate callback, key ownership, state and
finalizer. It admits one nonnullable integer grouping key and COUNT(*) or a
nonnullable integer COUNT argument under a complete immediate sort including
the unique group key. Per-batch native key reduction carries checked weights
through the existing ordinary and compact states. Unsupported shapes and
optional-capacity refusals keep the existing native path before state mutation.
Spill retains its existing owner. No new external engine, dependency, public
interface or source-format execution family is introduced; Vortex stays native
input and output.

The exact prototype passed formatting, strict default/native lint, 3,323 default
tests and 7,101 native workspace tests, with 40 intentionally ignored diagnostics.
Actual standalone native CI discovery found 4,144 tests and 36 declared ignored
tests across four partitions. Nine new behavior tests cover integer/encoding
boundaries, checked weights, nullability/unique/tiny/empty refusal, sort and source
order, credit denial and surviving children, cancellation, producer failure and
spill cleanup. I32 coverage is at the native array boundary; no public I32 cast
capability is claimed. Fresh release provenance and isolated installed-package
proof passed 441 tests with no skips and successful uninstall. No package was
published.

Candidate debug attribution passes all 19 requests and reduces ordinal/count
updates in 12. It preserves complete values, schema, input contributions, source
consumption and credited lifetimes. Eight diagnostic corruptions are rejected.
These observations do not establish a complete-query speed or memory gain.
Candidate source fingerprint:
`c317190e05e07cf2b824d1fccf70f432a65b809b2ef15b8a1ad13697b52171bf`.

After baseline refusal, exact restoration covered 19 task-owned paths: 15
tracked files restored and four prototype Rust files removed. The accepted
repository fingerprint above was recovered with a clean working tree. All
220 input generations and 12 protected runtime/evidence files stayed unchanged.
The proposal patch and all candidate source bytes remain preserved; patch
reapplication was checked against the restored source. No COUNT implementation,
test or ignored-test registration ships with this checkpoint.

## Evidence and continuation contract

PR #1682's first hosted CI run selected Rust 1.100.0 through the floating
`stable` toolchain. Its stricter Clippy checks rejected unchanged decimal/schema
loops. The separate CI correction pins all 16 toolchain inputs to Rust 1.99.0,
the compiler used for the preserved local validation. Every job, test command,
strict lint command and the Rust 1.96 minimum-version lane remains unchanged.
This pin requires deliberate validation before the next compiler upgrade.
There is no runtime, dependency or benchmark change in this correction.

Fresh hosted installations exposed a second setup requirement: the action's
minimal installation does not include Clippy or rustfmt unless requested.
All 16 setup steps now explicitly install both components. The missing-tool
failures remain separate evidence in `ci-toolchain-components`, alongside the
pinned action source, unchanged-job/command checks and six rejected corruptions.
This completes the compiler setup without changing lint policy or engine code.

The original hosted failure and the pin's local checks are preserved in the
packet's separate `ci-toolchain` supplement. Fourteen CI-plan tests, the gate
matrix and formatting pass; five corruptions challenge the compiler pin,
lint/job preservation and unchanged runtime source. `verify_ci.py` verifies
the current source with this one explicit workflow exception, then replays the
unchanged original packet and all 33 original corruption controls. The original
`verify.py`, acceptance manifest, archives and failed baseline remain byte-exact.
The original source-bound command applies to the first checkpoint commit;
the supplemented command applies after the CI correction.

The [portable packet](../benchmarks/evidence/native-direct-count-bridge-2026-11-12/README.md)
binds original source, validation, diagnostic observations, independent references,
the complete failed baseline, exact reuse, source restoration and proposal patch.
Verification reconstructs saved requests/results without executing a native
query, compiler or external reference engine. It rejects invented complete
timings, altered failed statuses, false candidate execution/admission and B
completion claims. Runtime/input payloads are identity-bound rather than copied
into rollback archives. Final retirement is conditional on portable verification,
exact hosted PR/main checks and preservation of accepted runtime and inputs.

The next chat should start from accepted main and this checkpoint:

1. Introduce a new versioned native SQL transport spelling of `uint64` and a new
   nullable Float64 mixed COUNT/SUM declaration/reference contract. Prove literal
   cases and accepted-source semantics independently before freezing either.
2. Preserve the failed v1 protocol and all 60 original failures. Do not edit old
   references or drop inconvenient cases. Audit exact unchanged requests and
   compatible-runtime reuse before deciding which new observations are needed.
3. Reconstruct the fixed prototype from the archived patch/source bytes, reconcile
   it with accepted main, and bind any required rebuild and validation explicitly.
   Freeze a corrected complete baseline/candidate protocol before new queries.
4. Apply the existing rule only after the full cohort passes correctness,
   resource, ownership and no-fallback checks: strictly lower complete engine
   time retains the candidate; equal or higher time drops it. No minimum gain is
   required. A failed baseline supplies no gain/loss decision.

Complete B's decision and hosted/retirement obligations before advancing H/I/J.
The remaining C–G, H–N and 11–15 investigations stay in their existing queue;
their source research and exposure inventories do not constitute accepted code.
