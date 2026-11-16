<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native optimization propagation: useful behavior retained

The shared dictionary, grouped COUNT and operation-scoped FSST changes are
retained for their verified behavior under the maintainer's direction to keep
useful work, fix the demonstrated propagation gap and prepare a coherent PR.
This is an integration judgment within that instruction, **not a performance
admission**. The original comparison remains **not admitted**; the focused
selection comparison remains **drop**. Neither protocol nor threshold changed.
The [integration record](evidence/native-optimization-propagation-2026-11-15/integration-disposition.json)
keeps this distinction explicit.

## Complete observations

All times below sum `engine_execution_seconds`. Fresh requests and prepared
operations are separate populations, each observed once per declared execution.

| Comparison | Population per runtime | Baseline seconds | Candidate seconds | Frozen decision |
| --- | ---: | ---: | ---: | --- |
| Original combined propagation | 11,579 fresh requests | 894.235685490 | 894.646518681 | Not admitted |
| Original prepared lifecycles | 540 operations | 0.516182452 | 0.521095814 | Not admitted |
| Focused selected dictionary | 2,190 fresh requests | 99.601052609 | 100.161598045 | Drop |
| Focused prepared lifecycles | 540 operations | 0.521095814 | 0.513496733 | Drop: fresh gate fails |

The original population contains 5,309 exact original-full10k declarations and
6,270 mechanism cases. It is not the full10k with only additions and cannot be
compared directly with the earlier 1,563.106319749-second full10k total. Complete
answers, schemas, order, native execution and resource evidence pass in the
original comparison, but 60 required selected-dictionary work witnesses fail.
Both complete engine totals also increase. Those failures remain unchanged.

The focused population is selected before measurement from all 1,380 dictionary
cases, all 525 dictionary propagation cases and every other saved case exposing
dictionary work: 205 original and 80 text-signature requests. It includes all
60 original misses and nearby flat, unique, empty, all-null, direct and UNION ALL
controls. The unchanged 270 two-call prepared plans supply 540 lifecycle
operations. Exact request, runtime, input, reference, resource and clock proof
permits reuse of the saved `3cec5a410` candidate as this baseline. Only the new
candidate's 2,190 fresh requests and 540 prepared operations execute.

All 120 required dictionary-retention and 80 kernel-reduction obligations now
pass, including 40 new non-null selection reductions and 20 formerly missing
all-null retention witnesses. The existing 40 achieved UNION ALL reductions are
preserved. All 120 grouped COUNT projection witnesses passed in the original
comparison; its COUNT implementation is unchanged by the focused correction.
Finite fixture eligibility is not a whole-engine opportunity denominator.

The focused fresh total increases by 0.560545436 seconds. Its 605 individual
regressions, totaling 1.803038520 positive-difference seconds, remain recorded.
The [complete cost inventory](evidence/native-optimization-propagation-2026-11-15/cost-inventory.md)
reconciles every pair and includes all 81 original subgroup summaries:

| Complete partition by request kind | Cases | Baseline seconds | Candidate seconds |
| --- | ---: | ---: | ---: |
| Original declarations | 205 | 53.575320586 | 53.329504455 |
| Dictionary controls | 1,380 | 2.193434837 | 2.177555234 |
| Dictionary propagation | 525 | 43.692967725 | 44.517480398 |
| Text-signature controls | 80 | 0.139329461 | 0.137057958 |

Within the propagation population, the 175 selected shapes decrease from
16.602609091 to 16.506721701 seconds; the 175 UNION ALL shapes increase from
19.641866087 to 20.642355652 seconds. The 30 scale all-null cases increase by
0.697960041 seconds. Shape and profile groups overlap and their differences
must not be added. These observations do not isolate the cause of an increase
or prove repeatability. They remain open cost evidence, not grounds to discard
controls or reinterpret the failed complete-cohort gate.

## Shared implementation and ownership

The existing set/alignment gather copies referenced UTF8 dictionary values into
a detached compact receiving domain, remaps original-width codes and preserves
order, duplicates, nullability and credits. At demanded values-only text
preparation, a noncontiguous selection over that dictionary composes checked
host codes instead of mistaking its inner row count for the value-domain size.
An immutable native UTF8-validation receipt permits reuse of the already checked
compact views. Complete inner-code and outer-index bounds still apply. Hash
preparation and unknown/deferred representations keep their existing behavior.

The pinned Vortex dictionary representation is reused. Its infallible fixed-width
take allocation does not meet the new buffer's host-budget contract, so code
construction uses the existing fallible typed result builder. The prepared key
retains its opaque source owner; final receiving outputs retain their own credits.
The first broad native suite caught enlarged shared key records exhausting an
unchanged temporal-window budget. Moving the lifetime reference inside existing
values-only state fixes that regression without raising the budget. The original
failure, exact failed source and same-budget passing regression remain archived.

The grouped COUNT binder traces sort keys through proven column-only rename,
reorder and duplicate projections in tree and compact forms. Computed columns,
filters, ranges and observation barriers remain in place; spill keeps its
separate ordered owner. The FSST change retains at most one credited immutable
exact table per admitted operation, keyed by complete symbol contents, lengths,
literal and options. Per-backing rejection, offsets, validity and results are
never cached. The session lock is released before lazy provider evaluation.

No public dispatch shortcut, new dependency, unsafe code, external execution
fallback, writer default or spill strategy is added. Vortex remains native input,
execution representation and output. These paths do not imply zero decode,
universal propagation, or an independent FSST/COUNT speedup.

## Source and validation

The original comparison binds accepted slice-corrected `f326e6328` to combined
candidate `3cec5a410`. Its planned matcher-only comparison was superseded before
execution by the combined protocol. The focused runtime is built from clean
`75596d0ee32e59ba77ea7cf4429166cb3052b3de`, source fingerprint
`c01bccc4c62d14b29c34741b6907cd00284feb74cc4b69555758e5c9eb58baa1`.
All 5,807 source members are retained and verified. Later report/evidence changes
are not substituted for the measured binary identity.

Final source-bound checks pass:

- Workspace formatting, strict default/native Clippy and all-feature checks.
- Default workspace: 3,344 passed, no failures or ignores.
- Native workspace: 7,288 passed, no failures; 38 opt-in diagnostics/fixtures
  and three non-Unix checks are explicitly ignored on this macOS host.
- VarBinView provider: 99 passed and one intentional ignore, plus strict lint.
  Unchanged FSST provider source retains its 127-pass/one-ignore check and
  native ownership/dispatch proof with explicit source identity bindings.
- License/provenance checks and a fresh release-user-surfaces build.
- Isolated installed wheel: 455 passed, no failures, errors or skips; uninstall
  verification passes. No package is published.

The new query pass is isolated from builds, packaging and other task work,
with the existing deadlines, storage limits, lightweight supervisor and cleanup.
Full hashing happens outside query execution. Host/cache state is uncontrolled.
The original comparison's small status-inspection disturbances remain described
in its disposition; no zero-disturbance or per-change causal claim is made.

## Portable evidence and remaining work

The [two-part packet](evidence/native-optimization-propagation-2026-11-15/README.md)
replays all 23,158 original fresh results and 1,080 original prepared operations,
reconstructs the exact focused baseline subset, and checks the 2,190 new fresh
results and 540 new prepared operations. Its relocated verifier forbids original
path reads, native-engine imports, subprocesses and network access. The
[complete result](evidence/native-optimization-propagation-2026-11-15/portable-result.json)
reproduces both failed performance decisions with zero query executions.

The archive preserves admission, test, packaging and verifier failures. The first
focused portable replay catches a full-identity versus content-only schema
mismatch. Its repair compares the complete recorded identities. The first
relocation also changes only filesystem change-time on 16 companion archive
files through hard links; a separate proof verifies their unchanged bytes,
inode, size and modification time, while every other frozen identity stays exact.
The repaired relocation uses independent copies. Five identity controls and
nine archive-rebinding controls pass, alongside 21 delta-transport controls,
32 focused evidence controls and the original 37 packet controls.

Verified retirement of 13,084 idle regenerable debug files restores archive
headroom. Independent/repeated inventories and three closed-handle checks
preserve current inputs, compared runtimes, tested wheels, release cache and
all failed observations. The 12-GiB floor remains unchanged; no query is repeated
for packaging or cleanup. Native binaries, input payloads and execution
environments are explicit identity-only exclusions from portable replay.

The next development unit maps the saved resident-join requests to their exact
inputs, then attributes input, key, index, payload and downstream consumption.
The 2,697 join/multiway requests contain 500.383376588 seconds of complete-query
time across 174 groups; existing counters do not assign that time to exclusive
join phases. Ordered spill, broader source-demand/encoding coverage, required
item-19 spatial work and the remaining item-20/RFC-0044/CG-1 through CG-23
obligations remain open. Future investigations use affected-owner populations
and nearby controls; this closeout authorizes no new full10k, Full43 or release.
