# Executable block recipes — C2.a

Status: retain the bounded numeric recipe under PERF-INTAKE / RFC 0044 after
complete public-call measurement, Full43 UAT and semantic/resource validation.
R8's evidence merged in PR #1466. R5.b reservation-owned buffer transfer is next.

Prepared aggregates already retain logical lowering and source identity. Native
accessors bind the current array owner, physical width and validity per block;
scalar numeric loops already dispatch outside their row loop. Reimplementing
those mechanisms or sharing mutable aggregate state is unnecessary.

The complete saved R9.b result envelopes include UTF8 timing omitted from the
flattened CLI fields. Q29's fastest candidate call records 6,250,928,539 ns in
the first-pass accessor span, including 2,475,530,654 ns of provider execution
and 3,771,199,276 ns of dictionary construction. The remaining 4,198,609 ns is
an inclusive remainder, not a pure binding timer or a bound on row-update cost.
The saved source is `paired43_20260926T234401225793Z/q29_completed.tar.xz`, member
`q29_run3_candidate.stdout.json`. This does not establish a cross-block accessor
cache opportunity.

The [baseline extraction](../benchmarks/recipe-route-attribution-2026-09-27.json)
preserves all three candidate runs for Q10/Q23/Q29, plus all 12 Q34/Q35
control/candidate route records. Its raw archive includes the original complete
envelopes, manifest, identities and extraction script. This closes the route
evidence archival gap raised in PR #1466; no new timing is claimed by extraction.

One distinct repeated operation remains in mixed grouped exact-DISTINCT updates:
the existing pair-preunion loop dispatches ordinary COUNT/SUM/AVG measures and
numeric physical types for each row. The retained block-local recipe binds
the current typed slice and validity once, then updates the existing per-group
states in original row/measure order. Keep exact pair preunion, group lookup,
DISTINCT state, merging, sorting and output unchanged.

Initial admission is COUNT(*) and identity SUM/AVG over retained native numeric
owners, without offsets; all other shapes retain the existing native update.
Bind afresh for every block. A recipe borrows that block's owners and cannot
escape or reuse another dictionary, validity mask or source generation. No cache,
JIT, new dependency, unsafe code, result reuse or external execution is introduced.

Vortex-first decision: use the existing Vortex 0.85 PrimitiveArray and native
validity provider through ShardLoom's current accessor boundary. The recipe only
specializes ShardLoom's aggregate consumer; it does not replace Vortex decoding
or claim zero decode. Fresh mutable state, resource admission and source checks
retain their current scope.

## Complete-query evidence

The [retention packet](../benchmarks/executable-block-recipes-2026-09-27.json)
preserves every sample, complete result envelope, source/binary identity,
command, source patch and validation log in a compressed archive. The frozen
candidate is `3dee8fc2150424b08fbe2dce54609b7fe28b2b8f`, executable SHA-256
`d6893ba09fd787576c01639727783609608d3d7ac4357557f6c7fe8ed2e1c707`.
The control is `5218557d7d776e95ad14b5eb18ab464711c1021c`, executable SHA-256
`646252db5d98e20537cf6d35bb0b2bc87708103d93368d6727053a765697f4de`;
it is byte-identical to the retained R9.b runtime. R8 changed evidence/tests.

Q10 groups RegionID and computes SUM(AdvEngineID), COUNT(*),
AVG(ResolutionWidth) and exact COUNT(DISTINCT UserID). The candidate activates
the recipe for all 1,550 blocks and 99,997,497 rows. Exact pair preunion still
elides 78,074,526 duplicate DISTINCT contributions while retaining every row's
ordinary measures. Both binaries return the same complete values.

| Counterbalanced experiment | Control best of three | Candidate best of three | Reduction |
| --- | ---: | ---: | ---: |
| Initial full-size Q10 screen | 6.437325 s | 5.256797 s | 18.34% |
| Q10 in paired Full43 | 7.385731 s | 5.732583 s | 22.38% |

All three initial pairs improve, by 1.118511 / 1.277277 / 1.180528 s. The
Full43 Q10 pairs are +0.571556 / +0.089785 / -2.595020 s candidate-minus-control;
its median is also lower (7.957287 versus 8.327603 s). Every observation remains
in the record; these are measured complete calls, not a claim that every sample
or workload improves by the best-run percentage. The clock includes process
creation, complete public CLI output and exit. OS caches and host workload are
uncontrolled, as accepted for this local candidate train.

All 258 Full43 calls pass complete-value checks and final identity validation.
Their sums of per-query best-of-three times are 83.054817 s control and
81.794268 s candidate. This is suite regression evidence, not an additional
attributed suite-wide speedup. Q9 alone crosses the recorded regression flag
(both 10% and 150 ms slower); its unchanged partitioned DISTINCT route does
not activate the recipe. The first counterbalanced follow-up passes all six
outputs, with best times 1.570290 s control and 1.748525 s candidate, still
crossing the flag. A final counterbalanced block passes all six outputs, with
best times 1.091533 s control and 1.098552 s candidate: 7.019 ms / 0.64% slower,
below the flag. The original flag and both follow-ups remain in the packet;
neither their results nor their samples replace the original run.

The pilot, Full43 and follow-ups preserve **276 complete outputs** against
retained native references. This is not an independent query oracle; independent
literal expected values in the focused tests supplement it. No new ingest or
storage measurement is claimed: this change modifies neither writer nor format.

## Correctness and review

Nine focused tests cover integer widths including values above 2^53, f32/f64,
nulls/empty blocks, fresh owners and validity, bounds, overflow/nonfinite errors,
rejected shapes, complete grouped DISTINCT multiplicities and partial mutations
when a later measure fails. Rejected shapes are checked before kernel allocation.
The recipe borrows current owners and allocates only its small kernel list;
it introduces no new retained data buffer or memory-reduction claim.

Required format and workspace Clippy checks pass; all 3,425 workspace tests pass.
Strict native-feature all-target Clippy and the write-only feature check pass.
The final native-feature all-target run passes 1,963 tests with 17 intentionally
ignored fixtures. The initial native run had one failure in the existing
`cancelled_recovery_preserves_marker_and_can_be_retried` assertion; that test
passed in isolation and the unchanged complete rerun passed. Its cause is not
established, and the first failed log is preserved in the packet.

Independent source review found no correctness blocker. Its allocation-preflight
and later-failure coverage requests were addressed. Independent archive review
verified the earlier attribution packet's 23 members, nine timing extractions and
12 Q34/Q35 route assertions. The native no-fallback, exact-error and Vortex owner
boundaries remain intact. This closes C2.a's bounded decision, not broad prepared
execution, memory, spill or serving obligations.
