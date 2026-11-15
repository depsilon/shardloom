<!-- SPDX-License-Identifier: Apache-2.0 -->

# Constant-key aggregate experiment evidence

The fixed candidate is **dropped**. Its exact experimental source is preserved
separately at `b6e9d5d5d6bbd00ad48890795b8fe95b27a1d088`; the accepted
scalar-regional implementation remains the baseline. Run `python3 -I -B verify.py`
with Python 3.13 or newer from this directory. The verifier checks both packet
and supplement, replays all 10,028 complete results and 40 lifecycle operations,
and recomputes the frozen decision without loading an engine or executing queries.

The [comparison](../../native-constant-key-aggregates-2026-11-14.md) and
[architecture disposition](../../../architecture/native-constant-key-aggregates-2026-11-14.md)
record the contract and its limits. Complete fresh engine time increases from
556.592642154 to 564.033758513 seconds, so the fixed candidate is rejected despite
the separate prepared-lifecycle improvement from 3.325826500 to 1.998268543 seconds
and all ten passing requested-read witnesses. Every one of the 3,211 fresh
regressions remains. The 810 new constant-group declarations and 350 inherited
regional controls pass their exact coverage/refusal expectations. This is neither
a Full43 nor a full10k result.

`packet/` retains all twelve original archive parts, exact member identities,
standalone verifier, complete raw ledgers, independent references, exact candidate
source and patch, validation logs and failures, and 47 saved-evidence controls.
All 5,762 captured source files were compared with committed blobs. The build
retains its actual `e5f6728a2` parent and dirty-source identity; the preservation
commit is not presented as the build's original revision. Large input, runtime
and environment payloads remain identity-only external artifacts. Portable
verification neither reopens those payloads nor regenerates references.

`supplement/` binds completed packaging, all 36 packet controls, and relocated
verification with original workspace/runtime/evidence paths forbidden. It also
preserves the saved difference attribution and documentation inventory. The first
candidate runtime's CLI evidence-field omission, its failed public checks and
exact source archive remain in the original packet; the corrected runtime was
frozen before this comparison. The pre-execution refusal to reuse an existing
freeze name wrote no replacement and ran no query. No failed observation,
acceptance threshold or timing value was rewritten.

The new fixture subgroup improves while original declarations and controls offset
that gain. Most original-case increase occurs without reported constant-group
activation. This single observation does not establish the cause of those
differences; unchanged identities and guards do not establish controlled cache
or host state. The complete-cohort drop stands without a repeat or narrowed gate.

Broader constant-key support, dictionary-domain work, the remaining queue,
item 19/20 and CG-1 through CG-23 remain open. No PR merge, publication, release
or writer-default change is implied.
