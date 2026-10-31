# Final source review of PR 1627

Reviewed head: `7d6c02c6d2358e205fb60f6868830d905ab4b62d`.
This is a reasoned source review, not another execution or timed observation.

1. Numeric ordering must not erase original integer precision, floating policy,
   null placement or stable ties. `native_relational_sort_numeric.rs` binds each
   key's original physical width, declines cross-batch type changes, uses the
   existing normalized analytical float identity and null comparator, and breaks
   equal keys by the original global ordinal. It borrows the already prepared
   owners; payload validation and gathering remain in the ordinary sort owner.
   The existing numeric ordering tests include mixed widths, integer endpoints,
   extended floating values, stable ties, offset, low memory, errors and lifecycle.

2. Shared extrema must not change the first failing measure or replace an
   extended-domain success with a proof of finite inputs. The adjacent-measure
   matcher requires matching plain argument/type domains and known physical
   validity before sharing COUNT. Typed pairing binds the exact owner/policy
   completion proof, checks count headroom for every existing group and permits
   no allocation or future validity execution after admission. Strict and
   extended proofs remain distinct. Existing proof/state tests cover rejected
   mismatches and fault ordering. No new error-order defect found.

3. Optional ordering metadata and retained output must remain accounted for.
   Borrowed key views and K row locations are admitted through existing memory
   leases before allocation; insufficient optional headroom keeps the ordinary
   path. Temporary storage drops before its credit, before payload gathering.
   The join follow-through introduces tests/measurements only; no borrowed
   selected-row production handoff is admitted. Existing final native/default,
   public and installed-package checks remain the execution proof.

No actionable source defect was found in this pass. This review does not claim
that whole-query copying or individual predicate phase times were measured.
