# Selected payload validation: required text correction

This addendum is recorded before the candidate's public correctness or timing
execution. It leaves the original 347-case workload, saved baseline, resources,
one-pass rule and benefit thresholds unchanged.

The additional malformed-text regression exposed a defect in the existing
zero-retention scalar path. A selected canonical text view may lack a completed
UTF-8 validation receipt. The provider's convenience scalar accessor assumes
valid text and can produce an unchecked string owner. Retained construction
eventually validates the output bytes; zero-retention construction has no
output text and therefore missed that check. The failure also occurs with the
candidate's nullable typed-validation exception removed. The isolated provider
reproducer preserves the selected mask and demonstrates that the checked text
accessor rejects row 2 while the convenience scalar accessor accepts it.

The correction uses the existing checked native text accessor at the shared
result scalar boundary before creating a string owner. It checks row validity
first and does not inspect missing parent positions. The local Vortex 0.90.0
accessor preserves the established detailed UTF-8 error and selected index.
It retains the checked-construction receipt optimization for valid immutable
views. No dependency, external execution, new representation or unsafe code is
added. Classification: `use_vortex_native_provider` within the existing native
feature and certificate boundary.

UTF-8 leaves with any missing parent positions and observed values continue on
their original coordinate-preserving scalar path, including nullable text.
All-missing text can compact to an empty domain. Other admitted leaves and
direct decimal validation remain subject to the original protocol. Both copied
and discarded payloads must reject observed malformed text, while hidden text
must remain unobserved. Native and provider regression tests, source-linked
lint/builds and the same complete public acceptance are required before any
candidate retention decision.

Preserve failed focused attempts e175b through e175e, their intervening source
inventories, and provider reproducer receipts e175f/e175g. The initial suspicion
that nullable typed validation alone caused the disagreement was disproved by
e175e. The combined candidate includes this correctness repair; timing cannot
isolate its cost from the two prospective optimization mechanisms. No timing
result or item-20 completion is asserted here.

The e175h build was explicitly stopped with its process group drained before
replacing an error-only `expect_err` with a fallible conversion that obeys the
provider's stricter lint policy. That interrupted build supplies no test proof;
the final source requires a new passing focused test and provider checks.
