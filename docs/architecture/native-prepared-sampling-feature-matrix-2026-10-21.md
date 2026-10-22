<!-- SPDX-License-Identifier: Apache-2.0 -->

# Prepared sampling: carry the integration feature-matrix corrections

Before final acceptance, carry three corrections from PR #1580 commit
`9cea50396a69a6f933fe67c391625328316f43c0` into the sampling branch. The PR's
reduced-feature job exposes an output-window method referring to Arrow types
when the corresponding write/format features are absent. The Python core
shard exposes two test modules using top-level imports during package-based
discovery. Neither failure comes from sampling execution.

Gate `VortexRelationalBatchInput::for_each_arrow_batch_with_output_window` on
`vortex-write` and `universal-format-io`, matching its existing adjacent Arrow
method and prepared implementation. Both features are present in the scored
`release-user-surfaces` build. The method body is byte-identical; this changes
reduced-feature compilation without changing the measured feature family's
execution. The two Python test modules use the existing package-relative or
top-level import convention according to `__package__`. Their test bodies are
unchanged.

The previously frozen test-only correction at `e1a8186a` remains a distinct
record. Bind this additional correction as candidate revision 5, preserving
the scored revision 3 source/binary, the failed suite, the revision 4 test-only
bridge and both complete comparison packets. Require an exact diff check for
the single cfg attribute and two import blocks, plus the unchanged compiler
input hashes outside the cyclic test module and gated method's module.
Verify that the original compiler feature set enables both cfg conditions.
No dependency, sampling algorithm, mathematical reference, observer, harness,
workload order, numerical threshold or recorded result changes.

Final acceptance still requires the corrected full native suite, default and
reduced-feature checks, public CLI and installed Python lifecycle tests, the
complete public matrix, Full43 and format/workflow checks. No new timing cohort
is implied by this source bridge, and no retention decision follows before
those gates pass. Historical measurements keep their original identities.
