<!-- SPDX-License-Identifier: Apache-2.0 -->

# Per-measure metadata experiment evidence

The candidate is **dropped** and the accepted source is restored. Run
`python3 -I -B verify.py` with Python 3.13 or newer from this directory. The
verifier checks the complete packet and closeout supplement, replays all 1,274
complete results and 40 lifecycle operations, and recomputes the frozen
decision without executing any query or loading an engine.

The [comparison](../../native-metadata-measures-2026-11-14.md) and
[architecture disposition](../../../architecture/native-metadata-measure-completion-2026-11-14.md)
explain both failed gates. Complete fresh engine time increased from
11.827542699 to 11.858516037 seconds, and none of the ten required large-input
controls demonstrated fewer requested-read bytes. The prepared lifecycle total
improved, but does not override those gates. All 278 fresh regressions remain.

`packet/` contains the original seven archive parts, its exact member manifest
and its standalone verifier. It preserves complete raw ledgers, independent
references, exact experimental sources and patch, validation logs and failures,
27 saved-evidence negative controls, and all new source/provider observations.
Large data, environment and executable payloads remain identity-only external
artifacts; replay does not reopen them or regenerate the references.

`supplement/` binds the completed packaging and 33 packet controls, including
relocated replay with original paths forbidden, the correction to the
temporary-cleanup audit, retirement of its verified disposable extraction, and
restoration of accepted commit `046488dc3`. The experimental commit
`6f9650473` remains preserved separately. The first packaging failure and
original control-driver failure are retained, not rewritten as successes.

The original replay status `incomplete_or_invalid_evidence` and decision
`not_admitted` remain unchanged. The final experiment disposition is `drop`.
This evidence does not close regional/constant-key work, broader optimization
coverage, item 19/20 or competitive gates, and does not authorize publication.
