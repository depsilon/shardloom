<!-- SPDX-License-Identifier: Apache-2.0 -->

# FSST active-slice correction evidence

PR [#1692](https://github.com/depsilon/shardloom/pull/1692#discussion_r4579321661)
identified that VarBin slices retain the parent's complete compressed buffer.
The corrected prefilter uses the active first/last offsets after pattern and
filter admission, then reuses those offsets for the original row scan.

[`verification.json`](verification.json) binds the corrected source, original
failing regression, nine passing checks and every member of
[`verification.tar.xz`](verification.tar.xz). The archive contains the complete
check receipts/logs, corrected source, earlier fixture/lint failures and source
variants, unchanged input references and oracle, native diagnostic records,
independent validator and exact supervision helpers. Every archived member was
reopened and verified after packaging. No original performance-packet member
changed.

Acceptance includes 115 provider tests (one existing opt-in ignore), 3,344
default workspace tests, 7,266 native workspace tests (41 declared opt-in
ignores), strict provider/default/native lint and formatting. All checks have
the same source inventory, unchanged sources and drained/reaped processes.
The native build's existing compact-unwind linker warning remains in its log.

The untimed existing-input diagnostic uses the same 54 flat/dictionary/FSST
inputs. All 1,999 records, 864 physical/shared declarations, 378 complete
requests and 378 refund records pass independent validation against 288 nullable
Boolean vectors, including rechecked input identities and population semantics.
It observes 246 active-span rejections and 442 exact matcher constructions;
the original diagnostic observed 236 and 452. Both observe 131,853 row
rejections and 66,435 exact row evaluations. These are logical work counts,
including repeated requests and null rows, and make no timing claim.

The original measured release runtime, installed-wheel results and performance
comparison remain historical evidence in the
[original packet](../native-fsst-consumer-2026-11-15/README.md). This later source
correction has no new scored performance observation or full-provider scratch
accounting claim. Hosted checks and merge status are separate from this local
acceptance. Matcher reuse and the broader queue remain open.

The storage preflight refusal is preserved. Its repair replaced 297 identical
tracked evidence copies in the isolated checkout with independent APFS clones,
after repeated identities and closed-handle checks. Original source files,
benchmark inputs and retained runtimes were untouched; storage limits did not
change.
