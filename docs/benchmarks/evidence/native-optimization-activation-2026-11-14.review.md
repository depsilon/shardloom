# Final acceptance review

The measured engine remains `f414d9dd305cc23dfb9e0389a106ba5d7b91d708`, source
fingerprint `504910e4fff98357b1933726687692e1d15b72c250b95df3aadfe1d00404c026`.
The prior runtime/source review is reused for unchanged bytes. The only changed
reviewed helpers are the three explicitly excluded postprocessing helpers;
their final bytes are bound into the portable manifest and exercised by its
complete replay and 50 refusal controls. No query helper or engine source was
changed after protocol freeze.

The main correctness challenge is retained ownership, not just returned rows.
The existing source-generation, mandatory-credit reclamation, cancellation,
late-error, selected-payload validation and last-owner tests remain in the exact
7,215-pass native suite. All 449 installed tests and seven public checks passed.
The writer correction keeps the native I/O scope open through final flush and
managed-object copy; the original installed/CI failures remain in the packet.
The five two-call lifecycles independently compare all 49,733 rows per call,
prove zero first-call cross-call hits, actual second-call hits and fewer completed
reader bytes, and check release of the source/session owner. This does not admit
result caching, join-build caching, effects or unrelated source generations.

The next challenge is overstating the performance result. The predeclared
coherent-unit rule passes both complete totals. The fresh-session gain is only
3.432088618 engine seconds, and 5,189 cases regress. The complete COUNT subgroup,
Parquet, Arrow IPC, CLI SQL and typed-plan subgroups regress. Those facts remain
visible. No individual COUNT, filter or skew speedup is inferred from activation
counters or from the combined total. One observation with uncontrolled OS cache
does not establish statistical significance or competitive superiority.

The r2 baseline passed capture but violated isolation when a read-only task file
inspection overlapped it. Its entire ledger and provenance are preserved and
excluded from scoring. That exclusion and the one r3 replacement were recorded
before consulting its timings. R3 retained every declaration, input, runtime,
reference, resource bound and acceptance condition. No task work overlapped its
four query stages. The portable correction validator rejects missing history,
selection of the disturbed baseline, lost attempt counts and changed policy.

The final transport preserves complete saved results, references, source bytes,
validation and failures. Three redundant historical preparation executables are
represented by their original identities, as are the current executable payloads
and original large inputs. Every split part and the joined archive are checked
before extraction; every regular member is bounded and hashed. Portable replay
rejects engine imports, subprocess execution and disabled assertions. The 50
negative controls include transport, identities, complete results, clocks,
no-fallback/resource evidence, actual mechanisms, lifecycle reuse and both
complete-decision thresholds. No query executes during replay.

Decision: retain the coherent activation candidate and advance its accepted
comparison baseline after the final receipt verifier succeeds. Preserve the
remaining coverage gaps, item 19, item 20, RFC 0044 and CG-1 through CG-23. The
next independent work unit is per-measure metadata completion. PR checks on
the matching engine publication head `81743f243` passed 58/58; later evidence
commits have their own remote check boundary. This review grants no merge,
release, package-publication or review-thread-resolution authority.
