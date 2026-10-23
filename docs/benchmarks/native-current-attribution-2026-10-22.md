<!-- SPDX-License-Identifier: Apache-2.0 -->

# Current native cost attribution — October 22, 2026

Status: **baseline attribution verified; no region or PGO benefit established**.
This completes the finite current-baseline diagnostic prerequisite under
`PERF-01.attribution` and supplies source-level evidence for later
`PERF-13.pgo` intake. It does not complete either owner or admit training.
The [canonical sequence](../architecture/phased-execution-plan.md#competitive-prerequisites-and-four-investigations)
still requires region decisions before the four conditional investigations.

## Bound workload and interpretation

The protocol predates the observations and binds the corrected native runtime
at `a973ad72af847010594311d852da854203d01768`, whose production sources are
unchanged in integration commit `3d07013fb12cf21e6040adb03f9e97e995996875`.
The executable SHA-256 is
`d692fa57c957aab63fdcafc86755a17ef36666e257cb6b9374335af61bdce870`.
It uses the existing 99,997,497-row Vortex input and independent complete-value
reference. These observations precede the scalar-region candidate.

There are sixteen native calls: one counter-control and one sampled call for
each of Q1, Q3, Q7, Q19, Q23, Q29, Q34 and Q41. Each call has complete independent
value validation, source/executable/input identities, engine timing, original
process counters and cleanup evidence. No extra repetition repairs a missing
sample. This diagnostic is separate from the required standard Full43 passes.

The Darwin instruction and cycle snapshots cover the entire native process,
including work outside the engine boundary. Stack samples are observations
across all threads, including blocked threads. The review reconstructs parent
and child ancestry and exclusive leaf residuals; it does not sum inclusive
stacks or convert samples into function instruction counts or CPU percentages.
Sampling can perturb execution, so the two roles do not constitute a speed
comparison or a sampling-overhead acceptance study.

## Source-grounded findings

| Observed path | Evidence and bounded conclusion |
| --- | --- |
| Q3 scalar numeric consumer | The main-thread graph contains `aggregate_direct_numeric_value`, `numeric_input_is_integer`, original-width primitive access and the measure update loop. The source confirms per-row type/value selection. This supports investigating useful scalar consumer work within independently admitted native regions. It does not prove branch misprediction, instruction-cache stalls or a recoverable instruction count. |
| Q23 text accessor preparation | ZSTD reconstruction and native UTF8 preparation occur beneath the serial aggregate accessor constructor. Preparation and aggregate updates are separate source boundaries. A text-preparation candidate would need its own preallocation and retained dictionary/array ownership proof; this observation does not admit it. |
| Q19, Q29 and Q34 controls | Their raw thread trees retain dictionary/interner, transformed-key, provider and waiting work. Each meets the frozen minimum main-thread observation condition. A shared query label is insufficient to assign all process instructions to one operator. |
| Q1 and Q7 metadata controls | Q1 exits before a usable sample; Q7 has an empty call graph. Both have complete result and counter records. Missing stacks do not establish zero work. |

The scalar proposal must preserve metadata answers and original Vortex batches.
Local SUM/AVG reassociation is limited to checked integer ranges that remain
exact in F64; large-integer compensated and floating evaluation policies remain
their existing contracts. Current source supports a prospective opportunity,
not a measured complete-workload gain.

PGO remains a separate investigation. Once earlier region decisions settle,
bind the resulting source and freeze representative training, independent
held-out evaluation, fresh control/instrumented/profile-use builds, portable
package correctness, complete query/ingest/pressure controls and compilation,
training and transfer break-even. Refresh attribution if an accepted change
affects the selected paths. The earlier
[PGO prerequisite review](../architecture/native-pgo-prerequisite-decision-2026-10-20.md)
remains historical evidence; this packet supplies the previously missing
current-baseline source attribution without rewriting that outcome.

## Portable evidence and verification

The [packet](evidence/native-current-attribution-2026-10-22/packet.tar.gz)
preserves 165 members, including original raw outputs, seven sample files,
frozen source excerpts, the protocol, demangling correspondence, review,
independent reconstruction and negative controls. Its
[index](evidence/native-current-attribution-2026-10-22/manifest.json)
records the exact archive and verifier identities. SHA-256:
`8b8bd3b421d2eaa51d2fc2a72e3a7e7dcd46a0eef1958d9eba2a421e45f7b693`.

The [verifier](evidence/native-current-attribution-2026-10-22/verify_attribution.py)
opens a fresh temporary directory and replays saved records without the native
executable, source dataset, compiler or network:

```bash
python3 docs/benchmarks/evidence/native-current-attribution-2026-10-22/verify_attribution.py \
  docs/benchmarks/evidence/native-current-attribution-2026-10-22/packet.tar.gz
```

The [verification receipt](evidence/native-current-attribution-2026-10-22/verification.json)
covers all sixteen values, seven sample files, 15,209 frames, 147 threads and
eight exact source excerpts. The
[fifteen negative controls](evidence/native-current-attribution-2026-10-22/corruption-checks.json)
alter result coverage, query identity, counters, clocks, source excerpts,
raw/demangled symbols, stack counts, observation admission and cleanup. They
repair outer hashes before replay, so rejection exercises semantic checks.
Original unsuccessful and successful diagnostic evidence remains preserved.

No compiler profile, dependency, native output format, public capability or
release is promoted by this report. `fallback_attempted=false` and
`external_engine_invoked=false` remain explicit.
