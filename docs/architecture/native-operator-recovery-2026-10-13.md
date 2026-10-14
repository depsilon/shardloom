# Native operator recovery acceptance

Status: local runtime and documentation acceptance passed; hosted integration
remains pending. This is a finite hardening unit under PERF-03/06/12 and CG-5/19/21,
following the October 13 streaming
composition work. It does not close those broader gates or change version 0.5.1.

## Contract

The original relational spill cleanup method constructed its own cancellation
token, so an embedding caller could not cancel that cleanup. Current source adds
`VortexRelationalSpillPolicy::cleanup_abandoned_controlled(directory, token)`
and keeps `cleanup_abandoned(directory)` as its compatible convenience wrapper.
Configuration validation remains side-effect-free. A cancelled valid request
must fail before filesystem access. Cancellation during deletion leaves the
ownership marker and all remaining owned files available for an explicit retry
with a fresh token. Cancellation after the last run deletion must also preserve
the marker until a fresh retry. Existing sort and aggregate policies already
carry cancellation and retain their current methods.

The shared query-run store has generic process-death recovery tests, and newer
relational families have actual operator tests. This unit adds the actual-query
coverage for numeric sort, single-key worker COUNT, compound-key COUNT and exact
integer COUNT DISTINCT. Each child runs the admitted public Rust query path
against a parent-owned source and stops only after two real native runs have
been written and validated. A test-only, workspace- and namespace-specific gate
must work across native runtime threads without mutating the parent environment.

The parent verifies that cleanup refuses an active owner without changing any
bytes, kills and reaps the child, verifies that unknown files prevent all cleanup,
then removes only its own control file and invokes the matching public cleanup.
A fresh complete query reproduces every returned value and required ordering
for the existing declared schemas, with native resource, spill, cleanup and
no-fallback evidence. This proves owned
cleanup followed by a fresh restart; it does not resume interrupted execution.

## Vortex-first provider check

- Subject: explicit cancellation and operational acceptance of existing native
  query-run recovery.
- Decision: `wrap_vortex_concept`; preserve the already admitted Vortex 0.85
  native array, Flat layout, writer and bounded run-reader provider paths.
- ShardLoom owns temporary directory admission, namespace/identity checks,
  cancellation, quota accounting and cleanup. Vortex file APIs do not replace
  these query-owner contracts. No new storage representation or provider is added.
- The production change passes the caller's existing cancellation token to the
  existing run store. Process handoff instrumentation is test-only and Unix-only,
  under the existing local-primitives and writer feature gates.
- Native inputs, run payloads and complete result semantics remain unchanged.
  No Arrow execution intermediary or external engine is introduced;
  `fallback_attempted=false` and `external_engine_invoked=false` remain required.

## Acceptance

The final source passes all 35 focused recovery tests, including four actual
operator process-death cases within three parent tests. Explicit cancellation,
inherited cancellation and an expired deadline fail before filesystem access.
Each of the four run namespaces also stops after its first removal with either
one or two initial runs; the marker survives, inherited descriptors preserve the
lock contract, and fresh-token cleanup succeeds. Required formatting and default
workspace Clippy/tests pass, as do release-feature and read-feature Clippy.
The [acceptance report](../benchmarks/native-operator-recovery-2026-10-13.md)
records exact commands, failed initial checks and the finite proof boundary.

- Focused public cleanup cancellation checks, including before filesystem access,
  after one run removal, and before final marker deletion; preserve inherited
  directory-descriptor lock behavior and retry cleanup.
- Actual operator process-death, active-owner refusal, unknown-file preservation,
  public cleanup and exact restart for all four dispatch cases above.
- Required formatting, workspace Clippy/tests and relevant native feature lanes,
  with source/receipt binding and explicit platform exclusions.
- Review, hosted CI and integration evidence before recording this finite unit
  as complete. Current pipeline and generated guide evidence from the preceding
  composition unit retain their own immutable source identity.

Reader/codec allocation coverage, unadmitted state families, compatibility
streaming destinations/fanout, broader platform/operational acceptance and all
conditional investigations remain with their existing owners. Reservations are
not process-RSS limits, and this work makes no performance claim.
