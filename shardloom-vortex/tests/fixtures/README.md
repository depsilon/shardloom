# Vortex Metadata/Footer Fixtures

## `metadata_footer_u64_20000.vortex`

- Purpose: feature-gated local metadata/footer open coverage and approved local
  encoded `CountAll` correctness evidence.
- Provenance: generated locally with upstream Apache-2.0 `vortex` crate version `0.70.0`.
- Data shape: one non-nullable `u64` primitive array with 20,000 deterministic pseudo-random values.
- Expected metadata: `row_count=20000`.
- Expected local encoded count: `count=20000`.
- Correctness manifest: `CorrectnessValidationPlan::default_foundation_plan`
  declares fixture id `vortex-metadata-footer-u64-20000` with
  `ExpectedOutcome::MetadataRowCount { row_count: 20000 }` and fixture id
  `vortex-local-encoded-count-u64-20000` with
  `ExpectedOutcome::EncodedCount { count: 20000 }`.
- Scope: tests may open the file and inspect footer metadata under
  `vortex-file-io`; tests may also run the approved feature-gated local
  `.vortex` array-length `CountAll` proof under `vortex-encoded-read-spike`.
- Not allowed: generalized scan/read-start approval, non-local or object-store
  reads, encoded predicates, projections, row reads, requested
  decode/materialization, Arrow conversion, writes, spill IO, external baseline
  execution, or fallback execution.

