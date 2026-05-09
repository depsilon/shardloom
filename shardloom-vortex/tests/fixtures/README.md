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

## `local_primitive_struct_five.vortex`

- Purpose: feature-gated local primitive scan-pushdown coverage for
  `count-where`, `project`, and `filter-project` correctness/certificate
  evidence.
- Provenance: generated locally with upstream Apache-2.0 `vortex` crate version
  `0.70.0` using the ignored
  `regenerate_checked_in_local_primitive_struct_fixture` test helper.
- Data shape: one non-nullable struct array with fields `value: u32` and
  `metric: i64`.
- Values:
  - `value`: `1, 2, 3, 4, 5`
  - `metric`: `10, 20, 30, 40, 50`
- Expected local primitive outputs:
  - `count_all` => `count=5`
  - `count-where:gte:value:3` => `row_count=3`
  - `project:metric` => `row_count=5`
  - `filter-project:gte:value:3|metric` => `row_count=3`
- Correctness manifest: `CorrectnessValidationPlan::default_foundation_plan`
  declares fixture ids `vortex-local-count-all-struct-five`,
  `vortex-local-count-where-struct-five`, `vortex-local-project-struct-five`, and
  `vortex-local-filter-project-struct-five` with `ExpectedOutcome::Rows`
  or `ExpectedOutcome::EncodedCount` counts matching the outputs above.
- Scope: tests and feature-gated local CLI smoke runs may open the file and run
  upstream Vortex scan filter/projection pushdown through ShardLoom's local
  primitive path.
- Not allowed: production SQL/operator certification, non-local or object-store
  reads, generalized adapter execution, row reads, requested
  decode/materialization, Arrow conversion, writes, spill IO, external baseline
  execution, or fallback execution.

