# Vortex Metadata/Footer Fixtures

## `metadata_footer_u64_20000.vortex`

- Purpose: feature-gated local metadata/footer open coverage only.
- Provenance: generated locally with upstream Apache-2.0 `vortex` crate version `0.70.0`.
- Data shape: one non-nullable `u64` primitive array with 20,000 deterministic pseudo-random values.
- Expected metadata: `row_count=20000`.
- Scope: tests may open the file and inspect footer metadata under `vortex-file-io`.
- Not allowed: scan/read-start, encoded-data traversal, row reads, decode/materialization, Arrow conversion, object-store IO, writes, or fallback execution.

