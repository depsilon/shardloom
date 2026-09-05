# Lazy query layout metadata candidate

Normal native queries now report footer row/segment counts, file-statistics
availability, schema, and root layout without realizing unprojected layout
children. The pinned Vortex 0.85 `DynLayout::slot`/`children` interface lazily
constructs child metadata; a diagnostic inventory previously forced that work
for the entire file before a projected scan could begin.

Vortex-first provider decision: use the existing native `Footer`, root layout,
file pruning and projection-aware scanner. Query execution, source generation
validation, native buffers and output behavior are unchanged. No external engine
or answer cache participates. The independent physical-encoding inspection
example remains a complete, explicit inspection with its existing limits.

`local_primitive_layout_encoding_inventory` now contains
`deferred_until_explicit_layout_inspection` during ordinary execution. The
additive `local_primitive_layout_inventory_scope` and
`local_primitive_layout_inventory_nodes_inspected` fields identify root-only
inspection and its one-node cost (summed across input files). They describe this
report's work, not provider scan or pruning traversal. Dictionary availability
and per-column physical encodings are explicitly uninspected; schema-derived
column roles and actual file-statistics/pruning evidence remain available.

Call `VortexLocalPrimitiveEmbeddedLayoutReport::inspect_file_layouts(file,
max_layout_nodes)` for a complete metadata-only layout inventory. This explicit
walk is iterative, bounded and fails on malformed children or exceeded limits;
it cannot return a successful partial inventory. It reads no array segments and
does not run a query. The already opened file retains its provider metadata cache.

Deterministic tests supply lazy native layout children with construction counters:
query reports realize zero children, full inspection realizes every branch and
reports every layout encoding, and malformed/over-budget inspection fails. The
existing metadata-pruned count test retains complete count/certificate checks
and now requires root-only metadata. CLI tests cover the additive fields.

Validation and measured ship/drop remain pending the root agent's serial gate.
The targeted comparison is Q2 and short Q37–43 against the same numeric artifact,
with exact results and raw timings. Existing 8.1→70.2 ms control-plane evidence
motivates this candidate but does not isolate inventory time or prove its win.
