# Vortex Native Capability Utilization Audit

## Scope

Audit-only review of `shardloom-vortex` dependency posture and adapter boundary behavior.
No execution/IO implementation was added.

## Key conclusions

- Keep upstream usage isolated in `shardloom-vortex`; this is currently true.
- Keep default builds lightweight (`default = []`), and keep upstream Vortex opt-in.
- Use upstream Vortex for type/encoding/layout/statistics semantics, but only through ShardLoom adapters.
- Build ShardLoom-native planning, diagnostics, runtime, memory, and policy layers.
- Avoid broadening to file/object-store/write paths until feature-gated contracts are landed.

## Default feature posture

Recommended staged feature layout:

- `default = []`
- `upstream-vortex` (opt-in)
- `vortex-file-io` (future, opt-in)
- `vortex-object-store` (future, opt-in, depends on file-io)
- `vortex-write` (future, opt-in, depends on file-io)

## Capability utilization decisions

| Capability | Decision |
|---|---|
| DType/logical type model | Use upstream via `shardloom-vortex` mapping layer |
| Array representation | Use upstream later; keep core execution abstractions ShardLoom-native |
| Encodings/layouts/statistics/validity | Use upstream semantics; normalize into ShardLoom core/report types |
| File metadata and scan/split APIs | Defer runtime use; retain plan-only bridge |
| Predicate pushdown | Build ShardLoom-native conservative pruning logic over normalized metadata |
| Write/output APIs | Defer implementation; preserve native Vortex target contract |
| Arrow interop | Boundary-only, not default internal execution substrate |
| Object-store integration | Defer; keep feature-gated and disabled by default |
| Compression codecs | Reuse upstream codecs; do not reimplement |
| Session/runtime APIs | Build ShardLoom-native runtime/task/memory/orchestration |

## Risks to watch

- Umbrella `vortex` feature can pull a heavy graph when enabled.
- Accidental decode-to-Arrow-first behavior if adapter boundaries loosen.
- Object-store creep before explicit capability/diagnostic contracts.
- Upstream API churn for typed mapping surfaces.
- Unnecessary duplication of Vortex internals in ShardLoom.
