# DataFrame, Notebook, And Package Surface Readiness

Status: `report_only`

GAR item: `GAR-0010-B`

## Summary

ShardLoom exposes a first-class readiness matrix for Python package, DataFrame, and notebook-facing
surfaces without upgrading those surfaces to broad runtime support. The matrix is available through
capability discovery and the Python typed capability view so users and agents can distinguish:

- local package metadata/import readiness,
- editable/source-tree install smoke,
- DataFrame/query-builder method posture,
- notebook display/runtime blockers,
- public package publication blockers,
- deterministic unsupported diagnostics.

The matrix is intentionally non-runtime. It does not publish packages, import DataFrame libraries,
render notebook output, execute broad DataFrame plans, invoke external engines, or relax the
no-fallback policy.

## Source References

- RFC 0010 Developer Experience.
- RFC 0024 Release Engineering.
- RFC 0032 Capability Surface.
- `python/README.md`.
- `docs/release/package-metadata-audit.md`.
- `docs/release/package-channel-readiness-matrix.md`.
- `docs/architecture/sql-parser-binder-readiness.md`.
- `docs/architecture/phased-execution-plan.md`.

## Capability Surface

The report is emitted for these capability scopes:

- `python`
- `dataframe`
- `notebook`
- `deployment`
- `api-surfaces`

Report fields use the prefix:

```text
dataframe_notebook_package_readiness_
```

The stable row order is:

```text
python_package_metadata
editable_install_smoke
dataframe_method_matrix
notebook_display_surface
public_package_publication
unsupported_diagnostics
```

## Row Semantics

`python_package_metadata` is `ready_local`. It means local metadata, source-tree import, and typed
capability documentation are present. It is not a PyPI, Conda, Homebrew, or production package
claim.

`editable_install_smoke` is `smoke_supported`. It means an editable/source-tree local smoke path can
be discussed separately from public publication. It is not release-channel readiness.

`dataframe_method_matrix` is `report_only` as a readiness row. It points users to the typed
DataFrame method matrix, where individual methods may be side-effect-free declarations,
fixture-smoke-supported scoped local workflows, or deterministic unsupported diagnostics. It does
not make broad DataFrame runtime supported.

`notebook_display_surface` remains `blocked` for broad notebook runtime certification. The Python
front door now has a scoped `WorkflowNotebookPreview` for admitted bounded local-source rows, but
general rich display, unbounded decoded DataFrame materialization, and production notebook
certification still require broader materialization-boundary, decode, and execution evidence.

`public_package_publication` is `blocked`. TestPyPI, PyPI, Conda, Homebrew, and other installer
channels require the package-channel release gates and provenance evidence before any public
availability claim.

`unsupported_diagnostics` is `ready_local`. Unsupported package/DataFrame/notebook requests remain
deterministic diagnostic surfaces with `fallback_attempted=false` and
`external_engine_invoked=false`.

## Claim Boundary

The matrix supports this claim only:

```text
ShardLoom exposes local package/DataFrame/notebook readiness posture and deterministic unsupported
diagnostics.
```

It does not support claims that ShardLoom has:

- a public package release,
- production package availability,
- production SQL/DataFrame support,
- notebook runtime support,
- broad DataFrame execution,
- object-store/lakehouse or Foundry runtime support,
- performance or superiority evidence,
- Spark replacement capability.

If evidence is missing, the report remains:

```text
claim_gate_status=not_claim_grade
```

## No-Fallback Requirements

Every row must preserve:

```text
fallback_attempted=false
external_engine_invoked=false
```

No row may invoke pandas, Polars, DuckDB, DataFusion, Spark, a notebook renderer, a network service,
or a package repository as fallback execution.

## Acceptance

- Capability discovery exposes the readiness matrix for Python, DataFrame, notebook, deployment,
  and API-surface scopes.
- Python typed accessors can inspect row status, blockers, required evidence, and claim boundaries.
- Local install smoke is visibly distinct from package publication and runtime support.
- Broad DataFrame and notebook runtime remain unclaimed.
- Public package publication remains blocked until the release/package gates pass.

## Verification

```powershell
cargo test -p shardloom-cli --test capability_discovery_snapshots
python -m unittest python.tests.test_cli_client
cargo test -p shardloom-contract-tests --test release_readiness_metadata
python -m compileall -q python/src python/tests scripts examples benchmarks/traditional_analytics
cargo fmt --all -- --check
git diff --check
```
