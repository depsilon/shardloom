import fieldGuide from "../data/field-guide.json";

export type FieldGuideTerm = (typeof fieldGuide)[number];

export const siteNav = [
  ["Home", "/", "home"],
  ["Start", "/start", "start"],
  ["Benchmarks", "/benchmarks", "benchmarks"],
  ["Compute Flow", "/compute-engine-flow", "compute-flow"],
  ["Field Guide", "/field-guide", "field-guide"],
  ["About", "/about", "about"],
  ["GitHub", "https://github.com/depsilon/shardloom", "github"],
] as const;

export const fieldGuideTerms = fieldGuide;

const REFERENCE_PROOFS: Record<string, string> = {
  "README.md": "Public technical-preview posture, Vortex-first positioning, and no-fallback boundaries.",
  "python/README.md": "Python wrapper scope, local smoke usage, and Python API claim boundaries.",
  "docs/architecture/compute-engine-flow-reference.md":
    "Canonical execution-mode, engine-mode, evidence, and claim-gate flow definitions.",
  "docs/architecture/effect-budget-plan.md":
    "Deny-by-default effect budget policy and the local fixture exceptions for the current effectful-operation slice.",
  "docs/architecture/effectful-operation-admission-matrix.md":
    "Effectful-operation admission rows for local SQLite, extension metadata, deterministic UDF fixture, and blocked external effects.",
  "docs/architecture/extension-manifest-effect-capability-matrix.md":
    "Extension manifest inspection posture and blockers for dynamic loading, plugin execution, and arbitrary UDF execution.",
  "docs/architecture/object-store-request-planner.md":
    "Object-store route admission, local-emulator evidence, and remote-provider blockers.",
  "docs/architecture/table-intelligence-layer.md":
    "Table maintenance execution posture and lakehouse/table claim boundaries.",
  "docs/architecture/phased-execution-completed-ledger.md":
    "Completed runtime provenance and historical phase evidence for this use case.",
  "docs/architecture/universal-compatibility-coverage-scoreboard.md":
    "Compatibility scoreboard status and source/sink support boundaries.",
  "docs/architecture/universal-input-contract.md":
    "Universal input contract posture and unsupported input-family diagnostics.",
  "docs/architecture/universal-ingress-route-taxonomy.md":
    "UniversalIngress, Vortex ingest, prepared-state, and route-timing contract boundaries.",
  "docs/status/cli-command-registry.md":
    "CLI registry status, public route facade command discovery, user-surface posture, and no-fallback metadata.",
  "docs/benchmarks/local-taxonomy-benchmark.md":
    "Local benchmark taxonomy, evidence rows, and workload-scoped interpretation boundaries.",
  "docs/benchmarks/baseline-comparison-boundary.md":
    "Benchmark comparison boundaries and external-baseline-only policy.",
};

export function slug(value: string): string {
  return value.toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/(^-|-$)/g, "") || "item";
}

export function siteStatus(value: string): string {
  const labels: Record<string, string> = {
    ready_local: "runtime_supported",
    smoke_supported: "smoke_supported",
    report_only: "report_only",
    planned: "not_planned",
    blocked: "blocked",
    unsupported: "unsupported",
    runtime_supported: "runtime_supported",
    global_runtime_supported: "global_runtime_supported",
    fixture_smoke_only: "fixture_smoke_only",
    not_planned: "not_planned",
    executable: "executable",
    feature_gated: "feature_gated",
    diagnostic_only: "diagnostic_only",
    claim_grade: "claim_grade",
    external_baseline_only: "external_baseline_only",
    future: "future",
  };
  return labels[value] ?? value;
}

export function formatList(values: unknown, fallback = "not reported"): string {
  if (Array.isArray(values)) return values.join(", ") || fallback;
  if (typeof values === "string") return values || fallback;
  return fallback;
}

export function referenceProof(reference: string): string {
  return REFERENCE_PROOFS[reference] ?? "This source anchors the page claim boundary, evidence fields, and support posture.";
}

export function repoLink(reference: string): string {
  return `https://github.com/depsilon/shardloom/blob/main/${reference}`;
}

export function statusClass(value: string): string {
  return `status-${siteStatus(value).replaceAll("_", "-")}`;
}
