import fieldGuide from "../data/field-guide.json";
import statusRows from "../data/status-rows.json";
import useCaseIndex from "../data/use-case-index.json";
import benchmarkEvidence from "../data/benchmark-evidence.json";
import benchmarkManifest from "../data/benchmark-manifest.json";

export type FieldGuideTerm = (typeof fieldGuide)[number];
export type StatusRow = (typeof statusRows)[number];
export type UseCase = (typeof useCaseIndex.use_cases)[number];

export const siteNav = [
  ["Home", "/", "home"],
  ["Start", "/start", "start"],
  ["Field Guide", "/field-guide", "field-guide"],
  ["Use Cases", "/use-cases", "use-cases"],
  ["Benchmarks", "/benchmarks", "benchmarks"],
  ["Architecture", "/architecture", "architecture"],
  ["Status", "/status", "status"],
  ["Docs", "/docs", "docs"],
  ["GitHub", "https://github.com/depsilon/shardloom", "github"],
] as const;

export const fieldGuideTerms = fieldGuide;
export const publicStatusRows = statusRows;
export const useCases = useCaseIndex.use_cases;
export const capabilityFamilies = useCaseIndex.capability_families;
export const benchmark = benchmarkEvidence;
export const manifest = benchmarkManifest;

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
    fixture_smoke_only: "fixture_smoke_only",
    not_planned: "not_planned",
  };
  return labels[value] ?? value;
}

export function formatList(values: unknown, fallback = "not reported"): string {
  if (Array.isArray(values)) return values.join(", ") || fallback;
  if (typeof values === "string") return values || fallback;
  return fallback;
}

export function routeMetrics() {
  const rows = Array.isArray((benchmark as any).rows) ? (benchmark as any).rows : [];
  const batchRows = Array.isArray((benchmark as any).batch_rows) ? (benchmark as any).batch_rows : [];
  const allRows = [...rows, ...batchRows];
  const routeRows = allRows.filter((row) => row && row.engine && String(row.engine).includes("shardloom"));
  const claimGrade = routeRows.filter((row) => row.claim_gate_status === "claim_grade").length;
  const fixtureSmoke = routeRows.filter((row) => row.claim_gate_status === "fixture_smoke_only").length;
  const sourceStateRows = routeRows.filter((row) =>
    Object.keys(row).some((key) => key.includes("source_state")),
  ).length;
  return {
    routeRows: routeRows.length,
    claimGrade,
    fixtureSmoke,
    sourceStateRows,
    expectedLanes: Array.isArray((manifest as any).expected_lanes) ? (manifest as any).expected_lanes.length : 0,
    availableLanes: Array.isArray((manifest as any).available_lanes) ? (manifest as any).available_lanes.length : 0,
    missingLanes: Array.isArray((manifest as any).missing_lanes) ? (manifest as any).missing_lanes.length : 0,
  };
}

export function repoLink(reference: string): string {
  return `https://github.com/depsilon/shardloom/blob/main/${reference}`;
}

export function statusClass(value: string): string {
  return `status-${siteStatus(value).replaceAll("_", "-")}`;
}
