export const siteNav = [
  ["Home", "/", "home"],
  ["Start", "/start", "start"],
  ["Benchmarks", "/field-guide/benchmark-methodology", "benchmarks"],
  ["Compute Flow", "/field-guide/compute-flow", "compute-flow"],
  ["Field Guide", "/field-guide", "field-guide"],
  ["About", "/about", "about"],
  ["GitHub", "https://github.com/depsilon/shardloom", "github"],
] as const;

export function repoLink(reference: string): string {
  return `https://github.com/depsilon/shardloom/blob/main/${reference}`;
}
