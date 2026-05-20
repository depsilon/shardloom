import fs from "node:fs";
import path from "node:path";

const repoRoot = path.resolve(process.cwd(), "..");
const flowPath = path.join(repoRoot, "docs", "architecture", "compute-engine-flow-reference.md");

function stripMarkdown(value: string): string {
  return value
    .replace(/\[([^\]]+)\]\([^)]+\)/g, "$1")
    .replace(/`([^`]+)`/g, "$1")
    .replace(/\*\*([^*]+)\*\*/g, "$1")
    .replace(/<[^>]+>/g, "")
    .replace(/\s+/g, " ")
    .trim();
}

function labelParts(label: string): [string, string] {
  const clean = label.replace(/<br\s*\/?>/g, "\n");
  const parts = clean.split(/\n+/).map((part) => stripMarkdown(part)).filter(Boolean);
  return [parts[0] ?? "Architecture step", parts.slice(1).join(" ")];
}

export function flowMarkdown(): string {
  return fs.readFileSync(flowPath, "utf8");
}

export function mermaidBlocks(markdown = flowMarkdown()) {
  const blocks: { heading: string; source: string }[] = [];
  const lines = markdown.split(/\r?\n/);
  let heading = "Architecture diagram";
  for (let index = 0; index < lines.length; index += 1) {
    const line = lines[index] ?? "";
    if (/^#{2,4}\s+/.test(line)) heading = stripMarkdown(line.replace(/^#{2,4}\s+/, ""));
    if (line.trim() !== "```mermaid") continue;
    const block: string[] = [];
    index += 1;
    while (index < lines.length && lines[index]?.trim() !== "```") {
      block.push(lines[index] ?? "");
      index += 1;
    }
    blocks.push({ heading, source: block.join("\n").trim() });
  }
  return blocks;
}

export function renderedDiagramData() {
  return mermaidBlocks().slice(0, 8).map((block, blockIndex) => {
    const labels = new Map<string, { title: string; detail: string }>();
    const order: string[] = [];
    const patterns = [
      /(?:subgraph\s+)?([A-Za-z][\w-]*)\["([^"]+)"\]/g,
      /([A-Za-z][\w-]*)\{"([^"]+)"\}/g,
    ];
    for (const line of block.source.split(/\r?\n/)) {
      for (const pattern of patterns) {
        for (const match of line.matchAll(pattern)) {
          const [, id, label] = match;
          if (!labels.has(id)) order.push(id);
          const [title, detail] = labelParts(label);
          labels.set(id, { title, detail });
        }
      }
    }
    const paths: string[][] = [];
    for (const line of block.source.split(/\r?\n/)) {
      if (!line.includes("-->")) continue;
      const normalized = line.trim().replace(/-->\|[^|]*\|/g, "-->");
      const path = normalized
        .split("-->")
        .map((piece) => piece.trim().match(/^([A-Za-z][\w-]*)/)?.[1])
        .filter((id): id is string => Boolean(id && labels.has(id)));
      if (path.length >= 2) paths.push(path);
    }
    let primary = paths.sort((left, right) => right.length - left.length)[0] ?? order.slice(0, 8);
    if (primary.length <= 2 && order.length > 2) primary = order.slice(0, 9);
    const primarySet = new Set(primary);
    return {
      number: blockIndex + 1,
      heading: block.heading,
      nodes: primary.slice(0, 9).map((id) => ({ id, ...labels.get(id)! })),
      branches: order.filter((id) => !primarySet.has(id)).slice(0, 8).map((id) => ({ id, ...labels.get(id)! })),
      source: block.source,
    };
  });
}
