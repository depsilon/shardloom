import { defineCollection } from "astro:content";
import { docsLoader } from "@astrojs/starlight/loaders";
import { docsSchema } from "@astrojs/starlight/schema";
import { glob } from "astro/loaders";
import { z } from "astro/zod";

const useCases = defineCollection({
  loader: glob({ base: "./src/content/use-cases", pattern: "**/*.json" }),
  schema: z.object({
    id: z.string(),
    title: z.string(),
    status: z.string(),
    audience: z.string(),
    execution_mode: z.string(),
    engine_mode: z.string(),
    inputs: z.array(z.string()).default([]),
    outputs: z.array(z.string()).default([]),
    evidence_fields: z.array(z.string()).default([]),
    claim_boundary: z.string(),
    references: z.array(z.string()).default([]),
    related_use_cases: z.array(z.string()).default([]),
  }),
});

const statusRows = defineCollection({
  loader: glob({ base: "./src/content/status", pattern: "**/*.json" }),
  schema: z.object({
    capability: z.string(),
    status: z.string(),
    route: z.string(),
    platform: z.string(),
    works: z.string(),
    blocked: z.string(),
    inputs: z.array(z.string()).default([]),
    outputs: z.array(z.string()).default([]),
    evidence: z.array(z.string()).default([]),
    references: z.array(z.string()).default([]),
  }),
});

export const collections = {
  docs: defineCollection({ loader: docsLoader(), schema: docsSchema() }),
  useCases,
  statusRows,
};
