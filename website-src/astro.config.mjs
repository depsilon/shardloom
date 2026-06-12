import { defineConfig } from "astro/config";
import mdx from "@astrojs/mdx";
import sitemap from "@astrojs/sitemap";
import starlight from "@astrojs/starlight";

export default defineConfig({
  site: "https://shardloom.io",
  output: "static",
  outDir: "../website",
  publicDir: "../website-public",
  trailingSlash: "never",
  integrations: [
    starlight({
      title: "ShardLoom",
      description: "ShardLoom field guide for Vortex-native, no-fallback compute evidence.",
      favicon: "/assets/logo/shardloom-favicon.png",
      customCss: ["./src/styles/starlight.css"],
      head: [
        {
          tag: "script",
          content:
            "try{if(!localStorage.getItem('starlight-theme'))localStorage.setItem('starlight-theme','light')}catch{}",
        },
        {
          tag: "link",
          attrs: { rel: "stylesheet", href: "/assets/site.css" },
        },
        {
          tag: "meta",
          attrs: { name: "robots", content: "index,follow" },
        },
      ],
      pagefind: true,
      social: [
        { icon: "github", label: "GitHub", href: "https://github.com/depsilon/shardloom" },
      ],
      sidebar: [
        {
          label: "Start",
          items: [
            { label: "Website home", link: "/" },
            { slug: "field-guide/start-local-proof" },
            { slug: "field-guide/python-surface" },
            { label: "Benchmarks", link: "/benchmarks" },
            { label: "Compute flow", link: "/compute-engine-flow" },
          ],
        },
        {
          label: "Core Concepts",
          items: [
            { slug: "field-guide/what-is-shardloom" },
            { slug: "field-guide/no-fallback" },
            { slug: "field-guide/evidence-gated-compute" },
            { slug: "field-guide/universal-ingress" },
            { slug: "field-guide/source-state" },
            { slug: "field-guide/vortex-ingest" },
            { slug: "field-guide/vortex-prepared-state" },
            { slug: "field-guide/prepared-vortex" },
            { slug: "field-guide/native-vortex" },
          ],
        },
        {
          label: "Benchmarks And Boundaries",
          items: [
            { slug: "field-guide/benchmark-methodology" },
            { slug: "field-guide/benchmark-evidence" },
            { slug: "field-guide/certified-cold-route" },
            { slug: "field-guide/prepared-warm-route" },
            { slug: "field-guide/external-baseline-only" },
            { slug: "field-guide/limitations" },
            { slug: "field-guide/deterministic-blockers" },
          ],
        },
        {
          label: "Reference Atlas",
          items: [{ slug: "field-guide" }],
        },
      ],
    }),
    mdx(),
    sitemap(),
  ],
});
