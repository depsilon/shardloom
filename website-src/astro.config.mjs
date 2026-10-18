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
      description: "Install ShardLoom, run local queries, and understand Vortex-native execution.",
      favicon: "/assets/logo/shardloom-mark.svg",
      customCss: ["./src/styles/starlight.css"],
      components: {
        SiteTitle: "./src/components/GuideSiteTitle.astro",
        MobileMenuToggle: "./src/components/GuideMenuToggle.astro",
      },
      pagination: false,
      credits: false,
      head: [
        {
          tag: "script",
          content:
            "try{if(!localStorage.getItem('starlight-theme'))localStorage.setItem('starlight-theme','dark')}catch{}",
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
            { slug: "field-guide", label: "Overview" },
            { slug: "field-guide/start-local-proof", label: "Install and run" },
          ],
        },
        {
          label: "Work with data",
          items: [
            { slug: "field-guide/python-surface" },
            { slug: "field-guide/sql" },
            { slug: "field-guide/cli" },
            { slug: "field-guide/runtime-and-io" },
            { slug: "field-guide/resources" },
          ],
        },
        {
          label: "Understand the engine",
          items: [
            { slug: "field-guide/execution-model" },
            { slug: "field-guide/compute-flow" },
            { slug: "field-guide/execution-routes" },
          ],
        },
        {
          label: "Reference",
          items: [
            { slug: "field-guide/reference" },
            { slug: "field-guide/embedding" },
          ],
        },
        {
          label: "Versions and help",
          items: [
            { slug: "field-guide/limitations" },
            { slug: "field-guide/troubleshooting" },
            { slug: "field-guide/development" },
            { slug: "field-guide/benchmark-methodology" },
          ],
        },
        {
          label: "Explore",
          items: [
            { label: "Website home", link: "/" },
          ],
        },
      ],
    }),
    mdx(),
    sitemap(),
  ],
});
