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
          label: "Start here",
          items: [
            { slug: "field-guide", label: "Overview" },
            { slug: "field-guide/start-local-proof", label: "Install and run" },
            { slug: "field-guide/python-surface", label: "Python" },
          ],
        },
        {
          label: "Understand the engine",
          items: [
            { slug: "field-guide/execution-model" },
            { slug: "field-guide/compute-flow" },
            { slug: "field-guide/execution-routes" },
            { slug: "field-guide/runtime-and-io" },
          ],
        },
        {
          label: "Evidence and limits",
          items: [
            { slug: "field-guide/benchmark-methodology" },
            { slug: "field-guide/limitations" },
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
