import { defineConfig } from "astro/config";

// base must match the GitHub repository name (case-sensitive)
export default defineConfig({
  site: "https://noppyhearty2-cmd.github.io",
  base: "/subsidy",
  output: "static",
});
