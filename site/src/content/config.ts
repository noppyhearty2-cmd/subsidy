import { defineCollection, z } from "astro:content";

const subsidies = defineCollection({
  type: "content",
  schema: z.object({
    title: z.string(),
    municipality: z.string(),
    target: z.string(),
    amount: z.string().optional(),
    deadline: z.string().optional(),
    tags: z.array(z.string()).default([]),
    key_points: z.array(z.string()).default([]),
    source_url: z.string().url(),
    summary_ja: z.string().optional(),
    scraped_at: z.string(),
    is_active: z.boolean().default(true),
    content_hash: z.string().optional(),
  }),
});

export const collections = { subsidies };
